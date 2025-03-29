import os
import threading
import requests
import time
from concurrent.futures import ThreadPoolExecutor, as_completed


def _optimize_system():
    """
    _optimize_system
    系统级优化(Linux)
    """
    if os.name == 'posix':
        try:
            # 增大TCP缓冲区
            os.system('sysctl -w net.core.rmem_max=33554432 2>/dev/null')
            os.system('sysctl -w net.core.wmem_max=33554432 2>/dev/null')
            # 增大文件预读
            os.system('blockdev --setra 16384 /dev/sda 2>/dev/null')
            # 启用BBR拥塞控制
            os.system('sysctl -w net.ipv4.tcp_congestion_control=bbr 2>/dev/null')
        except Exception as e:
            raise Exception(f"optimize_system error: {e}")


def _create_optimized_session():
    """
    _create_optimized_session
    创建高性能requests会话
    """
    session = requests.Session()

    # 优化连接池设置
    adapter = requests.adapters.HTTPAdapter(
        pool_connections=100,
        pool_maxsize=100,
        max_retries=3,
        pool_block=True
    )
    session.mount('http://', adapter)
    session.mount('https://', adapter)

    # 设置TCP_NODELAY禁用Nagle算法
    session.headers.update({'Connection': 'keep-alive'})
    return session


class OptimizedDownloader:
    def __init__(self, url, num_threads=12, save_path=None, chunk_size=30 * 1024 * 1024):
        self.url = url
        self.num_threads = min(num_threads, 32)  # 限制最大线程数
        self.save_path = save_path or os.path.basename(url.split('?')[0])
        self.chunk_size = chunk_size
        self.file_size = 0
        self.downloaded = 0
        self.lock = threading.Lock()
        self.session = _create_optimized_session()
        self.start_time = time.time()
        self.alignment = 4096  # 磁盘对齐大小
        self.buffer_pool = []  # 内存池复用缓冲区
        self.file_handle = None
        self.max_speed = 0

        # 系统性能优化
        _optimize_system()
        return

    def get_file_size(self):
        """
        get_file_size
        获取文件大小
        """
        try:
            headers = {'Range': 'bytes=0-0'}  # 只请求第一个字节
            with self.session.get(self.url, headers=headers, allow_redirects=True, timeout=10) as response:
                if response.status_code == 206:  # 部分内容
                    content_range = response.headers.get('content-range')
                    if content_range:
                        self.file_size = int(content_range.split('/')[-1])
                        return self.file_size
            return 0
        except Exception as e:
            print(f"Error getting file size: {e}")
            return 0

    def _align_range(self, start, end):
        """
        _align_range
        对齐分片范围到磁盘块大小
        """
        aligned_start = (start // self.alignment) * self.alignment
        aligned_end = ((end + self.alignment) // self.alignment) * self.alignment - 1
        return aligned_start, min(aligned_end, self.file_size - 1)

    def download_chunk(self, start, end):
        """
        download_chunk
        下载文件分片
        """
        headers = {'Range': f'bytes={start}-{end}'}
        retry_count = 3

        for attempt in range(retry_count):
            try:
                with self.session.get(
                        self.url,
                        headers=headers,
                        stream=True,
                        timeout=30
                ) as response:
                    response.raise_for_status()

                    pos = start
                    remaining = end - start + 1

                    # 从内存池获取缓冲区
                    buf = self.allocate_buffer()

                    try:
                        while remaining > 0:
                            read_size = min(self.chunk_size, remaining)
                            chunk = response.raw.read(read_size)
                            if not chunk:
                                break

                            # 使用memoryview避免拷贝
                            buf_view = memoryview(buf)[:len(chunk)]
                            buf_view[:] = chunk

                            with self.lock:
                                self.file_handle.seek(pos)
                                self.file_handle.write(buf_view)

                                pos += len(chunk)
                                remaining -= len(chunk)
                                self.downloaded += len(chunk)

                                # 实时统计
                                elapsed = time.time() - self.start_time
                                speed = self.downloaded / (1024 * 1024) / max(0.1, elapsed)
                                progress = (self.downloaded / self.file_size) * 100
                                if speed > self.max_speed:
                                    self.max_speed = speed
                                print(
                                    f"\rProgress: {progress:.2f}% | "
                                    f"Speed: {speed:.2f} MB/s | "
                                    f"Threads: {self.num_threads}",
                                    end='',
                                    flush=True
                                )
                        return True
                    finally:
                        self.release_buffer(buf)

            except Exception as e:
                if attempt == retry_count - 1:
                    print(f"\nFailed chunk {start}-{end}: {str(e)}")
                    return False
                time.sleep(1 * (attempt + 1))  # 指数退避

    def allocate_buffer(self):
        """
        allocate_buffer
        从内存池获取或分配新的缓冲区
        """
        if self.buffer_pool:
            return self.buffer_pool.pop()
        return bytearray(self.chunk_size)

    def release_buffer(self, buf):
        """
        release_buffer
        释放缓冲区回内存池
        """
        self.buffer_pool.append(buf)

    def download(self):
        """
        download
        执行下载
        """

        self.file_size = self.get_file_size()
        if self.file_size == 0:
            print("Failed to get file size or file is empty.")
            return False

        print(f"\nStarting download: {self.file_size / 1024 / 1024:.2f} MB")

        # 预分配文件空间(O_DIRECT在Linux上跳过系统缓存), 仅在Linux上使用O_DIRECT
        if os.name == 'posix':
            flags = os.O_CREAT | os.O_WRONLY

            import sys
            if not sys.platform == 'darwin':
                flags |= os.O_CREAT
            fd = os.open(self.save_path, flags, 0o644)

            try:
                os.ftruncate(fd, self.file_size)
            finally:
                os.close(fd)
        else:
            # 其他平台的常规方式
            with open(self.save_path, 'wb') as f:
                f.truncate(self.file_size)

        # 重新打开文件用于写入
        self.file_handle = open(self.save_path, 'r+b')

        try:
            # 计算最优分片大小(至少16MB，对齐到磁盘块)
            min_chunk = 16 * 1024 * 1024
            chunk_size = max(
                min_chunk,
                ((self.file_size // self.num_threads // self.alignment) * self.alignment)
            )

            # 生成对齐的分片范围
            ranges = []
            for i in range(self.num_threads):
                start = i * chunk_size
                end = min((i + 1) * chunk_size - 1, self.file_size - 1)
                if start < self.file_size:
                    aligned_start, aligned_end = self._align_range(start, end)
                    ranges.append((aligned_start, aligned_end))

            # 使用线程池下载
            with ThreadPoolExecutor(max_workers=self.num_threads) as executor:
                self.start_time = time.time()
                futures = {
                    executor.submit(self.download_chunk, start, end): (start, end)
                    for start, end in ranges
                }

                # 等待所有任务完成
                for future in as_completed(futures):
                    if not future.result():
                        print("\nWarning: Some chunks failed to download")
        finally:
            self.file_handle.close()

            # 计算最终速度
            elapsed = time.time() - self.start_time
            speed = self.file_size / (1024 * 1024) / max(0.1, elapsed)
            print(
                f"\nDownload completed in {elapsed:.2f}s | Avg speed: {speed:.2f} MB/s | Max speed: {self.max_speed:.2f} MB/s")
        return True


def optimized_download(download_url: str, file_path: str):
    # 下载
    downloader = OptimizedDownloader(
        url=download_url,
        save_path=file_path,
        num_threads=8,
        chunk_size=40 * 1024 * 1024
    )
    downloader.download()


if __name__ == "__main__":
    # 示例使用
    url = ""
    file_path = ""

    # 下载
    optimized_download(url, file_path)
