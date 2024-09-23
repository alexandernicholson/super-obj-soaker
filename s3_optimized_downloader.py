import argparse
import boto3
import logging
import os
import time
import shutil
from botocore.exceptions import ClientError
from collections import deque
from statistics import mean
from boto3.s3.transfer import TransferConfig
import urllib3
import multiprocessing
from multiprocessing import Process, Queue, Lock, Value, Event
import queue
import signal
import fnmatch
from threading import Event as ThreadingEvent
from datetime import datetime, timezone

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# At the beginning of the script, set the start method to 'fork' if available
if hasattr(multiprocessing, 'get_start_method'):
    try:
        multiprocessing.set_start_method('fork')
    except RuntimeError:
        # If it's already set and we can't change it, that's okay
        pass

class S3OptimizedDownloader:
    def __init__(self, bucket, prefix, destination, region, endpoint_url=None, include_patterns=None, exclude_patterns=None):
        # Disable warnings for self-signed certificates
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        
        self.region = region
        self.endpoint_url = endpoint_url
        self.verify_ssl = os.environ.get('S3_VERIFY_SSL', 'true').lower() == 'true'
        self.s3 = self._create_s3_client()

        self.bucket = bucket
        self.prefix = prefix
        self.destination = destination
        self.objects = []
        self.downloaded_bytes = Value('i', 0)
        self.start_time = time.time()
        self.min_processes = int(os.environ.get('MIN_PROCESSES', '1'))
        self.max_processes = int(os.environ.get('MAX_PROCESSES', '16'))
        self.max_speed = float(os.environ.get('MAX_SPEED', '99999999999999999999'))  # MB/s
        self.speed_history = deque(maxlen=60)  # Store last 60 seconds of speed data
        self.optimization_interval = float(os.environ.get('OPTIMIZATION_INTERVAL', '10'))  # Default to 10 seconds
        self.total_size = 0
        self.task_queue = Queue()
        self.completed_tasks = Value('i', 0)
        self.total_tasks = Value('i', 0)
        self.lock = Lock()
        self.processes = []
        self.optimizer_process = None
        self.shutdown_event = Event()
        self.max_retries = int(os.environ.get('MAX_RETRIES', '3'))
        self.retry_delay = float(os.environ.get('RETRY_DELAY', '5'))  # seconds
        # Shared value for process count
        self.process_count = Value('i', self.min_processes)
        self.include_patterns = include_patterns or []
        self.exclude_patterns = exclude_patterns or []
        self.download_complete = ThreadingEvent()
        self.shutdown_requested = False

    def _create_s3_client(self):
        return boto3.client('s3', region_name=self.region, endpoint_url=self.endpoint_url,
                            verify=self.verify_ssl)  # Disable SSL verification for local testing

    def list_objects(self):
        logger.info(f"Listing objects in s3://{self.bucket}/{self.prefix}")
        paginator = self.s3.get_paginator('list_objects_v2')
        try:
            for page in paginator.paginate(Bucket=self.bucket, Prefix=self.prefix):
                for obj in page.get('Contents', []):
                    if self.should_process_object(obj['Key']):
                        self.objects.append(obj)
                        self.total_size += obj['Size']
            logger.info(f"Found {len(self.objects)} objects with total size of {self.total_size / (1024**3):.2f} GB")
        except Exception as e:
            logger.error(f"Error listing objects: {e}")

    def should_process_object(self, key):
        # Remove the prefix from the key for pattern matching
        relative_key = key[len(self.prefix):].lstrip('/')
        
        # If there are include patterns, the key must match at least one
        if self.include_patterns and not any(fnmatch.fnmatch(relative_key, pattern) for pattern in self.include_patterns):
            return False
        
        # If there are exclude patterns, the key must not match any
        if self.exclude_patterns and any(fnmatch.fnmatch(relative_key, pattern) for pattern in self.exclude_patterns):
            return False
        
        return True

    def download_all(self):
        # Clear the download_complete event at the start of each download
        self.download_complete.clear()
        
        try:
            os.makedirs(self.destination, exist_ok=True)

            self.list_objects()  # Ensure we have the list of objects
            if not self.objects:
                logger.warning("No objects found to download. Exiting.")
                return

            self.populate_queue()
            self.start_optimizer()
            self.start_workers()

            # Wait for all tasks to complete or shutdown to be requested
            while self.completed_tasks.value < self.total_tasks.value and not self.shutdown_requested:
                time.sleep(1)

            # Terminate the optimizer process if it's still running
            if self.optimizer_process and self.optimizer_process.is_alive():
                self.optimizer_process.terminate()
                self.optimizer_process.join()

            logger.info("Download completed or shutdown requested")
            self.download_complete.set()  # Signal that download is complete
        except Exception as e:
            logger.error(f"An unexpected error occurred: {e}")
        finally:
            self.terminate_processes()

    def populate_queue(self):
        for obj in self.objects:
            self.task_queue.put(obj)
            with self.total_tasks.get_lock():
                self.total_tasks.value += 1
            logger.debug(f"Task queued for object: {obj['Key']}")

    def start_workers(self):
        current_process_count = self.process_count.value
        for _ in range(current_process_count):
            worker = Process(target=self.worker_download)
            worker.start()
            self.processes.append(worker)
            logger.info(f"Worker process {worker.pid} started. Total workers: {len(self.processes)}")

    def worker_download(self):
        # Set up signal handlers for the worker process
        signal.signal(signal.SIGTERM, self.worker_signal_handler)
        signal.signal(signal.SIGINT, self.worker_signal_handler)

        s3 = self._create_s3_client()
        while not self.shutdown_event.is_set():
            try:
                obj = self.task_queue.get(timeout=1)
            except queue.Empty:
                continue
            except Exception as e:
                logger.error(f"Error getting task from queue: {e}")
                break

            retries = 0
            while retries < self.max_retries:
                try:
                    downloaded_size = self.download_object(obj, self.bucket, self.destination, s3, self.prefix)
                    with self.lock:
                        self.downloaded_bytes.value += downloaded_size
                    with self.completed_tasks.get_lock():
                        self.completed_tasks.value += 1
                    break  # Success, exit retry loop
                except Exception as e:
                    retries += 1
                    logger.error(f"Error downloading {obj['Key']}: {e}. Retry {retries}/{self.max_retries}")
                    time.sleep(self.retry_delay)
            else:
                logger.error(f"Failed to download {obj['Key']} after {self.max_retries} retries")
                with self.completed_tasks.get_lock():
                    self.completed_tasks.value += 1

    def worker_signal_handler(self, signum, frame):
        logger.info(f"Worker process received signal {signum}. Initiating graceful shutdown...")
        self.shutdown_event.set()

    def terminate_processes(self):
        self.shutdown_event.set()
        # Terminate worker processes
        for process in self.processes:
            if process and process.is_alive():
                process.terminate()
                process.join()
                logger.info(f"Terminated worker process {process.pid}.")
        # Terminate optimizer process
        if self.optimizer_process and self.optimizer_process.is_alive():
            self.optimizer_process.terminate()
            self.optimizer_process.join()
            logger.info(f"Terminated optimizer process {self.optimizer_process.pid}.")
        logger.info("All processes have been terminated.")

    def start_optimizer(self):
        self.optimizer_process = Process(target=self.optimize_process_count)
        self.optimizer_process.start()
        logger.info(f"Optimizer process {self.optimizer_process.pid} started.")

    def optimize_process_count(self):
        previous_speed = 0
        while not self.shutdown_event.is_set():
            time.sleep(self.optimization_interval)
            if self.shutdown_requested:
                logger.info("Shutdown requested. Stopping optimizer.")
                break
            with self.lock:
                downloaded_bytes = self.downloaded_bytes.value
                self.downloaded_bytes.value = 0

            current_speed = downloaded_bytes / self.optimization_interval / (1024 * 1024)  # MB/s
            self.speed_history.append(current_speed)
            avg_speed = mean(self.speed_history) if self.speed_history else current_speed
            logger.info(f"Current speed: {current_speed:.2f} MB/s, Average speed: {avg_speed:.2f} MB/s")

            if current_speed >= self.max_speed:
                logger.info(f"Maximum speed reached: {current_speed:.2f} MB/s")
                continue

            # If no significant speed improvement, stop increasing
            if previous_speed > 0:
                speed_increase = (current_speed - previous_speed) / previous_speed
                if speed_increase < 0.05:  # Less than 5% improvement
                    logger.info("No significant speed improvement detected. Stopping ramp-up.")
                    continue

            # Increase process count aggressively up to max_processes
            with self.process_count.get_lock():
                if self.process_count.value < self.max_processes:
                    new_process_count = self.process_count.value + 5  # Increase by 5
                    if new_process_count > self.max_processes:
                        new_process_count = self.max_processes
                    self.process_count.value = new_process_count
                    logger.info(f"Optimizing process count to {self.process_count.value}")
                    self.adjust_worker_processes()
            previous_speed = current_speed

    def adjust_worker_processes(self):
        current_worker_count = len(self.processes)
        desired_worker_count = self.process_count.value

        if desired_worker_count > current_worker_count:
            # Add more workers
            for _ in range(desired_worker_count - current_worker_count):
                worker = Process(target=self.worker_download)
                worker.start()
                self.processes.append(worker)
                logger.info(f"Added worker process {worker.pid}. Total workers: {len(self.processes)}")
        elif desired_worker_count < current_worker_count:
            # We will not decrease workers as per the requirement
            pass  # Do nothing

    @staticmethod
    def download_object(obj, bucket, destination, s3, prefix=''):
        key = obj['Key']
        # Strip the prefix from the key
        relative_key = key[len(prefix):].lstrip('/')
        dest_path = os.path.join(destination, relative_key)
        os.makedirs(os.path.dirname(dest_path), exist_ok=True)

        should_download = True
        if os.path.exists(dest_path):
            local_size = os.path.getsize(dest_path)
            if local_size == obj['Size']:
                if 'LastModified' in obj:
                    local_mtime = datetime.fromtimestamp(os.path.getmtime(dest_path), tz=timezone.utc)
                    s3_mtime = obj['LastModified'].replace(tzinfo=timezone.utc)
                    
                    if local_mtime >= s3_mtime:
                        logger.info(f"File {key} is up to date, skipping download")
                        should_download = False
                    else:
                        logger.info(f"File {key} is outdated, re-downloading")
                        os.remove(dest_path)
                else:
                    logger.info(f"File {key} exists with correct size, skipping download")
                    should_download = False
            else:
                logger.info(f"File {key} exists but has incorrect size, resuming download")

        if should_download:
            # Configure multipart download
            config = TransferConfig(
                multipart_threshold=8 * 1024 * 1024,  # 8 MB
                max_concurrency=10,
                multipart_chunksize=8 * 1024 * 1024,  # 8 MB
                use_threads=True,
            )

            try:
                if os.path.exists(dest_path):
                    logger.info(f"Overwriting outdated file: {dest_path}")
                    with open(dest_path, 'wb') as f:
                        s3.download_fileobj(bucket, key, f, Config=config)
                    downloaded_size = obj['Size']
                else:
                    # Start new download
                    with open(dest_path, 'wb') as f:
                        s3.download_fileobj(bucket, key, f, Config=config)
                    downloaded_size = obj['Size']

                # Update the local file's timestamp to match S3 if 'LastModified' is available
                if 'LastModified' in obj:
                    os.utime(dest_path, (time.time(), obj['LastModified'].timestamp()))
                else:
                    # If 'LastModified' is not available, use the current time
                    current_time = time.time()
                    os.utime(dest_path, (current_time, current_time))

                logger.info(f"Downloaded: s3://{bucket}/{key} to {dest_path}")
                return downloaded_size
            except ClientError as e:
                logger.error(f"ClientError downloading {key}: {e}")
                raise
            except Exception as e:
                logger.error(f"Unexpected error downloading {key}: {e}")
                raise
        else:
            return 0  # No bytes downloaded

    def graceful_shutdown(self, signum, frame):
        logger.info(f"Received signal {signum}. Initiating graceful shutdown...")
        self.shutdown_requested = True
        self.shutdown_event.set()

def main():
    parser = argparse.ArgumentParser(description="S3 Optimized Downloader")
    parser.add_argument("source", help="Source S3 URI (s3://bucket/prefix)")
    parser.add_argument("destination", help="Destination local path")
    parser.add_argument("--region", default="us-east-1", help="AWS region")
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"],
                        help="Set the logging level")
    parser.add_argument("--endpoint-url", help="Custom S3 endpoint URL")
    parser.add_argument("--include", action='append', help="Pattern to include files (can be used multiple times)")
    parser.add_argument("--exclude", action='append', help="Pattern to exclude files (can be used multiple times)")
    args = parser.parse_args()

    logger.setLevel(args.log_level)

    if not args.source.startswith("s3://"):
        logger.error("Source must be an S3 URI (s3://bucket/prefix)")
        return

    try:
        bucket, prefix = args.source[5:].split("/", 1)
    except ValueError:
        bucket = args.source[5:]
        prefix = ''

    downloader = S3OptimizedDownloader(bucket, prefix, args.destination, args.region, args.endpoint_url,
                                       include_patterns=args.include, exclude_patterns=args.exclude)

    # Register signal handlers for graceful shutdown
    signal.signal(signal.SIGTERM, downloader.graceful_shutdown)
    signal.signal(signal.SIGINT, downloader.graceful_shutdown)

    # Use this instead of directly calling download_all
    try:
        downloader.download_all()
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received. Initiating graceful shutdown...")
        downloader.graceful_shutdown(signal.SIGINT, None)
    finally:
        downloader.terminate_processes()

    # Log the optimization interval
    optimization_interval = os.environ.get('OPTIMIZATION_INTERVAL', '10')
    logger.info(f"Optimization interval set to {optimization_interval} seconds")

if __name__ == "__main__":
    main()
