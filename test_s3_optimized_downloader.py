import os
import pytest
import boto3
import time
from botocore.exceptions import ClientError
from s3_optimized_downloader import S3OptimizedDownloader
import shutil
import logging
import sys
from multiprocessing import Value
import stat

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', stream=sys.stdout)
logger = logging.getLogger(__name__)

# Ensure that the logger is not filtered
logger.setLevel(logging.INFO)

# Add a stream handler to output to stdout
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

# Add a fixture to set optimization environment variables for all tests
@pytest.fixture(scope="module", autouse=True)
def set_optimization_env():
    # Set S3 optimization environment variables
    os.environ['MIN_PROCESSES'] = '2'
    os.environ['MAX_PROCESSES'] = '10'
    os.environ['MAX_SPEED'] = '200'
    os.environ['OPTIMIZATION_INTERVAL'] = '1'
    os.environ['MAX_RETRIES'] = '3'
    os.environ['RETRY_DELAY'] = '1'
    
    # Set AWS-related environment variables
    os.environ['AWS_ACCESS_KEY_ID'] = 'test'
    os.environ['AWS_SECRET_ACCESS_KEY'] = 'test'
    os.environ['AWS_DEFAULT_REGION'] = 'us-east-1'
    os.environ['AWS_ENDPOINT_URL'] = 'http://seaweedfs:8333'
    os.environ['S3_VERIFY_SSL'] = 'false'  # Disable SSL verification for testing
    
    yield
    
    # Clean up environment variables after tests
    env_vars = [
        'MIN_PROCESSES', 'MAX_PROCESSES', 'MAX_SPEED', 'OPTIMIZATION_INTERVAL',
        'MAX_RETRIES', 'RETRY_DELAY', 'AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY',
        'AWS_DEFAULT_REGION', 'AWS_ENDPOINT_URL', 'S3_VERIFY_SSL'
    ]
    for var in env_vars:
        if var in os.environ:
            del os.environ[var]

@pytest.fixture(scope="module")
def s3_client():
    verify_ssl = os.environ.get('S3_VERIFY_SSL', 'true').lower() == 'true'
    return boto3.client('s3',
                        endpoint_url=os.environ['AWS_ENDPOINT_URL'],
                        aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'],
                        aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'],
                        region_name=os.environ['AWS_DEFAULT_REGION'],
                        verify=verify_ssl)

@pytest.fixture(scope="module")
def test_bucket(s3_client):
    bucket_name = 'test-bucket'
    retry_count = 0
    while retry_count < 5:
        try:
            s3_client.create_bucket(Bucket=bucket_name)
            return bucket_name
        except ClientError as e:
            if e.response['Error']['Code'] == 'BucketAlreadyExists':
                return bucket_name
            retry_count += 1
            time.sleep(5)
    pytest.fail("Failed to create test bucket after multiple attempts")

@pytest.fixture(autouse=True)
def clean_destination():
    """
    Fixture to clean the destination directory before and after each test.
    """
    def remove_readonly(func, path, excinfo):
        os.chmod(path, os.stat(path).st_mode | stat.S_IWRITE)
        func(path)

    def rmtree_retry(path, retries=5):
        for attempt in range(retries):
            try:
                shutil.rmtree(path, onerror=remove_readonly)
                break
            except OSError as e:
                if attempt == retries - 1:  # last attempt
                    raise
                logger.warning(f"Failed to remove directory {path}, retrying... (Attempt {attempt + 1}/{retries})")
                time.sleep(1)

    if os.path.exists('/tmp/test_destination'):
        rmtree_retry('/tmp/test_destination')
    os.makedirs('/tmp/test_destination', exist_ok=True)
    yield
    if os.path.exists('/tmp/test_destination'):
        rmtree_retry('/tmp/test_destination')

def test_list_objects(s3_client, test_bucket):
    logger.info("Starting test_list_objects")
    # Upload some test objects
    for i in range(5):
        s3_client.put_object(Bucket=test_bucket, Key=f'test_object_{i}', Body=os.urandom(1024 * 1024))  # 1MB objects

    # Ensure S3_VERIFY_SSL is set before creating the downloader
    os.environ['S3_VERIFY_SSL'] = 'false'
    downloader = S3OptimizedDownloader(test_bucket, '', '/tmp/test_destination', 'us-east-1', 'http://seaweedfs:8333')
    downloader.list_objects()

    assert len(downloader.objects) == 5
    assert downloader.total_size == 5 * 1024 * 1024

def test_download_object(s3_client, test_bucket):
    logger.info("Starting test_download_object")
    test_data = b'Hello, World!'
    s3_client.put_object(Bucket=test_bucket, Key='test_download', Body=test_data)

    downloader = S3OptimizedDownloader(
        test_bucket, '', '/tmp/test_destination', 'us-east-1', 'http://seaweedfs:8333'
    )
    s3 = downloader._create_s3_client()
    obj = {'Key': 'test_download', 'Size': len(test_data)}
    downloaded_size = downloader.download_object(
        obj, downloader.bucket, downloader.destination, s3
    )

    assert downloaded_size == len(test_data)
    with open('/tmp/test_destination/test_download', 'rb') as f:
        assert f.read() == test_data

# Remove the test_distribute_objects function as it's no longer applicable

def test_download_large_file(s3_client, test_bucket):
    logger.info("Starting test_download_large_file")
    large_file_size = 100 * 1024 * 1024  # 100 MB
    s3_client.put_object(Bucket=test_bucket, Key='large_file', Body=os.urandom(large_file_size))

    downloader = S3OptimizedDownloader(test_bucket, '', '/tmp/test_destination', 'us-east-1', 'http://seaweedfs:8333')
    start_time = time.time()
    downloader.download_all()
    end_time = time.time()

    download_time = end_time - start_time
    download_speed = large_file_size / download_time / (1024 * 1024)  # MB/s

    assert os.path.exists('/tmp/test_destination/large_file')
    assert os.path.getsize('/tmp/test_destination/large_file') == large_file_size
    assert download_speed > 1  # Ensure download speed is greater than 1 MB/s

def test_download_many_small_files(s3_client, test_bucket):
    logger.info("Starting test_download_many_small_files")
    num_files = 1000
    file_size = 10 * 1024  # 10 KB

    logger.info(f"Creating {num_files} small files in S3 bucket")
    for i in range(num_files):
        key = f'small_file_{i}'
        s3_client.put_object(Bucket=test_bucket, Key=key, Body=os.urandom(file_size))
        logger.info(f"Created file: {key}")

    downloader = S3OptimizedDownloader(test_bucket, '', '/tmp/test_destination', 'us-east-1', 'http://seaweedfs:8333')
    
    # Start the download in a separate thread to allow optimization
    import threading
    download_thread = threading.Thread(target=downloader.download_all)
    download_thread.start()

    # Wait for a short period to allow the optimizer to adjust process_count
    time.sleep(downloader.optimization_interval * 3)  # Wait for 3 optimization intervals

    # Access the shared process_count
    with downloader.process_count.get_lock():
        current_process_count = downloader.process_count.value

    logger.info(f"Current process count during download: {current_process_count}")

    # Assert that the process count has increased from the minimum
    assert current_process_count > downloader.min_processes, (
        f"Process count did not increase during download. Current count: {current_process_count}"
    )

    # Wait for the download to complete
    download_thread.join()

    download_files = [f for f in os.listdir('/tmp/test_destination') if f.startswith('small_file_')]
    logger.info(f"Number of files in destination: {len(download_files)}")

    # List files in S3 bucket for comparison
    s3_files = []
    paginator = s3_client.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=test_bucket):
        for obj in page.get('Contents', []):
            if obj['Key'].startswith('small_file_'):
                s3_files.append(obj['Key'])

    logger.info(f"Number of files in S3 bucket: {len(s3_files)}")

    assert len(download_files) == num_files, f"Expected {num_files} files, but found {len(download_files)}"
    assert set(download_files) == set(s3_files), "Mismatch between S3 files and downloaded files"

def test_process_count_optimization(s3_client, test_bucket):
    logger.info("Starting test_process_count_optimization")
    file_size = 10 * 1024 * 1024  # 10 MB
    num_files = 100

    for i in range(num_files):
        s3_client.put_object(Bucket=test_bucket, Key=f'opt_file_{i}', Body=os.urandom(file_size))

    downloader = S3OptimizedDownloader(test_bucket, '', '/tmp/test_destination', 'us-east-1', 'http://seaweedfs:8333')
    downloader.optimization_interval = 1
    downloader.min_processes = 1
    downloader.max_processes = 16
    downloader.process_count = Value('i', downloader.min_processes)
    downloader.max_speed = 100000000000000000000
    
    # Start the download in a separate thread to allow for optimization
    import threading
    download_thread = threading.Thread(target=downloader.download_all)
    download_thread.start()

    # Wait for optimization to occur
    total_wait_time = downloader.optimization_interval * 5
    logger.info(f"Waiting for {total_wait_time} seconds for optimization to occur")
    time.sleep(total_wait_time)

    # Access the shared process_count
    with downloader.process_count.get_lock():
        current_process_count = downloader.process_count.value

    logger.info(f"Current process count: {current_process_count}")

    # Stop the download
    downloader.shutdown_event.set()
    download_thread.join()

    assert current_process_count > 1, f"Process count did not increase. Current count: {current_process_count}"

def test_resume_interrupted_download(s3_client, test_bucket):
    logger.info("Starting test_resume_interrupted_download")
    file_size = 20 * 1024 * 1024  # 20 MB
    s3_client.put_object(Bucket=test_bucket, Key='resume_file', Body=os.urandom(file_size))

    downloader = S3OptimizedDownloader(test_bucket, '', '/tmp/test_destination', 'us-east-1', 'http://seaweedfs:8333')
    
    # Simulate partial download
    os.makedirs('/tmp/test_destination', exist_ok=True)
    with open('/tmp/test_destination/resume_file', 'wb') as f:
        f.write(os.urandom(10 * 1024 * 1024))  # Write 10 MB

    # Start the download in a separate thread
    import threading
    download_thread = threading.Thread(target=downloader.download_all)
    download_thread.start()

    # Wait for the download to complete or timeout after 30 seconds
    download_thread.join(timeout=30)

    # If the thread is still alive, terminate the download
    if download_thread.is_alive():
        logger.warning("Download did not complete within the timeout. Terminating...")
        downloader.shutdown_event.set()
        downloader.terminate_processes()
        download_thread.join()

    assert os.path.getsize('/tmp/test_destination/resume_file') == file_size

    # Ensure all processes are terminated
    downloader.terminate_processes()

    # Wait for a moment to allow for process termination
    time.sleep(1)

    logger.info("Test completed, cleaning up...")

# Add more tests as needed
