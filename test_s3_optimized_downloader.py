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
import fnmatch
import uuid
from datetime import datetime, timezone

# Set up logging
logging.basicConfig(level=logging.WARNING, format='%(asctime)s - %(levelname)s - %(message)s', stream=sys.stdout)
logger = logging.getLogger(__name__)

# Ensure that the logger is not filtered
logger.setLevel(logging.WARNING)

# Add a stream handler to output to stdout
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.WARNING)
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

@pytest.fixture
def unique_prefix():
    return f"test_{uuid.uuid4().hex}/"

def test_list_objects(s3_client, test_bucket, unique_prefix):
    logger.info("Starting test_list_objects")
    # Upload some test objects
    for i in range(5):
        s3_client.put_object(Bucket=test_bucket, Key=f'{unique_prefix}test_object_{i}', Body=os.urandom(1024 * 1024))  # 1MB objects

    # Ensure S3_VERIFY_SSL is set before creating the downloader
    os.environ['S3_VERIFY_SSL'] = 'false'
    downloader = S3OptimizedDownloader(test_bucket, unique_prefix, '/tmp/test_destination', 'us-east-1', 'http://seaweedfs:8333')
    downloader.list_objects()

    assert len(downloader.objects) == 5
    assert downloader.total_size == 5 * 1024 * 1024

def test_download_object(s3_client, test_bucket, unique_prefix):
    logger.info("Starting test_download_object")
    test_data = b'Hello, World!'
    s3_client.put_object(Bucket=test_bucket, Key=f'{unique_prefix}test_download', Body=test_data)

    downloader = S3OptimizedDownloader(
        test_bucket, unique_prefix, '/tmp/test_destination', 'us-east-1', 'http://seaweedfs:8333'
    )
    s3 = downloader._create_s3_client()
    obj = {'Key': f'{unique_prefix}test_download', 'Size': len(test_data)}
    downloaded_size = downloader.download_object(
        obj, downloader.bucket, downloader.destination, s3, downloader.prefix
    )

    assert downloaded_size == len(test_data)
    
    expected_file_path = os.path.join('/tmp/test_destination', 'test_download')
    assert os.path.exists(expected_file_path), f"Downloaded file not found at {expected_file_path}"
    
    with open(expected_file_path, 'rb') as f:
        assert f.read() == test_data

def test_download_large_file(s3_client, test_bucket, unique_prefix):
    logger.info("Starting test_download_large_file")
    large_file_size = 100 * 1024 * 1024  # 100 MB
    s3_client.put_object(Bucket=test_bucket, Key=f'{unique_prefix}large_file', Body=os.urandom(large_file_size))

    downloader = S3OptimizedDownloader(test_bucket, unique_prefix, '/tmp/test_destination', 'us-east-1', 'http://seaweedfs:8333')
    start_time = time.time()
    downloader.download_all()
    end_time = time.time()

    download_time = end_time - start_time
    download_speed = large_file_size / download_time / (1024 * 1024)  # MB/s

    expected_file_path = os.path.join('/tmp/test_destination', 'large_file')
    assert os.path.exists(expected_file_path), f"File not found at {expected_file_path}"
    assert os.path.getsize(expected_file_path) == large_file_size, f"File size mismatch. Expected {large_file_size}, got {os.path.getsize(expected_file_path)}"
    assert download_speed > 1, f"Download speed too low: {download_speed:.2f} MB/s"

    logger.info(f"Large file downloaded successfully. Speed: {download_speed:.2f} MB/s")

def test_download_many_small_files(s3_client, test_bucket, unique_prefix):
    logger.info("Starting test_download_many_small_files")
    num_files = 1000
    file_size = 10 * 1024  # 10 KB

    # Create a unique destination directory for this test
    test_destination = f'/tmp/test_destination_{uuid.uuid4().hex}'

    logger.info(f"Creating {num_files} small files in S3 bucket")
    for i in range(num_files):
        key = f'{unique_prefix}small_file_{i}'
        s3_client.put_object(Bucket=test_bucket, Key=key, Body=os.urandom(file_size))
        logger.info(f"Created file: {key}")

    downloader = S3OptimizedDownloader(test_bucket, unique_prefix, test_destination, 'us-east-1', 'http://seaweedfs:8333')

    try:
        # Start the download in a separate thread to allow optimization
        import threading
        download_thread = threading.Thread(target=downloader.download_all)
        download_thread.start()

        # Wait for optimization to occur (still using sleep here as it's not related to completion)
        time.sleep(downloader.optimization_interval * 15)

        # Access the shared process_count
        with downloader.process_count.get_lock():
            current_process_count = downloader.process_count.value

        logger.info(f"Current process count during download: {current_process_count}")

        # Assert that the process count has increased from the minimum
        assert current_process_count > downloader.min_processes, (
            f"Process count did not increase during download. Current count: {current_process_count}"
        )

        # Wait for the download to complete or timeout after 60 seconds
        download_complete = downloader.download_complete.wait(timeout=60)

        if not download_complete:
            logger.warning("Download did not complete within the timeout.")
        
        download_files = []
        for root, dirs, files in os.walk(test_destination):
            for file in files:
                download_files.append(os.path.relpath(os.path.join(root, file), test_destination))
        logger.info(f"Number of files in destination: {len(download_files)}")

        # List files in S3 bucket for comparison
        s3_files = []
        paginator = s3_client.get_paginator('list_objects_v2')
        for page in paginator.paginate(Bucket=test_bucket, Prefix=unique_prefix):
            for obj in page.get('Contents', []):
                s3_files.append(os.path.relpath(obj['Key'], unique_prefix))

        logger.info(f"Number of files in S3 bucket: {len(s3_files)}")

        assert len(download_files) == num_files, f"Expected {num_files} files, but found {len(download_files)}"
        assert set(download_files) == set(s3_files), "Mismatch between S3 files and downloaded files"

    finally:
        # Ensure shutdown and cleanup
        downloader.shutdown_event.set()
        downloader.terminate_processes()
        if download_thread.is_alive():
            download_thread.join(timeout=5)
        logger.info("Test completed, all processes terminated.")

        # Clean up the test directory
        shutil.rmtree(test_destination, ignore_errors=True)

def test_process_count_optimization(s3_client, test_bucket, unique_prefix):
    logger.info("Starting test_process_count_optimization")
    file_size = 10 * 1024 * 1024  # 10 MB
    num_files = 100

    for i in range(num_files):
        s3_client.put_object(Bucket=test_bucket, Key=f'{unique_prefix}opt_file_{i}', Body=os.urandom(file_size))

    downloader = S3OptimizedDownloader(test_bucket, unique_prefix, '/tmp/test_destination', 'us-east-1', 'http://seaweedfs:8333')
    downloader.optimization_interval = 1
    downloader.min_processes = 1
    downloader.max_processes = 16
    downloader.process_count = Value('i', downloader.min_processes)
    downloader.max_speed = 100000000000000000000
    
    try:
        # Start the download in a separate thread to allow for optimization
        import threading
        download_thread = threading.Thread(target=downloader.download_all)
        download_thread.start()

        # Wait for optimization to occur
        time.sleep(downloader.optimization_interval * 5)

        # Access the shared process_count
        with downloader.process_count.get_lock():
            current_process_count = downloader.process_count.value

        logger.info(f"Current process count: {current_process_count}")

        assert current_process_count > 1, f"Process count did not increase. Current count: {current_process_count}"

        # Wait for the download to complete or timeout after 60 seconds
        download_complete = downloader.download_complete.wait(timeout=60)

        if not download_complete:
            logger.warning("Download did not complete within the timeout.")

    finally:
        # Ensure shutdown and cleanup
        downloader.shutdown_event.set()
        downloader.terminate_processes()
        if download_thread.is_alive():
            download_thread.join(timeout=5)
        logger.info("Test completed, all processes terminated.")

def test_resume_interrupted_download(s3_client, test_bucket, unique_prefix):
    logger.info("Starting test_resume_interrupted_download")
    file_size = 20 * 1024 * 1024  # 20 MB
    s3_client.put_object(Bucket=test_bucket, Key=f'{unique_prefix}resume_file', Body=os.urandom(file_size))

    downloader = S3OptimizedDownloader(test_bucket, unique_prefix, '/tmp/test_destination', 'us-east-1', 'http://seaweedfs:8333')
    
    # Simulate partial download
    partial_file_path = os.path.join('/tmp/test_destination', 'resume_file')
    os.makedirs(os.path.dirname(partial_file_path), exist_ok=True)
    with open(partial_file_path, 'wb') as f:
        f.write(os.urandom(10 * 1024 * 1024))  # Write 10 MB

    # Start the download
    downloader.download_all()

    expected_file_path = os.path.join('/tmp/test_destination', 'resume_file')
    assert os.path.exists(expected_file_path), f"File not found at {expected_file_path}"
    assert os.path.getsize(expected_file_path) == file_size, f"File size mismatch. Expected {file_size}, got {os.path.getsize(expected_file_path)}"

    # Ensure all processes are terminated
    downloader.terminate_processes()

    # Wait for a moment to allow for process termination
    time.sleep(1)

    logger.info("Test completed, cleaning up...")

def test_include_exclude_patterns(s3_client, test_bucket, unique_prefix):
    logger.info("Starting test_include_exclude_patterns")
    
    # Create test files
    files = [
        'file1.txt', 'file2.txt', 'file3.csv',
        'data1.json', 'data2.json',
        'image1.png', 'image2.jpg',
        'temp_file1.tmp', 'temp_file2.tmp'
    ]
    
    for file in files:
        s3_client.put_object(Bucket=test_bucket, Key=f'{unique_prefix}{file}', Body=b'test content')

    # Test cases
    test_cases = [
        {
            'include': ['*.txt'],
            'exclude': None,
            'expected': ['file1.txt', 'file2.txt']
        },
        {
            'include': ['*.json', '*.csv'],
            'exclude': None,
            'expected': ['file3.csv', 'data1.json', 'data2.json']
        },
        {
            'include': None,
            'exclude': ['*.tmp'],
            'expected': ['file1.txt', 'file2.txt', 'file3.csv', 'data1.json', 'data2.json', 'image1.png', 'image2.jpg']
        },
        {
            'include': ['*'],
            'exclude': ['*.tmp', '*.png'],
            'expected': ['file1.txt', 'file2.txt', 'file3.csv', 'data1.json', 'data2.json', 'image2.jpg']
        }
    ]

    for i, case in enumerate(test_cases):
        logger.info(f"Running test case {i + 1}")
        case_destination = f'/tmp/test_destination/case{i}'
        downloader = S3OptimizedDownloader(
            test_bucket, unique_prefix, case_destination, 'us-east-1', 'http://seaweedfs:8333',
            include_patterns=case['include'], exclude_patterns=case['exclude']
        )
        downloader.download_all()

        downloaded_files = []
        for root, _, files in os.walk(case_destination):
            for file in files:
                rel_path = os.path.relpath(os.path.join(root, file), case_destination)
                downloaded_files.append(rel_path.replace('\\', '/'))  # Normalize path separators

        assert set(downloaded_files) == set(case['expected']), \
            f"Case {i + 1}: Expected {case['expected']}, but got {downloaded_files}"

def test_complex_include_exclude_patterns(s3_client, test_bucket, unique_prefix):
    logger.info("Starting test_complex_include_exclude_patterns")
    
    # Create a more complex file structure
    files = [
        'docs/report1.pdf', 'docs/report2.pdf', 'docs/draft.txt',
        'images/photo1.jpg', 'images/photo2.png', 'images/icon.svg',
        'data/users/user1.json', 'data/users/user2.json',
        'data/logs/log1.txt', 'data/logs/log2.txt',
        'temp/temp1.tmp', 'temp/temp2.tmp'
    ]
    
    for file in files:
        s3_client.put_object(Bucket=test_bucket, Key=f'{unique_prefix}{file}', Body=b'test content')

    # Test case with complex patterns
    include_patterns = ['docs/*.pdf', 'images/*.jpg', 'data/**/*.json']
    exclude_patterns = ['**/draft*', 'temp/*']

    expected_files = [
        'docs/report1.pdf', 'docs/report2.pdf',
        'images/photo1.jpg',
        'data/users/user1.json', 'data/users/user2.json'
    ]

    downloader = S3OptimizedDownloader(
        test_bucket, unique_prefix, '/tmp/test_destination/complex', 'us-east-1', 'http://seaweedfs:8333',
        include_patterns=include_patterns, exclude_patterns=exclude_patterns
    )
    downloader.download_all()

    # Check if the correct files were downloaded
    downloaded_files = []
    for root, _, files in os.walk(os.path.join('/tmp/test_destination/complex')):
        for file in files:
            rel_path = os.path.relpath(os.path.join(root, file), os.path.join('/tmp/test_destination/complex'))
            downloaded_files.append(rel_path.replace('\\', '/'))  # Normalize path separators

    assert set(downloaded_files) == set(expected_files), \
        f"Expected {expected_files}, but got {downloaded_files}"

def test_timestamp_comparison(s3_client, test_bucket, unique_prefix):
    logger.info("Starting test_timestamp_comparison")
    file_content = b'test content'
    file_key = f'{unique_prefix}timestamp_test_file.txt'

    # Upload a file
    s3_client.put_object(Bucket=test_bucket, Key=file_key, Body=file_content)

    def download_with_timeout(downloader, timeout=30):
        import threading
        download_thread = threading.Thread(target=downloader.download_all)
        download_thread.start()
        download_thread.join(timeout)
        if download_thread.is_alive():
            logger.error("Download operation timed out")
            raise TimeoutError("Download operation timed out")

    # First download
    downloader = S3OptimizedDownloader(test_bucket, unique_prefix, '/tmp/test_destination', 'us-east-1', 'http://seaweedfs:8333')
    download_with_timeout(downloader)

    # Get the local file's modification time
    local_file_path = os.path.join('/tmp/test_destination', 'timestamp_test_file.txt')
    initial_mtime = os.path.getmtime(local_file_path)

    # Wait a moment to ensure different timestamps
    time.sleep(1)

    # Second download without modifying the S3 object
    downloader = S3OptimizedDownloader(test_bucket, unique_prefix, '/tmp/test_destination', 'us-east-1', 'http://seaweedfs:8333')
    download_with_timeout(downloader)

    # Check that the local file wasn't updated
    assert os.path.getmtime(local_file_path) == initial_mtime, "File was unnecessarily downloaded"

    # Modify the S3 object
    s3_client.put_object(Bucket=test_bucket, Key=file_key, Body=b'updated content')

    # Wait a moment to ensure different timestamps
    time.sleep(1)

    # Third download after modifying the S3 object
    downloader = S3OptimizedDownloader(test_bucket, unique_prefix, '/tmp/test_destination', 'us-east-1', 'http://seaweedfs:8333')
    download_with_timeout(downloader)

    # Check that the local file was updated
    assert os.path.getmtime(local_file_path) > initial_mtime, "File was not updated when S3 object changed"

    # Verify the content was updated
    with open(local_file_path, 'rb') as f:
        assert f.read() == b'updated content', "File content was not updated"

    # Try to download again without modifying the S3 object
    initial_mtime = os.path.getmtime(local_file_path)
    downloader = S3OptimizedDownloader(test_bucket, unique_prefix, '/tmp/test_destination', 'us-east-1', 'http://seaweedfs:8333')
    download_with_timeout(downloader)

    # Check that the local file wasn't updated
    assert os.path.getmtime(local_file_path) == initial_mtime, "File was unnecessarily downloaded after update"
