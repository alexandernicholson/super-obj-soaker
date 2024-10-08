# Super Obj Soaker

**Self-optimising Multi-process S3-Compatible Object Storage Downloader**

## Table of Contents

- [Introduction](#introduction)
- [Features](#features)
- [Architecture](#architecture)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
  - [Virtual Environment Setup](#virtual-environment-setup)
  - [Docker Setup](#docker-setup)
- [Configuration](#configuration)
- [Usage](#usage)
- [Testing](#testing)
- [Project Structure](#project-structure)
- [Contributing](#contributing)
- [License](#license)

## Introduction

Super Obj Soaker is an advanced S3 downloader that significantly improves upon the default S3 client. It uses multiple Python processes for concurrent downloads, bypassing the Global Interpreter Lock (GIL) to fully utilize CPU cores and network bandwidth. The tool dynamically self-optimizes by adjusting the number of worker processes based on performance. It supports resumable downloads, configurable speed limits, and works with any S3-compatible storage system. With robust error handling, retry mechanisms, and real-time progress monitoring, Super Obj Soaker offers significantly faster and more reliable downloads for large datasets compared to basic sequential downloads which is what the default S3 client uses.

## Features

- **Self-Optimising**: Dynamically adjusts the number of worker processes based on download performance to maximize efficiency.
- **Multi-process**: Utilizes multiple processes to handle concurrent downloads, reducing overall download time and bypassing Python's Global Interpreter Lock (GIL).
- **Resumable Downloads**: Supports resuming interrupted downloads, ensuring data integrity and reliability.
- **Configurable Parameters**: Easily adjust settings such as minimum and maximum processes, download speed limits, and optimization intervals.
- **S3-Compatible**: Works with any S3-compatible storage system, including local implementations for testing.
- **Comprehensive Error Handling**: Implements retry mechanisms and graceful shutdowns for improved reliability.
- **Include/Exclude Patterns**: Supports filtering of files to download based on include and exclude patterns, similar to the AWS CLI S3 sync command.

## Architecture

For an in-depth understanding of the system's architecture, refer to the [ARCHITECTURE.md](ARCHITECTURE.md) document.

## Prerequisites

- **Python 3.9+**: The project is built using Python 3.9 or later. [Download Python](https://www.python.org/downloads/)
- **Git**: To clone the repository. [Install Git](https://git-scm.com/downloads)
- **Docker and Docker Compose** (optional): For running the application in containers. [Install Docker](https://docs.docker.com/get-docker/)

## Installation

You can set up Super Obj Soaker using either a virtual environment or Docker.

### Virtual Environment Setup

1. **Clone the Repository**

   ```bash
   git clone https://github.com/alexandernicholson/super-obj-soaker.git
   cd super-obj-soaker
   ```

2. **Set Up a Virtual Environment using uv**

   First, install uv if you haven't already:

   ```bash
   pip install uv
   ```

   Then, create and activate the virtual environment:

   ```bash
   uv venv
   source .venv/bin/activate  # On Windows use `.venv\Scripts\activate`
   ```

3. **Install Dependencies**

   ```bash
   uv pip install -r requirements.txt
   ```

### Docker Setup

1. **Clone the Repository**

   ```bash
   git clone https://github.com/alexandernicholson/super-obj-soaker.git
   cd super-obj-soaker
   ```

2. **Build and Run with Docker Compose**

   ```bash
   docker-compose up --build
   ```

   This will set up the entire environment, including SeaweedFS for local testing.

## Configuration

Configuration is managed through environment variables, allowing you to customize the downloader's behavior.

### Environment Variables

- `MIN_PROCESSES`: Minimum number of worker processes. *(Default: 1)*
- `MAX_PROCESSES`: Maximum number of worker processes. *(Default: 16)*
- `MAX_SPEED`: Maximum download speed in MB/s. *(Default: 99999999999999999999)*
- `OPTIMIZATION_INTERVAL`: Time interval in seconds for optimization checks. *(Default: 10)*
- `MAX_RETRIES`: Maximum number of retries for failed downloads. *(Default: 3)*
- `RETRY_DELAY`: Delay in seconds between retries. *(Default: 5)*

These can be set in your shell environment or passed directly when running the script.

## Usage

The downloader can be executed via the command line with customizable parameters.

### Command-Line Arguments

- `source`: Source S3 URI (e.g., `s3://bucket/prefix`)
- `destination`: Destination local path
- `--region`: AWS region *(Default: us-east-1)*
- `--log-level`: Set the logging level (`DEBUG`, `INFO`, `WARNING`, `ERROR`) *(Default: INFO)*
- `--endpoint-url`: Custom S3 endpoint URL
- `--include`: Pattern to include files (can be used multiple times)
- `--exclude`: Pattern to exclude files (can be used multiple times)

### Examples

1. **Basic Download**

   ```bash
   python s3_optimized_downloader.py s3://mybucket/data /local/path
   ```

2. **With Custom Region and Log Level**

   ```bash
   python s3_optimized_downloader.py s3://mybucket/data /local/path --region us-west-2 --log-level DEBUG
   ```

3. **Using a Custom S3 Endpoint**

   ```bash
   python s3_optimized_downloader.py s3://mybucket/data /local/path --endpoint-url http://localhost:8333
   ```

4. **Using Include and Exclude Patterns**

   ```bash
   python s3_optimized_downloader.py s3://mybucket/data /local/path --exclude "*" --include "*.db"
   ```

   This example will download only files with the `.db` extension, similar to the AWS CLI command:
   ```
   aws s3 sync s3://mybucket/data . --exclude "*" --include "*.db"
   ```

## Testing

The project includes a test suite to ensure reliability and functionality.

### Running Tests

For the virtual environment setup:

```bash
pytest -v
```

For the Docker setup:

```bash
./run_tests.sh
```

This script will set up the Docker environment, run the tests, and tear down the containers afterward.

## Project Structure

```
super-obj-soaker/
│
├── docker-compose.yaml
├── Dockerfile
├── .gitignore
├── requirements.txt
├── run_tests.sh
├── README.md
├── ARCHITECTURE.md
│
├── s3_optimized_downloader.py
└── test_s3_optimized_downloader.py
```

## Contributing

Contributions are welcome! Please follow these steps to contribute:

1. Fork the repository
2. Create a new branch (`git checkout -b feature/your-feature-name`)
3. Make your changes and commit them (`git commit -am 'Add some feature'`)
4. Push to the branch (`git push origin feature/your-feature-name`)
5. Create a new Pull Request

## License

This project is licensed under the [MIT License](LICENSE). You are free to use, modify, and distribute this software as per the terms of the license.
