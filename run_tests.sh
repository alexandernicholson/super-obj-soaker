#!/bin/bash

# Function to tear down Docker containers
tear_down() {
    echo "Tearing down Docker containers..."
    docker-compose down -v
}

# Tear down existing containers before starting
tear_down

# Start the Docker containers
echo "Starting Docker containers..."
docker-compose up -d

# Function to check if SeaweedFS is ready
check_seaweedfs() {
    local max_attempts=60
    local attempt=1

    echo "Waiting for SeaweedFS to be ready..."
    while true; do
        if $(curl -s -o /dev/null -w "%{http_code}" http://localhost:8333 | grep -q 200); then
            echo "SeaweedFS is ready!"
            return 0
        fi

        if [ $attempt -eq $max_attempts ]; then
            echo "SeaweedFS is not ready after $max_attempts attempts. Exiting."
            return 1
        fi

        printf '.'
        sleep 2
        ((attempt++))
    done
}

# Check if SeaweedFS is ready
if ! check_seaweedfs; then
    tear_down
    exit 1
fi

# Run the tests
echo "Running tests..."
docker-compose run --build --rm test-runner pytest -v -s --log-cli-level=INFO -x

# Capture the exit code of the test run
TEST_EXIT_CODE=$?

# Tear down the Docker containers
tear_down

echo "Tests completed with exit code: $TEST_EXIT_CODE"

# Exit with the test exit code
exit $TEST_EXIT_CODE
