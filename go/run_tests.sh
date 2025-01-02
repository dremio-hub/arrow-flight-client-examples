#!/bin/bash

set -e

# Define variables
MOCKGEN_CMD="mockgen -destination=mock_flightclient.go -package=main github.com/apache/arrow-go/v18/arrow/flight Client"
TEST_CMD="go test ./... -v"

# Generate the mock
echo "Generating mock flight client class..."
if $MOCKGEN_CMD; then
echo "Mock Flight client class generated successfully."
else
echo "Failed to generate mock class. Exiting."
exit 1
fi

# Run the tests
echo "Running tests..."
if $TEST_CMD; then
echo "Tests ran successfully."
else
echo "Tests failed. Exiting."
exit 1
fi