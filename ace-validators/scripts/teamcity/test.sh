#!/bin/bash

# exit when any command fails
set -e

#echo "No Test"

echo "Test Started on Docker Container"

./scripts/test_in_docker.sh
