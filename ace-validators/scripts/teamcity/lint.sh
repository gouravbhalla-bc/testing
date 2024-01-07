#!/bin/bash

# exit when any command fails
set -e

source env/bin/activate
./scripts/code_quality.sh
