#!/bin/bash

# exit when any command fails
set -e

# activate venv
source env/bin/activate

eval scripts/teamcity/lint.sh
