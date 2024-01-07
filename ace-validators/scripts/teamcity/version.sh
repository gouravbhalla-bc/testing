#!/bin/bash

# exit when any command fails
set -e

source env/bin/activate
python setup.py --version
