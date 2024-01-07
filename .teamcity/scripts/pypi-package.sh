#!/bin/bash

# exit when any command fails
set -e

# activate venv
source env/bin/activate

# package
python -m pip install --upgrade pip setuptools wheel
python setup.py sdist bdist_wheel

# upload
scp dist/* "$ALT_REPO_PYPI_USER@$ALT_REPO_PYPI:packages"
