#!/bin/bash

# exit when any command fails
set -e

# activate venv
source ~/.bashrc
#pyenv local 3.7.9
pip install -U virtualenv
virtualenv env && source env/bin/activate

# install dependencies
pip install \
    -r requirements.txt \
    --extra-index-url "https://$ALT_REPO_PYPI" \
    --trusted-host $ALT_REPO_PYPI
