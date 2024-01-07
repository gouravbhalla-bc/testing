#!/bin/bash

# exit when any command fails
set -e

source ~/.bashrc
cd docs
make html
cd _build
scp -r html/* "$ALT_REPO_DOCS_USER@$ALT_REPO_DOCS:$ALT_REPO_DOCS_DIR/$PACKAGE_NAME"
