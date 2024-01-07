#!/bin/bash

# exit when any command fails
set -e

source ~/.bashrc
nvm use 14.2.0
npm run docs
scp -r docs/* "$ALT_REPO_DOCS_USER@$ALT_REPO_DOCS:$ALT_REPO_DOCS_DIR/$PACKAGE_NAME"
