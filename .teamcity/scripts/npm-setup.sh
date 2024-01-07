#!/bin/bash

# exit when any command fails
set -e

source ~/.bashrc
nvm install 14.2.0 && nvm use 14.2.0

# replace publishConfig.registry
sed -i "s/\"registry\": \".*\"/\"registry\": \"https:\/\/$ALT_REPO_NPM\"/g" package.json

if [ -x "package-lock.json" ]; then
    npm ci --verbose --registry "https://$ALT_REPO_NPM/"
else
    npm install --verbose --registry "https://$ALT_REPO_NPM/"
fi

npm audit fix --verbose
