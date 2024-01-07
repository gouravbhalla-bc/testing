#!/bin/bash

# exit when any command fails
set -e

source ~/.bashrc
npm run build
npm publish
