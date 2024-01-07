#!/bin/bash

# exit when any command fails
set -e

# set vars
VERSION=`cat altonomy/ace/__init__.py |  awk -F'"' '{print $2}'`

echo ${VERSION}
