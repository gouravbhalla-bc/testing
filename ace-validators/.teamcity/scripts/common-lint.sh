#!/bin/bash

# exit when any command fails
set -e

eval scripts/teamcity/lint.sh
