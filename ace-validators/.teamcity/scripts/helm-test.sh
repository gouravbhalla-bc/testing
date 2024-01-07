#!/bin/bash

# exit when any command fails
set -e

helm lint $HELM_CHART_PATH --debug
