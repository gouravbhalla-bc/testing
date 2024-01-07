#!/bin/bash

# exit when any command fails
set -e

# set vars
HELM_CHART_VERSION=$(helm show chart $HELM_CHART_PATH | grep 'version:' | cut -d ' ' -f 2)
HELM_CHART_NAME=$(helm show chart $HELM_CHART_PATH | grep 'name:' | cut -d ' ' -f 2)

# package chart
helm package $HELM_CHART_PATH --version "$HELM_CHART_VERSION$HELM_CHART_VERSION_TAG" --debug

# upload chart
curl --data-binary "@$HELM_CHART_NAME-$HELM_CHART_VERSION$HELM_CHART_VERSION_TAG.tgz" $ALT_REPO_HELM -vk
