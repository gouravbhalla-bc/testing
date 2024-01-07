#!/bin/bash

# exit when any command fails
set -e

branch=${GITHUB_REF##*/}

if [ ${branch} == 'master' ] ; then
  ENVIRONMENT='prod'
  DOCKER_IMAGE_TAG='latest'
elif [ ${branch}  == 'test_master' ] ; then
  ENVIRONMENT='test'
  DOCKER_IMAGE_TAG='test'
elif [ ${branch}  == 'github-actions' ] ; then
  ENVIRONMENT='test'
  DOCKER_IMAGE_TAG='test'
else
    echo Unsupported branch: ${branch}
    exit 1
fi

# set version
VERSION=`awk '/\[tool.poetry\]/,/^$/' pyproject.toml | grep version | awk -F'"' '{print $2}'`

echo ${VERSION}
