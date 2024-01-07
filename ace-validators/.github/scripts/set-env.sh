#!/bin/bash

# exit when any command fails
set -e
branch=$1
echo Branch name is: ${branch}
if [ ${branch} == 'master' ] ; then
  ENVIRONMENT='prod'
  HELM_CHART_VERSION_TAG=''
  DOCKER_IMAGE_TAG='latest'
elif [ ${branch}  == 'test_master' ] ; then
  ENVIRONMENT='test'
  HELM_CHART_VERSION_TAG='-test'
  DOCKER_IMAGE_TAG='test'
elif [ ${branch}  == 'github-actions' ] ; then
  ENVIRONMENT='test'
  HELM_CHART_VERSION_TAG='-test'
  DOCKER_IMAGE_TAG='test'
else
    echo Unsupported branch: ${branch}
    exit 1
fi

echo "##teamcity[setParameter name='env.ENVIRONMENT' value='$ENVIRONMENT']"
echo "##teamcity[setParameter name='env.HELM_CHART_VERSION_TAG' value='$HELM_CHART_VERSION_TAG']"
echo "##teamcity[setParameter name='env.DOCKER_IMAGE_TAG' value='$DOCKER_IMAGE_TAG']"
