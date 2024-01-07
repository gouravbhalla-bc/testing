#!/bin/bash

# exit when any command fails
set -e

echo Branch name is: '%teamcity.build.branch%'
if [ '%teamcity.build.branch%' == 'master' ] || [ '%teamcity.build.branch%' == 'refs/heads/master' ]; then
  ENVIRONMENT='prod'
  HELM_CHART_VERSION_TAG=''
  DOCKER_IMAGE_TAG='latest'
elif [ '%teamcity.build.branch%' == 'test_master' ] || [ '%teamcity.build.branch%' == 'refs/heads/test_master' ]; then
  ENVIRONMENT='test'
  HELM_CHART_VERSION_TAG='-test'
  DOCKER_IMAGE_TAG='test'
else
    echo Unsupported branch: '%teamcity.build.branch%'
    exit 1
fi

echo "##teamcity[setParameter name='env.ENVIRONMENT' value='$ENVIRONMENT']"
echo "##teamcity[setParameter name='env.HELM_CHART_VERSION_TAG' value='$HELM_CHART_VERSION_TAG']"
echo "##teamcity[setParameter name='env.DOCKER_IMAGE_TAG' value='$DOCKER_IMAGE_TAG']"
