#!/bin/bash

# exit when any command fails
set -e

# set vars
VERSION=$(scripts/teamcity/version.sh)

# build image
docker build \
    -t $DOCKER_IMAGE_NAME \
    -f $DOCKER_FILE_PATH \
    --build-arg ALT_REPO_PYPI \
    --build-arg ALT_REPO_NPM \
    .

# tag images
docker tag $DOCKER_IMAGE_NAME "$ALT_REPO_ECR/$DOCKER_IMAGE_NAME:$VERSION"
docker tag $DOCKER_IMAGE_NAME "$ALT_REPO_ECR/$DOCKER_IMAGE_NAME:$DOCKER_IMAGE_TAG"

# upload image
aws ecr get-login-password --region ap-southeast-1 | docker login --username AWS --password-stdin $ALT_REPO_ECR
docker push "$ALT_REPO_ECR/$DOCKER_IMAGE_NAME:$VERSION"
docker push "$ALT_REPO_ECR/$DOCKER_IMAGE_NAME:$DOCKER_IMAGE_TAG"
