#!/bin/bash

docker container prune -f
docker rmi -f $(docker images --filter=reference="$DOCKER_IMAGE_NAME" -q)  2>/dev/null
docker rmi -f $(docker images --filter=reference="*/$DOCKER_IMAGE_NAME" -q)  2>/dev/null
exit 0
