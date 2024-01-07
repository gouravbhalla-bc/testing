#!/bin/bash

# exit when any command fails
set -e

if [ -x "scripts/teamcity/deploy/$ALT_CLUSTER/$ENVIRONMENT.sh" ]; then
    eval "scripts/teamcity/deploy/$ALT_CLUSTER/$ENVIRONMENT.sh"
else
    echo "No deployment script for $ALT_CLUSTER in $ENVIRONMENT"
fi
