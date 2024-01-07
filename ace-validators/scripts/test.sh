#!/bin/bash

# Exit in case of error

if [ $(uname -s) = "Linux" ]; then
    echo "Remove __pycache__ files"
    find . -type d -name __pycache__ -exec rm -r {} \+
fi
echo "Remove Altonomy_test files"
find . -type d -name Altonomy_test -exec rm -r {} \+

testStauts=0

if [[ -z "${CI}" ]]; then
  echo "On local machine"
  coverage run --source=altonomy -m pytest $1 --keep-duplicates $2 \
  && coverage report -m \
  && coverage-badge -o tests/test_helpers/coverage.svg -f
  testStauts=$?
else
  echo "On CI"
  coverage run --source=altonomy -m pytest $1 --keep-duplicates $2 \
  && coverage report -m 
  testStauts=$?
fi

echo Pytest exited $testStauts

COLOR_REST="$(tput sgr0)"
COLOR_RED="$(tput setaf 1)"
if [ $testStauts -eq 2 ]; then
  echo -e $COLOR_RED "Pipeeline Error: Coverage perceentage below threshold." $COLOR_REST
fi

exit $testStauts