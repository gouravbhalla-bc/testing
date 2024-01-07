#!/bin/bash

flake8 --extend-ignore=W503 altonomy/
CODE_SUCCESS=$?

flake8 --extend-ignore=W503 tests/
TEST_SUCCESS=$?

if [ $TEST_SUCCESS -ne 0 ] || [ $CODE_SUCCESS -ne 0 ] ;
then
    exit 1
fi
