#!/bin/bash

docker compose up  --exit-code-from app-test

docker compose down
