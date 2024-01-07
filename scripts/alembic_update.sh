#!/bin/bash

alembic revision --autogenerate -m "Update Ace Table Schema" \
    && alembic upgrade head
