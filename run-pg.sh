#!/bin/bash

docker run \
    -d \
    --rm \
    --name pg-data-loader \
    -e POSTGRES_PASSWORD=postgres \
    -p 5432:5432 \
    -v $(pwd)/data:/var/lib/postgresql/data \
    postgres
