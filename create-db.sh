#!/usr/bin/env bash

for file in "./src/database"/*.sql; do
    psql -f "${file}"
done