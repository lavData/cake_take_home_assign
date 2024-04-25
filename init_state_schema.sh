#!/bin/bash

set -e

# Variables
DB_USER="airflow"
DB_PASSWORD="airflow"
DB_NAME="airflow"
SCHEMA_NAME="state"
TABLE_NAME="state"

psql -h localhost -p 15432  -d "${DB_NAME}" -U "${DB_USER}" -c "
CREATE SCHEMA IF NOT EXISTS ${SCHEMA_NAME};
CREATE TABLE IF NOT EXISTS ${SCHEMA_NAME}.${TABLE_NAME} (
    type_hook TEXT NOT NULL PRIMARY KEY,
    last_sync TEXT NOT NULL
);
"
