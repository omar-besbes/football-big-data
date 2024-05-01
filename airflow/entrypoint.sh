#!/bin/bash

# Wait for the webserver to be up
sleep 10

# Create default user if it does not exist
airflow users list | grep -q "ali" || airflow users create \
    --username ali \
    --password ali \
    --firstname Test \
    --lastname User \
    --role User \
    --email ali@example.com

# Pass all arguments to the original Airflow entrypoint script
exec /entrypoint "$@"
