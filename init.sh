#!/bin/bash

# Initialize database if it doesn't exist
if [ ! -f /app/data/nav_data.db ]; then
    echo "Initializing database..."
    cp /app/initial-data/nav_data.db /app/data/nav_data.db
    chmod 644 /app/data/nav_data.db
fi

# Start the API
python api.py 