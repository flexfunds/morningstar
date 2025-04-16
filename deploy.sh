#!/bin/bash

# Exit on any error
set -e

# Create necessary directories if they don't exist
mkdir -p output data

# Verify template files exist locally before building
echo "Verifying template files..."
required_templates=(
    "Series Qualitative Data.xlsx"
    "NAVs Historical Prices 03.21.2025.xlsx"
    "LAM_SFI_Price -SIX Financial Template.xlsx"
    "nav_seed_data.csv"
    "Exclude ISINs.csv"
    "Morningstar Performance Template.xls"
)

for template in "${required_templates[@]}"; do
    if [ ! -f "input/template/$template" ]; then
        echo "ERROR: Required template file $template is missing!"
        exit 1
    fi
done

# Verify database file exists
if [ ! -f "nav_data.db" ]; then
    echo "ERROR: Database file nav_data.db is missing! Please place it in the root directory."
    exit 1
fi

# Verify Google Drive credentials
if [ ! -f "ftp-drive-sync-33b2ad1dce15.json" ]; then
    echo "ERROR: Google Drive credentials file is missing!"
    exit 1
fi

# Build and start the containers
echo "Building and starting containers..."
docker-compose up --build -d

# Wait for the container to be healthy
echo "Waiting for service to be healthy..."
max_attempts=30
attempt=1
while [ $attempt -le $max_attempts ]; do
    if curl -s http://localhost:9080/health > /dev/null; then
        echo "Service is healthy!"
        break
    fi
    echo "Attempt $attempt: Waiting for service to be healthy..."
    sleep 5
    attempt=$((attempt + 1))
done

if [ $attempt -gt $max_attempts ]; then
    echo "ERROR: Service did not become healthy within the expected time"
    docker-compose logs
    exit 1
fi

# Check if the service is running
if docker-compose ps | grep -q "nav-processor.*Up"; then
    echo "Deployment successful! The service is running on port 9080"
    
    # Get container ID
    container_id=$(docker-compose ps -q nav-processor)
    
    # Verify template files in container
    echo "Verifying template files in container..."
    for template in "${required_templates[@]}"; do
        if ! docker exec $container_id test -f "/app/input/template/$template"; then
            echo "ERROR: Template file $template is missing in container!"
            exit 1
        else
            echo "✓ $template found in container"
        fi
    done
    
    # Verify environment variables
    echo "Verifying environment variables..."
    required_vars=(
        "API_KEY"
        "ETPCAP2_FTP_HOST"
        "SMTP_HOST"
        "GOOGLE_DRIVE_CREDENTIALS_PATH"
    )
    
    for var in "${required_vars[@]}"; do
        if ! docker exec $container_id env | grep -q "^$var="; then
            echo "WARNING: Environment variable $var is not set in the container"
        fi
    done
    
    echo "Deployment verification complete!"
else
    echo "Deployment failed. Check the logs with 'docker-compose logs'"
    exit 1
fi 