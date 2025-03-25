# Morningstar NAV Processor

A service for processing and managing NAV (Net Asset Value) data from Morningstar.

## Overview

This application processes NAV data from Morningstar, handles data validation, and provides an API for accessing the processed data.

## Features

- NAV data processing and validation
- RESTful API for data access
- Google Drive integration for file management
- Automated email notifications
- Database management with SQLAlchemy

## Prerequisites

- Python 3.8+
- Docker and Docker Compose
- Google Drive API credentials
- Required environment variables (see `.env.example`)

## Quick Start

1. Clone the repository
2. Copy `.env.example` to `.env` and fill in your credentials
3. Place your Google Drive credentials file in the root directory
4. Run the deployment script:
   ```bash
   ./deploy.sh
   ```

## Documentation

Detailed documentation can be found in the `documentation/` directory.

## API Documentation

The API documentation is available at `/docs` when the service is running.

## Deployment

See [DEPLOYMENT.md](DEPLOYMENT.md) for detailed deployment instructions.

## License

[Your License Here]
