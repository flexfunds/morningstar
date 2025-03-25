# Deployment Guide

## Prerequisites

- Docker and Docker Compose installed
- Git installed
- Access to required credentials and environment files

## Deployment Steps

1. **Clone the Repository**

   ```bash
   git clone <repository-url>
   cd morningstar
   ```

2. **Required Files**
   Before starting the deployment, ensure you have the following files:

   - `.env.production` - Production environment variables
   - `ftp-drive-sync-33b2ad1dce15.json` - Google Drive credentials
   - `nav_data.db` - Initial database file

3. **File Placement**
   Place the required files in the following locations:

   ```
   morningstar/
   ├── .env.production
   ├── ftp-drive-sync-33b2ad1dce15.json
   └── data/
       └── nav_data.db
   ```

4. **Deploy the Application**

   ```bash
   # Make the deployment script executable
   chmod +x deploy.sh

   # Run the deployment script
   ./deploy.sh
   ```

5. **Verify Deployment**
   - The application will be available on port 8080
   - Check the logs using: `docker-compose logs -f`
   - Verify the service is running: `docker-compose ps`

## Important Notes

- Never commit sensitive files to the repository
- Keep your credentials secure and rotate them regularly
- The application requires specific template files in the `input/template/` directory
- Database backups should be performed regularly

## Troubleshooting

If you encounter issues:

1. Check the logs: `docker-compose logs -f`
2. Verify all required files are present
3. Ensure all environment variables are correctly set
4. Check Docker container status: `docker-compose ps`
