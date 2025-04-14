# Use Python 3.12.3 slim image
FROM python:3.12.3-slim

# Set working directory
WORKDIR /app

# Create non-root user
RUN useradd -m -u 1000 appuser

# Copy requirements first to leverage Docker cache
COPY requirements.docker.txt ./

# Install dependencies (excluding Windows-specific packages)
RUN pip install --no-cache-dir -r requirements.docker.txt

# Create necessary directories
RUN mkdir -p /app/input/template /app/output /app/data

# Create temp directory for nav processor
RUN mkdir -p /tmp/nav_processor && \
    chown appuser:appuser /tmp/nav_processor && \
    chmod 777 /tmp/nav_processor

# Debug: Show what's in the local template directory
RUN echo "Contents of local directory before copy:" && ls -la input/template || true

# Copy template files
COPY input/template/ /app/input/template/

# Debug: Show what was copied
RUN echo "Contents after copy:" && ls -la /app/input/template/

# Copy Google Drive credentials
COPY ftp-drive-sync-33b2ad1dce15.json /app/
RUN chown appuser:appuser /app/ftp-drive-sync-33b2ad1dce15.json && \
    chmod 600 /app/ftp-drive-sync-33b2ad1dce15.json

# Copy initialization script
COPY init.sh /app/
RUN chmod +x /app/init.sh && \
    chown appuser:appuser /app/init.sh

# Copy the rest of the application
COPY . .

# Set proper permissions
RUN chown -R appuser:appuser /app

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    PORT=8080 \
    DATABASE_URL=sqlite:////app/data/nav_data.db \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH=/app

# Switch to non-root user
USER appuser

# Expose the port
EXPOSE 8080

# Run the initialization script
CMD ["/app/init.sh"] 