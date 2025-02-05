# Use Python 3.12.3 slim image
FROM python:3.12.3-slim

# Set working directory
WORKDIR /app

# Copy requirements first to leverage Docker cache
COPY requirements.docker.txt .

# Install dependencies (excluding Windows-specific packages)
RUN pip install --no-cache-dir -r requirements.docker.txt

# Copy the application
COPY . .

# Create directories for input/output
RUN mkdir -p input/template output

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV PORT=8080

# Expose the port
EXPOSE 8080

# Run the API
CMD ["python", "api.py"] 