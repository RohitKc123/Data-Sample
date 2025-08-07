# Use Python 3.9 slim image as base
FROM python:3.9-slim

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64

# Install system dependencies including Java (required for PySpark)
RUN apt-get update && apt-get install -y \
    openjdk-11-jdk \
    wget \
    curl \
    postgresql-client \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy requirements first for better caching
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Create necessary directories
RUN mkdir -p data/raw data/processed logs

# Set permissions
RUN chmod +x scripts/*.py
RUN chmod +x src/*.py

# Expose port (if needed for web interface)
EXPOSE 8080

# Default command
CMD ["python", "src/etl_pipeline.py"] 