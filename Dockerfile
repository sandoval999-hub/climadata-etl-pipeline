FROM python:3.12-slim

LABEL maintainer="ClimaData Solutions"
LABEL description="ETL Pipeline for weather data ingestion"

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    default-mysql-client \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for Docker layer caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Create data directories
RUN mkdir -p data/reports logs

# Default command: run the full pipeline
ENTRYPOINT ["python", "main.py"]
CMD ["--mode", "all"]
