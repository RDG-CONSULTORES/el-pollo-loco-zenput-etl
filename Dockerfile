# Use Python 3.11 slim - conocido que funciona
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    libpq-dev \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy essential files for ETL
COPY main.py .
COPY etl_supervisiones_completo.py .

# Create data directory and copy essential CSV
RUN mkdir -p data
COPY data/86_sucursales_master.csv ./data/

# Set environment variables  
ENV PORT=8080
ENV PYTHONUNBUFFERED=1

# Expose the port
EXPOSE 8080

# Run the application
CMD ["python", "main.py"]