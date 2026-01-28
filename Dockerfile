# Dockerfile for Chargeflow Data Engineering Assignment
# Uses python:3.9-slim for minimal footprint (FinOps best practice)

FROM python:3.9-slim

# Set working directory
WORKDIR /app

# Explicitly add the current directory to Python path
# This fixes the ModuleNotFoundError inside the container
ENV PYTHONPATH=/app

# Copy requirements first for better layer caching
COPY requirements.txt .

# Install dependencies
# --no-cache-dir reduces image size by not caching pip packages
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code and tests
COPY src/ ./src/
COPY tests/ ./tests/
COPY data/ ./data/

# Default command runs the test suite
CMD ["pytest", "tests/", "-v"]