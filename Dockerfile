# Dockerfile
FROM python:3.10-slim

# Set working directory
WORKDIR /app

# Copy requirements and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Expose port (Railway default PORT)
ENV PORT 8000

# Start the API using uvicorn
CMD ["uvicorn", "api.rag_api:app", "--host", "0.0.0.0", "--port", "$PORT"] 