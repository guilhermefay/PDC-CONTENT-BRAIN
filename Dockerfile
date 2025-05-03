# Use an official Python runtime as a parent image
FROM python:3.11-slim

# Set the working directory in the container
WORKDIR /app

# Install system dependencies that might be needed by Python packages
# (Example: build-essential for packages that compile C code)
# Add more if needed based on specific dependency errors
# RUN apt-get update && apt-get install -y --no-install-recommends build-essential && rm -rf /var/lib/apt/lists/*

# Upgrade pip to the latest version
RUN pip install --upgrade pip

# Copy the requirements file into the container
COPY requirements.txt .

# Install any needed packages specified in requirements.txt
# Using --no-cache-dir reduces image size
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application code into the container
COPY . .

# Make port 8000 available to the world outside this container
# Railway will map its internal port ($PORT) to this automatically if needed
EXPOSE 8000

# Define environment variable for the port (optional, Railway often injects $PORT)
# ENV PORT=8000

# Run uvicorn server when the container launches
# Use 0.0.0.0 to allow connections from outside the container
CMD ["python", "etl/annotate_and_index.py", "--source", "gdrive"] 