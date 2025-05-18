# Start from the official Airflow image
FROM apache/airflow:2.8.0

# Switch to root to install dependencies
USER root

# Optional: install OS packages if your Python scripts need them
RUN apt-get update && apt-get install -y build-essential

# Switch back to airflow user
USER airflow

# Copy Python scripts and requirements
COPY requirements.txt /DataEngineering/requirements.txt
COPY scripts /DataEngineering/scripts

# Install custom Python dependencies
RUN pip install --no-cache-dir -r /requirements.txt
