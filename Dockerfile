# Minimal Spark-capable image for local PySpark (single container)
FROM python:3.11-slim

# Install Java for PySpark
RUN apt-get update && apt-get install -y --no-install-recommends \    openjdk-17-jre-headless \    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="$JAVA_HOME/bin:${PATH}"

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Default command (override in docker-compose)
CMD ["python", "-c", "print('Container ready. Override CMD to run flows.')"]
