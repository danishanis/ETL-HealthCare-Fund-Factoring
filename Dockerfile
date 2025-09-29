# Dockerfile for Airflow + PySpark + Java
FROM apache/airflow:2.9.3-python3.10

USER root

# Install Java (works on arm64/amd64) and procps (ps command for Spark)
RUN apt-get update && \
    apt-get install -y openjdk-17-jdk procps && \
    apt-get clean

# Set JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-arm64
ENV PATH=$JAVA_HOME/bin:$PATH

USER airflow

# Install Python requirements
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy project code
COPY src /opt/airflow/src
COPY airflow/dags /opt/airflow/dags
COPY airflow/include /opt/airflow/include
COPY data /opt/data