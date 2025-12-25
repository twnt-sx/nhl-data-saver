FROM apache/airflow:2.11.0-python3.11

COPY requirements.txt /requirements.txt
RUN python -m pip install --upgrade pip && \
    pip install --no-cache-dir -r /requirements.txt

USER root

RUN apt-get update && \
    apt-get install -y openjdk-17-jdk

USER airflow