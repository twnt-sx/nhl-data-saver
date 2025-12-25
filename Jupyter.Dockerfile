FROM jupyter/datascience-notebook:python-3.11

COPY requirements.txt /requirements.txt

USER root

RUN python -m pip install --upgrade pip && \
    pip install --no-cache-dir -r /requirements.txt && \
    apt-get update && \
    apt-get install -y --no-install-recommends openjdk-17-jdk && \
    rm -rf /var/lib/apt/lists/*

RUN mkdir -p /opt/jupyter && \
    chown -R ${NB_UID}:${NB_GID} /opt/jupyter

USER ${NB_UID}

WORKDIR /opt/jupyter