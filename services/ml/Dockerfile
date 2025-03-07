# Dockerfile
FROM nvidia/cuda:12.8.0-cudnn-runtime-ubuntu22.04

# Set the working directory
WORKDIR /app
RUN apt-get update -y && \
    apt-get install -y python3 \
    python3-pip \
    vim \
    unzip \
    zip \
    ssh && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*
RUN ln -s /usr/bin/python3 /usr/bin/python

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt