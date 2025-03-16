FROM python:3.13-alpine

WORKDIR /home/app
# Install Faker and Kafka
COPY ./requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY ./src .