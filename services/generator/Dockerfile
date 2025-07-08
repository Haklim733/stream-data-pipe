FROM python:3.12-alpine

WORKDIR /home/app
# Install Faker and Kafka
COPY ./requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

CMD ["sh", "-c", "tail -f /dev/null"]