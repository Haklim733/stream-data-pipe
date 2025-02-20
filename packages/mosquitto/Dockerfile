FROM python:3.12-alpine

RUN apk add mosquitto-clients

WORKDIR /app

COPY requirements.txt .

RUN pip install -r requirements.txt

COPY . .

CMD ["bash"]