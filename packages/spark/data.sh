#!/bin/bash
curl https://data.cityofnewyork.us/resource/tg4x-b46p.json > data/nyc_film_permits.json \
    && curl https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2022-04.parquet -o data/yellow_tripdata_2022-04.parquet \
    && curl https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2022-03.parquet -o data/yellow_tripdata_2022-03.parquet \
    && curl https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2022-02.parquet -o data/yellow_tripdata_2022-02.parquet \
    && curl https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2022-01.parquet -o data/yellow_tripdata_2022-01.parquet \
    && curl https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-12.parquet -o data/yellow_tripdata_2021-12.parquet \
    && curl https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-11.parquet -o data/yellow_tripdata_2021-11.parquet \
    && curl https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-10.parquet -o data/yellow_tripdata_2021-10.parquet \
    && curl https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-09.parquet -o data/yellow_tripdata_2021-09.parquet \
    && curl https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-08.parquet -o data/yellow_tripdata_2021-08.parquet \
    && curl https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-07.parquet -o data/yellow_tripdata_2021-07.parquet \
    && curl https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-06.parquet -o data/yellow_tripdata_2021-06.parquet \
    && curl https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-05.parquet -o data/yellow_tripdata_2021-05.parquet \
    && curl https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-04.parquet -o data/yellow_tripdata_2021-04.parquet
