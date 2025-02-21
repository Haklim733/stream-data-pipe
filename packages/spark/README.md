customized docker image https://github.com/databricks/docker-spark-iceberg

- removed gcp
- removed azure
- upgrade iceberg to 1.8.0
- upgrade python package pyiceberg to 0.8.1
- retains python 3.11
- retains spark \_2.12 (important)
- retains python path for java PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.9.7-src.zip:$PYTHONPATH (important)
