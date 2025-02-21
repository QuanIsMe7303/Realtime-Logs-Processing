FROM bitnami/spark:latest

USER root

COPY requirements.txt /opt/

RUN apt-get update && apt-get install -y curl && \
    pip install -r /opt/requirements.txt

RUN mkdir -p /opt/spark-apps/checkpoints && \
chmod -R 777 /opt/spark-apps/checkpoints

WORKDIR /opt/spark-apps

# CMD ["spark-submit", "--master", "spark://spark:7077", "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0", "stream_processor.py"]
# CMD ["spark-submit", "--master", "spark://spark:7077", \
#      "--jars", "/root/.ivy2/jars/*", \
#      "main.py"]