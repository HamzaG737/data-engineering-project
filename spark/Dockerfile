FROM bitnami/spark:latest

USER root

#RUN pip install --upgrade pip

WORKDIR /opt/bitnami/spark

RUN pip install py4j==0.10.9.7


#COPY ./jars ./jars
COPY ./src/spark_pgsql/spark_streaming.py ./spark_streaming.py
COPY ./src/constants.py ./src/constants.py

ENV POSTGRES_DOCKER_USER=host.docker.internal
ARG POSTGRES_PASSWORD
ENV POSTGRES_PASSWORD=$POSTGRES_PASSWORD

#CMD ["./bin/spark-submit", "--master", "local[*]", "--packages", "org.postgresql:postgresql:42.5.4,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"  ,"./spark_streaming.py"]
