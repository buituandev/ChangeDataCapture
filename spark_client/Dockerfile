FROM veribilimiokulu/pyspark-3.4.1_python-3.8:1.0

# Install MinIO Client
RUN curl -o mc https://dl.min.io/client/mc/release/linux-amd64/mc && chmod +x mc && mv mc /usr/local/bin/

RUN mc alias set dataops_minio http://minio:9000 dataops Ankara06 --api S3v4

RUN apt-get update && apt-get install -y wget

# Download PostgreSQL JDBC Driver
RUN wget -O /opt/postgresql-42.7.5.jar https://jdbc.postgresql.org/download/postgresql-42.7.5.jar

#Install pyspark 3.4.1
RUN pip install pyspark==3.4.1

# Set SPARK_CLASSPATH to include PostgreSQL JDBC driver
ENV SPARK_CLASSPATH="/opt/postgresql-42.7.5.jar"

COPY requirements.txt /tmp/
RUN pip install -r /tmp/requirements.txt