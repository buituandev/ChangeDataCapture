# Project: Change Data Capture

The task is to stream data from a PostgreSQL database through Kafka to Minio object storage. A Debezium connector is established on Kafka Connect for capturing operational changes in the data. Spark Streaming is used for real-time data streaming and writing to Minio.

![Architecture of cdc](/assets/Operation.png)

## Step 1: Create docker-compose.yaml

Services that should be in the container:

- spark-master
- spark-worker
- spark-client
- postgresql
- minio
- kafka
- kafka-connect
- zookeeper

# Terminal 1

Start the services using Docker Compose.

```bash
(base) [train@10 change_data_capture_final_project]$ docker-compose up --build -d
```

## Step 2: Create Debezium PostgreSQL connector

# Terminal 2

Create the Debezium PostgreSQL connector using the provided JSON configuration.

```bash
(base) [train@10 change_data_capture_final_project]$ curl -i -X POST -H "Accept:application/json" -H "Content-Type:application/json" http://localhost:8083/connectors/ -d @debezium-postgres-connector.json
```

# Terminal 3

List all the topics to verify that the connector worked and the topic is created.

```bash
(base) [train@10 change_data_capture_final_project]$ docker-compose exec kafka /kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --list
```

## Step 3: Create Kafka Console Consumer

Create a Kafka Console Consumer to consume messages from the new topic created by the connector.

# Terminal 4

```bash
(base) [train@10 change_data_capture_final_project]$ docker-compose exec kafka /kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 --from-beginning --property print.key=true --topic dbserver2.public.links
```

## Step 4: Write to PostgreSQL with data-generator

# Terminal 5

Download the data source and write the first 1000 records to PostgreSQL.

```bash
(base) [train@10 input]$ curl --silent "https://raw.githubusercontent.com/erkansirin78/datasets/master/retail_db/customers.csv" > customers.csv

(base) [train@10 data-generator]$ python dataframe_to_postgresql.py -i /home/train/input/customers.csv -hst localhost -p 5432 -s , -u postgres -psw postgres -db postgres -t customers1 -rst 1 -es csv
```

## Step 5: Connect to PostgreSQL Shell

# Terminal 6

Connect to the PostgreSQL shell and capture changes.

```bash
(base) [train@10 change_data_capture_final_project]$ docker exec -it postgres bash
root@5da45c02cf36:/# psql -U postgres -d postgres
```

- Capture Changes

```sql
postgres=# ALTER TABLE links REPLICA IDENTITY FULL;
postgres=# delete from links where "customerId" = 10;
postgres=# UPDATE links SET "customerFName" = 'HUSEYIN' WHERE "customerId" = 17;
```

- The sample is expected at the Consumer terminal

```json
{
    "schema": { ... },
    "payload": {
        "before": {
            "customerId": 1,
            "customerFName": null,
            "customerLName": null,
            "customerEmail": null,
            "customerPassword": null,
            "customerStreet": null,
            "customerCity": null,
            "customerState": null,
            "customerZipcode": null
        },
        "after": null,
        "source": {
            "version": "1.9.6.Final",
            "connector": "postgresql",
            "name": "dbserver2",
            "ts_ms": 1666164350827,
            "snapshot": "false",
            "db": "postgres",
            "sequence": "[\"37082784\",\"37082784\"]",
            "schema": "public",
            "table": "LİNKS",
            "txId": 778,
            "lsn": 37082784,
            "xmin": null
        },
        "op": "d",
        "ts_ms": 1666164351137,
        "transaction": null
    }
}
```

- Update a row
  - The sample is expected at the Consumer terminal

```json
{
    "schema": { ... },
    "payload": {
        "before": null,
        "after": {
            "customerId": 3,
            "customerFName": "test",
            "customerLName": "Smith",
            "customerEmail": "XXXXXXXXX",
            "customerPassword": "XXXXXXXXX",
            "customerStreet": "3422 Blue Pioneer Bend",
            "customerCity": "Caguas",
            "customerState": "PR",
            "customerZipcode": 725
        },
        "source": {
            "version": "1.9.6.Final",
            "connector": "postgresql",
            "name": "dbserver2",
            "ts_ms": 1666164929489,
            "snapshot": "false",
            "db": "postgres",
            "sequence": "[\"37092672\",\"37101112\"]",
            "schema": "public",
            "table": "LİNKS",
            "txId": 780,
            "lsn": 37101112,
            "xmin": null
        },
        "op": "u",
        "ts_ms": 1666164929967,
        "transaction": null
    }
}
```

## Step 6: Write to Minio

Open MinIO Web UI - http://localhost:9001
Create a bucket called `change-data-capture`.

## Step 7: Spark Streaming

- Read the messages from Kafka using Spark.
- Parse JSON data.

# Terminal 7

Write to Minio using Spark.

```bash
(base) [train@10 change_data_capture_final_project]$ docker exec -it spark-client bash
root@db8b5f6e0e2e:/# spark-submit --master local --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,io.delta:delta-core_2.12:2.4.0 opt/examples/streaming/streaming_from_kafka_to_minio.py
```

- Expected schema

```plaintext
root
|-- payload.before.customerId: string (nullable = true)
|-- payload.before.customerFName: string (nullable = true)
|-- payload.before.customerLName: string (nullable = true)
|-- payload.before.customerEmail: string (nullable = true)
|-- payload.before.customerPassword: string (nullable = true)
|-- payload.before.customerStreet: string (nullable = true)
|-- payload.before.customerCity: string (nullable = true)
|-- payload.before.customerState: string (nullable = true)
|-- payload.before.customerZipcode: string (nullable = true)
|-- payload.after.customerId: string (nullable = true)
|-- payload.after.customerFName: string (nullable = true)
|-- payload.after.customerLName: string (nullable = true)
|-- payload.after.customerEmail: string (nullable = true)
|-- payload.after.customerPassword: string (nullable = true)
|-- payload.after.customerStreet: string (nullable = true)
|-- payload.after.customerCity: string (nullable = true)
|-- payload.after.customerState: string (nullable = true)
|-- payload.after.customerZipcode: string (nullable = true)
|-- payload.ts_ms: string (nullable = true)
|-- payload.op: string (nullable = true)
```

# Terminal 6

- Capture Changes

```sql
postgres=# delete from customers1 where "customerId" = 55;
postgres=# UPDATE customers1 SET "customerFName" = 'SAPAYDIN' WHERE "customerId" = 21;
```
