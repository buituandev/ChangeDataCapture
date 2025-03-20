# 🔄 Change Data Capture (CDC) Pipeline

<div align="center">

<img src="/assets/logo.png" alt="CDC Logo" width="650px" style="border-radius: 10px; box-shadow: 0 4px 8px rgba(0,0,0,0.2);"/>

<br>

[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-336791?style=for-the-badge&logo=postgresql&logoColor=white)](https://www.postgresql.org/)
[![MongoDB](https://img.shields.io/badge/MongoDB-47A248?style=for-the-badge&logo=mongodb&logoColor=white)](https://www.mongodb.com/)
[![Kafka](https://img.shields.io/badge/Apache%20Kafka-231F20?style=for-the-badge&logo=apache-kafka&logoColor=white)](https://kafka.apache.org/)
[![Spark](https://img.shields.io/badge/Apache%20Spark-E25A1C?style=for-the-badge&logo=apache-spark&logoColor=white)](https://spark.apache.org/)
[![MinIO](https://img.shields.io/badge/MinIO-C72E49?style=for-the-badge&logo=minio&logoColor=white)](https://min.io/)
[![Docker](https://img.shields.io/badge/Docker-2496ED?style=for-the-badge&logo=docker&logoColor=white)](https://www.docker.com/)

<p align="center">
  <i>A high-performance, real-time data streaming solution for modern data architectures</i>
</p>

</div>

---

## 📑 Table of Contents

<p align="center">
  <a href="#-overview">Overview</a> •
  <a href="#-architecture">Architecture</a> •
  <a href="#-project-structure">Project Structure</a> •
  <a href="#-features">Features</a> •
  <a href="#-implementation-guide">Implementation Guide</a> •
  <a href="#-expected-schema">Expected Schema</a> •
  <a href="#-monitoring-and-maintenance">Monitoring</a> •
  <a href="#-troubleshooting">Troubleshooting</a>
</p>

---

## 🔍 Overview

This project implements a robust real-time **Change Data Capture (CDC)** pipeline that seamlessly streams data from both **PostgreSQL** and **MongoDB** databases through **Kafka** into **MinIO** object storage. The system leverages **Debezium** connectors to capture operational data changes, while **Apache Spark Streaming** processes the data in real-time, providing a scalable and fault-tolerant solution for modern data architectures.

## 🏗️ Architecture

<div align="center">
<img src="/assets/Operation.png" alt="Architecture of CDC" width="800px" style="border-radius: 8px; box-shadow: 0 6px 12px rgba(0,0,0,0.15); margin: 20px 0;"/>
</div>

The system follows a modern streaming data architecture with the following components:

<table>
  <tr>
    <td width="20%"><b>Source Systems</b></td>
    <td>PostgreSQL and MongoDB act as primary data sources with their change logs enabled</td>
  </tr>
  <tr>
    <td><b>CDC Layer</b></td>
    <td>Debezium connectors capture database changes in real-time without impacting source performance</td>
  </tr>
  <tr>
    <td><b>Message Broker</b></td>
    <td>Apache Kafka provides a resilient message queue with topic-based organization</td>
  </tr>
  <tr>
    <td><b>Processing Engine</b></td>
    <td>Apache Spark Streaming enables complex transformations with micro-batch processing</td>
  </tr>
  <tr>
    <td><b>Data Lake</b></td>
    <td>MinIO provides S3-compatible object storage with Delta Lake format support</td>
  </tr>
</table>

## 📁 Project Structure

```
📦 cdc-pipeline
 ┣ 📂 spark_client              # Spark streaming application
 ┃ ┗ 📂 src                     # Source code
 ┃   ┣ 📜 cdc_stream.py         # Main CDC processing logic
 ┃   ┣ 📜 config_manager.py     # Configuration management
 ┃   ┗ 📜 streaming_from_kafka_to_minio.py # Kafka to MinIO streaming
 ┣ 📂 debezium_connector_config # Debezium connector configurations
 ┣ 📂 docs                      # Project documentation
 ┗ 📜 docker-compose.yaml       # Docker services configuration
```

## ✨ Features

<div align="center">
<table>
  <tr>
    <td align="center"><b>⚡</b></td>
    <td><b>Real-time CDC</b></td>
    <td>Capture changes from PostgreSQL and MongoDB with millisecond latency</td>
  </tr>
  <tr>
    <td align="center"><b>🔄</b></td>
    <td><b>Schema Evolution</b></td>
    <td>Dynamic schema discovery and support for evolving data structures</td>
  </tr>
  <tr>
    <td align="center"><b>📝</b></td>
    <td><b>Full CRUD Support</b></td>
    <td>Handle inserts, updates, and deletes with proper tracking</td>
  </tr>
  <tr>
    <td align="center"><b>🛡️</b></td>
    <td><b>Fault Tolerance</b></td>
    <td>Resilient processing with checkpoint management and exactly-once semantics</td>
  </tr>
  <tr>
    <td align="center"><b>⚙️</b></td>
    <td><b>Configurability</b></td>
    <td>Easily adjustable processing intervals and transformation rules</td>
  </tr>
  <tr>
    <td align="center"><b>📊</b></td>
    <td><b>Delta Format</b></td>
    <td>Delta table support in MinIO for ACID transactions and time travel</td>
  </tr>
  <tr>
    <td align="center"><b>🚨</b></td>
    <td><b>Error Handling</b></td>
    <td>Comprehensive error management with dead-letter queues</td>
  </tr>
</table>
</div>

## 📋 Implementation Guide (With example/Testing used)

### <span style="color:#4285F4">Step 1: Create Docker Environment</span>

Start the services using Docker Compose:

```bash
# Terminal 1
docker-compose up --build -d
```

<details>
<summary>📌 Service Health Check</summary>

```bash
# Check if all containers are running
docker-compose ps

# View service logs
docker-compose logs -f
```
</details>

### <span style="color:#4285F4">Step 2: Create Debezium Connectors</span>

Create the Debezium PostgreSQL connector:

```bash
# Terminal 2
curl -i -X POST -H "Accept:application/json" -H "Content-Type:application/json" \
  http://localhost:8083/connectors/ \
  -d @debezium_connector_config/postgres-connector.json
```

Create the Debezium MongoDB connector:

```bash
curl -i -X POST -H "Accept:application/json" -H "Content-Type:application/json" \
  http://localhost:8083/connectors/ \
  -d @debezium_connector_config/mongo-connector.json
```

<details>
<summary>📌 Verify Connector Status</summary>

```bash
# List all connectors
curl -s http://localhost:8083/connectors | jq

# Check connector status
curl -s http://localhost:8083/connectors/postgres-connector/status | jq
curl -s http://localhost:8083/connectors/mongodb-connector/status | jq
```
</details>

### <span style="color:#4285F4">Step 3: Monitor Kafka Topics</span>

List all the topics to verify that the connectors worked:

```bash
# Terminal 3
docker-compose exec kafka /kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:9092 --list
```

<div align="center">
<table>
  <tr>
    <th>Database</th>
    <th>Monitoring Command</th>
  </tr>
  <tr>
    <td>PostgreSQL</td>
    <td>

```bash
docker-compose exec kafka /kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 --from-beginning \
  --property print.key=true --topic dbserver2.public.links
```

</td>
  </tr>
  <tr>
    <td>MongoDB</td>
    <td>

```bash
docker-compose exec kafka /kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 --from-beginning \
  --property print.key=true --topic dbserver1.inventory.customers
```

</td>
  </tr>
</table>
</div>

### <span style="color:#4285F4">Step 4: Write to Databases</span>

For PostgreSQL:

```bash
# Terminal 5
curl --silent "https://raw.githubusercontent.com/erkansirin78/datasets/master/retail_db/customers.csv" > customers.csv
python dataframe_to_postgresql.py -i customers.csv -hst localhost -p 5432 -s , \
  -u postgres -psw postgres -db postgres -t customers1 -rst 1 -es csv
```

<details>
<summary>📌 MongoDB Initialization</summary>

```
# Data will be automatically loaded through mongo-init scripts in the Docker Compose setup
```
</details>

### <span style="color:#4285F4">Step 5: Database Operations</span>

<div align="center">
<table>
  <tr>
    <th width="50%">PostgreSQL Operations</th>
    <th width="50%">MongoDB Operations</th>
  </tr>
  <tr>
    <td>

```sql
# Connect to PostgreSQL
psql -U postgres -d postgres

# Enable full replica identity
ALTER TABLE links REPLICA IDENTITY FULL;

# Delete operation
DELETE FROM links WHERE "customerId" = 10;

# Update operation
UPDATE links SET "customerFName" = 'HUSEYIN' 
WHERE "customerId" = 17;
```

</td>
    <td>

```javascript
# Connect to MongoDB
mongosh

# Switch to database
use inventory

# Update operation
db.customers.updateOne(
  {_id: ObjectId("...")}, 
  {$set: {name: "Updated Name"}}
)

# Delete operation
db.customers.deleteOne({_id: ObjectId("...")})
```

</td>
  </tr>
</table>
</div>

### <span style="color:#4285F4">Step 6: Configure MinIO</span>

1. Open MinIO Web UI - http://localhost:9001
2. Create a bucket called `change-data-capture`
3. Configure access credentials in `spark_client/src/config.json`

<div align="center">
<img src="/assets/minio-setup.png" alt="MinIO Setup" width="650px" style="border-radius: 8px; box-shadow: 0 4px 8px rgba(0,0,0,0.15); margin: 20px 0;"/>
</div>

### <span style="color:#4285F4">Step 7: Run Spark Streaming Application</span>

```bash
# Terminal 7
docker exec -it spark-client bash
spark-submit --master local \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,io.delta:delta-core_2.12:2.4.0 \
  opt/examples/streaming/streaming_from_kafka_to_minio.py
```

<details>
<summary>📌 Spark Application Monitoring</summary>

```bash
# View Spark application logs
docker exec -it spark-client bash -c "cat /opt/spark/logs/spark-*.out"

# Check Spark UI
# Open http://localhost:4040 in your browser
```
</details>

## 📊 Expected Schema

The streaming application processes events with the following schema:

<div style="background-color: #f8f9fa; padding: 15px; border-radius: 5px; margin: 10px 0;">

```json
{
  "root": {
    "payload.before": {
      "customerId": "string",
      "customerFName": "string",
      "customerLName": "string",
      "customerEmail": "string",
      "customerPassword": "string",
      "customerStreet": "string",
      "customerCity": "string",
      "customerState": "string",
      "customerZipcode": "string"
    },
    "payload.after": {
      "// same fields as before": ""
    },
    "payload.ts_ms": "string",
    "payload.op": "string"
  }
}
```

</div>

<details>
<summary>📌 Operation Types</summary>
<ul>
  <li><code>c</code>: Create (Insert)</li>
  <li><code>u</code>: Update</li>
  <li><code>d</code>: Delete</li>
  <li><code>r</code>: Read (Initial snapshot)</li>
</ul>
</details>

## 📈 Monitoring and Maintenance

<div align="center">
<table>
  <tr>
    <td align="center" width="80"><b>💾</b></td>
    <td><b>Checkpointing</b></td>
    <td>Checkpoint files are stored in MinIO for fault tolerance and recovery</td>
  </tr>
  <tr>
    <td align="center"><b>📝</b></td>
    <td><b>Logging</b></td>
    <td>Comprehensive logs available in the spark-client container</td>
  </tr>
  <tr>
    <td align="center"><b>📊</b></td>
    <td><b>Kafka Monitoring</b></td>
    <td>Regular monitoring of Kafka topics for message flow and lag</td>
  </tr>
  <tr>
    <td align="center"><b>🖥️</b></td>
    <td><b>Data Verification</b></td>
    <td>Use MinIO UI to verify data storage and partitioning</td>
  </tr>
</table>
</div>

<div align="center">
<img src="/assets/monitoring-dashboard.png" alt="Monitoring Dashboard" width="700px" style="border-radius: 8px; box-shadow: 0 4px 8px rgba(0,0,0,0.15); margin: 20px 0;"/>
</div>

## 🔧 Troubleshooting

<div class="troubleshooting">
<table>
  <tr>
    <th>Issue</th>
    <th>Solution</th>
  </tr>
  <tr>
    <td><b>Connectors fail to start</b></td>
    <td>
      <ul>
        <li>Check database connectivity</li>
        <li>Verify connector configurations</li>
        <li>Check Kafka Connect logs: <code>docker-compose logs -f connect</code></li>
        <li>Ensure database permissions are correct</li>
      </ul>
    </td>
  </tr>
  <tr>
    <td><b>Streaming application fails</b></td>
    <td>
      <ul>
        <li>Verify MinIO credentials and bucket permissions</li>
        <li>Check Spark logs: <code>docker exec -it spark-client bash -c "cat /opt/spark/logs/spark-*.out"</code></li>
        <li>Ensure sufficient resources for Spark workers</li>
        <li>Validate Kafka topic existence and permissions</li>
      </ul>
    </td>
  </tr>
  <tr>
    <td><b>Data consistency issues</b></td>
    <td>
      <ul>
        <li>Verify REPLICA IDENTITY settings for PostgreSQL</li>
        <li>Check checkpoint files for corruption</li>
        <li>Review CDC event payloads for schema compatibility</li>
        <li>Ensure no network partitioning between services</li>
      </ul>
    </td>
  </tr>
</table>
</div>

---

<div align="center">
<p>
  <a href="https://github.com/yourusername/cdc-pipeline/issues">Report Bug</a> •
  <a href="https://github.com/yourusername/cdc-pipeline/issues">Request Feature</a>
</p>

<p>© 2025 Your Organization | Licensed under MIT</p>

</div>