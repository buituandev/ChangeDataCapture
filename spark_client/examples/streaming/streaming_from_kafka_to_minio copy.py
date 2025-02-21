import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, schema_of_json
from pyspark.sql.types import *
from delta.tables import DeltaTable

# Initialize Spark Session
accessKeyId='12345678'
secretAccessKey='12345678'
minio_output_path = "s3a://change-data-capture/customers-delta"
checkpoint_dir = "s3a://change-data-capture/checkpoint"

# create a SparkSession
spark = SparkSession.builder \
    .appName("Spark Example MinIO") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.0,io.delta:delta-core_2.12:2.4.0") \
    .config("spark.hadoop.fs.s3a.access.key", accessKeyId) \
    .config("spark.hadoop.fs.s3a.secret.key", secretAccessKey) \
    .config("spark.hadoop.fs.s3a.path.style.access", True) \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", 
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
    .getOrCreate()
spark.sparkContext.setLogLevel('ERROR')


def create_dynamic_schema(sample_json):
    df = spark.createDataFrame([sample_json], StringType())
    parsed_schema = schema_of_json(sample_json)
    return parsed_schema

def generate_select_statements(payload_schema):
    before_fields = payload_schema["payload"]["before"].fields if "before" in payload_schema["payload"] else []
    after_fields = payload_schema["payload"]["after"].fields if "after" in payload_schema["payload"] else []
    
    select_cols = [
        col("data.payload.op").alias("operation"),
        col("data.payload.ts_ms").alias("timestamp")
    ]
    
    for field in before_fields:
        field_name = field.name
        select_cols.append(
            col(f"data.payload.before.{field_name}").alias(f"before_{field_name}")
        )
    
    for field in after_fields:
        field_name = field.name
        select_cols.append(
            col(f"data.payload.after.{field_name}").alias(f"after_{field_name}")
        )
    
    return select_cols

def debug(message):
    if message is None:
        message = "None"
    txt_path = "/opt/examples/streaming/debug.txt"
    if not os.path.exists(txt_path):
        with open(txt_path, "w") as f:
            f.write(message + "\n")
    else:
        with open(txt_path, "a") as f:
            f.write(message + "\n")

def process_batch(batch_df, batch_id):
    if batch_df.isEmpty():
        return
    
    json_data = batch_df.first()["value"]
    schema = create_dynamic_schema(json_data)
    
    parsed_batch = batch_df.select(from_json(col("value"), schema).alias("data"))
    select_cols = generate_select_statements(schema)
    parsed_data = parsed_batch.select(select_cols)
    
    for op_type in parsed_data.select("operation").distinct().collect():
        operation = op_type["operation"]
        debug(f"Operation: {operation}")
        try:
            existing_data = spark.read.format("delta").load(minio_output_path)
            debug(existing_data.show())
            delta_table = DeltaTable.forPath(spark, minio_output_path)
        except:
            if operation == "c":
                after_cols = [c for c in parsed_data.columns if c.startswith("after_")]
                insert_data = parsed_data.filter(col("operation") == "c") \
                    .select([col(c).alias(c.replace("after_", "")) for c in after_cols] + [col("timestamp")])
                if not insert_data.isEmpty():
                    debug("Inserting data 1")
                    debug(insert_data.show())
                    insert_data.write.format("delta").mode("append").save(minio_output_path)
            continue

        if operation == "c":
            after_cols = [c for c in parsed_data.columns if c.startswith("after_")]
            insert_data = parsed_data.filter(col("operation") == "c") \
                .select([col(c).alias(c.replace("after_", "")) for c in after_cols] + [col("timestamp")])

            if not insert_data.isEmpty():
                debug("Inserting data")
                debug(insert_data.show())
                insert_data.write.format("delta").mode("append").save(minio_output_path)

        elif operation == "u":
            after_cols = [c for c in parsed_data.columns if c.startswith("after_")]
            update_data = parsed_data.filter(col("operation") == "u") \
                .select([col(c).alias(c.replace("after_", "")) for c in after_cols] + [col("timestamp")])

            if not update_data.isEmpty():
                key_column = after_cols[0].replace("after_", "")
                exists = existing_data.join(
                    update_data.select(key_column),
                    key_column,
                    "inner"
                ).count() > 0

                if exists:
                    debug("Updating data")
                    debug(update_data.show())
                    delta_table.alias("target").merge(
                        update_data.alias("source"),
                        f"target.{key_column} = source.{key_column}"
                    ).whenMatchedUpdateAll().execute()

        elif operation == "d":
            before_cols = [c for c in parsed_data.columns if c.startswith("before_")]
            key_column = before_cols[0].replace("before_", "")
            delete_data = parsed_data.filter(col("operation") == "d") \
                .select(col(f"before_{key_column}").alias(key_column))

            if not delete_data.isEmpty():
                exists = existing_data.join(
                    delete_data,
                    key_column,
                    "inner"
                ).count() > 0

                if exists:
                    debug("Deleting data")
                    debug(delete_data.show())
                    delta_table.alias("target").merge(
                        delete_data.alias("source"),
                        f"target.{key_column} = source.{key_column}"
                    ).whenMatchedDelete().execute()

df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "dbserver2.public.links") \
    .option("failOnDataLoss", "false") \
    .load() \
    .selectExpr("CAST(value AS STRING) as value")

# Write stream with foreachBatch
query = df.writeStream \
    .foreachBatch(process_batch) \
    .option("checkpointLocation", checkpoint_dir) \
    .trigger(processingTime='30 seconds') \
    .start()

query.awaitTermination()