import os
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
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

def get_field_order_from_debezium_schema(json_str):
    data = json.loads(json_str)
    schema = data['schema']
    
    for field in schema['fields']:
        if field['field'] in ['before', 'after']:
            field_defs = field['fields']
            return [f['field'] for f in field_defs]
    return []

def create_dynamic_schema(sample_json):
    json_df = spark.read.json(spark.sparkContext.parallelize([sample_json]))
    return json_df.schema, sample_json

def generate_select_statements(schema, original_json):
    # Get the ordered fields from the original Debezium schema
    ordered_fields = get_field_order_from_debezium_schema(original_json)
    
    # Extract the payload struct fields
    payload_struct = schema["payload"].dataType
    
    # Initialize select columns with operation and timestamp
    select_cols = [
        col("data.payload.op").alias("operation"),
        col("data.payload.ts_ms").alias("timestamp")
    ]
    
    # Add before fields if they exist and are a struct
    if "before" in [f.name for f in payload_struct.fields]:
        before_field = payload_struct["before"]
        if isinstance(before_field.dataType, StructType):
            for field_name in ordered_fields:
                select_cols.append(
                    col(f"data.payload.before.{field_name}").alias(f"before_{field_name}")
                )
    
    # Add after fields if they exist and are a struct
    if "after" in [f.name for f in payload_struct.fields]:
        after_field = payload_struct["after"]
        if isinstance(after_field.dataType, StructType):
            for field_name in ordered_fields:
                select_cols.append(
                    col(f"data.payload.after.{field_name}").alias(f"after_{field_name}")
                )
    
    return select_cols, ordered_fields

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
    
    sample_json = batch_df.first()["value"]
    schema, original_json = create_dynamic_schema(sample_json)
    
    parsed_batch = batch_df.select(from_json(col("value"), schema).alias("data"))
    select_cols, ordered_fields = generate_select_statements(schema, original_json)
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
                insert_cols = [col(f"after_{field}").alias(field) for field in ordered_fields] + [col("timestamp")]
                insert_data = parsed_data.filter(col("operation") == "c").select(insert_cols)
                if not insert_data.isEmpty():
                    debug("Inserting data 1")
                    debug(insert_data.show())
                    insert_data.write.format("delta").mode("append").save(minio_output_path)
            continue

        if operation == "c":
            insert_cols = [col(f"after_{field}").alias(field) for field in ordered_fields] + [col("timestamp")]
            insert_data = parsed_data.filter(col("operation") == "c").select(insert_cols)

            if not insert_data.isEmpty():
                debug("Inserting data")
                debug(insert_data.show())
                insert_data.write.format("delta").mode("append").save(minio_output_path)

        elif operation == "u":
            update_cols = [col(f"after_{field}").alias(field) for field in ordered_fields] + [col("timestamp")]
            update_data = parsed_data.filter(col("operation") == "u").select(update_cols)
            
            if not update_data.isEmpty():
                key_column = ordered_fields[0]
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
            key_column = ordered_fields[0]  # Use first field as key
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
    .trigger(processingTime='10 seconds') \
    .start()

query.awaitTermination()