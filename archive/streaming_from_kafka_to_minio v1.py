import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import *
from delta.tables import DeltaTable

# Initialize Spark Session
accessKeyId='12345678'
secretAccessKey='12345678'

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

# Schema definition for the Kafka JSON payload
customerFields = [
    StructField("customerId", LongType()),
    StructField("customerFName", StringType()),
    StructField("customerLName", StringType()),
    StructField("customerEmail", StringType()),
    StructField("customerPassword", StringType()),
    StructField("customerStreet", StringType()),
    StructField("customerCity", StringType()),
    StructField("customerState", StringType()),
    StructField("customerZipcode", LongType())
]

schema = StructType([
    StructField("payload", StructType([
        StructField("before", StructType(customerFields)),
        StructField("after", StructType(customerFields)),
        StructField("ts_ms", LongType()),
        StructField("op", StringType())
    ]))
])

# Define MinIO paths
minio_output_path = "s3a://change-data-capture/customers-delta"
checkpoint_dir = "s3a://change-data-capture/checkpoint"

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
    parsed_batch = batch_df.select(from_json(col("value"), schema).alias("data"))
    parsed_data = parsed_batch.select(
        col("data.payload.op").alias("operation"),
        col("data.payload.ts_ms").alias("timestamp"),
        col("data.payload.before.customerId").alias("before_customerId"),
        col("data.payload.before.customerFName").alias("before_customerFName"),
        col("data.payload.before.customerLName").alias("before_customerLName"),
        col("data.payload.before.customerEmail").alias("before_customerEmail"),
        col("data.payload.before.customerPassword").alias("before_customerPassword"),
        col("data.payload.before.customerStreet").alias("before_customerStreet"),
        col("data.payload.before.customerCity").alias("before_customerCity"),
        col("data.payload.before.customerState").alias("before_customerState"),
        col("data.payload.before.customerZipcode").alias("before_customerZipcode"),
        col("data.payload.after.customerId").alias("after_customerId"),
        col("data.payload.after.customerFName").alias("after_customerFName"),
        col("data.payload.after.customerLName").alias("after_customerLName"),
        col("data.payload.after.customerEmail").alias("after_customerEmail"),
        col("data.payload.after.customerPassword").alias("after_customerPassword"),
        col("data.payload.after.customerStreet").alias("after_customerStreet"),
        col("data.payload.after.customerCity").alias("after_customerCity"),
        col("data.payload.after.customerState").alias("after_customerState"),
        col("data.payload.after.customerZipcode").alias("after_customerZipcode")
    )
    for op_type in parsed_data.select("operation").distinct().collect():
        operation = op_type["operation"]
        debug(f"Operation: {operation}")
        try:
            existing_data = spark.read.format("delta").load(minio_output_path)
            debug(existing_data.show())
            delta_table = DeltaTable.forPath(spark, minio_output_path)
        except:
            if operation == "c":
                insert_data = parsed_data.filter(col("operation") == "c") \
                    .select(
                    col("after_customerId").alias("customerId"),
                    col("after_customerFName").alias("customerFName"),
                    col("after_customerLName").alias("customerLName"),
                    col("after_customerEmail").alias("customerEmail"),
                    col("after_customerPassword").alias("customerPassword"),
                    col("after_customerStreet").alias("customerStreet"),
                    col("after_customerCity").alias("customerCity"),
                    col("after_customerState").alias("customerState"),
                    col("after_customerZipcode").alias("customerZipcode"),
                    col("timestamp")
                )
                if not insert_data.isEmpty():
                    debug("Inserting data 1")
                    debug(insert_data.show())
                    insert_data.write.format("delta").mode("append").save(minio_output_path)
            continue

        if operation == "c":
            insert_data = parsed_data.filter(col("operation") == "c") \
                .select(
                col("after_customerId").alias("customerId"),
                col("after_customerFName").alias("customerFName"),
                col("after_customerLName").alias("customerLName"),
                col("after_customerEmail").alias("customerEmail"),
                col("after_customerPassword").alias("customerPassword"),
                col("after_customerStreet").alias("customerStreet"),
                col("after_customerCity").alias("customerCity"),
                col("after_customerState").alias("customerState"),
                col("after_customerZipcode").alias("customerZipcode"),
                col("timestamp")
            )

            if not insert_data.isEmpty():
                debug("Inserting data")
                debug(insert_data.show())
                insert_data.write.format("delta").mode("append").save(minio_output_path)

        elif operation == "u":
            update_data = parsed_data.filter(col("operation") == "u") \
                .select(
                col("after_customerId").alias("customerId"),
                col("after_customerFName").alias("customerFName"),
                col("after_customerLName").alias("customerLName"),
                col("after_customerEmail").alias("customerEmail"),
                col("after_customerPassword").alias("customerPassword"),
                col("after_customerStreet").alias("customerStreet"),
                col("after_customerCity").alias("customerCity"),
                col("after_customerState").alias("customerState"),
                col("after_customerZipcode").alias("customerZipcode"),
                col("timestamp")
            )

            if not update_data.isEmpty():
                exists = existing_data.join(
                    update_data.select("customerId"),
                    "customerId",
                    "inner"
                ).count() > 0

                if exists:
                    debug("Updating data")
                    debug(update_data.show())
                    delta_table.alias("target").merge(
                        update_data.alias("source"),
                        "target.customerId = source.customerId"
                    ).whenMatchedUpdateAll().execute()

        elif operation == "d":
            delete_data = parsed_data.filter(col("operation") == "d") \
                .select(col("before_customerId").alias("customerId"))

            if not delete_data.isEmpty():
                exists = existing_data.join(
                    delete_data,
                    "customerId",
                    "inner"
                ).count() > 0

                if exists:
                    debug("Deleting data")
                    debug(delete_data.show())
                    delta_table.alias("target").merge(
                        delete_data.alias("source"),
                        "target.customerId = source.customerId"
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