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
cached_schema = None
cached_field_info = None
select_cols = None
ordered_fields = None

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


def get_spark_type(debezium_type, optional=True):
    type_mapping = {
        "int32": IntegerType(),
        "int64": LongType(),
        "float": FloatType(),
        "double": DoubleType(),
        "boolean": BooleanType(),
        "string": StringType(),
        "bytes": BinaryType(),
        "decimal": DecimalType(38, 18),
    }
    spark_type = type_mapping.get(debezium_type, StringType())
    return spark_type if not optional else spark_type

def get_schema_info_from_debezium(json_str):
    data = json.loads(json_str)
    schema = data['schema']
    
    field_info = []
    for field in schema['fields']:
        if field['field'] in ['before', 'after']:
            field_defs = field['fields']
            for f in field_defs:
                field_info.append({
                    'name': f['field'],
                    'type': f['type'],
                    'optional': f.get('optional', True)
                })
            break
            
    return field_info

def create_struct_type_from_debezium(field_info):
    fields = []
    for field in field_info:
        spark_type = get_spark_type(field['type'], field['optional'])
        fields.append(StructField(field['name'], spark_type, field['optional']))
    return StructType(fields)

def create_dynamic_schema(sample_json):
    field_info = get_schema_info_from_debezium(sample_json)
    record_schema = create_struct_type_from_debezium(field_info)
    schema = StructType([
        StructField("schema", StringType(), True),
        StructField("payload", StructType([
            StructField("before", record_schema, True),
            StructField("after", record_schema, True),
            StructField("source", StringType(), True),
            StructField("op", StringType(), True),
            StructField("ts_ms", LongType(), True),
            StructField("transaction", StringType(), True)
        ]), True)
    ])
    return schema, sample_json, field_info

def generate_select_statements(schema, field_info):
    select_cols = [
        col("data.payload.op").alias("operation"),
        col("data.payload.ts_ms").alias("timestamp")
    ]
    
    for field in field_info:
        field_name = field['name']
        select_cols.extend([
            col(f"data.payload.before.{field_name}").alias(f"before_{field_name}"),
            col(f"data.payload.after.{field_name}").alias(f"after_{field_name}")
        ])
    
    return select_cols, [f['name'] for f in field_info]

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

def process_batch(batch_df, batch_id, key_column_name='id'):
    global cached_schema, catched_field_info, select_cols, ordered_fields
    if batch_df.isEmpty():
        return
    if not cached_schema:
        sample_json = batch_df.first()["value"]
        cached_schema, _, cached_field_info = create_dynamic_schema(sample_json)
    
    parsed_batch = batch_df.select(from_json(col("value"), cached_schema).alias("data"))
    if not select_cols or not ordered_fields:
        select_cols, ordered_fields = generate_select_statements(cached_schema, cached_field_info)
    
    if key_column_name not in ordered_fields:
        raise ValueError(f"Key column '{key_column_name}' not found in schema fields: {ordered_fields}")
    
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
                    print('Initializing data')
                    debug(insert_data.show())
                    insert_data.write.format("delta").mode("append").save(minio_output_path)
            continue

        if operation == "c":
            insert_cols = [col(f"after_{field}").alias(field) for field in ordered_fields] + [col("timestamp")]
            insert_data = parsed_data.filter(col("operation") == "c").select(insert_cols)

            if not insert_data.isEmpty():
                debug("Inserting data")
                print('Inserting data')
                debug(insert_data.show())
                insert_data.write.format("delta").mode("append").save(minio_output_path)

        elif operation == "u":
            update_cols = [col(f"after_{field}").alias(field) for field in ordered_fields] + [col("timestamp")]
            update_data = parsed_data.filter(col("operation") == "u").select(update_cols)
            
            if not update_data.isEmpty():
                exists = existing_data.join(
                    update_data.select(key_column_name),
                    key_column_name,
                    "inner"
                ).count() > 0

                if exists:
                    debug("Updating data")
                    print('Updating data')
                    debug(update_data.show())
                    delta_table.alias("target").merge(
                        update_data.alias("source"),
                        f"target.{key_column_name} = source.{key_column_name}"
                    ).whenMatchedUpdateAll().execute()

        elif operation == "d":
            delete_data = parsed_data.filter(col("operation") == "d") \
                .select(col(f"before_{key_column_name}").alias(key_column_name))

            if not delete_data.isEmpty():
                exists = existing_data.join(
                    delete_data,
                    key_column_name,
                    "inner"
                ).count() > 0

                if exists:
                    debug("Deleting data")
                    print('Deleting data')
                    debug(delete_data.show())
                    delta_table.alias("target").merge(
                        delete_data.alias("source"),
                        f"target.{key_column_name} = source.{key_column_name}"
                    ).whenMatchedDelete().execute()

df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "dbserver2.public.links") \
    .option("failOnDataLoss", "false") \
    .load() \
    .selectExpr("CAST(value AS STRING) as value")

query = df.writeStream \
    .foreachBatch(lambda df, id: process_batch(df, id, key_column_name="customerId")) \
    .option("checkpointLocation", checkpoint_dir) \
    .trigger(processingTime='10 seconds') \
    .start()

query.awaitTermination()