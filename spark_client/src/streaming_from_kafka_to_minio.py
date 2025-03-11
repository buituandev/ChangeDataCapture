import os
import json

from pyspark.errors import AnalysisException
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, IntegerType, LongType, FloatType, DoubleType, StringType, StructField, \
    BinaryType, DecimalType, BooleanType
from pyspark.sql import SparkSession, Row
from delta.tables import DeltaTable
from config_manager import ConfigManager

# region Spark Configuration
config_manager = ConfigManager("/opt/src/config.json")
accessKeyId = config_manager.get("s3_config", "access_key_id")
secretAccessKey = config_manager.get("s3_config", "secret_access_key")
minio_endpoint = config_manager.get("s3_config", "endpoint")
minio_output_path = config_manager.get("delta_config", "output_path")
checkpoint_dir = config_manager.get("delta_config", "checkpoint_dir")
table = config_manager.get("kafka_config", "topic")
cache_schema_path = config_manager.get("cache_config", "schema_path")
cache_field_info_path = config_manager.get("cache_config", "field_info_path")
cache_sql_history_path = config_manager.get("cache_config", "sql_history_path")
process_time = config_manager.get("processing_config", "process_time")

# Global variables
cached_schema = None
cached_field_info = None
select_cols = None
ordered_fields = None
future_data = None
is_halfway = False
existing_data = None

spark = SparkSession.builder \
    .appName("Spark x MinIO") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.0,io.delta:delta-core_2.12:2.4.0") \
    .config("spark.hadoop.fs.s3a.access.key", accessKeyId) \
    .config("spark.hadoop.fs.s3a.secret.key", secretAccessKey) \
    .config("spark.hadoop.fs.s3a.path.style.access", config_manager.get("s3_config", "path_style_access")) \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint) \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", str(config_manager.get("s3_config", "ssl_enabled")).lower()) \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
    .getOrCreate()
spark.sparkContext.setLogLevel('ERROR')


# endregion

# region Dynamic Schema Generation
def get_spark_type(debezium_type):
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
    return type_mapping.get(debezium_type, StringType())


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
        spark_type = get_spark_type(field['type'])
        fields.append(StructField(field['name'], spark_type, field['optional']))
    return StructType(fields)


def create_dynamic_schema(data_json):
    field_info = get_schema_info_from_debezium(data_json)
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
    return schema, data_json, field_info


def generate_select_statements(schema, field_info):
    select_columns = [
        col("data.payload.op").alias("operation"),
        col("data.payload.ts_ms").alias("timestamp")
    ]

    for field in field_info:
        field_name = field['name']
        select_columns.extend([
            col(f"data.payload.before.{field_name}").alias(f"before_{field_name}"),
            col(f"data.payload.after.{field_name}").alias(f"after_{field_name}")
        ])

    return select_columns, [f['name'] for f in field_info]


def format_sql_value(value, data_type):
    """Format a value correctly for SQL based on its type."""
    if value is None or str(value).upper() == 'NONE':
        return "NULL"
    elif data_type in ['int32', 'int64', 'float', 'double', 'decimal']:
        return str(value)
    else:
        escaped_value = str(value).replace("'", "''")
        return f"'{escaped_value}'"


# endregion

# region Cache Schema
def save_cached_schema(schema, field_info):
    schema_json = schema.json()
    with open(cache_schema_path, "w") as f:
        f.write(schema_json)
    with open(cache_field_info_path, "w") as f:
        f.write(json.dumps(field_info))


def load_cached_schema():
    with open(cache_schema_path, "r") as f:
        schema_json = f.read()
    schema = StructType.fromJson(json.loads(schema_json))
    with open(cache_field_info_path, "r") as f:
        field_info = json.loads(f.read())
    return schema, field_info


def is_cached_schema():
    return os.path.exists(cache_schema_path) and os.path.exists(cache_field_info_path)


# endregion

# region Batch Processing
def process_batch(batch_df, batch_id, key_column_name='id', time_data='1 minute'):
    global cached_schema, cached_field_info, select_cols, ordered_fields, future_data, is_halfway, existing_data

    if batch_df.isEmpty():
        return

    if is_cached_schema() and cached_schema is None:
        cached_schema, cached_field_info = load_cached_schema()

    if not cached_schema:
        data_json = batch_df.first()["value"]
        cached_schema, _, cached_field_info = create_dynamic_schema(data_json)
        save_cached_schema(cached_schema, cached_field_info)

    parsed_batch = batch_df.select(from_json(col("value"), cached_schema).alias("data"))
    if not select_cols or not ordered_fields:
        select_cols, ordered_fields = generate_select_statements(cached_schema, cached_field_info)

    if key_column_name not in ordered_fields:
        raise ValueError(f"Key column '{key_column_name}' not found in schema fields: {ordered_fields}")

    parsed_data = parsed_batch.select(select_cols)
    sorted_events = parsed_data.orderBy("timestamp")

    for event in sorted_events.collect():
        operation = event["operation"]
        try:
            existing_data = spark.read.format("delta").load(minio_output_path)
            delta_table = DeltaTable.forPath(spark, minio_output_path)
        except AnalysisException:
            if operation == "c":
                initial_insert_operation_processing(event, ordered_fields, key_column_name)
            continue

        if operation == "c":
            insert_operation_processing(event, ordered_fields, delta_table, key_column_name)
        elif operation == "u":
            update_operation_processing(event, ordered_fields, delta_table, key_column_name)
        elif operation == "d":
            delete_operation_processing(event, ordered_fields, delta_table, key_column_name)

    try:
        existing_data = spark.read.format("delta").load(minio_output_path)
        print(f"Data count {existing_data.count()}")
    except AnalysisException:
        pass


def initial_insert_operation_processing(event, fields_ordered, key_column_name):
    schema_fields = []
    row_data = {}
    for field in fields_ordered:
        field_value = event[f"after_{field}"]
        row_data[field] = field_value
        
        field_type = None
        for info in cached_field_info:
            if info['name'] == field:
                field_type = get_spark_type(info['type'])
                break
        
        if field_type is None:
            field_type = StringType()
            
        schema_fields.append(StructField(field, field_type, True))
    
    row_data["timestamp"] = event["timestamp"]
    schema_fields.append(StructField("timestamp", LongType(), True))
    
    explicit_schema = StructType(schema_fields)
    print('Inserting record:', row_data[key_column_name])
    
    insert_df = spark.createDataFrame([row_data], schema=explicit_schema)
    insert_df.write.format("delta").mode("append").save(minio_output_path)


def insert_operation_processing(event, fields_ordered, delta_table, key_column_name):
    schema_fields = []
    row_data = {}
    
    for field in fields_ordered:
        field_value = event[f"after_{field}"]
        row_data[field] = field_value
        
        field_type = None
        for info in cached_field_info:
            if info['name'] == field:
                field_type = get_spark_type(info['type'])
                break
        
        if field_type is None:
            field_type = StringType()
            
        schema_fields.append(StructField(field, field_type, True))
    
    row_data["timestamp"] = event["timestamp"]
    schema_fields.append(StructField("timestamp", LongType(), True))
    
    explicit_schema = StructType(schema_fields)
    print('Inserting record:', row_data[key_column_name])
    
    insert_df = spark.createDataFrame([row_data], schema=explicit_schema)
    
    delta_table.alias("target").merge(
        insert_df.alias("source"),
        f"target.{key_column_name} = source.{key_column_name}"
    ).whenNotMatchedInsertAll().execute()


def update_operation_processing(event, fields_ordered, delta_table, key_column_name):
    schema_fields = []
    row_data = {}
    
    for field in fields_ordered:
        field_value = event[f"after_{field}"]
        row_data[field] = field_value
        
        field_type = None
        for info in cached_field_info:
            if info['name'] == field:
                field_type = get_spark_type(info['type'])
                break
        
        if field_type is None:
            field_type = StringType()
            
        schema_fields.append(StructField(field, field_type, True))
    
    explicit_schema = StructType(schema_fields)
    print('Updating record:', row_data[key_column_name])
    
    update_df = spark.createDataFrame([row_data], schema=explicit_schema)
    
    delta_table.alias("target").merge(
        update_df.alias("source"),
        f"target.{key_column_name} = source.{key_column_name}"
    ).whenMatchedUpdate(
        condition=None,
        set={field: f"source.{field}" for field in fields_ordered}
    ).execute()


def delete_operation_processing(event, fields_ordered, delta_table, key_column_name):
    key_value = event[f"before_{key_column_name}"]
    
    key_type = None
    for info in cached_field_info:
        if info['name'] == key_column_name:
            key_type = get_spark_type(info['type'])
            break
    
    if key_type is None:
        key_type = StringType()
        
    schema = StructType([StructField(key_column_name, key_type, False)])
    
    print('Deleting record:', key_value)
    delete_data = spark.createDataFrame([(key_value,)], schema=schema)
    
    delta_table.alias("target").merge(
        delete_data.alias("source"),
        f"target.{key_column_name} = source.{key_column_name}"
    ).whenMatchedDelete().execute()
# endregion

# region Application
def run_stream():
    # Get latest config values
    config = config_manager.get_config()
    kafka_servers = config["kafka_config"]["bootstrap_servers"]
    topic = config["kafka_config"]["topic"]
    fail_on_data_loss = config["kafka_config"]["fail_on_data_loss"]
    key_column = config["processing_config"]["key_column"]
    process_time_val = config["processing_config"]["process_time"]
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_servers) \
        .option("subscribe", topic) \
        .option("failOnDataLoss", str(fail_on_data_loss).lower()) \
        .load() \
        .selectExpr("CAST(value AS STRING) as value")

    query = df.writeStream \
        .foreachBatch(
        lambda dataframe, b_id: process_batch(dataframe, id, key_column_name=key_column)) \
        .option("checkpointLocation", config["delta_config"]["checkpoint_dir"]) \
        .trigger(processingTime=process_time_val) \
        .start()

    query.awaitTermination()


if __name__ == "__main__":
    run_stream()
# endregion
