"""
Spark Structured Streaming application for Change Data Capture (CDC) processing.

This module implements a streaming ETL pipeline that:
1. Reads CDC events from Kafka in Debezium format
2. Dynamically discovers and caches the schema
3. Processes upsert, and delete operations
4. Applies changes to a target Delta table in MinIO

The application handles schema evolution and provides resilient processing
with checkpoint management and configurable processing intervals.
"""

import os
import json

from pyspark.errors import AnalysisException
from pyspark.sql.functions import from_json, col, max_by, struct, when
from pyspark.sql.types import StructType, IntegerType, LongType, FloatType, DoubleType, StringType, StructField, \
    BinaryType, DecimalType, BooleanType
from pyspark.sql import SparkSession
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
    """
    Convert Debezium data type to corresponding Spark SQL type.
    
    Args:
        debezium_type (str): The Debezium data type name
        
    Returns:
        pyspark.sql.types.DataType: The corresponding Spark SQL data type
    """
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
    """
    Convert Debezium data type to corresponding Spark SQL type.
    
    Args:
        debezium_type (str): The Debezium data type name
        
    Returns:
        pyspark.sql.types.DataType: The corresponding Spark SQL data type
    """
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
    """
    Create a Spark StructType from Debezium field information.
    
    Args:
        field_info (list): List of dictionaries with field information
        
    Returns:
        pyspark.sql.types.StructType: Spark schema for record structure
    """
    fields = []
    for field in field_info:
        spark_type = get_spark_type(field['type'])
        fields.append(StructField(field['name'], spark_type, field['optional']))
    return StructType(fields)

def create_dynamic_schema(data_json):
    """
    Create a complete Spark schema from a Debezium JSON payload.
    
    Analyzes the Debezium format and constructs a nested schema that includes
    the 'before' and 'after' record states along with metadata fields.
    
    Args:
        data_json (str): JSON string containing a Debezium event
        
    Returns:
        tuple: (
            pyspark.sql.types.StructType: Complete schema for Debezium data,
            str: Original JSON string,
            list: List of field information dictionaries
        )
    """
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
    """
    Generate column selection expressions for processing CDC data.
    
    Creates a list of column selections that extract metadata and 'before'/'after'
    record states from the nested Debezium format.
    
    Args:
        schema (pyspark.sql.types.StructType): Spark schema for Debezium data
        field_info (list): List of dictionaries with field information
        
    Returns:
        tuple: (
            list: Column expressions for selecting data,
            list: Ordered list of field names
        )
    """
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
    """
    Format a value correctly for SQL based on its data type.
    
    Args:
        value: The value to format
        data_type (str): The Debezium data type
        
    Returns:
        str: Properly formatted SQL value representation
    """
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
    """
    Save schema and field information to disk for future use.
    
    Args:
        schema (pyspark.sql.types.StructType): Spark schema to save
        field_info (list): Field information dictionaries
    """
    schema_json = schema.json()
    with open(cache_schema_path, "w") as f:
        f.write(schema_json)
    with open(cache_field_info_path, "w") as f:
        f.write(json.dumps(field_info))


def load_cached_schema():
    """
    Load previously cached schema and field information from disk.
    
    Returns:
        tuple: (
            pyspark.sql.types.StructType: Loaded schema,
            list: Loaded field information
        )
    """
    with open(cache_schema_path, "r") as f:
        schema_json = f.read()
    schema = StructType.fromJson(json.loads(schema_json))
    with open(cache_field_info_path, "r") as f:
        field_info = json.loads(f.read())
    return schema, field_info


def is_cached_schema():
    """
    Check if cached schema files exist.
    
    Returns:
        bool: True if cached schema exists, False otherwise
    """
    return os.path.exists(cache_schema_path) and os.path.exists(cache_field_info_path)
# endregion

# region Batch Processing
def process_batch(batch_df, batch_id, key_column_name='id'):
    """
    Process a batch of CDC events from Kafka.
    
    This is the core processing function that:
    1. Initializes or loads the schema
    2. Parses the Debezium format
    3. Aggregates operations by key to get the latest state
    4. Applies changes to the target Delta table
    
    Args:
        batch_df (pyspark.sql.DataFrame): Batch of CDC events from Kafka
        batch_id (int): The batch identifier
        key_column_name (str, optional): Primary key column name. Defaults to 'id'.
    """
    global cached_schema, cached_field_info, select_cols, ordered_fields, process_time
    
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
    parsed_data = parsed_data.filter(col("operation").isNotNull())
    # parsed_data.select("operation", 
    #                    f"before_{key_column_name}", 
    #                    f"after_{key_column_name}").show(truncate=False)
    
    parsed_data = parsed_data.withColumn(
        "key_value", 
        when(col("operation") == "d", col(f"before_{key_column_name}"))
        .when(col(f"after_{key_column_name}").isNotNull(), col(f"after_{key_column_name}"))
    )
        
    aggregated_batch = parsed_data.groupBy("key_value").agg(
        max_by(
            struct(
                "operation", 
                "timestamp",
                *[col(f"after_{field}") for field in ordered_fields],
                *[col(f"before_{field}") for field in ordered_fields]
            ), 
            "timestamp"
        ).alias("latest")
    )
    
    final_df = aggregated_batch.select(
        col("key_value"),
        col("latest.operation").alias("operation"),
        col("latest.timestamp").alias("timestamp"),
        *[col(f"latest.after_{field}").alias(f"after_{field}") for field in ordered_fields],
        *[col(f"latest.before_{field}").alias(f"before_{field}") for field in ordered_fields]
    )
        
    table_exists = False
    try:
        delta_table = DeltaTable.forPath(spark, minio_output_path)
        table_exists = True
    except AnalysisException:
        table_exists = False
    
    if not table_exists:
        creates = final_df.filter(col("operation") == "c")
        if creates.isEmpty():
            # print("No create operations to initialize table")
            return
        
        initial_data = creates.select(
            *[col(f"after_{field}").alias(field) for field in ordered_fields],
            col("timestamp")
        )
        
        # print(f"Creating initial table with {initial_data.count()} records")
        initial_data.write.format("delta").mode("append").save(minio_output_path)
        
        delta_table = DeltaTable.forPath(spark, minio_output_path)
        table_exists = True
        
        final_df = final_df.filter(col("operation") != "c")
    
    if table_exists and not final_df.isEmpty():
        cu_ops = final_df.filter(col("operation").isin(["c", "u"]))
        if not cu_ops.isEmpty():
            update_df = cu_ops.select(
                *[col(f"after_{field}").alias(field) for field in ordered_fields],
                col("timestamp")
            )
            
            # print(f"Processing {update_df.count()} upserts in one merge")
            delta_table.alias("target").merge(
                update_df.alias("source"),
                f"target.{key_column_name} = source.{key_column_name}"
            ).whenMatchedUpdate(
                condition=None,
                set={**{field: f"source.{field}" for field in ordered_fields}, 
                     "timestamp": "source.timestamp"}
            ).whenNotMatchedInsertAll().execute()
        
        d_ops = final_df.filter(col("operation") == "d")
        if not d_ops.isEmpty():
            delete_df = d_ops.select(
                col(f"before_{key_column_name}").alias(key_column_name)
            )
            
            # print(f"Processing {delete_df.count()} deletes in one merge")
            delta_table.alias("target").merge(
                delete_df.alias("source"),
                f"target.{key_column_name} = source.{key_column_name}"
            ).whenMatchedDelete().execute()
    
    # Use this only for debugging purposes
    if table_exists:
        final_count = spark.read.format("delta").load(minio_output_path).count()
        print(f"Final data count: {final_count}")
    
    new_process_time = config_manager.get("processing_config", "process_time")
    if new_process_time != process_time:
        # Use this only for debugging purposes
        # print(f"Process time config changed from {process_time} to {new_process_time}")
        process_time = new_process_time
# endregion

# region Application
def run_stream():
    """
    Run the Spark Structured Streaming application.
    
    Sets up and starts the streaming query that:
    1. Reads CDC events from Kafka
    2. Processes batches using the process_batch function
    3. Handles configuration changes and graceful shutdown
    4. Manages checkpointing for fault tolerance
    
    The function runs indefinitely until interrupted.
    """
    global process_time
    
    # Get initial config values
    config = config_manager.get_config()
    kafka_servers = config["kafka_config"]["bootstrap_servers"]
    topic = config["kafka_config"]["topic"]
    fail_on_data_loss = config["kafka_config"]["fail_on_data_loss"]
    key_column = config["processing_config"]["key_column"]
    process_time = config["processing_config"]["process_time"]
    
    restart_required = [False]
    batch_in_progress = [False]
    
    def create_query():
        """
        Create and return a streaming query.
        
        Returns:
            pyspark.sql.streaming.StreamingQuery: The running streaming query
        """
        df = spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", kafka_servers) \
            .option("subscribe", topic) \
            .option("failOnDataLoss", str(fail_on_data_loss).lower()) \
            .load() \
            .selectExpr("CAST(value AS STRING) as value")
        
        def managed_batch_processing(dataframe, b_id):
            current_process_time = process_time
            
            batch_in_progress[0] = True
            
            try:
                process_batch(dataframe, b_id, key_column_name=key_column)
                
                if process_time != current_process_time:
                    # print(f"Process time changed from {current_process_time} to {process_time}")
                    restart_required[0] = True
            finally:
                batch_in_progress[0] = False
        
        return df.writeStream \
            .foreachBatch(managed_batch_processing) \
            .option("checkpointLocation", config["delta_config"]["checkpoint_dir"]) \
            .trigger(processingTime=process_time) \
            .start()
    
    query = create_query()
    # print(f"Started streaming query with process time: {process_time}")
    
    try:
        while True:
            import time
            time.sleep(1)
            
            if restart_required[0] and not batch_in_progress[0]:
                status = query.status
                if status["isTriggerActive"] == False:
                    # print("Restarting query with new process time...")
                    query.stop()
                    query = create_query()
                    restart_required[0] = False
                    # print(f"Query restarted with process time: {process_time}")
    except KeyboardInterrupt:
        # print("Stopping stream processing...")
        # print("Waiting for any in-progress batch to complete before stopping...")
        while batch_in_progress[0]:
            time.sleep(1)
        try:
            if query.isActive:
                query.stop()
        except Exception as e:
            pass


if __name__ == "__main__":
    run_stream()
# endregion