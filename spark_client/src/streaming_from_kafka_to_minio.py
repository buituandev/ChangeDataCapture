import os
import json
import pandas as pd
from sqlalchemy import create_engine, text

from pyspark.errors import AnalysisException
from pyspark.sql.functions import from_json, col, max_by, struct, when, lit
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
    global cached_schema, cached_field_info, select_cols, ordered_fields
    
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
    
    # Filter null operations
    parsed_data = parsed_data.filter(col("operation").isNotNull())
    
    parsed_data.select("operation", 
                       f"before_{key_column_name}", 
                       f"after_{key_column_name}").show(truncate=False)
    
    # Then modify the withColumn
    parsed_data = parsed_data.withColumn(
        "key_value", 
        when(col("operation") == "d", col(f"before_{key_column_name}"))
        .when(col(f"after_{key_column_name}").isNotNull(), col(f"after_{key_column_name}"))
    )
    
    # parsed_data.show()
    
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
    
    # final_df.show()
    
    table_exists = False
    try:
        delta_table = DeltaTable.forPath(spark, minio_output_path)
        table_exists = True
    except AnalysisException:
        table_exists = False
    
    # Create initial table if needed
    if not table_exists:
        creates = final_df.filter(col("operation") == "c")
        if creates.isEmpty():
            print("No create operations to initialize table")
            return
        
        # Create initial table with 'create' operations
        initial_data = creates.select(
            *[col(f"after_{field}").alias(field) for field in ordered_fields],
            col("timestamp")
        )
        
        print(f"Creating initial table with {initial_data.count()} records")
        initial_data.write.format("delta").mode("append").save(minio_output_path)
        
        # Now the table exists
        delta_table = DeltaTable.forPath(spark, minio_output_path)
        table_exists = True
        
        # Filter out already processed creates
        final_df = final_df.filter(col("operation") != "c")
    
    # Process all remaining operations in batches
    if table_exists and not final_df.isEmpty():
        # Process creates and updates together
        cu_ops = final_df.filter(col("operation").isin(["c", "u"]))
        if not cu_ops.isEmpty():
            update_df = cu_ops.select(
                *[col(f"after_{field}").alias(field) for field in ordered_fields],
                col("timestamp")
            )
            
            print(f"Processing {update_df.count()} inserts/updates in one merge")
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
            
            print(f"Processing {delete_df.count()} deletes in one merge")
            delta_table.alias("target").merge(
                delete_df.alias("source"),
                f"target.{key_column_name} = source.{key_column_name}"
            ).whenMatchedDelete().execute()
    
    if table_exists:
        final_count = spark.read.format("delta").load(minio_output_path).count()
        print(f"Final data count: {final_count}")
    
    process_time = config_manager.get("processing_config", "process_time")


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

#region Data Validation    
def validate_data_in_minio():
    """Validate that the data in MinIO Delta table matches PostgreSQL source."""
    print("Starting data validation...")
    
    # Get connection details from config
    pg_config = config_manager.get_config().get("postgres_config", {})
    pg_host = pg_config.get("host", "postgres")
    pg_port = pg_config.get("port", 5432)
    pg_user = pg_config.get("user", "postgres")
    pg_password = pg_config.get("password", "postgres")
    pg_database = pg_config.get("database", "postgres")
    pg_schema = pg_config.get("schema", "public")
    pg_table_full = config_manager.get("kafka_config", "topic")
    
    # Extract actual table name from Debezium format (dbserver2.public.links -> links)
    pg_table_parts = pg_table_full.split('.')
    if len(pg_table_parts) >= 3:
        # Format is dbserver.schema.table
        pg_table = pg_table_parts[2]
        pg_schema = pg_table_parts[1]
    elif len(pg_table_parts) == 2:
        # Format is schema.table
        pg_table = pg_table_parts[1]
        pg_schema = pg_table_parts[0]
    else:
        # Just table name
        pg_table = pg_table_full
    
    print(f"Using table: {pg_schema}.{pg_table}")
    
    # Determine key column from config
    key_column = config_manager.get("processing_config", "key_column", "id")
    
    try:
        # Create PostgreSQL connection - fix the connection string to avoid schema ambiguity
        pg_conn_string = f'postgresql+psycopg2://{pg_user}:{pg_password}@{pg_host}:{pg_port}/{pg_database}'
        pg_engine = create_engine(pg_conn_string)
        
        # Test PostgreSQL connection
        with pg_engine.connect() as conn:
            conn.execute(text("SELECT 1")).fetchone()
        print(f"Connected to PostgreSQL database at {pg_host}:{pg_port}")
        
        # Get PostgreSQL data - use text() to properly escape table name
        with pg_engine.connect() as connection:
            query = text(f'SELECT * FROM {pg_schema}."{pg_table}"')
            
            # Execute query and fetch results directly
            result = connection.execute(query)
            pg_data = pd.DataFrame(result.fetchall(), columns=result.keys())
            print(f"Retrieved {len(pg_data)} records from PostgreSQL table {pg_schema}.{pg_table}")
        
        # Get Delta Table data
        delta_path = minio_output_path
        delta_df = spark.read.format("delta").load(delta_path).toPandas()
        print(f"Retrieved {len(delta_df)} records from Delta table at {delta_path}")
        
        # Sort both DataFrames by the key column
        pg_sorted = pg_data.sort_values(by=key_column).reset_index(drop=True)
        delta_sorted = delta_df.sort_values(by=key_column).reset_index(drop=True)
        
        # Check columns (ignoring timestamp in delta)
        pg_cols = set(pg_sorted.columns)
        delta_cols = set(delta_sorted.columns)
        delta_cols.discard('timestamp')  # Ignore timestamp column
        
        missing_in_delta = pg_cols - delta_cols
        extra_in_delta = delta_cols - pg_cols
        
        if missing_in_delta:
            print(f"WARNING: Columns missing in Delta table: {missing_in_delta}")
        if extra_in_delta:
            print(f"WARNING: Extra columns in Delta table: {extra_in_delta}")
        
        # Check keys
        pg_keys = set(pg_sorted[key_column])
        delta_keys = set(delta_sorted[key_column])
        
        missing_keys = pg_keys - delta_keys
        extra_keys = delta_keys - pg_keys
        
        if missing_keys:
            print(f"WARNING: Records missing in Delta table: {missing_keys}")
        if extra_keys:
            print(f"WARNING: Extra records in Delta table: {extra_keys}")
        
        # Compare matching records
        common_keys = pg_keys.intersection(delta_keys)
        print(f"Found {len(common_keys)} matching keys to compare")
        
        mismatches = []
        for key in common_keys:
            pg_row = pg_sorted[pg_sorted[key_column] == key].iloc[0]
            delta_row = delta_sorted[delta_sorted[key_column] == key].iloc[0]
            
            for col in pg_cols.intersection(delta_cols):
                if pd.isna(pg_row[col]) and pd.isna(delta_row[col]):
                    continue  # Both are NaN/None
                elif pg_row[col] != delta_row[col]:
                    mismatches.append((key, col, pg_row[col], delta_row[col]))
                    break  # Only report first mismatch per record
        
        # Print summary
        print("=" * 60)
        print("CDC Validation Summary")
        print("=" * 60)
        print(f"PostgreSQL records: {len(pg_sorted)}")
        print(f"Delta table records: {len(delta_sorted)}")
        print(f"Common records: {len(common_keys)}")
        print(f"Records missing in Delta: {len(missing_keys)}")
        print(f"Extra records in Delta: {len(extra_keys)}")
        print(f"Records with value mismatches: {len(mismatches)}")
        
        # Show mismatch details
        if mismatches:
            print("Mismatch details (showing up to 5):")
            for _, (key, col, pg_val, delta_val) in enumerate(mismatches[:5]):
                print(f"  Key {key}, Column '{col}': PG='{pg_val}', Delta='{delta_val}'")
                
        is_consistent = len(missing_keys) == 0 and len(extra_keys) == 0 and len(mismatches) == 0
        print(f"Data consistent: {is_consistent}")
        print("=" * 60)
        
        return is_consistent
        
    except Exception as e:
        print(f"Validation failed: {str(e)}")
        import traceback
        traceback.print_exc()
        return False
# endregion

# region Application
def run_stream():
    # Get latest config values
    config = config_manager.get_config()
    kafka_servers = config["kafka_config"]["bootstrap_servers"]
    topic = config["kafka_config"]["topic"]
    fail_on_data_loss = config["kafka_config"]["fail_on_data_loss"]
    key_column = config["processing_config"]["key_column"]
    process_time = config["processing_config"]["process_time"]
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
        .trigger(processingTime=process_time) \
        .start()

    query.awaitTermination()


if __name__ == "__main__":
    #run_stream()
    validate_data_in_minio()
# endregion