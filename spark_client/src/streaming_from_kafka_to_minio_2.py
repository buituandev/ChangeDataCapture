import os
import json
import time

from pyspark.errors import AnalysisException
from pyspark.sql.functions import from_json, col, from_unixtime, window
from pyspark.sql.types import StructType, IntegerType, LongType, FloatType, DoubleType, StringType, StructField, BinaryType, DecimalType, BooleanType
from pyspark.sql import SparkSession
from delta.tables import DeltaTable
from datetime import datetime, timedelta
from croniter import croniter

#region Spark Configuration
accessKeyId = '12345678'
secretAccessKey = '12345678'
minio_output_path = "s3a://change-data-capture/customers-delta"
checkpoint_dir = "s3a://change-data-capture/checkpoint"
table = "dbserver2.public.links"
cache_schema_path = "/opt/src/schema.json"
cache_field_info_path = "/opt/src/field_info.json"
cache_sql_history_path = "/opt/src/sql_history.csv"
cached_schema = None
cached_field_info = None
select_cols = None
ordered_fields = None
future_data = None
is_halfway = False

spark = SparkSession.builder \
    .appName("Spark x MinIO") \
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
#endregion

#region Dynamic Schema Generation
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
#endregion

#region Cache Schema
def operation_to_sql_history(time_windows, sql):
    if not os.path.exists(cache_sql_history_path):
        with open(cache_sql_history_path, "w") as f:
            f.write("time_windows;sql\n")
    with open(cache_sql_history_path, "a") as f:
        f.write(f"{time_windows};{sql}\n")
            
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
#endregion

#region Batch Processing
def process_batch(batch_df, batch_id, key_column_name='id', time_data = '1 minute'):
    global cached_schema, cached_field_info, select_cols, ordered_fields, future_data, is_halfway

    if batch_df.isEmpty():
        if future_data is None:
            return
    
    if is_cached_schema() and cached_schema is None:
        cached_schema, cached_field_info = load_cached_schema()
    
    if not cached_schema:
        data_json = batch_df.first()["value"]
        cached_schema, _, cached_field_info = create_dynamic_schema(data_json)
        save_cached_schema(cached_schema, cached_field_info)
    
    if not batch_df.isEmpty():
        parsed_batch = batch_df.select(from_json(col("value"), cached_schema).alias("data"))
        if not select_cols or not ordered_fields:
            select_cols, ordered_fields = generate_select_statements(cached_schema, cached_field_info)

        if key_column_name not in ordered_fields:
            raise ValueError(f"Key column '{key_column_name}' not found in schema fields: {ordered_fields}")

        parsed_data = parsed_batch.select(select_cols) \
            .withColumn("event_time", from_unixtime(col("timestamp") / 1000))
            
        windowed_data = parsed_data \
        .withColumn("window_start", window(col("event_time"), time_data).getField("start")) \
        .withColumn("window_end", window(col("event_time"), time_data).getField("end"))
            
        window_groups = windowed_data.select("window_start", "window_end").distinct().collect()
    else:
        window_groups = future_data.select("window_start", "window_end").distinct().collect()
    
    for window_group in window_groups:
        window_start = window_group["window_start"]
        window_end = window_group["window_end"]

        window_batch = windowed_data.filter(
            (col("window_start") == window_start) & 
            (col("window_end") == window_end)
        )
        
        print(f"Processing window: {window_start} to {window_end}")
        
        current_time = datetime.now()
        if(window_end > current_time):
            if future_data is None:
                future_data = window_batch
            is_halfway = True
            return
        else:
            if future_data is not None and not is_halfway:
                future_data = None
        
        if future_data is not None:
            window_batch = future_data.union(window_batch)
            future_data = None
        
        future_data = windowed_data.filter(
            col("event_time") >= window_end
        )
        window_batch.show()

        for op_type in window_batch.select("operation").distinct().collect():
            operation = op_type["operation"]
            try:
                existing_data = spark.read.format("delta").load(minio_output_path)
                existing_data.show()
                delta_table = DeltaTable.forPath(spark, minio_output_path)
            except AnalysisException:
                if operation == "c":
                    insert_operation_processing(ordered_fields, window_batch)
                continue

            if operation == "c":
                insert_operation_processing(ordered_fields, window_batch)

            elif operation == "u":
                update_operation_processing(ordered_fields, window_batch, delta_table, key_column_name)

            elif operation == "d":
                delete_operation_processing(ordered_fields, window_batch, delta_table, key_column_name)
        
        if existing_data is not None:    
            existing_data.show()
        future_data.show()
            
        
def insert_operation_processing(ordered_fields, window_batch):
    insert_cols = [col(f"after_{field}").alias(field) for field in ordered_fields] + [col("timestamp")]
    insert_data = window_batch.filter(col("operation") == "c").select(insert_cols)
    if not insert_data.isEmpty():
        print('Insert data')
        print(insert_data.show())
        fields_str = ", ".join(ordered_fields + ["timestamp"])
        values_list = insert_data.collect()
        
        all_value_sets = []
        for values in values_list:
            formatted_values = []
            for v in values:
                if v is None or str(v).upper() == 'NONE':
                    formatted_values.append("NULL")
                elif isinstance(v, (int, float)):
                    formatted_values.append(str(v))
                else:
                    escaped_str = str(v).replace("'", "''")
                    formatted_values.append(f"'{escaped_str}'")
                    
            value_set = "(" + ", ".join(formatted_values) + ")"
            all_value_sets.append(value_set)
            
        batch_sql = f"INSERT INTO {table} ({fields_str}) VALUES {', '.join(all_value_sets)}"
        operation_to_sql_history("none", batch_sql)
        insert_data.write.format("delta").mode("append").save(minio_output_path)

def update_operation_processing(ordered_fields, window_batch, delta_table, key_column_name):
    update_cols = [col(f"after_{field}").alias(field) for field in ordered_fields] + [col("timestamp")]
    update_data = window_batch.filter(col("operation") == "u").select(update_cols)

    if not update_data.isEmpty():
        print('Updating data')
        print(update_data.show())
        values_list = update_data.collect()
        
        # Get field type for key column early
        key_field_type = next((f['type'] for f in cached_field_info if f['name'] == key_column_name), 'string')
        
        fields_to_update = ordered_fields.copy()
        updates = {}
        key_values = []
        
        for values in values_list:
            str_values = [str(v) for v in values]
            key_value = str_values[ordered_fields.index(key_column_name)]
            key_values.append(key_value)
            
            for i, field in enumerate(fields_to_update):
                if field != key_column_name:
                    if field not in updates:
                        updates[field] = {}
                    
                    value = str_values[i]
                    if value.upper() == 'NONE':
                        updates[field][key_value] = "NULL"
                    else:
                        escaped_value = value.replace("'", "''")
                        updates[field][key_value] = escaped_value
        
        # Build CASE statements with proper key formatting
        case_statements = []
        for field, field_updates in updates.items():
            case_parts = []
            for key, value in field_updates.items():
                # Format key correctly based on its type
                formatted_key = format_sql_value(key, key_field_type)
                
                if value == "NULL":
                    case_parts.append(f"WHEN {key_column_name} = {formatted_key} THEN {value}")
                else:
                    case_parts.append(f"WHEN {key_column_name} = {formatted_key} THEN '{value}'")
            
            case_statement = f"{field} = CASE {' '.join(case_parts)} ELSE {field} END"
            case_statements.append(case_statement)
        
        if case_statements:
            keys_in_clause = ", ".join([format_sql_value(k, key_field_type) for k in key_values])
            batch_sql = f"UPDATE {table} SET {', '.join(case_statements)} WHERE {key_column_name} IN ({keys_in_clause})"
            operation_to_sql_history("none", batch_sql)
            
        delta_table.alias("target").merge(
            update_data.alias("source"),
            f"target.{key_column_name} = source.{key_column_name}"
        ).whenMatchedUpdateAll().execute()

def delete_operation_processing(ordered_fields, window_batch, delta_table, key_column_name):
    delete_data = window_batch.filter(col("operation") == "d") \
                .select(col(f"before_{key_column_name}").alias(key_column_name))
    delete_cols = [col(f"before_{field}").alias(field) for field in ordered_fields] + [col("timestamp")]
    delete_data_time_event = window_batch.filter(col("operation") == "d").select(delete_cols)

    if not delete_data.isEmpty():
        print('Deleting data')
        print(delete_data.show())
        values_list = delete_data_time_event.collect()
        key_values = []
        
        # Get field type for proper formatting
        key_field_type = next((f['type'] for f in cached_field_info if f['name'] == key_column_name), 'string')

        for values in values_list:
            key_value = str(values[0])
            key_values.append(key_value)
        
        if key_values:
            # Format keys properly based on type
            keys_in_clause = ", ".join([format_sql_value(k, key_field_type) for k in key_values])
            batch_sql = f"DELETE FROM {table} WHERE {key_column_name} IN ({keys_in_clause})"
            operation_to_sql_history("none", batch_sql)
               
        delta_table.alias("target").merge(
            delete_data.alias("source"),
            f"target.{key_column_name} = source.{key_column_name}"
        ).whenMatchedDelete().execute()
#endregion

#region Application
def run_stream(process_time):    
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", table) \
        .option("failOnDataLoss", "false") \
        .load() \
        .selectExpr("CAST(value AS STRING) as value")

    query = df.writeStream \
        .foreachBatch(lambda dataframe, b_id: process_batch(dataframe, id, key_column_name="customerId", time_data=process_time)) \
        .option("checkpointLocation", checkpoint_dir) \
        .trigger(processingTime=process_time) \
        .start()

    query.awaitTermination()

def _calculate_processing_window(cron_expression: str) -> int:
    """Calculate processing window time."""
    now = datetime.now()
    cron = croniter(cron_expression, now)
    next_run = cron.get_next(datetime)
    following_run = cron.get_next(datetime)
    
    interval = (following_run - next_run).total_seconds()
    return int(interval)

def get_time_until_next_cron(cron_expression: str) -> tuple:
    """
    Calculate time until next cron trigger and the interval between triggers.
    Returns (seconds_until_next_trigger, interval_seconds)
    """
    now = datetime.now()
    cron = croniter(cron_expression, now)
    next_run = cron.get_next(datetime)
    following_run = cron.get_next(datetime)
    
    seconds_until_next = (next_run - now).total_seconds()
    interval_seconds = (following_run - next_run).total_seconds()
    
    return seconds_until_next, int(interval_seconds)

if __name__ == "__main__":
    cron_expression = "*/1 * * * *"
    delay_seconds, interval_seconds = get_time_until_next_cron(cron_expression)
    
    print(f"Current time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Next scheduled run: {(datetime.now() + timedelta(seconds=delay_seconds)).strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Waiting {delay_seconds:.1f} seconds until next scheduled time...")

    MAX_TEST_DELAY = 60  # 60 seconds max for testing
    actual_delay = min(delay_seconds, MAX_TEST_DELAY) if delay_seconds > 0 else 0
    
    if actual_delay > 0:
        print(f"Sleeping for {actual_delay:.1f} seconds...")
        time.sleep(actual_delay)
        print(f"Waking up at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    process_time = f"{interval_seconds} seconds"
    print(f"Setting processing interval to {process_time}")
    
    run_stream(process_time)
#endregion