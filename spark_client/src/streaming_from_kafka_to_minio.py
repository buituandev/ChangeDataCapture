import os
import json

from pyspark.errors import AnalysisException
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, IntegerType, LongType, FloatType, DoubleType, StringType, StructField, \
    BinaryType, DecimalType, BooleanType
from pyspark.sql import SparkSession
from delta.tables import DeltaTable

# region Spark Configuration
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
existing_data = None

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
    
    # Sort all operations by timestamp to maintain correct order
    sorted_data = parsed_data.orderBy("timestamp")

    try:
        existing_data = spark.read.format("delta").load(minio_output_path)
        delta_table = DeltaTable.forPath(spark, minio_output_path)
    except AnalysisException:
        # Handle first-time table creation
        insert_data = sorted_data.filter(col("operation") == "c")
        if not insert_data.isEmpty():
            insert_cols = [col(f"after_{field}").alias(field) for field in ordered_fields] + [col("timestamp")]
            insert_data_selected = insert_data.select(insert_cols)
            insert_data_selected.write.format("delta").mode("append").save(minio_output_path)
        return

    for row in sorted_data.collect():
        operation = row["operation"]
        
        if operation == "c":
            process_single_insert(row, ordered_fields, delta_table, key_column_name)
        elif operation == "u":
            process_single_update(row, ordered_fields, delta_table, key_column_name)
        elif operation == "d":
            process_single_delete(row, delta_table, key_column_name)

    print('Data count: ')
    print(spark.read.format("delta").load(minio_output_path).count())


def process_single_insert(row, fields_ordered, delta_table, key_column_name):
    # Create DataFrame with just this single row
    values = {}
    for field in fields_ordered:
        values[field] = row[f"after_{field}"]
    values["timestamp"] = row["timestamp"]
    
    # Create a DataFrame from the single row
    insert_data = spark.createDataFrame([values])
    
    print('Insert data')
    print(insert_data.show())

    # Generate SQL for logging
    fields_str = ", ".join(fields_ordered + ["timestamp"])
    formatted_values = []
    for field in fields_ordered + ["timestamp"]:
        v = values[field]
        if v is None or str(v).upper() == 'NONE':
            formatted_values.append("NULL")
        elif isinstance(v, (int, float)):
            formatted_values.append(str(v))
        else:
            escaped_str = str(v).replace("'", "''")
            formatted_values.append(f"'{escaped_str}'")
            
    value_set = "(" + ", ".join(formatted_values) + ")"
    batch_sql = f"INSERT INTO {table} ({fields_str}) VALUES {value_set}"
    operation_to_sql_history("none", batch_sql)
    
    # Execute the merge
    delta_table.alias("target").merge(
        insert_data.alias("source"),
        f"target.{key_column_name} = source.{key_column_name}"
    ).whenNotMatchedInsertAll().execute()


def process_single_update(row, fields_ordered, delta_table, key_column_name):
    # Create DataFrame with just this single row
    values = {}
    for field in fields_ordered:
        values[field] = row[f"after_{field}"]
    values["timestamp"] = row["timestamp"]
    
    # Create a DataFrame from the single row
    update_data = spark.createDataFrame([values])
    
    print('Updating data')
    print(update_data.show())

    # Generate SQL for logging
    key_field_type = next((f['type'] for f in cached_field_info if f['name'] == key_column_name), 'string')
    key_value = str(values[key_column_name])
    formatted_key = format_sql_value(key_value, key_field_type)
    
    set_clauses = []
    for field in fields_ordered:
        if field != key_column_name:
            value = values[field]
            if value is None or str(value).upper() == 'NONE':
                set_clauses.append(f"{field} = NULL")
            else:
                escaped_value = str(value).replace("'", "''")
                set_clauses.append(f"{field} = '{escaped_value}'")
    
    batch_sql = f"UPDATE {table} SET {', '.join(set_clauses)} WHERE {key_column_name} = {formatted_key}"
    operation_to_sql_history("none", batch_sql)
    
    # Execute the merge
    delta_table.alias("target").merge(
        update_data.alias("source"),
        f"target.{key_column_name} = source.{key_column_name}"
    ).whenMatchedUpdate(
        condition=None,
        set={field: f"source.{field}" for field in fields_ordered}
    ).execute()


def process_single_delete(row, delta_table, key_column_name):
    # Create DataFrame with just this single row
    key_value = row[f"before_{key_column_name}"]
    delete_data = spark.createDataFrame([(key_value,)], [key_column_name])
    
    print('Deleting data')
    print(delete_data.show())

    # Generate SQL for logging
    key_field_type = next((f['type'] for f in cached_field_info if f['name'] == key_column_name), 'string')
    formatted_key = format_sql_value(key_value, key_field_type)
    
    batch_sql = f"DELETE FROM {table} WHERE {key_column_name} = {formatted_key}"
    operation_to_sql_history("none", batch_sql)
    
    # Execute the delete
    delta_table.alias("target").merge(
        delete_data.alias("source"),
        f"target.{key_column_name} = source.{key_column_name}"
    ).whenMatchedDelete().execute()

# endregion


# region Application
def run_stream(time_process):
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", table) \
        .option("failOnDataLoss", "false") \
        .load() \
        .selectExpr("CAST(value AS STRING) as value")

    query = df.writeStream \
        .foreachBatch(
        lambda dataframe, b_id: process_batch(dataframe, id, key_column_name="customerId", time_data=time_process)) \
        .option("checkpointLocation", checkpoint_dir) \
        .trigger(processingTime=time_process) \
        .start()

    query.awaitTermination()


if __name__ == "__main__":
    process_time = "1 minute"
    run_stream(process_time)
# endregion
