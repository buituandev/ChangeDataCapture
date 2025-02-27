import os
import json

from pyspark.errors import AnalysisException
from pyspark.sql.functions import from_json, col, from_unixtime, window
from pyspark.sql.types import *
from delta.tables import *
from datetime import datetime
from croniter import croniter

# Initialize Spark Session
accessKeyId = '12345678'
secretAccessKey = '12345678'
minio_output_path = "s3a://change-data-capture/customers-delta"
checkpoint_dir = "s3a://change-data-capture/checkpoint"
table = "dbserver2.public.links"
cached_schema = None
cached_field_info = None
select_cols = None
ordered_fields = None
future_data = None

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


def operation_to_sql_history(time_windows, sql):
    csv_path = "/opt/examples/streaming/sql_history.csv"
    if not os.path.exists(csv_path):
        with open(csv_path, "w") as f:
            f.write("time_windows;sql\n")
    with open(csv_path, "a") as f:
        f.write(f"{time_windows};{sql}\n")
            
def save_cached_schema(schema, field_info):
    schema_json = schema.json()
    with open("/opt/examples/streaming/schema.json", "w") as f:
        f.write(schema_json)
    with open("/opt/examples/streaming/field_info.json", "w") as f:
        f.write(json.dumps(field_info))

def load_cached_schema():
    with open("/opt/examples/streaming/schema.json", "r") as f:
        schema_json = f.read()
    schema = StructType.fromJson(json.loads(schema_json))
    with open("/opt/examples/streaming/field_info.json", "r") as f:
        field_info = json.loads(f.read())
    return schema, field_info

def is_cached_schema():
    return os.path.exists("/opt/examples/streaming/schema.json") and os.path.exists("/opt/examples/streaming/field_info.json")


def process_batch(batch_df, batch_id, key_column_name='id', time_data = '1 minute'):
    global cached_schema, cached_field_info, select_cols, ordered_fields, future_data

    if batch_df.isEmpty():
        if future_data is None:
            return
    
    if is_cached_schema():
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
        
        if future_data is not None:
            window_batch = window_batch.union(future_data)
            future_data = None
        
        future_data = windowed_data.filter(
            col("event_time") >= window_end
        )

    for op_type in window_batch.select("operation").distinct().collect():
        operation = op_type["operation"]
        event_time = window_batch.select("event_time").first()["event_time"]
        try:
            existing_data = spark.read.format("delta").load(minio_output_path)
            delta_table = DeltaTable.forPath(spark, minio_output_path)
        except AnalysisException:
            if operation == "c":
                insert_cols = [col(f"after_{field}").alias(field) for field in ordered_fields] + [col("timestamp")]
                insert_data = window_batch.filter(col("operation") == "c").select(insert_cols)
                if not insert_data.isEmpty():
                    print('Initializing data')
                    fields_str = ", ".join(ordered_fields + ["timestamp"])
                    values_list = insert_data.collect()
                    for values in values_list:
                        values = [str(v) for v in values]
                        values_str = ", ".join(values)
                        values_str = values_str.replace("None", "NULL")
                        sql = f"INSERT INTO `{table}` ({fields_str}) VALUES ({values_str})"
                        operation_to_sql_history(event_time, sql)
                    insert_data.write.format("delta").mode("append").save(minio_output_path)
            continue

        if operation == "c":
            insert_cols = [col(f"after_{field}").alias(field) for field in ordered_fields] + [col("timestamp")]
            insert_data = window_batch.filter(col("operation") == "c").select(insert_cols)

            if not insert_data.isEmpty():
                print('Inserting data')
                fields_str = ", ".join(ordered_fields + ["timestamp"])
                values_list = insert_data.collect()
                for values in values_list:
                    values = [str(v) for v in values]
                    values_str = ", ".join(values)
                    values_str = values_str.replace("None", "NULL")
                    sql = f"INSERT INTO `{table}` ({fields_str}) VALUES ({values_str})"
                    operation_to_sql_history(event_time, sql)
                
                insert_data.write.format("delta").mode("append").save(minio_output_path)

        elif operation == "u":
            update_cols = [col(f"after_{field}").alias(field) for field in ordered_fields] + [col("timestamp")]
            update_data = window_batch.filter(col("operation") == "u").select(update_cols)

            if not update_data.isEmpty():
                exists = existing_data.join(
                    update_data.select(key_column_name),
                    key_column_name,
                    "inner"
                ).count() > 0

                if exists:
                    print('Updating data')
                    fields = ordered_fields + ["timestamp"]
                    values_list = update_data.collect()
                    for values in values_list:
                        str_values = [str(v) for v in values]
                        set_clause = ", ".join([f"{field} = '{value}'" for field, value in zip(fields, str_values)])
                        key_value = str_values[ordered_fields.index(key_column_name)]
                        set_clause = set_clause.replace("None", "NULL")
                        sql = f"UPDATE `{table}` SET {set_clause} WHERE {key_column_name} = '{key_value}'"
                        operation_to_sql_history(event_time, sql)
                    delta_table.alias("target").merge(
                        update_data.alias("source"),
                        f"target.{key_column_name} = source.{key_column_name}"
                    ).whenMatchedUpdateAll().execute()

        elif operation == "d":
            delete_data = window_batch.filter(col("operation") == "d") \
                .select(col(f"before_{key_column_name}").alias(key_column_name))

            if not delete_data.isEmpty():
                exists = existing_data.join(
                    delete_data,
                    key_column_name,
                    "inner"
                ).count() > 0

                if exists:
                    print('Deleting data')
                    values_list = delete_data.collect()
                    for values in values_list:
                        key_value = str(values[0])
                        sql = f"DELETE FROM `{table}` WHERE {key_column_name} = '{key_value}'"
                        operation_to_sql_history(event_time, sql)   
                    delta_table.alias("target").merge(
                        delete_data.alias("source"),
                        f"target.{key_column_name} = source.{key_column_name}"
                    ).whenMatchedDelete().execute()

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
    """Calculate processing window with safety margin."""
    now = datetime.now()
    cron = croniter(cron_expression, now)
    next_run = cron.get_next(datetime)
    following_run = cron.get_next(datetime)
    
    interval = (following_run - next_run).total_seconds()
    return int(interval)

if __name__ == "__main__":
    cronn_expression = "*/1 * * * *"
    process_time = str(_calculate_processing_window(cronn_expression)) + ' seconds'
    print(f"Processing time: {process_time}")
    run_stream(process_time)
