from pyspark.sql import SparkSession

# Connection information
postgres_url = "jdbc:postgresql://172.18.0.3:5432/postgres"
postgres_user = "postgres"
postgres_password = "postgres"
new_table_name = "links"

properties = {
    "user": "postgres",
    "password": "postgres",
    "driver": "org.postgresql.Driver"
}

spark = SparkSession.builder \
    .appName("Write PostgreSQL") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.jars", "/opt/postgresql-42.7.5.jar") \
    .getOrCreate()
    
df = spark.createDataFrame([
    ['red', 'banana', 1, 10], ['blue', 'banana', 2, 20], ['red', 'carrot', 3, 30],
    ['blue', 'grape', 4, 40], ['red', 'carrot', 5, 50], ['black', 'carrot', 6, 60],
    ['red', 'banana', 7, 70], ['red', 'grape', 8, 80]], schema=['color', 'fruit', 'v1', 'v2'])

# df.show()
# df.groupby('color').avg().show()

# df = spark.read.jdbc(url=postgres_url, table=new_table_name, properties=properties)

df.createOrReplaceTempView("links")

spark.sql("SELECT * FROM links WHERE color = 'blue'").show()

# limit_df = df.limit(10)
# df.show(1)
# df.printSchema()

# To parquet file
# limit_df.write.parquet("/output/links.parquet")

# Stop the SparkSession
spark.stop()