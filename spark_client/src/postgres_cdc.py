import os
import json
import argparse

from pyspark.errors import AnalysisException
from pyspark.sql.functions import from_json, col, max_by, struct, when, udf, explode, map_keys
from pyspark.sql.types import StructType, IntegerType, LongType, FloatType, DoubleType, StringType, StructField, \
    BinaryType, DecimalType, BooleanType, MapType
from pyspark.sql import SparkSession
from delta.tables import DeltaTable

