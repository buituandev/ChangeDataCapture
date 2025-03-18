"""
Schema management for CDC events.

This module contains classes and functions for dynamically generating,
caching, and loading schemas for CDC events in various formats.
"""

import os
import json
from typing import Dict, List, Tuple, Any, Optional

from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, LongType,
    FloatType, DoubleType, BooleanType, BinaryType, DecimalType
)


class SchemaManager:
    """
    Manages schemas for CDC events.
    
    This class handles the dynamic generation, caching, and loading of schemas
    for different CDC event formats (Debezium, MongoDB, etc.).
    """
    
    def __init__(self, schema_path: str, field_info_path: str):
        """
        Initialize the schema manager.
        
        Args:
            schema_path: Path to cache the schema JSON
            field_info_path: Path to cache the field information JSON
        """
        self.schema_path = schema_path
        self.field_info_path = field_info_path
        self.cached_schema: Optional[StructType] = None
        self.cached_field_info: Optional[List[Dict[str, Any]]] = None
    
    def get_spark_type(self, debezium_type: str):
        """
        Convert Debezium data type to corresponding Spark SQL type.
        
        Args:
            debezium_type: The Debezium data type name
            
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
    
    def get_schema_info_from_debezium(self, json_str: str) -> List[Dict[str, Any]]:
        """
        Extract field information from a Debezium JSON payload.
        
        Args:
            json_str: JSON string containing a Debezium event
            
        Returns:
            List of dictionaries with field information
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
    
    def create_struct_type_from_debezium(self, field_info: List[Dict[str, Any]]) -> StructType:
        """
        Create a Spark StructType from Debezium field information.
        
        Args:
            field_info: List of dictionaries with field information
            
        Returns:
            Spark schema for record structure
        """
        fields = []
        for field in field_info:
            spark_type = self.get_spark_type(field['type'])
            fields.append(StructField(field['name'], spark_type, field['optional']))
        return StructType(fields)
    
    def create_dynamic_schema(self, data_json: str) -> Tuple[StructType, str, List[Dict[str, Any]]]:
        """
        Create a complete Spark schema from a Debezium JSON payload.
        
        Args:
            data_json: JSON string containing a Debezium event
            
        Returns:
            Tuple containing:
            - Complete schema for Debezium data
            - Original JSON string
            - List of field information dictionaries
        """
        field_info = self.get_schema_info_from_debezium(data_json)
        record_schema = self.create_struct_type_from_debezium(field_info)
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
    
    def get_schema_info_from_mongodb_debezium(self, json_str: str) -> List[Dict[str, Any]]:
        """
        Extract schema information from a MongoDB Debezium CDC event.
        
        Args:
            json_str: JSON string containing a MongoDB Debezium event
            
        Returns:
            List of dictionaries with field information
        """
        data = json.loads(json_str)
        
        # For MongoDB, we need to extract schema from the actual data
        field_info = []
        
        # Check if after/before fields exist and extract schema from them
        if 'payload' in data and 'after' in data['payload'] and data['payload']['after']:
            # Parse the "after" field which contains the actual document as a JSON string
            after_data = json.loads(data['payload']['after'])
            for field_name, field_value in after_data.items():
                field_type = self.get_mongodb_field_type(field_value)
                field_info.append({
                    'name': field_name,
                    'type': field_type,
                    'optional': True
                })
        elif 'payload' in data and 'before' in data['payload'] and data['payload']['before']:
            # Parse the "before" field as a fallback
            before_data = json.loads(data['payload']['before'])
            for field_name, field_value in before_data.items():
                field_type = self.get_mongodb_field_type(field_value)
                field_info.append({
                    'name': field_name,
                    'type': field_type,
                    'optional': True
                })
        
        return field_info
    
    def get_mongodb_field_type(self, value: Any) -> str:
        """
        Determine the Debezium data type based on a MongoDB field value.
        
        Args:
            value: The value to analyze
            
        Returns:
            The corresponding Debezium data type
        """
        if value is None:
            return "string"
        
        if isinstance(value, bool):
            return "boolean"
        elif isinstance(value, int):
            # Check if it's within 32-bit integer range
            if -2147483648 <= value <= 2147483647:
                return "int32"
            else:
                return "int64"
        elif isinstance(value, float):
            return "double"
        elif isinstance(value, dict):
            # Handle ObjectId and other BSON types that come as dictionaries
            if "$oid" in value:
                return "string"  # ObjectId is treated as string
            elif "$date" in value:
                return "int64"   # Timestamps are treated as int64
            elif "$numberDecimal" in value:
                return "decimal"
            else:
                return "string"  # Other objects are treated as JSON strings
        else:
            return "string"
    
    def create_dynamic_schema_mongodb(self, data_json: str) -> Tuple[StructType, str, List[Dict[str, Any]]]:
        """
        Create a complete Spark schema from a MongoDB Debezium JSON payload.
        
        Args:
            data_json: JSON string containing a MongoDB Debezium event
            
        Returns:
            Tuple containing:
            - Complete schema for MongoDB Debezium data
            - Original JSON string
            - List of field information dictionaries
        """
        field_info = self.get_schema_info_from_mongodb_debezium(data_json)
        
        # MongoDB Debezium format has before/after as JSON strings
        schema = StructType([
            StructField("schema", StringType(), True),
            StructField("payload", StructType([
                StructField("before", StringType(), True),
                StructField("after", StringType(), True),
                StructField("source", StructType([
                    StructField("connector", StringType(), True),
                    StructField("version", StringType(), True),
                    StructField("name", StringType(), True),
                    StructField("ts_ms", LongType(), True),
                    StructField("snapshot", StringType(), True),
                    StructField("db", StringType(), True),
                    StructField("rs", StringType(), True),
                    StructField("collection", StringType(), True),
                    StructField("ord", IntegerType(), True),
                    StructField("h", StringType(), True)
                ]), True),
                StructField("op", StringType(), True),
                StructField("ts_ms", LongType(), True),
                StructField("transaction", StringType(), True)
            ]), True)
        ])
        
        return schema, data_json, field_info
    
    def generate_select_statements(self, schema: StructType, field_info: List[Dict[str, Any]]) -> Tuple[List, List]:
        """
        Generate column selection expressions for processing CDC data.
        
        Args:
            schema: Spark schema for Debezium data
            field_info: List of dictionaries with field information
            
        Returns:
            Tuple containing:
            - Column expressions for selecting data
            - Ordered list of field names
        """
        from pyspark.sql.functions import col
        
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
    
    def save_cached_schema(self, schema: StructType, field_info: List[Dict[str, Any]]) -> None:
        """
        Save schema and field information to disk for future use.
        
        Args:
            schema: Spark schema to save
            field_info: Field information dictionaries
        """
        schema_json = schema.json()
        with open(self.schema_path, "w") as f:
            f.write(schema_json)
        with open(self.field_info_path, "w") as f:
            f.write(json.dumps(field_info))
    
    def load_cached_schema(self) -> Tuple[StructType, List[Dict[str, Any]]]:
        """
        Load previously cached schema and field information from disk.
        
        Returns:
            Tuple containing:
            - Loaded schema
            - Loaded field information
        """
        with open(self.schema_path, "r") as f:
            schema_json = f.read()
        schema = StructType.fromJson(json.loads(schema_json))
        with open(self.field_info_path, "r") as f:
            field_info = json.loads(f.read())
        return schema, field_info
    
    def is_cached_schema(self) -> bool:
        """
        Check if cached schema files exist.
        
        Returns:
            True if cached schema exists, False otherwise
        """
        return os.path.exists(self.schema_path) and os.path.exists(self.field_info_path)
    
    def get_schema(self, data_json: str, db_type: str = "postgres") -> Tuple[StructType, List[Dict[str, Any]]]:
        """
        Get schema for CDC events.
        
        This method first tries to load the cached schema.
        If no cached schema exists, it dynamically generates one based on the data.
        
        Args:
            data_json: JSON string containing a CDC event
            db_type: Database type ("postgres" or "mongo")
            
        Returns:
            Tuple containing:
            - Schema for CDC data
            - Field information list
        """
        if self.is_cached_schema():
            schema, field_info = self.load_cached_schema()
            self.cached_schema = schema
            self.cached_field_info = field_info
            return schema, field_info
        
        if db_type == "postgres":
            schema, _, field_info = self.create_dynamic_schema(data_json)
        elif db_type == "mongo":
            schema, _, field_info = self.create_dynamic_schema_mongodb(data_json)
        else:
            raise ValueError(f"Unsupported database type: {db_type}")
        
        self.save_cached_schema(schema, field_info)
        self.cached_schema = schema
        self.cached_field_info = field_info
        
        return schema, field_info 