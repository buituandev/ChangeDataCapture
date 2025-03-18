"""
MongoDB utility functions for CDC processing.

This module provides functions to process and transform MongoDB documents,
especially handling MongoDB extended JSON (BSON) format.
"""

import json
from typing import Dict, List, Any, Optional, Union


def extract_bson_value(value: Any) -> Any:
    """
    Extract the actual value from MongoDB extended JSON format.
    
    Args:
        value: Value potentially in MongoDB extended JSON format
        
    Returns:
        The extracted value in a format suitable for Delta Lake
    """
    if not isinstance(value, dict):
        return value
        
    # Handle MongoDB extended JSON types
    if "$oid" in value:
        return value["$oid"]  # Return ObjectId as string
    elif "$numberLong" in value:
        return int(value["$numberLong"])  # Convert to integer
    elif "$numberInt" in value:
        return int(value["$numberInt"])
    elif "$numberDouble" in value:
        return float(value["$numberDouble"])
    elif "$numberDecimal" in value:
        return float(value["$numberDecimal"])  # Convert to float as an approximation
    elif "$date" in value:
        # Could be timestamp or ISO string
        date_val = value["$date"]
        if isinstance(date_val, dict) and "$numberLong" in date_val:
            return int(date_val["$numberLong"])
        return date_val
    elif "$binary" in value:
        # For binary data, store as a JSON string representation
        return json.dumps(value)
    elif "$regex" in value:
        return json.dumps(value)
    else:
        # For other complex objects, serialize to JSON
        return json.dumps(value)


def process_mongodb_document(doc_dict: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """
    Process a MongoDB document to handle extended JSON types.
    
    Args:
        doc_dict: Dictionary representing a MongoDB document
        
    Returns:
        Processed document with normalized values
    """
    if not doc_dict:
        return None
        
    result = {}
    for field, value in doc_dict.items():
        if isinstance(value, dict):
            # Check if it's a MongoDB extended JSON type
            if any(key.startswith("$") for key in value.keys()):
                result[field] = extract_bson_value(value)
            else:
                # It's a nested document
                result[field] = process_mongodb_document(value)
        elif isinstance(value, list):
            # Handle arrays
            result[field] = [
                process_mongodb_document(item) if isinstance(item, dict) else item 
                for item in value
            ]
        else:
            result[field] = value
            
    return result 