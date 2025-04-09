"""
JSON utilities for the rental marketplace ETL pipeline.
"""

import json
from datetime import datetime, date
from decimal import Decimal
from typing import Any


class CustomJSONEncoder(json.JSONEncoder):
    """Custom JSON encoder that handles datetime, date, and Decimal objects."""

    def default(self, obj: Any) -> Any:
        """Convert special types to JSON serializable types."""
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        if isinstance(obj, Decimal):
            return float(obj)
        return super().default(obj)


def json_serialize(obj: Any) -> str:
    """
    Serialize an object to JSON, handling special types.
    
    Args:
        obj: Object to serialize
        
    Returns:
        JSON string
    """
    return json.dumps(obj, cls=CustomJSONEncoder)
