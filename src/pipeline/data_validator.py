from typing import Dict, Any, List
import jsonschema
from datetime import datetime

class DataValidator:
    """Validate data structure and content."""
    
    def __init__(self):
        self.schemas = self.load_schemas()
    
    def load_schemas(self) -> Dict[str, Dict]:
        """Load JSON schemas for validation."""
        return {
            'blockchain_data': {
                'type': 'object',
                'properties': {
                    'blocks': {
                        'type': 'array',
                        'items': {
                            'type': 'object',
                            'required': ['number', 'hash', 'timestamp']
                        }
                    },
                    'timestamp': {'type': 'string'},
                    'metrics': {'type': 'object'}
                },
                'required': ['blocks', 'timestamp']
            }
        }
    
    def validate_data(self, data: Dict[str, Any], schema_type: str) -> bool:
        """Validate data against schema."""
        try:
            jsonschema.validate(instance=data, schema=self.schemas[schema_type])
            return True
        except jsonschema.exceptions.ValidationError as e:
            logger.error(f"Data validation failed: {str(e)}")
            return False 