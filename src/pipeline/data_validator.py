from typing import Dict, Any, List, Optional
import logging
from datetime import datetime
import json

logger = logging.getLogger(__name__)

class DataValidator:
    """Validate data for LLM processing pipeline."""
    
    def __init__(self):
        self.schema_validators = {
            'llm_input': self._validate_llm_input,
            'llm_output': self._validate_llm_output,
            'model_metrics': self._validate_model_metrics,
            'training_data': self._validate_training_data
        }
    
    def validate_data(self, data: List[Dict[str, Any]], target: str) -> bool:
        """Validate data against target schema."""
        try:
            if target not in self.schema_validators:
                raise ValueError(f"Unknown target schema: {target}")
            
            validator = self.schema_validators[target]
            for record in data:
                if not validator(record):
                    return False
            
            logger.info(f"Data validation successful for target: {target}")
            return True
            
        except Exception as e:
            logger.error(f"Data validation failed: {str(e)}")
            return False
    
    def _validate_llm_input(self, record: Dict[str, Any]) -> bool:
        """Validate LLM input data."""
        required_fields = {
            'prompt': str,
            'timestamp': str,
            'model_version': str,
            'parameters': dict,
            'max_tokens': int,
            'temperature': float,
            'context_length': int,
            'safety_settings': dict
        }
        
        if not self._check_fields(record, required_fields):
            return False
        
        try:
            if len(record['prompt']) < 1 or len(record['prompt']) > 32000:
                logger.warning("Prompt length out of bounds")
                return False
            
            if not 0.0 <= record['temperature'] <= 2.0:
                logger.warning("Temperature out of range (0.0-2.0)")
                return False
            
            if record['context_length'] > 128000:
                logger.warning("Context length exceeds model capacity")
                return False
            
            required_safety = {'toxicity', 'hate_speech', 'violence'}
            if not required_safety.issubset(record['safety_settings'].keys()):
                logger.warning("Missing required safety settings")
                return False
            
            return True
            
        except Exception as e:
            logger.error(f"LLM input validation error: {str(e)}")
            return False
    
    def _validate_llm_output(self, record: Dict[str, Any]) -> bool:
        """Validate LLM output data."""
        required_fields = {
            'input_id': str,
            'response': str,
            'timestamp': str,
            'model_version': str,
            'metrics': dict,
            'tokens': int
        }
        
        return self._check_fields(record, required_fields)
    
    def _validate_model_metrics(self, record: Dict[str, Any]) -> bool:
        """Validate model performance metrics."""
        required_fields = {
            'model_id': str,
            'timestamp': str,
            'latency': float,
            'tokens_per_second': float,
            'memory_usage': float,
            'error_rate': float
        }
        
        return self._check_fields(record, required_fields)
    
    def _validate_training_data(self, record: Dict[str, Any]) -> bool:
        """Validate training data format."""
        required_fields = {
            'input': str,
            'output': str,
            'dataset_version': str,
            'quality_score': float,
            'metadata': dict
        }
        
        return self._check_fields(record, required_fields)
    
    def _check_fields(self, record: Dict[str, Any], required_fields: Dict[str, type]) -> bool:
        """Check if record has all required fields of correct type."""
        try:
            for field, field_type in required_fields.items():
                if field not in record:
                    logger.warning(f"Missing required field: {field}")
                    return False
                
                if not isinstance(record[field], field_type):
                    logger.warning(
                        f"Invalid type for {field}: "
                        f"expected {field_type}, got {type(record[field])}"
                    )
                    return False
            
            return True
            
        except Exception as e:
            logger.error(f"Field validation error: {str(e)}")
            return False 