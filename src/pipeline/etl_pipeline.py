from typing import Dict, Any, List, Optional
import logging
from datetime import datetime
from src.pipeline.data_validator import DataValidator
from src.pipeline.performance_optimizer import PerformanceOptimizer
from src.storage.storage_optimizer import StorageOptimizer

logger = logging.getLogger(__name__)

class ETLPipeline:
    """Handle data extraction, transformation, and loading."""
    
    def __init__(self, storage: StorageOptimizer):
        self.storage = storage
        self.validator = DataValidator()
        self.optimizer = PerformanceOptimizer()
        self.transformations = {}
        
    def register_transformation(self, name: str, transform_fn: callable):
        """Register a transformation function."""
        self.transformations[name] = transform_fn
        logger.info(f"Registered transformation: {name}")
    
    def extract(self, source: str, query: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Extract data from source."""
        try:
            with self.storage.session_scope() as session:
                # Execute extraction query
                result = session.execute(query['query'], query.get('params', {}))
                data = [dict(row) for row in result]
                
                logger.info(f"Extracted {len(data)} records from {source}")
                return data
                
        except Exception as e:
            logger.error(f"Error extracting data: {str(e)}")
            raise
    
    def transform(self, data: List[Dict[str, Any]], transformations: List[str]) -> List[Dict[str, Any]]:
        """Apply transformations to data."""
        try:
            transformed_data = data
            
            for transform_name in transformations:
                if transform_name not in self.transformations:
                    raise ValueError(f"Unknown transformation: {transform_name}")
                    
                transform_fn = self.transformations[transform_name]
                transformed_data = [
                    transform_fn(record) for record in transformed_data
                ]
                
            logger.info(f"Applied transformations: {transformations}")
            return transformed_data
            
        except Exception as e:
            logger.error(f"Error transforming data: {str(e)}")
            raise
    
    def load(self, data: List[Dict[str, Any]], target: str) -> bool:
        """Load data into target."""
        try:
            # Validate data before loading
            if not self.validator.validate_data(data, target):
                raise ValueError("Data validation failed")
            
            # Optimize data for loading
            optimized_data = self.optimizer.optimize_for_loading(data)
            
            with self.storage.session_scope() as session:
                # Execute load query
                for record in optimized_data:
                    session.execute(
                        f"INSERT INTO {target} ({','.join(record.keys())}) "
                        f"VALUES ({','.join([':' + k for k in record.keys()])})",
                        record
                    )
                
            logger.info(f"Loaded {len(data)} records into {target}")
            return True
            
        except Exception as e:
            logger.error(f"Error loading data: {str(e)}")
            return False
    
    def process_batch(
        self,
        source: str,
        target: str,
        query: Dict[str, Any],
        transformations: List[str]
    ) -> bool:
        """Process a complete ETL batch."""
        try:
            # Extract
            data = self.extract(source, query)
            if not data:
                logger.warning("No data extracted")
                return False
            
            # Transform
            transformed_data = self.transform(data, transformations)
            if not transformed_data:
                logger.warning("No data after transformation")
                return False
            
            # Load
            success = self.load(transformed_data, target)
            if not success:
                logger.error("Failed to load data")
                return False
            
            logger.info(f"Successfully processed batch from {source} to {target}")
            return True
            
        except Exception as e:
            logger.error(f"Error processing batch: {str(e)}")
            return False 