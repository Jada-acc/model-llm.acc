from typing import Dict, Any, List
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

class DataQualityChecker:
    """Check data quality during ingestion and transformation."""
    
    def __init__(self):
        self.quality_metrics = {}
        
    def check_data_quality(self, data: Dict[str, Any], stage: str) -> bool:
        """Check data quality at different pipeline stages."""
        try:
            # First check required fields
            required_fields = {'timestamp', 'blocks'}
            if not all(field in data for field in required_fields):
                logger.warning(f"Missing required fields in {stage}")
                return False
            
            metrics = {
                'timestamp': datetime.now().isoformat(),
                'stage': stage,
                'total_records': self._count_records(data),
                'missing_values': self._check_missing_values(data),
                'data_types': self._validate_data_types(data),
                'consistency': self.check_data_consistency(data)
            }
            
            self.quality_metrics[f"{stage}_{datetime.now().isoformat()}"] = metrics
            
            # Validate data quality
            if metrics['missing_values']['percentage'] > 20:
                logger.warning(f"High percentage of missing values in {stage}: {metrics['missing_values']['percentage']}%")
                return False
            
            if not all(metrics['data_types'].values()):
                logger.warning(f"Invalid data types detected in {stage}")
                return False
            
            if not all(metrics['consistency'].values()):
                logger.warning(f"Data consistency issues detected in {stage}")
                return False
            
            # Additional validation for blocks
            for block in data['blocks']:
                if not isinstance(block, dict):
                    logger.warning(f"Invalid block format in {stage}")
                    return False
                if not all(field in block for field in ['number', 'hash', 'timestamp']):
                    logger.warning(f"Missing required block fields in {stage}")
                    return False
            
            return True
            
        except Exception as e:
            logger.error(f"Error checking data quality: {str(e)}")
            return False
    
    def _count_records(self, data: Dict[str, Any]) -> int:
        """Count total records in the data."""
        if 'blocks' in data:
            return len(data['blocks'])
        return 1
    
    def _check_missing_values(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Check for missing or null values."""
        missing_count = 0
        total_fields = 0
        
        def count_missing(d: Dict[str, Any]):
            nonlocal missing_count, total_fields
            for k, v in d.items():
                total_fields += 1
                if v is None or v == '':
                    missing_count += 1
                elif isinstance(v, dict):
                    count_missing(v)
                elif isinstance(v, list):
                    for item in v:
                        if isinstance(item, dict):
                            count_missing(item)
        
        count_missing(data)
        
        return {
            'count': missing_count,
            'total': total_fields,
            'percentage': (missing_count / total_fields * 100) if total_fields > 0 else 0
        }
    
    def _validate_data_types(self, data: Dict[str, Any]) -> Dict[str, bool]:
        """Validate data types of key fields."""
        return {
            'timestamp_valid': isinstance(data.get('timestamp'), (str, datetime)),
            'blocks_valid': isinstance(data.get('blocks', []), list),
            'metrics_valid': isinstance(data.get('metrics', {}), dict)
        }
    
    def get_quality_report(self) -> Dict[str, Any]:
        """Generate a quality report for all stages."""
        return {
            'metrics': self.quality_metrics,
            'summary': {
                'total_checks': len(self.quality_metrics),
                'last_check': max(self.quality_metrics.keys()) if self.quality_metrics else None,
                'stages_checked': list(set(m['stage'] for m in self.quality_metrics.values()))
            }
        }

    def check_data_consistency(self, data: Dict[str, Any]) -> Dict[str, bool]:
        """Check data consistency and relationships."""
        consistency = {
            'block_sequence_valid': self._validate_block_sequence(data),
            'transaction_counts_match': self._validate_transaction_counts(data),
            'timestamps_valid': self._validate_timestamps(data)
        }
        return consistency

    def _validate_block_sequence(self, data: Dict[str, Any]) -> bool:
        """Validate that block numbers are sequential."""
        if 'blocks' not in data:
            return True
        
        block_numbers = [block['number'] for block in data['blocks']]
        return all(
            b2 - b1 == 1 
            for b1, b2 in zip(block_numbers, block_numbers[1:])
        )

    def _validate_transaction_counts(self, data: Dict[str, Any]) -> bool:
        """Validate transaction counts match across different parts of data."""
        if 'blocks' not in data:
            return True
        
        total_txs = sum(len(block.get('transactions', [])) for block in data['blocks'])
        reported_txs = data.get('transaction_metrics', {}).get('total_transactions', total_txs)
        return total_txs == reported_txs

    def _validate_timestamps(self, data: Dict[str, Any]) -> bool:
        """Validate timestamp ordering and ranges."""
        if 'blocks' not in data:
            return True
        
        timestamps = [
            datetime.fromisoformat(block['timestamp']) 
            if isinstance(block['timestamp'], str) 
            else block['timestamp'] 
            for block in data['blocks']
        ]
        
        return all(t1 <= t2 for t1, t2 in zip(timestamps, timestamps[1:]))

    def check_data_completeness(self, data: Dict[str, Any]) -> Dict[str, float]:
        """Check completeness of different data aspects."""
        required_block_fields = {'number', 'hash', 'timestamp', 'transactions'}
        required_tx_fields = {'hash', 'from', 'to', 'value'}
        
        completeness = {
            'block_completeness': self._calculate_completeness(
                data.get('blocks', []), required_block_fields
            ),
            'transaction_completeness': self._calculate_tx_completeness(
                data.get('blocks', []), required_tx_fields
            )
        }
        return completeness

    def _calculate_completeness(self, items: List[Dict], required_fields: set) -> float:
        """Calculate completeness percentage for a list of items."""
        if not items:
            return 100.0
        
        total_fields = len(items) * len(required_fields)
        present_fields = sum(
            1 for item in items
            for field in required_fields
            if field in item and item[field] is not None
        )
        return (present_fields / total_fields) * 100

    def _calculate_tx_completeness(self, blocks: List[Dict], required_fields: set) -> float:
        """Calculate completeness percentage for transactions."""
        all_txs = [
            tx for block in blocks
            for tx in block.get('transactions', [])
            if isinstance(tx, dict)
        ]
        return self._calculate_completeness(all_txs, required_fields) 