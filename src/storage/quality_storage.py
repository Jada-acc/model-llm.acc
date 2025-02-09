from typing import Dict, Any, List
import logging
from datetime import datetime
from sqlalchemy import text
from src.storage.storage_optimizer import StorageOptimizer

logger = logging.getLogger(__name__)

class QualityStorage:
    """Store and manage data quality metrics."""
    
    def __init__(self, storage: StorageOptimizer):
        self.storage = storage
    
    def store_quality_metrics(self, metrics: Dict[str, Any]) -> bool:
        """Store quality metrics in the database."""
        try:
            # Extract stage from the first metrics entry
            first_metric = next(iter(metrics.values())) if isinstance(metrics, dict) else metrics
            stage = first_metric.get('stage', 'unknown')
            
            with self.storage.session_scope() as session:
                session.execute(
                    text("""
                    INSERT INTO quality_metrics 
                    (timestamp, stage, metrics, report, score)
                    VALUES (:timestamp, :stage, :metrics, :report, :score)
                    """),
                    {
                        'timestamp': datetime.now().isoformat(),
                        'stage': stage,
                        'metrics': str(metrics),
                        'report': str(self.generate_quality_report(metrics)),
                        'score': self.calculate_quality_score(first_metric)
                    }
                )
                return True
        except Exception as e:
            logger.error(f"Error storing quality metrics: {str(e)}")
            return False
    
    def generate_quality_report(self, metrics: Dict[str, Any]) -> Dict[str, Any]:
        """Generate a detailed quality report."""
        return {
            'summary': {
                'total_records': metrics.get('total_records', 0),
                'quality_score': self.calculate_quality_score(metrics),
                'timestamp': datetime.now().isoformat()
            },
            'issues': self.identify_quality_issues(metrics),
            'recommendations': self.generate_recommendations(metrics)
        }
    
    def calculate_quality_score(self, metrics: Dict[str, Any]) -> float:
        """Calculate overall quality score."""
        try:
            weights = {
                'missing_values': 0.4,
                'data_types': 0.3,
                'consistency': 0.3
            }
            
            scores = {
                'missing_values': 100 - metrics.get('missing_values', {}).get('percentage', 0),
                'data_types': sum(
                    1 for v in metrics.get('data_types', {}).values() if v
                ) / max(len(metrics.get('data_types', {})), 1) * 100,
                'consistency': sum(
                    1 for v in metrics.get('consistency', {}).values() if v
                ) / max(len(metrics.get('consistency', {})), 1) * 100
            }
            
            return sum(
                scores.get(key, 0) * weight 
                for key, weight in weights.items()
            )
        except Exception as e:
            logger.error(f"Error calculating quality score: {str(e)}")
            return 0.0
    
    def identify_quality_issues(self, metrics: Dict[str, Any]) -> List[str]:
        """Identify quality issues from metrics."""
        issues = []
        
        if metrics.get('missing_values', {}).get('percentage', 0) > 10:
            issues.append(f"High percentage of missing values: {metrics['missing_values']['percentage']}%")
            
        for field, valid in metrics.get('data_types', {}).items():
            if not valid:
                issues.append(f"Invalid data type for field: {field}")
                
        return issues
    
    def generate_recommendations(self, metrics: Dict[str, Any]) -> List[str]:
        """Generate recommendations based on quality issues."""
        recommendations = []
        
        if metrics.get('missing_values', {}).get('percentage', 0) > 10:
            recommendations.append("Consider implementing data validation at source")
            
        if not all(metrics.get('data_types', {}).values()):
            recommendations.append("Review data type handling in ETL pipeline")
            
        return recommendations

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if hasattr(self, 'storage'):
            self.storage.cleanup() 