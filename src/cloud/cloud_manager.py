import os
import logging
from typing import Dict, Any, Optional, List
import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)

class CloudManager:
    """Manages cloud infrastructure and resources."""
    
    def __init__(self, config: Dict[str, Any]):
        """Initialize cloud manager with configuration."""
        self.config = config
        self.provider = config.get('provider', 'aws').lower()
        self.region = config.get('region', 'us-west-2')
        self._initialize_clients()
        
    def _initialize_clients(self):
        """Initialize cloud service clients based on provider."""
        if self.provider == 'aws':
            self.s3_client = boto3.client('s3', region_name=self.region)
            self.s3_resource = boto3.resource('s3', region_name=self.region)
            self.sagemaker_client = boto3.client('sagemaker', region_name=self.region)
            self.cloudwatch_client = boto3.client('cloudwatch', region_name=self.region)
        else:
            raise ValueError(f"Unsupported cloud provider: {self.provider}")
            
    def upload_file(self, local_path: str, remote_path: str, bucket: Optional[str] = None) -> bool:
        """Upload a file to cloud storage.
        
        Args:
            local_path: Path to local file
            remote_path: Destination path in cloud storage
            bucket: Optional bucket name, defaults to configured bucket
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            bucket = bucket or self.config['storage']['bucket_name']
            self.s3_client.upload_file(local_path, bucket, remote_path)
            logger.info(f"Successfully uploaded {local_path} to {bucket}/{remote_path}")
            return True
        except ClientError as e:
            logger.error(f"Failed to upload file: {str(e)}")
            return False
            
    def download_file(self, remote_path: str, local_path: str, bucket: Optional[str] = None) -> bool:
        """Download a file from cloud storage.
        
        Args:
            remote_path: Path in cloud storage
            local_path: Destination path for local file
            bucket: Optional bucket name, defaults to configured bucket
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            bucket = bucket or self.config['storage']['bucket_name']
            self.s3_client.download_file(bucket, remote_path, local_path)
            logger.info(f"Successfully downloaded {bucket}/{remote_path} to {local_path}")
            return True
        except ClientError as e:
            logger.error(f"Failed to download file: {str(e)}")
            return False
            
    def list_files(self, prefix: str = "", bucket: Optional[str] = None) -> List[str]:
        """List files in cloud storage.
        
        Args:
            prefix: Optional prefix to filter results
            bucket: Optional bucket name, defaults to configured bucket
            
        Returns:
            List of file paths
        """
        try:
            bucket = bucket or self.config['storage']['bucket_name']
            paginator = self.s3_client.get_paginator('list_objects_v2')
            files = []
            
            for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
                if 'Contents' in page:
                    files.extend([obj['Key'] for obj in page['Contents']])
                    
            return files
        except ClientError as e:
            logger.error(f"Failed to list files: {str(e)}")
            return []
            
    def create_ml_instance(self, instance_type: Optional[str] = None) -> Dict[str, Any]:
        """Create a machine learning instance.
        
        Args:
            instance_type: Optional instance type, defaults to configured type
            
        Returns:
            Dictionary with instance details
        """
        try:
            instance_type = instance_type or self.config['instance_type']
            
            # Create SageMaker notebook instance
            response = self.sagemaker_client.create_notebook_instance(
                NotebookInstanceName=f"autonomous-llm-{os.getpid()}",
                InstanceType=instance_type,
                RoleArn=self.config.get('role_arn', ''),  # IAM role ARN
                Tags=[
                    {
                        'Key': 'Project',
                        'Value': 'AutonomousLLM'
                    }
                ]
            )
            
            logger.info(f"Created ML instance: {response['NotebookInstanceArn']}")
            return response
            
        except ClientError as e:
            logger.error(f"Failed to create ML instance: {str(e)}")
            return {}
            
    def monitor_resources(self) -> Dict[str, Any]:
        """Monitor cloud resource usage.
        
        Returns:
            Dictionary containing resource metrics
        """
        try:
            # Get basic AWS CloudWatch metrics
            metrics = {
                'cpu_utilization': self._get_metric('CPUUtilization'),
                'memory_usage': self._get_metric('MemoryUtilization'),
                'disk_usage': self._get_metric('DiskUtilization'),
                'network_in': self._get_metric('NetworkIn'),
                'network_out': self._get_metric('NetworkOut')
            }
            
            return metrics
            
        except ClientError as e:
            logger.error(f"Failed to monitor resources: {str(e)}")
            return {}
            
    def _get_metric(self, metric_name: str) -> float:
        """Helper method to get CloudWatch metrics."""
        try:
            response = self.cloudwatch_client.get_metric_statistics(
                Namespace='AWS/SageMaker',
                MetricName=metric_name,
                Period=300,  # 5-minute periods
                StartTime='PT1H',  # Last hour
                Statistics=['Average']
            )
            
            if response['Datapoints']:
                return response['Datapoints'][0]['Average']
            return 0.0
            
        except ClientError:
            return 0.0