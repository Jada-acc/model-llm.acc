import boto3
import logging
from botocore.exceptions import ClientError

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_project_bucket(bucket_name: str, region: str = 'us-west-2'):
    """Create an S3 bucket for the project."""
    try:
        s3_client = boto3.client('s3', region_name=region)
        
        # Create bucket with location constraint
        location = {'LocationConstraint': region}
        s3_client.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration=location
        )
        
        logger.info(f"Successfully created bucket: {bucket_name}")
        return True
        
    except ClientError as e:
        error = e.response['Error']
        if error['Code'] == 'BucketAlreadyOwnedByYou':
            logger.info(f"Bucket {bucket_name} already exists and is owned by you")
            return True
        elif error['Code'] == 'BucketAlreadyExists':
            logger.error(f"Bucket {bucket_name} already exists but is owned by another account")
            return False
        else:
            logger.error(f"Error creating bucket: {str(e)}")
            return False

if __name__ == "__main__":
    # Replace with your desired bucket name - must be globally unique
    BUCKET_NAME = "autonomous-llm-data-" + input("Enter a unique suffix for your bucket name: ")
    create_project_bucket(BUCKET_NAME)