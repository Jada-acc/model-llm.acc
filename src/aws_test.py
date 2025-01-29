import boto3
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_aws_connection():
    try:
        # Test S3 connection
        s3 = boto3.client('s3')
        response = s3.list_buckets()
        logger.info("Successfully connected to AWS!")
        logger.info(f"Found {len(response['Buckets'])} buckets")
        return True
    except Exception as e:
        logger.error(f"Error connecting to AWS: {str(e)}")
        return False

if __name__ == "__main__":
    test_aws_connection()