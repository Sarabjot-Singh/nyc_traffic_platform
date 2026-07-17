import os
import sys
import boto3
from botocore.config import Config
from pathlib import Path
from dotenv import load_dotenv


project_root = Path(__file__).resolve().parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from src.common.logger import get_logger
from src.common.favicon import favicon

logger = get_logger()

load_dotenv(project_root / ".env")


class S3Util:
    def __init__(self):
        self.s3_client = boto3.client(
            "s3",
            endpoint_url=os.getenv("MINIO_URL"),
            aws_access_key_id=os.getenv("MINIO_ROOT_USER"),
            aws_secret_access_key=os.getenv("MINIO_ROOT_PASSWORD"),
            config=Config(signature_version="s3v4"),
            region_name=os.getenv("AWS_REGION"),
        )

    def upload_file(self, file_path: str, bucket_name: str, object_key: str) -> None:
        """
        Upload a file to an S3 bucket.
        """
        try:
            self.s3_client.upload_file(file_path, bucket_name, object_key)
            logger.info(f"{favicon['right']} File %s uploaded to S3 bucket %s with key %s.", file_path, bucket_name, object_key)
        except Exception as e:
            logger.error(f"{favicon['error']} Failed to upload file %s to S3 bucket %s with key %s. Error: %s", file_path, bucket_name, object_key, str(e))
            raise

    def upload_fileobj(self, file_obj, bucket_name: str, object_key: str) -> None:
        """
        Upload a file-like object to an S3 bucket.
        """
        try:
            self.s3_client.upload_fileobj(file_obj, bucket_name, object_key)
            logger.info(f"{favicon['right']} File-like object uploaded to S3 bucket %s with key %s.", bucket_name, object_key)
        except Exception as e:
            logger.error(f"{favicon['error']} Failed to upload file-like object to S3 bucket %s with key %s. Error: %s", bucket_name, object_key, str(e))
            raise

    def get_partitions(self,bucket_name, prefix):
        file_set = set()

        objects = self.s3_client.list_objects(
            Bucket=bucket_name,
            Prefix=prefix,
        )

        for content in objects['Contents']:
            path = content['Key']
            folder = path.rsplit("/", 1)[0]
            file_set.add(folder)

        return file_set

        
