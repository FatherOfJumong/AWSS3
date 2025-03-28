import json
import logging
import os
import tempfile
import boto3
import requests
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)

def init_client(aws_access_key_id=None, aws_secret_access_key=None, region_name=None, endpoint_url=None, aws_session_token=None):
    session = boto3.session.Session(
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        region_name=region_name,
        aws_session_token=aws_session_token
    )
    
    return session.client('s3', endpoint_url=endpoint_url)

def list_buckets(s3_client=None):
    if s3_client is None:
        s3_client = init_client()
        
    try:
        response = s3_client.list_buckets()
        buckets = [bucket['Name'] for bucket in response.get('Buckets', [])]
        return buckets
    except ClientError as e:
        logger.error(f"Failed to list buckets: {e}")
        raise

def create_bucket(bucket_name, region=None, s3_client=None):
    if s3_client is None:
        s3_client = init_client(region_name=region)
        
    try:
        if region is None:
            s3_client.create_bucket(Bucket=bucket_name)
        else:
            location = {'LocationConstraint': region}
            s3_client.create_bucket(
                Bucket=bucket_name,
                CreateBucketConfiguration=location
            )
        logger.info(f"Bucket {bucket_name} created successfully")
        return True
    except ClientError as e:
        logger.error(f"Failed to create bucket {bucket_name}: {e}")
        raise

def delete_bucket(bucket_name, s3_client=None):
    if s3_client is None:
        s3_client = init_client()
        
    try:
        s3_client.delete_bucket(Bucket=bucket_name)
        logger.info(f"Bucket {bucket_name} deleted successfully")
        return True
    except ClientError as e:
        logger.error(f"Failed to delete bucket {bucket_name}: {e}")
        raise

def bucket_exists(bucket_name, s3_client=None):
    if s3_client is None:
        s3_client = init_client()
        
    try:
        s3_client.head_bucket(Bucket=bucket_name)
        return True
    except ClientError:
        return False

def download_file_and_upload_to_s3(url, bucket_name, object_key, s3_client=None):
    if s3_client is None:
        s3_client = init_client()
        
    try:
        with tempfile.NamedTemporaryFile() as temp_file:
            response = requests.get(url, stream=True)
            response.raise_for_status()
            
            for chunk in response.iter_content(chunk_size=8192):
                temp_file.write(chunk)
            
            temp_file.flush()
            s3_client.upload_file(
                temp_file.name,
                bucket_name,
                object_key
            )
            
        logger.info(f"File from {url} uploaded to s3://{bucket_name}/{object_key}")
        return True
    except (ClientError, requests.RequestException) as e:
        logger.error(f"Failed to download and upload file: {e}")
        raise

def set_object_access_policy(bucket_name, object_key, acl='public-read', s3_client=None):
    if s3_client is None:
        s3_client = init_client()
        
    try:
        s3_client.put_object_acl(
            Bucket=bucket_name,
            Key=object_key,
            ACL=acl
        )
        logger.info(f"Set {acl} ACL for s3://{bucket_name}/{object_key}")
        return True
    except ClientError as e:
        logger.error(f"Failed to set object ACL: {e}")
        raise

def generate_public_read_policy(bucket_name, prefix=""):
    resource = f"arn:aws:s3:::{bucket_name}"
    if prefix:
        object_resource = f"{resource}/{prefix}/*"
    else:
        object_resource = f"{resource}/*"
        
    return {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "PublicRead",
                "Effect": "Allow",
                "Principal": "*",
                "Action": ["s3:GetObject"],
                "Resource": object_resource
            }
        ]
    }

def create_bucket_policy(bucket_name, policy, s3_client=None):
    if s3_client is None:
        s3_client = init_client()
        
    if isinstance(policy, dict):
        policy = json.dumps(policy)
        
    try:
        s3_client.put_bucket_policy(
            Bucket=bucket_name,
            Policy=policy
        )
        logger.info(f"Policy applied to bucket {bucket_name}")
        return True
    except ClientError as e:
        logger.error(f"Failed to set bucket policy: {e}")
        raise

def read_bucket_policy(bucket_name, s3_client=None):
    if s3_client is None:
        s3_client = init_client()
        
    try:
        response = s3_client.get_bucket_policy(Bucket=bucket_name)
        return json.loads(response['Policy'])
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchBucketPolicy':
            logger.warning(f"No policy found for bucket {bucket_name}")
            return None
        logger.error(f"Failed to get bucket policy: {e}")
        raise
