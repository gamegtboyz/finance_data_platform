import boto3
from dotenv import load_dotenv
import os

load_dotenv()

# create the boto3 client using boto3 and environment variables for AWS credentials
def create_s3_client():
    """
    Create and return an S3 client using boto3 and environment variables for AWS credentials.
    """
    s3_client = boto3.client(
        's3',
        region_name = os.getenv('AWS_REGION'),
        aws_access_key_id = os.getenv('AWS_ACCESS_KEY'),
        aws_secret_access_key = os.getenv('AWS_SECRET_KEY')
    )

    return s3_client

# create the wrapper functions of uploading, object listing, and downloading files from S3
def s3_upload(file_name, bucket_name, object_name=None):
    """
    Upload a file to an S3 bucket.
    """
    
    # create s3 client to access s3 bucket
    s3_client = create_s3_client()

    # if the object name is not specified, use the file name as the object name
    if object_name is None:
        object_name = os.path.basename(file_name)

    # upload the file to the specified S3 bucket
    s3_client.upload_file(file_name, bucket_name, object_name)

    return print(f"File {file_name} has been uploaded to bucket {bucket_name} as {object_name}")

def s3_list_objects(bucket_name):
    """
    Returns some or all (up to 1,000) of the objects in a bucket.
    """
    # create s3 client to access s3 bucket
    s3_client = create_s3_client()

    # list the objects in the specified S3 bucket
    response = s3_client.list_objects_v2(Bucket=bucket_name)

    # return the list of objects in the bucket, if there are any, otherwise return an empty list
    return response.get('Contents', [])

def s3_download(bucket_name, object_name, file_name):
    """
    Download a file from an S3 bucket.
    """
    # create s3 client to access s3 bucket
    s3_client = create_s3_client()
    
    # download the file from the specified S3 bucket
    s3_client.download_file(bucket_name, object_name, file_name)

    return print(f"File {object_name} has been downloaded from bucket {bucket_name} as {file_name}")