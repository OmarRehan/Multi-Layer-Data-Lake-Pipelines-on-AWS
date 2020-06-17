import logging
import boto3


def create_bucket(**kwargs):
    """Create a S3 Bucket in a specific region, region & bucket_name should be passed as parameters in dictionary"""

    # Getting the region and the bucket name from params dictionary
    region = kwargs['params']['region']
    bucket_name = kwargs['params']['bucket_name']

    s3_client = boto3.client('s3', region_name=region)  # Initializing S3 Client

    bucket_exists = False
    # Checking if the bucket already exists
    for bucket in s3_client.list_buckets()['Buckets']:
        if bucket['Name'] == bucket_name:
            bucket_exists = True

    # Create bucket
    if not bucket_exists:
        location = {'LocationConstraint': region}
        str_response = s3_client.create_bucket(Bucket=bucket_name,
                                               CreateBucketConfiguration=location)
        logging.info(str_response)

    else:
        logging.info(f'{bucket_name} already exists')
