import logging
from sql_queries.landing_zone_ddl import dict_landing_zone_ddls
import constants
import boto3


def create_s3_directories():
    list_directories = dict_landing_zone_ddls.keys()
    bucket_name = constants.s3_bucket_name

    s3_client = boto3.client("s3")

    for table_name in list_directories:
        s3_client.put_object(Bucket=bucket_name,Key=table_name+'/')
        logging.info(f'{table_name} has been created.')


if __name__ == '__main__':
    create_s3_directories()
