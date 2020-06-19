import logging
from sql_queries.landing_zone_ddl import dict_landing_zone_ddls
import constants
import boto3
import os

logging.basicConfig(level=logging.INFO, format="%(asctime)s:%(levelname)s:%(message)s")


def create_s3_directories():
    list_directories = dict_landing_zone_ddls.keys()
    bucket_name = constants.s3_bucket_name

    s3_client = boto3.client("s3")

    for table_name in list_directories:
        s3_client.put_object(Bucket=bucket_name, Key=table_name + '/')
        logging.info(f'{table_name} has been created.')

    # Creating a Directory for Bootstrap actions to install python Libraries
    s3_resource = boto3.resource('s3')
    s3_resource.meta.client.upload_file(
        os.path.join(os.environ['FLIGHT_PROJECT_PATH'], 'emr_bootstrap.sh'), Bucket=bucket_name,
        Key='BOOTSTRAP_ACTIONS/BOOTSTRAP_ACTIONS.sh')
    logging.info(f'BOOTSTRAP_ACTIONS has been created & uploaded.')


if __name__ == '__main__':
    create_s3_directories()
