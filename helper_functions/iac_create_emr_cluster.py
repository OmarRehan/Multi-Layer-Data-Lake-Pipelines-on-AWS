import logging
from time import sleep
import boto3
import constants
import os

if __name__ == '__main__':

    logging.basicConfig(level=logging.INFO, format="%(asctime)s:%(levelname)s:%(message)s")

    emr_client = boto3.client(
        'emr',
        region_name=constants.aws_region,
        aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'],
        aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY']
    )

    emr_cluster = emr_client.run_job_flow(
        Name='flights_dl',
        LogUri='s3://aws-logs-131785130434-us-west-2/elasticmapreduce/',
        ReleaseLabel='emr-5.29.0',
        Applications=[
            {
                'Name': 'Spark'
            },
        ],
        Instances={
            'InstanceGroups': [
                {
                    'Name': "Master nodes",
                    'Market': 'ON_DEMAND',
                    'InstanceRole': 'MASTER',
                    'InstanceType': 'm5.xlarge',
                    'InstanceCount': 1,
                },
                {
                    'Name': "Slave nodes",
                    'Market': 'ON_DEMAND',
                    'InstanceRole': 'CORE',
                    'InstanceType': 'm5.xlarge',
                    'InstanceCount': 2,
                }
            ],
            'Ec2KeyName': os.environ['EC2_KEY_NAME'],
            'KeepJobFlowAliveWhenNoSteps': True,
            'TerminationProtected': False,
            'Ec2SubnetId': os.environ['EC2_VPC_SUBNET'],
        },
        VisibleToAllUsers=True,
        JobFlowRole='EMR_EC2_DefaultRole',
        ServiceRole='EMR_DefaultRole',
    )

    cluster_id = emr_cluster['JobFlowId']

    cluster_status = emr_client.describe_cluster(ClusterId=cluster_id)['Cluster']['Status']['State']

    # while True:
    #     logging.info('Creating the cluster.')
    #     sleep(5)
    #     cluster_status = emr_client.describe_cluster(ClusterId='j-38L58ZV1AMT5C')['Cluster']['Status']['State']

    if cluster_status == 'STARTING':
        while True:
            logging.info('Creating the cluster.')
            sleep(5)
            cluster_status = emr_client.describe_cluster(ClusterId=cluster_id)['Cluster']['Status']['State']

            if cluster_status != 'STARTING':
                print(cluster_status)
                break
    else:
        print(cluster_status)
