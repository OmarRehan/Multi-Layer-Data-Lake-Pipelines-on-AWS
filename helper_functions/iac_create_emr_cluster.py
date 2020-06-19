import logging
from time import sleep
import boto3
import constants
import os

logging.basicConfig(level=logging.INFO, format="%(asctime)s:%(levelname)s:%(message)s")


def iac_create_emr_cluster():
    try:
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
            Configurations=[
                {
                    "Classification": "spark-env",
                    "Configurations": [
                        {
                            "Classification": "export",
                            "Properties": {
                                "PYSPARK_PYTHON": "/usr/bin/python3"
                            }
                        }
                    ]
                }
            ],
            BootstrapActions=[
                {
                    'Name': 'Install Python Packages',
                    'ScriptBootstrapAction': {
                        'Path': 's3://flights-dl-edge-node/BOOTSTRAP_ACTIONS/BOOTSTRAP_ACTIONS.sh'
                    },
                }
            ],
            VisibleToAllUsers=True,
            JobFlowRole='EMR_EC2_DefaultRole',
            ServiceRole='EMR_DefaultRole',
        )

        cluster_id = emr_cluster['JobFlowId']

        cluster_description = emr_client.describe_cluster(ClusterId=cluster_id)

        cluster_status = cluster_description['Cluster']['Status']['State']

        if cluster_status == 'STARTING':
            while True:
                logging.info('Creating the cluster.')
                sleep(5)
                cluster_status = emr_client.describe_cluster(ClusterId=cluster_id)['Cluster']['Status']['State']

                if cluster_status != 'STARTING' and cluster_status == 'WAITING':
                    cluster_dns = emr_client.describe_cluster(ClusterId=cluster_id)['Cluster']['MasterPublicDnsName']

                    logging.info('The Cluster is up and waiting')
                    break
                elif cluster_status != 'STARTING' and cluster_status in ('TERMINATING','TERMINATED','TERMINATED_WITH_ERRORS'):
                    raise Exception(f'Failed to start the Cluster, {cluster_description}')
        else:
            raise Exception(f'Failed to start the Cluster, {cluster_description}')

        return cluster_dns

    except Exception as e:
        logging.error(f'Failed to start the Cluster, {cluster_description}, {e}')


if __name__ == '__main__':
    iac_create_emr_cluster()
