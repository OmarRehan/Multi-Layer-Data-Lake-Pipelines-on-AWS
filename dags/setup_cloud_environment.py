from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
import constants
from helper_functions import create_s3_bucket, create_s3_directories
from airflow.operators.bash_operator import BashOperator

default_args = {
    'owner': 'flights_dl',
    'depends_on_past': False,
    'retries': 0,
    'catchup': False,
    'email_on_retry': False,
    'concurrency': 3
}

with DAG('setup_cloud_environment',
         default_args=default_args,
         description='TBD',
         schedule_interval=None,
         start_date=datetime(2018, 11, 3)
         ) as main_dag:
    task_start_operator = DummyOperator(task_id='Begin_execution')

    task_create_bucket = PythonOperator(
        task_id='create_s3_bucket',
        python_callable=create_s3_bucket.create_bucket,
        provide_context=True,
        params={'region': constants.aws_region, 'bucket_name': constants.s3_bucket_name}
    )

    task_create_s3_directories = PythonOperator(
        task_id='create_s3_directories',
        python_callable=create_s3_directories.create_s3_directories
    )

    task_create_emr_cluster = BashOperator(task_id='iac_create_emr_cluster',
                                           bash_command='python $FLIGHT_PROJECT_PATH/helper_functions/iac_create_emr_cluster.py')

    task_start_operator >> task_create_bucket >> task_create_s3_directories
    task_start_operator >> task_create_emr_cluster
