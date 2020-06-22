from datetime import datetime
from airflow import DAG
from airflow.contrib.hooks.ssh_hook import SSHHook
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.hooks.base_hook import BaseHook
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
import constants
from helper_functions import create_s3_bucket, create_s3_directories, iac_create_emr_cluster
from airflow.operators.bash_operator import BashOperator
from airflow.hooks.base_hook import BaseHook
from airflow import settings
import os


def update_cluster_connection(**kwargs):
    ti = kwargs['ti']

    emr_dns = ti.xcom_pull(task_ids='iac_create_emr_cluster')

    ssh_conn = BaseHook.get_connection('ssh_default')
    ssh_conn.host = emr_dns

    session = settings.Session()  # get the session
    session.add(ssh_conn)
    session.commit()


ssh_emr_host = BaseHook.get_connection('ssh_default').host
ssh_emr_key = BaseHook.get_connection('ssh_default').extra_dejson.get('key_file')
ssh_emr_user = BaseHook.get_connection('ssh_default').login
files_to_upload = '{constants.py,create_integration_layer.py,create_landing_zone.py,create_presentation_layer.py,' \
                  'helper_functions,load_integration_layer,load_landing_zone,load_presentation_layer,sql_queries}'

spark_master = 'yarn'

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
         concurrency=32,
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

    task_create_emr_cluster = PythonOperator(
        task_id='iac_create_emr_cluster',
        python_callable=iac_create_emr_cluster.iac_create_emr_cluster
    )

    task_update_cluster_connection = PythonOperator(
        task_id='update_cluster_connection',
        python_callable=update_cluster_connection,
        provide_context=True
    )

    task_upload_env_vars = BashOperator(
        task_id='upload_aws_env_vars_file',
        bash_command=f"scp -vi {ssh_emr_key} -o StrictHostKeyChecking=no $FLIGHT_PROJECT_PATH/aws_env_vars.sh {ssh_emr_user}@{ssh_emr_host}:~/"
    )

    task_move_env_vars_file = SSHOperator(
        task_id='move_env_vars_file_to_profile_dir',
        ssh_hook=SSHHook(ssh_conn_id="ssh_default"),
        command='sudo mv aws_env_vars.sh /etc/profile.d/'
    )

    task_mk_project_dir = SSHOperator(
        task_id='mk_project_dir',
        ssh_hook=SSHHook(ssh_conn_id="ssh_default"),
        command="""mkdir $FLIGHT_PROJECT_PATH"""
    )

    # TODO : Solve the static path issue
    task_upload_project_files = BashOperator(
        task_id='upload_project_files',
        bash_command=f"scp -vri {ssh_emr_key} -o StrictHostKeyChecking=no $FLIGHT_PROJECT_PATH/{files_to_upload} {ssh_emr_user}@{ssh_emr_host}:/home/hadoop/FLIGHTS_PROJECT"
    )

    task_create_landing_zone = SSHOperator(
        task_id='create_landing_zone',
        ssh_hook=SSHHook(ssh_conn_id="ssh_default"),
        command=f"""$SPARK_SUBMIT $FLIGHT_PROJECT_PATH/create_landing_zone.py --master {spark_master}"""
    )

    task_create_integration_layer = SSHOperator(
        task_id='create_integration_layer',
        ssh_hook=SSHHook(ssh_conn_id="ssh_default"),
        command=f"""$SPARK_SUBMIT $FLIGHT_PROJECT_PATH/create_integration_layer.py --master {spark_master}"""
    )

    task_create_presentation_layer = SSHOperator(
        task_id='create_presentation_layer',
        ssh_hook=SSHHook(ssh_conn_id="ssh_default"),
        command=f"""$SPARK_SUBMIT $FLIGHT_PROJECT_PATH/create_presentation_layer.py --master {spark_master}"""
    )

    task_start_operator >> task_create_bucket >> task_create_s3_directories >> task_create_emr_cluster >> task_update_cluster_connection >> task_upload_env_vars >> task_move_env_vars_file >> \
        task_mk_project_dir >> task_upload_project_files >> [task_create_landing_zone,task_create_integration_layer,task_create_presentation_layer]
