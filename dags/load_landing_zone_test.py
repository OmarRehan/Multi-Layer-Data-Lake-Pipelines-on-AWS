from datetime import datetime

from airflow import DAG
from airflow.contrib.hooks.ssh_hook import SSHHook
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator

default_args = {
    'owner': 'flights_dl',
    'depends_on_past': False,
    'retries': 0,
    'catchup': False,
    'email_on_retry': False,
    'concurrency': 3
}

default_spark_submit_cmd = """$SPARK_SUBMIT $FLIGHT_PROJECT_PATH/load_landing_zone/{script_name} --master yarn {args}"""

with DAG('load_landing_zone_test',
         default_args=default_args,
         concurrency=32,
         description='TBD',
         schedule_interval=None,
         start_date=datetime(2020, 4, 1)
         ) as main_dag:
    task_load_lz_flights = BashOperator(
        task_id='load_lz_flights',
        bash_command='echo '+default_spark_submit_cmd.format(script_name='load_lz_flights.py', args='year={{ macros.ds_format(ds_nodash,"%Y%m%d", "%Y") }} month={{ macros.ds_format(ds_nodash,"%Y%m%d", "%m") }}'),
        trigger_rule='dummy'
    )
