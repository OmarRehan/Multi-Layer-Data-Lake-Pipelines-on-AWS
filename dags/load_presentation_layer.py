from datetime import datetime

from airflow import DAG
from airflow.contrib.hooks.ssh_hook import SSHHook
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.operators.dummy_operator import DummyOperator

default_args = {
    'owner': 'flights_dl',
    'depends_on_past': False,
    'retries': 0,
    'catchup': False,
    'email_on_retry': False,
    'concurrency': 3
}

default_spark_submit_cmd = """$SPARK_SUBMIT $FLIGHT_PROJECT_PATH/load_presentation_layer/{script_name} --master yarn {args}"""

with DAG('load_presentation_layer',
         default_args=default_args,
         concurrency=32,
         description='TBD',
         schedule_interval=None,
         start_date=datetime(2020, 4, 1)
         ) as main_dag:
    task_start_operator = DummyOperator(task_id='begin_execution')

    task_load_pl_flights = SSHOperator(
        task_id='load_pl_flights',
        ssh_hook=SSHHook(ssh_conn_id="ssh_default"),
        command=default_spark_submit_cmd.format(script_name='load_pl_flights.py',
                                                args='yearmonth={{ macros.ds_format(ds_nodash,"%Y%m%d", "%Y%m") }}')
    )

    task_load_pl_airline = SSHOperator(
        task_id='load_pl_airline',
        ssh_hook=SSHHook(ssh_conn_id="ssh_default"),
        command=default_spark_submit_cmd.format(script_name='load_pl_airline.py', args='')
    )

    task_load_pl_airport = SSHOperator(
        task_id='load_pl_airport',
        ssh_hook=SSHHook(ssh_conn_id="ssh_default"),
        command=default_spark_submit_cmd.format(script_name='load_pl_airport.py', args='')
    )

    task_load_pl_calendar = SSHOperator(
        task_id='load_pl_calendar',
        ssh_hook=SSHHook(ssh_conn_id="ssh_default"),
        command=default_spark_submit_cmd.format(script_name='load_pl_calendar.py', args='start_date={{ ds }} end_date={{ ds }}')
    )

    task_load_pl_cancellation = SSHOperator(
        task_id='load_pl_cancellation',
        ssh_hook=SSHHook(ssh_conn_id="ssh_default"),
        command=default_spark_submit_cmd.format(script_name='load_pl_cancellation.py', args='')
    )

    task_load_pl_city = SSHOperator(
        task_id='load_pl_city',
        ssh_hook=SSHHook(ssh_conn_id="ssh_default"),
        command=default_spark_submit_cmd.format(script_name='load_pl_city.py', args='')
    )

    task_load_pl_city_demographics = SSHOperator(
        task_id='load_pl_city_demographics',
        ssh_hook=SSHHook(ssh_conn_id="ssh_default"),
        command=default_spark_submit_cmd.format(script_name='load_pl_city_demographics.py', args='')
    )

    task_load_pl_state = SSHOperator(
        task_id='load_pl_state',
        ssh_hook=SSHHook(ssh_conn_id="ssh_default"),
        command=default_spark_submit_cmd.format(script_name='load_pl_state.py', args='')
    )

    task_load_pl_world_area_codes = SSHOperator(
        task_id='load_pl_world_area_codes',
        ssh_hook=SSHHook(ssh_conn_id="ssh_default"),
        command=default_spark_submit_cmd.format(script_name='load_pl_world_area_codes.py', args='')
    )

    task_check_pl_counts = SSHOperator(
        task_id='check_pl_counts',
        ssh_hook=SSHHook(ssh_conn_id="ssh_default"),
        command=default_spark_submit_cmd.format(script_name='pl_data_quality_checks/check_pl_counts.py', args='')
    )

    task_end_operator = DummyOperator(task_id='end_execution')

    task_start_operator >> task_load_pl_cancellation >> task_load_pl_flights

    task_start_operator >> task_load_pl_world_area_codes >> task_load_pl_state >> task_load_pl_city >>\
        [task_load_pl_airport,task_load_pl_city_demographics] >> task_load_pl_flights

    task_start_operator >> task_load_pl_airline >> task_load_pl_flights

    task_start_operator >> task_load_pl_calendar >> task_load_pl_flights

    task_load_pl_flights >> task_check_pl_counts >> task_end_operator
