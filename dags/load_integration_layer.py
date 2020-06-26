from datetime import datetime

from airflow import DAG
from airflow.contrib.hooks.ssh_hook import SSHHook
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.operators.dummy_operator import DummyOperator

default_args = {
    'owner': 'flights_dl',
    'depends_on_past': True,
    'retries': 0,
    'catchup': False,
    'email_on_retry': False,
    'concurrency': 3
}

default_spark_submit_cmd = """$SPARK_SUBMIT $FLIGHT_PROJECT_PATH/load_integration_layer/{script_name} --master yarn {args}"""


with DAG('load_integration_layer',
         default_args=default_args,
         concurrency=32,
         description='loads all flights table in the integration layer, get triggered by the load_landing_zone DAG',
         schedule_interval=None,
         start_date=datetime(2019, 1, 1)
         ) as main_dag:
    task_start_operator = DummyOperator(task_id='begin_execution')

    task_load_il_flights = SSHOperator(
        task_id='load_il_flights',
        ssh_hook=SSHHook(ssh_conn_id="ssh_default"),
        command=default_spark_submit_cmd.format(script_name='load_il_flights.py',
                                                args='year={{ macros.ds_format(ds_nodash,"%Y%m%d", "%Y") }} month={{ macros.ds_format(ds_nodash,"%Y%m%d", "%m") }}'),
        trigger_rule='dummy'
    )

    task_load_il_city_demographics = SSHOperator(
        task_id='load_il_city_demographics',
        ssh_hook=SSHHook(ssh_conn_id="ssh_default"),
        command=default_spark_submit_cmd.format(script_name='load_il_city_demographics.py', args=''),
        trigger_rule='dummy'
    )

    task_load_il_l_airline_id = SSHOperator(
        task_id='load_il_l_airline_id',
        ssh_hook=SSHHook(ssh_conn_id="ssh_default"),
        command=default_spark_submit_cmd.format(script_name='load_il_l_airline_id.py', args=''),
        trigger_rule='dummy'
    )

    task_load_il_l_deparrblk = SSHOperator(
        task_id='load_il_l_deparrblk',
        ssh_hook=SSHHook(ssh_conn_id="ssh_default"),
        command=default_spark_submit_cmd.format(script_name='load_il_l_deparrblk.py', args=''),
        trigger_rule='dummy'
    )

    task_load_il_l_state_fips = SSHOperator(
        task_id='load_il_l_state_fips',
        ssh_hook=SSHHook(ssh_conn_id="ssh_default"),
        command=default_spark_submit_cmd.format(script_name='load_il_l_state_fips.py', args=''),
        trigger_rule='dummy'
    )

    task_load_il_l_airport = SSHOperator(
        task_id='load_il_l_airport',
        ssh_hook=SSHHook(ssh_conn_id="ssh_default"),
        command=default_spark_submit_cmd.format(script_name='load_il_l_airport.py', args=''),
        trigger_rule='dummy'
    )

    task_load_il_l_airport_id = SSHOperator(
        task_id='load_il_l_airport_id',
        ssh_hook=SSHHook(ssh_conn_id="ssh_default"),
        command=default_spark_submit_cmd.format(script_name='load_il_l_airport_id.py', args=''),
        trigger_rule='dummy'
    )

    task_load_il_l_airport_seq_id = SSHOperator(
        task_id='load_il_l_airport_seq_id',
        ssh_hook=SSHHook(ssh_conn_id="ssh_default"),
        command=default_spark_submit_cmd.format(script_name='load_il_l_airport_seq_id.py', args=''),
        trigger_rule='dummy'
    )

    task_load_il_l_cancellation = SSHOperator(
        task_id='load_il_l_cancellation',
        ssh_hook=SSHHook(ssh_conn_id="ssh_default"),
        command=default_spark_submit_cmd.format(script_name='load_il_l_cancellation.py', args=''),
        trigger_rule='dummy'
    )

    task_load_il_l_months = SSHOperator(
        task_id='load_il_l_months',
        ssh_hook=SSHHook(ssh_conn_id="ssh_default"),
        command=default_spark_submit_cmd.format(script_name='load_il_l_months.py', args=''),
        trigger_rule='dummy'
    )

    task_load_il_l_world_area_codes = SSHOperator(
        task_id='load_il_l_world_area_codes',
        ssh_hook=SSHHook(ssh_conn_id="ssh_default"),
        command=default_spark_submit_cmd.format(script_name='load_il_l_world_area_codes.py', args=''),
        trigger_rule='dummy'
    )

    task_load_il_l_weekdays = SSHOperator(
        task_id='load_il_l_weekdays',
        ssh_hook=SSHHook(ssh_conn_id="ssh_default"),
        command=default_spark_submit_cmd.format(script_name='load_il_l_weekdays.py', args=''),
        trigger_rule='dummy'
    )

    task_load_il_l_diversions = SSHOperator(
        task_id='load_il_l_diversions',
        ssh_hook=SSHHook(ssh_conn_id="ssh_default"),
        command=default_spark_submit_cmd.format(script_name='load_il_l_diversions.py', args=''),
        trigger_rule='dummy'
    )

    task_load_il_l_distance_group_250 = SSHOperator(
        task_id='load_il_l_distance_group_250',
        ssh_hook=SSHHook(ssh_conn_id="ssh_default"),
        command=default_spark_submit_cmd.format(script_name='load_il_l_distance_group_250.py', args=''),
        trigger_rule='dummy'
    )

    task_load_il_l_unique_carriers = SSHOperator(
        task_id='load_il_l_unique_carriers',
        ssh_hook=SSHHook(ssh_conn_id="ssh_default"),
        command=default_spark_submit_cmd.format(script_name='load_il_l_unique_carriers.py', args=''),
        trigger_rule='dummy'
    )

    task_load_il_l_state_abr_aviation = SSHOperator(
        task_id='load_il_l_state_abr_aviation',
        ssh_hook=SSHHook(ssh_conn_id="ssh_default"),
        command=default_spark_submit_cmd.format(script_name='load_il_l_state_abr_aviation.py', args=''),
        trigger_rule='dummy'
    )

    task_load_il_l_city_market = SSHOperator(
        task_id='load_il_l_city_market',
        ssh_hook=SSHHook(ssh_conn_id="ssh_default"),
        command=default_spark_submit_cmd.format(script_name='load_il_l_city_market.py', args=''),
        trigger_rule='dummy'
    )

    task_load_il_l_ontime_delay_groups = SSHOperator(
        task_id='load_il_l_ontime_delay_groups',
        ssh_hook=SSHHook(ssh_conn_id="ssh_default"),
        command=default_spark_submit_cmd.format(script_name='load_il_l_ontime_delay_groups.py', args=''),
        trigger_rule='dummy'
    )

    task_load_il_l_yesno_resp = SSHOperator(
        task_id='load_il_l_yesno_resp',
        ssh_hook=SSHHook(ssh_conn_id="ssh_default"),
        command=default_spark_submit_cmd.format(script_name='load_il_l_yesno_resp.py', args=''),
        trigger_rule='dummy'
    )

    task_load_il_l_carrier_history = SSHOperator(
        task_id='load_il_l_carrier_history',
        ssh_hook=SSHHook(ssh_conn_id="ssh_default"),
        command=default_spark_submit_cmd.format(script_name='load_il_l_carrier_history.py', args=''),
        trigger_rule='dummy'
    )

    task_load_il_l_quarters = SSHOperator(
        task_id='load_il_l_quarters',
        ssh_hook=SSHHook(ssh_conn_id="ssh_default"),
        command=default_spark_submit_cmd.format(script_name='load_il_l_quarters.py', args=''),
        trigger_rule='dummy'
    )

    task_check_il_counts = SSHOperator(
        task_id='check_il_counts',
        ssh_hook=SSHHook(ssh_conn_id="ssh_default"),
        command=default_spark_submit_cmd.format(script_name='il_data_quality_checks/check_il_counts.py', args='')
    )

    task_end_operator = DummyOperator(task_id='end_execution')

    task_trigger_load_presentation_layer = TriggerDagRunOperator(
        task_id="trigger_load_presentation_layer",
        trigger_dag_id="load_presentation_layer",
        execution_date="{{ execution_date }}"
    )

    task_start_operator >> task_load_il_flights >> task_load_il_l_yesno_resp >> task_load_il_l_quarters >> task_check_il_counts

    task_start_operator >> task_load_il_city_demographics >> task_load_il_l_airline_id >> task_load_il_l_deparrblk >> task_load_il_l_world_area_codes >> task_load_il_l_weekdays >> task_load_il_l_diversions >> task_check_il_counts

    task_start_operator >> task_load_il_l_state_fips >> task_load_il_l_airport >> task_load_il_l_airport_id >> task_load_il_l_distance_group_250 >> task_load_il_l_unique_carriers >> task_load_il_l_state_abr_aviation >> task_check_il_counts

    task_start_operator >> task_load_il_l_airport_seq_id >> task_load_il_l_cancellation >> task_load_il_l_months >> task_load_il_l_city_market >> task_load_il_l_ontime_delay_groups >> task_load_il_l_carrier_history >> task_check_il_counts

    task_check_il_counts >> task_end_operator >> task_trigger_load_presentation_layer
