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

default_spark_submit_cmd = """$SPARK_SUBMIT $FLIGHT_PROJECT_PATH/load_landing_zone/{script_name} --master yarn {args}"""

with DAG('load_landing_zone',
         default_args=default_args,
         concurrency=32,
         description='TBD',
         schedule_interval=None,
         start_date=datetime(2020, 4, 1)
         ) as main_dag:
    task_start_operator = DummyOperator(task_id='begin_execution')

    task_load_lz_flights = SSHOperator(
        task_id='load_lz_flights',
        ssh_hook=SSHHook(ssh_conn_id="ssh_default"),
        command=default_spark_submit_cmd.format(script_name='load_lz_flights.py', args='yearmonth={{ macros.ds_format(ds_nodash,"%Y%m%d", "%Y%m") }}'),
        trigger_rule='dummy'
    )

    task_load_lz_city_demographics = SSHOperator(
        task_id='load_lz_city_demographics',
        ssh_hook=SSHHook(ssh_conn_id="ssh_default"),
        command=default_spark_submit_cmd.format(script_name='load_lz_city_demographics.py', args=''),
        trigger_rule='dummy'
    )

    task_load_lz_l_airline_id = SSHOperator(
        task_id='load_lz_l_airline_id',
        ssh_hook=SSHHook(ssh_conn_id="ssh_default"),
        command=default_spark_submit_cmd.format(script_name='load_landing_zone_standard_lookup.py',
                                                args='table_name=L_AIRLINE_ID'),
        trigger_rule='dummy'
    )

    task_load_lz_l_deparrblk = SSHOperator(
        task_id='load_lz_l_deparrblk',
        ssh_hook=SSHHook(ssh_conn_id="ssh_default"),
        command=default_spark_submit_cmd.format(script_name='load_landing_zone_standard_lookup.py',
                                                args='table_name=L_DEPARRBLK'),
        trigger_rule='dummy'
    )

    task_load_lz_l_state_fips = SSHOperator(
        task_id='load_lz_l_state_fips',
        ssh_hook=SSHHook(ssh_conn_id="ssh_default"),
        command=default_spark_submit_cmd.format(script_name='load_landing_zone_standard_lookup.py',
                                                args='table_name=L_STATE_FIPS'),
        trigger_rule='dummy'
    )

    task_load_lz_l_airport = SSHOperator(
        task_id='load_lz_l_airport',
        ssh_hook=SSHHook(ssh_conn_id="ssh_default"),
        command=default_spark_submit_cmd.format(script_name='load_landing_zone_standard_lookup.py',
                                                args='table_name=L_AIRPORT')
    )

    task_load_lz_l_airport_id = SSHOperator(
        task_id='load_lz_l_airport_id',
        ssh_hook=SSHHook(ssh_conn_id="ssh_default"),
        command=default_spark_submit_cmd.format(script_name='load_landing_zone_standard_lookup.py',
                                                args='table_name=L_AIRPORT_ID'),
        trigger_rule='dummy'
    )

    task_load_lz_l_airport_seq_id = SSHOperator(
        task_id='load_lz_l_airport_seq_id',
        ssh_hook=SSHHook(ssh_conn_id="ssh_default"),
        command=default_spark_submit_cmd.format(script_name='load_landing_zone_standard_lookup.py',
                                                args='table_name=L_AIRPORT_SEQ_ID'),
        trigger_rule='dummy'
    )

    task_load_lz_l_cancellation = SSHOperator(
        task_id='load_lz_l_cancellation',
        ssh_hook=SSHHook(ssh_conn_id="ssh_default"),
        command=default_spark_submit_cmd.format(script_name='load_landing_zone_standard_lookup.py',
                                                args='table_name=L_CANCELLATION'),
        trigger_rule='dummy'
    )

    task_load_lz_l_months = SSHOperator(
        task_id='load_lz_l_months',
        ssh_hook=SSHHook(ssh_conn_id="ssh_default"),
        command=default_spark_submit_cmd.format(script_name='load_landing_zone_standard_lookup.py',
                                                args='table_name=L_MONTHS'),
        trigger_rule='dummy'
    )

    task_load_lz_l_world_area_codes = SSHOperator(
        task_id='load_lz_l_world_area_codes',
        ssh_hook=SSHHook(ssh_conn_id="ssh_default"),
        command=default_spark_submit_cmd.format(script_name='load_landing_zone_standard_lookup.py',
                                                args='table_name=L_WORLD_AREA_CODES'),
        trigger_rule='dummy'
    )

    task_load_lz_l_weekdays = SSHOperator(
        task_id='load_lz_l_weekdays',
        ssh_hook=SSHHook(ssh_conn_id="ssh_default"),
        command=default_spark_submit_cmd.format(script_name='load_landing_zone_standard_lookup.py',
                                                args='table_name=L_WEEKDAYS'),
        trigger_rule='dummy'
    )

    task_load_lz_l_diversions = SSHOperator(
        task_id='load_lz_l_diversions',
        ssh_hook=SSHHook(ssh_conn_id="ssh_default"),
        command=default_spark_submit_cmd.format(script_name='load_landing_zone_standard_lookup.py',
                                                args='table_name=L_DIVERSIONS'),
        trigger_rule='dummy'
    )

    task_load_lz_l_distance_group_250 = SSHOperator(
        task_id='load_lz_l_distance_group_250',
        ssh_hook=SSHHook(ssh_conn_id="ssh_default"),
        command=default_spark_submit_cmd.format(script_name='load_landing_zone_standard_lookup.py',
                                                args='table_name=L_DISTANCE_GROUP_250'),
        trigger_rule='dummy'
    )

    task_load_lz_l_unique_carriers = SSHOperator(
        task_id='load_lz_l_unique_carriers',
        ssh_hook=SSHHook(ssh_conn_id="ssh_default"),
        command=default_spark_submit_cmd.format(script_name='load_landing_zone_standard_lookup.py',
                                                args='table_name=L_UNIQUE_CARRIERS'),
        trigger_rule='dummy'
    )

    task_load_lz_l_state_abr_aviation = SSHOperator(
        task_id='load_lz_l_state_abr_aviation',
        ssh_hook=SSHHook(ssh_conn_id="ssh_default"),
        command=default_spark_submit_cmd.format(script_name='load_landing_zone_standard_lookup.py',
                                                args='table_name=L_STATE_ABR_AVIATION'),
        trigger_rule='dummy'
    )

    task_load_lz_l_city_market_id = SSHOperator(
        task_id='load_lz_l_city_market_id',
        ssh_hook=SSHHook(ssh_conn_id="ssh_default"),
        command=default_spark_submit_cmd.format(script_name='load_landing_zone_standard_lookup.py',
                                                args='table_name=L_CITY_MARKET_ID'),
        trigger_rule='dummy'
    )

    task_load_lz_l_ontime_delay_groups = SSHOperator(
        task_id='load_lz_l_ontime_delay_groups',
        ssh_hook=SSHHook(ssh_conn_id="ssh_default"),
        command=default_spark_submit_cmd.format(script_name='load_landing_zone_standard_lookup.py',
                                                args='table_name=L_ONTIME_DELAY_GROUPS'),
        trigger_rule='dummy'
    )

    task_load_lz_l_yesno_resp = SSHOperator(
        task_id='load_lz_l_yesno_resp',
        ssh_hook=SSHHook(ssh_conn_id="ssh_default"),
        command=default_spark_submit_cmd.format(script_name='load_landing_zone_standard_lookup.py',
                                                args='table_name=L_YESNO_RESP'),
        trigger_rule='dummy'
    )

    task_load_lz_l_carrier_history = SSHOperator(
        task_id='load_lz_l_carrier_history',
        ssh_hook=SSHHook(ssh_conn_id="ssh_default"),
        command=default_spark_submit_cmd.format(script_name='load_landing_zone_standard_lookup.py',
                                                args='table_name=L_CARRIER_HISTORY'),
        trigger_rule='dummy'
    )

    task_load_lz_l_quarters = SSHOperator(
        task_id='load_lz_l_quarters',
        ssh_hook=SSHHook(ssh_conn_id="ssh_default"),
        command=default_spark_submit_cmd.format(script_name='load_landing_zone_standard_lookup.py',
                                                args='table_name=L_QUARTERS'),
        trigger_rule='dummy'
    )

    task_end_operator = DummyOperator(task_id='end_execution')

    task_start_operator >> task_load_lz_flights >> task_load_lz_l_yesno_resp >> task_load_lz_l_quarters >> task_end_operator

    task_start_operator >> task_load_lz_city_demographics >> task_load_lz_l_airline_id >> task_load_lz_l_deparrblk >> task_load_lz_l_world_area_codes >> task_load_lz_l_weekdays >> task_load_lz_l_diversions >> task_end_operator

    task_start_operator >> task_load_lz_l_state_fips >> task_load_lz_l_airport >> task_load_lz_l_airport_id >> task_load_lz_l_distance_group_250 >> task_load_lz_l_unique_carriers >> task_load_lz_l_state_abr_aviation >> task_end_operator

    task_start_operator >> task_load_lz_l_airport_seq_id >> task_load_lz_l_cancellation >> task_load_lz_l_months >> task_load_lz_l_city_market_id >> task_load_lz_l_ontime_delay_groups >> task_load_lz_l_carrier_history >> task_end_operator
