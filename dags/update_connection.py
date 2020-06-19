from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.hooks.ssh_hook import SSHHook
from airflow.contrib.operators.ssh_operator import SSHOperator
import datetime


def return_dns():
    return 'ec2-52-38-162-194.us-west-2.compute.amazonaws.com'


def update_connection(**kwargs):
    from airflow.hooks.base_hook import BaseHook
    from airflow import settings
    from airflow.models import Connection

    ti = kwargs['ti']

    emr_dns = ti.xcom_pull(task_ids='return_dns')

    print(emr_dns)

    ssh_conn = BaseHook.get_connection('ssh_default')
    ssh_conn.host = emr_dns
    session = settings.Session()  # get the session
    session.add(ssh_conn)
    session.commit()

    # print(BaseHook.get_connection('ssh_default').host)
    # print(BaseHook.get_connection('ssh_default').extra)
    # print(BaseHook.get_connection('ssh_default').extra_dejson)
    # print(BaseHook.get_connection('ssh_default').login)


dag = DAG(
    'update_connection',
    description='DAG Desc',
    start_date=datetime.datetime(2020, 5, 1),
    schedule_interval=None
)

task_1 = PythonOperator(
    task_id='return_dns',
    python_callable=return_dns,
    dag=dag)

task_2 = PythonOperator(
    task_id='update_connection',
    python_callable=update_connection,
    dag=dag,
    provide_context=True)

task_1 >> task_2
