from airflow import DAG
from airflow.providers.sftp.hooks.sftp import SFTPHook
from airflow.operators.python import PythonOperator
from airflow.models import Connection
from airflow import settings
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

source_sftp_connection_id = "my_source_sftp_ssh_conn"
dest_sftp_connection_id = "my_dest_sftp_ssh_conn"

dag = DAG('sftp_sync', 
          default_args=default_args,
          description='Sync files from source SFTP to target SFTP',
          schedule_interval=timedelta(days=1))


def list_files_from_source():
    source_hook = SFTPHook(ssh_conn_id=source_sftp_connection_id)
    files = source_hook.list_directory()
    return files


def transfer_files_to_target(files):
    target_hook = SFTPHook(ssh_conn_id=dest_sftp_connection_id)
    for file in files:
        source_path = f'sftp://source/{file}'
        target_path = f'sftp://target/{file}'
        target_hook.retrieve_file(source_path, target_path)


list_files_task = PythonOperator(
    task_id='list_files_from_source',
    python_callable=list_files_from_source,
    dag=dag,
)

transfer_files_task = PythonOperator(
    task_id='transfer_files_to_target',
    python_callable=transfer_files_to_target,
    op_args=[list_files_task.output],
    dag=dag,
)

list_files_task >> transfer_files_task
