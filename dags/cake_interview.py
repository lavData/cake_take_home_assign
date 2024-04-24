from airflow import DAG
import os
from airflow.providers.sftp.hooks.sftp import SFTPHook
from airflow.providers.sftp.sensors.sftp import SFTPSensor
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.amazon.aws.operators.s3 import S3CreateBucketOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import json
import sqlite3

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


class StateEngine:
    db_name = 'state.db'
    table_name = 'sftp_sync'

    @classmethod
    def detect_new_file(cls, hook: SFTPHook, path):
        conn = sqlite3.connect(cls.db_name)
        cursor = conn.cursor()
        cursor.execute(f"""
        SELECT last_sync FROM {cls.table_name}
        """)
        last_sync = str(cursor.fetchone()[0])
        files = hook.list_directory(path)
        print(files)
        new_files = []

        for file in files:
            print(files, 'modify time', hook.get_mod_time(os.path.join(path, file)), 'last sync', last_sync)
            if hook.get_mod_time(os.path.join(path, file)) > last_sync:
                new_files.append(file)
        print(new_files)
        conn.close()
        return new_files

    @classmethod
    def update_last_sync(cls, hook: SFTPHook, path, file_name):
        conn = sqlite3.connect(cls.db_name)
        cursor = conn.cursor()
        cursor.execute(f"""
        UPDATE {cls.table_name}
        SET last_sync = {hook.get_mod_time(os.path.join(path, file_name))}
        """)
        conn.commit()
        conn.close()


def download_file_func(task_instance, **kwargs):
    stfp_hook = SFTPHook(ftp_conn_id=source_sftp_connection_id)
    input_path = kwargs['templates_dict']['input_path']

    payload_files = {}

    for file in StateEngine.detect_new_file(stfp_hook, input_path):
        payload_files[file] = stfp_hook.get_conn().open(os.path.join(input_path, file)).read().decode('utf-8')

    task_instance.xcom_push(key='raw_input_file', value=payload_files)


def upload_file_func(**kwargs):
    stfp_hook = SFTPHook(ftp_conn_id=dest_sftp_connection_id)
    output_path = kwargs['templates_dict']['output_path']
    ti = kwargs['ti']

    payload_files = ti.xcom_pull(task_ids="process_files", key="transformed_file")
    for file_name, payload in payload_files.items():
        stfp_client = stfp_hook.get_conn().open(os.path.join(output_path, file_name), mode='w')
        stfp_client.write(payload)

        StateEngine.update_last_sync(stfp_hook, output_path, file_name)
        stfp_client.close()


def process_file_func(**kwargs):
    ti = kwargs['ti']
    payload_files = ti.xcom_pull(task_ids="download_files", key="raw_input_file")
    ti.xcom_push(key="transformed_file", value=payload_files)


sensor = SFTPSensor(task_id="check-for-file",
                    sftp_conn_id=source_sftp_connection_id,
                    path="upload",
                    poke_interval=10,
                    dag=dag)

download_files = PythonOperator(task_id='download_files',
                                python_callable=download_file_func,
                                templates_dict={'input_path': 'upload'},
                                dag=dag)

process_file = PythonOperator(task_id="process_files",
                              python_callable=process_file_func,
                              dag=dag)

upload_files = PythonOperator(task_id='upload_files',
                              python_callable=upload_file_func,
                              templates_dict={'output_path': 'download'},
                              dag=dag)


sensor >> download_files >> process_file >> upload_files
