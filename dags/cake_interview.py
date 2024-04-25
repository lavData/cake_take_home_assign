from airflow import DAG
from enum import Enum
import os
from airflow.providers.sftp.hooks.sftp import SFTPHook
from airflow.providers.sftp.sensors.sftp import SFTPSensor
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from typing import Iterator
import sqlite3

default_args = {
    'owner': 'airflow',
    'depends_on_past': True,
    'start_date': datetime(2024, 4, 24),
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


class Transformer:
    def transform(self, payload):
        raise NotImplementedError


class UpperCaseTransformer(Transformer):
    def transform(self, payload: Iterator):
        for chunk in payload:
            yield chunk.upper()


class LowerCaseTransformer(Transformer):
    def transform(self, payload):
        return payload.lower()


class NoOpTransformer(Transformer):
    def transform(self, payload):
        return payload


class TypeHook(Enum):
    SFTP = 'sftp'
    S3 = 's3'


class StateEngine:
    db_name = 'state.db'
    table_name = 'state'
    min_date = '19700101000000'

    @classmethod
    def get_last_sync(cls, type_hook: str):
        conn = sqlite3.connect(cls.db_name)
        cursor = conn.cursor()
        cursor.execute(f"""
        SELECT last_sync FROM {cls.table_name} WHERE type_hook = '{type_hook}'
        """)
        last_sync = cursor.fetchone()
        last_sync = str(last_sync[0]) if last_sync else cls.min_date

        conn.close()
        return last_sync

    @classmethod
    def update_last_sync(cls, mod_time, type_hook: str):
        conn = sqlite3.connect(cls.db_name)
        cursor = conn.cursor()
        cursor.execute(f"""
        INSERT OR REPLACE INTO {cls.table_name}(type_hook, last_sync)
        VALUES ('{type_hook}', '{mod_time}')
        """)
        conn.commit()
        conn.close()


class Source:
    conn_id = None
    type_hook: str

    def __init__(self, conn_id, **kwargs):
        pass

    def get_files(self, path):
        raise NotImplementedError

    def get_mod_time(self, file_path):
        raise NotImplementedError

    def get_pay_load(self, file_path):
        raise NotImplementedError

    def get_new_files(self, path):
        raise NotImplementedError

    def mark_file_processed(self, file_path):
        raise NotImplementedError

    def close(self):
        raise NotImplementedError


class SFTPSource(Source):
    type_hook = TypeHook.SFTP.value

    def __init__(self, conn_id, **kwargs):
        super().__init__(conn_id, **kwargs)
        self.hook = SFTPHook(ftp_conn_id=conn_id)

    def get_files(self, path):
        return self.hook.list_directory(path)

    def get_mod_time(self, file_path):
        return self.hook.get_mod_time(file_path)

    def get_chunk_payload(self, file_path, chunk_size=1024):
        with self.hook.get_conn().open(file_path, mode='r') as file:
            while True:
                data = file.read(chunk_size)
                if not data:
                    break
                yield data

    def get_pay_load(self, file_path):
        return self.hook.get_conn().open(file_path).read().decode('utf-8')

    def get_new_files(self, path):
        last_sync = StateEngine.get_last_sync(type_hook=self.type_hook)
        files = self.hook.list_directory(path)
        new_files = []

        for file in files:
            if self.hook.get_mod_time(os.path.join(path, file)) > last_sync:
                new_files.append([file, self.hook.get_mod_time(os.path.join(path, file))])
        return new_files

    def mark_file_processed(self, file_path):
        StateEngine.update_last_sync(self.hook.get_mod_time(file_path), self.type_hook)

    def close(self):
        self.hook.close_conn()


def download_file_func(task_instance, **kwargs):
    source_sftp = SFTPSource(conn_id=source_sftp_connection_id)
    input_path = kwargs['templates_dict']['input_path']

    new_files = source_sftp.get_new_files(path=input_path)
    task_instance.xcom_push(key='raw_input_file', value=new_files)

    source_sftp.close()


def process_file_func(**kwargs):
    ti = kwargs['ti']
    transformer = kwargs['templates_dict']['transform']()
    payload_files = ti.xcom_pull(task_ids="download_files", key="raw_input_file")
    for index, (file_name, _,  payload) in enumerate(payload_files):
        payload_files[index][2] = transformer.transform(payload)
    ti.xcom_push(key="transformed_file", value=payload_files)


def upload_file_func(**kwargs):
    dest_hook = SFTPHook(ftp_conn_id=dest_sftp_connection_id)
    src_hook = SFTPSource(conn_id=source_sftp_connection_id)
    output_path = kwargs['templates_dict']['output_path']
    input_path = kwargs['templates_dict']['input_path']

    ti = kwargs['ti']
    payload_files = ti.xcom_pull(task_ids="download_files", key="raw_input_file")

    for file_name, _ in sorted(payload_files, key=lambda x: x[1]):
        with dest_hook.get_conn().open(os.path.join(output_path, file_name), mode='a') as file:
            for chunk in src_hook.get_chunk_payload(os.path.join(input_path, file_name)):
                file.write(chunk)
        src_hook.mark_file_processed(os.path.join(input_path, file_name))

    src_hook.close()
    dest_hook.close_conn()


sensor = SFTPSensor(task_id="check-for-file",
                    sftp_conn_id=source_sftp_connection_id,
                    path="upload",
                    poke_interval=10,
                    dag=dag)

download_files = PythonOperator(task_id='download_files',
                                python_callable=download_file_func,
                                templates_dict={'input_path': 'upload'},
                                dag=dag)

# process_file = PythonOperator(task_id="process_files",
#                               python_callable=process_file_func,
#                               dag=dag,
#                               templates_dict={'transform': UpperCaseTransformer}
#                               )

upload_files = PythonOperator(task_id='upload_files',
                              python_callable=upload_file_func,
                              templates_dict={'output_path': 'download',
                                              'input_path': 'upload'
                                              },
                              dag=dag)

sensor >> download_files  >> upload_files
