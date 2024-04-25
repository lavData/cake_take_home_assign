import datetime
import os
import logging
import psycopg2
from enum import Enum
from typing import Iterator
from urllib.parse import urlparse
from datetime import timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.sftp.hooks.sftp import SFTPHook
from airflow.providers.sftp.sensors.sftp import SFTPSensor

DEST_SFTP_CONNECTION_ID = "my_dest_sftp_ssh_conn"
SOURCE_SFTP_CONNECTION_ID = "my_source_sftp_ssh_conn"
STATE_TABLE_NAME = os.environ["SFTP_SYNC_STATE_TABLE"]
URI = os.environ["AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"]
STATE_SCHEMA_NAME = os.environ["SFTP_SYNC_STATE_SCHEMA"]

default_args = {
    "owner": "airflow",
    "depends_on_past": True,
    "start_date": datetime.datetime.today(),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "sftp_sync",
    default_args=default_args,
    description="Sync files from source SFTP to target SFTP",
    schedule_interval=timedelta(minutes=3),
)


class Transformer:
    """
    Base class for all transformers.
    If you want any custom transformer, you can inherit this class
    """

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


class Postgre:
    def __init__(self, uri, **kwargs):
        uri_parsed = urlparse(uri)
        password = uri_parsed.password
        database = uri_parsed.path[1:]
        hostname = uri_parsed.hostname
        port = uri_parsed.port if uri_parsed.port else 5432

        try:
            self.conn = psycopg2.connect(
                database=database,
                user=uri_parsed.username,
                password=password,
                host=hostname,
                port=port,
                options=f"-c search_path={STATE_SCHEMA_NAME}",
            )
        except psycopg2.Error as e:
            logging.error(f"Failed to connect to PostgreSQL: {e}")
            raise e

        self.cursor = self.conn.cursor()

        # Init state schema
        self.cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {STATE_SCHEMA_NAME};")
        # Init state table
        self.cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {STATE_SCHEMA_NAME}.{STATE_TABLE_NAME} (
                type_hook TEXT NOT NULL PRIMARY KEY,
                last_sync TEXT NOT NULL);
                """
        )

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def close(self, commit=True):
        if commit:
            self.conn.commit()
        self.cursor.close()
        self.conn.close()


class StateEngine:
    """
    StateEngine is a class that manages the state of the sync process.
    It stores the last sync time for each type of hook.
    It means that it can store the last sync time for SFTP, S3, etc.
    """

    min_date = "19700101000000"

    @classmethod
    def get_last_sync(cls, db: Postgre, table_name, type_hook: str):
        db.cursor.execute(
            f"""
        SELECT last_sync FROM {table_name} WHERE type_hook = '{type_hook}'
        """
        )
        last_sync = db.cursor.fetchone()
        last_sync = str(last_sync[0]) if last_sync else cls.min_date
        return last_sync

    @classmethod
    def update_last_sync(cls, db: Postgre, table_name, mod_time, type_hook: str):
        db.cursor.execute(
            f"""
        INSERT  INTO {table_name}(type_hook, last_sync) VALUES ('{type_hook}', '{mod_time}')
        ON CONFLICT (type_hook) DO UPDATE SET last_sync = '{mod_time}'
        """
        )
        db.conn.commit()


class TypeHook(Enum):
    SFTP = "sftp"
    S3 = "s3"


class Source:
    """
    Interface for all sources.
    It defines the methods that all sources should implement.
    """

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
        with self.hook.get_conn().open(file_path, mode="r") as file:
            while True:
                data = file.read(chunk_size)
                if not data:
                    break
                yield data

    def get_pay_load(self, file_path):
        return self.hook.get_conn().open(file_path).read().decode("utf-8")

    def get_new_files(self, path):
        new_files = []

        with Postgre(uri=URI) as db:
            last_sync = StateEngine.get_last_sync(
                db, STATE_TABLE_NAME, type_hook=self.type_hook
            )
            files = self.hook.list_directory(path)

            for file in files:
                if self.hook.get_mod_time(os.path.join(path, file)) > last_sync:
                    new_files.append(
                        [file, self.hook.get_mod_time(os.path.join(path, file))]
                    )
        logging.info(
            f"Dectected new files: {new_files} on path: {path}, last sync: {last_sync}"
        )
        return new_files

    def mark_file_processed(self, file_path):
        with Postgre(uri=URI) as db:
            StateEngine.update_last_sync(
                db, STATE_TABLE_NAME, self.hook.get_mod_time(file_path), self.type_hook
            )
        logging.info(
            f"Marked file {file_path} as processed at time {self.hook.get_mod_time(file_path)}"
        )

    def close(self):
        self.hook.close_conn()


class Destination:
    """
    Interface for all destinations.
    """

    conn_id = None

    def __init__(self, conn_id, **kwargs):
        pass

    def upload_file(self, file_path, payload):
        raise NotImplementedError

    def upload_chunk_file(self, file_path, payload: Iterator):
        raise NotImplementedError

    def close(self):
        raise NotImplementedError


class SFTPDestination(Destination):
    def __init__(self, conn_id, **kwargs):
        super().__init__(conn_id, **kwargs)
        self.hook = SFTPHook(ftp_conn_id=conn_id)

    def upload_file(self, file_path, payload):
        with self.hook.get_conn().open(file_path, mode="a") as file:
            file.write(payload)

        logging.info(f"Uploaded file {file_path} successfully")

    def upload_chunk_file(self, file_path, payload: Iterator):
        with (
            self.hook.get_conn().open(file_path, mode="a") as file_append,
            self.hook.get_conn().open(file_path, mode="w") as file_write,
        ):
            try:
                file_write.write(next(payload))
                for chunk in payload:
                    file_append.write(chunk)
            except Exception as e:
                logging.error(f"Failed to upload file {file_path}: {e}")
                self.hook.delete_file(file_path)
                raise e

        logging.info(f"Uploaded file {file_path} successfully")

    def close(self):
        self.hook.close_conn()


def detect_new_files(**kwargs):
    source_sftp = SFTPSource(conn_id=SOURCE_SFTP_CONNECTION_ID)
    task_instance = kwargs["ti"]
    input_path = kwargs["templates_dict"]["input_path"]

    new_files = source_sftp.get_new_files(path=input_path)
    task_instance.xcom_push(key="new_files_list", value=new_files)

    source_sftp.close()


def upload_files(**kwargs):
    dest_hook = SFTPDestination(conn_id=DEST_SFTP_CONNECTION_ID)
    src_hook = SFTPSource(conn_id=SOURCE_SFTP_CONNECTION_ID)
    transformer = kwargs["templates_dict"]["transform"]()
    output_path = kwargs["templates_dict"]["output_path"]
    input_path = kwargs["templates_dict"]["input_path"]
    task_instance = kwargs["ti"]

    payload_files = task_instance.xcom_pull(
        task_ids="detect_new_files", key="new_files_list"
    )

    for file_name, _ in sorted(payload_files, key=lambda x: x[1]):
        transformed_data = transformer.transform(
            src_hook.get_chunk_payload(os.path.join(input_path, file_name))
        )
        dest_hook.upload_chunk_file(
            os.path.join(output_path, file_name), transformed_data
        )
        src_hook.mark_file_processed(os.path.join(input_path, file_name))

    src_hook.close()
    dest_hook.close()


sensor_source = SFTPSensor(
    task_id="check_files_source",
    sftp_conn_id=SOURCE_SFTP_CONNECTION_ID,
    path="upload",
    poke_interval=10,
    dag=dag,
)

sensor_destination = SFTPSensor(
    task_id="check_files_destination",
    sftp_conn_id=DEST_SFTP_CONNECTION_ID,
    path="download",
    poke_interval=10,
    dag=dag,
)

detect_new_files = PythonOperator(
    task_id="detect_new_files",
    python_callable=detect_new_files,
    templates_dict={"input_path": "upload"},
    dag=dag,
)

upload_files = PythonOperator(
    task_id="upload_files",
    python_callable=upload_files,
    templates_dict={
        "output_path": "download",
        "input_path": "upload",
        "transform": UpperCaseTransformer,
    },
    dag=dag,
)

sensor_source >> sensor_destination >> detect_new_files >> upload_files
