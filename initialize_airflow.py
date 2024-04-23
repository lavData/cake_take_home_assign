from airflow import settings
from airflow.models import Connection


source_sftp_connection_id = "my_source_sftp_ssh_conn"

source_conn = Connection(
    conn_id=source_sftp_connection_id,
    conn_type='SSH',
    host="localhost",
    login="foo",
    password="pass",
    port=22
)

dest_sftp_connection_id = "my_dest_sftp_ssh_conn"

dest_conn = Connection(
    conn_id=dest_sftp_connection_id,
    conn_type="SSH",
    host="localhost",
    login="foo",
    password="pass",
    port=22
)

session = settings.Session()
session.add(source_conn)
session.add(dest_conn)
session.commit()
source_sftp_connection_id = "my_source_sftp_ssh_conn"

source_conn = Connection(
    conn_id=source_sftp_connection_id,
    conn_type='SSH',
    host="localhost",
    login="foo",
    password="pass",
    port=22
)

dest_sftp_connection_id = "my_dest_sftp_ssh_conn"

dest_conn = Connection(
    conn_id=dest_sftp_connection_id,
    conn_type="SSH",
    host="localhost",
    login="foo",
    password="pass",
    port=22
)

session = settings.Session()
session.add(source_conn)
session.add(dest_conn)
session.commit()
