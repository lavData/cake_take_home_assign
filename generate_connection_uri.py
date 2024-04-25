from airflow.models import Connection

source_sftp_connection_id = "my_source_sftp_ssh_conn"

source_conn = Connection(
    conn_id=source_sftp_connection_id,
    conn_type='SSH',
    host="sftp",
    login="source",
    password="src",
    port=22
)

print(f"AIRFLOW_CONN_{source_conn.conn_id.upper()}='{source_conn.get_uri()}'")

dest_sftp_connection_id = "my_dest_sftp_ssh_conn"

dest_conn = Connection(
    conn_id=dest_sftp_connection_id,
    conn_type="SSH",
    password="dst",
    host="sftp",
    login="destination",
    port=22
)

print(f"AIRFLOW_CONN_{dest_conn.conn_id.upper()}='{dest_conn.get_uri()}'")