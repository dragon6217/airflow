import pendulum
from hooks.s09_l50_custom_postgres_hook import CustomPostgresHook

from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator


with DAG(
        dag_id='s09_l50_dags_python_with_custom_hook_bulk_load',
        start_date=pendulum.datetime(2026, 1, 1, tz='Asia/Seoul'),
        schedule='0 7 * * *',
        catchup=False
) as dag:
    def insrt_postgres(postgres_conn_id, tbl_nm, file_nm, **kwargs):
        custom_postgres_hook = CustomPostgresHook(postgres_conn_id=postgres_conn_id)
        custom_postgres_hook.bulk_load(table_name=tbl_nm, file_name=file_nm, delimiter=',', is_header=True, is_replace=True)

    insrt_postgres = PythonOperator(
        task_id='insrt_postgres',
        python_callable=insrt_postgres,
        op_kwargs={'postgres_conn_id': 'conn_db_postgres_260103_airflow',
                   'tbl_nm':'TbCorona19CountStatus_bulk2',
                   'file_nm':'/opt/airflow/files/TbCorona19CountStatus/{{data_interval_end.in_timezone("Asia/Seoul") | ds_nodash}}/TbCorona19CountStatus.csv'}
    )