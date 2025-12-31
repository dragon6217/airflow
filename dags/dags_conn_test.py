from airflow import DAG
import pendulum, datetime
from airflow.providers.standard.operators.empty import EmptyOperator

with DAG(
    dag_id="dags_conn_test",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Seoul"),
    catchup=False,
) as dag:
    empty_t1 = EmptyOperator(
        task_id="empty_t1",
    )
    empty_t2 = EmptyOperator(
        task_id="empty_t2",
    )
    empty_t3 = EmptyOperator(
        task_id="empty_t3",
    )
    empty_t4 = EmptyOperator(
        task_id="empty_t4",
    )
    empty_t5 = EmptyOperator(
        task_id="empty_t5",
    )
    empty_t6 = EmptyOperator(
        task_id="empty_t6",
    )
    empty_t7 = EmptyOperator(
        task_id="empty_t7",
    )
    empty_t8 = EmptyOperator(
        task_id="empty_t8",
    )

    empty_t1 >> [empty_t2, empty_t3] >> empty_t4
    empty_t5 >> empty_t4
    [empty_t4, empty_t7] >> empty_t6 >> empty_t8