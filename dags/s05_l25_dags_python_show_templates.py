import pendulum
from airflow.sdk import DAG, task


with DAG(
    dag_id="s05_l25_dags_python_show_templates",
    schedule="30 9 * * *",
    start_date=pendulum.datetime(2025, 12, 25, tz="Asia/Seoul"),
    catchup=True
) as dag:
    
    @task(task_id='python_task')
    def show_templates(**kwargs):
        from pprint import pprint 
        pprint(kwargs)

    show_templates()