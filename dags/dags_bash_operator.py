from airflow import DAG
import pendulum, datetime
#from airflow.operators.bash import BashOperator
from airflow.providers.standard.operators.bash import BashOperator

with DAG(
    dag_id="dags_bash_operator", #"example_complex",
    schedule="0 0 * * *", #None,
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Seoul"), #UTC
    catchup=False,
) as dag:
    bash_t1 = BashOperator(
        task_id="bash_t1",
        bash_command="echo whoami",
    )
    bash_t2 = BashOperator(
        task_id="bash_t2",
        #bash_command="echo $HOSTNAME",
        bash_command="hostname",
    )

    bash_t1 >> bash_t2