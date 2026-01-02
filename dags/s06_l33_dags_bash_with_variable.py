import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, Variable



with DAG(
    dag_id="s06_l33_dags_bash_with_variable",
    schedule="10 9 * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:
    # scheduler 주기적 파싱시 부하. 메타 db 테이블 접속 하는 1안 비추
    var_value = Variable.get("sample_key")
    bash_var_1 = BashOperator(
        task_id="bash_var_1",
        bash_command=f"echo variable:{var_value}"
    )

    bash_var_2 = BashOperator(
        task_id="bash_var_2",
        bash_command="echo variable:{{var.value.sample_key}}"
    )