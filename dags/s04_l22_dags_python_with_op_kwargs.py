from common.s04_l18_common_func import regist2
import pendulum
from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator



with DAG(
    dag_id="s04_l22_dags_python_with_op_kwargs",
    schedule="30 6 * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:
    
    regist2_t1 = PythonOperator(
        task_id='regist2_t1',
        python_callable=regist2,
        op_args=['hjkim','man','kr','seoul'],
        op_kwargs={'email':'hjkim_sun@naver.com','phone':'010'}
    )

    regist2_t1