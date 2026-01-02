import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, task

with DAG(
    dag_id="s06_l31_dags_bash_python_with_xcom",
    schedule="30 9 * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:

    @task(task_id='python_push')
    def python_push_xcom():
        result_dict = {'status':'Good','data':[1,2,3],'options_cnt':100}
        return result_dict

    bash_pull = BashOperator(
        task_id='bash_pull',
        env={
            'STATUS':'{{ti.xcom_pull(task_ids="python_push")["status"]}}',
            'DATA':'{{ti.xcom_pull(task_ids="python_push")["data"]}}',
            'OPTIONS_CNT':'{{ti.xcom_pull(task_ids="python_push")["options_cnt"]}}'

        },
        bash_command='echo $STATUS && '
                    'echo $DATA && '
                    '{{ti.xcom_pull(task_ids="python_push")["data"]}}'
                    'echo $OPTIONS_CNT '
    )
    python_push_xcom() >> bash_pull

    bash_push = BashOperator(
        task_id='bash_push',
        env={
            'ENV_TEST_VALUE':'{{ti.xcom_push(key="env_test_pushed",value=333)}}',
        },
        bash_command='echo PUSH_START '
                    '{{ti.xcom_push(key="bash_pushed",value=200)}} && '
                    'echo PUSH_COMPLETE'
    )

    @task(task_id='python_pull')
    def python_pull_xcom(**kwargs):
        ti = kwargs['ti']

        # 2025/07/06 추가 사항
        # 3.0.0 버전부터 task_ids 값을 주지 않으면 Xcom 을 찾지 못합니다.
        # 버그인지, 의도한 것인지는 확실치 않으나 해결될 때까지 task_ids 값을 넣어서 수행합니다.

        # status_value = ti.xcom_pull(key='bash_pushed')
        status_value = ti.xcom_pull(key='bash_pushed', task_ids='bash_push')
        return_value = ti.xcom_pull(task_ids='bash_push')

        print('status_value:' + str(status_value))
        print('return_value:' + return_value)

        env_test_value = ti.xcom_pull(key='env_test_pushed', task_ids='bash_push')
        print('env_test_value:' + str(env_test_value))

    bash_push >> python_pull_xcom()

