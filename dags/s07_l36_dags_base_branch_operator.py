import pendulum
from airflow.sdk import DAG, task
from airflow.providers.standard.operators.branch import BaseBranchOperator
from airflow.providers.standard.operators.python import PythonOperator

with DAG(
    dag_id='s07_l36_dags_base_branch_operator',
    start_date=pendulum.datetime(2026, 1, 1, tz='Asia/Seoul'),
    schedule=None,
    catchup=False
) as dag:
    class CustomBranchOperator(BaseBranchOperator):
        def choose_branch(self, context):
            import random
            from pprint import pprint
            print('===========context===========')
            pprint(context)
            print('===========context===========')
            
            item_lst = ['A', 'B', 'C']
            selected_item = random.choice(item_lst)
            if selected_item == ['A']:
                return 'task_a'
            elif selected_item in ['B','C']:
                return ['task_b','task_c']

    
    custom_branch_operator = CustomBranchOperator(
        task_id='python_branch_task',
        templates_dict={'xxxxxxxxxxxxx': 'xxxxxxxxxxxxx_value'}
    )

    
    def common_func(**kwargs):
        print(kwargs['selected'])

    task_a = PythonOperator(
        task_id='task_a',
        python_callable=common_func,
        op_kwargs={'selected':'A'}
    )

    task_b = PythonOperator(
        task_id='task_b',
        python_callable=common_func,
        op_kwargs={'selected':'B'}
    )

    task_c = PythonOperator(
        task_id='task_c',
        python_callable=common_func,
        op_kwargs={'selected':'C'}
    )

    custom_branch_operator >> [task_a, task_b, task_c]



    @task(task_id='test_task')
    def test_task(**kwargs):
        from pprint import pprint
        print('===========test_task===========')
        pprint(kwargs)
        print('===========test_task===========')
        data_i_e=kwargs['data_interval_end']
        print(data_i_e)
        print(type(data_i_e))
        print(data_i_e.strftime('%Y-%m-%d'))
        print(type(data_i_e.strftime('%Y-%m-%d')))
        print(data_i_e.in_timezone('Asia/Seoul'))
        print(type(data_i_e.in_timezone('Asia/Seoul')))
        print(data_i_e.in_timezone('Asia/Seoul').strftime('%Y-%m-%d'))
        print(type(data_i_e.in_timezone('Asia/Seoul').strftime('%Y-%m-%d')))
        print('===========test_task===========')

    test_task()