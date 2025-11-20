from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

dag = DAG(
    'test_dag',
    default_args={
        'email': ['huanDAG@gmail.com'],
        'email_on_failure': True,
        'reties': 1,
        'retry_delay': timedelta(minutes=1)
    },
    description='A simple test DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    tags=['example'],
)

t1 = BashOperator(
    task_id='print_date',
    bash_command='date' ,
    dag=dag,
)

t2 = BashOperator(
    task_id='sleep',
    bash_command='sleep 5',
    retries=3,
    dag=dag,
)

t3 = BashOperator(
    task_id='print_hello',
    bash_command='echo "Hello World"',
    dag=dag,
)

t1 >> t2 >> t3