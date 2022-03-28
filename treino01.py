from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta


default_args = {
    'owner': 'Michelle',
    'depends_on_past' : False,
    'start_date' : datetime(2020,11,15,15),
    'email': ['airflow@airflow.com'],
    'email_on_failure' : False,
    'email_on_retry': False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1)
}

dag = DAG(
    "treino-01",
    description="Básico de Bash Operator e Python",
    default_args=default_args,
    schedule_interval=timedelta(minutes=2)

)

#tarefas

hello_bash = BashOperator(
    task_id="Hello_Bash",
    bash_command='echo "Hello world :)"',
    dag=dag
)

def say_hello():
    print("Hello from Py :D")

hello_python = PythonOperator(
    task_id="Hello_Python",
    python_callable=say_hello,
    dag=dag
)

hello_bash >> hello_python

