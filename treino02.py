from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import pandas as pd

default_args = {
    'owner': 'Michelle ',
    'depends_on_past' : False,
    'start_date' : datetime(2020,11,15,15),
    'email': ['airflow@airflow.com'],
    'email_on_failure' : False,
    'email_on_retry': False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1)
}

dag = DAG(
    "treino-02",
    description="Dados Titanic",
    default_args=default_args,
    schedule_interval=timedelta(minutes=2)

)

get_data = BashOperator(
    task_id='get-data',
    bash_command='curl -o /Users/michellemesquita/airflow/dags/titanic.csv https://raw.githubusercontent.com/datasciencedojo/datasets/master/titanic.csv',
    dag=dag
)

def calculate_mean_age():
    df = pd.read_csv('/Users/michellemesquita/airflow/dags/titanic.csv' )
    med = df.Age.mean()
    return med

def print_age(**context):
    value = context['task_instance'].xcom_pull(task_ids='calcula-idade-media')
    print(f"idade media no titatic era {value} anos.")

task_idade_media = PythonOperator(
    task_id='calcula-idade-media',
    python_callable=calculate_mean_age,
    provide_context=True,
    dag=dag
)

task_print_idade = PythonOperator(
    task_id='mostra_idade',
    python_callable=print_age,
    dag=dag
)

get_data >> task_idade_media >> task_print_idade