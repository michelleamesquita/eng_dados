from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from pymongo import MongoClient
import pandas as pd
import json, urllib.request
from datetime import datetime, timedelta


default_args = {
    'owner': 'Michelle Mesquita',
    "depends_on_past": False,
    "start_date": datetime(2022, 4, 4),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

dag = DAG(
    "desafio-final",
    description="Desafio Final IGTI", 
    default_args=default_args, 
    schedule_interval=timedelta(days=1)
)


def get_from_mongodb():
    conn = MongoClient('mongodb+srv://url', serverSelectionTimeoutMS=360000, tlsAllowInvalidCertificates=True)

    db = conn['test']
    mycollection = db['tst']

    registros = mycollection.find()

    lista = []
    # for loop adicionando todos valores para uma lista
    for row in registros:
        lista.append(row)

    # transformando lista em um DataFrame
    df = pd.DataFrame(lista)

    # salva o df
    df.to_csv('/usr/local/airflow/data/dados_mongoDB.csv', sep=";", encoding='utf-8', index=False)

def get_from_ibge():

    # extrai os dados da api do ibge
    url1 = 'https://servicodados.ibge.gov.br/api/v1/localidades/estados'
    with urllib.request.urlopen(url1) as url:
        data = json.loads(url.read().decode())

    # faz um loop pela lista de jsons normalizando os dados
    ufs = []
    for uf in data:
        uf['regiao_nome'] = uf['regiao']['nome']
        ufs.append(uf)
    
    # converte a lista em um dataframe 
    df_ibge = pd.DataFrame(ufs)

    df_ibge.to_csv('/usr/local/airflow/data/dados_igbe.csv', sep=";", encoding='utf-8', index=False)

def transform_mongodb_data():
    dados_mongo = pd.read_csv('/usr/local/airflow/data/dados_mongoDB.csv', sep=';', encoding='utf-8')
    dados_mulher = dados_mongo[(dados_mongo['sexo'] == 'Mulher') & ((dados_mongo['idade'] >= 20) & (dados_mongo['idade'] <= 40))]
    dados_mulher.to_csv('/usr/local/airflow/data/dados_transformados.csv', sep=';', encoding='utf-8')

task_get_from_mongodb = PythonOperator(
    task_id='task_get_from_mongodb',
    python_callable=get_from_mongodb,
    dag=dag
)

task_get_from_ibge = PythonOperator(
    task_id='task_get_from_ibge',
    python_callable=get_from_ibge,
    dag=dag
)

task_transform_mongodb_data = PythonOperator(
    task_id='task_transform_mongodb_data',
    python_callable=transform_mongodb_data,
    dag=dag
)

[task_get_from_mongodb, task_get_from_ibge] >> task_transform_mongodb_data
