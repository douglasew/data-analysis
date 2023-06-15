import pandas as pd
from sqlalchemy import create_engine
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

def load_data():
    chamadas = pd.read_csv('./filtered/chamadas.csv')
    vacinas = pd.read_csv('./filtered/vacinas.csv')
    merged = pd.merge(chamadas, vacinas, on='Data')
    return merged

def save_data(merged):
    merged.to_csv('./result/merged.csv', index=False)

def insert_data(table):
    dados = pd.read_csv('./result/merged.csv', sep=',', nrows=10000)
    engine = create_engine("postgresql://postgres:root@localhost:5432/test_db")
    dados.to_sql(table, engine, if_exists="replace", index=False)

def data_bigger(merged):
    maior = merged['Data'].value_counts()
    return maior

def media_idade_vacinados(merged):
    media = merged['Idade_y'].mean()
    return media

def media_idade_chamadas(merged):
    media = merged['Idade_x'].mean()
    return media

def maior_idade_chamadas(merged):
    maior = max(merged['Idade_x'])
    return maior

def maior_idade_vacinados(merged):
    maior = max(merged['Idade_y'])
    return maior

default_args = {
    'owner': 'seu_nome',
    'start_date': datetime(2023, 6, 14),
}

dag = DAG('01_Merge', default_args=default_args, schedule_interval=None)

load_task = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    dag=dag
)

save_task = PythonOperator(
    task_id='save_data',
    python_callable=save_data,
    op_args=[load_task.output],
    dag=dag
)

insert_task = PythonOperator(
    task_id='insert_data',
    python_callable=insert_data,
    op_args=['nome_da_tabela'],
    dag=dag
)

data_bigger_task = PythonOperator(
    task_id='data_bigger',
    python_callable=data_bigger,
    op_args=[load_task.output],
    dag=dag
)

media_idade_vacinados_task = PythonOperator(
    task_id='media_idade_vacinados',
    python_callable=media_idade_vacinados,
    op_args=[load_task.output],
    dag=dag
)

media_idade_chamadas_task = PythonOperator(
    task_id='media_idade_chamadas',
    python_callable=media_idade_chamadas,
    op_args=[load_task.output],
    dag=dag
)
