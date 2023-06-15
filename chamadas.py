import pandas as pd
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

def run_code():
    dados = pd.read_csv('./csv/ocorrencias2022.csv', sep=';')

    # Remover colunas desnecessárias
    dados.drop(
        [
            "hora_minuto",
            'endereco',
            'origem_chamado',
            'motivo_finalizacao',
            'motivo_desfecho'
        ],
        axis=1,
        inplace=True,
    )

    # Tirar os valores nulos da idade
    dados = dados[dados['idade'].notna()]

    # Filtrar as linhas
    filtro = (dados['idade'] == 0) | (dados['tipo'] != 'RESPIRATORIA') | (dados['subtipo'] != 'CASO SUSPEITO COVID-19')
    dados = dados[~filtro]
    dados.reset_index(drop=True, inplace=True)

    # Deixar a primeira letra de cada coluna em maiúscula
    dados = dados.rename(columns=lambda x: x.title())

    # Salvar os dados em um novo csv
    dados.to_csv('./filtered/chamadas.csv', index=False)

    print(dados)

default_args = {
    'owner': 'seu_nome',
    'start_date': datetime(2023, 6, 14),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    '01_Chamadas',
    description='Executar código no Airflow',
    schedule_interval=None,
    default_args=default_args,
    catchup=False
)

executar_tarefa = PythonOperator(
    task_id='executar_tarefa',
    python_callable=run_code,
    dag=dag
)
