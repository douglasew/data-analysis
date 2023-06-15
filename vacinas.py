import pandas as pd
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

def run_code():
    dados = pd.read_csv('./csv/covid_AL.csv', sep=';')

    # Filtrar por UF
    filtro = (dados['paciente_endereco_uf'] != 'PE')
    dados = dados[~filtro]
    dados.reset_index(drop=True, inplace=True)

    # Excluir colunas
    dados.drop(
        [
            "document_id",
            "paciente_id",
            "vacina_codigo",
            "sistema_origem",
            "vacina_descricao_dose",
            "paciente_racaCor_codigo",
            "paciente_racaCor_valor",
            "estabelecimento_valor",
            "estabelecimento_razaoSocial",
            "estalecimento_noFantasia",
            "vacina_grupoAtendimento_codigo",
            "vacina_grupoAtendimento_nome",
            "vacina_categoria_codigo",
            "vacina_categoria_nome",
            "vacina_lote",
            "vacina_fabricante_nome",
            "vacina_fabricante_referencia",
            "vacina_descricao_dose",
            "vacina_codigo",
            "estabelecimento_uf",
            "estabelecimento_municipio_codigo",
            "paciente_nacionalidade_enumNacionalidade",
            "estabelecimento_municipio_nome",
            "paciente_endereco_coIbgeMunicipio",
            "paciente_endereco_coPais",
            "paciente_endereco_nmPais",
            "paciente_dataNascimento",
            "paciente_endereco_uf"
        ],
        axis=1,
        inplace=True,
    )

    # Renomear o nome das colunas
    dados.rename(columns={'paciente_enumSexoBiologico': 'Sexo'}, inplace=True)
    dados.rename(columns={'paciente_idade': 'Idade'}, inplace=True)
    dados.rename(columns={'paciente_endereco_nmMunicipio': 'Municipio'}, inplace=True)
    dados.rename(columns={'paciente_endereco_cep': 'Cep'}, inplace=True)
    dados.rename(columns={'vacina_dataAplicacao': 'Data'}, inplace=True)
    dados.rename(columns={'vacina_nome': 'Vacina'}, inplace=True)

    # Salvar arquivo csv
    dados.to_csv('./filtered/vacinas.csv', index=False)

    print(dados)

default_args = {
    'owner': 'seu_nome',
    'start_date': datetime(2023, 6, 14),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    '01_Vacinas',
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
