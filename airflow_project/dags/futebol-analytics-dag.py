"""
Autor: Andrey Henrique
Objetivo/Finalidade: Orquestrar o Pipeline de dados 'futebol_analytics' utilizando Apache Airflow e Astronomer Cosmos.
Data_Utilizacao: 2026-03-17
Arquitetura: A DAG lê dinamicamente o manifest.json do dbt e converte cada modelo/teste em uma Task Individual, 
garantindo paralelismo e reprocessamento isolado.
"""

from datetime import datetime
from airflow.decorators import dag
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles import DatabricksTokenProfileMapping

# 1. Mapeamento Físico dentro do Contâiner Docker
DBT_PROJECT_PATH = "/usr/local/airflow/dags/dbt/futebol_analytics" # Caminho para os arquivos do projeto (Pasta Espelhada)
DBT_EXECUTABLE_PATH = "/usr/local/airflow/dbt_venv/bin/dbt" # Caminho para o programa do dbt (Ambiente Virtual isolado no dockerfile)

# 2. Configuração de Perfil (Conexão Segura com Databricks via Airflow Connections)
profile_config = ProfileConfig(
    profile_name="futebol_analytics",
    target_name="dev",
    profile_mapping=DatabricksTokenProfileMapping(
        conn_id="databricks_default",
        profile_args={"schema": "gold"},
    )
)

# 3. Definição da DAG (Grafo de Execução)
@dag(
    schedule_interval="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["dbt", "databricks", "futebol_analytics", "silver_to_gold"],
)
def run_futebol_analytics_pipeline():

    # O Cosmos DbtTaskGroup engolirá o projeto dbt inteiro e criará a topologia visual
    dbt_tg = DbtTaskGroup(
        group_id="dbt_models_and_tests",
        project_config=ProjectConfig(dbt_project_path=DBT_PROJECT_PATH),
        profile_config=profile_config,
        execution_config=ExecutionConfig(dbt_executable_path=DBT_EXECUTABLE_PATH),
    )

    dbt_tg

# Instancia a DAG
run_futebol_analytics_pipeline()