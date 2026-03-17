FROM astrocrpublic.azurecr.io/runtime:3.1-13
# Instala o dbt em um ambiente virtual para evitar conflitos de dependências
RUN python -m venv dbt_venv && \
    dbt_venv/bin/pip install --no-cache-dir dbt-databricks==1.11.5

# Copia o projeto dbt da máquina para dentro do container do Airflow
COPY ../futebol_analytics /usr/local/airflow/dbt/futebol_analytics