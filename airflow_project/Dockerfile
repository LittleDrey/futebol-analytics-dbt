# 1. Usamos a imagem '-base' para desativar os gatilhos ONBUILD e assumir o controle
FROM quay.io/astronomer/astro-runtime:13.5.1-base

# 2. Varoáveis Globais de Bypass para o PIP
ENV PIP_TRUSTED_HOST="pypi.org files.pythonhosted.org pip.astronomer.io" 
ENV ASTRO_USE_UV="false"

# 3. Copiamos o arquivo vazio (exigido pelos scripts internos para não quebrar)
COPY requirements.txt .

# 4. Executa apenas a atualização do Sistema Operacional (Linux/Ubuntu)
RUN /usr/local/bin/install-system-packages

# ENGENHARIA REVERSA: O Bypass da Caixa Preta
# Removemos o script proprietario que forçava o 'uv' e instalamos
# O pacote interno da Astronomer manualmente usando o PIP liberado
RUN pip install --no-cache-dir astro-run-dag

# 5. Instalamos as integrações Cosmos e Databricks de forma imperativa
RUN pip install --no-cache-dir \
    astronomer-cosmos==1.7.1 \
    apache-airflow-providers-databricks==6.2.0

# 6. Isolamento do dbt no ambiente virtual
RUN python -m venv dbt_venv && \
    dbt_venv/bin/pip install --no-cache-dir \
    dbt-databricks==1.11.5