/* Autor: Andrey Henrique
Objetivo/Finalidade: Garantir que o pipeline de integração contínua (Github Actions/Gitlab CI) valide o código de forma previsível
Impacto: Redução drástica da superfície de erros
Data_Utilizacao: 2026-03-18
Ganhos Reais: Refatoração para preservar as nomenclaturas originais das chaves de negócio (Business Keys) vindas da camada Silver.
*/

-- macros/build_scd1_dimension.sql
{% macro build_scd1_dimension(source_table, business_key, surrogate_key) %}

WITH source_data AS (
    SELECT * FROM {{ source('silver', source_table)}}
),

hash_sk AS (
    SELECT 
        -- 1. Gera a SK de forma determinística
        md5(cast({{ business_key }} as string)) AS {{ surrogate_key }},

        -- 2. Traz todas as colunas da Silver, EXCETO a SK que já veio da origem
        * EXCEPT ({{ surrogate_key }}),

        -- 3. Injeta metadados de auditoria da camada Gold
        current_timestamp() AS gold_ingestion_date
    FROM source_data
)

SELECT * FROM hash_sk

{% endmacro %}