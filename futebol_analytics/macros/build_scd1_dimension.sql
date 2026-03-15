/* Autor: Andrey Henrique
Objetivo/Finalide: Garantir que o pipeline de integração contínua 
(Github Actions/Gitlab CI) valide o código de forma previsível
Impacto: Redução drástica da superfície de erros
Data_Utilizacao: 2026-03-12
*/

SQL
-- macros/build_scd1_dimension.sql
{% macro build_scd1_dimension(source_table, business_key, surrogate_key) %}

WITH source_data AS (
    SELECT * FROM {{ source('silver', source_table)}}
),

hash_sk AS (
    SELECT 
        -- 1. Gera a SK de forma determinística
        md5(cast({{ business_key }} as string)) AS {{ surrogate_key }},

        -- 2. Renomeia a Chave Original para o padrão de Rastreabilidade
        {{ business_key }} AS {{ source_table }}_src_id,

        -- 3. Traz todas as colunas da Silver, exceto a chave original que já foi renomeada
        * EXCEPT ({{ business_key }}, {{ surrogate_key }}),

        -- 4. Injeta metadados de auditoria
        current_timestamp() AS gold_ingestion_date
    FROM source_data
)

SELECT * FROM hash_sk

{% endmacro %}