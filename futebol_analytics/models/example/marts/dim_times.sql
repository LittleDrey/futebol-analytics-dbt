-- Dimensão em SCD Type 1 (Hash Determinístico)
-- Conceito Aplicado: Geração de Surrogate Key via `md5` para garantir unicidade absoluta e idempotência

{{  config(
       unique_key='team_sk'
) }}

WITH source_data AS (
    SELECT * FROM {{ source('silver', 'times')}}
),

hash_sk AS (
    SELECT
        md5(cast(team_id as string)) AS team_sk,
        team_id AS team_src_id,
        team_name,
        team_code,
        country_name,
        founded_year,
        is_national,
        source_file,
        current_timestamp() AS gold_ingestion_date
    FROM source_data
)

SELECT * FROM hash_sk