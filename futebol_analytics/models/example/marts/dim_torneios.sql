-- Dimensão em Histórico (Comportamento SDC Type 2 / Chave Composta)
-- Conceito Aplicado: Respeito a granularidade temporal (Ano + Torneio)

{{ config(
    unique_key='tournament_sk'
) }}

WITH source_data AS (
    SELECT * FROM {{source('silver', 'torneios') }}
),

hash_sk AS (
    SELECT
        -- Hash baseado na Chave Composta para Manter o Rastreio Histórico
        md5(concat_ws('||', cast(tournament_id as string), cast(season_year as string))) AS tournament_sk,
        tournament_id AS tournament_src_id,
        tournament_name,
        country_name,
        season_year,
        season_start,
        season_end,
        is_current_season,
        source_file,
        current_timestamp() AS gold_ingestion_date
    FROM source_data
)

SELECT * FROM hash_sk