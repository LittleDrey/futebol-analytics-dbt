/*
Autor: Andrey Henrique
Objetivo/Finalidade: Materializar a dimensão de estádios aplicando SCD Tipo 1 via abstração Jinja.
Data_Utilizacao: 2026-03-12
Ganhos reais: Código limpo, padronização da Surrogate Key (tournament_sk) e injeção automática de metadados de ingestão.
*/

{{ config(
    materialized='table',
    unique_key='tournament_sk'
) }}

WITH source_data AS (
    SELECT * FROM {{ source('silver', 'torneios') }}
),

hashed_sk AS (
    SELECT
        -- 1. Chave Determínistica Composta (ID + Temporada/Ano) para garantir Unicidade
        md5(concat_ws('||', cast(tournament_id as string), cast(season_year as string))) AS tournament_sk,

        -- 2. Chave de Negócio
        tournament_id AS tournament_scr_id,

        -- 3. Atributos da Silver (Exceto IDs e chaves pré=existentes, se houver)
        * EXCEPT (tournament_id, tournament_sk),

        current_timestamp() AS gold_ingestion_date
    FROM source_data
)

SELECT * FROM hashed_sk