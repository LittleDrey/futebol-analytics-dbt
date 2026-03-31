/*
Autor: Andrey Henrique
Tabela Fato e Data Skipping
Conceito Aplicado: Otimização de Leitura (ZORDER) injetada diretamente via propriedades do
dbt (post-hook ou claúsulas específicas do adaptador Databricks). O cálculo das SKs é refeito
para manter o acoplamento fraco e evitar Shuffle de JOINs desnecessários
Data_Utilizacao: 2026-03-10
*/

{{ config(
    materialized='incremental',
    unique_key='match_sk'
) }}

WITH source_data AS (
    SELECT * FROM {{ source('silver', 'partidas') }}
),

fact_build AS (
    SELECT
        md5(cast(match_id as string)) AS match_sk,
        match_id AS match_src_id,
        md5(cast(home_team_id as string)) AS home_team_sk,
        md5(cast(away_team_id as string)) AS away_team_sk,
        md5(cast(venue_id as string)) AS venue_sk,
        -- Dentro da CTE fact_build na fct_partidas
        md5(concat_ws('||', 
            coalesce(cast(tournament_id as string), ''), 
            coalesce(cast(match_season_year as string), '')
        )) AS tournament_sk,
        match_date,
        match_season_year,
        match_round,
        match_status,
        match_referee,
        home_team_goals,
        away_team_goals,
        home_team_halftime_goals,
        away_team_halftime_goals,
        home_team_fulltime_goals,
        away_team_fulltime_goals,
        is_finished,
        ingestion_date AS silver_ingestion_date,
        current_timestamp() AS gold_ingestion_date
    FROM source_data
)

SELECT * FROM fact_build