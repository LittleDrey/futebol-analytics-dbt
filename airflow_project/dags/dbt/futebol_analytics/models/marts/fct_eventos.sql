/*
Autor: Andrey Henrique
Objetivo/Finalidade: Consolidar a tabela fato de eventos das partidas, estabelecendo chaves relacionais para as dimensões
Data_Utilizacao: 2026-04-01
Ganhos Reais: Criação de chave composta determinística (event_sk) e desnormalização do contexto temporal (match_date) 
diretamente na Fato, garantindo um Star Schema perfeito e eliminando filtros bidirecionais no BI.
*/

{{ config (
    materialized='incremental',
    unique_key='event_sk',
    on_schema_change='fail'
) }}

WITH max_date AS (
    {% if is_incremental() %}
        -- Isola o cálculo de agregação para burlar a restrição do compilador Spark no WHERE
        SELECT coalesce(max(silver_ingestion_date), cast('1900-01-01' as timestamp)) AS max_ingestion_date FROM {{ this }}
    {% else %}
        -- Para execução Full Refresh (recarga total)
        SELECT cast('1900-01-01' as timestamp) AS max_ingestion_date
    {% endif %}
),

source_events AS (
    SELECT s.* FROM {{ source('silver', 'eventos') }} s
    CROSS JOIN max_date m
    {% if is_incremental() %}
        -- A cláusula WHERE agora compara com uma coluna direta, sem funções de agregação
        WHERE s.ingestion_date > m.max_ingestion_date
    {% endif %}
),

fato_partidas AS (
    -- NOVA CTE: Captura do contexto temporal da Fato Principal
    SELECT 
        cast(match_src_id as int) AS match_src_id,
        match_date,
        match_season_year
    FROM {{ ref('fct_partidas') }}
),

hashed_and_casted AS (
    SELECT
        -- 1. Chave Primária (Geração da SK para cumprir o contrato do dbt e modelagem dimensional)
        md5(cast(event_sk as string)) AS event_sk,
        
        -- Chave de Negócio (Rastreabilidade com a camada Silver)
        cast(event_sk as int) AS event_src_id,

        -- 2. Foreign Keys para Dimensões (Transformação das chaves de negócio em SKs do Tipo String)
        md5(cast(match_src_id as string)) AS match_sk,
        md5(cast(player_id as string)) AS player_sk,
        md5(cast(team_id as string)) AS team_sk,
        md5(cast(assist_player_id as string)) AS assist_player_sk,

        -- 3. Dimensão Degenerada e Métricas Temporais
        cast(match_src_id as int) AS match_src_id,
        cast(match_minute_abs as int) AS match_minute_abs,
        cast(minute as int) AS match_minute,
        cast(extra_time as int) AS extra_time,

        -- 4. Atributos do Evento
        cast(event_type as string) AS event_type,
        cast(event_detail as string) AS event_detail,
        cast(comments as string) AS comments,

        -- 5. Governança e Rastreabilidade Temporal
        cast(ingestion_date as timestamp) AS silver_ingestion_date,
        current_timestamp() AS gold_ingestion_date

        -- Nota de Arquitetura:
        -- Colunas: 'team_name', 'player_name', 'assist_player_name' e 'source_file' foram intencionalmente omitidas
        -- O modelo dimensional exige que descrições residam apenas nas dimensões para garantir performance analítica

    FROM source_events
),

eventos_enriquecidos AS (
    -- NOVA CTE: Cruzamento e tipagem final do contexto temporal
    SELECT 
        h.*,
        cast(p.match_date as date) AS match_date,
        cast(p.match_season_year as int) AS match_season_year
    FROM hashed_and_casted h
    LEFT JOIN fato_partidas p
        ON h.match_src_id = p.match_src_id
)

SELECT * FROM eventos_enriquecidos