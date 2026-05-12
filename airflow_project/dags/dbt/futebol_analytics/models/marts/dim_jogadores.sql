/*
Autor: Andrey Henrique
Objetivo/Finalidade: Materializar a dimensão de jogadores aplicando SCD Type 1.
Data_Utilizacao: 2026-03-12
Ganhos Reais: Rastreabilidade unificada, garantindo que downstream (BI) consuma chaves hasheadas e otimizadas para JOINs
*/

{{ config(
    materialized='table'
) }}

WITH silver_limpa AS (
    -- 1. Remoção preventiva da SK legada da Silver para evitar bug de Catalyst
    SELECT * EXCEPT(player_sk)
    FROM {{ source('silver', 'jogadores') }}
),

source_jogadores AS (
    -- 2. Ingestão da Tabela Oficial (Garantindo Unicidade SCD1)
    SELECT
        cast(player_id as string) AS business_key,
        cast(player_age as int) AS player_age,
        cast(player_firstname as string) AS player_firstname,
        cast(player_lastname as string) AS player_lastname,
        cast(player_name as string) AS player_name,
        cast(is_injured as boolean) AS is_injured,
        cast(player_birth_date as date) AS player_birth_date,
        cast(player_country as string) AS player_country,
        cast(player_place as string) AS player_place,
        cast(player_height_cm as int) AS player_height_cm,
        cast(player_weight_kg as int) AS player_weight_kg,
        cast(player_nationality as string) AS player_nationality
    FROM silver_limpa
    WHERE player_id IS NOT NULL
    -- GOVERNANÇA: Tie-Breaker em caso de duplicidade na origem oficial
    QUALIFY ROW_NUMBER() OVER(
        PARTITION BY player_id 
        ORDER BY ingestion_date DESC, player_name DESC
    ) = 1
),

jogadores_inferidos AS (
    -- 3. Motor de Cura com Agregação Forçada (Evita Fantasmas Mutantes)
    SELECT
        cast(e.player_id as string) AS business_key,
        cast(NULL as int) AS player_age,
        cast('Desconhecido' as string) AS player_firstname,
        cast('Desconhecido' as string) AS player_lastname,
        cast(MAX(e.player_name) as string) AS player_name, -- Agregação: Garante apenas 1 nome por ID
        cast(FALSE as boolean) AS is_injured,
        cast(NULL as date) AS player_birth_date,
        cast('Desconhecido' as string) AS player_country,
        cast('Desconhecido' as string) AS player_place,
        cast(NULL as int) AS player_height_cm,
        cast(NULL as int) AS player_weight_kg,
        cast('Desconhecido' as string) AS player_nationality
    FROM {{ source('silver', 'eventos') }} e
    WHERE e.player_id IS NOT NULL
      -- Blindagem: Só aceita se o ID for numérico
      AND cast(e.player_id as string) RLIKE '^[0-9]+$'
      -- Condicional de Injeção
      AND cast(e.player_id as string) NOT IN (SELECT business_key FROM source_jogadores)
    -- O Agrupamento garante a unicidade absoluta da chave
    GROUP BY cast(e.player_id as string)
),

dimensao_unificada AS (
    -- 4. Consolidação Segura
    SELECT * FROM source_jogadores
    UNION ALL
    SELECT * FROM jogadores_inferidos
)

-- 5. Geração Determinística Definitiva
SELECT
    md5(business_key) AS player_sk,
    business_key AS player_src_id,
    * EXCEPT(business_key),
    current_timestamp() AS gold_ingestion_date
FROM dimensao_unificada