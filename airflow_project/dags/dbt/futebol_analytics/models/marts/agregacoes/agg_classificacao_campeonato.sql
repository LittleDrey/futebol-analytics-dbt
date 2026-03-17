/*
Autor: Andrey Henrique
Objetivo/Finalidade: Tabela de consumo agregada (OBT) para cálculo da classificação do campeonato.
Data_Utilizacao: 2026-03-13
Ganhos Reais: Pré-processamento integral no Databricks. A tabela já entrega as chaves e as descrições, permitindo
importação direta (DirectQuery ou Import Mode) no PBI sem necessidade de modelagem adicional no DAX.
*/

{{ config(
    materialized='table'
) }}

WITH match_results AS (
    SELECT
        home_team_sk AS team_fk,
        home_team_goals AS goals_scored,
        away_team_goals AS goals_conceded,
        CASE
            WHEN home_team_goals > away_team_goals THEN 3
            WHEN home_team_goals = away_team_goals THEN 1
            ELSE 0
        END AS points
    FROM {{ ref('fct_partidas')}}

    UNION ALL

    SELECT 
        away_team_sk AS team_fk,
        away_team_goals AS goals_scored,
        home_team_goals AS goals_conceded,
        CASE
            WHEN away_team_goals > home_team_goals THEN 3
            WHEN away_team_goals = home_team_goals THEN 1
            ELSE 0
        END AS points
    FROM {{ ref('fct_partidas') }}
),

team_aggregation AS (
    SELECT
        team_fk,
        COUNT(1) AS matches_played,
        SUM(CASE WHEN points = 3 THEN 1 ELSE 0 END) AS wins,
        SUM(CASE WHEN points = 1 THEN 1 ELSE 0 END) AS draws,
        SUM(CASE WHEN points = 0 THEN 1 ELSE 0 END) AS losses,
        SUM(goals_scored) AS total_goals_scored,
        SUM(goals_conceded) AS total_goals_conceded,
        (SUM(goals_scored) - SUM(goals_conceded)) AS goal_difference,
        SUM(points) AS total_points
    FROM match_results
    GROUP BY team_fk
),

-- Enriquecimento Dimensional (Join no Data Warehouse)
enriched_aggregation AS (
    SELECT
        agg.*,
        dt.team_name
    FROM team_aggregation agg
    LEFT JOIN {{ ref('dim_times') }} dt ON agg.team_fk = dt.team_sk
)

SELECT * FROM enriched_aggregation