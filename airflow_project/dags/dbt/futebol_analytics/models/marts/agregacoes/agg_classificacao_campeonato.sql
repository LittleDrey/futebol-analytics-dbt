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
    -- 1. Captura de Resultados como Mandante
    SELECT
        tournament_sk,
        match_season_year,
        home_team_sk AS team_fk,
        home_team_goals AS goals_scored,
        away_team_goals AS goals_conceded,
        CASE
            WHEN home_team_goals > away_team_goals THEN 3
            WHEN home_team_goals = away_team_goals THEN 1
            ELSE 0
        END AS points
    FROM {{ ref('fct_partidas') }}

    UNION ALL

    -- 2. Captura de Resultados como Visitante
    SELECT 
        tournament_sk,
        match_season_year,
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
    -- 3. Agregação Particionada por Torneio e Temporada
    SELECT
        tournament_sk,
        match_season_year,
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
    GROUP BY 
        tournament_sk,
        match_season_year,
        team_fk
),

enriched_aggregation AS (
    -- 4. Enriquecimento Dimensional (Join com Times e Torneios)
    SELECT
        agg.tournament_sk,
        agg.match_season_year,
        agg.team_fk,
        dt.team_name,
        dtor.season_start,
        dtor.season_end,
        dtor.is_current_season,
        agg.matches_played,
        agg.wins,
        agg.draws,
        agg.losses,
        agg.total_goals_scored,
        agg.total_goals_conceded,
        agg.goal_difference,
        agg.total_points
    FROM team_aggregation agg
    LEFT JOIN {{ ref('dim_times') }} dt 
        ON agg.team_fk = dt.team_sk
    -- CORREÇÃO: Injeção da granularidade temporal no relacionamento
    LEFT JOIN {{ ref('dim_torneios') }} dtor 
        ON agg.tournament_sk = dtor.tournament_sk
        AND agg.match_season_year = dtor.season_year -- Ajuste para o nome exato da coluna de ano na dim_torneios
),

ranked_classification AS (
    -- 5. Inteligência Analítica: Cálculo da Posição na Tabela
    -- O critério de desempate clássico: Pontos > Vitórias > Saldo de Gols > Gols Pró
    SELECT 
        *,
        RANK() OVER(
            PARTITION BY tournament_sk, match_season_year 
            ORDER BY total_points DESC, wins DESC, goal_difference DESC, total_goals_scored DESC
        ) AS championship_position
    FROM enriched_aggregation
)

SELECT * FROM ranked_classification