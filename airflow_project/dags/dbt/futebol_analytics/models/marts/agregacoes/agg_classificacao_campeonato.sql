/*
Autor: Andrey Henrique
Objetivo/Finalidade: Tabela de consumo agregada (OBT) para cálculo da classificação do campeonato, incluindo histórico recente (Form).
Data_Utilizacao: 2026-04-01
Ganhos Reais: Pré-processamento integral no Databricks. Incorporação de Lógica de Form (Últimos 5 jogos) via Spark SQL para suportar Dataviz de alta performance sem sobrecarregar motor DAX.
*/

{{ config(
    materialized='table'
) }}

WITH match_results AS (
    -- 1. Captura de Resultados como Mandante
    SELECT
        tournament_sk,
        match_season_year,
        match_date, -- COLUNA NECESSÁRIA PARA ORDENAR OS ÚLTIMOS JOGOS
        home_team_sk AS team_fk,
        home_team_goals AS goals_scored,
        away_team_goals AS goals_conceded,
        CASE
            WHEN home_team_goals > away_team_goals THEN 3
            WHEN home_team_goals = away_team_goals THEN 1
            ELSE 0
        END AS points,
        CASE
            WHEN home_team_goals > away_team_goals THEN 'V'
            WHEN home_team_goals = away_team_goals THEN 'E'
            ELSE 'D'
        END AS result_char
    FROM {{ ref('fct_partidas') }}

    UNION ALL

    -- 2. Captura de Resultados como Visitante
    SELECT 
        tournament_sk,
        match_season_year,
        match_date, -- COLUNA NECESSÁRIA PARA ORDENAR OS ÚLTIMOS JOGOS
        away_team_sk AS team_fk,
        away_team_goals AS goals_scored,
        home_team_goals AS goals_conceded,
        CASE
            WHEN away_team_goals > home_team_goals THEN 3
            WHEN away_team_goals = home_team_goals THEN 1
            ELSE 0
        END AS points,
        CASE
            WHEN away_team_goals > home_team_goals THEN 'V'
            WHEN away_team_goals = home_team_goals THEN 'E'
            ELSE 'D'
        END AS result_char
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

form_ranking AS (
    -- 4. Motor do Histórico: Numera os jogos do mais recente para o mais antigo
    SELECT 
        tournament_sk,
        match_season_year,
        team_fk,
        result_char,
        ROW_NUMBER() OVER(
            PARTITION BY tournament_sk, match_season_year, team_fk 
            ORDER BY match_date DESC
        ) AS rn
    FROM match_results
),

recent_form_aggregated AS (
    -- 5. Isola os Top 5 mais recentes e concatena em ordem cronológica (antigo→recente)
    -- Usa struct + sort_array para garantir ordenação determinística no Spark SQL
    -- ORDER BY em subquery sem LIMIT é ignorado pelo otimizador — esta abordagem é segura
    SELECT
        tournament_sk,
        match_season_year,
        team_fk,
        array_join(
            transform(
                sort_array(collect_list(struct(rn, result_char)), false),
                x -> x.result_char
            ),
            ''
        ) AS ultimos_5
    FROM form_ranking
    WHERE rn <= 5
    GROUP BY
        tournament_sk,
        match_season_year,
        team_fk
),

enriched_aggregation AS (
    -- 6. Enriquecimento Dimensional (Join com Times, Torneios e Form Recente)
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
        agg.total_points,
        rf.ultimos_5
    FROM team_aggregation agg
    LEFT JOIN {{ ref('dim_times') }} dt 
        ON agg.team_fk = dt.team_sk
    LEFT JOIN {{ ref('dim_torneios') }} dtor 
        ON agg.tournament_sk = dtor.tournament_sk
        AND agg.match_season_year = dtor.season_year
    LEFT JOIN recent_form_aggregated rf
        ON agg.tournament_sk = rf.tournament_sk
        AND agg.match_season_year = rf.match_season_year
        AND agg.team_fk = rf.team_fk
),

ranked_classification AS (
    -- 7. Inteligência Analítica: Cálculo da Posição na Tabela (Com critério oficial de desempate)
    SELECT 
        *,
        RANK() OVER(
            PARTITION BY tournament_sk, match_season_year 
            ORDER BY total_points DESC, wins DESC, goal_difference DESC, total_goals_scored DESC
        ) AS championship_position
    FROM enriched_aggregation
)

SELECT * FROM ranked_classification