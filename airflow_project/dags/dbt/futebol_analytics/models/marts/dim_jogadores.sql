/*
Autor: Andrey Henrique
Objetivo/Finalidade: Materializar a dimensão de jogadores aplicando SCD Type 1.
Data_Utilizacao: 2026-03-12
Ganhos Reais: Rastreabilidade unificada, garantindo que downstream (BI) consuma chaves hasheadas e otimizadas para JOINs
*/

{{ config(
    materialized='table',
    unique_key='player_sk'
) }}

-- Chamada da macro para estruturação da tabela
{{ build_scd1_dimension('jogadores', 'player_id', 'player_sk')}}