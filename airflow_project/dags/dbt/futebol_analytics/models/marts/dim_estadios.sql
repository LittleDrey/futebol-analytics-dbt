/*
Autor: Andrey Henrique
Objetivo/Finalidade: Materializar a dimensão de estádios aplicando SCD Type 1 via abstração Jinja.
Data_Utilizacao: 2026-03-12
Ganhos Reais: Código limpo, padronização de Surrogate Key (stadium_sk) e injeção automática de metadados da ingestão.
*/

{{ config(
    materialized='table',
    unique_key='stadium_sk'
) }}

-- Chamada da macro para estruturação da tabela
{{ build_scd1_dimension('estadios', 'stadium_id', 'stadium_sk')}}