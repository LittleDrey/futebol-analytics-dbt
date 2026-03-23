/*
Autor: Andrey Henrique
Dimensão em SCD Type 1 (Hash Determinístico)
Conceito Aplicado: Geração de Surrogate Key via `md5` para garantir unicidade absoluta e idempotência
Data_Utilizacao: 2026-03-10
*/

{{ config(
    materialized='table',
    unique_key='team_sk'
) }}

-- Chamada da Macro para estruturação da tabela
{{ build_scd1_dimension('times', 'team_id', 'team_sk') }}