# 📚 Dicionário de Dados (Camada Gold / Marts)

Este dicionário documenta a estrutura, a tipagem e a semântica das tabelas disponíveis na **Camada Gold** do Data Lake. Estas tabelas foram modeladas via **dbt** utilizando arquitetura dimensional (Fato/Dimensão), com Chaves Substitutas (Surrogate Keys) determinísticas em Hash MD5 e tipagem rigorosa para consumo direto em ferramentas de BI.

---

### 1. Tabela: `dim_estadios` (Venues)
Dimensão contendo os metadados geográficos e estruturais dos locais onde as partidas ocorrem. 

| Coluna | Tipo | Descrição |
| :--- | :--- | :--- |
| `stadium_sk` | String | Surrogate Key gerada internamente (Hash MD5) para garantir unicidade do estádio. |
| `stadium_id` | Integer | Identificador único da chave de negócio do estádio na fonte (API). |
| `stadium_name` | String | Nome oficial do estádio. |
| `stadium_city` | String | Cidade onde o estádio está localizado. |
| `stadium_state` | String | Estado da localização do estádio. |
| `stadium_road` | String | Endereço (rua/avenida) do estádio. |
| `stadium_district` | String | Bairro onde o estádio se encontra. |
| `stadium_country` | String | País de origem. |
| `stadium_capacity` | Integer | Capacidade máxima de público suportada. |
| `stadium_surface` | String | Tipo de gramado (grass, artificial, etc.). |
| `source_file` | String | Caminho do arquivo de origem na raw/bronze (Rastreabilidade). |
| `silver_ingestion_date` | Timestamp | Data e hora exata da ingestão na camada Silver. |
| `gold_ingestion_date` | Timestamp | Data e hora exata da materialização na camada Gold. |

---

### 2. Tabela: `fct_eventos` (Events)
Tabela transacional (Fato Secundária) de altíssima granularidade. Registra cada ocorrência intrajogo (gols, cartões, substituições) minuto a minuto.

| Coluna | Tipo | Descrição |
| :--- | :--- | :--- |
| `event_sk` | String | Surrogate Key única do evento (Hash MD5). |
| `match_sk` | String | Foreign Key (MD5) conectando à Fato de Partidas. |
| `team_sk` | String | Foreign Key (MD5) conectando à Dimensão de Times. |
| `player_sk` | String | Foreign Key (MD5) apontando para o autor do evento. |
| `assist_player_sk`| String | Foreign Key (MD5) apontando para o assistente (Nulo se não houver). |
| `match_src_id` | Integer | Identificador original da partida na fonte. |
| `match_minute_abs`| Integer | Minuto absoluto do evento (soma do minuto + extra time) para ordenação. |
| `extra_time` | Integer | Minutos de acréscimo no momento do evento (0 se não houver). |
| `event_type` | String | Categoria do evento (Goal, Card, Subst). |
| `event_detail` | String | Detalhamento específico (Normal Goal, Yellow Card, etc.). |
| `comments` | String | Comentários adicionais da arbitragem ou sistema. |
| `silver_ingestion_date` | Timestamp | Data de ingestão na camada Silver. |
| `gold_ingestion_date` | Timestamp | Data de materialização na camada Gold. |

---

### 3. Tabela: `dim_jogadores` (Players)
Dimensão contendo o perfil biológico e demográfico dos atletas. Modelada sob o padrão **SCD Tipo 1**, garantindo a extração do registro mais atualizado (Golden Record) de cada atleta.

| Coluna | Tipo | Descrição |
| :--- | :--- | :--- |
| `player_sk` | String | Surrogate Key do jogador (Hash MD5). |
| `player_id` | Integer | Identificador da chave de negócio do jogador na fonte (API). |
| `player_age` | Integer | Idade do jogador. |
| `player_firstname` | String | Primeiro nome do atleta. |
| `player_lastname` | String | Sobrenome do atleta. |
| `player_name` | String | Nome completo/conhecido do jogador. |
| `is_injured` | Boolean | Flag indicando se o jogador encontra-se lesionado (True/False). |
| `player_nationality` | String | Nacionalidade oficial do jogador. |
| `player_birth_date` | Date | Data de nascimento. |
| `player_country` | String | País de nascimento. |
| `player_place` | String | Cidade/Estado de nascimento. |
| `player_height_cm` | Integer | Altura do jogador em centímetros (higienizado na Silver). |
| `player_weight_kg` | Integer | Peso do jogador em quilogramas (higienizado na Silver). |
| `source_file` | String | Caminho do arquivo de origem. |
| `ingestion_date` | Timestamp | Data de ingestão dos dados na Silver. |
| `gold_ingestion_date` | Timestamp | Data de materialização na camada Gold. |

---

### 4. Tabela: `fct_partidas` (Matches)
Tabela Fato principal consolidando o resultado transacional, as métricas brutas e o status de encerramento de cada jogo.

| Coluna | Tipo | Descrição |
| :--- | :--- | :--- |
| `match_sk` | String | Surrogate Key primária da partida (Hash MD5). |
| `match_src_id` | Integer | Identificador único da partida na fonte original. |
| `tournament_sk` | String | Foreign Key (MD5) conectando à dimensão do Torneio/Temporada. |
| `venue_sk` | String | Foreign Key (MD5) conectando à dimensão do Estádio. |
| `home_team_sk` | String | Foreign Key (MD5) conectando à dimensão do Time Mandante. |
| `away_team_sk` | String | Foreign Key (MD5) conectando à dimensão do Time Visitante. |
| `match_date` | Timestamp | Data e hora oficial de início da partida. |
| `match_season_year` | Integer | Ano base da temporada correspondente ao jogo. |
| `match_round` | String | Rodada específica do campeonato. |
| `match_status` | String | Status final (Match Finished, Cancelled, etc.). |
| `match_referee` | String | Nome oficial do árbitro principal. |
| `home_team_goals` | Integer | Métrica: Gols totais do time mandante (Placar Final). |
| `away_team_goals` | Integer | Métrica: Gols totais do time visitante (Placar Final). |
| `home_team_halftime_goals`| Integer | Métrica: Gols do time mandante apenas no 1º tempo. |
| `away_team_halftime_goals`| Integer | Métrica: Gols do time visitante apenas no 1º tempo. |
| `home_team_fulltime_goals`| Integer | Métrica: Gols do time mandante no tempo regulamentar (90 min). |
| `away_team_fulltime_goals`| Integer | Métrica: Gols do time visitante no tempo regulamentar (90 min). |
| `is_finished` | Boolean | Flag de negócio indicando encerramento oficial da partida. |
| `silver_ingestion_date` | Timestamp | Data de ingestão na camada Silver. |
| `gold_ingestion_date` | Timestamp | Data de materialização na camada Gold. |

---

### 5. Tabela: `dim_times` (Teams)
Dimensão contendo os metadados e o histórico cadastral dos clubes de futebol.

| Coluna | Tipo | Descrição |
| :--- | :--- | :--- |
| `team_sk` | String | Surrogate Key da equipe (Hash MD5). |
| `team_id` | Integer | Identificador da chave de negócio do time na fonte original. |
| `team_name` | String | Nome oficial e padronizado do clube (ex: "Flamengo"). |
| `team_code` | String | Sigla oficial do time (ex: "FLA"). |
| `country_name` | String | País sede do clube. |
| `founded_year` | Integer | Ano oficial de fundação. |
| `is_national` | Boolean | Flag de negócio (True para 'Brazil', False para demais). |
| `source_file` | String | Caminho do arquivo de origem. |
| `ingestion_date` | Timestamp | Data de ingestão na camada Silver. |
| `gold_ingestion_date` | Timestamp | Data de materialização na camada Gold. |

---

### 6. Tabela: `dim_torneios` (Leagues)
Dimensão catalogando as edições dos campeonatos. Opera sob o padrão **SCD Tipo 2**, onde a unicidade da entidade é definida pela **Chave Composta** (ID do Torneio + Ano da Temporada), preservando o histórico de todas as edições.

| Coluna | Tipo | Descrição |
| :--- | :--- | :--- |
| `tournament_sk` | String | Surrogate Key gerada sobre a chave composta (Hash MD5 determinístico). |
| `tournament_scr_id` | Integer | Identificador do torneio na fonte (Não é único por si só). |
| `tournament_name` | String | Nome oficial da liga/copa (ex: "Serie A"). |
| `country_name` | String | País sede de realização do torneio. |
| `season_year` | Integer | Ano base de realização da edição (ex: 2023). |
| `season_start` | Date | Data oficial de abertura do campeonato. |
| `season_end` | Date | Data oficial de encerramento do campeonato. |
| `is_current_season`| Boolean | Flag de controle indicando se a temporada é a vigente no momento. |
| `source_file` | String | Caminho do arquivo de origem. |
| `gold_ingestion_date` | Timestamp | Data de materialização na camada Gold. |