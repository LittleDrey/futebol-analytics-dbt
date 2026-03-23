# ⚽ Futebol Analytics Data Pipeline

> Um projeto de Engenharia de Dados End-to-End aplicando os conceitos fundamentais do livro *"Fundamentals of Data Engineering"* (Joe Reis & Matt Housley) aliado a práticas modernas de Analytics Engineering e Orquestração de Grafos.

## 🎯 Objetivo do Projeto
Construir um pipeline de dados robusto, escalável e moderno para processar dados de futebol (jogadores, partidas, torneios, eventos, estádios, times), traduzindo a teoria de engenharia de dados em prática real utilizando **Databricks**, **Delta Lake**, **dbt (Data Build Tool)** e **Apache Airflow**.

O projeto segue a arquitetura **Medallion** (Bronze, Silver, Gold), com foco em qualidade de dados, governança, otimização de armazenamento e entrega de valor analítico direto para o negócio (BI).

---

## 📚 Fundamentação Teórica & Decisões Arquiteturais

Este projeto é guiado pelas fases do Ciclo de Vida da Engenharia de Dados. Abaixo estão as decisões técnicas tomadas para cada etapa:

### 1. Ingestão (Ingestion)
*Baseado no Capítulo 7: Ingestão de Dados*

* **Padrão de Movimentação:** **Push** (Empurrar). Os arquivos são enviados da origem local para o Data Lake.
* **Frequência:** **Batch** (Lote). Como lidamos com dados históricos de partidas e torneios, a ingestão em lote é a escolha pragmática, evitando a complexidade desnecessária de streaming para dados que não exigem latência de milissegundos.
* **Filosofia:** "Encanamento" (Plumbing). Nesta etapa, o foco foi puramente mover os dados do ponto A (Local) para o ponto B (Staging Zone) sem transformações, garantindo uma cópia fiel da origem.

## 🛠️ Infraestrutura e Organização

### Estrutura do Data Lake (Staging Zone)
Para evitar o antipadrão do "Data Swamp" (Pântano de Dados), a zona de aterrissagem (Staging) foi estruturada hierarquicamente para garantir governança e facilitar a leitura automatizada:

    staging_zone/ (Volume Databricks)
    ├── futebol_db/          <-- Sistema de Origem (Source System)
    │   ├── torneios/        <-- Entidade de Negócio
    │   │   ├── csv/         <-- Formato do Arquivo
    │   │       └── _torneios.csv
    │   ├── jogadores/
    │   │   ├── json/
    │   │       └── _jogadores.json
    │   └── partidas/
    │       ├── csv/
    │           └── _partidas.csv

---

## 🚧 Fase 1: Desafios de Ingestão (War Stories)

Durante a fase de Ingestão, enfrentei limitações de infraestrutura e problemas de qualidade de dados na origem. Abaixo documentei como superar cada barreira:

* **Restrição de Infraestrutura (Streaming em Cluster Compartilhado):** A intenção era utilizar Databricks Auto Loader (`cloud_files`). Porém, o *Shared Cluster* bloqueou permissões. A arquitetura foi adaptada para **Batch Read**, aceitando o trade-off temporário do checkpoint automático em favor da execução funcional.
* **Serialização e Encoding (UTF-16):** A ingestão inicial de CSVs resultou em "Mojibake". Implementado tratamento específico no Reader do Spark para forçar o encoding correto detectado na origem (`UTF-16`).
* **Integridade do Schema (JSON Aninhado):** Arquivos JSON complexos não foram "explodidos" na ingestão. Seguindo o princípio da camada Bronze, foram persistidos como Strings ou Structs brutos para tratamento futuro, evitando que falhas de parser quebrassem o pipeline.

---

## 🏗️ Fase 2: Transformação Raw (Bronze) → Silver

Foco na **limpeza, padronização e estruturação** dos dados brutos via PySpark. Transformação de dados "caóticos" em tabelas confiáveis, tipadas e otimizadas.

### ⚙️ Architecture Decision Record (ADR): Particionamento Físico vs. Z-Order
* **Decisão:** **NÃO PARTICIONAR** fisicamente as tabelas deste projeto.
* **Justificativa:** O particionamento é recomendado para diretórios com 1GB a 2GB de dados. Particionar um dataset de baixa volumetria geraria o *Small Files Problem*, degradando a leitura devido ao *Metadata Overhead*.
* **Estratégia Adotada:** Utilização dos recursos nativos do Delta Lake: execução diária de `OPTIMIZE` e aplicação de `ZORDER BY (match_id, match_season_year)` nas tabelas Fato para permitir *Data Skipping* dinâmico.

### ⚔️ War Stories da Camada Silver
1. **O Desafio do "Encoding Híbrido":** Dados históricos em UTF-16LE e recentes em UTF-8 geraram corrupção de IDs. **Solução:** Desenvolvimento de uma função "Sniffer" que inspeciona os *Magic Bytes* do arquivo antes da leitura, aplicando o decoder correto dinamicamente.
2. **Schema Drift no Delta Lake:** Conflito de merge de metadados após correção de tipagem. **Solução:** Implementação de Schema Evolution controlada com `.option("overwriteSchema", "true")`.
3. **Data Wrangling Avançado:** Tratamento de "Fake JSONs" via Regex (aspas simples para duplas, `None` para `null`) e *Flattening* de Structs aninhados na tabela de jogadores.
4. **Falso Positivo no Databricks Monitoring:** O Unity Catalog apontava ausência de duplicatas em tabelas corrompidas devido ao delay do job de monitoramento. **Solução:** Separação estrita de schemas com um *Sidecar* (`data_governance`) para métricas de perfilamento.
5. **Deduplicação Determinística:** Substituição do `dropDuplicates()` nativo por Window Functions ordenadas por `ingestion_date DESC` (para extrair o *Golden Record* em SCD Tipo 1) e validação de chaves compostas (para SCD Tipo 2).

---

## 🚀 Fase 3: Analytics Engineering & Orquestração (Silver → Gold)

Na transição para a camada de consumo (Gold), o paradigma evolui da engenharia de dados pura (PySpark) para **Analytics Engineering (SQL-first)**.

* **Stack Adotada:** O **dbt (Data Build Tool)** foi introduzido para modularizar regras de negócio, aplicar testes de qualidade (Shift-Left Testing) e governar a linhagem.
* **Orquestração Dinâmica:** O **Apache Airflow**, operando com a biblioteca Astronomer Cosmos, traduz fisicamente a linhagem do projeto dbt em uma DAG interativa.
* **Modelagem:** Modelo híbrido contendo Dimensões conformadas, Fatos transacionais e uma **OBT (One Big Table)** analítica.

### 💡 Ganhos de Negócio (Shift-Left Analytics)
A OBT `agg_classificacao_campeonato` foi materializada inteiramente no Databricks. Regras de pontuação (Vitória=3, Empate=1), saldo de gols e ranqueamento (Window Functions) foram pré-processadas no Data Warehouse. Isso garante que o Power BI consuma os dados (DirectQuery/Import) sem a necessidade de DAX complexo, escalando a performance da visualização.

### ⚔️ War Stories da Camada Gold

#### 1. O Paradoxo da Surrogate Key Determinística (Join Miss)
* **O Problema:** JOINs retornando nulo na OBT.
* **Causa/Solução:** Divergência sintática na geração de hashes MD5 (`concat_ws` na Dimensão vs `concat` na Fato). Refatoração para garantir simetria determinística na geração da *Surrogate Key*.

#### 2. O Efeito Bumerangue no Airflow (Cosmos TaskGroup Cycle)
* **O Problema:** O Airflow falhava com `AirflowDagCycleException` ao renderizar o dbt, mesmo sem dependências circulares reais no SQL.
* **Causa/Solução:** O parser do Cosmos colapsava ao ler testes estruturais do tipo `relationships` (Foreign Keys). Solução arquitetural: Conversão desses testes para documentação passiva (`meta: references`) no `.yml`, destravando o orquestrador e mantendo o catálogo de dados intacto.

#### 3. Concorrência ACID no Delta Lake (ConcurrentAppendException)
* **O Problema:** Falhas de Merge incremental no Databricks disparadas pela DAG.
* **Causa/Solução:** O Airflow gerou um *Double Trigger* (Workers paralelos concorrendo pelo mesmo log transacional). Aplicação da trava `max_active_runs=1` na DAG e execução de `dbt run --full-refresh` para sanear o estado.

---

## 📖 Governança de Dados e Metadados (Data Catalog)

A excelência técnica do pipeline exige que o dado seja facilmente descoberto:

1. **Documentação Modular:** Metadados, tipagem e testes documentados rigorosamente nos arquivos `.yml` (separação de responsabilidades).
2. **Data Catalog Interativo:** Compilação dos metadados (`dbt docs generate`) e exposição da interface gráfica (`dbt docs serve`) para visualização do *Lineage Graph* ponta a ponta.
3. **Padrão de Versionamento:** Utilização do Git Flow (branches isoladas para `feat/`, `docs/`, `chore/`, `fix/`) e *Conventional Commits* garantindo histórico rastreável e aderência a práticas de integração contínua (CI/CD).