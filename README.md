# ETL Pipeline Code — Agentic Data Vault Pipeline

<p align="center">
  <img src="https://img.shields.io/badge/Python-3.9%2B-blue?style=flat-square&logo=python" />
  <img src="https://img.shields.io/badge/dbt-1.7%2B-orange?style=flat-square&logo=dbt" />
  <img src="https://img.shields.io/badge/Snowflake-Target-29B5E8?style=flat-square&logo=snowflake" />
  <img src="https://img.shields.io/badge/datavault4dbt-v1.17.0-green?style=flat-square" />
  <img src="https://img.shields.io/badge/FIBO-2025%2FQ4-purple?style=flat-square" />
  <img src="https://img.shields.io/badge/BIAN-v13-red?style=flat-square" />
  <img src="https://img.shields.io/badge/License-MIT-yellow?style=flat-square" />
</p>

**Author:** gokulram.krishnan  
**Repository:** https://github.com/gokulramkrishnan/ETL-Pipeline-Code

An agentic, end-to-end data engineering pipeline that reverse-engineers DataStage and Informatica ETL exports and produces a complete **Data Vault 2.0** solution on **Snowflake** using **dbt + datavault4dbt**, with built-in PII detection, data profiling, reconciliation, data quality, and **Mermaid ER diagrams** annotated with **FIBO** and **BIAN** reference model alignment.

---

## Table of Contents

- [Architecture Overview](#architecture-overview)
- [Repository Structure](#repository-structure)
- [Pipeline Agents](#pipeline-agents)
  - [Agent 1 — Reverse Engineer](#agent-1--reverse-engineer)
  - [Agent 2 — STTM Generator](#agent-2--sttm-generator)
  - [Agent 3 — Data Vault 2.0 Modeler](#agent-3--data-vault-20-modeler)
  - [Agent 4 — DBT Project Generator](#agent-4--dbt-project-generator)
  - [Agent 5 — Data Profiler](#agent-5--data-profiler)
  - [Agent 6 — Data Reconciliation](#agent-6--data-reconciliation)
  - [Agent 7 — Data Quality](#agent-7--data-quality)
  - [Agent 8 — Mermaid ER + FIBO/BIAN Alignment](#agent-8--mermaid-er--fibobian-alignment)
- [Standalone Notebooks](#standalone-notebooks)
- [Input File Formats](#input-file-formats)
- [Output Files](#output-files)
- [Setup & Installation](#setup--installation)
- [Configuration](#configuration)
- [Running the Pipeline](#running-the-pipeline)
- [dbt Deployment](#dbt-deployment)
- [Reference Models](#reference-models)
- [Dependencies](#dependencies)
- [License](#license)

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────┐
│                          INPUT FILES                                │
│         DataStage (.dsx / .xml)  |  Informatica (.xml / .ipc)       │
└───────────────────────┬─────────────────────────────────────────────┘
                        │
            ┌───────────▼────────────┐
            │  Agent 1               │  Reverse Engineer
            │  parse_datastage()     │  → Unified metadata schema
            │  parse_informatica()   │  → Sources, targets, columns,
            └───────────┬────────────┘    transformations, load strategy
                        │
            ┌───────────▼────────────┐
            │  Agent 2               │  STTM Generator
            │  build_sttm()          │  → STTM.xlsx (5 sheets)
            │                        │  → STTM.json
            └───────────┬────────────┘
                        │
            ┌───────────▼────────────┐
            │  Agent 3               │  Data Vault 2.0 Modeler
            │  build_hubs()          │  → Hubs, Links, Satellites
            │  build_links()         │  → PIT tables
            │  build_satellites()    │  → dv_model.json
            └───────────┬────────────┘
                        │
            ┌───────────▼────────────┐
            │  Agent 4               │  DBT Project Generator
            │  generate_hub_model()  │  → dbt_project.yml
            │  generate_link_model() │  → packages.yml (datavault4dbt)
            │  generate_sat_model()  │  → profiles.yml (Snowflake)
            │  generate_staging()    │  → Staging / Raw Vault SQL
            └───────────┬────────────┘
                        │
            ┌───────────▼────────────┐
            │  Agent 5               │  Data Profiler
            │  run_ydata_profiling() │  → HTML profile reports
            │  run_pii_detection()   │  → PII column + cell detail
            └───────────┬────────────┘
                        │
            ┌───────────▼────────────┐
            │  Agent 6               │  Data Reconciliation
            │  row_count_check()     │  → recon_report.xlsx
            │  checksum_check()      │  → recon_report.json
            │  missing_key_check()   │  → Reconciliation SQL scripts
            └───────────┬────────────┘
                        │
            ┌───────────▼────────────┐
            │  Agent 7               │  Data Quality
            │  infer_dq_rules()      │  → dbt schema test YAML
            │  evaluate_rules()      │  → DQ SQL checks
            │  generate_dq_report()  │  → dq_report.xlsx
            └───────────┬────────────┘
                        │
            ┌───────────▼────────────┐
            │  Agent 8               │  Mermaid ER + FIBO/BIAN
            │  build_mermaid_er()    │  → source_er.mmd
            │  build_fibo_bian_      │  → data_vault_er.mmd
            │    alignment()         │  → fibo_bian_class.mmd
            └───────────┬────────────┘
                        │
┌───────────────────────▼─────────────────────────────────────────────┐
│                         SNOWFLAKE                                    │
│   STAGING schema  →  RAW_VAULT schema  →  BUSINESS_VAULT schema     │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Repository Structure

```
ETL-Pipeline-Code/
│
├── etl_pipeline/
│   ├── __init__.py
│   ├── agents/
│   │   ├── __init__.py                  # Exports all agent functions
│   │   ├── agent_reverse_engineer.py    # Agent 1 — DataStage & Informatica parser
│   │   ├── agent_sttm.py                # Agent 2 — Source-to-Target Mapping
│   │   ├── agent_data_vault.py          # Agent 3 — Data Vault 2.0 modeler
│   │   ├── agent_dbt_generator.py       # Agent 4 — dbt project generator
│   │   ├── agent_profiler.py            # Agent 5 — ydata-profiling + PII
│   │   ├── agent_reconciliation.py      # Agent 6 — reconciliation checks
│   │   ├── agent_data_quality.py        # Agent 7 — DQ rule engine
│   │   └── agent_mermaid_er.py          # Agent 8 — Mermaid ER + FIBO/BIAN
│   │
│   ├── input/
│   │   ├── datastage_sample.dsx         # Sample DataStage export
│   │   └── informatica_sample.xml       # Sample Informatica export
│   │
│   └── output/                          # Generated at runtime (gitignored)
│       ├── sttm/
│       │   ├── sttm.xlsx
│       │   └── sttm.json
│       ├── data_vault/
│       │   └── dv_model.json
│       ├── dbt_project/
│       │   ├── dbt_project.yml
│       │   ├── packages.yml
│       │   ├── profiles.yml
│       │   └── models/
│       │       ├── staging/
│       │       ├── raw_vault/
│       │       │   ├── hubs/
│       │       │   ├── links/
│       │       │   └── satellites/
│       │       └── business_vault/
│       ├── profiling/
│       │   ├── profile_report.html
│       │   ├── profiling_summary.json
│       │   └── pii_column_summary.csv
│       ├── reconciliation/
│       │   ├── recon_report.xlsx
│       │   └── recon_report.json
│       ├── data_quality/
│       │   ├── dq_report.xlsx
│       │   └── dq_rules.json
│       ├── mermaid/
│       │   ├── source_er.mmd
│       │   ├── staging_er.mmd
│       │   ├── data_vault_er.mmd
│       │   ├── fibo_bian_class.mmd
│       │   ├── fibo_bian_alignment.json
│       │   └── README_diagrams.md
│       └── metadata/
│           └── unified_metadata.json
│
├── main_pipeline.ipynb                  # Main orchestration notebook
├── etl_reverse_eng_dv_pipeline.ipynb    # Self-contained end-to-end notebook
├── pii_metadata_pipeline.ipynb          # PII detection → SQL Server
├── pii_metadata_pipeline_anon.ipynb     # PII + anonymization + de-anonymization
├── ydata_profiling_to_sql.ipynb         # ydata-profiling → SQL Server
├── ydata_profiling_to_sql_with_pii.ipynb # ydata-profiling + PII → SQL Server
│
├── requirements.txt
├── .gitignore
└── README.md
```

---

## Pipeline Agents

### Agent 1 — Reverse Engineer

**File:** `etl_pipeline/agents/agent_reverse_engineer.py`

Parses DataStage and Informatica ETL export files into a unified metadata schema consumed by all downstream agents.

**Functions:**

| Function | Description |
|----------|-------------|
| `parse_datastage(file_path)` | Parses DataStage `.dsx` / `.xml` exports. Extracts job name, source/target stages, column mappings, derivation expressions, load strategies. |
| `parse_informatica(file_path)` | Parses Informatica PowerCenter `.xml` / `.ipc` exports. Extracts MAPPING elements, SOURCE/TARGET definitions, CONNECTOR lineage, transformations. |
| `run_agent1_reverse_engineer(ds_files, inf_files)` | Orchestrator — processes all input files and returns unified metadata dict. |

**Input:**

| Format | Extension | Tool |
|--------|-----------|------|
| DSX export | `.dsx` | IBM DataStage |
| XML export | `.xml` | IBM DataStage |
| PowerCenter XML | `.xml` | Informatica PowerCenter |
| Repository export | `.ipc` | Informatica PowerCenter |

**Output:**

| File | Location | Description |
|------|----------|-------------|
| `unified_metadata.json` | `output/metadata/` | Sources, targets, columns, transformations, load strategy per mapping |

**Unified metadata schema:**
```json
{
  "sources": [{"name": "SRC_TABLE", "columns": [...], "schema": "..."}],
  "targets": [{"name": "TGT_TABLE", "columns": [...], "schema": "..."}],
  "mappings": [{"source": "...", "target": "...", "column_map": [...]}],
  "transformations": [{"column": "...", "expression": "..."}],
  "load_strategy": "FULL | INCREMENTAL"
}
```

---

### Agent 2 — STTM Generator

**File:** `etl_pipeline/agents/agent_sttm.py`

Produces a Source-to-Target Mapping document in Excel and JSON from the unified metadata.

**Functions:**

| Function | Description |
|----------|-------------|
| `build_sttm(metadata, target_database, target_schema)` | Builds STTM rows from metadata, classifies columns, identifies PII candidates. |
| `STTMAgent.run(metadata, output_dir, target_database, target_schema)` | Orchestrator — writes Excel + JSON outputs. |

**Output:**

| File | Location | Sheets / Contents |
|------|----------|-------------------|
| `sttm.xlsx` | `output/sttm/` | All Mappings, Mapped Columns, Unmapped Columns, PII Columns, Summary |
| `sttm.json` | `output/sttm/` | Full STTM as JSON array |

**STTM Excel columns:**

| Column | Description |
|--------|-------------|
| Mapping Name | Source ETL job / mapping name |
| ETL Tool | DataStage or Informatica |
| Load Strategy | FULL / INCREMENTAL / UNKNOWN |
| Source Table | Fully qualified source table |
| Source Column | Source column name |
| Target Table | Fully qualified target table |
| Target Column | Target column name |
| Data Type | Snowflake-compatible data type |
| Primary Key | Yes / No |
| Nullable | Yes / No |
| Transformation | SQL expression or DIRECT |
| DV Entity Type | HUB / LINK / SAT (filled by Agent 3) |
| DV Table | Data Vault table name |
| DQ Rule | Applied DQ rules (filled by Agent 7) |
| Notes | Free text |

---

### Agent 3 — Data Vault 2.0 Modeler

**File:** `etl_pipeline/agents/agent_data_vault.py`

Auto-classifies columns into Data Vault 2.0 entities (Hubs, Links, Satellites) following Dan Linstedt's DV2.0 standard and datavault4dbt v1.17.0 naming conventions.

**Functions:**

| Function | Description |
|----------|-------------|
| `hub_name(entity)` | Returns `HUB_<ENTITY>` |
| `link_name(entities)` | Returns `LNK_<ENTITY1>_<ENTITY2>` |
| `sat_name(entity, source)` | Returns `SAT_<ENTITY>_<SOURCE>` |
| `build_hubs(sources, bk_map, record_source, load_date_col, hash_algorithm)` | Identifies business keys and builds Hub definitions |
| `build_links(sources, bk_map, mappings, ...)` | Detects FK relationships and builds Link definitions |
| `build_satellites(sources, bk_map, ...)` | Assigns descriptive columns to Satellites with HASH_DIFF |
| `DataVaultAgent.run(metadata, sttm, output_dir, hash_algorithm)` | Orchestrator — produces full DV model |

**Classification rules:**

| Rule | Action |
|------|--------|
| Column is PK or ends in `_ID`, `_KEY`, `_NO`, `_CODE`, `_NBR` | → Business Key → Hub |
| Two+ business keys from different entities in same mapping | → Link |
| Remaining descriptive columns | → Satellite |
| `LOAD_DATE`, `RECORD_SOURCE` | → Technical columns (all entities) |

**Output:**

| File | Location | Description |
|------|----------|-------------|
| `dv_model.json` | `output/data_vault/` | Complete DV model: hubs, links, satellites, staging, PIT definitions |

**DV naming conventions (datavault4dbt aligned):**

| Convention | Pattern | Example |
|-----------|---------|---------|
| Hub hash key | `HK_<ENTITY>` | `HK_CUSTOMER` |
| Link hash key | `HK_LNK_<E1>_<E2>` | `HK_LNK_CUSTOMER_ORDER` |
| Satellite hash diff | `HD_<ENTITY>` | `HD_CUSTOMER` |
| Load date | `LOAD_DATE` | — |
| Record source | `RECORD_SOURCE` | — |

---

### Agent 4 — DBT Project Generator

**File:** `etl_pipeline/agents/agent_dbt_generator.py`

Generates a complete, ready-to-run dbt project using **datavault4dbt v1.17.0** macros for Snowflake.

**Functions:**

| Function | Description |
|----------|-------------|
| `generate_packages_yml(output_dir, datavault4dbt_version)` | Writes `packages.yml` with datavault4dbt dependency |
| `generate_dbt_project_yml(output_dir, project_name, profile_name, ...)` | Writes `dbt_project.yml` with Snowflake schema config and global vars |
| `generate_profiles_yml(output_dir, profile_name, target_database)` | Writes `profiles.yml` with Snowflake connection template |
| `generate_staging_model(output_dir, source_table, columns, bk_cols, ...)` | Writes `stg_<entity>.sql` using `datavault4dbt.stage()` macro |
| `generate_hub_model(output_dir, hub)` | Writes `hub_<entity>.sql` using `datavault4dbt.hub()` macro |
| `generate_link_model(output_dir, link)` | Writes `lnk_<e1>_<e2>.sql` using `datavault4dbt.link()` macro |
| `generate_satellite_model(output_dir, sat)` | Writes `sat_<entity>.sql` using `datavault4dbt.sat()` macro |
| `generate_sources_yml(output_dir, metadata)` | Writes `sources.yml` with all source definitions |
| `generate_schema_yml(output_dir, dv_model)` | Writes `schema.yml` with model descriptions and dbt tests |
| `DBTGeneratorAgent.run(...)` | Orchestrator — generates full project |

**Output:**

| File | Location | Description |
|------|----------|-------------|
| `dbt_project.yml` | `output/dbt_project/` | Project config, Snowflake schemas, datavault4dbt global vars |
| `packages.yml` | `output/dbt_project/` | datavault4dbt v1.17.0 dependency |
| `profiles.yml` | `output/dbt_project/` | Snowflake connection (dev/prod targets) |
| `stg_<entity>.sql` | `models/staging/` | Staging view with hash key + hash diff computation |
| `hub_<entity>.sql` | `models/raw_vault/hubs/` | Hub model using `datavault4dbt.hub()` |
| `lnk_<e1>_<e2>.sql` | `models/raw_vault/links/` | Link model using `datavault4dbt.link()` |
| `sat_<entity>.sql` | `models/raw_vault/satellites/` | Satellite model using `datavault4dbt.sat()` |
| `sources.yml` | `models/staging/` | dbt source definitions |
| `schema.yml` | `models/raw_vault/` | Model descriptions + dbt column tests |
| `dq_tests.yml` | `models/raw_vault/` | Data quality tests from Agent 7 |

**datavault4dbt global variables (set in `dbt_project.yml`):**

| Variable | Default | Description |
|----------|---------|-------------|
| `hash` | `MD5` | Hash algorithm (MD5 or SHA-256) |
| `concat_char` | `\|` | Separator for composite keys |
| `load_date` | `LOAD_DATE` | Technical load timestamp column |
| `record_source` | `RECORD_SOURCE` | Source system identifier column |
| `hash_dtype` | `string` | Hash key data type |

**Snowflake schema layout:**

| dbt Layer | Snowflake Schema | Materialization |
|-----------|-----------------|-----------------|
| Staging | `STAGING` | view |
| Raw Vault | `RAW_VAULT` | incremental |
| Business Vault | `BUSINESS_VAULT` | table |

---

### Agent 5 — Data Profiler

**File:** `etl_pipeline/agents/agent_profiler.py`

Profiles source DataFrames using **ydata-profiling** and detects PII using **Microsoft Presidio** with spaCy NLP.

**Functions:**

| Function | Description |
|----------|-------------|
| `run_ydata_profiling(df, dataset_name, output_dir)` | Runs ProfileReport — extracts dataset overview, column stats, descriptive stats, correlations |
| `run_pii_detection(df, dataset_name)` | Scans all string columns with Presidio — returns per-cell and per-column PII results |
| `ProfilerAgent.run(df, dataset_name, output_dir)` | Orchestrator — runs both, saves all outputs |

**PII entity types detected:**

| Entity | Recognizer Type | Default Threshold |
|--------|----------------|-------------------|
| `EMAIL_ADDRESS` | Pattern (regex) | 0.50 |
| `PHONE_NUMBER` | Pattern (regex) | 0.50 |
| `US_SSN` | Pattern (regex) | 0.50 |
| `CREDIT_CARD` | Pattern + Luhn check | 0.50 |
| `IP_ADDRESS` | Pattern (regex) | 0.50 |
| `DATE_TIME` | Pattern (regex) | 0.60 |
| `PERSON` | spaCy NER | 0.70 |
| `LOCATION` | spaCy NER | 0.70 |

**Output:**

| File | Location | Description |
|------|----------|-------------|
| `profile_report.html` | `output/profiling/` | Interactive ydata-profiling HTML report |
| `profiling_summary.json` | `output/profiling/` | Dataset overview, column stats, descriptive stats, correlations |
| `pii_column_summary.csv` | `output/profiling/` | Per-column: entity types, avg/max confidence, % flagged rows |

---

### Agent 6 — Data Reconciliation

**File:** `etl_pipeline/agents/agent_reconciliation.py`

Performs source-to-target reconciliation checks and generates reusable SQL scripts.

**Functions:**

| Function | Description |
|----------|-------------|
| `row_count_check(src_df, tgt_df)` | Compares row counts source vs target |
| `null_check(df, columns)` | Counts nulls in key columns |
| `checksum_check(src_df, tgt_df, numeric_cols)` | SUM/AVG comparison on numeric columns |
| `missing_key_check(src_df, tgt_df, key_col)` | Records in source but not in target |
| `extra_key_check(src_df, tgt_df, key_col)` | Records in target but not in source |
| `duplicate_key_check(df, key_cols)` | Duplicate PK detection |
| `generate_reconciliation_report(checks, output_dir)` | Writes Excel + JSON recon report |
| `ReconciliationAgent.run(src_df, tgt_df, key_column, numeric_cols, output_dir)` | Orchestrator |

**Reconciliation checks performed:**

| Check | Description | Fail Condition |
|-------|-------------|---------------|
| Row Count | Source vs target row count | Counts differ |
| Null Check | Nulls in PK/non-nullable columns | Any null found |
| Numeric Checksum | SUM + AVG of numeric columns | Values differ by > 0.01 |
| Missing Keys | Keys in source not in target | Any missing |
| Extra Keys | Keys in target not in source | Any extra |
| Duplicate Keys | Duplicate PK values in target | Any duplicate |

**Output:**

| File | Location | Sheets / Description |
|------|----------|----------------------|
| `recon_report.xlsx` | `output/reconciliation/` | Summary, All Checks, Failed Checks |
| `recon_report.json` | `output/reconciliation/` | Machine-readable recon results |
| `recon_<mapping>.sql` | `output/reconciliation/` | Ready-to-run reconciliation SQL per mapping |

**Recommendations generated per mapping:**
- Run row-count check before and after each load
- Validate checksum on all numeric columns post-load
- Assert uniqueness on PK columns via dbt test
- Store recon results in Snowflake audit table for trending
- For incremental loads: reconcile only the delta window
- For Data Vault: compare business key counts (insert-only pattern means row counts will differ)

---

### Agent 7 — Data Quality

**File:** `etl_pipeline/agents/agent_data_quality.py`

Infers DQ rules from column metadata and generates dbt schema tests, SQL DQ checks, and an Excel DQ report.

**Functions:**

| Function | Description |
|----------|-------------|
| `infer_dq_rules(col, mapping)` | Infers applicable rules from column name, dtype, PK flag, nullability |
| `evaluate_rules(df, rules)` | Evaluates rules against a DataFrame, returns pass/fail per rule |
| `generate_dbt_dq_tests(dq_rules, dv_model, output_dir)` | Writes `dq_tests.yml` with dbt schema tests |
| `generate_dq_report(rules_evaluated, output_dir)` | Writes Excel + JSON DQ report |
| `DataQualityAgent.run(df, metadata, dv_model, output_dir, dbt_project_dir)` | Orchestrator |

**DQ rules inferred by column pattern:**

| Rule | Trigger | dbt Test |
|------|---------|----------|
| `not_null` | PK column or non-nullable | `not_null` |
| `unique` | PK column | `unique` |
| `positive_values` | Column name contains AMOUNT, PRICE, SALARY, COST | `dbt_utils.expression_is_true` |
| `no_future_dates` | DATE or TIMESTAMP column | `dbt_utils.expression_is_true` |
| `date_format` | DATE or TIMESTAMP column | `dbt_utils.expression_is_true` |
| `email_pattern` | Column name contains EMAIL | `dbt_utils.expression_is_true` |
| `accepted_values` | Column name contains STATUS, CODE, TYPE | `accepted_values` |
| `referential_integrity` | FK column (ends `_ID`, `_KEY`, non-PK) | `relationships` |
| `load_date_not_future` | Column name is LOAD_DATE | `dbt_utils.expression_is_true` |

**Output:**

| File | Location | Sheets / Description |
|------|----------|----------------------|
| `dq_report.xlsx` | `output/data_quality/` | Summary, All Rules, Failed Rules, Critical Columns |
| `dq_rules.json` | `output/data_quality/` | Machine-readable DQ rules with pass/fail status |
| `dq_tests.yml` | `output/dbt_project/models/raw_vault/` | dbt schema tests ready for `dbt test` |
| `dq_<entity>.sql` | `output/data_quality/` | Standalone SQL DQ checks per entity |

---

### Agent 8 — Mermaid ER + FIBO/BIAN Alignment

**File:** `etl_pipeline/agents/agent_mermaid_er.py`

Converts the Data Vault model into **Mermaid ER and class diagrams** and annotates each entity with its **FIBO ontology class** and **BIAN Service Domain**.

**Functions:**

| Function | Description |
|----------|-------------|
| `match_fibo(entity_name)` | Matches entity name → FIBO ontology class URI (keyword lookup) |
| `match_bian(entity_name)` | Matches entity name → BIAN Service Domain (keyword lookup) |
| `sanitize_mermaid_id(name)` | Converts names to valid Mermaid identifiers |
| `dtype_to_mermaid(dtype)` | Normalizes SQL dtypes to Mermaid type labels |
| `build_mermaid_er(mappings, dv_model)` | Generates source, staging, and Data Vault ER diagrams |
| `build_fibo_bian_alignment(mappings, dv_model)` | Generates FIBO+BIAN annotated class diagram + alignment table |
| `run_agent8_mermaid_er(mappings, dv_model, output_dir)` | Orchestrator — writes all `.mmd` files + README |

**Output:**

| File | Location | Description |
|------|----------|-------------|
| `source_er.mmd` | `output/mermaid/` | ER diagram of source tables with PK/FK markers |
| `staging_er.mmd` | `output/mermaid/` | ER diagram of STG layer |
| `data_vault_er.mmd` | `output/mermaid/` | Full DV2.0 ER — Hubs, Links, Satellites with cardinality |
| `fibo_bian_class.mmd` | `output/mermaid/` | Class diagram with FIBO + BIAN annotations per entity |
| `fibo_bian_alignment.json` | `output/mermaid/` | Machine-readable alignment table |
| `README_diagrams.md` | `output/mermaid/` | Markdown embedding all diagrams (renders on GitHub) |

**Rendering options:**

```bash
# CLI (mermaid-js)
npm install -g @mermaid-js/mermaid-cli
mmdc -i output/mermaid/data_vault_er.mmd -o data_vault_er.png

# Online
# Paste .mmd content at https://mermaid.live

# GitHub
# Use ``` mermaid ``` code block — renders natively
```

---

## Standalone Notebooks

| Notebook | Description |
|----------|-------------|
| `main_pipeline.ipynb` | Main orchestration — runs all 8 agents end-to-end |
| `etl_reverse_eng_dv_pipeline.ipynb` | Self-contained single-file pipeline with synthetic demo data |
| `pii_metadata_pipeline.ipynb` | PII detection (Presidio) → SQL Server metadata tables |
| `pii_metadata_pipeline_anon.ipynb` | PII detection + masking + Fernet encryption + de-anonymization |
| `ydata_profiling_to_sql.ipynb` | ydata-profiling → 4 SQL Server metadata tables |
| `ydata_profiling_to_sql_with_pii.ipynb` | ydata-profiling + Presidio PII → 6 SQL Server metadata tables |

---

## Input File Formats

### DataStage
- **`.dsx`** — DataStage export file (binary-wrapped XML)
- **`.xml`** — DataStage XML export
- Key elements parsed: `<Job>`, `<Stage>`, `<Column>`, `<Property name="TableName">`, `<DSField>`, derivation expressions

### Informatica PowerCenter
- **`.xml`** — PowerCenter repository XML export
- **`.ipc`** — Informatica PowerCenter export package
- Key elements parsed: `<MAPPING>`, `<SOURCE>`, `<TARGET>`, `<INSTANCE>`, `<CONNECTOR>`, `<TARGETFIELD>`, `<TRANSFORMATION>`

### Synthetic Demo Mode
If no input files are provided, the pipeline runs with 3 built-in synthetic mappings (Customer, Orders, Product) covering all entity types and transformation patterns.

---

## Output Files

### Complete output inventory

| Category | File | Format | Description |
|----------|------|--------|-------------|
| Metadata | `unified_metadata.json` | JSON | Raw parsed metadata from all input files |
| STTM | `sttm.xlsx` | Excel | 5-sheet STTM workbook |
| STTM | `sttm.json` | JSON | STTM as flat JSON array |
| Data Vault | `dv_model.json` | JSON | Hubs, Links, Satellites, Staging, PIT definitions |
| dbt | `dbt_project.yml` | YAML | dbt project config + datavault4dbt global vars |
| dbt | `packages.yml` | YAML | datavault4dbt v1.17.0 dependency |
| dbt | `profiles.yml` | YAML | Snowflake connection (fill credentials) |
| dbt | `stg_*.sql` | SQL | Staging models with hash computation |
| dbt | `hub_*.sql` | SQL | Hub models |
| dbt | `lnk_*.sql` | SQL | Link models |
| dbt | `sat_*.sql` | SQL | Satellite models |
| dbt | `sources.yml` | YAML | dbt source definitions |
| dbt | `schema.yml` | YAML | Model tests and descriptions |
| dbt | `dq_tests.yml` | YAML | Data quality dbt tests |
| Profiling | `profile_report.html` | HTML | Interactive ydata-profiling report |
| Profiling | `profiling_summary.json` | JSON | Column stats, descriptive stats, correlations |
| Profiling | `pii_column_summary.csv` | CSV | PII detection results per column |
| Reconciliation | `recon_report.xlsx` | Excel | Reconciliation check results |
| Reconciliation | `recon_report.json` | JSON | Machine-readable recon results |
| Reconciliation | `recon_*.sql` | SQL | Reconciliation SQL scripts per mapping |
| Data Quality | `dq_report.xlsx` | Excel | DQ rules, pass/fail, critical columns |
| Data Quality | `dq_rules.json` | JSON | Machine-readable DQ rule results |
| Mermaid | `source_er.mmd` | Mermaid | Source layer ER diagram |
| Mermaid | `staging_er.mmd` | Mermaid | Staging layer ER diagram |
| Mermaid | `data_vault_er.mmd` | Mermaid | Data Vault 2.0 ER diagram |
| Mermaid | `fibo_bian_class.mmd` | Mermaid | FIBO + BIAN annotated class diagram |
| Mermaid | `fibo_bian_alignment.json` | JSON | FIBO/BIAN alignment per entity |
| Mermaid | `README_diagrams.md` | Markdown | All diagrams embedded, renders on GitHub |

---

## Setup & Installation

### 1. Clone

```bash
git clone https://github.com/gokulramkrishnan/ETL-Pipeline-Code.git
cd ETL-Pipeline-Code
```

### 2. Create virtual environment

```bash
python -m venv venv
source venv/bin/activate        # Mac/Linux
# venv\Scripts\activate         # Windows
```

### 3. Install Python dependencies

```bash
pip install -r requirements.txt
```

### 4. Install spaCy model (required for Presidio PII detection)

```bash
python -m spacy download en_core_web_lg   # recommended
# python -m spacy download en_core_web_sm  # lighter alternative
```

### 5. Install Mermaid CLI (optional — for PNG/SVG export)

```bash
npm install -g @mermaid-js/mermaid-cli
```

---

## Configuration

Edit **Cell 3** in `main_pipeline.ipynb` or `etl_reverse_eng_dv_pipeline.ipynb`:

```python
# ── Input files ────────────────────────────────────────────────────────────────
DATASTAGE_FILES   = ["path/to/job.dsx"]
INFORMATICA_FILES = ["path/to/mapping.xml"]
# Leave both empty [] to use synthetic demo data

# ── Snowflake connection ───────────────────────────────────────────────────────
SNOWFLAKE_CONFIG = {
    "account":   "YOUR_ACCOUNT",       # e.g. xy12345.us-east-1
    "user":      "YOUR_USER",
    "password":  "YOUR_PASSWORD",
    "warehouse": "YOUR_WAREHOUSE",
    "database":  "YOUR_DATABASE",
    "schema":    "YOUR_SCHEMA",
    "role":      "YOUR_ROLE",
}

# ── dbt project settings ───────────────────────────────────────────────────────
DBT_PROJECT_NAME = "dv_snowflake"
DV_SCHEMA_RAW    = "RAW_VAULT"
DV_SCHEMA_BIZ    = "BUSINESS_VAULT"
DV_SCHEMA_STG    = "STAGING"
HASH_ALGO        = "MD5"              # MD5 or SHA-256
```

---

## Running the Pipeline

### Option A — Jupyter notebook (recommended)

```bash
jupyter notebook main_pipeline.ipynb
```

Run cells in order. Cell 3 is configuration. Leave input file lists empty for synthetic demo mode.

### Option B — Execute notebook headlessly

```bash
jupyter nbconvert --to notebook --execute main_pipeline.ipynb --output main_pipeline_output.ipynb
```

### Option C — Import agents directly in Python

```python
from etl_pipeline.agents import (
    run_agent1_reverse_engineer,
    run_agent2_sttm,
    classify_dv_entities,
    run_agent4_dbt_generator,
    run_agent5_profiler,
    run_agent6_reconciliation,
    run_agent7_data_quality,
    run_agent8_mermaid_er,
)

# Run individual agents
mappings   = run_agent1_reverse_engineer(ds_files=[], inf_files=[], use_synthetic=True)
sttm_df    = run_agent2_sttm(mappings)
dv_model   = classify_dv_entities(mappings)
dbt_files  = run_agent4_dbt_generator(mappings, dv_model)
profiles   = run_agent5_profiler(mappings)
recon      = run_agent6_reconciliation(mappings)
dq_df      = run_agent7_data_quality(mappings, dv_model)
diagrams   = run_agent8_mermaid_er(mappings, dv_model, output_dir=Path("output"))
```

---

## dbt Deployment

```bash
cd etl_pipeline/output/dbt_project

# 1. Install datavault4dbt v1.17.0
dbt deps

# 2. Copy profiles and fill credentials
cp profiles.yml ~/.dbt/profiles.yml
# Edit ~/.dbt/profiles.yml with your Snowflake credentials

# 3. Validate connection
dbt debug

# 4. Build models in layer order
dbt run --select staging
dbt run --select raw_vault
dbt run --select business_vault

# 5. Run all DQ tests
dbt test

# 6. Generate documentation
dbt docs generate
dbt docs serve
```

---

## Reference Models

### FIBO — Financial Industry Business Ontology

- **Maintained by:** [EDM Council](https://edmcouncil.org) / standardized by [OMG](https://omg.org)
- **Specification:** [spec.edmcouncil.org/fibo](https://spec.edmcouncil.org/fibo)
- **FIB-DM (data model layer):** [fib-dm.com](https://fib-dm.com) — 2,436 classes, 2025/Q4 release
- **GitHub:** [github.com/edmcouncil/fibo](https://github.com/edmcouncil/fibo)

FIBO defines the sets of things of interest in financial business applications and the relationships between them. It provides a common, unambiguous vocabulary enabling cross-system federation and regulatory reporting.

| FIBO Module | Domain | Entities Mapped |
|-------------|--------|----------------|
| `FND` | Foundations | Party, Currency, Address, Date, Amount, Contract, Identifier |
| `BE` | Business Entities | LegalPerson (Customer), Organization |
| `FBC` | Financial Business & Commerce | Account, Product, Payment, Transaction, Instrument |
| `SEC` | Securities | Security, InstrumentPrice |
| `LOAN` | Loans | Loan |

### BIAN — Banking Industry Architecture Network

- **Website:** [bian.org](https://bian.org) — not-for-profit, established 2008
- **Version:** v13 (current as of 2024)
- **Scope:** 326 Service Domains | 250+ APIs | 5,000+ service operations
- **Wikipedia:** [Banking Industry Architecture Network](https://en.wikipedia.org/wiki/Banking_Industry_Architecture_Network)

BIAN provides a common architectural framework for banking interoperability by decomposing banking operations into discrete, non-overlapping **Service Domains** — each representing a specific business capability.

| BIAN Business Area | Service Domains Mapped |
|--------------------|------------------------|
| Sales & Service | Customer Relationship Management, Sales Order, Customer Agreement |
| Operations & Execution | Current Account, Payment Execution, Securities Position Keeping, Market Data |
| Products | Product Directory, Consumer Loan, Investment Portfolio Management |
| Business Support | Party Reference Data, Financial Accounting, Human Resources |
| Risk & Compliance | Credit Risk Operations |

### datavault4dbt

- **GitHub:** [github.com/ScalefreeCOM/datavault4dbt](https://github.com/ScalefreeCOM/datavault4dbt)
- **Version:** v1.17.0 (latest)
- **dbt Hub:** [hub.getdbt.com/ScalefreeCOM/datavault4dbt](https://hub.getdbt.com/ScalefreeCOM/datavault4dbt/latest/)
- Macros used: `stage()`, `hub()`, `link()`, `sat()`, `nh_link()`, `pit()`

### Additional References

- [Data Vault 2.0 on Databricks/Snowflake — Microsoft Tech Community](https://techcommunity.microsoft.com/blog/analyticsazure/data-vault-2-0-using-databricks-lakehouse-architecture-on-azure/3797493)
- [turbovault4dbt — auto-generate DV models from metadata](https://github.com/ScalefreeCOM/turbovault4dbt)
- [ydata-profiling](https://github.com/ydataai/ydata-profiling)
- [Microsoft Presidio — PII detection](https://github.com/microsoft/presidio)
- [dbt Documentation](https://docs.getdbt.com)

---

## Dependencies

```
# Core
pandas>=2.0.0
numpy>=1.24.0
openpyxl>=3.1.0
xlsxwriter>=3.1.0
lxml>=4.9.0
pyyaml>=6.0
jinja2>=3.1.0
rich>=13.0.0

# Profiling
ydata-profiling>=4.6.0

# PII detection
presidio-analyzer>=2.2.0
presidio-anonymizer>=2.2.0
spacy>=3.7.0
# python -m spacy download en_core_web_lg

# Encryption (anonymization notebooks)
cryptography>=41.0.0

# Synthetic data
faker>=20.0.0

# Snowflake
snowflake-connector-python>=3.5.0
snowflake-sqlalchemy>=1.5.0

# SQL Server (PII metadata notebooks)
pyodbc>=5.0.0
sqlalchemy>=2.0.0

# dbt
dbt-core>=1.7.0
dbt-snowflake>=1.7.0

# Data quality
great-expectations>=0.18.0
```

---

## License

MIT License — © gokulram.krishnan

Permission is hereby granted, free of charge, to any person obtaining a copy of this software to deal in the software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the software.
