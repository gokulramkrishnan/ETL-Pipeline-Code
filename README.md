# ETL Data Vault Pipeline
**Author:** gokulram.krishnan  
**Repository:** https://github.com/gokulramkrishnan/Useful-Jupyter-Notebook  

A fully agentic, end-to-end data engineering pipeline that reverse-engineers DataStage and Informatica ETL files and produces a complete Data Vault 2.0 solution on Snowflake using dbt + datavault4dbt.

---

## 📦 Package Contents

```
etl_dv_pipeline/
│
├── etl_pipeline/
│   ├── agents/
│   │   ├── agent_reverse_engineer.py   # Agent 1 — Parse DataStage & Informatica files
│   │   ├── agent_sttm.py               # Agent 2 — Source-to-Target Mapping generator
│   │   ├── agent_data_vault.py         # Agent 3 — Data Vault 2.0 modeler (Hub/Link/Sat)
│   │   ├── agent_dbt_generator.py      # Agent 4 — dbt model + config generator
│   │   ├── agent_profiler.py           # Agent 5 — ydata-profiling + Presidio PII
│   │   ├── agent_reconciliation.py     # Agent 6 — Reconciliation SQL + recommendations
│   │   └── agent_data_quality.py       # Agent 7 — DQ rule engine + dbt tests
│   ├── input/
│   │   ├── datastage_sample.dsx        # Sample DataStage export
│   │   └── informatica_sample.xml      # Sample Informatica export
│   └── main_pipeline.ipynb             # Main orchestration notebook
│
├── etl_reverse_eng_dv_pipeline.ipynb   # Full self-contained pipeline notebook
├── pii_metadata_pipeline.ipynb         # PII detection + SQL Server metadata pipeline
├── pii_metadata_pipeline_anon.ipynb    # PII detection + anonymization + de-anonymization
├── ydata_profiling_to_sql.ipynb        # ydata-profiling → SQL Server
├── ydata_profiling_to_sql_with_pii.ipynb # ydata-profiling + PII → SQL Server
│
├── .gitignore
├── requirements.txt
└── README.md
```

---

## 🚀 Pipeline Architecture

```
INPUT: DataStage (.dsx/.xml) | Informatica (.xml/.ipc)
    │
    ▼
[Agent 1] Reverse Engineer   → Unified mapping schema
    │
    ▼
[Agent 2] STTM Generator     → STTM.xlsx + STTM.json
    │
    ▼
[Agent 3] Data Vault Modeler → Hubs, Links, Satellites, PIT (datavault4dbt)
    │
    ▼
[Agent 4] DBT Generator      → Staging + Raw Vault + Business Vault SQL/YAML
    │
    ▼
[Agent 5] Data Profiler      → ydata-profiling HTML + Presidio PII classification
    │
    ▼
[Agent 6] Reconciliation     → Row count, checksum, duplicate, missing record SQL
    │
    ▼
[Agent 7] Data Quality       → dbt schema tests + SQL DQ checks + DQ report
```

---

## 🛠️ Setup

### 1. Clone
```bash
git clone https://github.com/gokulramkrishnan/Useful-Jupyter-Notebook.git
cd Useful-Jupyter-Notebook
```

### 2. Install dependencies
```bash
pip install -r requirements.txt
python -m spacy download en_core_web_lg
```

### 3. Configure
Edit Cell 3 in `etl_reverse_eng_dv_pipeline.ipynb`:
- Point `DATASTAGE_FILES` / `INFORMATICA_FILES` to your input files
- Fill in `SNOWFLAKE_CONFIG` with your connection details
- Leave both lists empty to run with synthetic demo data

### 4. Run
```bash
jupyter notebook etl_reverse_eng_dv_pipeline.ipynb
```
Or run all agents end-to-end:
```bash
jupyter nbconvert --to notebook --execute etl_reverse_eng_dv_pipeline.ipynb
```

### 5. Deploy dbt models
```bash
cd dv_pipeline_output/dbt
dbt deps          # installs datavault4dbt v1.17.0
dbt debug         # validates Snowflake connection
dbt run           # builds staging → hubs → links → satellites
dbt test          # runs all DQ rules
```

---

## 📊 Output Files

| Output | Location | Description |
|--------|----------|-------------|
| STTM.xlsx | `output/sttm/` | Source-to-Target Mapping workbook |
| dv_model.json | `output/data_vault/` | Data Vault 2.0 entity model |
| dbt models | `output/dbt_project/models/` | Ready-to-run dbt SQL + YAML |
| Profile reports | `output/profiling/` | HTML profiling per entity |
| Recon SQL | `output/reconciliation/` | Reconciliation query scripts |
| DQ report | `output/data_quality/` | Data quality rules + Excel report |

---

## 🔗 References
- [datavault4dbt v1.17.0](https://github.com/ScalefreeCOM/datavault4dbt)
- [turbovault4dbt](https://github.com/ScalefreeCOM/turbovault4dbt)
- [Data Vault 2.0 on Databricks/Snowflake](https://techcommunity.microsoft.com/blog/analyticsazure/data-vault-2-0-using-databricks-lakehouse-architecture-on-azure/3797493)
- [dbt Documentation](https://docs.getdbt.com)

---

## 📄 License
MIT License — © gokulram.krishnan
