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



Agent 8 — Mermaid ER + FIBO/BIAN Alignment
What it does
Agent 8 converts the Data Vault model produced by Agent 3 into Mermaid ER and class diagrams and annotates every entity with its corresponding FIBO ontology class and BIAN Service Domain.
Output files (output/mermaid/)
FileDescriptionsource_er.mmdER diagram of original source tables with PK/FK markersstaging_er.mmdER diagram of STG layer tablesdata_vault_er.mmdFull Data Vault 2.0 ER — Hubs, Links, Satellites with cardinalityfibo_bian_class.mmdClass diagram annotating each Hub/Sat with FIBO class + BIAN Service Domainfibo_bian_alignment.jsonMachine-readable alignment tableREADME_diagrams.mdMarkdown file embedding all diagrams — renders natively on GitHub

Reference Models Used
FIBO — Financial Industry Business Ontology

Maintained by: EDM Council / standardized by OMG
Spec: spec.edmcouncil.org/fibo
FIB-DM (data model layer): fib-dm.com — 2,436 classes across 7 domains
FIBO domains mapped in this pipeline:

FIBO ModuleDomainExample EntitiesFNDFoundationsParty, Currency, Address, Date, AmountBEBusiness EntitiesLegalPerson (Customer), OrganizationFBCFinancial Business & CommerceAccount, Product, Payment, TransactionSECSecuritiesSecurity, InstrumentPriceLOANLoansLoan
BIAN — Banking Industry Architecture Network

Website: bian.org — not-for-profit, established 2008
Version: v13 (current as of 2024)
Scope: 326 Service Domains | 250+ APIs | 5,000+ service operations
BIAN domains mapped in this pipeline:

BIAN Business AreaService Domains CoveredSales & ServiceCustomer Relationship Management, Sales Order, Customer AgreementOperations & ExecutionCurrent Account, Payment Execution, Securities Position KeepingProductsProduct Directory, Consumer Loan, Investment Portfolio ManagementBusiness SupportParty Reference Data Management, Financial Accounting, Human ResourcesRisk & ComplianceCredit Risk Operations

Usage
As part of the pipeline (in main_pipeline.ipynb)
pythonfrom agents.agent_mermaid_er import run_agent8_mermaid_er

MERMAID_RESULT = run_agent8_mermaid_er(
    mappings   = MAPPINGS,    # from Agent 1
    dv_model   = DV_MODEL,    # from Agent 3
    output_dir = OUTPUT_DIR,
)
Standalone
bashcd etl_pipeline
python agents/agent_mermaid_er.py
Render diagrams
bash# Install Mermaid CLI
npm install -g @mermaid-js/mermaid-cli

# Export to PNG
mmdc -i output/mermaid/data_vault_er.mmd -o output/mermaid/data_vault_er.png
mmdc -i output/mermaid/fibo_bian_class.mmd -o output/mermaid/fibo_bian_class.png

---

## 📄 License
MIT License — © gokulram.krishnan
