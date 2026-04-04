# Testing Guide

**Author:** gokulram.krishnan  
**Repository:** https://github.com/gokulramkrishnan/ETL-Pipeline-Code

---

## Quick Start

```bash
# 1. Install the package in editable mode
pip install -e .
pip install -r requirements.txt
python -m spacy download en_core_web_lg

# 2. Run the full test suite
python test_pipeline.py
```

Outputs:
- `test_results.html` — interactive HTML report with per-test assertions
- `test_results.json` — machine-readable results

---

## Test Results Summary (v1.0.0)

| Agent | Tests | Pass | Fail | Error |
|-------|-------|------|------|-------|
| Agent 1 — Reverse Engineer | 5 | ✅ 5 | 0 | 0 |
| Agent 2 — STTM Generator | 5 | ✅ 5 | 0 | 0 |
| Agent 3 — Data Vault Modeler | 6 | ✅ 6 | 0 | 0 |
| Agent 4 — DBT Generator | 6 | ✅ 6 | 0 | 0 |
| Agent 6 — Reconciliation | 5 | ✅ 5 | 0 | 0 |
| Agent 7 — Data Quality | 5 | ✅ 5 | 0 | 0 |
| Agent 8 — Mermaid ER + FIBO/BIAN | 5 | ✅ 5 | 0 | 0 |
| **TOTAL** | **37** | **✅ 37** | **0** | **0** |
| **Pass Rate** | | **100%** | | |

---

## Sample Data Used

| Dataset | Rows | Purpose | Intentional Issues |
|---------|------|---------|-------------------|
| DataStage DSX (synthetic XML) | 2 jobs, 9 cols | Agent 1 parser | None |
| Informatica PowerCenter XML | 1 mapping, 6 cols | Agent 1 parser | None |
| Unified metadata dict | 3 sources, 2 targets | Agents 2–8 | Mixed PK/FK/nullable |
| `src_customer` DataFrame | 100 rows | Agent 6 recon source | None |
| `tgt_customer` DataFrame | 95 rows | Agent 6 recon target | 5 rows missing (gap test) |
| `src_with_issues` DataFrame | 100 rows | Agent 7 DQ evaluation | 2× null EMAIL, 1× negative ORDER_AMOUNT |

---

## What Each Test Validates

### Agent 1 — Reverse Engineer
- `Parse DataStage DSX/XML` — parses 2-job DSX, extracts sources/targets/columns/transformations
- `Parse Informatica PowerCenter XML` — parses mapping XML, extracts connectors/transformations
- `Merge DataStage + Informatica` — deduplicates sources, combines provenance from both tools
- `Sample metadata fallback` — returns realistic sample when no files provided
- `Column-level extraction` — CUSTOMER_ID and EMAIL columns extracted with correct data types

### Agent 2 — STTM Generator
- `STTM structure` — output has mappings + summary keys
- `STTM required columns` — source_table, source_column, target_table, target_column present
- `Transformation classification` — EXPRESSION/HASH/STRING_TRANSFORM correctly classified
- `PII flagging` — EMAIL columns flagged as PII
- `Excel + JSON output` — sttm.xlsx (multiple sheets) + sttm.json written

### Agent 3 — Data Vault 2.0 Modeler
- `Naming conventions` — HUB_CUSTOMER, LNK_CUSTOMER_ORDER, SAT_CUSTOMER match datavault4dbt standard
- `Business key inference` — CUSTOMER_ID, ORDER_ID, PRODUCT_ID correctly identified as BKs
- `Build Hubs` — 3 hubs with hub_name, hash_key, business_keys, ldts, rsrc
- `Build Links` — 2 links (LNK_CUSTOMER_ORDER, LNK_ORDER_PRODUCT) with FK hash keys
- `Build Satellites` — 3 satellites with hub_hash_key, hashdiff, attributes
- `Full DV model + JSON` — dv_model.json written with complete model

### Agent 4 — DBT Generator
- `packages.yml` — datavault4dbt v1.17.0 dependency correct
- `dbt_project.yml` — RAW_VAULT/BUSINESS_VAULT/STAGING schemas, MD5 hash algo, global vars
- `profiles.yml` — Snowflake adapter configured
- `Hub SQL models` — uses `datavault4dbt.hub()` macro with src_pk
- `Satellite SQL models` — uses `datavault4dbt.sat()` macro with src_hashdiff
- `Full dbt project` — all config files + staging SQL generated end-to-end

### Agent 6 — Reconciliation
- `Row count mismatch` — correctly detects gap of 5 (100 src vs 95 tgt), status=FAIL
- `Row count match` — equal counts correctly return PASS
- `Missing key detection` — identifies 5 missing CUSTOMER_IDs with sample list
- `Aggregate checksum` — SUM(ORDER_AMOUNT) differs by ~3.4% due to 5 missing rows, status=FAIL
- `Report output` — recon_report.xlsx + recon_report.json written

### Agent 7 — Data Quality
- `DQ rule generation` — 24 rules generated across COMPLETENESS, ACCURACY, UNIQUENESS, etc.
- `not_null rules` — not_null rules present for non-nullable columns
- `Rule evaluation` — PASS/FAIL statuses returned for all evaluated rules
- `Failure detection` — 1 FAIL detected (negative ORDER_AMOUNT violates ACCURACY rule)
- `Report output` — dq_report.xlsx + summary with pass_pct written

### Agent 8 — Mermaid ER + FIBO/BIAN
- `FIBO matching` — CUSTOMER→fibo-be-le-lp:LegalPerson, ORDER→fibo-fbc-pas-fpas:ContractualProduct
- `BIAN matching` — CUSTOMER→Customer Relationship Management, PAYMENT→Payment Execution
- `ER diagram generation` — source_er, staging_er, data_vault_er all start with `erDiagram`
- `Class diagram` — FIBO and BIAN annotations present per entity, starts with `classDiagram`
- `File output` — all 6 files written: source_er.mmd, staging_er.mmd, data_vault_er.mmd, fibo_bian_class.mmd, fibo_bian_alignment.json, README_diagrams.md
