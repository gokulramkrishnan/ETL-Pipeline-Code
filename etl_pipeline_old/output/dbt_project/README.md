# dv_snowflake_project
Auto-generated dbt Data Vault 2.0 project using datavault4dbt v1.17.0.

## Setup
```bash
pip install dbt-snowflake
cp profiles.yml ~/.dbt/profiles.yml   # fill in credentials
dbt deps                               # install datavault4dbt
dbt run --select staging               # run staging models
dbt run --select raw_vault             # run raw vault
dbt test                               # run all tests
```

## Structure
```
models/
  staging/          — view models, hashing + technical columns
  raw_vault/
    hubs/           — 4 Hub(s): HUB_CUSTOMER, HUB_ORDER, HUB_DIM_CUSTOMER, HUB_PRODUCT
    links/          — 2 Link(s): LNK_CUSTOMER_ORDER, LNK_ORDER_PRODUCT
    satellites/     — 5 Satellite(s)
  business_vault/   — add your Business Vault models here
```

## datavault4dbt global variables (dbt_project.yml)
| Variable | Value |
|---|---|
| hash | MD5 |
| load_datetime | LOAD_DATE |
| record_source | RECORD_SOURCE |
