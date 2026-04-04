# =============================================================================
# Author      : gokulram.krishnan
# Repository  : https://github.com/gokulramkrishnan/Useful-Jupyter-Notebook
# Description : """
# =============================================================================
"""
Agent 3: Data Vault 2.0 Modeler
Generates Hub, Link, Satellite definitions aligned to datavault4dbt v1.17.0
conventions for Snowflake. Uses MD5/SHA-256 hash keys, LDTS, RSRC columns.

References:
  - datavault4dbt: github.com/ScalefreeCOM/datavault4dbt
  - DV 2.0 standard: Dan Linstedt
"""

import json
import re
from typing import Optional


# ─────────────────────────────────────────────────────────────────────────────
# Naming conventions (datavault4dbt aligned)
# ─────────────────────────────────────────────────────────────────────────────

def hub_name(entity: str) -> str:
    return f"HUB_{entity.upper()}"

def link_name(entity_a: str, entity_b: str) -> str:
    return f"LNK_{entity_a.upper()}_{entity_b.upper()}"

def sat_name(entity: str, source: str) -> str:
    src_clean = re.sub(r"^SRC_", "", source.upper())
    return f"SAT_{entity.upper()}_{src_clean}"

def hash_key_col(entity: str) -> str:
    return f"{entity.upper()}_HK"

def biz_key_col(entity: str) -> str:
    return f"{entity.upper()}_BK"

def hashdiff_col(entity: str, source: str) -> str:
    src_clean = re.sub(r"^SRC_", "", source.upper())
    return f"{entity.upper()}_{src_clean}_HASHDIFF"


# ─────────────────────────────────────────────────────────────────────────────
# Core modeling functions
# ─────────────────────────────────────────────────────────────────────────────

def infer_business_keys(sources: list) -> dict:
    """
    Infer business key columns from source metadata.
    Looks for PRIMARY KEY flags, columns ending in _ID, _CODE, _KEY, _NUM.
    Returns: {table_name: [bk_column, ...]}
    """
    bk_map = {}
    for src in sources:
        table = src.get("table_name", "")
        bks = []
        for col in src.get("columns", []):
            col_name = col.get("name", "")
            key_type = col.get("key_type", "").upper()
            is_pk    = "PRIMARY" in key_type
            is_bk    = bool(re.search(r"(_ID|_CODE|_KEY|_NUM|_NO)$", col_name, re.IGNORECASE))
            if is_pk or is_bk:
                bks.append(col_name)
        if not bks and src.get("columns"):
            # Fallback: first column assumed as BK
            bks = [src["columns"][0]["name"]]
        bk_map[table] = list(dict.fromkeys(bks))  # deduplicate preserving order
    return bk_map


def infer_entity_name(table_name: str) -> str:
    """Strip SRC_/TGT_/DIM_/FACT_ prefixes to get clean entity name."""
    clean = re.sub(r"^(SRC_|TGT_|STG_|DIM_|FACT_|ODS_|RAW_)", "", table_name, flags=re.IGNORECASE)
    return clean.upper()


def build_hubs(sources: list, bk_map: dict, record_source: str, load_date_col: str, hash_algorithm: str) -> list:
    """Build Hub definitions for each source entity."""
    hubs = []
    seen = set()
    for src in sources:
        table      = src.get("table_name","")
        entity     = infer_entity_name(table)
        if entity in seen:
            continue
        seen.add(entity)

        bk_columns = bk_map.get(table, [])
        hubs.append({
            "hub_name":        hub_name(entity),
            "entity":          entity,
            "source_table":    table,
            "hash_key":        hash_key_col(entity),
            "business_keys":   bk_columns,
            "ldts":            load_date_col,
            "rsrc":            "RECORD_SOURCE",
            "hash_algorithm":  hash_algorithm,
            # datavault4dbt macro parameters
            "dv4dbt": {
                "src_pk":     hash_key_col(entity),
                "src_bk":     bk_columns,
                "src_ldts":   load_date_col,
                "src_source": "RECORD_SOURCE",
                "source_model": f"stg_{table.lower()}",
            }
        })
    return hubs


def build_links(sources: list, bk_map: dict, mappings: list,
                record_source: str, load_date_col: str, hash_algorithm: str) -> list:
    """
    Build Link definitions for FK relationships detected in source metadata.
    Also infers links from column name patterns (e.g. CUSTOMER_ID in ORDER table).
    """
    links = []
    seen  = set()

    # Build entity→table map
    entity_table = {infer_entity_name(s["table_name"]): s["table_name"] for s in sources}

    for src in sources:
        src_table  = src.get("table_name", "")
        src_entity = infer_entity_name(src_table)

        for col in src.get("columns", []):
            col_name = col.get("name", "")
            key_type = col.get("key_type", "").upper()
            is_fk    = "FOREIGN" in key_type

            # Infer FK by column naming pattern (e.g. CUSTOMER_ID in ORDER table)
            fk_match = re.match(r"^([A-Z_]+?)_ID$", col_name.upper())
            related_entity = fk_match.group(1) if fk_match else None

            if (is_fk or related_entity) and related_entity and related_entity != src_entity:
                pair = tuple(sorted([src_entity, related_entity]))
                if pair not in seen:
                    seen.add(pair)
                    ea, eb = pair
                    # Determine staging sources for both sides
                    src_model_a = f"stg_{entity_table.get(ea, ea).lower()}"
                    src_model_b = f"stg_{entity_table.get(eb, eb).lower()}"

                    links.append({
                        "link_name":      link_name(ea, eb),
                        "entity_a":       ea,
                        "entity_b":       eb,
                        "hash_key":       f"LNK_{ea}_{eb}_HK",
                        "hub_a_hk":       hash_key_col(ea),
                        "hub_b_hk":       hash_key_col(eb),
                        "ldts":           load_date_col,
                        "rsrc":           "RECORD_SOURCE",
                        "hash_algorithm": hash_algorithm,
                        "dv4dbt": {
                            "src_pk":        f"LNK_{ea}_{eb}_HK",
                            "src_fk":        [hash_key_col(ea), hash_key_col(eb)],
                            "src_ldts":      load_date_col,
                            "src_source":    "RECORD_SOURCE",
                            "source_model":  [src_model_a, src_model_b],
                        }
                    })
    return links


def build_satellites(sources: list, bk_map: dict,
                     record_source: str, load_date_col: str, hash_algorithm: str) -> list:
    """Build Satellite definitions — one SAT per source table, containing all non-BK columns."""
    satellites = []
    for src in sources:
        table     = src.get("table_name","")
        entity    = infer_entity_name(table)
        bk_cols   = set(bk_map.get(table, []))

        # Non-BK, non-technical columns become satellite attributes
        sat_cols = [
            {
                "name":      c["name"],
                "data_type": c.get("data_type",""),
                "nullable":  c.get("nullable","Y"),
                "pii":       _is_pii(c["name"]),
            }
            for c in src.get("columns",[])
            if c["name"] not in bk_cols
            and c["name"].upper() not in {load_date_col.upper(), "RECORD_SOURCE", "RSRC", "LDTS"}
        ]

        if not sat_cols:
            continue

        hd_col = hashdiff_col(entity, table)
        satellites.append({
            "sat_name":       sat_name(entity, table),
            "entity":         entity,
            "source_table":   table,
            "hub_hash_key":   hash_key_col(entity),
            "hashdiff":       hd_col,
            "attributes":     sat_cols,
            "ldts":           load_date_col,
            "rsrc":           "RECORD_SOURCE",
            "hash_algorithm": hash_algorithm,
            "dv4dbt": {
                "src_pk":       hash_key_col(entity),
                "src_hashdiff": {
                    "is_hashdiff": True,
                    "columns":     [c["name"] for c in sat_cols],
                },
                "src_payload":  [c["name"] for c in sat_cols],
                "src_ldts":     load_date_col,
                "src_source":   "RECORD_SOURCE",
                "source_model": f"stg_{table.lower()}",
            }
        })
    return satellites


def _is_pii(col_name: str) -> bool:
    patterns = ["email","ssn","phone","credit","card","dob","birth",
                "address","passport","license","name","ip_address","tax"]
    return any(p in col_name.lower() for p in patterns)


# ─────────────────────────────────────────────────────────────────────────────
# Agent class
# ─────────────────────────────────────────────────────────────────────────────

class DataVaultAgent:
    def run(self, metadata: dict, sttm: dict, output_dir: str,
            record_source: str, load_date_col: str, hash_algorithm: str) -> dict:

        sources = metadata.get("sources", [])
        print(f"  Inferring business keys from {len(sources)} source tables...")
        bk_map = infer_business_keys(sources)
        for table, bks in bk_map.items():
            print(f"    {table}: BK = {bks}")

        hubs       = build_hubs(sources, bk_map, record_source, load_date_col, hash_algorithm)
        links      = build_links(sources, bk_map, metadata.get("mappings",[]), record_source, load_date_col, hash_algorithm)
        satellites = build_satellites(sources, bk_map, record_source, load_date_col, hash_algorithm)

        dv_model = {
            "hash_algorithm": hash_algorithm,
            "load_date_col":  load_date_col,
            "record_source":  record_source,
            "hubs":           hubs,
            "links":          links,
            "satellites":     satellites,
        }

        with open(f"{output_dir}/dv_model.json", "w") as f:
            json.dump(dv_model, f, indent=2)

        print(f"    ✅ Hubs       : {len(hubs)}")
        print(f"    ✅ Links      : {len(links)}")
        print(f"    ✅ Satellites : {len(satellites)}")

        return dv_model
