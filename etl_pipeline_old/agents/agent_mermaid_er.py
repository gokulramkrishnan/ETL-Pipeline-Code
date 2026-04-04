# =============================================================================
# Author      : gokulram.krishnan
# Repository  : https://github.com/gokulramkrishnan/ETL-Pipeline-Code
# Description : Agent 8 — Data Model to Mermaid ER + FIBO/BIAN reference alignment
#
# References:
#   FIBO  : https://spec.edmcouncil.org/fibo/ (EDM Council, OMG standard)
#           FIB-DM : https://fib-dm.com — 2,436 classes across Foundations,
#           Business Entities, Finance, Securities, Derivatives, Instruments
#   BIAN  : https://bian.org — 326 Service Domains, 250+ APIs
#           Banking Industry Architecture Network (not-for-profit, est. 2008)
# =============================================================================

from __future__ import annotations
import json
import re
from pathlib import Path
from datetime import datetime
from typing import Optional

# ── FIBO concept mapping ──────────────────────────────────────────────────────
# Maps common entity/column name patterns → FIBO ontology class URIs
# Source: FIBO Production release 2025/Q4, spec.edmcouncil.org/fibo
FIBO_CONCEPT_MAP = {
    # Business Entities (BE)
    "CUSTOMER":          {"fibo_class": "fibo-be-le-lp:LegalPerson",          "fibo_domain": "BusinessEntities",    "fibo_module": "BE"},
    "PARTY":             {"fibo_class": "fibo-fnd-pty-pty:Party",              "fibo_domain": "Foundations",         "fibo_module": "FND"},
    "ORGANIZATION":      {"fibo_class": "fibo-fnd-org-org:Organization",       "fibo_domain": "Foundations",         "fibo_module": "FND"},
    "ACCOUNT":           {"fibo_class": "fibo-fbc-pas-caa:Account",            "fibo_domain": "FinancialBusinessAndCommerce", "fibo_module": "FBC"},
    "PRODUCT":           {"fibo_class": "fibo-fbc-pas-fpas:FinancialProduct",  "fibo_domain": "FinancialBusinessAndCommerce", "fibo_module": "FBC"},
    "ORDER":             {"fibo_class": "fibo-fbc-pas-fpas:ContractualProduct", "fibo_domain": "FinancialBusinessAndCommerce", "fibo_module": "FBC"},
    "CONTRACT":          {"fibo_class": "fibo-fnd-agr-ctr:Contract",           "fibo_domain": "Foundations",         "fibo_module": "FND"},
    "TRANSACTION":       {"fibo_class": "fibo-fbc-pas-caa:AccountTransaction",  "fibo_domain": "FinancialBusinessAndCommerce", "fibo_module": "FBC"},
    "PAYMENT":           {"fibo_class": "fibo-fbc-pas-caa:Payment",            "fibo_domain": "FinancialBusinessAndCommerce", "fibo_module": "FBC"},
    "LOAN":              {"fibo_class": "fibo-loan-ln-ln:Loan",                "fibo_domain": "Loans",               "fibo_module": "LOAN"},
    "SECURITY":          {"fibo_class": "fibo-sec-sec-lst:Security",           "fibo_domain": "Securities",          "fibo_module": "SEC"},
    "INSTRUMENT":        {"fibo_class": "fibo-fbc-fi-fi:FinancialInstrument",  "fibo_domain": "FinancialBusinessAndCommerce", "fibo_module": "FBC"},
    "PRICE":             {"fibo_class": "fibo-fbc-fi-ip:InstrumentPrice",      "fibo_domain": "Securities",          "fibo_module": "SEC"},
    "CURRENCY":          {"fibo_class": "fibo-fnd-acc-cur:Currency",           "fibo_domain": "Foundations",         "fibo_module": "FND"},
    "ADDRESS":           {"fibo_class": "fibo-fnd-plc-adr:Address",            "fibo_domain": "Foundations",         "fibo_module": "FND"},
    "IDENTIFIER":        {"fibo_class": "fibo-fnd-rel-rel:hasIdentifier",      "fibo_domain": "Foundations",         "fibo_module": "FND"},
    "DATE":              {"fibo_class": "fibo-fnd-dt-fd:Date",                 "fibo_domain": "Foundations",         "fibo_module": "FND"},
    "AMOUNT":            {"fibo_class": "fibo-fnd-acc-cur:MonetaryAmount",     "fibo_domain": "Foundations",         "fibo_module": "FND"},
}

# ── BIAN Service Domain mapping ───────────────────────────────────────────────
# Maps entity name patterns → BIAN Service Domains (v13, bian.org)
# BIAN organises 326 service domains across Business Areas → Business Domains → Service Domains
BIAN_SERVICE_DOMAIN_MAP = {
    "CUSTOMER":    {"service_domain": "Customer Relationship Management", "business_area": "Sales & Service",         "bian_version": "v13"},
    "ACCOUNT":     {"service_domain": "Current Account",                  "business_area": "Operations & Execution",   "bian_version": "v13"},
    "PRODUCT":     {"service_domain": "Product Directory",                "business_area": "Products",                 "bian_version": "v13"},
    "ORDER":       {"service_domain": "Sales Order",                      "business_area": "Sales & Service",          "bian_version": "v13"},
    "PAYMENT":     {"service_domain": "Payment Execution",                "business_area": "Operations & Execution",   "bian_version": "v13"},
    "LOAN":        {"service_domain": "Consumer Loan",                    "business_area": "Products",                 "bian_version": "v13"},
    "TRANSACTION": {"service_domain": "Financial Accounting",             "business_area": "Business Support",         "bian_version": "v13"},
    "PARTY":       {"service_domain": "Party Reference Data Management",  "business_area": "Business Support",         "bian_version": "v13"},
    "RISK":        {"service_domain": "Credit Risk Operations",           "business_area": "Risk & Compliance",        "bian_version": "v13"},
    "CONTRACT":    {"service_domain": "Customer Agreement",               "business_area": "Sales & Service",          "bian_version": "v13"},
    "INSTRUMENT":  {"service_domain": "Investment Portfolio Management",  "business_area": "Products",                 "bian_version": "v13"},
    "SECURITY":    {"service_domain": "Securities Position Keeping",      "business_area": "Operations & Execution",   "bian_version": "v13"},
    "PRICE":       {"service_domain": "Market Data Switch Operation",     "business_area": "Operations & Execution",   "bian_version": "v13"},
    "EMPLOYEE":    {"service_domain": "Human Resources",                  "business_area": "Business Support",         "bian_version": "v13"},
}


def match_fibo(entity_name: str) -> Optional[dict]:
    """Match an entity name to a FIBO concept using keyword lookup."""
    name_upper = entity_name.upper()
    for keyword, concept in FIBO_CONCEPT_MAP.items():
        if keyword in name_upper:
            return {**concept, "matched_keyword": keyword}
    return None


def match_bian(entity_name: str) -> Optional[dict]:
    """Match an entity name to a BIAN Service Domain using keyword lookup."""
    name_upper = entity_name.upper()
    for keyword, domain in BIAN_SERVICE_DOMAIN_MAP.items():
        if keyword in name_upper:
            return {**domain, "matched_keyword": keyword}
    return None


def sanitize_mermaid_id(name: str) -> str:
    """Convert table/entity names to valid Mermaid identifiers."""
    return re.sub(r"[^A-Za-z0-9_]", "_", name).strip("_")


def dtype_to_mermaid(dtype: str) -> str:
    """Normalize data types to Mermaid-compatible type labels."""
    dtype = dtype.upper()
    if any(t in dtype for t in ["VARCHAR", "CHAR", "TEXT", "STRING", "NVARCHAR"]):
        return "string"
    elif any(t in dtype for t in ["NUMBER", "NUMERIC", "DECIMAL", "FLOAT", "DOUBLE"]):
        return "float"
    elif any(t in dtype for t in ["INT", "INTEGER", "BIGINT", "SMALLINT"]):
        return "int"
    elif any(t in dtype for t in ["DATE"]):
        return "date"
    elif any(t in dtype for t in ["TIMESTAMP", "DATETIME", "TIMESTAMP_NTZ"]):
        return "datetime"
    elif "BOOL" in dtype or "BIT" in dtype:
        return "boolean"
    else:
        return "string"


def build_mermaid_er(mappings: list, dv_model: dict) -> dict:
    """
    Build Mermaid ER diagrams from:
      1. Source layer  — original source tables from mappings
      2. Staging layer — STG tables
      3. Data Vault    — Hubs, Links, Satellites (raw vault)

    Returns dict of {diagram_name: mermaid_string}
    """
    diagrams = {}

    # ── 1. Source ER diagram ──────────────────────────────────────────────────
    src_lines = ["erDiagram"]
    src_entities = {}

    for m in mappings:
        src_tbl = sanitize_mermaid_id(m["source_table"].split(".")[-1])
        if src_tbl not in src_entities:
            src_entities[src_tbl] = []
        for col in m["columns"]:
            pk_marker = "PK" if col["pk"] else ("FK" if col["tgt"].endswith(("_ID","_KEY")) else "")
            dtype     = dtype_to_mermaid(col["dtype"])
            src_entities[src_tbl].append(
                f'        {dtype} {col["src"]} {pk_marker}'
            )

    for tbl, cols in src_entities.items():
        src_lines.append(f"    {tbl} {{")
        src_lines.extend(cols)
        src_lines.append("    }")

    # Relationships between source tables (infer from FK columns)
    seen_rels = set()
    for m in mappings:
        src_tbl = sanitize_mermaid_id(m["source_table"].split(".")[-1])
        for col in m["columns"]:
            if col["tgt"].endswith("_ID") and not col["pk"]:
                ref_entity = col["tgt"].replace("_ID", "").upper()
                for other_m in mappings:
                    other_entity = sanitize_mermaid_id(other_m["source_table"].split(".")[-1])
                    if ref_entity in other_entity.upper():
                        rel_key = f"{other_entity}||--o{{{src_tbl}"
                        if rel_key not in seen_rels:
                            src_lines.append(
                                f'    {other_entity} ||--o{{ {src_tbl} : "has"'
                            )
                            seen_rels.add(rel_key)

    diagrams["source_er"] = "\n".join(src_lines)

    # ── 2. Staging ER diagram ─────────────────────────────────────────────────
    stg_lines = ["erDiagram"]
    for stg in dv_model.get("staging", []):
        tbl  = sanitize_mermaid_id(stg["table_name"])
        stg_lines.append(f"    {tbl} {{")
        bk_set = set(stg["bk_columns"])
        for col in stg["all_columns"]:
            pk_marker = "PK" if col["tgt"] in bk_set else ""
            dtype     = dtype_to_mermaid(col["dtype"])
            stg_lines.append(f'        {dtype} {col["tgt"]} {pk_marker}')
        stg_lines.append("    }")

    diagrams["staging_er"] = "\n".join(stg_lines)

    # ── 3. Data Vault ER diagram ──────────────────────────────────────────────
    dv_lines = ["erDiagram"]

    for hub in dv_model.get("hubs", []):
        tbl = sanitize_mermaid_id(hub["table_name"])
        dv_lines.append(f"    {tbl} {{")
        dv_lines.append(f'        string  {hub["hash_key"]}   PK')
        dv_lines.append(f'        string  {hub["business_key"]}')
        dv_lines.append(f'        datetime LOAD_DATE')
        dv_lines.append(f'        string  RECORD_SOURCE')
        dv_lines.append("    }")

    for lnk in dv_model.get("links", []):
        tbl = sanitize_mermaid_id(lnk["table_name"])
        dv_lines.append(f"    {tbl} {{")
        dv_lines.append(f'        string  {lnk["hash_key"]}   PK')
        for fk in lnk["fk_hash_keys"]:
            dv_lines.append(f'        string  {fk}   FK')
        dv_lines.append(f'        datetime LOAD_DATE')
        dv_lines.append(f'        string  RECORD_SOURCE')
        dv_lines.append("    }")

    for sat in dv_model.get("satellites", []):
        tbl = sanitize_mermaid_id(sat["table_name"])
        dv_lines.append(f"    {tbl} {{")
        dv_lines.append(f'        string  {sat["hash_key"]}   FK')
        dv_lines.append(f'        string  {sat["hash_diff"]}')
        for col in sat["descriptive_cols"]:
            dv_lines.append(f'        string  {col}')
        dv_lines.append(f'        datetime LOAD_DATE')
        dv_lines.append(f'        string  RECORD_SOURCE')
        dv_lines.append("    }")

    # Hub → Satellite relationships
    for sat in dv_model.get("satellites", []):
        hub_tbl = sanitize_mermaid_id(sat["parent_hub"])
        sat_tbl = sanitize_mermaid_id(sat["table_name"])
        dv_lines.append(f'    {hub_tbl} ||--o{{ {sat_tbl} : "describes"')

    # Hub → Link relationships
    for lnk in dv_model.get("links", []):
        lnk_tbl = sanitize_mermaid_id(lnk["table_name"])
        for entity in lnk["entities"]:
            hub_tbl = sanitize_mermaid_id(f"HUB_{entity}")
            dv_lines.append(f'    {hub_tbl} ||--o{{ {lnk_tbl} : "links"')

    diagrams["data_vault_er"] = "\n".join(dv_lines)

    return diagrams


def build_fibo_bian_alignment(mappings: list, dv_model: dict) -> dict:
    """
    Build FIBO + BIAN alignment report for each entity in the DV model.
    Returns structured alignment dict with Mermaid class diagram.
    """
    alignment = []
    class_lines = ["classDiagram"]

    for hub in dv_model.get("hubs", []):
        entity    = hub["entity"]
        fibo_ref  = match_fibo(entity)
        bian_ref  = match_bian(entity)

        entry = {
            "entity":          entity,
            "dv_table":        hub["table_name"],
            "dv_layer":        "HUB",
            "fibo_class":      fibo_ref["fibo_class"]    if fibo_ref else "No FIBO match",
            "fibo_domain":     fibo_ref["fibo_domain"]   if fibo_ref else "—",
            "fibo_module":     fibo_ref["fibo_module"]   if fibo_ref else "—",
            "bian_service_domain": bian_ref["service_domain"] if bian_ref else "No BIAN match",
            "bian_business_area":  bian_ref["business_area"]  if bian_ref else "—",
            "bian_version":        bian_ref["bian_version"]   if bian_ref else "—",
        }
        alignment.append(entry)

        # Mermaid class diagram node
        hub_id   = sanitize_mermaid_id(hub["table_name"])
        fibo_lbl = fibo_ref["fibo_class"].split(":")[-1] if fibo_ref else "Unclassified"
        bian_lbl = bian_ref["service_domain"] if bian_ref else "Unclassified"

        class_lines.append(f"    class {hub_id} {{")
        class_lines.append(f'        +string {hub["hash_key"]} PK')
        class_lines.append(f'        +string {hub["business_key"]}')
        class_lines.append(f'        <<FIBO: {fibo_lbl}>>')
        class_lines.append(f'        <<BIAN: {bian_lbl}>>')
        class_lines.append("    }")

    for lnk in dv_model.get("links", []):
        lnk_id = sanitize_mermaid_id(lnk["table_name"])
        class_lines.append(f"    class {lnk_id} {{")
        class_lines.append(f'        +string {lnk["hash_key"]} PK')
        for fk in lnk["fk_hash_keys"]:
            class_lines.append(f'        +string {fk} FK')
        class_lines.append(f'        <<BIAN: Association>>')
        class_lines.append("    }")

        for entity in lnk["entities"]:
            hub_id = sanitize_mermaid_id(f"HUB_{entity}")
            class_lines.append(f"    {hub_id} --> {lnk_id} : participates")

    for sat in dv_model.get("satellites", []):
        sat_id  = sanitize_mermaid_id(sat["table_name"])
        hub_id  = sanitize_mermaid_id(sat["parent_hub"])
        entity  = sat["parent_hub"].replace("HUB_", "")
        fibo_ref= match_fibo(entity)

        class_lines.append(f"    class {sat_id} {{")
        class_lines.append(f'        +string {sat["hash_key"]} FK')
        class_lines.append(f'        +string {sat["hash_diff"]}')
        fibo_lbl = fibo_ref["fibo_class"].split(":")[-1] if fibo_ref else "Attribute"
        class_lines.append(f'        <<FIBO: {fibo_lbl}Attributes>>')
        class_lines.append("    }")
        class_lines.append(f"    {hub_id} <|-- {sat_id} : describes")

    return {
        "alignment":            alignment,
        "mermaid_class_diagram": "\n".join(class_lines),
    }


def run_agent8_mermaid_er(
    mappings: list,
    dv_model: dict,
    output_dir: Path,
) -> dict:
    """
    Agent 8 orchestrator: generates all Mermaid ER + class diagrams
    with FIBO ontology and BIAN service domain alignment.

    Outputs:
      - mermaid/source_er.mmd
      - mermaid/staging_er.mmd
      - mermaid/data_vault_er.mmd
      - mermaid/fibo_bian_class.mmd
      - mermaid/fibo_bian_alignment.json
      - mermaid/README_diagrams.md   (embeds all diagrams as Mermaid blocks)
    """
    mermaid_dir = output_dir / "mermaid"
    mermaid_dir.mkdir(parents=True, exist_ok=True)

    print(f"[Agent8] Generating Mermaid ER diagrams...")

    # ── ER diagrams ────────────────────────────────────────────────────────────
    er_diagrams = build_mermaid_er(mappings, dv_model)
    for name, content in er_diagrams.items():
        path = mermaid_dir / f"{name}.mmd"
        path.write_text(content)
        print(f"[Agent8]   ER diagram → {path.name}")

    # ── FIBO + BIAN alignment ─────────────────────────────────────────────────
    alignment_result = build_fibo_bian_alignment(mappings, dv_model)

    class_path = mermaid_dir / "fibo_bian_class.mmd"
    class_path.write_text(alignment_result["mermaid_class_diagram"])
    print(f"[Agent8]   Class diagram → {class_path.name}")

    alignment_path = mermaid_dir / "fibo_bian_alignment.json"
    alignment_path.write_text(json.dumps(alignment_result["alignment"], indent=2))
    print(f"[Agent8]   Alignment JSON → {alignment_path.name}")

    # ── Markdown README with embedded diagrams ─────────────────────────────────
    md_lines = [
        "# Data Model Diagrams",
        f"**Author:** gokulram.krishnan  ",
        f"**Generated:** {datetime.utcnow().isoformat()} UTC  ",
        f"**Repository:** https://github.com/gokulramkrishnan/ETL-Pipeline-Code",
        "",
        "---",
        "",
        "## References",
        "- **FIBO** — [Financial Industry Business Ontology](https://spec.edmcouncil.org/fibo/) (EDM Council / OMG)",
        "  - 2,436 classes: Foundations, Business Entities, Finance, Securities, Derivatives, Instruments",
        "  - [FIB-DM](https://fib-dm.com) — enterprise data model derived from FIBO",
        "- **BIAN** — [Banking Industry Architecture Network](https://bian.org) v13",
        "  - 326 Service Domains | 250+ APIs | not-for-profit, est. 2008",
        "",
        "---",
        "",
        "## 1. Source ER Diagram",
        "```mermaid",
        er_diagrams["source_er"],
        "```",
        "",
        "---",
        "",
        "## 2. Staging ER Diagram",
        "```mermaid",
        er_diagrams["staging_er"],
        "```",
        "",
        "---",
        "",
        "## 3. Data Vault 2.0 ER Diagram",
        "> Hubs, Links, Satellites following datavault4dbt v1.17.0 conventions",
        "```mermaid",
        er_diagrams["data_vault_er"],
        "```",
        "",
        "---",
        "",
        "## 4. FIBO + BIAN Alignment Class Diagram",
        "> Each Hub is annotated with its FIBO ontology class and BIAN Service Domain",
        "```mermaid",
        alignment_result["mermaid_class_diagram"],
        "```",
        "",
        "---",
        "",
        "## 5. FIBO + BIAN Alignment Table",
        "",
        "| Entity | DV Table | FIBO Class | FIBO Domain | BIAN Service Domain | BIAN Business Area |",
        "|--------|----------|-----------|-------------|--------------------|--------------------|",
    ]

    for row in alignment_result["alignment"]:
        md_lines.append(
            f"| {row['entity']} | {row['dv_table']} | `{row['fibo_class']}` | "
            f"{row['fibo_domain']} | {row['bian_service_domain']} | {row['bian_business_area']} |"
        )

    md_lines += [
        "",
        "---",
        "",
        "## 6. How to render these diagrams",
        "",
        "**GitHub** — paste any `.mmd` file content into a markdown code block with ` ```mermaid ` — GitHub renders it natively.",
        "",
        "**VSCode** — install the [Mermaid Preview](https://marketplace.visualstudio.com/items?itemName=bierner.markdown-mermaid) extension.",
        "",
        "**CLI** — use [mermaid-js/mermaid-cli](https://github.com/mermaid-js/mermaid-cli):",
        "```bash",
        "npm install -g @mermaid-js/mermaid-cli",
        "mmdc -i data_vault_er.mmd -o data_vault_er.png",
        "```",
        "",
        "**Online** — paste into [mermaid.live](https://mermaid.live)",
    ]

    readme_path = mermaid_dir / "README_diagrams.md"
    readme_path.write_text("\n".join(md_lines))
    print(f"[Agent8]   Diagram README → {readme_path.name}")

    print(f"[Agent8] ✅ All diagrams saved to {mermaid_dir}")

    return {
        "er_diagrams":       er_diagrams,
        "alignment":         alignment_result["alignment"],
        "class_diagram":     alignment_result["mermaid_class_diagram"],
        "output_dir":        str(mermaid_dir),
    }


# ── Standalone execution ───────────────────────────────────────────────────────
if __name__ == "__main__":
    import sys
    sys.path.insert(0, str(Path(__file__).parent.parent))

    # Load model from pipeline output
    dv_path  = Path("output/data_vault/dv_model.json")
    stm_path = Path("output/sttm/sttm.json")

    if not dv_path.exists() or not stm_path.exists():
        print("Run main_pipeline.ipynb first to generate dv_model.json and sttm.json")
        sys.exit(1)

    dv_model = json.loads(dv_path.read_text())
    sttm     = json.loads(stm_path.read_text())

    # Reconstruct minimal mappings from STTM JSON
    mapping_groups = {}
    for row in sttm:
        name = row["Mapping Name"]
        if name not in mapping_groups:
            mapping_groups[name] = {
                "mapping_name":  name,
                "tool":          row.get("ETL Tool", ""),
                "source_table":  row.get("Source Table", ""),
                "target_table":  row.get("Target Table", ""),
                "load_strategy": row.get("Load Strategy", ""),
                "columns":       [],
            }
        mapping_groups[name]["columns"].append({
            "src":            row.get("Source Column", ""),
            "tgt":            row.get("Target Column", ""),
            "dtype":          row.get("Data Type", "VARCHAR"),
            "pk":             row.get("Primary Key", "No") == "Yes",
            "nullable":       row.get("Nullable", "Yes") == "Yes",
            "transformation": row.get("Transformation", "DIRECT"),
        })

    result = run_agent8_mermaid_er(
        mappings  = list(mapping_groups.values()),
        dv_model  = dv_model,
        output_dir= Path("output"),
    )
    print(json.dumps({"status": "complete", "output_dir": result["output_dir"]}, indent=2))
