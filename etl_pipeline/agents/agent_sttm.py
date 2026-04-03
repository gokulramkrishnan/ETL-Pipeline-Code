# =============================================================================
# Author      : gokulram.krishnan
# Repository  : https://github.com/gokulramkrishnan/Useful-Jupyter-Notebook
# Description : """
# =============================================================================
"""
Agent 2: STTM Generator
Produces a Source-to-Target Mapping document (Excel + JSON)
from the unified metadata model.
"""

import json
import pandas as pd
from pathlib import Path
from datetime import datetime
from typing import Optional


# ─────────────────────────────────────────────────────────────────────────────
# Standalone functions
# ─────────────────────────────────────────────────────────────────────────────

def build_sttm(metadata: dict, target_database: str, target_schema: str) -> dict:
    """
    Build a structured STTM from unified metadata.
    Returns a dict with 'mappings' list and 'summary' stats.
    """
    mappings = []
    seen = set()

    # ── Column-level mappings ─────────────────────────────────────────────────
    for m in metadata.get("mappings", []):
        key = (
            m.get("source_table", m.get("source_stage", "")),
            m.get("source_column", m.get("from_field", "")),
            m.get("target_table", m.get("to_instance", "")),
            m.get("target_column", m.get("to_field", m.get("target_column", ""))),
        )
        if key in seen:
            continue
        seen.add(key)

        # Resolve expression from transformations if not directly on mapping
        expression = m.get("expression", "")
        if not expression:
            expression = _lookup_expression(
                m.get("source_column", ""), metadata.get("transformations", [])
            )

        mappings.append({
            "mapping_id":         f"MAP_{len(mappings)+1:04d}",
            "tool":               m.get("_tool", ""),
            "source_schema":      _infer_source_schema(m, metadata),
            "source_table":       m.get("source_table", m.get("source_stage", m.get("from_instance", ""))),
            "source_column":      m.get("source_column", m.get("from_field", "")),
            "source_data_type":   _lookup_source_dtype(m, metadata),
            "target_database":    target_database,
            "target_schema":      target_schema,
            "target_table":       m.get("target_table", m.get("to_instance", "")),
            "target_column":      m.get("target_column", m.get("to_field", "")),
            "target_data_type":   _lookup_target_dtype(m, metadata),
            "transformation_type":_classify_expression(expression),
            "expression":         expression,
            "nullable":           _lookup_nullable(m, metadata),
            "is_pk":              _infer_pk(m, metadata),
            "is_fk":              _infer_fk(m, metadata),
            "pii_flag":           _flag_pii(m.get("source_column", "")),
            "notes":              "",
        })

    # ── Add any source columns not yet in mappings (unmapped columns) ─────────
    for src in metadata.get("sources", []):
        for col in src.get("columns", []):
            key = (src.get("table_name",""), col.get("name",""), "", "")
            if key not in seen:
                mappings.append({
                    "mapping_id":         f"MAP_{len(mappings)+1:04d}",
                    "tool":               src.get("_tool",""),
                    "source_schema":      src.get("schema",""),
                    "source_table":       src.get("table_name",""),
                    "source_column":      col.get("name",""),
                    "source_data_type":   col.get("data_type",""),
                    "target_database":    "",
                    "target_schema":      "",
                    "target_table":       "*** UNMAPPED ***",
                    "target_column":      "",
                    "target_data_type":   "",
                    "transformation_type":"NONE",
                    "expression":         "",
                    "nullable":           col.get("nullable",""),
                    "is_pk":              "Y" if "PRIMARY" in col.get("key_type","").upper() else "N",
                    "is_fk":              "Y" if "FOREIGN" in col.get("key_type","").upper() else "N",
                    "pii_flag":           _flag_pii(col.get("name","")),
                    "notes":              "UNMAPPED — review required",
                })

    summary = {
        "total_mappings":   len(mappings),
        "mapped":           len([m for m in mappings if m["target_table"] != "*** UNMAPPED ***"]),
        "unmapped":         len([m for m in mappings if m["target_table"] == "*** UNMAPPED ***"]),
        "pii_columns":      len([m for m in mappings if m["pii_flag"] == "Y"]),
        "transformations":  len([m for m in mappings if m["transformation_type"] not in ("DIRECT","NONE","")]),
        "source_tables":    len({m["source_table"] for m in mappings}),
        "target_tables":    len({m["target_table"] for m in mappings if m["target_table"] != "*** UNMAPPED ***"}),
    }

    return {"mappings": mappings, "summary": summary}


def write_sttm_excel(sttm: dict, output_path: str):
    """Write STTM to Excel with multiple formatted sheets."""
    df_all     = pd.DataFrame(sttm["mappings"])
    df_mapped  = df_all[df_all["target_table"] != "*** UNMAPPED ***"]
    df_unmap   = df_all[df_all["target_table"] == "*** UNMAPPED ***"]
    df_pii     = df_all[df_all["pii_flag"] == "Y"]
    df_summary = pd.DataFrame([sttm["summary"]])

    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        df_all.to_excel(writer,     sheet_name="All Mappings",      index=False)
        df_mapped.to_excel(writer,  sheet_name="Mapped Columns",    index=False)
        df_unmap.to_excel(writer,   sheet_name="Unmapped Columns",  index=False)
        df_pii.to_excel(writer,     sheet_name="PII Columns",       index=False)
        df_summary.to_excel(writer, sheet_name="Summary",           index=False)

        # Auto-width columns
        for sheet_name, df in [("All Mappings", df_all), ("Summary", df_summary)]:
            ws = writer.sheets[sheet_name]
            for col_idx, col in enumerate(df.columns, 1):
                max_len = max(len(str(col)), df[col].astype(str).str.len().max() if len(df) > 0 else 10)
                ws.column_dimensions[ws.cell(1, col_idx).column_letter].width = min(max_len + 2, 50)


# ─────────────────────────────────────────────────────────────────────────────
# Helper functions
# ─────────────────────────────────────────────────────────────────────────────

PII_PATTERNS = ["email","ssn","phone","credit","card","dob","birth","address",
                "passport","license","national_id","tax_id","name","ip_address"]

def _flag_pii(col_name: str) -> str:
    col_lower = col_name.lower()
    return "Y" if any(p in col_lower for p in PII_PATTERNS) else "N"

def _classify_expression(expr: str) -> str:
    if not expr:
        return "DIRECT"
    expr_up = expr.upper()
    if any(f in expr_up for f in ["MD5","SHA","HASH"]):     return "HASH"
    if any(f in expr_up for f in ["SUM","AVG","COUNT","MAX","MIN"]): return "AGGREGATE"
    if any(f in expr_up for f in ["CASE","IFF","DECODE","NVL","COALESCE"]): return "CONDITIONAL"
    if any(f in expr_up for f in ["TRIM","UPPER","LOWER","CONCAT","||","SUBSTR"]): return "STRING_TRANSFORM"
    if any(f in expr_up for f in ["CAST","TO_DATE","TO_TIMESTAMP","TO_NUMBER"]): return "TYPE_CAST"
    if any(f in expr_up for f in ["JOIN","LOOKUP"]): return "LOOKUP"
    return "EXPRESSION"

def _lookup_expression(col_name: str, transformations: list) -> str:
    for t in transformations:
        for expr in t.get("expressions", []):
            if expr.get("field","").upper() == col_name.upper():
                return expr.get("expression","")
    return ""

def _infer_source_schema(m: dict, metadata: dict) -> str:
    src_table = m.get("source_table", m.get("source_stage",""))
    for s in metadata.get("sources", []):
        if s.get("table_name","") == src_table:
            return s.get("schema","")
    return ""

def _lookup_source_dtype(m: dict, metadata: dict) -> str:
    src_table = m.get("source_table","")
    src_col   = m.get("source_column","")
    for s in metadata.get("sources",[]):
        if s.get("table_name","") == src_table:
            for c in s.get("columns",[]):
                if c.get("name","") == src_col:
                    return c.get("data_type","")
    return ""

def _lookup_target_dtype(m: dict, metadata: dict) -> str:
    tgt_table = m.get("target_table","")
    tgt_col   = m.get("target_column","")
    for t in metadata.get("targets",[]):
        if t.get("table_name","") == tgt_table:
            for c in t.get("columns",[]):
                if c.get("name","") == tgt_col:
                    return c.get("data_type","")
    return ""

def _lookup_nullable(m: dict, metadata: dict) -> str:
    src_table = m.get("source_table","")
    src_col   = m.get("source_column","")
    for s in metadata.get("sources",[]):
        if s.get("table_name","") == src_table:
            for c in s.get("columns",[]):
                if c.get("name","") == src_col:
                    return c.get("nullable","")
    return ""

def _infer_pk(m: dict, metadata: dict) -> str:
    src_table = m.get("source_table","")
    src_col   = m.get("source_column","")
    for s in metadata.get("sources",[]):
        if s.get("table_name","") == src_table:
            for c in s.get("columns",[]):
                if c.get("name","") == src_col:
                    return "Y" if "PRIMARY" in c.get("key_type","").upper() else "N"
    return "N"

def _infer_fk(m: dict, metadata: dict) -> str:
    src_table = m.get("source_table","")
    src_col   = m.get("source_column","")
    for s in metadata.get("sources",[]):
        if s.get("table_name","") == src_table:
            for c in s.get("columns",[]):
                if c.get("name","") == src_col:
                    return "Y" if "FOREIGN" in c.get("key_type","").upper() else "N"
    return "N"


# ─────────────────────────────────────────────────────────────────────────────
# Agent class
# ─────────────────────────────────────────────────────────────────────────────

class STTMAgent:
    def run(self, metadata: dict, output_dir: str,
            target_database: str, target_schema: str) -> dict:

        print("  Building STTM...")
        sttm = build_sttm(metadata, target_database, target_schema)

        # Save JSON
        json_path = f"{output_dir}/sttm.json"
        with open(json_path, "w") as f:
            json.dump(sttm, f, indent=2)

        # Save Excel
        xlsx_path = f"{output_dir}/sttm.xlsx"
        write_sttm_excel(sttm, xlsx_path)

        s = sttm["summary"]
        print(f"    Total mappings : {s['total_mappings']}")
        print(f"    Mapped         : {s['mapped']}")
        print(f"    Unmapped       : {s['unmapped']}  ← review required")
        print(f"    PII columns    : {s['pii_columns']}")
        print(f"    Transformations: {s['transformations']}")

        return sttm
