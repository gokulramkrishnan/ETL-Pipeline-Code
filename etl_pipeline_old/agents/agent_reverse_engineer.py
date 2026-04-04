# =============================================================================
# Author      : gokulram.krishnan
# Repository  : https://github.com/gokulramkrishnan/Useful-Jupyter-Notebook
# Description : """
# =============================================================================
"""
Agent 1: Reverse Engineer
Parses DataStage .dsx/.xml and Informatica .xml/.ipc exports
into a unified metadata model consumed by all downstream agents.
"""

import xml.etree.ElementTree as ET
import json
import re
from pathlib import Path
from typing import Optional
from datetime import datetime


# ─────────────────────────────────────────────────────────────────────────────
# Standalone parser functions
# ─────────────────────────────────────────────────────────────────────────────

def parse_datastage(file_path: str) -> dict:
    """
    Parse a DataStage .dsx or .xml export.
    Extracts: job name, stages, links, source/target schemas, transformations.
    """
    tree = ET.parse(file_path)
    root = tree.getroot()

    sources, targets, mappings, transformations = [], [], [], []

    # DataStage DSX export wraps jobs in <Job> or <DSExportedObject>
    jobs = (
        root.findall(".//Job") or
        root.findall(".//DSJob") or
        root.findall(".//DSExportedObject")
    )

    if not jobs:
        # Fallback: treat root as job
        jobs = [root]

    for job in jobs:
        job_name = (
            job.get("Identifier") or
            job.get("name") or
            job.get("Name") or
            "UNKNOWN_JOB"
        )

        # ── Stages ────────────────────────────────────────────────────────────
        stages = (
            job.findall(".//Stage") or
            job.findall(".//DSStage") or
            job.findall(".//stage")
        )

        for stage in stages:
            stage_type = (
                stage.get("StageType") or
                stage.get("type") or
                stage.get("Type") or ""
            ).lower()
            stage_name = (
                stage.get("Identifier") or
                stage.get("name") or
                stage.get("Name") or "UNKNOWN_STAGE"
            )

            columns = _extract_datastage_columns(stage)

            record = {
                "job":        job_name,
                "stage_name": stage_name,
                "stage_type": stage_type,
                "columns":    columns,
            }

            if any(k in stage_type for k in ["source", "seq", "odbc", "db2", "oracle", "jdbc"]):
                table_name = _get_attr(stage, ["TableName", "table_name", "SourceTable"]) or stage_name
                sources.append({**record, "table_name": table_name})
            elif any(k in stage_type for k in ["target", "sink", "write"]):
                table_name = _get_attr(stage, ["TableName", "table_name", "TargetTable"]) or stage_name
                targets.append({**record, "table_name": table_name})
            elif any(k in stage_type for k in ["transform", "filter", "aggregat", "join", "lookup", "sort"]):
                transform_expr = _extract_datastage_expressions(stage)
                transformations.append({**record, "expressions": transform_expr})

        # ── Links (column-level lineage) ──────────────────────────────────────
        links = job.findall(".//Link") or job.findall(".//DSLink")
        for link in links:
            src_stage  = link.get("SourceStage")  or link.get("source_stage")  or ""
            tgt_stage  = link.get("TargetStage")  or link.get("target_stage")  or ""
            link_cols  = _extract_datastage_columns(link)
            for col in link_cols:
                mappings.append({
                    "job":           job_name,
                    "source_stage":  src_stage,
                    "target_stage":  tgt_stage,
                    "source_column": col.get("name", ""),
                    "target_column": col.get("name", ""),
                    "data_type":     col.get("data_type", ""),
                    "expression":    col.get("expression", ""),
                })

    return {
        "tool":            "datastage",
        "file":            file_path,
        "parsed_at":       datetime.utcnow().isoformat(),
        "sources":         sources,
        "targets":         targets,
        "mappings":        mappings,
        "transformations": transformations,
    }


def parse_informatica(file_path: str) -> dict:
    """
    Parse an Informatica PowerCenter .xml export.
    Extracts: mappings, sources, targets, transformations, column-level lineage.
    """
    tree = ET.parse(file_path)
    root = tree.getroot()

    sources, targets, mappings, transformations = [], [], [], []

    # Informatica XML structure: POWERMART > REPOSITORY > FOLDER > MAPPING
    for mapping in root.iter("MAPPING"):
        mapping_name = mapping.get("NAME", "UNKNOWN_MAPPING")

        # ── Source definitions ─────────────────────────────────────────────────
        for src in mapping.findall(".//SOURCE") + root.findall(".//SOURCE"):
            table = src.get("NAME") or src.get("DBDNAME") or "UNKNOWN_SOURCE"
            schema = src.get("DBDNAME") or src.get("SCHEMANAME") or ""
            columns = [
                {
                    "name":      c.get("NAME", ""),
                    "data_type": c.get("DATATYPE", ""),
                    "nullable":  c.get("NULLABLE", ""),
                    "key_type":  c.get("KEYTYPE", ""),
                }
                for c in src.findall("SOURCEFIELD")
            ]
            sources.append({
                "mapping":    mapping_name,
                "table_name": table,
                "schema":     schema,
                "columns":    columns,
            })

        # ── Target definitions ─────────────────────────────────────────────────
        for tgt in mapping.findall(".//TARGET") + root.findall(".//TARGET"):
            table = tgt.get("NAME") or tgt.get("DBDNAME") or "UNKNOWN_TARGET"
            schema = tgt.get("DBDNAME") or tgt.get("SCHEMANAME") or ""
            columns = [
                {
                    "name":      c.get("NAME", ""),
                    "data_type": c.get("DATATYPE", ""),
                    "nullable":  c.get("NULLABLE", ""),
                    "key_type":  c.get("KEYTYPE", ""),
                }
                for c in tgt.findall("TARGETFIELD")
            ]
            targets.append({
                "mapping":    mapping_name,
                "table_name": table,
                "schema":     schema,
                "columns":    columns,
            })

        # ── Transformations ────────────────────────────────────────────────────
        for trans in mapping.findall(".//TRANSFORMATION"):
            trans_name = trans.get("NAME", "")
            trans_type = trans.get("TYPE", "").lower()
            exprs = []
            for field in trans.findall(".//TRANSFORMFIELD"):
                expr = field.get("EXPRESSION") or field.get("DEFAULTVALUE") or ""
                if expr:
                    exprs.append({
                        "field":      field.get("NAME", ""),
                        "expression": expr,
                        "data_type":  field.get("DATATYPE", ""),
                    })
            if exprs:
                transformations.append({
                    "mapping":     mapping_name,
                    "name":        trans_name,
                    "type":        trans_type,
                    "expressions": exprs,
                })

        # ── Connector (column-level lineage) ──────────────────────────────────
        for conn in mapping.findall(".//CONNECTOR"):
            mappings.append({
                "mapping":       mapping_name,
                "from_instance": conn.get("FROMINSTANCE", ""),
                "from_field":    conn.get("FROMFIELD", ""),
                "to_instance":   conn.get("TOINSTANCE", ""),
                "to_field":      conn.get("TOFIELD", ""),
                "source_column": conn.get("FROMFIELD", ""),
                "target_column": conn.get("TOFIELD", ""),
                "expression":    "",
            })

    return {
        "tool":            "informatica",
        "file":            file_path,
        "parsed_at":       datetime.utcnow().isoformat(),
        "sources":         sources,
        "targets":         targets,
        "mappings":        mappings,
        "transformations": transformations,
    }


def merge_metadata(ds_meta: Optional[dict], inf_meta: Optional[dict]) -> dict:
    """
    Merge DataStage and Informatica metadata into a single unified model.
    Deduplicates tables by name across tools.
    """
    merged = {
        "sources":         [],
        "targets":         [],
        "mappings":        [],
        "transformations": [],
        "provenance":      [],
    }

    seen_sources = set()
    seen_targets = set()

    for meta in [ds_meta, inf_meta]:
        if not meta:
            continue

        tool = meta.get("tool", "unknown")
        merged["provenance"].append({"tool": tool, "file": meta.get("file"), "parsed_at": meta.get("parsed_at")})

        for s in meta.get("sources", []):
            key = (s.get("table_name", ""), tool)
            if key not in seen_sources:
                s["_tool"] = tool
                merged["sources"].append(s)
                seen_sources.add(key)

        for t in meta.get("targets", []):
            key = (t.get("table_name", ""), tool)
            if key not in seen_targets:
                t["_tool"] = tool
                merged["targets"].append(t)
                seen_targets.add(key)

        for m in meta.get("mappings", []):
            m["_tool"] = tool
            merged["mappings"].append(m)

        for tr in meta.get("transformations", []):
            tr["_tool"] = tool
            merged["transformations"].append(tr)

    return merged


# ─────────────────────────────────────────────────────────────────────────────
# Helper functions
# ─────────────────────────────────────────────────────────────────────────────

def _get_attr(element, attr_names: list, default="") -> str:
    for name in attr_names:
        val = element.get(name)
        if val:
            return val
    return default


def _extract_datastage_columns(element) -> list:
    columns = []
    for col in (element.findall(".//Column") + element.findall(".//DSColumn") + element.findall(".//column")):
        columns.append({
            "name":       _get_attr(col, ["Identifier", "name", "Name"]),
            "data_type":  _get_attr(col, ["SqlType", "DataType", "data_type", "type"]),
            "length":     _get_attr(col, ["Length", "length"]),
            "nullable":   _get_attr(col, ["Nullable", "nullable"]),
            "expression": _get_attr(col, ["Derivation", "expression", "Expression"]),
        })
    return columns


def _extract_datastage_expressions(stage) -> list:
    exprs = []
    for clause in (stage.findall(".//Clause") + stage.findall(".//Expression")):
        val = clause.get("Value") or clause.get("value") or clause.text or ""
        if val:
            exprs.append({"expression": val.strip()})
    return exprs


# ─────────────────────────────────────────────────────────────────────────────
# Agent class
# ─────────────────────────────────────────────────────────────────────────────

class ReverseEngineerAgent:
    """
    Agentic wrapper that orchestrates parsing of DataStage and/or Informatica
    files and returns a unified metadata dictionary.
    """

    def run(
        self,
        datastage_path:   Optional[str] = None,
        informatica_path: Optional[str] = None,
    ) -> dict:
        ds_meta  = None
        inf_meta = None

        if datastage_path and Path(datastage_path).exists():
            print(f"  Parsing DataStage  : {datastage_path}")
            try:
                ds_meta = parse_datastage(datastage_path)
                print(f"    ✅ {len(ds_meta['sources'])} sources, {len(ds_meta['targets'])} targets")
            except Exception as e:
                print(f"    ⚠️  DataStage parse error: {e}")
                ds_meta = self._sample_metadata("datastage")
        else:
            print("  ℹ️  No DataStage file — using sample metadata")
            ds_meta = self._sample_metadata("datastage")

        if informatica_path and Path(informatica_path).exists():
            print(f"  Parsing Informatica: {informatica_path}")
            try:
                inf_meta = parse_informatica(informatica_path)
                print(f"    ✅ {len(inf_meta['sources'])} sources, {len(inf_meta['targets'])} targets")
            except Exception as e:
                print(f"    ⚠️  Informatica parse error: {e}")
                inf_meta = self._sample_metadata("informatica")
        else:
            print("  ℹ️  No Informatica file — using sample metadata")
            inf_meta = self._sample_metadata("informatica")

        return merge_metadata(ds_meta, inf_meta)

    def _sample_metadata(self, tool: str) -> dict:
        """Return realistic sample metadata for demo/testing when no file is provided."""
        return {
            "tool":       tool,
            "file":       f"sample_{tool}.xml",
            "parsed_at":  datetime.utcnow().isoformat(),
            "sources": [
                {
                    "table_name": "SRC_CUSTOMER",
                    "schema": "OPERATIONAL",
                    "columns": [
                        {"name": "CUSTOMER_ID",   "data_type": "INTEGER",      "nullable": "N", "key_type": "PRIMARY KEY"},
                        {"name": "FIRST_NAME",     "data_type": "VARCHAR(100)", "nullable": "Y", "key_type": ""},
                        {"name": "LAST_NAME",      "data_type": "VARCHAR(100)", "nullable": "Y", "key_type": ""},
                        {"name": "EMAIL",           "data_type": "VARCHAR(255)", "nullable": "Y", "key_type": ""},
                        {"name": "PHONE",           "data_type": "VARCHAR(20)",  "nullable": "Y", "key_type": ""},
                        {"name": "COUNTRY_CODE",   "data_type": "CHAR(2)",      "nullable": "Y", "key_type": ""},
                        {"name": "CREATED_DATE",   "data_type": "DATE",         "nullable": "N", "key_type": ""},
                    ],
                },
                {
                    "table_name": "SRC_ORDER",
                    "schema": "OPERATIONAL",
                    "columns": [
                        {"name": "ORDER_ID",       "data_type": "INTEGER",      "nullable": "N", "key_type": "PRIMARY KEY"},
                        {"name": "CUSTOMER_ID",    "data_type": "INTEGER",      "nullable": "N", "key_type": "FOREIGN KEY"},
                        {"name": "PRODUCT_ID",     "data_type": "INTEGER",      "nullable": "N", "key_type": "FOREIGN KEY"},
                        {"name": "ORDER_DATE",     "data_type": "DATE",         "nullable": "N", "key_type": ""},
                        {"name": "ORDER_AMOUNT",   "data_type": "DECIMAL(18,2)","nullable": "Y", "key_type": ""},
                        {"name": "STATUS",         "data_type": "VARCHAR(50)",  "nullable": "Y", "key_type": ""},
                    ],
                },
                {
                    "table_name": "SRC_PRODUCT",
                    "schema": "OPERATIONAL",
                    "columns": [
                        {"name": "PRODUCT_ID",     "data_type": "INTEGER",      "nullable": "N", "key_type": "PRIMARY KEY"},
                        {"name": "PRODUCT_NAME",   "data_type": "VARCHAR(255)", "nullable": "N", "key_type": ""},
                        {"name": "CATEGORY",       "data_type": "VARCHAR(100)", "nullable": "Y", "key_type": ""},
                        {"name": "UNIT_PRICE",     "data_type": "DECIMAL(18,2)","nullable": "N", "key_type": ""},
                        {"name": "IS_ACTIVE",      "data_type": "BOOLEAN",      "nullable": "N", "key_type": ""},
                    ],
                },
            ],
            "targets": [
                {
                    "table_name": "TGT_DIM_CUSTOMER",
                    "schema": "DW",
                    "columns": [
                        {"name": "CUSTOMER_HK",   "data_type": "VARCHAR(32)",  "nullable": "N", "key_type": "PRIMARY KEY"},
                        {"name": "CUSTOMER_BK",   "data_type": "INTEGER",      "nullable": "N", "key_type": ""},
                        {"name": "FULL_NAME",      "data_type": "VARCHAR(200)", "nullable": "Y", "key_type": ""},
                        {"name": "EMAIL",          "data_type": "VARCHAR(255)", "nullable": "Y", "key_type": ""},
                        {"name": "COUNTRY_CODE",  "data_type": "CHAR(2)",      "nullable": "Y", "key_type": ""},
                        {"name": "LOAD_DATE",      "data_type": "TIMESTAMP",   "nullable": "N", "key_type": ""},
                        {"name": "RECORD_SOURCE",  "data_type": "VARCHAR(100)","nullable": "N", "key_type": ""},
                    ],
                },
                {
                    "table_name": "TGT_FACT_ORDER",
                    "schema": "DW",
                    "columns": [
                        {"name": "ORDER_HK",       "data_type": "VARCHAR(32)",  "nullable": "N", "key_type": "PRIMARY KEY"},
                        {"name": "CUSTOMER_HK",    "data_type": "VARCHAR(32)",  "nullable": "N", "key_type": "FOREIGN KEY"},
                        {"name": "PRODUCT_HK",     "data_type": "VARCHAR(32)",  "nullable": "N", "key_type": "FOREIGN KEY"},
                        {"name": "ORDER_DATE",     "data_type": "DATE",         "nullable": "N", "key_type": ""},
                        {"name": "ORDER_AMOUNT",   "data_type": "DECIMAL(18,2)","nullable": "Y", "key_type": ""},
                        {"name": "LOAD_DATE",      "data_type": "TIMESTAMP",   "nullable": "N", "key_type": ""},
                        {"name": "RECORD_SOURCE",  "data_type": "VARCHAR(100)","nullable": "N", "key_type": ""},
                    ],
                },
            ],
            "mappings": [
                {"source_table": "SRC_CUSTOMER", "source_column": "CUSTOMER_ID",  "target_table": "TGT_DIM_CUSTOMER", "target_column": "CUSTOMER_BK",  "expression": "CUSTOMER_ID",                          "data_type": "INTEGER"},
                {"source_table": "SRC_CUSTOMER", "source_column": "FIRST_NAME",   "target_table": "TGT_DIM_CUSTOMER", "target_column": "FULL_NAME",     "expression": "TRIM(FIRST_NAME||' '||LAST_NAME)",      "data_type": "VARCHAR(200)"},
                {"source_table": "SRC_CUSTOMER", "source_column": "EMAIL",         "target_table": "TGT_DIM_CUSTOMER", "target_column": "EMAIL",          "expression": "LOWER(EMAIL)",                          "data_type": "VARCHAR(255)"},
                {"source_table": "SRC_ORDER",    "source_column": "ORDER_ID",      "target_table": "TGT_FACT_ORDER",   "target_column": "ORDER_HK",       "expression": "MD5(ORDER_ID)",                         "data_type": "VARCHAR(32)"},
                {"source_table": "SRC_ORDER",    "source_column": "ORDER_AMOUNT",  "target_table": "TGT_FACT_ORDER",   "target_column": "ORDER_AMOUNT",   "expression": "NVL(ORDER_AMOUNT,0)",                   "data_type": "DECIMAL(18,2)"},
            ],
            "transformations": [
                {"name": "EXPR_FULL_NAME",    "type": "expression",   "expressions": [{"field": "FULL_NAME",    "expression": "TRIM(FIRST_NAME||' '||LAST_NAME)"}]},
                {"name": "EXPR_HASH_KEY",     "type": "expression",   "expressions": [{"field": "CUSTOMER_HK",  "expression": "MD5(CAST(CUSTOMER_ID AS VARCHAR))"}]},
                {"name": "AGG_REVENUE",       "type": "aggregation",  "expressions": [{"field": "TOTAL_REVENUE","expression": "SUM(ORDER_AMOUNT)"}]},
                {"name": "FILTER_ACTIVE",     "type": "filter",       "expressions": [{"field": "FILTER",       "expression": "IS_ACTIVE = TRUE"}]},
            ],
        }
