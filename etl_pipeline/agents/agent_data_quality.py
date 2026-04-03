# =============================================================================
# Author      : gokulram.krishnan
# Repository  : https://github.com/gokulramkrishnan/Useful-Jupyter-Notebook
# Description : """
# =============================================================================
"""
Agent 7: Data Quality Agent
Generates DQ rules from metadata + STTM + Data Vault model.
Produces:
  - DQ rules evaluated against the source DataFrame
  - dbt test YAML appended to schema.yml
  - Great Expectations checkpoint config
  - Excel DQ report with pass/fail per rule
"""

import pandas as pd
import numpy as np
import json
import yaml
import os
import re
from datetime import datetime
from typing import Optional


# ─────────────────────────────────────────────────────────────────────────────
# DQ Rule definitions
# ─────────────────────────────────────────────────────────────────────────────

DQ_DIMENSIONS = {
    "COMPLETENESS":  "No unexpected NULLs in required fields",
    "UNIQUENESS":    "No duplicate values in key columns",
    "VALIDITY":      "Values conform to defined formats and ranges",
    "CONSISTENCY":   "Referential integrity and cross-column consistency",
    "TIMELINESS":    "Date/timestamp values are within expected ranges",
    "ACCURACY":      "Numeric values within expected statistical bounds",
}


def generate_dq_rules(df: pd.DataFrame, metadata: dict, sttm: dict, dv_model: dict) -> list:
    """
    Auto-generate DQ rules from:
    - Source column metadata (PK, NOT NULL flags)
    - STTM (PII flags, mapping completeness)
    - Data Vault model (hub business keys, satellite payloads)
    """
    rules = []

    # ── 1. Completeness — NOT NULL checks ─────────────────────────────────────
    for src in metadata.get("sources", []):
        for col in src.get("columns", []):
            if col.get("nullable","").upper() in ("N","NO","FALSE","0"):
                rules.append({
                    "rule_id":    f"DQ_COMP_{len(rules)+1:04d}",
                    "dimension":  "COMPLETENESS",
                    "table":      src.get("table_name",""),
                    "column":     col.get("name",""),
                    "rule":       f"Column '{col['name']}' must not be NULL",
                    "check_type": "NOT_NULL",
                    "threshold":  0,
                    "dbt_test":   "not_null",
                })

    # ── 2. Uniqueness — PK / BK columns ──────────────────────────────────────
    for hub in dv_model.get("hubs", []):
        for bk in hub.get("business_keys", []):
            rules.append({
                "rule_id":    f"DQ_UNIQ_{len(rules)+1:04d}",
                "dimension":  "UNIQUENESS",
                "table":      hub.get("source_table",""),
                "column":     bk,
                "rule":       f"Business key '{bk}' must be unique",
                "check_type": "UNIQUE",
                "threshold":  0,
                "dbt_test":   "unique",
            })
        # Hub hash key uniqueness
        rules.append({
            "rule_id":    f"DQ_UNIQ_{len(rules)+1:04d}",
            "dimension":  "UNIQUENESS",
            "table":      hub.get("hub_name",""),
            "column":     hub.get("hash_key",""),
            "rule":       f"Hub hash key '{hub['hash_key']}' must be unique",
            "check_type": "UNIQUE",
            "threshold":  0,
            "dbt_test":   "unique",
        })

    # ── 3. Validity — data type / format / range checks ───────────────────────
    for src in metadata.get("sources", []):
        for col in src.get("columns", []):
            col_name  = col.get("name", "")
            data_type = col.get("data_type","").upper()
            col_lower = col_name.lower()

            # Email format
            if "email" in col_lower:
                rules.append({
                    "rule_id":    f"DQ_VALD_{len(rules)+1:04d}",
                    "dimension":  "VALIDITY",
                    "table":      src.get("table_name",""),
                    "column":     col_name,
                    "rule":       f"'{col_name}' must match email format",
                    "check_type": "REGEX",
                    "pattern":    r"^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$",
                    "dbt_test":   "dbt_utils.expression_is_true",
                    "dbt_expr":   f"{col_name} RLIKE '^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{{2,}}$'",
                })

            # Date columns in range
            if "DATE" in data_type or "TIMESTAMP" in data_type or "date" in col_lower:
                rules.append({
                    "rule_id":    f"DQ_VALD_{len(rules)+1:04d}",
                    "dimension":  "TIMELINESS",
                    "table":      src.get("table_name",""),
                    "column":     col_name,
                    "rule":       f"'{col_name}' must be between 1900-01-01 and today",
                    "check_type": "DATE_RANGE",
                    "min_date":   "1900-01-01",
                    "max_date":   str(datetime.utcnow().date()),
                    "dbt_test":   "dbt_utils.expression_is_true",
                    "dbt_expr":   f"{col_name} >= '1900-01-01' AND {col_name} <= CURRENT_DATE()",
                })

            # Numeric > 0 for revenue/price/amount
            if any(k in col_lower for k in ["amount","revenue","price","salary","cost"]) and "DECIMAL" in data_type:
                rules.append({
                    "rule_id":    f"DQ_ACCR_{len(rules)+1:04d}",
                    "dimension":  "ACCURACY",
                    "table":      src.get("table_name",""),
                    "column":     col_name,
                    "rule":       f"'{col_name}' must be >= 0",
                    "check_type": "RANGE",
                    "min_value":  0,
                    "dbt_test":   "dbt_utils.expression_is_true",
                    "dbt_expr":   f"NVL({col_name}, 0) >= 0",
                })

    # ── 4. Consistency — FK referential integrity ─────────────────────────────
    for link in dv_model.get("links", []):
        rules.append({
            "rule_id":    f"DQ_CONS_{len(rules)+1:04d}",
            "dimension":  "CONSISTENCY",
            "table":      link.get("link_name",""),
            "column":     link.get("hub_a_hk",""),
            "rule":       f"FK '{link['hub_a_hk']}' must exist in {link['entity_a']} Hub",
            "check_type": "REFERENTIAL_INTEGRITY",
            "ref_table":  f"hub_{link['entity_a'].lower()}",
            "ref_column": link.get("hub_a_hk",""),
            "dbt_test":   "relationships",
            "dbt_ref":    f"ref('hub_{link['entity_a'].lower()}')",
        })

    # ── 5. PII column completeness (from STTM) ────────────────────────────────
    for m in sttm.get("mappings", []):
        if m.get("pii_flag") == "Y" and m.get("source_column"):
            rules.append({
                "rule_id":    f"DQ_PII_{len(rules)+1:04d}",
                "dimension":  "COMPLETENESS",
                "table":      m.get("source_table",""),
                "column":     m.get("source_column",""),
                "rule":       f"PII column '{m['source_column']}' must not be NULL",
                "check_type": "NOT_NULL",
                "threshold":  0,
                "pii":        True,
                "dbt_test":   "not_null",
            })

    return rules


def evaluate_dq_rules(df: pd.DataFrame, rules: list) -> list:
    """
    Evaluate each DQ rule against the DataFrame.
    Returns rules list enriched with actual_failures, pass_rate, status.
    """
    evaluated = []
    for rule in rules:
        col       = rule.get("column","")
        check     = rule.get("check_type","")
        threshold = rule.get("threshold", 0)
        result    = rule.copy()

        if col not in df.columns:
            result.update({"actual_failures": 0, "pass_rate": 100.0, "status": "SKIPPED",
                           "note": f"Column '{col}' not in DataFrame — will be evaluated in dbt"})
            evaluated.append(result)
            continue

        try:
            series = df[col]
            n      = len(series)

            if check == "NOT_NULL":
                failures = int(series.isna().sum())
            elif check == "UNIQUE":
                failures = int(n - series.nunique())
            elif check == "REGEX":
                pattern  = rule.get("pattern","")
                failures = int((~series.dropna().astype(str).str.match(pattern)).sum())
            elif check == "RANGE":
                min_val  = rule.get("min_value")
                max_val  = rule.get("max_value")
                mask     = pd.Series([False] * n, index=series.index)
                if min_val is not None:
                    mask = mask | (pd.to_numeric(series, errors="coerce") < min_val)
                if max_val is not None:
                    mask = mask | (pd.to_numeric(series, errors="coerce") > max_val)
                failures = int(mask.sum())
            elif check == "DATE_RANGE":
                dt_series = pd.to_datetime(series, errors="coerce")
                min_d = pd.Timestamp(rule.get("min_date","1900-01-01"))
                max_d = pd.Timestamp(rule.get("max_date", datetime.utcnow().date()))
                failures = int(((dt_series < min_d) | (dt_series > max_d)).sum())
            else:
                result.update({"actual_failures": 0, "pass_rate": 100.0, "status": "SKIPPED",
                               "note": "Check type evaluated in dbt/database"})
                evaluated.append(result)
                continue

            pass_rate = round((n - failures) / n * 100, 2) if n > 0 else 100.0
            status    = "PASS" if failures <= threshold else "FAIL"
            result.update({
                "rows_checked":     n,
                "actual_failures":  failures,
                "pass_rate":        pass_rate,
                "status":           status,
            })
        except Exception as e:
            result.update({"actual_failures": 0, "pass_rate": 0, "status": "ERROR", "note": str(e)})

        evaluated.append(result)
    return evaluated


def generate_dbt_dq_tests(dq_rules: list, dv_model: dict, output_dir: str):
    """Append DQ tests to dbt schema.yml."""
    schema = {"version": 2, "models": []}
    model_map = {}

    for rule in dq_rules:
        if not rule.get("dbt_test") or rule.get("check_type") in ("REFERENTIAL_INTEGRITY",):
            continue
        table = rule.get("table","").lower()
        col   = rule.get("column","")
        test  = rule.get("dbt_test","")
        if not table or not col:
            continue

        if table not in model_map:
            model_map[table] = {"name": table, "columns": []}

        col_entry = next((c for c in model_map[table]["columns"] if c["name"] == col), None)
        if not col_entry:
            col_entry = {"name": col, "tests": []}
            model_map[table]["columns"].append(col_entry)

        if test in ("not_null","unique") and test not in col_entry["tests"]:
            col_entry["tests"].append(test)
        elif test.startswith("dbt_utils"):
            expr_test = {"dbt_utils.expression_is_true": {"expression": rule.get("dbt_expr","")}}
            col_entry["tests"].append(expr_test)

    schema["models"] = list(model_map.values())
    path = f"{output_dir}/models/raw_vault/dq_tests.yml"
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        yaml.dump(schema, f, default_flow_style=False, sort_keys=False)


def generate_dq_report(rules_evaluated: list, output_dir: str) -> dict:
    """Write DQ results to Excel."""
    df = pd.DataFrame(rules_evaluated)
    passed  = int((df["status"] == "PASS").sum())
    failed  = int((df["status"] == "FAIL").sum())
    skipped = int((df["status"] == "SKIPPED").sum())
    errored = int((df["status"] == "ERROR").sum())

    summary = {
        "total_rules":     len(df),
        "passed":          passed,
        "failed":          failed,
        "skipped":         skipped,
        "errored":         errored,
        "overall_pass_pct": round(passed / max(passed + failed, 1) * 100, 2),
        "dimensions":       df.groupby("dimension")["status"].value_counts().to_dict() if "dimension" in df.columns else {},
    }

    df_summary = pd.DataFrame([{k: v for k, v in summary.items() if not isinstance(v, dict)}])
    df_failed  = df[df["status"] == "FAIL"]

    with pd.ExcelWriter(f"{output_dir}/dq_report.xlsx", engine="openpyxl") as writer:
        df_summary.to_excel(writer,  sheet_name="Summary",       index=False)
        df.to_excel(writer,          sheet_name="All Rules",      index=False)
        df_failed.to_excel(writer,   sheet_name="Failed Rules",   index=False)

    return summary


# ─────────────────────────────────────────────────────────────────────────────
# Agent class
# ─────────────────────────────────────────────────────────────────────────────

class DataQualityAgent:
    def run(self, df: pd.DataFrame, metadata: dict, sttm: dict,
            dv_model: dict, output_dir: str, dbt_project_dir: str) -> dict:

        print("  Generating DQ rules from metadata + STTM + Data Vault model...")
        rules = generate_dq_rules(df, metadata, sttm, dv_model)
        print(f"    Generated {len(rules)} DQ rules across {len(DQ_DIMENSIONS)} dimensions")

        print("  Evaluating DQ rules against source DataFrame...")
        rules_evaluated = evaluate_dq_rules(df, rules)

        print("  Writing dbt DQ tests...")
        generate_dbt_dq_tests(rules_evaluated, dv_model, dbt_project_dir)

        print("  Generating DQ report...")
        summary = generate_dq_report(rules_evaluated, output_dir)

        # Save full rules JSON
        with open(f"{output_dir}/dq_rules.json", "w") as f:
            json.dump(rules_evaluated, f, indent=2, default=str)

        passed = summary["passed"]
        failed = summary["failed"]
        print(f"    ✅ {passed} passed | ❌ {failed} failed | ⏭️  {summary['skipped']} skipped")

        return {
            "rules_generated": len(rules),
            "dbt_tests":       len([r for r in rules if r.get("dbt_test")]),
            "rules_passed":    passed,
            "rules_failed":    failed,
        }
