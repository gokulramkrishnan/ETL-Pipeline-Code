# =============================================================================
# Author      : gokulram.krishnan
# Repository  : https://github.com/gokulramkrishnan/Useful-Jupyter-Notebook
# Description : """
# =============================================================================
"""
Agent 6: Data Reconciliation Agent
Performs source-to-target reconciliation checks:
  - Row count comparison
  - Missing / extra key detection
  - Aggregate comparison (SUM, AVG, COUNT) per numeric column
  - Cell-level value mismatch detection
  - Hash-based row comparison
  - Recommendations for each discrepancy found
"""

import pandas as pd
import numpy as np
import json
from datetime import datetime
from typing import Optional


# ─────────────────────────────────────────────────────────────────────────────
# Standalone reconciliation functions
# ─────────────────────────────────────────────────────────────────────────────

def check_row_counts(df_source: pd.DataFrame, df_target: pd.DataFrame) -> dict:
    src_count = len(df_source)
    tgt_count = len(df_target)
    diff      = src_count - tgt_count
    return {
        "check":           "ROW_COUNT",
        "source_count":    src_count,
        "target_count":    tgt_count,
        "difference":      diff,
        "pct_difference":  round(abs(diff) / src_count * 100, 4) if src_count > 0 else 0,
        "status":          "PASS" if diff == 0 else "FAIL",
        "recommendation":  (
            "Row counts match." if diff == 0
            else f"{abs(diff)} rows {'missing in target' if diff > 0 else 'extra in target'}. "
                 f"Investigate ETL filter conditions, rejected records log, or truncation."
        ),
    }


def check_missing_keys(df_source: pd.DataFrame, df_target: pd.DataFrame, key_col: str) -> dict:
    if key_col not in df_source.columns or key_col not in df_target.columns:
        return {"check": "MISSING_KEYS", "status": "SKIPPED", "reason": f"Key column '{key_col}' not found"}

    src_keys = set(df_source[key_col].dropna().astype(str))
    tgt_keys = set(df_target[key_col].dropna().astype(str))

    missing_in_tgt = src_keys - tgt_keys
    extra_in_tgt   = tgt_keys - src_keys

    return {
        "check":              "MISSING_KEYS",
        "key_column":         key_col,
        "missing_in_target":  len(missing_in_tgt),
        "extra_in_target":    len(extra_in_tgt),
        "sample_missing":     list(missing_in_tgt)[:10],
        "sample_extra":       list(extra_in_tgt)[:10],
        "status":             "PASS" if (not missing_in_tgt and not extra_in_tgt) else "FAIL",
        "recommendation": (
            "All keys matched." if not missing_in_tgt and not extra_in_tgt
            else (
                (f"{len(missing_in_tgt)} keys missing in target — check for ETL load failures, key generation mismatches. "
                 f"Sample: {list(missing_in_tgt)[:5]}. " if missing_in_tgt else "") +
                (f"{len(extra_in_tgt)} extra keys in target — check for duplicate loads or stale data. " if extra_in_tgt else "")
            )
        ),
    }


def check_aggregates(df_source: pd.DataFrame, df_target: pd.DataFrame,
                     numeric_cols: list, tolerance_pct: float = 0.001) -> list:
    """Compare SUM and AVG for each numeric column. Flag if diff > tolerance_pct."""
    results = []
    for col in numeric_cols:
        src_col = df_source[col].dropna() if col in df_source.columns else pd.Series(dtype=float)
        tgt_col = df_target[col].dropna() if col in df_target.columns else pd.Series(dtype=float)

        for agg_fn, label in [(np.sum, "SUM"), (np.mean, "AVG"), (len, "COUNT")]:
            src_val = float(agg_fn(src_col)) if len(src_col) > 0 else 0
            tgt_val = float(agg_fn(tgt_col)) if len(tgt_col) > 0 else 0
            diff    = abs(src_val - tgt_val)
            pct_diff = diff / abs(src_val) * 100 if src_val != 0 else (0 if tgt_val == 0 else 100)
            status  = "PASS" if pct_diff <= tolerance_pct else "FAIL"

            results.append({
                "check":           f"AGGREGATE_{label}",
                "column":          col,
                "aggregate":       label,
                "source_value":    round(src_val, 4),
                "target_value":    round(tgt_val, 4),
                "absolute_diff":   round(diff, 4),
                "pct_diff":        round(pct_diff, 6),
                "tolerance_pct":   tolerance_pct,
                "status":          status,
                "recommendation": (
                    f"{label}({col}) matches within tolerance." if status == "PASS"
                    else f"{label}({col}) differs by {round(pct_diff,4)}%. "
                         f"Check for NULL handling differences, rounding, or missing rows. "
                         f"Source={round(src_val,2)}, Target={round(tgt_val,2)}."
                ),
            })
    return results


def check_value_mismatches(df_source: pd.DataFrame, df_target: pd.DataFrame,
                            key_col: str, check_cols: Optional[list] = None) -> dict:
    """Row-level value comparison for matched keys."""
    if key_col not in df_source.columns or key_col not in df_target.columns:
        return {"check": "VALUE_MISMATCH", "status": "SKIPPED", "reason": f"Key '{key_col}' missing"}

    df_src = df_source.set_index(key_col)
    df_tgt = df_target.set_index(key_col)
    common_keys = df_src.index.intersection(df_tgt.index)

    cols_to_check = check_cols or [c for c in df_src.columns if c in df_tgt.columns]
    mismatches    = []

    for key in common_keys:
        for col in cols_to_check:
            src_val = df_src.loc[key, col] if col in df_src.columns else None
            tgt_val = df_tgt.loc[key, col] if col in df_tgt.columns else None
            src_null = pd.isna(src_val)
            tgt_null = pd.isna(tgt_val)

            if src_null != tgt_null:
                mismatches.append({"key": key, "column": col,
                                   "source": None if src_null else src_val,
                                   "target": None if tgt_null else tgt_val,
                                   "mismatch_type": "NULL_DIFFERENCE"})
            elif not src_null and str(src_val) != str(tgt_val):
                mismatches.append({"key": key, "column": col,
                                   "source": src_val, "target": tgt_val,
                                   "mismatch_type": "VALUE_DIFFERENCE"})

    return {
        "check":             "VALUE_MISMATCH",
        "columns_checked":   cols_to_check,
        "rows_compared":     len(common_keys),
        "mismatches_found":  len(mismatches),
        "sample_mismatches": mismatches[:20],
        "status":            "PASS" if not mismatches else "FAIL",
        "recommendation": (
            "All values matched for compared rows." if not mismatches
            else f"{len(mismatches)} value mismatch(es) found. "
                 f"Top issue types: {pd.Series([m['mismatch_type'] for m in mismatches]).value_counts().to_dict()}. "
                 f"Review transformation logic for affected columns."
        ),
    }


def check_hash_comparison(df_source: pd.DataFrame, df_target: pd.DataFrame,
                           key_col: str) -> dict:
    """MD5 hash of each row for fast bulk comparison."""
    import hashlib

    def row_hash(row):
        return hashlib.md5("|".join(str(v) for v in row).encode()).hexdigest()

    if key_col not in df_source.columns or key_col not in df_target.columns:
        return {"check": "HASH_COMPARISON", "status": "SKIPPED"}

    src_hashes = df_source.apply(row_hash, axis=1)
    tgt_hashes = df_target.apply(row_hash, axis=1)

    df_source = df_source.copy()
    df_target = df_target.copy()
    df_source["_row_hash"] = src_hashes
    df_target["_row_hash"] = tgt_hashes

    src_map = dict(zip(df_source[key_col].astype(str), df_source["_row_hash"]))
    tgt_map = dict(zip(df_target[key_col].astype(str), df_target["_row_hash"]))

    common  = set(src_map) & set(tgt_map)
    diff_keys = [k for k in common if src_map[k] != tgt_map[k]]

    return {
        "check":            "HASH_COMPARISON",
        "rows_compared":    len(common),
        "hash_mismatches":  len(diff_keys),
        "sample_diff_keys": diff_keys[:10],
        "status":           "PASS" if not diff_keys else "FAIL",
        "recommendation": (
            "All row hashes matched." if not diff_keys
            else f"{len(diff_keys)} rows have differing hashes. "
                 f"Run VALUE_MISMATCH check on these keys to pinpoint columns."
        ),
    }


def generate_reconciliation_report(checks: list, output_dir: str):
    """Write reconciliation results to Excel with pass/fail summary."""
    rows = []
    for c in checks:
        if isinstance(c, list):
            rows.extend(c)
        else:
            rows.append(c)

    df = pd.DataFrame(rows)
    summary = {
        "total_checks": len(df),
        "passed":       int((df["status"] == "PASS").sum()),
        "failed":       int((df["status"] == "FAIL").sum()),
        "skipped":      int((df["status"] == "SKIPPED").sum()),
    }

    df_summary = pd.DataFrame([summary])
    df_failed  = df[df["status"] == "FAIL"]

    with pd.ExcelWriter(f"{output_dir}/recon_report.xlsx", engine="openpyxl") as writer:
        df_summary.to_excel(writer, sheet_name="Summary",         index=False)
        df.to_excel(writer,         sheet_name="All Checks",      index=False)
        df_failed.to_excel(writer,  sheet_name="Failed Checks",   index=False)

    with open(f"{output_dir}/recon_report.json", "w") as f:
        json.dump({"summary": summary, "checks": rows}, f, indent=2, default=str)

    return summary


# ─────────────────────────────────────────────────────────────────────────────
# Agent class
# ─────────────────────────────────────────────────────────────────────────────

class ReconciliationAgent:
    def run(self, df_source: pd.DataFrame, df_target: pd.DataFrame,
            key_column: str, numeric_cols: list, output_dir: str) -> dict:

        print(f"  Running reconciliation checks (key={key_column})...")

        check_cols = [c for c in df_source.columns if c in df_target.columns and c != key_column]

        checks = [
            check_row_counts(df_source, df_target),
            check_missing_keys(df_source, df_target, key_column),
            check_aggregates(df_source, df_target, numeric_cols),
            check_value_mismatches(df_source, df_target, key_column, check_cols[:10]),
            check_hash_comparison(df_source, df_target, key_column),
        ]

        summary = generate_reconciliation_report(checks, output_dir)

        # Flatten checks (aggregate check returns a list)
        flat_checks = []
        for c in checks:
            if isinstance(c, list):
                flat_checks.extend(c)
            else:
                flat_checks.append(c)

        # Collect top-level metrics for orchestrator
        rc_check   = next((c for c in flat_checks if c.get("check") == "ROW_COUNT"), {})
        key_check  = next((c for c in flat_checks if c.get("check") == "MISSING_KEYS"), {})
        val_check  = next((c for c in flat_checks if c.get("check") == "VALUE_MISMATCH"), {})

        print(f"    ✅ {summary['passed']} passed | ❌ {summary['failed']} failed")

        return {
            "row_count_match":    rc_check.get("status") == "PASS",
            "missing_in_target":  key_check.get("missing_in_target", 0),
            "value_mismatches":   val_check.get("mismatches_found", 0),
            "checks_passed":      summary["passed"],
            "checks_failed":      summary["failed"],
        }
