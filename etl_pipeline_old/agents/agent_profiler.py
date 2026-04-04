# =============================================================================
# Author      : gokulram.krishnan
# Repository  : https://github.com/gokulramkrishnan/Useful-Jupyter-Notebook
# Description : """
# =============================================================================
"""
Agent 5: Data Profiler
Runs ydata-profiling + Presidio PII detection on source DataFrames.
Outputs: HTML profile report, JSON summary, PII column/cell detail.
"""

import pandas as pd
import numpy as np
import json
import warnings
from collections import defaultdict
from datetime import datetime
from pathlib import Path

warnings.filterwarnings("ignore")


def run_ydata_profiling(df: pd.DataFrame, dataset_name: str, output_dir: str) -> dict:
    """Run ydata-profiling and return extracted statistics."""
    from ydata_profiling import ProfileReport

    profile = ProfileReport(df, title=dataset_name, explorative=True, minimal=False)
    profile.to_file(f"{output_dir}/profile_report.html")

    desc        = profile.get_description()
    table_stats = desc.table
    n_rows      = int(table_stats.get("n", 1))

    overview = {
        "dataset_name":       dataset_name,
        "n_rows":             n_rows,
        "n_cols":             int(table_stats.get("n_var", 0)),
        "n_missing_cells":    int(table_stats.get("n_cells_missing", 0)),
        "pct_missing_cells":  round(float(table_stats.get("p_cells_missing", 0)) * 100, 4),
        "n_duplicate_rows":   int(table_stats.get("n_duplicates", 0)),
        "n_numeric_cols":     int(table_stats.get("types", {}).get("Numeric", 0)),
        "n_categorical_cols": int(table_stats.get("types", {}).get("Categorical", 0)),
    }

    col_stats = []
    for col_name, col_data in desc.variables.items():
        col_type = str(col_data.get("type", "Unknown"))
        stat = {
            "column_name": col_name,
            "column_type": col_type,
            "n_missing":   int(col_data.get("n_missing", 0)),
            "pct_missing": round(float(col_data.get("p_missing", 0)) * 100, 4),
            "n_unique":    int(col_data.get("n_unique", col_data.get("n_distinct", 0))),
        }
        if col_data.get("mean") is not None:
            stat.update({
                "mean":     round(float(col_data.get("mean",     0) or 0), 4),
                "std":      round(float(col_data.get("std",      0) or 0), 4),
                "min":      round(float(col_data.get("min",      0) or 0), 4),
                "max":      round(float(col_data.get("max",      0) or 0), 4),
                "skewness": round(float(col_data.get("skewness", 0) or 0), 4),
            })
        col_stats.append(stat)

    return {"overview": overview, "col_stats": col_stats}


def run_pii_detection(df: pd.DataFrame, dataset_name: str) -> dict:
    """Run Presidio PII detection and return column + cell level results."""
    import spacy
    from presidio_analyzer import AnalyzerEngine, RecognizerRegistry
    from presidio_analyzer.nlp_engine import NlpEngineProvider
    from presidio_analyzer.predefined_recognizers import (
        EmailRecognizer, PhoneRecognizer, CreditCardRecognizer,
        IpRecognizer, UsSsnRecognizer, DateRecognizer, SpacyRecognizer,
    )

    # Model selection
    installed = spacy.util.get_installed_models()
    spacy_model = next((m for m in ["en_core_web_lg","en_core_web_md","en_core_web_sm"] if m in installed), None)
    if not spacy_model:
        print("    ⚠️  No spaCy model — skipping NER-based PII (install en_core_web_lg)")
        spacy_model = None

    nlp_config  = {"nlp_engine_name": "spacy", "models": [{"lang_code": "en", "model_name": spacy_model or "en_core_web_sm"}]}
    nlp_engine  = NlpEngineProvider(nlp_configuration=nlp_config).create_engine()
    registry    = RecognizerRegistry()
    registry.add_recognizer(EmailRecognizer(supported_language="en"))
    registry.add_recognizer(PhoneRecognizer(supported_language="en", supported_regions=["US","GB","CA"]))
    registry.add_recognizer(CreditCardRecognizer(supported_language="en"))
    registry.add_recognizer(IpRecognizer(supported_language="en"))
    registry.add_recognizer(UsSsnRecognizer(supported_language="en"))
    registry.add_recognizer(DateRecognizer(supported_language="en"))
    if spacy_model:
        registry.add_recognizer(SpacyRecognizer(supported_language="en", supported_entities=["PERSON","LOCATION"], ner_strength=0.85))

    analyzer = AnalyzerEngine(nlp_engine=nlp_engine, registry=registry, supported_languages=["en"])

    ENTITY_THRESHOLDS = {
        "EMAIL_ADDRESS": 0.5, "US_SSN":      0.5, "PHONE_NUMBER": 0.5,
        "CREDIT_CARD":   0.5, "IP_ADDRESS":  0.5, "DATE_TIME":    0.6,
        "PERSON":        0.7, "LOCATION":    0.7,
    }
    PII_ENTITIES  = list(ENTITY_THRESHOLDS.keys())
    string_cols   = df.select_dtypes(include=["object","string"]).columns.tolist()

    cell_rows   = []
    col_staging = defaultdict(list)

    for col in string_cols:
        for row_idx, val in df[col].items():
            if pd.isna(val) or not str(val).strip():
                continue
            try:
                results = analyzer.analyze(text=str(val), entities=PII_ENTITIES, language="en")
            except Exception:
                continue
            for r in results:
                if r.score < ENTITY_THRESHOLDS.get(r.entity_type, 0.5):
                    continue
                cell_rows.append({
                    "column_name":  col,
                    "row_index":    int(row_idx),
                    "entity_type":  r.entity_type,
                    "confidence":   round(r.score, 4),
                })
                col_staging[col].append((r.entity_type, r.score))

    col_summary = []
    for col in string_cols:
        dets     = col_staging.get(col, [])
        entities = sorted({e for e, _ in dets})
        scores   = [s for _, s in dets]
        n_nn     = int(df[col].notna().sum())
        flagged  = len({r["row_index"] for r in cell_rows if r["column_name"] == col})
        col_summary.append({
            "column_name":          col,
            "is_pii":               len(entities) > 0,
            "detected_entity_types":  ", ".join(entities) or None,
            "avg_confidence":       round(float(np.mean(scores)), 4) if scores else None,
            "max_confidence":       round(float(np.max(scores)),  4) if scores else None,
            "n_flagged_rows":       flagged,
            "pct_flagged_rows":     round(flagged / n_nn * 100, 2) if n_nn > 0 else 0,
        })

    return {
        "pii_column_summary": col_summary,
        "pii_cell_detail":    cell_rows,
        "pii_columns":        len([c for c in col_summary if c["is_pii"]]),
    }


# ─────────────────────────────────────────────────────────────────────────────
# Agent class
# ─────────────────────────────────────────────────────────────────────────────

class ProfilerAgent:
    def run(self, df: pd.DataFrame, dataset_name: str, output_dir: str) -> dict:
        print(f"  Running ydata-profiling on {df.shape[0]} rows × {df.shape[1]} cols...")
        profiling = run_ydata_profiling(df, dataset_name, output_dir)
        print(f"    ✅ Profile report saved")

        print("  Running Presidio PII detection...")
        pii = run_pii_detection(df, dataset_name)
        print(f"    ✅ PII columns detected: {pii['pii_columns']}")

        result = {**profiling, **pii, "dataset_name": dataset_name, "run_at": datetime.utcnow().isoformat()}
        with open(f"{output_dir}/profiling_summary.json", "w") as f:
            json.dump(result, f, indent=2, default=str)

        # Save PII summary as CSV for easy review
        pd.DataFrame(pii["pii_column_summary"]).to_csv(f"{output_dir}/pii_column_summary.csv", index=False)

        return result
