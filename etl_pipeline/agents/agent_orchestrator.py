# =============================================================================
# Author      : gokulram.krishnan
# Repository  : https://github.com/gokulramkrishnan/ETL-Pipeline-Code
# Description : Agent 9 — Pipeline Orchestrator
#               Horizontal coordination layer that stitches all agents
#               (0–8) into a single end-to-end execution plan with
#               dependency management, state tracking, retry logic,
#               and a consolidated run report.
#
# Execution order:
#   Agent 0  → LegacyScannerAgent   (discover legacy objects & patterns)
#   Agent 1  → ReverseEngineerAgent (parse DataStage / Informatica files)
#   Agent 2  → STTMAgent            (source-to-target mapping)
#   Agent 3  → DataVaultAgent       (DV2.0 Hub/Link/Satellite model)
#   Agent 4  → DBTGeneratorAgent    (dbt project SQL + YAML)
#   Agent 5  → ProfilerAgent        (ydata-profiling + Presidio PII)
#   Agent 6  → ReconciliationAgent  (row count, checksum, missing keys)
#   Agent 7  → DataQualityAgent     (DQ rule engine + dbt tests)
#   Agent 8  → run_agent8_mermaid_er (Mermaid ER + FIBO/BIAN diagrams)
# =============================================================================

from __future__ import annotations

import json
import os
import sys
import time
import traceback
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Optional

import pandas as pd


# ─────────────────────────────────────────────────────────────────────────────
# AgentStep — wraps each agent call with status tracking
# ─────────────────────────────────────────────────────────────────────────────

class AgentStep:
    """Represents a single agent execution step with full run metadata."""

    def __init__(self, agent_id: str, name: str, fn: Callable, depends_on: list[str] = None):
        self.agent_id   = agent_id
        self.name       = name
        self.fn         = fn
        self.depends_on = depends_on or []

        self.status     = "PENDING"    # PENDING | RUNNING | SUCCESS | FAILED | SKIPPED
        self.result     : Any   = None
        self.error      : str   = ""
        self.start_time : float = 0.0
        self.end_time   : float = 0.0
        self.duration_s : float = 0.0
        self.retries    : int   = 0

    def to_dict(self) -> dict:
        return {
            "agent_id":   self.agent_id,
            "name":       self.name,
            "status":     self.status,
            "duration_s": round(self.duration_s, 3),
            "retries":    self.retries,
            "depends_on": self.depends_on,
            "error":      self.error[:500] if self.error else "",
        }


# ─────────────────────────────────────────────────────────────────────────────
# PipelineState — shared context passed between agents
# ─────────────────────────────────────────────────────────────────────────────

class PipelineState:
    """
    Shared mutable state passed between all agents.
    Each agent reads inputs from state and writes its outputs back.
    """

    def __init__(self, config: dict):
        self.config     : dict  = config
        self.output_dir : str   = config.get("output_dir", "output")
        self.run_ts     : str   = datetime.utcnow().isoformat()

        # ── Agent outputs (populated as pipeline runs) ────────────────────────
        self.legacy_scan     : dict = {}   # Agent 0
        self.metadata        : dict = {}   # Agent 1
        self.sttm            : dict = {}   # Agent 2
        self.dv_model        : dict = {}   # Agent 3
        self.dbt_files       : dict = {}   # Agent 4
        self.profile         : dict = {}   # Agent 5
        self.recon           : dict = {}   # Agent 6
        self.dq              : Any  = None  # Agent 7 (DataFrame)
        self.mermaid_diagrams: dict = {}   # Agent 8

        # ── Source DataFrames for profiling/recon/DQ ─────────────────────────
        self.source_df  : Optional[pd.DataFrame] = None
        self.target_df  : Optional[pd.DataFrame] = None

    def get_dbt_output_dir(self) -> str:
        d = os.path.join(self.output_dir, "dbt_project")
        os.makedirs(d, exist_ok=True)
        return d

    def get_subdir(self, name: str) -> str:
        d = os.path.join(self.output_dir, name)
        os.makedirs(d, exist_ok=True)
        return d


# ─────────────────────────────────────────────────────────────────────────────
# Orchestrator Agent
# ─────────────────────────────────────────────────────────────────────────────

class OrchestratorAgent:
    """
    Agent 9: Pipeline Orchestrator

    Horizontal coordination layer that manages the full end-to-end
    execution of all pipeline agents (0–8). Responsibilities:

    - Dependency resolution: agents only run when prerequisites succeed
    - State passing: outputs from each agent flow as inputs to downstream agents
    - Retry logic: configurable retries per step on transient failures
    - Skip logic: agents can be skipped via config (e.g., skip_agents=['agent5'])
    - Run report: consolidated HTML + JSON execution report
    - Graceful degradation: pipeline continues if non-critical agents fail

    Pipeline topology (dependency graph):
    ┌─────────────────────────────────────────────────────────────────────┐
    │                        ORCHESTRATOR (Agent 9)                       │
    │                                                                     │
    │  [Agent 0: Legacy Scanner]                                          │
    │       │                                                             │
    │       ▼                                                             │
    │  [Agent 1: Reverse Engineer]  ←── DataStage / Informatica files    │
    │       │                                                             │
    │       ▼                                                             │
    │  [Agent 2: STTM Generator]                                         │
    │       │                                                             │
    │       ▼                                                             │
    │  [Agent 3: Data Vault Modeler]                                      │
    │       │                                                             │
    │       ├──────────────────────┐                                      │
    │       ▼                      ▼                                      │
    │  [Agent 4: DBT Generator]  [Agent 5: Profiler + PII]               │
    │       │                      │                                      │
    │       │              ┌───────┘                                      │
    │       │              ▼                                              │
    │       │       [Agent 6: Reconciliation]                             │
    │       │              │                                              │
    │       │              ▼                                              │
    │       │       [Agent 7: Data Quality]                               │
    │       │              │                                              │
    │       └──────────────┤                                              │
    │                      ▼                                              │
    │              [Agent 8: Mermaid ER + FIBO/BIAN]                      │
    └─────────────────────────────────────────────────────────────────────┘
    """

    def __init__(self, config: dict = None):
        self.config   = config or {}
        self.steps    : list[AgentStep] = []
        self.state    : Optional[PipelineState] = None
        self.run_ts   = datetime.utcnow().isoformat()

    def run(
        self,
        config:          Optional[dict]         = None,
        datastage_files: Optional[list[str]]    = None,
        informatica_files:Optional[list[str]]   = None,
        scan_paths:      Optional[list[str]]    = None,
        inline_sources:  Optional[dict[str,str]]= None,
        source_df:       Optional[pd.DataFrame] = None,
        target_df:       Optional[pd.DataFrame] = None,
        output_dir:      str  = "output",
        skip_agents:     Optional[list[str]]    = None,
        max_retries:     int  = 1,
        use_synthetic:   bool = True,
    ) -> dict:
        """
        Execute the full pipeline end-to-end.

        Args:
            config:            Pipeline configuration dict
            datastage_files:   List of DataStage .dsx/.xml file paths
            informatica_files: List of Informatica .xml/.ipc file paths
            scan_paths:        Legacy system files/dirs for Agent 0 scanning
            inline_sources:    {filename: content} for in-memory scanning
            source_df:         Source DataFrame for profiling/reconciliation/DQ
            target_df:         Target DataFrame for reconciliation
            output_dir:        Root output directory
            skip_agents:       Agent IDs to skip (e.g. ['agent5', 'agent6'])
            max_retries:       Number of retry attempts per agent on failure
            use_synthetic:     Use synthetic demo data if no real files provided

        Returns:
            Full pipeline run report dict.
        """
        cfg = config or self.config
        cfg["output_dir"] = output_dir
        skip = set(skip_agents or [])

        # ── Initialise state ──────────────────────────────────────────────────
        self.state            = PipelineState(cfg)
        self.state.source_df  = source_df
        self.state.target_df  = target_df
        os.makedirs(output_dir, exist_ok=True)

        print("=" * 70)
        print("  PIPELINE ORCHESTRATOR (Agent 9)")
        print(f"  Author     : gokulram.krishnan")
        print(f"  Repository : https://github.com/gokulramkrishnan/ETL-Pipeline-Code")
        print(f"  Run        : {self.run_ts}")
        print(f"  Output     : {output_dir}")
        print("=" * 70)

        # ── Build step definitions ─────────────────────────────────────────────
        self._build_steps(
            datastage_files   = datastage_files   or [],
            informatica_files = informatica_files or [],
            scan_paths        = scan_paths        or [],
            inline_sources    = inline_sources    or {},
            use_synthetic     = use_synthetic,
            max_retries       = max_retries,
            cfg               = cfg,
        )

        # ── Execute steps in dependency order ─────────────────────────────────
        completed: set[str] = set()

        for step in self.steps:
            print(f"\n  ▶  {step.agent_id}: {step.name}")

            # Check skip list
            if step.agent_id in skip:
                step.status = "SKIPPED"
                completed.add(step.agent_id)
                print(f"     ⏭  SKIPPED (in skip_agents list)")
                continue

            # Check dependencies
            missing_deps = [d for d in step.depends_on if d not in completed or
                            next((s for s in self.steps if s.agent_id == d), None) and
                            next(s for s in self.steps if s.agent_id == d).status == "FAILED"]
            failed_deps  = [d for d in step.depends_on
                            if any(s.agent_id == d and s.status == "FAILED" for s in self.steps)]

            if failed_deps:
                step.status = "SKIPPED"
                completed.add(step.agent_id)
                print(f"     ⏭  SKIPPED (dependency failed: {failed_deps})")
                continue

            # Execute with retries
            attempt = 0
            while attempt <= max_retries:
                step.start_time = time.time()
                step.status     = "RUNNING"
                try:
                    step.result  = step.fn(self.state)
                    step.status  = "SUCCESS"
                    step.end_time = time.time()
                    break
                except Exception as e:
                    step.error    = traceback.format_exc()
                    step.retries  = attempt
                    step.end_time = time.time()
                    if attempt < max_retries:
                        print(f"     ⚠️  Attempt {attempt+1} failed — retrying...")
                        attempt += 1
                    else:
                        step.status = "FAILED"
                        print(f"     ❌ FAILED after {attempt+1} attempt(s): {e}")
                        break

            step.duration_s = step.end_time - step.start_time
            completed.add(step.agent_id)

            icon = "✅" if step.status == "SUCCESS" else ("⏭" if step.status == "SKIPPED" else "❌")
            print(f"     {icon} {step.status} ({step.duration_s:.2f}s)")

        # ── Build and save run report ─────────────────────────────────────────
        report = self._build_report(output_dir)
        self._write_report(report, output_dir)

        # Print summary
        counts = {s: sum(1 for st in self.steps if st.status == s)
                  for s in ["SUCCESS","FAILED","SKIPPED","PENDING"]}
        total_time = sum(st.duration_s for st in self.steps)
        print(f"\n{'='*70}")
        print(f"  PIPELINE COMPLETE: ✅ {counts['SUCCESS']} SUCCESS  "
              f"❌ {counts['FAILED']} FAILED  "
              f"⏭  {counts['SKIPPED']} SKIPPED  "
              f"({total_time:.2f}s total)")
        print(f"{'='*70}")

        return report

    # ─────────────────────────────────────────────────────────────────────────
    # Step definitions
    # ─────────────────────────────────────────────────────────────────────────

    def _build_steps(self, datastage_files, informatica_files, scan_paths,
                     inline_sources, use_synthetic, max_retries, cfg):
        """Register all agent steps with their execution functions."""
        self.steps = []

        # ── Agent 0: Legacy Scanner ───────────────────────────────────────────
        def run_agent0(state: PipelineState) -> dict:
            from etl_pipeline.agents.agent_legacy_scanner import LegacyScannerAgent
            agent  = LegacyScannerAgent()
            result = agent.run(
                scan_paths         = scan_paths,
                inline_sources     = inline_sources,
                use_synthetic_demo = use_synthetic,
                output_dir         = state.get_subdir("legacy_scan"),
            )
            state.legacy_scan = result
            # Merge discovered files into datastage/informatica lists
            for candidate in agent.get_reverse_eng_candidates():
                if candidate.get("agent1_compatible") and candidate.get("tool") == "datastage":
                    datastage_files.append(candidate["file"])
                elif candidate.get("agent1_compatible") and candidate.get("tool") == "informatica":
                    informatica_files.append(candidate["file"])
            return result

        self.steps.append(AgentStep("agent0", "Legacy System Scanner", run_agent0))

        # ── Agent 1: Reverse Engineer ─────────────────────────────────────────
        def run_agent1(state: PipelineState) -> dict:
            from etl_pipeline.agents.agent_reverse_engineer import ReverseEngineerAgent
            agent  = ReverseEngineerAgent()
            result = agent.run(
                datastage_path   = datastage_files[0]   if datastage_files   else None,
                informatica_path = informatica_files[0] if informatica_files else None,
            )
            # Save metadata JSON
            meta_path = Path(state.get_subdir("metadata")) / "unified_metadata.json"
            with open(meta_path, "w") as f:
                json.dump(result, f, indent=2)
            state.metadata = result
            return result

        self.steps.append(AgentStep("agent1", "Reverse Engineer (DataStage + Informatica)",
                                    run_agent1, depends_on=["agent0"]))

        # ── Agent 2: STTM Generator ───────────────────────────────────────────
        def run_agent2(state: PipelineState) -> dict:
            from etl_pipeline.agents.agent_sttm import STTMAgent
            agent  = STTMAgent()
            result = agent.run(
                metadata      = state.metadata,
                output_dir    = state.get_subdir("sttm"),
                target_database = cfg.get("snowflake_database", "SNOWFLAKE_DB"),
                target_schema   = cfg.get("dv_schema_raw",       "RAW_VAULT"),
            )
            state.sttm = result
            return result

        self.steps.append(AgentStep("agent2", "STTM Generator",
                                    run_agent2, depends_on=["agent1"]))

        # ── Agent 3: Data Vault Modeler ───────────────────────────────────────
        def run_agent3(state: PipelineState) -> dict:
            from etl_pipeline.agents.agent_data_vault import DataVaultAgent
            agent  = DataVaultAgent()
            result = agent.run(
                metadata       = state.metadata,
                sttm           = state.sttm,
                output_dir     = state.get_subdir("data_vault"),
                record_source  = cfg.get("record_source",  "RECORD_SOURCE"),
                load_date_col  = cfg.get("load_date_col",  "LOAD_DATE"),
                hash_algorithm = cfg.get("hash_algorithm", "MD5"),
            )
            state.dv_model = result
            return result

        self.steps.append(AgentStep("agent3", "Data Vault 2.0 Modeler",
                                    run_agent3, depends_on=["agent2"]))

        # ── Agent 4: DBT Generator ────────────────────────────────────────────
        def run_agent4(state: PipelineState) -> dict:
            from etl_pipeline.agents.agent_dbt_generator import (
                generate_packages_yml, generate_dbt_project_yml,
                generate_profiles_yml, generate_sources_yml,
                generate_schema_yml, generate_hub_model,
                generate_link_model, generate_satellite_model,
            )
            dbt_dir = state.get_dbt_output_dir()
            os.makedirs(os.path.join(dbt_dir, "models", "staging"),   exist_ok=True)
            os.makedirs(os.path.join(dbt_dir, "models", "raw_vault"), exist_ok=True)

            generate_packages_yml(dbt_dir, cfg.get("datavault4dbt_version", "1.17.0"))
            generate_dbt_project_yml(
                dbt_dir,
                cfg.get("dbt_project_name",  "dv_snowflake"),
                cfg.get("dbt_profile_name",  "dv_snowflake"),
                cfg.get("dv_schema_raw",     "RAW_VAULT"),
                cfg.get("dv_schema_biz",     "BUSINESS_VAULT"),
                cfg.get("dv_schema_stg",     "STAGING"),
                cfg.get("hash_algorithm",    "MD5"),
                cfg.get("load_date_col",     "LOAD_DATE"),
                cfg.get("record_source",     "RECORD_SOURCE"),
            )
            generate_profiles_yml(dbt_dir, cfg.get("dbt_profile_name", "dv_snowflake"), "snowflake")
            generate_sources_yml(dbt_dir, state.metadata)
            generate_schema_yml(dbt_dir, state.dv_model)

            generated = {"hubs": [], "links": [], "satellites": []}
            for hub in state.dv_model.get("hubs", []):
                p = generate_hub_model(dbt_dir, hub)
                generated["hubs"].append(p)
            for lnk in state.dv_model.get("links", []):
                p = generate_link_model(dbt_dir, lnk)
                generated["links"].append(p)
            for sat in state.dv_model.get("satellites", []):
                p = generate_satellite_model(dbt_dir, sat)
                generated["satellites"].append(p)

            state.dbt_files = generated
            return generated

        self.steps.append(AgentStep("agent4", "DBT Project Generator (datavault4dbt v1.17.0)",
                                    run_agent4, depends_on=["agent3"]))

        # ── Agent 5: Profiler + PII ───────────────────────────────────────────
        def run_agent5(state: PipelineState) -> dict:
            from etl_pipeline.agents.agent_profiler import ProfilerAgent
            if state.source_df is None:
                # Build synthetic DataFrame from metadata for demo
                import numpy as np
                from faker import Faker
                fake = Faker(); Faker.seed(42)
                n = 200
                state.source_df = pd.DataFrame({
                    "CUSTOMER_ID":  range(1001, 1001 + n),
                    "FIRST_NAME":   [fake.first_name() for _ in range(n)],
                    "LAST_NAME":    [fake.last_name()  for _ in range(n)],
                    "EMAIL":        [fake.email()       for _ in range(n)],
                    "PHONE":        [fake.phone_number() for _ in range(n)],
                    "ORDER_AMOUNT": list(np.random.uniform(10, 5000, n).round(2)),
                    "STATUS":       list(np.random.choice(["ACTIVE","INACTIVE","PENDING"], n)),
                })
            agent  = ProfilerAgent()
            result = agent.run(
                df           = state.source_df,
                dataset_name = "pipeline_source",
                output_dir   = state.get_subdir("profiling"),
            )
            state.profile = result
            return result

        self.steps.append(AgentStep("agent5", "Data Profiler + PII Detection",
                                    run_agent5, depends_on=["agent1"]))

        # ── Agent 6: Reconciliation ───────────────────────────────────────────
        def run_agent6(state: PipelineState) -> dict:
            from etl_pipeline.agents.agent_reconciliation import (
                check_row_counts, check_missing_keys, check_aggregates,
                generate_reconciliation_report,
            )
            if state.source_df is None or state.target_df is None:
                # Skip gracefully — need both DataFrames
                print("     ℹ️  No source/target DataFrames provided — using demo split")
                state.target_df = state.source_df.head(int(len(state.source_df) * 0.95)) \
                    if state.source_df is not None else pd.DataFrame()
                if state.source_df is None:
                    return {"skipped": True, "reason": "no source DataFrame"}

            key_col  = cfg.get("primary_key_col", "CUSTOMER_ID")
            num_cols = [c for c in state.source_df.columns
                        if state.source_df[c].dtype in ["float64","int64"]
                        and c != key_col][:3]

            checks = [check_row_counts(state.source_df, state.target_df)]
            if key_col in state.source_df.columns:
                checks.append(check_missing_keys(state.source_df, state.target_df, key_col))
            if num_cols:
                checks.extend(check_aggregates(state.source_df, state.target_df, num_cols))

            recon_dir = state.get_subdir("reconciliation")
            summary   = generate_reconciliation_report(checks, recon_dir)
            state.recon = summary
            return summary

        self.steps.append(AgentStep("agent6", "Data Reconciliation",
                                    run_agent6, depends_on=["agent5"]))

        # ── Agent 7: Data Quality ─────────────────────────────────────────────
        def run_agent7(state: PipelineState) -> dict:
            from etl_pipeline.agents.agent_data_quality import (
                generate_dq_rules, evaluate_dq_rules, generate_dq_report,
            )
            if state.source_df is None:
                return {"skipped": True, "reason": "no source DataFrame"}

            dbt_dir = state.get_dbt_output_dir()
            os.makedirs(os.path.join(dbt_dir, "models", "raw_vault"), exist_ok=True)

            rules    = generate_dq_rules(state.source_df, state.metadata, state.sttm, state.dv_model)
            evaluated= evaluate_dq_rules(state.source_df, rules)
            summary  = generate_dq_report(evaluated, state.get_subdir("data_quality"))
            state.dq = evaluated
            return summary

        self.steps.append(AgentStep("agent7", "Data Quality Rule Engine",
                                    run_agent7, depends_on=["agent6"]))

        # ── Agent 8: Mermaid ER + FIBO/BIAN ──────────────────────────────────
        def run_agent8(state: PipelineState) -> dict:
            # Build normalized dv_model keys for agent_mermaid_er
            raw_dv   = state.dv_model
            norm_dv  = {
                "staging":    [],
                "hubs":       [{
                    "table_name": h["hub_name"],
                    "entity":     h["entity"],
                    "business_key": h["business_keys"][0] if h.get("business_keys") else "",
                    "hash_key":   h["hash_key"],
                    "load_date_col": h.get("ldts", "LOAD_DATE"),
                    "record_src_col": h.get("rsrc", "RECORD_SOURCE"),
                } for h in raw_dv.get("hubs", [])],
                "links":      [{
                    "table_name":  l["link_name"],
                    "entities":    [l["entity_a"], l["entity_b"]],
                    "hash_key":    l["hash_key"],
                    "fk_hash_keys":[l["hub_a_hk"], l["hub_b_hk"]],
                } for l in raw_dv.get("links", [])],
                "satellites": [{
                    "table_name":       s["sat_name"],
                    "parent_hub":       f"HUB_{s['entity']}",
                    "hash_key":         s["hub_hash_key"],
                    "hash_diff":        s["hashdiff"],
                    "descriptive_cols": [a["name"] for a in s.get("attributes", [])],
                    "load_date_col":    s.get("ldts", "LOAD_DATE"),
                    "record_src_col":   s.get("rsrc", "RECORD_SOURCE"),
                } for s in raw_dv.get("satellites", [])],
            }

            # Build minimal mappings list
            mappings = []
            for src in state.metadata.get("sources", []):
                mappings.append({
                    "mapping_name":  f"m_{src['table_name'].lower()}",
                    "tool":          src.get("_tool", "unknown"),
                    "source_table":  src["table_name"],
                    "target_table":  f"TGT_{src['table_name'].replace('SRC_','').replace('src_','')}",
                    "load_strategy": "FULL",
                    "columns": [
                        {"src": c["name"], "tgt": c["name"], "dtype": c.get("data_type","VARCHAR"),
                         "pk":  c.get("key_type","") == "PRIMARY KEY",
                         "nullable": c.get("nullable","Y") != "N",
                         "transformation": "DIRECT"}
                        for c in src.get("columns", [])
                    ]
                })

            # Dynamic import of agent_mermaid_er
            import importlib.util
            candidates = [
                Path(__file__).parent / "agent_mermaid_er.py",
                Path(__file__).parent.parent.parent / "agent_mermaid_er.py",
            ]
            mod = None
            for c in candidates:
                if c.exists():
                    spec = importlib.util.spec_from_file_location("agent_mermaid_er", c)
                    mod  = importlib.util.module_from_spec(spec)
                    spec.loader.exec_module(mod)
                    break

            if mod is None:
                raise ImportError("agent_mermaid_er.py not found in agents directory")

            result = mod.run_agent8_mermaid_er(
                mappings   = mappings,
                dv_model   = norm_dv,
                output_dir = Path(state.output_dir),
            )
            state.mermaid_diagrams = result
            return result

        self.steps.append(AgentStep("agent8", "Mermaid ER + FIBO/BIAN Alignment",
                                    run_agent8, depends_on=["agent3"]))

    # ─────────────────────────────────────────────────────────────────────────
    # Report generation
    # ─────────────────────────────────────────────────────────────────────────

    def _build_report(self, output_dir: str) -> dict:
        """Build structured run report from all step results."""
        total   = len(self.steps)
        success = sum(1 for s in self.steps if s.status == "SUCCESS")
        failed  = sum(1 for s in self.steps if s.status == "FAILED")
        skipped = sum(1 for s in self.steps if s.status == "SKIPPED")
        total_t = sum(s.duration_s for s in self.steps)

        report = {
            "run_timestamp":  self.run_ts,
            "author":         "gokulram.krishnan",
            "repository":     "https://github.com/gokulramkrishnan/ETL-Pipeline-Code",
            "output_dir":     output_dir,
            "summary": {
                "total_steps":   total,
                "success":       success,
                "failed":        failed,
                "skipped":       skipped,
                "total_time_s":  round(total_t, 3),
                "pass_rate_pct": round(success / total * 100, 1) if total else 0,
                "status":        "SUCCESS" if failed == 0 else "PARTIAL" if success > 0 else "FAILED",
            },
            "steps": [s.to_dict() for s in self.steps],
            "outputs": {
                "legacy_scan":    bool(self.state and self.state.legacy_scan),
                "metadata":       bool(self.state and self.state.metadata),
                "sttm":           bool(self.state and self.state.sttm),
                "dv_model":       bool(self.state and self.state.dv_model),
                "dbt_files":      bool(self.state and self.state.dbt_files),
                "profiling":      bool(self.state and self.state.profile),
                "reconciliation": bool(self.state and self.state.recon),
                "data_quality":   self.state.dq is not None if self.state else False,
                "mermaid_er":     bool(self.state and self.state.mermaid_diagrams),
            },
        }

        # Legacy scan summary
        if self.state and self.state.legacy_scan:
            summary = self.state.legacy_scan.get("summary", {})
            report["legacy_scan_summary"] = {
                "total_files":     summary.get("total_files", 0),
                "platforms":       summary.get("platform_counts", {}),
                "patterns":        summary.get("pattern_counts", {}),
                "formats":         summary.get("format_counts", {}),
                "total_objects":   summary.get("total_objects", 0),
                "complexity_dist": summary.get("complexity_distribution", {}),
                "top_recs":        summary.get("unique_recommendations", [])[:5],
            }

        # DV model summary
        if self.state and self.state.dv_model:
            report["dv_model_summary"] = {
                "hubs":       len(self.state.dv_model.get("hubs", [])),
                "links":      len(self.state.dv_model.get("links", [])),
                "satellites": len(self.state.dv_model.get("satellites", [])),
            }

        return report

    def _write_report(self, report: dict, output_dir: str):
        """Write run report as JSON and HTML."""
        # JSON
        json_path = Path(output_dir) / "orchestrator_run_report.json"
        with open(json_path, "w") as f:
            json.dump(report, f, indent=2)

        # HTML
        html_path = Path(output_dir) / "orchestrator_run_report.html"
        html_path.write_text(self._render_html_report(report))

        print(f"\n  📄 Run report → {json_path}")
        print(f"  📄 HTML report → {html_path}")

    def _render_html_report(self, report: dict) -> str:
        """Render a styled HTML run report."""
        summary = report["summary"]
        steps   = report["steps"]

        step_rows = ""
        for step in steps:
            icon  = "✅" if step["status"]=="SUCCESS" else ("⏭" if step["status"]=="SKIPPED" else "❌")
            color = "#eafaf1" if step["status"]=="SUCCESS" else ("#fff3e0" if step["status"]=="SKIPPED" else "#fdecea")
            badge = "#27ae60" if step["status"]=="SUCCESS" else ("#e67e22" if step["status"]=="SKIPPED" else "#e74c3c")
            deps  = ", ".join(step["depends_on"]) or "—"
            err   = f'<div style="font-size:10px;color:#c0392b;margin-top:4px">{step["error"][:200]}</div>' if step.get("error") else ""
            step_rows += f"""
            <tr style="background:{color}">
              <td style="padding:8px 12px">{icon} <span style="background:{badge};color:white;padding:2px 6px;border-radius:3px;font-size:11px">{step["status"]}</span></td>
              <td style="padding:8px 12px;font-weight:500">{step["agent_id"]}</td>
              <td style="padding:8px 12px">{step["name"]}</td>
              <td style="padding:8px 12px;text-align:right">{step["duration_s"]:.3f}s</td>
              <td style="padding:8px 12px;color:#888">{deps}</td>
            </tr>
            {'<tr style="background:' + color + '"><td colspan="5" style="padding:2px 24px 8px">' + err + '</td></tr>' if err else ''}
            """

        legacy_html = ""
        if "legacy_scan_summary" in report:
            ls  = report["legacy_scan_summary"]
            plat = " | ".join(f"{k}: {v}" for k,v in ls.get("platforms",{}).items())
            patt = " | ".join(f"{k}: {v}" for k,v in ls.get("patterns",{}).items())
            fmt  = " | ".join(f"{k}: {v}" for k,v in ls.get("formats",{}).items())
            legacy_html = f"""
            <div style="background:white;border-radius:8px;padding:20px;margin-bottom:20px;box-shadow:0 1px 3px rgba(0,0,0,.1)">
              <h2 style="margin-top:0;color:#2c3e50">🔍 Legacy System Scan Summary</h2>
              <table style="width:100%;border-collapse:collapse;font-size:13px">
                <tr><td style="padding:4px 12px;font-weight:bold;width:200px">Files Scanned</td><td>{ls.get('total_files',0)}</td></tr>
                <tr style="background:#f9f9f9"><td style="padding:4px 12px;font-weight:bold">Platforms</td><td>{plat or '—'}</td></tr>
                <tr><td style="padding:4px 12px;font-weight:bold">Patterns</td><td>{patt or '—'}</td></tr>
                <tr style="background:#f9f9f9"><td style="padding:4px 12px;font-weight:bold">File Formats</td><td>{fmt or '—'}</td></tr>
                <tr><td style="padding:4px 12px;font-weight:bold">Total Objects</td><td>{ls.get('total_objects',0)}</td></tr>
              </table>
              <div style="margin-top:12px"><strong>Top Recommendations:</strong>
              <ul style="font-size:12px;color:#555">{"".join(f"<li>{r}</li>" for r in ls.get('top_recs',[]))}</ul></div>
            </div>"""

        dv_html = ""
        if "dv_model_summary" in report:
            dv = report["dv_model_summary"]
            dv_html = f"""
            <div style="background:white;border-radius:8px;padding:20px;margin-bottom:20px;box-shadow:0 1px 3px rgba(0,0,0,.1)">
              <h2 style="margin-top:0;color:#2c3e50">🏗️ Data Vault 2.0 Model</h2>
              <div style="display:flex;gap:16px">
                {"".join(f'<div style="background:{"#1E4FA3" if i==0 else "#0E9AA7" if i==1 else "#C9A84C"};color:white;padding:16px 24px;border-radius:8px;text-align:center"><div style="font-size:28px;font-weight:700">{v}</div><div style="font-size:11px;margin-top:4px">{k.upper()}S</div></div>' for i,(k,v) in enumerate(dv.items()))}
              </div>
            </div>"""

        status_color = "#27ae60" if summary["status"]=="SUCCESS" else ("#e67e22" if summary["status"]=="PARTIAL" else "#e74c3c")

        return f"""<!DOCTYPE html>
<html lang="en"><head><meta charset="UTF-8">
<title>Pipeline Orchestrator Run Report — gokulram.krishnan</title>
<style>
body{{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;margin:0;background:#f0f2f5;color:#333}}
.c{{max-width:1100px;margin:0 auto;padding:24px}}
h1{{margin:0 0 4px;color:#0D1B3E}} h2{{color:#2c3e50}}
.meta{{color:#7f8c8d;font-size:13px;margin-bottom:20px}}
.sc{{display:flex;gap:14px;margin-bottom:20px;flex-wrap:wrap}}
.card{{background:white;border-radius:8px;padding:16px 22px;flex:1;min-width:100px;box-shadow:0 1px 3px rgba(0,0,0,.1);text-align:center}}
.card .n{{font-size:32px;font-weight:700;line-height:1}} .card .l{{font-size:11px;color:#7f8c8d;margin-top:4px}}
.pass .n{{color:#27ae60}} .fail .n{{color:#e74c3c}} .skip .n{{color:#e67e22}} .tot .n{{color:#2c3e50}} .pct .n{{color:#3498db}}
table{{width:100%;border-collapse:collapse}} th{{background:#0D1B3E;color:white;padding:10px 12px;text-align:left;font-size:12px}}
td{{border-bottom:1px solid #eee;font-size:12px}}
.s{{background:white;border-radius:8px;padding:20px;margin-bottom:20px;box-shadow:0 1px 3px rgba(0,0,0,.1)}}
</style></head><body><div class="c">
<h1>🎛️ Pipeline Orchestrator — Run Report</h1>
<div class="meta">
  Author: <b>gokulram.krishnan</b> | Repository: <a href="https://github.com/gokulramkrishnan/ETL-Pipeline-Code">github.com/gokulramkrishnan/ETL-Pipeline-Code</a> |
  Run: {report['run_timestamp']} UTC | Output: {report['output_dir']}
</div>
<div style="background:{status_color};color:white;padding:10px 16px;border-radius:6px;margin-bottom:20px;font-weight:bold;font-size:14px">
  Pipeline Status: {summary['status']} — {summary['pass_rate_pct']}% success rate | {summary['total_time_s']}s total
</div>
<div class="sc">
  <div class="card tot"><div class="n">{summary['total_steps']}</div><div class="l">Total Steps</div></div>
  <div class="card pass"><div class="n">{summary['success']}</div><div class="l">Success ✅</div></div>
  <div class="card fail"><div class="n">{summary['failed']}</div><div class="l">Failed ❌</div></div>
  <div class="card skip"><div class="n">{summary['skipped']}</div><div class="l">Skipped ⏭</div></div>
  <div class="card pct"><div class="n">{summary['pass_rate_pct']}%</div><div class="l">Pass Rate</div></div>
</div>
{legacy_html}
{dv_html}
<div class="s"><h2 style="margin-top:0">📋 Step Execution Details</h2>
<table><tr><th>Status</th><th>Agent</th><th>Name</th><th>Duration</th><th>Depends On</th></tr>
{step_rows}</table></div>
<div style="text-align:center;color:#bbb;font-size:11px;margin-top:16px">
  ETL Pipeline Orchestrator — gokulram.krishnan — {report['run_timestamp']} UTC
</div></div></body></html>"""
