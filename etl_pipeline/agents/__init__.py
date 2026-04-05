# =============================================================================
# Author      : gokulram.krishnan
# Repository  : https://github.com/gokulramkrishnan/ETL-Pipeline-Code
# Description : ETL Pipeline agents package — exposes all agent classes
#               and standalone functions for direct import.
#
# Agent Map:
#   Agent 0 — LegacyScannerAgent        (legacy system discovery)
#   Agent 1 — ReverseEngineerAgent       (DataStage + Informatica parsing)
#   Agent 2 — STTMAgent                  (Source-to-Target Mapping)
#   Agent 3 — DataVaultAgent             (DV2.0 Hub/Link/Satellite)
#   Agent 4 — DBTGeneratorAgent          (dbt project SQL + YAML)
#   Agent 5 — ProfilerAgent              (ydata-profiling + Presidio PII)
#   Agent 6 — ReconciliationAgent        (row count, checksum, missing keys)
#   Agent 7 — DataQualityAgent           (DQ rule engine + dbt tests)
#   Agent 8 — run_agent8_mermaid_er      (Mermaid ER + FIBO/BIAN diagrams)
#   Agent 9 — OrchestratorAgent          (horizontal coordination layer)
# =============================================================================

# Agent 0 — Legacy System Scanner
from .agent_legacy_scanner import (
    LegacyScannerAgent,
    scan_file,
    scan_directory,
    detect_platform,
    detect_file_formats,
    detect_patterns,
    extract_objects,
    compute_complexity_score,
    recommend_reverse_eng_strategy,
)

# Agent 1 — Reverse Engineer
from .agent_reverse_engineer import (
    ReverseEngineerAgent,
    parse_datastage,
    parse_informatica,
    merge_metadata,
)

# Agent 2 — STTM Generator
from .agent_sttm import (
    STTMAgent,
    build_sttm,
)

# Agent 3 — Data Vault 2.0 Modeler
from .agent_data_vault import (
    DataVaultAgent,
    hub_name,
    link_name,
    sat_name,
    hash_key_col,
    infer_business_keys,
    infer_entity_name,
    build_hubs,
    build_links,
    build_satellites,
)

# Agent 4 — DBT Generator
from .agent_dbt_generator import (
    DBTGeneratorAgent,
    generate_packages_yml,
    generate_dbt_project_yml,
    generate_profiles_yml,
    generate_staging_model,
    generate_hub_model,
    generate_link_model,
    generate_satellite_model,
    generate_sources_yml,
    generate_schema_yml,
)

# Agent 5 — Data Profiler + PII
from .agent_profiler import (
    ProfilerAgent,
    run_ydata_profiling,
    run_pii_detection,
)

# Agent 6 — Reconciliation
from .agent_reconciliation import (
    ReconciliationAgent,
    check_row_counts,
    check_missing_keys,
    check_aggregates,
    check_value_mismatches,
    generate_reconciliation_report,
)

# Agent 7 — Data Quality
from .agent_data_quality import (
    DataQualityAgent,
    generate_dq_rules,
    evaluate_dq_rules,
    generate_dq_report,
)

# Agent 8 — Mermaid ER + FIBO/BIAN
from .agent_mermaid_er import (
    run_agent8_mermaid_er,
    build_mermaid_er,
    build_fibo_bian_alignment,
    match_fibo,
    match_bian,
)

# Agent 9 — Orchestrator (import last — depends on all others)
from .agent_orchestrator import (
    OrchestratorAgent,
    AgentStep,
    PipelineState,
)

__version__ = "1.1.0"
__author__  = "gokulram.krishnan"

__all__ = [
    # Agent 0
    "LegacyScannerAgent",
    "scan_file",
    "scan_directory",
    "detect_platform",
    "detect_file_formats",
    "detect_patterns",
    "extract_objects",
    "compute_complexity_score",
    "recommend_reverse_eng_strategy",
    # Agent 1
    "ReverseEngineerAgent",
    "parse_datastage",
    "parse_informatica",
    "merge_metadata",
    # Agent 2
    "STTMAgent",
    "build_sttm",
    # Agent 3
    "DataVaultAgent",
    "hub_name",
    "link_name",
    "sat_name",
    "hash_key_col",
    "infer_business_keys",
    "infer_entity_name",
    "build_hubs",
    "build_links",
    "build_satellites",
    # Agent 4
    "DBTGeneratorAgent",
    "generate_packages_yml",
    "generate_dbt_project_yml",
    "generate_profiles_yml",
    "generate_staging_model",
    "generate_hub_model",
    "generate_link_model",
    "generate_satellite_model",
    "generate_sources_yml",
    "generate_schema_yml",
    # Agent 5
    "ProfilerAgent",
    "run_ydata_profiling",
    "run_pii_detection",
    # Agent 6
    "ReconciliationAgent",
    "check_row_counts",
    "check_missing_keys",
    "check_aggregates",
    "check_value_mismatches",
    "generate_reconciliation_report",
    # Agent 7
    "DataQualityAgent",
    "generate_dq_rules",
    "evaluate_dq_rules",
    "generate_dq_report",
    # Agent 8
    "run_agent8_mermaid_er",
    "build_mermaid_er",
    "build_fibo_bian_alignment",
    "match_fibo",
    "match_bian",
    # Agent 9
    "OrchestratorAgent",
    "AgentStep",
    "PipelineState",
]
