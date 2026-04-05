# =============================================================================
# Author      : gokulram.krishnan
# Repository  : https://github.com/gokulramkrishnan/ETL-Pipeline-Code
# Description : Agent 0 — Legacy System Scanner
#               Scans legacy platforms (Netezza, Teradata, Oracle, SQL Server,
#               Mainframe, APIs) to discover objects, file formats, and data
#               movement patterns that need reverse engineering.
#
# Supported platforms  : Netezza | Teradata | Oracle | SQL Server | Mainframe | API
# File formats detected: CSV | TSV | JSON | BLOB | Parquet | XML | Fixed-Width
# Patterns detected    : Batch | Near-Realtime | Realtime
# =============================================================================

from __future__ import annotations

import json
import re
import os
from pathlib import Path
from datetime import datetime
from collections import defaultdict, Counter
from typing import Optional


# ─────────────────────────────────────────────────────────────────────────────
# Constants
# ─────────────────────────────────────────────────────────────────────────────

SUPPORTED_PLATFORMS = ["netezza", "teradata", "oracle", "sqlserver", "mainframe", "api"]

FILE_FORMAT_SIGNATURES = {
    "csv":         [r"\.csv$", r"DELIMITER\s*[=,]\s*['\"],", r"field_delimiter", r"ROW FORMAT.*CSV"],
    "tsv":         [r"\.tsv$", r"DELIMITER\s*[=,]\s*['\"]\\t", r"tab.delimited", r"field_delimiter.*\\t"],
    "json":        [r"\.json$", r"FORMAT\s*=\s*JSON", r"json_extract", r"parse_json", r"JSON_VALUE"],
    "blob":        [r"\.blob$", r"VARBINARY", r"BLOB\b", r"BYTEA", r"IMAGE\b", r"BINARY.*LOB"],
    "parquet":     [r"\.parquet$", r"FORMAT\s*=\s*PARQUET", r"STORED AS PARQUET"],
    "xml":         [r"\.xml$", r"XMLTYPE", r"FOR XML", r"xml_parse", r"XMLPARSE"],
    "fixed_width": [r"RECFM=F", r"LRECL=", r"fixed.width", r"positional.*layout", r"PIC\s+X\("],
    "avro":        [r"\.avro$", r"FORMAT\s*=\s*AVRO", r"STORED AS AVRO"],
}

PATTERN_SIGNATURES = {
    "realtime":        [r"KAFKA", r"KINESIS", r"STREAM", r"CDC\b", r"CHANGE.DATA.CAPTURE",
                        r"EVENT.*HUB", r"PUBSUB", r"REAL.?TIME", r"WEBSOCKET", r"MQTT"],
    "near_realtime":   [r"MICRO.?BATCH", r"SPARK.STREAMING", r"FLINK", r"INTERVAL.*MINUTE",
                        r"EVERY.*MINUTE", r"SCHEDULE.*\d+\s*MIN", r"REFRESH.*MINUTE",
                        r"NRT\b", r"NEAR.?REAL", r"FREQUENT.*LOAD"],
    "batch":           [r"CRON", r"SCHEDULE", r"DAILY", r"WEEKLY", r"MONTHLY", r"NIGHTLY",
                        r"BATCH", r"ETL.*JOB", r"JOB.*SCHEDULE", r"BULK.INSERT",
                        r"BTEQ", r"FASTLOAD", r"MULTILOAD", r"TPUMP"],
}

OBJECT_TYPE_PATTERNS = {
    "table":           [r"\bCREATE\s+TABLE\b", r"\bFROM\s+\w+\.\w+\b", r"\bJOIN\s+\w+\.\w+\b"],
    "view":            [r"\bCREATE\s+(OR\s+REPLACE\s+)?VIEW\b", r"\bFROM\s+V_\w+", r"\bVIEW\b"],
    "stored_procedure":[r"\bCREATE\s+(OR\s+REPLACE\s+)?PROCEDURE\b", r"\bEXEC\s+\w+\b", r"\bCALL\s+\w+\b"],
    "function":        [r"\bCREATE\s+(OR\s+REPLACE\s+)?FUNCTION\b", r"\bSELECT\s+\w+\("],
    "macro":           [r"\bCREATE\s+MACRO\b", r"\.COMPILE\s+MACRO"],   # Teradata
    "copybook":        [r"\bCOPY\s+\w+\b", r"INCLUDE\s+MEMBER", r"01\s+\w+"],  # Mainframe
    "jcl":             [r"//\w+\s+JOB\b", r"//\w+\s+EXEC\b", r"//\w+\s+DD\b"],  # Mainframe
    "api_endpoint":    [r"GET\s+/", r"POST\s+/", r"PUT\s+/", r"DELETE\s+/",
                        r"swagger", r"openapi", r"REST.*API", r"GraphQL"],
    "external_table":  [r"\bCREATE\s+EXTERNAL\s+TABLE\b", r"LOCATION\s*=", r"FORMAT\s*="],
    "sequence":        [r"\bCREATE\s+SEQUENCE\b", r"NEXTVAL", r"IDENTITY\s*\("],
}

# Platform-specific SQL dialect indicators
PLATFORM_DIALECT = {
    "netezza":    [r"NZ_\w+", r"GROOM TABLE", r"GENERATE STATISTICS", r"NZLOAD",
                   r"\.\.IBM Netezza", r"SYSTEM\.DEFINITION_SCHEMA", r"NZSQL"],
    "teradata":   [r"BTEQ", r"FASTLOAD", r"MULTILOAD", r"SEL\b", r"LOCKING\s+ROW",
                   r"COLLECT\s+STATISTICS", r"\.LOGON\b", r"TDMS", r"TDPID", r"DBC\."],
    "oracle":     [r"DBMS_\w+", r"NVL\b", r"DECODE\b", r"ROWNUM\b", r"SYSDATE\b",
                   r"V\$\w+", r"ALL_\w+", r"DBA_\w+", r"USER_\w+", r"TO_CHAR\b"],
    "sqlserver":  [r"GETDATE\(\)", r"ISNULL\b", r"TOP\s+\d+", r"NOLOCK", r"\[dbo\]\.",
                   r"sys\.\w+", r"DATEADD\b", r"DATEDIFF\b", r"CONVERT\b", r"@@\w+"],
    "mainframe":  [r"//\w+\s+JOB\b", r"COBOL\b", r"PIC\s+X\(", r"RECFM=", r"LRECL=",
                   r"EXEC\s+PGM=", r"DSNAME=", r"SYSOUT=", r"SORT FIELDS=", r"MVS\b"],
    "api":        [r"swagger", r"openapi", r"REST", r"GraphQL", r"WSDL", r"SOAP",
                   r"Bearer\s+token", r"OAuth", r"api.key", r"endpoint", r"JSON.Schema"],
}


# ─────────────────────────────────────────────────────────────────────────────
# Core detection functions
# ─────────────────────────────────────────────────────────────────────────────

def detect_platform(content: str, filename: str = "") -> list[str]:
    """Detect which legacy platform(s) a file or content belongs to."""
    detected = []
    content_upper = content.upper()
    fname_lower   = filename.lower()

    # File extension hints
    ext_hints = {
        ".bteq": "teradata", ".sql": None, ".ddl": None,
        ".jcl": "mainframe", ".cbl": "mainframe", ".cob": "mainframe",
        ".yaml": "api", ".json": "api", ".wsdl": "api",
    }
    ext = Path(fname_lower).suffix
    if ext in ext_hints and ext_hints[ext]:
        detected.append(ext_hints[ext])

    for platform, patterns in PLATFORM_DIALECT.items():
        score = sum(1 for p in patterns if re.search(p, content_upper, re.IGNORECASE))
        if score >= 1:
            detected.append(platform)

    return list(set(detected)) or ["unknown"]


def detect_file_formats(content: str) -> list[str]:
    """Detect file formats referenced in source code or config."""
    found = []
    for fmt, patterns in FILE_FORMAT_SIGNATURES.items():
        for p in patterns:
            if re.search(p, content, re.IGNORECASE):
                found.append(fmt)
                break
    return list(set(found)) or ["unknown"]


def detect_patterns(content: str, filename: str = "") -> list[str]:
    """Detect data movement patterns: batch, near-realtime, realtime."""
    found = []
    for pattern, sigs in PATTERN_SIGNATURES.items():
        for sig in sigs:
            if re.search(sig, content, re.IGNORECASE):
                found.append(pattern)
                break
    return list(set(found)) or ["batch"]  # default assumption is batch


def extract_objects(content: str, platform: str) -> dict[str, list[str]]:
    """
    Extract database objects referenced in the content.
    Returns dict: {object_type: [object_name, ...]}
    """
    objects: dict[str, list[str]] = defaultdict(list)

    # Generic object extraction by type
    for obj_type, patterns in OBJECT_TYPE_PATTERNS.items():
        for p in patterns:
            matches = re.findall(p, content, re.IGNORECASE)
            if matches:
                objects[obj_type].extend([str(m).strip() for m in matches])

    # Named object extraction — tables/views
    table_refs = re.findall(
        r"(?:FROM|JOIN|INTO|UPDATE|TABLE)\s+([\w\$#\.\"]+)",
        content, re.IGNORECASE
    )
    for ref in table_refs:
        clean = ref.strip("\"").strip()
        if clean and len(clean) > 1 and "(" not in clean:
            objects["table"].append(clean)

    # Procedure/function calls
    proc_refs = re.findall(
        r"(?:EXEC(?:UTE)?|CALL)\s+([\w\.]+)\s*(?:\(|$)",
        content, re.IGNORECASE
    )
    for ref in proc_refs:
        objects["stored_procedure"].append(ref.strip())

    # Deduplicate and count frequency
    result = {}
    for obj_type, names in objects.items():
        counted  = Counter(names)
        # Sort by frequency (most used first)
        result[obj_type] = [
            {"name": name, "frequency": count}
            for name, count in counted.most_common()
            if name.strip()
        ]

    return result


def compute_complexity_score(objects: dict, formats: list, patterns: list) -> dict:
    """
    Compute a complexity score for reverse engineering effort.
    Higher score = more effort required.
    """
    base_score = 0

    # Object complexity weights
    weights = {
        "stored_procedure": 8,
        "function":         6,
        "view":             4,
        "table":            2,
        "macro":            7,
        "jcl":              9,
        "copybook":         9,
        "api_endpoint":     5,
        "external_table":   3,
        "sequence":         1,
    }
    for obj_type, items in objects.items():
        count = len(items)
        w     = weights.get(obj_type, 3)
        base_score += count * w

    # Pattern multipliers
    if "realtime" in patterns:
        base_score *= 1.8
    elif "near_realtime" in patterns:
        base_score *= 1.4

    # Format complexity
    complex_formats = {"blob", "xml", "fixed_width", "avro"}
    for f in formats:
        if f in complex_formats:
            base_score += 15

    # Normalize to 1–100
    normalized = min(100, max(1, int(base_score / 5)))

    if normalized >= 70:
        level = "HIGH"
    elif normalized >= 40:
        level = "MEDIUM"
    else:
        level = "LOW"

    return {"score": normalized, "level": level}


def recommend_reverse_eng_strategy(
    platform: str, patterns: list, formats: list, objects: dict
) -> list[str]:
    """
    Generate actionable reverse engineering recommendations
    based on detected platform, patterns, and objects.
    """
    recs = []

    # Platform-specific
    strategy_map = {
        "netezza":   ["Export DDL using `nz_ddl` utility for all tables and views",
                      "Use GENERATE STATISTICS output to capture column cardinality",
                      "Extract NZLoad scripts to understand file-based ingestion patterns"],
        "teradata":  ["Export BTEQ scripts and FastLoad/MultiLoad definitions",
                      "Use DBC.TablesV and DBC.ColumnsV for full schema discovery",
                      "Capture COLLECT STATISTICS output for query optimization metadata"],
        "oracle":    ["Query ALL_TABLES, ALL_VIEWS, ALL_PROCEDURES for full object inventory",
                      "Export DBMS_METADATA.GET_DDL for stored procedures and packages",
                      "Capture V$SQL for frequently executed query patterns"],
        "sqlserver": ["Use sys.objects, sys.columns, sys.sql_modules for full inventory",
                      "Query sys.dm_exec_query_stats for frequently used objects",
                      "Export SQL Agent jobs to understand scheduling patterns"],
        "mainframe": ["Parse JCL EXEC and DD statements to map data flows",
                      "Extract COBOL COPY members (copybooks) for data layouts",
                      "Map RECFM/LRECL definitions to understand fixed-width formats"],
        "api":       ["Parse OpenAPI/Swagger YAML or WSDL files for endpoint inventory",
                      "Capture request/response JSON schemas for data model mapping",
                      "Document authentication patterns (OAuth, API key, mTLS)"],
    }
    recs.extend(strategy_map.get(platform, ["Perform manual DDL extraction and schema analysis"]))

    # Pattern-specific
    if "realtime" in patterns:
        recs.append("Map Kafka topics / event streams to understand message schemas and partitioning")
        recs.append("Document CDC capture points, LSN offsets, and replication lag thresholds")
    if "near_realtime" in patterns:
        recs.append("Capture micro-batch window sizes and trigger conditions for streaming jobs")
    if "batch" in patterns:
        recs.append("Export cron/scheduler definitions and job dependency chains")
        recs.append("Document batch window start/end times, SLAs, and failure alerting")

    # Format-specific
    for fmt in formats:
        if fmt == "fixed_width":
            recs.append("Convert fixed-width COBOL/Mainframe copybook definitions to column-width maps")
        elif fmt == "blob":
            recs.append("Document BLOB/VARBINARY columns — assess if content requires parsing (e.g. embedded XML/JSON)")
        elif fmt == "json":
            recs.append("Extract JSON schema from sample records; identify nested array/object paths")

    # High-frequency objects
    for obj_type, items in objects.items():
        top = [i["name"] for i in items[:3] if i["frequency"] > 1]
        if top:
            recs.append(f"Prioritize reverse engineering of high-frequency {obj_type}s: {', '.join(top)}")

    return recs


# ─────────────────────────────────────────────────────────────────────────────
# File scanner
# ─────────────────────────────────────────────────────────────────────────────

def scan_file(file_path: str) -> dict:
    """
    Scan a single file and return its legacy system profile.
    Handles: .sql, .ddl, .bteq, .jcl, .cbl, .cob, .yaml, .json, .xml, .txt
    """
    path    = Path(file_path)
    content = ""

    try:
        content = path.read_text(encoding="utf-8", errors="replace")
    except Exception as e:
        return {"file": str(path), "error": str(e), "scanned": False}

    platforms  = detect_platform(content, path.name)
    formats    = detect_file_formats(content)
    patterns   = detect_patterns(content, path.name)
    objects    = extract_objects(content, platforms[0] if platforms else "unknown")
    complexity = compute_complexity_score(objects, formats, patterns)

    total_objects = sum(len(v) for v in objects.values())

    return {
        "file":             str(path),
        "filename":         path.name,
        "extension":        path.suffix.lower(),
        "size_bytes":       path.stat().st_size,
        "scanned":          True,
        "platforms":        platforms,
        "file_formats":     formats,
        "data_patterns":    patterns,
        "objects":          objects,
        "total_objects":    total_objects,
        "complexity":       complexity,
        "recommendations":  recommend_reverse_eng_strategy(
                                platforms[0] if platforms else "unknown",
                                patterns, formats, objects
                            ),
        "scanned_at":       datetime.utcnow().isoformat(),
    }


def scan_directory(
    directory: str,
    extensions: Optional[list[str]] = None,
    recursive: bool = True,
) -> list[dict]:
    """
    Recursively scan a directory for all legacy system files.

    Args:
        directory:  Root path to scan
        extensions: File extensions to include (default: all supported)
        recursive:  Whether to scan sub-directories

    Returns:
        List of scan results, one per file.
    """
    if extensions is None:
        extensions = [".sql", ".ddl", ".bteq", ".jcl", ".cbl", ".cob",
                      ".yaml", ".yml", ".json", ".xml", ".txt", ".proc",
                      ".pkb", ".pks", ".fnc", ".trg", ".vw"]

    root   = Path(directory)
    glob   = "**/*" if recursive else "*"
    files  = [f for f in root.glob(glob) if f.is_file() and f.suffix.lower() in extensions]
    return [scan_file(str(f)) for f in sorted(files)]


# ─────────────────────────────────────────────────────────────────────────────
# Synthetic sample data generator (for demo / testing)
# ─────────────────────────────────────────────────────────────────────────────

def generate_sample_legacy_content() -> dict[str, str]:
    """Return synthetic content samples for each supported platform."""
    return {
        "netezza_customer_etl.sql": """
-- Netezza: Customer dimension load
-- NZSQL export
CREATE TABLE ANALYTICS.DIM_CUSTOMER (
    CUST_ID     INTEGER NOT NULL,
    CUST_NAME   VARCHAR(200),
    EMAIL       VARCHAR(255),
    COUNTRY_CD  CHAR(2)
) DISTRIBUTE ON (CUST_ID);

GENERATE STATISTICS ON ANALYTICS.DIM_CUSTOMER;
GROOM TABLE ANALYTICS.DIM_CUSTOMER;

INSERT INTO ANALYTICS.DIM_CUSTOMER
SELECT CUST_ID, UPPER(CUST_NAME), LOWER(EMAIL), COUNTRY_CD
FROM STAGING.SRC_CUSTOMER
WHERE LOAD_DT = CURRENT_DATE;
        """,
        "teradata_orders_bteq.sql": """
-- Teradata BTEQ script
.LOGON TDSERVER/myuser,mypass;

COLLECT STATISTICS ON DW.FACT_ORDER COLUMN (ORDER_ID);

LOCKING ROW FOR ACCESS
SELECT ORDER_ID, CUST_ID, ORDER_AMT, ORDER_DT
FROM DW.FACT_ORDER
WHERE ORDER_DT BETWEEN '2024-01-01' AND '2024-12-31';

.EXPORT FILE = orders_export.csv
.LOGOFF;
        """,
        "oracle_procedures.sql": """
-- Oracle PL/SQL stored procedures
CREATE OR REPLACE PROCEDURE usp_load_customer (
    p_run_date IN DATE DEFAULT SYSDATE
) AS
BEGIN
    DBMS_OUTPUT.PUT_LINE('Loading customer data...');
    INSERT INTO DW.DIM_CUSTOMER
    SELECT * FROM STAGING.SRC_CUSTOMER
    WHERE TRUNC(CREATE_DT) = TRUNC(p_run_date);
    COMMIT;
END;
/

CREATE OR REPLACE VIEW V_ACTIVE_CUSTOMERS AS
SELECT CUST_ID, CUST_NAME, EMAIL
FROM DW.DIM_CUSTOMER
WHERE STATUS_CD = 'ACTIVE'
  AND ROWNUM <= 1000;
        """,
        "sqlserver_batch_job.sql": """
-- SQL Server ETL job
USE DataWarehouse;
GO

-- Batch scheduled daily at 02:00 via SQL Agent
SELECT TOP 100 *
FROM [dbo].[FACT_SALES]
WITH (NOLOCK)
WHERE GETDATE() BETWEEN START_DT AND END_DT;

EXEC usp_refresh_summary @run_date = GETDATE();

BULK INSERT [dbo].[STAGING_ORDERS]
FROM 'D:\\data\\orders.csv'
WITH (FIELDTERMINATOR = ',', ROWTERMINATOR = '\n', FIRSTROW = 2);
        """,
        "mainframe_jcl_job.jcl": """
//CUSTLOAD JOB (ACCT),'CUSTOMER LOAD',CLASS=A,MSGCLASS=X
//STEP001  EXEC PGM=SORT
//SORTIN   DD   DSNAME=PROD.CUSTOMER.MASTER,DISP=SHR
//SORTOUT  DD   DSNAME=TEMP.CUSTOMER.SORTED,DISP=(NEW,CATLG,DELETE),
//              RECFM=FB,LRECL=200,BLKSIZE=20000
//SYSIN    DD   *
  SORT FIELDS=(1,10,CH,A)
//STEP002  EXEC PGM=CUSTPROC,PARM='LOAD'
//INPUT    DD   DSNAME=TEMP.CUSTOMER.SORTED,DISP=SHR
//OUTPUT   DD   SYSOUT=*
        """,
        "api_openapi_spec.yaml": """
openapi: "3.0.0"
info:
  title: Customer API
  version: "1.0"
paths:
  /customers:
    get:
      summary: List customers
      parameters:
        - name: country
          in: query
          schema:
            type: string
  /customers/{id}:
    get:
      summary: Get customer by ID
  /orders:
    post:
      summary: Create order
      requestBody:
        content:
          application/json:
            schema:
              $ref: '#/components/schemas/Order'
components:
  schemas:
    Order:
      type: object
      properties:
        customer_id: {type: integer}
        amount:      {type: number}
        status:      {type: string}
        """,
        "kafka_realtime_config.sql": """
-- Kafka CDC realtime stream configuration
-- CHANGE DATA CAPTURE from Oracle to Kafka
-- Topics: customer.updates, order.events
-- Real-time processing via Kafka Streams

CREATE STREAM customer_stream (
    CUST_ID    INTEGER,
    EVENT_TYPE VARCHAR(50),
    PAYLOAD    VARCHAR(MAX)
) WITH (KAFKA_TOPIC='customer.updates', FORMAT='JSON');

-- Near-realtime micro-batch: every 5 minutes
-- SCHEDULE EVERY 5 MINUTE
INSERT INTO DW.CUSTOMER_EVENTS
SELECT * FROM customer_stream
WHERE EVENT_TYPE IN ('INSERT', 'UPDATE', 'DELETE');
        """,
        "mixed_formats_etl.sql": """
-- Mixed file format ETL
-- Reads CSV, JSON, BLOB, XML sources

-- CSV source
COPY staging.customer_csv FROM 's3://datalake/customers/2024/*.csv'
DELIMITER ',' CSV HEADER;

-- JSON source
SELECT json_extract(payload, '$.customer_id') AS cust_id
FROM staging.events_json;

-- BLOB/binary assets
INSERT INTO media.documents (doc_id, content)
SELECT doc_id, CAST(file_content AS VARBINARY(MAX))
FROM source.binary_files;

-- XML transformation
SELECT XMLPARSE(DOCUMENT xml_col) AS parsed_xml
FROM legacy.xml_source;

-- Fixed-width file (COBOL-style)
-- RECFM=FB LRECL=250
-- PIC X(10) CUST-ID
-- PIC X(100) CUST-NAME
        """
    }


# ─────────────────────────────────────────────────────────────────────────────
# Agent class
# ─────────────────────────────────────────────────────────────────────────────

class LegacyScannerAgent:
    """
    Agent 0: Legacy System Scanner

    Scans legacy ETL systems — Netezza, Teradata, Oracle, SQL Server,
    Mainframe, and API — to discover:
      - Database objects (tables, views, procedures, functions, JCL, copybooks)
      - File formats (CSV, TSV, JSON, BLOB, Parquet, XML, Fixed-Width)
      - Data movement patterns (Batch, Near-Realtime, Realtime)
      - Complexity scores and reverse engineering recommendations

    All findings are returned as a structured inventory that feeds directly
    into Agent 1 (ReverseEngineerAgent) and the Orchestrator.
    """

    def __init__(self):
        self.scan_results  : list[dict]  = []
        self.summary       : dict        = {}
        self.scanned_at    : str         = ""

    def run(
        self,
        scan_paths:         Optional[list[str]] = None,
        inline_sources:     Optional[dict[str, str]] = None,
        use_synthetic_demo: bool = False,
        output_dir:         str  = "output/legacy_scan",
        extensions:         Optional[list[str]] = None,
        recursive:          bool = True,
    ) -> dict:
        """
        Main entry point for the legacy scanner.

        Args:
            scan_paths:         List of file paths or directories to scan
            inline_sources:     Dict of {filename: content} for in-memory scanning
            use_synthetic_demo: Use built-in synthetic samples if no real files provided
            output_dir:         Where to write scan results JSON
            extensions:         File extensions to include in directory scans
            recursive:          Scan sub-directories recursively

        Returns:
            Full scan inventory with summary, platform breakdown, object counts,
            file format distribution, pattern distribution, and recommendations.
        """
        self.scanned_at = datetime.utcnow().isoformat()
        self.scan_results = []
        os.makedirs(output_dir, exist_ok=True)

        print("  [Agent0] Legacy System Scanner starting...")

        # ── Determine source content ──────────────────────────────────────────
        sources: dict[str, str] = {}

        if use_synthetic_demo or (not scan_paths and not inline_sources):
            print("  [Agent0] Using synthetic demo content (no files provided)")
            sources = generate_sample_legacy_content()

        if inline_sources:
            sources.update(inline_sources)

        # Scan in-memory / inline sources
        import tempfile
        for fname, content in sources.items():
            tmp = Path(tempfile.mkdtemp()) / fname
            tmp.write_text(content, encoding="utf-8")
            result = scan_file(str(tmp))
            result["filename"] = fname  # restore original name
            self.scan_results.append(result)

        # Scan file paths / directories
        for path_str in (scan_paths or []):
            path = Path(path_str)
            if path.is_file():
                self.scan_results.append(scan_file(path_str))
            elif path.is_dir():
                self.scan_results.extend(scan_directory(path_str, extensions, recursive))
            else:
                print(f"  [Agent0] ⚠️  Path not found: {path_str}")

        print(f"  [Agent0] Scanned {len(self.scan_results)} source(s)")

        # ── Build summary ─────────────────────────────────────────────────────
        self.summary = self._build_summary()

        # ── Write outputs ─────────────────────────────────────────────────────
        inventory_path = Path(output_dir) / "legacy_inventory.json"
        summary_path   = Path(output_dir) / "legacy_summary.json"

        with open(inventory_path, "w") as f:
            json.dump(self.scan_results, f, indent=2)

        with open(summary_path, "w") as f:
            json.dump(self.summary, f, indent=2)

        print(f"  [Agent0] ✅ Inventory → {inventory_path}")
        print(f"  [Agent0] ✅ Summary   → {summary_path}")
        print(f"  [Agent0] Platforms : {self.summary['platform_counts']}")
        print(f"  [Agent0] Patterns  : {self.summary['pattern_counts']}")
        print(f"  [Agent0] Formats   : {self.summary['format_counts']}")
        print(f"  [Agent0] Objects   : {self.summary['total_objects']} across {self.summary['total_files']} file(s)")

        return {
            "scan_results": self.scan_results,
            "summary":      self.summary,
            "output_dir":   output_dir,
        }

    def _build_summary(self) -> dict:
        """Aggregate scan results into a consolidated summary."""
        platform_counts : Counter = Counter()
        pattern_counts  : Counter = Counter()
        format_counts   : Counter = Counter()
        object_type_counts: Counter = Counter()
        all_objects     : dict[str, list] = defaultdict(list)
        total_objects   = 0
        complexity_dist : Counter = Counter()
        all_recs        : list[str] = []

        for result in self.scan_results:
            if not result.get("scanned"):
                continue

            for p in result.get("platforms", []):
                platform_counts[p] += 1
            for pat in result.get("data_patterns", []):
                pattern_counts[pat] += 1
            for fmt in result.get("file_formats", []):
                format_counts[fmt] += 1

            complexity_dist[result.get("complexity", {}).get("level", "UNKNOWN")] += 1

            for obj_type, items in result.get("objects", {}).items():
                object_type_counts[obj_type] += len(items)
                total_objects += len(items)
                for item in items:
                    all_objects[obj_type].append(item)

            all_recs.extend(result.get("recommendations", []))

        # Top objects by frequency across all files
        top_objects: dict[str, list] = {}
        for obj_type, items in all_objects.items():
            freq_map: Counter = Counter()
            for item in items:
                freq_map[item["name"]] += item.get("frequency", 1)
            top_objects[obj_type] = [
                {"name": name, "total_frequency": count}
                for name, count in freq_map.most_common(10)
            ]

        # Deduplicate recommendations
        seen_recs = set()
        unique_recs = []
        for r in all_recs:
            key = r[:60]
            if key not in seen_recs:
                seen_recs.add(key)
                unique_recs.append(r)

        return {
            "total_files":       len(self.scan_results),
            "scanned_at":        self.scanned_at,
            "platform_counts":   dict(platform_counts.most_common()),
            "pattern_counts":    dict(pattern_counts.most_common()),
            "format_counts":     dict(format_counts.most_common()),
            "object_type_counts":dict(object_type_counts.most_common()),
            "total_objects":     total_objects,
            "complexity_distribution": dict(complexity_dist),
            "top_objects_by_type": top_objects,
            "unique_recommendations": unique_recs,
            "reverse_engineering_priority": self._prioritize_files(),
        }

    def _prioritize_files(self) -> list[dict]:
        """
        Rank files by reverse engineering priority:
        HIGH complexity + realtime pattern = highest priority.
        """
        scored = []
        for r in self.scan_results:
            if not r.get("scanned"):
                continue
            score   = r.get("complexity", {}).get("score", 0)
            level   = r.get("complexity", {}).get("level", "LOW")
            patterns= r.get("data_patterns", [])
            if "realtime" in patterns:
                score += 40
            elif "near_realtime" in patterns:
                score += 20
            scored.append({
                "file":        r.get("filename", r.get("file")),
                "platforms":   r.get("platforms", []),
                "patterns":    patterns,
                "formats":     r.get("file_formats", []),
                "complexity":  level,
                "priority_score": score,
            })
        return sorted(scored, key=lambda x: x["priority_score"], reverse=True)

    def get_reverse_eng_candidates(self) -> list[dict]:
        """
        Return files that should be fed to Agent 1 (ReverseEngineerAgent),
        filtered to DataStage-compatible or Informatica-compatible formats.
        Pure legacy SQL objects without an ETL tool wrapper are flagged
        as 'needs_custom_parser'.
        """
        candidates = []
        for r in self.scan_results:
            if not r.get("scanned"):
                continue
            platforms = r.get("platforms", [])
            entry = {
                "file":                r.get("file"),
                "filename":            r.get("filename"),
                "platforms":           platforms,
                "patterns":            r.get("data_patterns", []),
                "formats":             r.get("file_formats", []),
                "complexity":          r.get("complexity", {}),
                "needs_custom_parser": True,   # all legacy objects need the scanner
                "agent1_compatible":   False,  # flag: direct Agent1 parse possible
            }
            # DataStage / Informatica files can go directly to Agent 1
            ext = r.get("extension", "")
            if ext in (".dsx", ".ipc") or "datastage" in r.get("filename","").lower():
                entry["agent1_compatible"] = True
                entry["tool"] = "datastage"
            elif ext in (".xml",) and "informatica" in r.get("filename","").lower():
                entry["agent1_compatible"] = True
                entry["tool"] = "informatica"
            candidates.append(entry)
        return candidates
