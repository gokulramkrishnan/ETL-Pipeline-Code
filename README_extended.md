# RE Extended — Reverse Engineer for DataStage XML · SQL · VQL · ATL · VW

**Author:** gokulram.krishnan  
**Part of:** [ETL-Pipeline-Code](https://github.com/gokulramkrishnan/ETL-Pipeline-Code)

Standalone reverse-engineer agent that parses **DataStage XML, SQL, VQL (Denodo/Vertica), ATL (Qlik/Attunity), and VW (view DDL)** files and produces a complete documentation and pipeline package — with no mandatory API keys and no heavy dependencies.

---

## What it produces

| Output | File | Description |
|--------|------|-------------|
| BRD | `BRD.md` | Business Requirements Document with scope, business rules, stakeholder table |
| TRD | `TRD.md` | Technical Requirements Document with schema, mapping table, constraints |
| STTM | `STTM.xlsx` | Source-to-Target Mapping (5 sheets: All Mappings, Table Inventory, Column Schema, Views, Summary) |
| dbt project | `dbt/` | Ready-to-run dbt project — staging views, Hub and Satellite models, `packages.yml`, `profiles.yml`, `schema.yml` |
| Lineage | `lineage.json` + `lineage.mmd` | Column-level lineage graph as JSON and Mermaid flowchart |
| Confidence matrix | `confidence_matrix.xlsx` | Optional: compare this output vs other tools |

---

## Supported input formats

| Extension | Format | Parser details |
|-----------|--------|----------------|
| `.xml` | DataStage DSX/XML | Parses `<Job>`, `<Stage>`, `<Column>`, `<Link>` elements |
| `.sql` | Standard SQL | `CREATE TABLE`, `CREATE VIEW`, `INSERT INTO ... SELECT` |
| `.vql` | Denodo / Vertica VQL | `CREATE BASE VIEW`, `CREATE VIEW ... AS SELECT` + data source definitions |
| `.atl` | Qlik Replicate (JSON) / Attunity (XML) | Table rules, column definitions, replication expressions |
| `.vw` | View DDL | Single `CREATE VIEW` statement — treated as a named view |

---

## Installation

```bash
# 1. Clone (or copy the re_extended/ folder into your existing project)
git clone https://github.com/gokulramkrishnan/ETL-Pipeline-Code.git
cd ETL-Pipeline-Code

# 2. Install dependencies (minimal — just openpyxl for Excel output)
pip install -r requirements_extended.txt

# 3. Optional: install dbt if you want to run the generated dbt project
pip install dbt-snowflake
```

### Python version
Python 3.9+ required. No NumPy, no Pandas in the core agent.

---

## Running from the command line

```bash
# Basic — all five file types, no LLM
python etl_pipeline/agents/agent_re_extended.py \
  sample_inputs/customer_job.xml \
  sample_inputs/orders.sql \
  sample_inputs/products.vql \
  sample_inputs/replication.atl \
  sample_inputs/customer_orders.vw \
  --out output/re_extended \
  --name my_project

# With LLM enrichment (Groq free tier — enriches BRD and TRD with AI-generated paragraphs)
export GROQ_API_KEY=gsk_...
python etl_pipeline/agents/agent_re_extended.py \
  my_files/*.xml my_files/*.sql \
  --out output/enriched

# Fully offline (no API calls at all)
python etl_pipeline/agents/agent_re_extended.py \
  my_files/*.sql --out output/offline --no-llm
```

**All CLI flags:**

| Flag | Default | Description |
|------|---------|-------------|
| `files` (positional) | — | One or more `.xml`, `.sql`, `.vql`, `.atl`, `.vw` files |
| `--out` | `output/re_extended` | Output directory |
| `--name` | `re_pipeline` | dbt project name |
| `--no-llm` | off | Disable LLM enrichment (fully deterministic, offline) |

---

## Running from VS Code

### Prerequisites

1. Install the Python extension: `ms-python.python`
2. Install recommended extensions (VS Code will prompt when you open the folder):
   - **dbt Power User** — run dbt models from VS Code
   - **Rainbow CSV** — preview STTM CSV
   - **Mermaid Preview** — render `lineage.mmd` in-editor
   - **YAML** — syntax highlighting for dbt YAML

### Step-by-step

**Step 1 — Open the project**

```
File → Open Folder → select ETL-Pipeline-Code/
```

**Step 2 — Select Python interpreter**

```
Ctrl+Shift+P → Python: Select Interpreter → choose your venv
```

**Step 3 — Run the agent (pre-configured launch)**

Press `F5` or go to **Run → Start Debugging** and pick one of these launch configs from `.vscode/launch.json`:

| Config name | What it does |
|-------------|-------------|
| `RE Extended — Run on sample inputs` | Runs all 5 sample files, no LLM, writes to `output/re_extended/` |
| `RE Extended — Run with LLM (Groq)` | Same but with Groq LLM enrichment (set `GROQ_API_KEY` first) |
| `RE Extended — No LLM (fully offline)` | XML + SQL only, no API calls |
| `Confidence Matrix — Compare vs reference` | Runs comparator (requires a reference JSON) |

**Step 4 — Point at your own files**

Edit `.vscode/launch.json`, find the `"args"` array and replace the sample paths:

```json
"args": [
  "path/to/your/job.xml",
  "path/to/your/transforms.sql",
  "--out", "output/my_run",
  "--name", "my_project"
]
```

**Step 5 — View outputs**

After the run finishes, outputs appear in `output/re_extended/`:

| File | How to view |
|------|-------------|
| `BRD.md`, `TRD.md` | Open in VS Code (Markdown preview: `Ctrl+Shift+V`) |
| `STTM.xlsx` | Download and open in Excel / LibreOffice |
| `dbt/` | Open in dbt Power User sidebar |
| `lineage.mmd` | Open → `Ctrl+Shift+P` → "Mermaid: Preview" |
| `lineage.json` | Open in VS Code — JSON with folding |

**Step 6 — Run generated dbt project**

```bash
cd output/re_extended/dbt
dbt deps
# Edit profiles.yml with your Snowflake credentials, then:
dbt run --select staging
dbt run --select raw_vault
dbt test
```

---

## Free LLM enrichment options

The agent enriches BRD and TRD sections with AI-generated paragraphs when an LLM is available. All options are free.

### Option A — Groq (recommended, fastest)

Free tier: 6,000 tokens/min on `llama3-8b-8192`. No credit card required.

1. Sign up at [console.groq.com](https://console.groq.com) — free
2. Create an API key
3. Set the environment variable:

```bash
# Mac/Linux
export GROQ_API_KEY=gsk_...

# Windows
set GROQ_API_KEY=gsk_...

# VS Code — add to .vscode/launch.json env section:
"env": { "GROQ_API_KEY": "gsk_..." }
```

No Python package needed — the agent calls Groq via `urllib.request`.

### Option B — Ollama (fully offline)

Runs locally on your laptop/server. Zero cost, zero data sent externally.

```bash
# 1. Install Ollama
# Mac:    brew install ollama
# Linux:  curl -fsSL https://ollama.ai/install.sh | sh
# Win:    download from https://ollama.ai

# 2. Pull the model (one time, ~4.7GB)
ollama pull llama3

# 3. Start the server (it auto-starts on Mac/Win)
ollama serve

# 4. Tell the agent where to find it
export OLLAMA_URL=http://localhost:11434
```

### Option C — No LLM

Pass `--no-llm` or set neither env var. The BRD and TRD are generated from deterministic templates — no quality loss on the structured sections (tables, mappings, schema).

---

## Confidence matrix

Compare this agent's output against Informatica, hand-crafted STTM, or any other tool that exports JSON in the same schema.

```bash
# Generate reference JSON from the existing ReverseEngineerAgent first:
python -c "
from etl_pipeline.agents import ReverseEngineerAgent
import json
meta = ReverseEngineerAgent().run(informatica_path='my_map.xml')
with open('reference/informatica_output.json','w') as f:
    json.dump(meta, f, indent=2, default=str)
"

# Then compare:
python tools/compare_outputs.py \
  --our   output/re_extended/unified_metadata.json \
  --ref   reference/informatica_output.json  "Informatica" \
  --ref   reference/manual_sttm.json         "Manual STTM" \
  --out   output/confidence_matrix.xlsx
```

**Scoring dimensions and weights:**

| Dimension | Weight | Measures |
|-----------|--------|---------|
| Table Coverage | 25% | F1 score on table names found vs reference |
| Column Coverage | 25% | Avg F1 per table on column names |
| Mapping Accuracy | 30% | F1 on (src_table, src_col, tgt_table, tgt_col) tuples + expression match % |
| View Detection | 10% | F1 score on view names |
| Lineage Depth | 10% | F1 on table-level lineage hops |

Overall confidence is a weighted F1 score expressed as a percentage. 80%+ = high confidence. 50–79% = review recommended. Below 50% = manual validation required.

---

## Using programmatically

```python
from etl_pipeline.agents.agent_re_extended import REExtendedAgent

result = REExtendedAgent().run(
    input_files  = ["job.xml", "transforms.sql", "views.vw"],
    output_dir   = "output/my_run",
    use_llm      = True,     # False = fully offline
    project_name = "my_dbt_project",
)

print(result["summary"])
# {'tables': 8, 'views': 3, 'mappings': 13,
#  'dbt_staging': 8, 'dbt_hubs': 4, 'dbt_sats': 4}

print(result["files"])
# {'brd': '...BRD.md', 'trd': '...TRD.md', 'sttm': '...STTM.xlsx',
#  'dbt': '.../dbt', 'lineage': '...lineage.json', 'metadata': '...unified_metadata.json'}
```

---

## File structure

```
re_extended/
│
├── etl_pipeline/agents/
│   └── agent_re_extended.py    ← main agent (standalone, ~550 lines)
│
├── tools/
│   └── compare_outputs.py      ← confidence matrix comparator (~300 lines)
│
├── sample_inputs/
│   ├── customer_job.xml         DataStage export sample
│   ├── orders.sql               SQL DDL + INSERT sample
│   ├── products.vql             Denodo VQL sample
│   ├── replication.atl          Qlik ATL (JSON) sample
│   └── customer_orders.vw       View DDL sample
│
├── .vscode/
│   ├── launch.json              4 pre-configured debug/run configs
│   └── extensions.json          Recommended VS Code extensions
│
├── requirements_extended.txt    Minimal dependencies (openpyxl + optional dbt)
└── README_extended.md           This file
```

---

## Design decisions

**Why no LangChain / heavy agentic framework?**  
LangChain adds 100MB+ of dependencies and introduces version-conflict risk. The file parsing and output generation here are deterministic — LLM is only used for narrative enrichment in BRD/TRD. Direct `urllib.request` calls to Groq and Ollama are zero-dependency and faster.

**Why stdlib for parsing instead of sqlparse/antlr?**  
`sqlparse` misses DataStage-specific derivation syntax, VQL base-view declarations, and ATL JSON structures. The regex + XML parsers here are purpose-built and tested against the actual formats.

**Why openpyxl and nothing else for core output?**  
The entire agent runs with just `openpyxl` installed (and even that degrades to CSV if absent). This makes it usable in restricted enterprise environments without pip access.

---

## Changelog

| Version | Date | Change |
|---------|------|--------|
| 1.0.0 | 2026-04-09 | Initial release — XML, SQL, VQL, ATL, VW parsers; BRD, TRD, STTM, dbt, lineage outputs; confidence matrix |
