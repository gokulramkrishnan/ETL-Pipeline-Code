#!/usr/bin/env python3
"""
run.py — Entry point for RE Extended Agent
Author  : gokulram.krishnan
Repository: https://github.com/gokulramkrishnan/ETL-Pipeline-Code

Usage examples
--------------
# All five sample files, no LLM
python run.py

# Your own files
python run.py --files path/to/job.xml path/to/transforms.sql --out output/my_run

# With Groq LLM enrichment
GROQ_API_KEY=gsk_... python run.py --files *.xml *.sql

# Confidence matrix
python run.py --compare \
    --our output/re_extended/unified_metadata.json \
    --ref  reference/informatica.json "Informatica" \
    --out  output/confidence_matrix.xlsx

# Run tests
python run.py --test
"""

import argparse
import json
import sys
from pathlib import Path

# Ensure etl_pipeline package is importable regardless of working directory
sys.path.insert(0, str(Path(__file__).parent))


def run_agent(args):
    from etl_pipeline.agents.agent_re_extended import REExtendedAgent
    result = REExtendedAgent().run(
        input_files  = args.files,
        output_dir   = args.out,
        use_llm      = not args.no_llm,
        project_name = args.name,
    )
    print(json.dumps(result, indent=2))
    if "error" not in result:
        print(f"\n✅ Outputs written to: {result['output_dir']}")
        print(f"   BRD    → {result['files']['brd']}")
        print(f"   TRD    → {result['files']['trd']}")
        print(f"   STTM   → {result['files']['sttm']}")
        print(f"   dbt    → {result['files']['dbt']}/")
        print(f"   Lineage→ {result['files']['lineage']}")


def run_compare(args):
    from tools.compare_outputs import build_confidence_matrix, write_excel_report, write_markdown_report
    if not args.ref:
        print("❌ Provide at least one --ref PATH LABEL argument")
        sys.exit(1)
    ref_configs = [{"path": p, "label": lbl} for p, lbl in args.ref]
    result      = build_confidence_matrix(args.our, ref_configs)
    out         = args.out
    if out.endswith(".xlsx"):
        write_excel_report(result, out)
        print(f"✅ Excel confidence matrix → {out}")
    else:
        md = out if out.endswith(".md") else out + ".md"
        write_markdown_report(result, md)
        print(f"✅ Markdown confidence matrix → {md}")


def run_tests():
    import unittest
    loader = unittest.TestLoader()
    suite  = loader.discover(".", pattern="test_re_extended.py")
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    sys.exit(0 if result.wasSuccessful() else 1)


def main():
    parser = argparse.ArgumentParser(
        prog="run.py",
        description="RE Extended — Reverse Engineer DataStage XML · SQL · VQL · ATL · VW",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python run.py                               # all sample files, no LLM
  python run.py --files job.xml orders.sql    # your own files
  python run.py --no-llm --files *.sql        # fully offline
  python run.py --test                        # run test suite
  python run.py --compare \\
      --our output/re_extended/unified_metadata.json \\
      --ref ref.json Informatica              # confidence matrix
        """,
    )

    parser.add_argument(
        "--files", nargs="*",
        default=[
            "sample_inputs/customer_job.xml",
            "sample_inputs/orders.sql",
            "sample_inputs/products.vql",
            "sample_inputs/replication.atl",
            "sample_inputs/customer_orders.vw",
        ],
        help="Input files (.xml .sql .vql .atl .vw). Defaults to all sample files.",
    )
    parser.add_argument("--out",    default="output/re_extended", help="Output directory")
    parser.add_argument("--name",   default="re_pipeline",        help="dbt project name")
    parser.add_argument("--no-llm", action="store_true",          help="Disable LLM enrichment")

    # Compare mode
    parser.add_argument("--compare", action="store_true",         help="Run confidence matrix comparator")
    parser.add_argument("--our",                                  help="Our unified_metadata.json (for --compare)")
    parser.add_argument("--ref",  action="append", nargs=2, metavar=("PATH","LABEL"),
                        help="Reference file+label (for --compare, repeatable)")

    # Test mode
    parser.add_argument("--test", action="store_true",            help="Run test suite")

    args = parser.parse_args()

    if args.test:
        run_tests()
    elif args.compare:
        run_compare(args)
    else:
        run_agent(args)


if __name__ == "__main__":
    main()
