from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Optional, Sequence

from .validator import DEFAULT_MAX_CSV_FILE_BYTES, run_csv_validation


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="cm-csv-validator",
        description="Validate a CSV for UiPath Communications Mining upload readiness.",
    )
    parser.add_argument("input", type=Path, help="Path to the CSV file to validate.")
    parser.add_argument(
        "--report",
        type=Path,
        default=None,
        help="Optional path to write the validation report. Use .json for machine-readable output.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Print the full JSON validation report to stdout instead of the text summary.",
    )
    parser.add_argument(
        "--max-file-bytes",
        type=int,
        default=DEFAULT_MAX_CSV_FILE_BYTES,
        help="Maximum allowed CSV size in bytes before validation fails.",
    )
    parser.add_argument(
        "--examples",
        type=int,
        default=10,
        help="Maximum number of issue examples to include in the report.",
    )
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    report = run_csv_validation(
        input_path=args.input,
        report_path=args.report,
        max_file_bytes=args.max_file_bytes,
        example_limit=args.examples,
    )
    if args.json:
        print(json.dumps(report.to_dict(), indent=2))
    else:
        print(report.to_text(), end="")
    return 0 if report.ready_for_upload else 1
