from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Optional, Sequence

from .repair import GRAPH_SOURCE, repair_microsoft_graph_csv
from .validator import DEFAULT_MAX_CSV_FILE_BYTES


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="cm-csv-repair",
        description=(
            "Repair source-specific data-shape problems so a CSV can be re-validated for "
            "Communications Mining upload readiness. This command only fixes concrete data "
            "issues such as encoding, row stitching, column mapping, ID normalization, and "
            "file splitting. It does not solve arbitrary semantic or source-data problems."
        ),
    )
    parser.add_argument("input", type=Path, help="Path to the source CSV file.")
    parser.add_argument(
        "--source",
        default=GRAPH_SOURCE,
        choices=[GRAPH_SOURCE],
        help="Source adapter to use for repair. Only microsoft-graph is supported right now.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("repaired"),
        help="Directory to write repaired CM-ready CSV outputs to.",
    )
    parser.add_argument(
        "--max-file-bytes",
        type=int,
        default=DEFAULT_MAX_CSV_FILE_BYTES,
        help="Maximum bytes per emitted CSV shard.",
    )
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    result = repair_microsoft_graph_csv(
        input_path=args.input,
        output_dir=args.output_dir,
        max_file_bytes=args.max_file_bytes,
    )
    print(json.dumps(result.to_dict(), indent=2))
    return 0 if result.validation_after_ready else 1


if __name__ == "__main__":
    raise SystemExit(main())
