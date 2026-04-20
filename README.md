# cm-csv-validator

`cm-csv-validator` is a standalone CLI that checks whether a CSV is ready for UiPath Communications Mining upload.

It is intentionally narrow:

- it does not transform source data
- it does not scrub or rewrite fields
- it validates an existing CSV and reports exactly what is wrong when the file is not upload-ready

The project also includes a source-specific repair command for known export-shape problems.

Important scope boundary:

- the repair command only fixes concrete data issues such as encoding, malformed CSV row stitching, field remapping, recipient delimiter normalization, deterministic CM-safe IDs, timestamp normalization, and file splitting
- it can also apply narrow source-specific cleanup like dropping duplicate source IDs, removing known banner markers, and truncating overlong bodies to CM-safe byte bounds when the adapter has explicit rules for them
- it does not guarantee semantic cleanup
- it does not infer missing business data that is not present in the source
- it does not resolve arbitrary corrupt exports
- it does not remove duplicate business content unless a source-specific repair rule explicitly does so

## What It Checks

The validator checks:

- file size against the default `128 MiB` upload cap
- UTF-8 validity
- CSV parseability
- blank or duplicate header columns
- required Communications Mining columns:
  - `message_id`
  - `sent_at`
  - `from`
  - `message_body`
  - `subject`
  - `to`
  - `thread_id`
- row-level field constraints:
  - `message_body` is not blank
  - `message_body <= 65536` bytes
  - `subject <= 4096` bytes
  - `message_id` and `thread_id` are non-blank, ASCII-safe, contain no `/`, and are `<= 128` bytes
  - `sent_at` parses as `dd/mm/yyyy HH:MM`
  - sender and recipient cells stay within safe byte bounds
  - sender and recipient items stay within safe byte bounds
  - no `ZjQcmQRYFpfpt` banner markers leak into `message_body`
- duplicate `message_id` values
- duplicate-content signals:
  - body-only duplicates
  - strict duplicates by `subject + message_body + sent_at`

## Install

Clone the repo, move into it, then install the CLI:

```bash
git clone https://github.com/MJSevern/cm_csv_validator.git
cd cm_csv_validator
python3 -m pip install -e .
```

If you do not want to install the CLI, you can run it directly from the repo root:

```bash
git clone https://github.com/MJSevern/cm_csv_validator.git
cd cm_csv_validator
PYTHONPATH=src python3 -m cm_csv_validator /path/to/input_export.csv
```

If you are already in the cloned repo, this is the only install command you need:

```bash
python3 -m pip install -e .
```

## Usage

Validate a CSV and print a human-readable summary:

```bash
cm-csv-validator /path/to/input_export.csv
```

Optional for automation: write a structured report file or print JSON to stdout.

```bash
cm-csv-validator /path/to/input_export.csv --report validation-report.json
cm-csv-validator /path/to/input_export.csv --json
```

Run without installing:

```bash
PYTHONPATH=src python3 -m cm_csv_validator /path/to/input_export.csv
```

Repair a known Microsoft Graph export shape and emit CM-ready shards:

```bash
cm-csv-repair --source microsoft-graph /path/to/graph_export.csv --output-dir repaired
```

The intended flow is:

```bash
cm-csv-validator /path/to/graph_export.csv
cm-csv-repair --source microsoft-graph /path/to/graph_export.csv --output-dir repaired
cm-csv-validator repaired/graph_export_cm_ready_part001.csv
```

## Exit Codes

- `0`: file is ready for upload
- `1`: validation failed

## Notes

- This project is meant for customer-owned CSV files.
- The validator reports likely source-column matches when required CM columns are missing.
- Duplicate-content warnings are diagnostic. A file can still fail or pass independently of those warnings depending on the fatal checks.
- The repair command is source-specific and intentionally conservative. If the source data is missing required values or is damaged beyond recovery, the tool may drop rows and report that it did so.
- The `--report` and `--json` options are optional. They are useful for scripted pipelines or CI checks, but not required for normal interactive use.
