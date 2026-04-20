# cm-csv-validator

`cm-csv-validator` is a standalone CLI that checks whether a CSV is ready for UiPath Communications Mining upload.

It is intentionally narrow:

- it does not transform source data
- it does not scrub or rewrite fields
- it validates an existing CSV and reports exactly what is wrong when the file is not upload-ready

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

```bash
python3 -m pip install -e .
```

## Usage

Validate a CSV and print a human-readable summary:

```bash
cm-csv-validator /path/to/file.csv
```

Write a machine-readable JSON report:

```bash
cm-csv-validator /path/to/file.csv --report validation-report.json
```

Print JSON to stdout:

```bash
cm-csv-validator /path/to/file.csv --json
```

Run without installing:

```bash
PYTHONPATH=src python3 -m cm_csv_validator /path/to/file.csv
```

## Exit Codes

- `0`: file is ready for upload
- `1`: validation failed

## Notes

- This project is meant for customer-owned CSV files. Do not commit customer CSVs into the repository.
- The validator reports likely source-column matches when required CM columns are missing.
- Duplicate-content warnings are diagnostic. A file can still fail or pass independently of those warnings depending on the fatal checks.
