from __future__ import annotations

import csv
import json
import sys
from collections import Counter
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

from .models import CsvValidationIssue, CsvValidationReport, OutputRow
from .validate import utf8_len, validate_row


OUTPUT_COLUMNS = [
    "message_id",
    "sent_at",
    "from",
    "message_body",
    "subject",
    "to",
    "thread_id",
]
DEFAULT_MAX_CSV_FILE_BYTES = 128 * 1024 * 1024
FALLBACK_ENCODINGS = ("cp1252", "latin-1")
HEADER_ALIAS_HINTS = {
    "message_id": ("id", "unique_id", "internetmessageid", "messageid"),
    "sent_at": ("sent", "timestamp", "createddate", "created_at", "receiveddatetime"),
    "from": ("from_address", "sender", "sender_email", "fromname", "from_name"),
    "message_body": ("message", "body", "text", "content"),
    "subject": ("title",),
    "to": ("torecipients", "recipients", "to_address", "toaddresses"),
    "thread_id": ("conversationid", "threadid", "thread", "caseid"),
}


def _set_csv_field_size_limit() -> None:
    limit = sys.maxsize
    while True:
        try:
            csv.field_size_limit(limit)
            return
        except OverflowError:
            limit = limit // 10


def _read_with_encoding(path: Path, encoding: str) -> Iterable[List[str]]:
    _set_csv_field_size_limit()
    with path.open("r", encoding=encoding, newline="") as handle:
        yield from csv.reader(handle)


def _detect_encoding(path: Path) -> Tuple[str, bool, Optional[UnicodeDecodeError]]:
    try:
        with path.open("r", encoding="utf-8-sig", newline="") as handle:
            for _ in handle:
                pass
        return "utf-8-sig", True, None
    except UnicodeDecodeError as exc:
        for encoding in FALLBACK_ENCODINGS:
            try:
                with path.open("r", encoding=encoding, newline="") as handle:
                    for _ in handle:
                        pass
                return encoding, False, exc
            except UnicodeDecodeError:
                continue
        return "latin-1", False, exc


def _maybe_write_report(report: CsvValidationReport, report_path: Optional[Path]) -> None:
    if report_path is None:
        return
    report_path.parent.mkdir(parents=True, exist_ok=True)
    if report_path.suffix.lower() == ".json":
        report_path.write_text(json.dumps(report.to_dict(), indent=2), encoding="utf-8")
    else:
        report_path.write_text(report.to_text(), encoding="utf-8")


def _suggest_columns(missing_columns: Sequence[str], present_columns: Sequence[str]) -> Dict[str, List[str]]:
    normalized_present = {column.lower(): column for column in present_columns}
    suggestions: Dict[str, List[str]] = {}
    for required in missing_columns:
        candidates: List[str] = []
        for alias in HEADER_ALIAS_HINTS.get(required, ()):
            matched = normalized_present.get(alias.lower())
            if matched and matched not in candidates:
                candidates.append(matched)
        if not candidates:
            compact_required = required.replace("_", "")
            for present in present_columns:
                normalized = present.lower().replace("_", "").replace("-", "")
                if compact_required in normalized or normalized in compact_required:
                    if present not in candidates:
                        candidates.append(present)
        suggestions[required] = candidates[:3]
    return suggestions


def _group_issues(issues: Sequence[CsvValidationIssue], limit: int = 20) -> List[Dict[str, object]]:
    grouped: Dict[Tuple[str, str, str], Dict[str, object]] = {}
    for issue in issues:
        key = (issue.severity, issue.code, issue.message)
        entry = grouped.setdefault(
            key,
            {
                "severity": issue.severity,
                "code": issue.code,
                "message": issue.message,
                "count": 0,
                "examples": [],
            },
        )
        entry["count"] += 1
        if len(entry["examples"]) < 3:
            example = {}
            if issue.row_number is not None:
                example["row_number"] = issue.row_number
            if issue.field_name:
                example["field_name"] = issue.field_name
            if issue.message_id:
                example["message_id"] = issue.message_id
            entry["examples"].append(example)
    ordered = sorted(
        grouped.values(),
        key=lambda item: (
            0 if item["severity"] == "fatal" else 1,
            -int(item["count"]),
            str(item["message"]),
        ),
    )
    return ordered[:limit]


def _row_from_csv(row_number: int, record: Dict[str, str]) -> OutputRow:
    message_id = (record.get("message_id") or "").strip()
    return OutputRow(
        row_number=row_number,
        message_id=message_id,
        sent_at=(record.get("sent_at") or "").strip(),
        sender=(record.get("from") or "").strip(),
        message_body=record.get("message_body") or "",
        subject=record.get("subject") or "",
        recipients=(record.get("to") or "").strip(),
        thread_id=(record.get("thread_id") or "").strip(),
    )


def run_csv_validation(
    input_path: Path,
    report_path: Optional[Path] = None,
    max_file_bytes: int = DEFAULT_MAX_CSV_FILE_BYTES,
    example_limit: int = 10,
) -> CsvValidationReport:
    issues: List[CsvValidationIssue] = []
    present_columns: List[str] = []
    blank_header_column_count = 0
    missing_columns: List[str] = []
    row_count = 0
    distinct_threads = set()
    duplicate_message_ids = 0
    body_only_duplicate_rows = 0
    body_only_duplicate_groups = 0
    strict_duplicate_rows = 0
    strict_duplicate_groups = 0
    max_body_bytes = 0
    max_subject_bytes = 0
    max_message_id_bytes = 0
    max_thread_id_bytes = 0
    row_level_checks_ran = False

    if not input_path.exists():
        issues.append(
            CsvValidationIssue(
                severity="fatal",
                code="missing_file",
                message="input CSV does not exist",
            )
        )
        report = CsvValidationReport(
            input_path=str(input_path),
            ready_for_upload=False,
            file_size_bytes=0,
            max_file_size_bytes=max_file_bytes,
            encoding_used="unknown",
            utf8_valid=False,
            row_count=0,
            distinct_thread_count=0,
            present_columns=[],
            blank_header_column_count=0,
            missing_columns=list(OUTPUT_COLUMNS),
            suggested_column_map={column: [] for column in OUTPUT_COLUMNS},
            max_observed_message_body_bytes=0,
            max_observed_subject_bytes=0,
            max_observed_message_id_bytes=0,
            max_observed_thread_id_bytes=0,
            duplicate_message_id_count=0,
            body_only_duplicate_row_count=0,
            body_only_duplicate_group_count=0,
            strict_duplicate_row_count=0,
            strict_duplicate_group_count=0,
            row_level_checks_ran=False,
            fatal_count=1,
            warning_count=0,
            grouped_issues=_group_issues(issues),
            example_issues=[issue.to_dict() for issue in issues[:example_limit]],
        )
        _maybe_write_report(report, report_path)
        return report

    file_size_bytes = input_path.stat().st_size
    if file_size_bytes > max_file_bytes:
        issues.append(
            CsvValidationIssue(
                severity="fatal",
                code="file_size_exceeded",
                message="file exceeds {} bytes by {}".format(max_file_bytes, file_size_bytes - max_file_bytes),
            )
        )

    encoding_used, utf8_valid, unicode_error = _detect_encoding(input_path)
    if not utf8_valid and unicode_error is not None:
        issues.append(
            CsvValidationIssue(
                severity="fatal",
                code="encoding_not_utf8",
                message=(
                    "CSV is not valid UTF-8; first decode error at byte {}: {}".format(
                        unicode_error.start,
                        unicode_error.reason,
                    )
                ),
            )
        )

    seen_message_ids: Dict[str, int] = {}
    body_counter: Counter = Counter()
    strict_counter: Counter = Counter()

    try:
        reader = _read_with_encoding(input_path, encoding_used)
        header = next(reader, None)
        if header is None:
            issues.append(
                CsvValidationIssue(
                    severity="fatal",
                    code="empty_file",
                    message="CSV file is empty",
                )
            )
            present_columns = []
        else:
            present_columns = [column.strip() for column in header]
            blank_header_column_count = sum(1 for column in present_columns if not column)
            if blank_header_column_count:
                issues.append(
                    CsvValidationIssue(
                        severity="fatal",
                        code="blank_header_columns",
                        message="CSV header contains {} blank column names".format(blank_header_column_count),
                    )
                )
            duplicate_columns = sorted(
                column for column, count in Counter(present_columns).items() if column and count > 1
            )
            if duplicate_columns:
                issues.append(
                    CsvValidationIssue(
                        severity="fatal",
                        code="duplicate_columns",
                        message="CSV header contains duplicate columns: {}".format(", ".join(duplicate_columns)),
                    )
                )
            missing_columns = [column for column in OUTPUT_COLUMNS if column not in present_columns]
            column_index = {column: index for index, column in enumerate(present_columns)}
            if missing_columns:
                suggestions = _suggest_columns(missing_columns, present_columns)
                details = []
                for missing in missing_columns:
                    hints = suggestions.get(missing) or []
                    if hints:
                        details.append("{} -> {}".format(missing, ", ".join(hints)))
                detail_text = ""
                if details:
                    detail_text = " Possible source columns: {}.".format("; ".join(details))
                issues.append(
                    CsvValidationIssue(
                        severity="fatal",
                        code="missing_required_columns",
                        message="missing required columns: {}.{}".format(
                            ", ".join(missing_columns),
                            detail_text,
                        ),
                    )
                )

            for row_number, row in enumerate(reader, start=2):
                if not row or not any(cell.strip() for cell in row):
                    continue
                row_count += 1
                if len(row) != len(present_columns):
                    issues.append(
                        CsvValidationIssue(
                            severity="fatal",
                            code="malformed_row_length",
                            message="row has {} fields but header defines {}".format(len(row), len(present_columns)),
                            row_number=row_number,
                        )
                    )
                if len(row) < len(present_columns):
                    row = row + [""] * (len(present_columns) - len(row))
                elif len(row) > len(present_columns):
                    row = row[: len(present_columns)]

                if missing_columns or duplicate_columns:
                    continue

                row_level_checks_ran = True
                record = {column: row[column_index[column]] for column in OUTPUT_COLUMNS}
                output_row = _row_from_csv(row_number, record)
                max_body_bytes = max(max_body_bytes, utf8_len(output_row.message_body))
                max_subject_bytes = max(max_subject_bytes, utf8_len(output_row.subject))
                max_message_id_bytes = max(max_message_id_bytes, utf8_len(output_row.message_id))
                max_thread_id_bytes = max(max_thread_id_bytes, utf8_len(output_row.thread_id))
                if output_row.thread_id:
                    distinct_threads.add(output_row.thread_id)

                for validation_issue in validate_row(output_row):
                    issues.append(
                        CsvValidationIssue(
                            severity=validation_issue.severity,
                            code="row_validation",
                            message=validation_issue.message,
                            row_number=row_number,
                            field_name=validation_issue.field_name,
                            message_id=output_row.message_id,
                        )
                    )

                if output_row.message_id:
                    original_row = seen_message_ids.get(output_row.message_id)
                    if original_row is not None:
                        duplicate_message_ids += 1
                        issues.append(
                            CsvValidationIssue(
                                severity="fatal",
                                code="duplicate_message_id",
                                message="message_id is duplicated; first seen on row {}".format(original_row),
                                row_number=row_number,
                                field_name="message_id",
                                message_id=output_row.message_id,
                            )
                        )
                    else:
                        seen_message_ids[output_row.message_id] = row_number

                if output_row.message_body.strip():
                    body_counter[output_row.message_body] += 1
                    strict_counter[
                        (
                            output_row.subject,
                            output_row.message_body,
                            output_row.sent_at,
                        )
                    ] += 1

        if row_count == 0 and present_columns:
            issues.append(
                CsvValidationIssue(
                    severity="fatal",
                    code="no_data_rows",
                    message="CSV contains a header but no data rows",
                )
            )
    except csv.Error as exc:
        issues.append(
            CsvValidationIssue(
                severity="fatal",
                code="csv_parse_error",
                message="CSV parse error: {}".format(str(exc)),
            )
        )

    body_duplicate_counts = [count for count in body_counter.values() if count > 1]
    if body_duplicate_counts:
        body_only_duplicate_rows = sum(count - 1 for count in body_duplicate_counts)
        body_only_duplicate_groups = len(body_duplicate_counts)
        issues.append(
            CsvValidationIssue(
                severity="warning",
                code="body_only_duplicates",
                message="message_body duplicates remain: {} rows across {} groups".format(
                    body_only_duplicate_rows,
                    body_only_duplicate_groups,
                ),
            )
        )

    strict_duplicate_counts = [count for count in strict_counter.values() if count > 1]
    if strict_duplicate_counts:
        strict_duplicate_rows = sum(count - 1 for count in strict_duplicate_counts)
        strict_duplicate_groups = len(strict_duplicate_counts)
        issues.append(
            CsvValidationIssue(
                severity="warning",
                code="strict_duplicates",
                message="subject + message_body + sent_at duplicates remain: {} rows across {} groups".format(
                    strict_duplicate_rows,
                    strict_duplicate_groups,
                ),
            )
        )

    fatal_count = sum(1 for issue in issues if issue.severity == "fatal")
    warning_count = sum(1 for issue in issues if issue.severity == "warning")
    report = CsvValidationReport(
        input_path=str(input_path),
        ready_for_upload=fatal_count == 0,
        file_size_bytes=file_size_bytes,
        max_file_size_bytes=max_file_bytes,
        encoding_used=encoding_used,
        utf8_valid=utf8_valid,
        row_count=row_count,
        distinct_thread_count=len(distinct_threads),
        present_columns=present_columns,
        blank_header_column_count=blank_header_column_count,
        missing_columns=missing_columns,
        suggested_column_map=_suggest_columns(missing_columns, present_columns),
        max_observed_message_body_bytes=max_body_bytes,
        max_observed_subject_bytes=max_subject_bytes,
        max_observed_message_id_bytes=max_message_id_bytes,
        max_observed_thread_id_bytes=max_thread_id_bytes,
        duplicate_message_id_count=duplicate_message_ids,
        body_only_duplicate_row_count=body_only_duplicate_rows,
        body_only_duplicate_group_count=body_only_duplicate_groups,
        strict_duplicate_row_count=strict_duplicate_rows,
        strict_duplicate_group_count=strict_duplicate_groups,
        row_level_checks_ran=row_level_checks_ran,
        fatal_count=fatal_count,
        warning_count=warning_count,
        grouped_issues=_group_issues(issues),
        example_issues=[issue.to_dict() for issue in issues[:example_limit]],
    )
    _maybe_write_report(report, report_path)
    return report
