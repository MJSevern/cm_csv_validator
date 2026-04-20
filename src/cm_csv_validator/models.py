from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Dict, List, Optional


@dataclass
class OutputRow:
    row_number: int
    message_id: str
    sent_at: str
    sender: str
    message_body: str
    subject: str
    recipients: str
    thread_id: str


@dataclass
class ValidationIssue:
    severity: str
    message_id: str
    field_name: str
    message: str


@dataclass
class CsvValidationIssue:
    severity: str
    code: str
    message: str
    row_number: Optional[int] = None
    field_name: str = ""
    message_id: str = ""

    def to_dict(self) -> Dict[str, object]:
        return asdict(self)


@dataclass
class CsvValidationReport:
    input_path: str
    ready_for_upload: bool
    file_size_bytes: int
    max_file_size_bytes: int
    encoding_used: str
    utf8_valid: bool
    row_count: int
    distinct_thread_count: int
    present_columns: List[str]
    blank_header_column_count: int
    missing_columns: List[str]
    suggested_column_map: Dict[str, List[str]]
    max_observed_message_body_bytes: int
    max_observed_subject_bytes: int
    max_observed_message_id_bytes: int
    max_observed_thread_id_bytes: int
    duplicate_message_id_count: int
    body_only_duplicate_row_count: int
    body_only_duplicate_group_count: int
    strict_duplicate_row_count: int
    strict_duplicate_group_count: int
    row_level_checks_ran: bool
    fatal_count: int
    warning_count: int
    grouped_issues: List[Dict[str, object]]
    example_issues: List[Dict[str, object]]

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    def to_text(self) -> str:
        named_columns = [column for column in self.present_columns if column]
        if len(named_columns) > 20:
            columns_text = "{}, ... (+{} more)".format(
                ", ".join(named_columns[:20]),
                len(named_columns) - 20,
            )
        else:
            columns_text = ", ".join(named_columns) if named_columns else "(none)"
        lines = [
            "CM CSV Validation Report",
            "",
            "Path: {}".format(self.input_path),
            "Ready for upload: {}".format("yes" if self.ready_for_upload else "no"),
            "Rows checked: {}".format(self.row_count),
            "Distinct threads: {}".format(self.distinct_thread_count),
            "Encoding used for parse: {}".format(self.encoding_used),
            "UTF-8 valid: {}".format("yes" if self.utf8_valid else "no"),
            "File size: {} bytes (limit {})".format(self.file_size_bytes, self.max_file_size_bytes),
            "",
            "Columns present: {}".format(columns_text),
        ]
        if self.blank_header_column_count:
            lines.append("Blank header columns: {}".format(self.blank_header_column_count))
        if self.missing_columns:
            lines.append("Missing required columns: {}".format(", ".join(self.missing_columns)))
            for column, suggestions in self.suggested_column_map.items():
                if suggestions:
                    lines.append("  {} -> possible matches: {}".format(column, ", ".join(suggestions)))
        if not self.row_level_checks_ran:
            lines.append("Row-level CM field checks ran: no")
        lines.extend(
            [
                "",
                "Observed maxima:",
                "  message_body bytes: {}".format(self.max_observed_message_body_bytes),
                "  subject bytes: {}".format(self.max_observed_subject_bytes),
                "  message_id bytes: {}".format(self.max_observed_message_id_bytes),
                "  thread_id bytes: {}".format(self.max_observed_thread_id_bytes),
                "",
                "Duplicate content signals:",
                "  duplicate message_id rows: {}".format(self.duplicate_message_id_count),
                "  body-only duplicates: {} rows across {} groups".format(
                    self.body_only_duplicate_row_count,
                    self.body_only_duplicate_group_count,
                ),
                "  strict duplicates: {} rows across {} groups".format(
                    self.strict_duplicate_row_count,
                    self.strict_duplicate_group_count,
                ),
                "",
                "Fatal findings: {}".format(self.fatal_count),
                "Warnings: {}".format(self.warning_count),
            ]
        )
        if self.grouped_issues:
            lines.append("")
            lines.append("Findings:")
            for finding in self.grouped_issues:
                lines.append(
                    "- [{}] {} ({} occurrence{})".format(
                        finding["severity"],
                        finding["message"],
                        finding["count"],
                        "" if finding["count"] == 1 else "s",
                    )
                )
        if self.example_issues:
            lines.append("")
            lines.append("Examples:")
            for issue in self.example_issues:
                context = []
                if issue.get("row_number") is not None:
                    context.append("row {}".format(issue["row_number"]))
                if issue.get("field_name"):
                    context.append(issue["field_name"])
                if issue.get("message_id"):
                    context.append("message_id={}".format(issue["message_id"]))
                if context:
                    lines.append("- {}: {}".format(", ".join(context), issue["message"]))
                else:
                    lines.append("- {}".format(issue["message"]))
        return "\n".join(lines) + "\n"
