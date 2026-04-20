from __future__ import annotations

import csv
import hashlib
import io
import json
import re
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

from .validate import MAX_MESSAGE_BODY_BYTES, utf8_len
from .validator import (
    DEFAULT_MAX_CSV_FILE_BYTES,
    _detect_encoding,
    _read_with_encoding,
    run_csv_validation,
)


GRAPH_SOURCE = "microsoft-graph"
GRAPH_HEADERS = [
    "id",
    "conversationId",
    "internetMessageId",
    "subject",
    "from_address",
    "from_name",
    "toRecipients",
    "ccRecipients",
    "bccRecipients",
    "replyTo",
    "receivedDateTime",
    "sentDateTime",
    "createdDateTime",
    "lastModifiedDateTime",
    "importance",
    "categories",
    "isRead",
    "isDraft",
    "hasAttachments",
    "inferenceClassification",
    "conversationIndex",
    "parentFolderId",
    "bodyPreview",
    "body",
    "bodyContentType",
    "webLink",
    "flag_status",
]
CM_COLUMNS = ["message_id", "sent_at", "from", "message_body", "subject", "to", "thread_id"]
GRAPH_ID_RE = re.compile(r"^[A-Za-z0-9+/=_-]{40,}$")
GRAPH_CONVERSATION_RE = re.compile(r"^[A-Za-z0-9+/=_-]{20,}$")
BANNER_MARKER_RE = re.compile(r"ZjQcmQRYFpfpt(?:PreheaderEnd|BannerStart|BannerEnd)?", re.IGNORECASE)
EXTERNAL_SENDER_RE = re.compile(r"This Message Is From an External Sender", re.IGNORECASE)
DO_NOT_CLICK_RE = re.compile(r"DO NOT click links[^.\n]*(?:[.\n]|$)", re.IGNORECASE)
ISO_TIMESTAMP_PATTERNS = (
    "%Y-%m-%dT%H:%M:%SZ",
    "%Y-%m-%dT%H:%M:%S.%fZ",
    "%Y-%m-%dT%H:%M:%S%z",
    "%Y-%m-%dT%H:%M:%S.%f%z",
)


@dataclass
class RepairReport:
    source: str
    input_path: str
    output_dir: str
    output_files: List[str]
    encoding_used: str
    utf8_input_valid: bool
    physical_row_count: int
    logical_row_count: int
    continuation_rows_stitched: int
    orphan_continuation_rows_dropped: int
    duplicate_source_ids_dropped: int
    rows_dropped_missing_required_values: int
    rows_dropped_unparseable_timestamp: int
    validation_before_ready: bool
    validation_after_ready: bool
    validation_after_failures: List[Dict[str, object]]
    warnings: List[str]

    def to_dict(self) -> Dict[str, object]:
        return asdict(self)


def _serialize_csv_row(values: Sequence[str]) -> str:
    buffer = io.StringIO(newline="")
    writer = csv.writer(buffer, quoting=csv.QUOTE_ALL)
    writer.writerow(list(values))
    return buffer.getvalue()


def _safe_id(prefix: str, value: str) -> str:
    digest = hashlib.sha1((value or "").encode("utf-8", errors="ignore")).hexdigest()
    return "{}_{}".format(prefix, digest)


def _looks_like_graph_start(row: Sequence[str]) -> bool:
    if len(row) < len(GRAPH_HEADERS):
        return False
    first = (row[0] or "").strip()
    second = (row[1] or "").strip()
    return bool(GRAPH_ID_RE.match(first) and GRAPH_CONVERSATION_RE.match(second))


def _normalize_recipients(*values: str) -> str:
    items: List[str] = []
    seen = set()
    for value in values:
        for raw_item in (value or "").replace("\n", ",").split(","):
            item = raw_item.strip()
            if not item:
                continue
            lowered = item.lower()
            if lowered in seen:
                continue
            seen.add(lowered)
            items.append(item)
    return "; ".join(items)


def _normalize_text(value: str) -> str:
    text = (value or "").replace("\r\n", "\n").replace("\r", "\n")
    text = text.replace("\x00", "").replace("\ufeff", "")
    text = text.replace("???", "").strip()
    return text


def _truncate_utf8(text: str, max_bytes: int) -> str:
    if utf8_len(text) <= max_bytes:
        return text
    data = text.encode("utf-8")[:max_bytes]
    while True:
        try:
            return data.decode("utf-8")
        except UnicodeDecodeError as exc:
            data = data[: exc.start]


def _clean_body(text: str) -> str:
    cleaned = _normalize_text(text)
    cleaned = BANNER_MARKER_RE.sub(" ", cleaned)
    cleaned = EXTERNAL_SENDER_RE.sub(" ", cleaned)
    cleaned = DO_NOT_CLICK_RE.sub(" ", cleaned)
    cleaned = re.sub(r"[ \t]+", " ", cleaned)
    cleaned = re.sub(r"\n{3,}", "\n\n", cleaned)
    cleaned = cleaned.strip()
    return _truncate_utf8(cleaned, MAX_MESSAGE_BODY_BYTES)


def _parse_graph_timestamp(*candidates: str) -> Optional[str]:
    for candidate in candidates:
        text = (candidate or "").strip()
        if not text:
            continue
        for pattern in ISO_TIMESTAMP_PATTERNS:
            try:
                dt = datetime.strptime(text, pattern)
                return dt.strftime("%d/%m/%Y %H:%M")
            except ValueError:
                continue
    return None


def _write_cm_shards(rows: Sequence[Dict[str, str]], output_base: Path, max_file_bytes: int) -> List[str]:
    output_base.parent.mkdir(parents=True, exist_ok=True)
    header_row = _serialize_csv_row(CM_COLUMNS)
    current_handle = None
    current_bytes = 0
    current_rows = 0
    part_index = 0
    paths: List[str] = []

    def open_next() -> Tuple[object, int]:
        nonlocal part_index, current_bytes, current_rows
        part_index += 1
        final_path = output_base.with_name("{}_part{:03d}.csv".format(output_base.stem, part_index))
        handle = final_path.open("w", encoding="utf-8", newline="")
        handle.write(header_row)
        paths.append(str(final_path))
        current_bytes = len(header_row.encode("utf-8"))
        current_rows = 0
        return handle, current_bytes

    for row in rows:
        serialized = _serialize_csv_row([row[column] for column in CM_COLUMNS])
        row_bytes = len(serialized.encode("utf-8"))
        if current_handle is None:
            current_handle, _ = open_next()
        if current_rows > 0 and current_bytes + row_bytes > max_file_bytes:
            current_handle.close()
            current_handle, _ = open_next()
        current_handle.write(serialized)
        current_bytes += row_bytes
        current_rows += 1

    if current_handle is None:
        current_handle, _ = open_next()
    current_handle.close()
    return paths


def repair_microsoft_graph_csv(
    input_path: Path,
    output_dir: Path,
    max_file_bytes: int = DEFAULT_MAX_CSV_FILE_BYTES,
) -> RepairReport:
    output_dir.mkdir(parents=True, exist_ok=True)
    validation_before = run_csv_validation(input_path)
    encoding_used, utf8_valid, _ = _detect_encoding(input_path)

    physical_row_count = 0
    continuation_rows_stitched = 0
    orphan_continuation_rows_dropped = 0
    duplicate_source_ids_dropped = 0
    rows_dropped_missing_required_values = 0
    rows_dropped_unparseable_timestamp = 0
    warnings: List[str] = []

    logical_rows: List[Dict[str, str]] = []
    current: Optional[Dict[str, str]] = None

    reader = _read_with_encoding(input_path, encoding_used)
    header = next(reader, None)
    if header is None:
        raise ValueError("input CSV is empty")

    for row in reader:
        if not row or not any(cell.strip() for cell in row):
            continue
        physical_row_count += 1
        if _looks_like_graph_start(row):
            if current is not None:
                logical_rows.append(current)
            values = list(row[: len(GRAPH_HEADERS)])
            if len(values) < len(GRAPH_HEADERS):
                values.extend([""] * (len(GRAPH_HEADERS) - len(values)))
            current = dict(zip(GRAPH_HEADERS, values))
            extra_tail = [cell.strip() for cell in row[len(GRAPH_HEADERS) :] if cell.strip()]
            if extra_tail:
                current["body"] = "{}\n{}".format(current.get("body", ""), "\n".join(extra_tail)).strip()
                warnings.append("encountered non-empty overflow columns on a graph start row")
        else:
            if current is None:
                orphan_continuation_rows_dropped += 1
                continue
            continuation_rows_stitched += 1
            chunk = " ".join(cell.strip() for cell in row if cell.strip())
            if chunk:
                current["body"] = "{}\n{}".format(current.get("body", ""), chunk).strip()

    if current is not None:
        logical_rows.append(current)

    repaired_rows: List[Dict[str, str]] = []
    seen_raw_ids = set()
    for record in logical_rows:
        raw_id = (record.get("id") or "").strip()
        thread_value = (record.get("conversationId") or "").strip()
        subject = _normalize_text(record.get("subject", ""))
        body = _clean_body(record.get("body", ""))
        if not raw_id or not thread_value or not body:
            rows_dropped_missing_required_values += 1
            continue
        if raw_id in seen_raw_ids:
            duplicate_source_ids_dropped += 1
            continue
        seen_raw_ids.add(raw_id)
        sent_at = _parse_graph_timestamp(
            record.get("sentDateTime", ""),
            record.get("receivedDateTime", ""),
            record.get("createdDateTime", ""),
        )
        if not sent_at:
            rows_dropped_unparseable_timestamp += 1
            continue
        repaired_rows.append(
            {
                "message_id": _safe_id("msg", raw_id),
                "sent_at": sent_at,
                "from": _normalize_recipients(record.get("from_address", "")),
                "message_body": body,
                "subject": subject,
                "to": _normalize_recipients(
                    record.get("toRecipients", ""),
                    record.get("ccRecipients", ""),
                    record.get("bccRecipients", ""),
                ),
                "thread_id": _safe_id("thr", thread_value),
            }
        )

    output_base = output_dir / "{}_cm_ready".format(input_path.stem)
    output_files = _write_cm_shards(repaired_rows, output_base, max_file_bytes=max_file_bytes)

    validation_failures: List[Dict[str, object]] = []
    validation_after_ready = True
    for path_str in output_files:
        report = run_csv_validation(Path(path_str))
        if not report.ready_for_upload:
            validation_after_ready = False
            validation_failures.append(
                {
                    "path": path_str,
                    "fatal_count": report.fatal_count,
                    "warning_count": report.warning_count,
                    "grouped_issues": report.grouped_issues,
                }
            )

    return RepairReport(
        source=GRAPH_SOURCE,
        input_path=str(input_path),
        output_dir=str(output_dir),
        output_files=output_files,
        encoding_used=encoding_used,
        utf8_input_valid=utf8_valid,
        physical_row_count=physical_row_count,
        logical_row_count=len(repaired_rows),
        continuation_rows_stitched=continuation_rows_stitched,
        orphan_continuation_rows_dropped=orphan_continuation_rows_dropped,
        duplicate_source_ids_dropped=duplicate_source_ids_dropped,
        rows_dropped_missing_required_values=rows_dropped_missing_required_values,
        rows_dropped_unparseable_timestamp=rows_dropped_unparseable_timestamp,
        validation_before_ready=validation_before.ready_for_upload,
        validation_after_ready=validation_after_ready,
        validation_after_failures=validation_failures,
        warnings=sorted(set(warnings)),
    )
