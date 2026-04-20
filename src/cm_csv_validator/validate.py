from __future__ import annotations

from datetime import datetime
from typing import List

from .models import OutputRow, ValidationIssue


MAX_MESSAGE_BODY_BYTES = 65536
MAX_ADDRESS_ITEM_BYTES = 512
MAX_ADDRESS_CELL_BYTES = 8192
MAX_SUBJECT_BYTES = 4096
MAX_ID_BYTES = 128
TIMESTAMP_FORMAT = "%d/%m/%Y %H:%M"


def utf8_len(text: str) -> int:
    return len((text or "").encode("utf-8"))


def _validate_address_items(
    message_id: str,
    field_name: str,
    value: str,
) -> List[ValidationIssue]:
    issues: List[ValidationIssue] = []
    if utf8_len(value) > MAX_ADDRESS_CELL_BYTES:
        issues.append(
            ValidationIssue(
                severity="fatal",
                message_id=message_id,
                field_name=field_name,
                message="address cell exceeds {} bytes".format(MAX_ADDRESS_CELL_BYTES),
            )
        )
    for item in [item.strip() for item in value.split(";") if item.strip()]:
        if utf8_len(item) > MAX_ADDRESS_ITEM_BYTES:
            issues.append(
                ValidationIssue(
                    severity="fatal",
                    message_id=message_id,
                    field_name=field_name,
                    message="address item exceeds {} bytes".format(MAX_ADDRESS_ITEM_BYTES),
                )
            )
    return issues


def validate_row(row: OutputRow) -> List[ValidationIssue]:
    issues: List[ValidationIssue] = []
    if not row.message_id.strip():
        issues.append(
            ValidationIssue(
                severity="fatal",
                message_id=row.message_id,
                field_name="message_id",
                message="message_id must not be blank",
            )
        )
    if not row.thread_id.strip():
        issues.append(
            ValidationIssue(
                severity="fatal",
                message_id=row.message_id,
                field_name="thread_id",
                message="thread_id must not be blank",
            )
        )
    if not row.message_body.strip():
        issues.append(
            ValidationIssue(
                severity="fatal",
                message_id=row.message_id,
                field_name="message_body",
                message="message_body must not be blank",
            )
        )
    if utf8_len(row.message_body) > MAX_MESSAGE_BODY_BYTES:
        issues.append(
            ValidationIssue(
                severity="fatal",
                message_id=row.message_id,
                field_name="message_body",
                message="message_body exceeds {} bytes".format(MAX_MESSAGE_BODY_BYTES),
            )
        )
    if utf8_len(row.subject) > MAX_SUBJECT_BYTES:
        issues.append(
            ValidationIssue(
                severity="fatal",
                message_id=row.message_id,
                field_name="subject",
                message="subject exceeds {} bytes".format(MAX_SUBJECT_BYTES),
            )
        )
    if utf8_len(row.message_id) > MAX_ID_BYTES:
        issues.append(
            ValidationIssue(
                severity="fatal",
                message_id=row.message_id,
                field_name="message_id",
                message="message_id exceeds {} bytes".format(MAX_ID_BYTES),
            )
        )
    if utf8_len(row.thread_id) > MAX_ID_BYTES:
        issues.append(
            ValidationIssue(
                severity="fatal",
                message_id=row.message_id,
                field_name="thread_id",
                message="thread_id exceeds {} bytes".format(MAX_ID_BYTES),
            )
        )
    if "/" in row.message_id or not row.message_id.isascii():
        issues.append(
            ValidationIssue(
                severity="fatal",
                message_id=row.message_id,
                field_name="message_id",
                message="message_id must be ASCII-safe and must not contain '/'",
            )
        )
    if "/" in row.thread_id or not row.thread_id.isascii():
        issues.append(
            ValidationIssue(
                severity="fatal",
                message_id=row.message_id,
                field_name="thread_id",
                message="thread_id must be ASCII-safe and must not contain '/'",
            )
        )
    try:
        datetime.strptime(row.sent_at, TIMESTAMP_FORMAT)
    except ValueError:
        issues.append(
            ValidationIssue(
                severity="fatal",
                message_id=row.message_id,
                field_name="sent_at",
                message="sent_at is not in dd/mm/yyyy HH:MM format",
            )
        )
    issues.extend(_validate_address_items(row.message_id, "from", row.sender))
    issues.extend(_validate_address_items(row.message_id, "to", row.recipients))
    if "ZjQcmQRYFpfpt" in row.message_body:
        issues.append(
            ValidationIssue(
                severity="fatal",
                message_id=row.message_id,
                field_name="message_body",
                message="banner marker leaked into message_body",
            )
        )
    return issues
