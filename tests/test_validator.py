import csv
import tempfile
from pathlib import Path
from unittest import TestCase

from cm_csv_validator.validator import OUTPUT_COLUMNS, run_csv_validation


class ValidatorTests(TestCase):
    def _write_csv(self, root: Path, name: str, rows, fieldnames=OUTPUT_COLUMNS) -> Path:
        path = root / name
        with path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=fieldnames)
            writer.writeheader()
            for row in rows:
                writer.writerow(row)
        return path

    def test_valid_csv_passes(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            path = self._write_csv(
                Path(tmpdir),
                "valid.csv",
                [
                    {
                        "message_id": "msg_1",
                        "sent_at": "05/03/2026 14:17",
                        "from": "sender@example.com",
                        "message_body": "Hello world",
                        "subject": "Subject",
                        "to": "recipient@example.com",
                        "thread_id": "thr_1",
                    }
                ],
            )
            report = run_csv_validation(path)

        self.assertTrue(report.ready_for_upload)
        self.assertEqual(report.row_count, 1)
        self.assertEqual(report.distinct_thread_count, 1)

    def test_duplicate_ids_fail_and_duplicate_content_warns(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            path = self._write_csv(
                Path(tmpdir),
                "duplicates.csv",
                [
                    {
                        "message_id": "msg_1",
                        "sent_at": "05/03/2026 14:17",
                        "from": "sender@example.com",
                        "message_body": "Duplicate body",
                        "subject": "Subject",
                        "to": "recipient@example.com",
                        "thread_id": "thr_1",
                    },
                    {
                        "message_id": "msg_1",
                        "sent_at": "05/03/2026 14:17",
                        "from": "sender@example.com",
                        "message_body": "Duplicate body",
                        "subject": "Subject",
                        "to": "recipient@example.com",
                        "thread_id": "thr_1",
                    },
                ],
            )
            report = run_csv_validation(path)

        self.assertFalse(report.ready_for_upload)
        self.assertEqual(report.duplicate_message_id_count, 1)
        self.assertEqual(report.body_only_duplicate_row_count, 1)
        self.assertEqual(report.strict_duplicate_row_count, 1)

    def test_missing_columns_report_possible_source_matches(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            path = self._write_csv(
                Path(tmpdir),
                "missing.csv",
                [
                    {
                        "id": "abc",
                        "conversationId": "conv_1",
                        "internetMessageId": "<x@example.com>",
                        "subject": "Subject",
                        "from_address": "sender@example.com",
                        "toRecipients": "recipient@example.com",
                    }
                ],
                fieldnames=[
                    "id",
                    "conversationId",
                    "internetMessageId",
                    "subject",
                    "from_address",
                    "toRecipients",
                ],
            )
            report = run_csv_validation(path)

        self.assertFalse(report.ready_for_upload)
        self.assertIn("message_id", report.missing_columns)
        self.assertIn("from_address", report.suggested_column_map["from"])
        self.assertIn("toRecipients", report.suggested_column_map["to"])

    def test_invalid_utf8_is_reported(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "cp1252.csv"
            path.write_bytes(
                b"message_id,sent_at,from,message_body,subject,to,thread_id\n"
                b"msg_1,05/03/2026 14:17,sender@example.com,Body with smart quote \x92,Subject,recipient@example.com,thr_1\n"
            )
            report = run_csv_validation(path)

        self.assertFalse(report.ready_for_upload)
        self.assertFalse(report.utf8_valid)
        self.assertTrue(any("not valid UTF-8" in issue["message"] for issue in report.example_issues))

    def test_blank_header_columns_fail(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "blank-header.csv"
            path.write_text(
                "message_id,sent_at,from,message_body,subject,to,thread_id,,\n"
                "msg_1,05/03/2026 14:17,sender@example.com,Hello,Subject,recipient@example.com,thr_1,,\n",
                encoding="utf-8",
            )
            report = run_csv_validation(path)

        self.assertFalse(report.ready_for_upload)
        self.assertEqual(report.blank_header_column_count, 2)
