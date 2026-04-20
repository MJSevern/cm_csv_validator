import csv
import tempfile
from pathlib import Path
from unittest import TestCase

from cm_csv_validator.repair import GRAPH_HEADERS, repair_microsoft_graph_csv
from cm_csv_validator.validator import run_csv_validation


class RepairTests(TestCase):
    def _write_graph_like_csv(self, path: Path) -> None:
        header = GRAPH_HEADERS + ["", ""]
        start_row = [
            "AAMkValidGraphMessageIdentifier000000000000000000000000000000==",
            "AAQkValidConversationIdentifier0000000000000000==",
            "<msg@example.com>",
            "Sample subject",
            "sender@example.com",
            "Sender Name",
            "to1@example.com, to2@example.com",
            "cc1@example.com",
            "",
            "",
            "2026-04-06T16:05:16Z",
            "2026-04-06T16:05:01Z",
            "2026-04-06T16:05:16Z",
            "2026-04-06T16:05:29Z",
            "normal",
            "",
            "false",
            "false",
            "false",
            "focused",
            "abc123",
            "folder123",
            "Preview text",
            "Body line one",
            "html",
            "https://example.test/message",
            "notFlagged",
            "",
            "",
        ]
        continuation = [
            "continued body text from a broken physical row",
            "with another fragment",
            "",
            "",
        ]
        with path.open("w", encoding="latin-1", newline="") as handle:
            writer = csv.writer(handle)
            writer.writerow(header)
            writer.writerow(start_row)
            writer.writerow(continuation)

    def test_repair_microsoft_graph_csv_produces_valid_output(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            input_path = root / "messages_raw.csv"
            output_dir = root / "repaired"
            self._write_graph_like_csv(input_path)

            report = repair_microsoft_graph_csv(input_path=input_path, output_dir=output_dir)

            self.assertEqual(report.continuation_rows_stitched, 1)
            self.assertEqual(len(report.output_files), 1)
            self.assertTrue(report.validation_after_ready)

            output_path = Path(report.output_files[0])
            with output_path.open("r", encoding="utf-8", newline="") as handle:
                rows = list(csv.DictReader(handle))
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["sent_at"], "06/04/2026 16:05")
            self.assertEqual(rows[0]["from"], "sender@example.com")
            self.assertEqual(rows[0]["to"], "to1@example.com; to2@example.com; cc1@example.com")
            self.assertIn("continued body text", rows[0]["message_body"])

            validation = run_csv_validation(output_path)
            self.assertTrue(validation.ready_for_upload)

    def test_repair_drops_duplicate_source_ids_and_cleans_banner_markers(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            input_path = root / "messages_raw.csv"
            output_dir = root / "repaired"
            header = GRAPH_HEADERS + ["", ""]
            duplicate_id = "AAMkDuplicateGraphMessageIdentifier000000000000000000000000=="
            rows = [
                [
                    duplicate_id,
                    "AAQkConversationIdentifier0000000000000000==",
                    "<msg1@example.com>",
                    "Subject",
                    "sender@example.com",
                    "Sender",
                    "to@example.com",
                    "",
                    "",
                    "",
                    "2026-04-06T16:05:16Z",
                    "2026-04-06T16:05:01Z",
                    "2026-04-06T16:05:16Z",
                    "2026-04-06T16:05:29Z",
                    "normal",
                    "",
                    "false",
                    "false",
                    "false",
                    "focused",
                    "abc123",
                    "folder123",
                    "Preview",
                    "Hello ZjQcmQRYFpfptBannerStart This Message Is From an External Sender",
                    "html",
                    "",
                    "notFlagged",
                    "",
                    "",
                ],
                [
                    duplicate_id,
                    "AAQkConversationIdentifier0000000000000000==",
                    "<msg2@example.com>",
                    "Subject",
                    "sender@example.com",
                    "Sender",
                    "to@example.com",
                    "",
                    "",
                    "",
                    "2026-04-06T16:06:16Z",
                    "2026-04-06T16:06:01Z",
                    "2026-04-06T16:06:16Z",
                    "2026-04-06T16:06:29Z",
                    "normal",
                    "",
                    "false",
                    "false",
                    "false",
                    "focused",
                    "abc124",
                    "folder124",
                    "Preview",
                    "Second row should drop",
                    "html",
                    "",
                    "notFlagged",
                    "",
                    "",
                ],
            ]
            with input_path.open("w", encoding="latin-1", newline="") as handle:
                writer = csv.writer(handle)
                writer.writerow(header)
                writer.writerows(rows)

            report = repair_microsoft_graph_csv(input_path=input_path, output_dir=output_dir)

            self.assertTrue(report.validation_after_ready)
            self.assertEqual(report.duplicate_source_ids_dropped, 1)
            output_path = Path(report.output_files[0])
            with output_path.open("r", encoding="utf-8", newline="") as handle:
                output_rows = list(csv.DictReader(handle))
            self.assertEqual(len(output_rows), 1)
            self.assertNotIn("ZjQcmQRYFpfpt", output_rows[0]["message_body"])
            self.assertNotIn("This Message Is From an External Sender", output_rows[0]["message_body"])
