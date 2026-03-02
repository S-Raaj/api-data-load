from __future__ import annotations

import json
import os
import sys
import tempfile
import textwrap
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

try:
    from orchestrator import main as orchestrator_main
    REQUESTS_AVAILABLE = True
except ModuleNotFoundError:
    orchestrator_main = None
    REQUESTS_AVAILABLE = False


@unittest.skipUnless(REQUESTS_AVAILABLE, "requests is not installed")
class OrchestrationEndToEndTests(unittest.TestCase):
    def _write_mock_config(
        self,
        tmp_path: Path,
        work_dir: Path,
        log_dir: Path,
        metrics_dir: Path,
        *,
        control_strict: bool,
    ) -> Path:
        config_path = tmp_path / "config.mock.yaml"
        config_path.write_text(
            textwrap.dedent(
                f"""
                api:
                  base_url: "https://example.internal"
                  verify_ssl: false
                  auth:
                    path: "/auth/token"
                    method: "GET"
                    client_id: "client-123"
                    secret: "secret-xyz"
                    client_id_param: "client_id"
                    secret_header: "X-API-SECRET"
                  data:
                    method: "GET"
                    path_template: "/data/table={{table_name}}/asofdate={{asofdate}}"
                  control:
                    enabled: true
                    method: "GET"
                    path_template: "/data/table={{table_name}}?asofdate={{asofdate}}"
                    response_format: "json"
                    count_field: "lastUploadCount"
                    date_field: "lastUploadDate"
                    selection_strategy: "latest"
                    strict: {"true" if control_strict else "false"}
                    exclude_header: true
                token:
                  field: "token"
                  expiry_field: "expiringAt"
                  header: "Authorization"
                  prefix: "Bearer"
                  refresh_buffer_minutes: 5
                retry:
                  max_retries: 3
                  backoff_seconds: 2.0
                  retryable_status_codes: [429, 500, 502, 503, 504]
                orchestrator:
                  download_dir: "{work_dir}"
                  log_dir: "{log_dir}"
                  metrics_dir: "{metrics_dir}"
                  cleanup_local_files: true
                  fail_fast: false
                hdfs:
                  target_dir: "/data/raw/internal_api"
                tables:
                  - customers
                  - orders
                  - invoices
                """
            ).strip()
            + "\n",
            encoding="utf-8",
        )
        return config_path

    def test_main_runs_end_to_end_with_mock_config_api_and_hdfs(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            work_dir = tmp_path / "downloads"
            log_dir = tmp_path / "logs"
            metrics_dir = tmp_path / "metrics"
            config_path = self._write_mock_config(
                tmp_path, work_dir, log_dir, metrics_dir, control_strict=True
            )

            fake_client = MagicMock()
            fake_client.get_stats.return_value = {
                "auth_requests": 1,
                "token_refreshes": 1,
                "api_retries": 1,
                "api_401_refresh_retries": 0,
            }

            def write_mock_csv(table_name: str, business_date: str, output_file: Path) -> None:
                output_file.parent.mkdir(parents=True, exist_ok=True)
                output_file.write_text(
                    f"id,name\n1,{table_name}_{business_date}\n",
                    encoding="utf-8",
                )

            fake_client.download_csv.side_effect = write_mock_csv
            fake_client.fetch_control_count.return_value = 1
            fake_client.count_downloaded_rows.return_value = 1

            with (
                patch.dict(os.environ, {}, clear=False),
                patch.object(
                    sys,
                    "argv",
                    [
                        "orchestrator.main",
                        "--config",
                        str(config_path),
                        "--asofdate",
                        "20260301",
                    ],
                ),
                patch("orchestrator.main.ApiClient", return_value=fake_client),
                patch("orchestrator.main.ensure_hdfs_dir") as ensure_hdfs_dir_mock,
                patch("orchestrator.main.upload_file") as upload_file_mock,
            ):
                return_code = orchestrator_main.main()

            self.assertEqual(return_code, 0)
            fake_client.get_valid_token.assert_called_once_with()
            self.assertEqual(fake_client.download_csv.call_count, 3)
            self.assertEqual(fake_client.fetch_control_count.call_count, 3)
            self.assertEqual(fake_client.count_downloaded_rows.call_count, 3)
            ensure_hdfs_dir_mock.assert_called_once_with("/data/raw/internal_api")
            self.assertEqual(upload_file_mock.call_count, 3)

            metrics_files = list(metrics_dir.glob("run_*.json"))
            self.assertEqual(len(metrics_files), 1)
            metrics_payload = json.loads(metrics_files[0].read_text(encoding="utf-8"))

            self.assertEqual(metrics_payload["counters"]["auth_success"], 1)
            self.assertEqual(metrics_payload["counters"]["downloads_success"], 3)
            self.assertEqual(metrics_payload["counters"]["uploads_success"], 3)
            self.assertEqual(metrics_payload["counters"]["cleanup_success"], 3)
            self.assertEqual(metrics_payload["counters"]["control_validation_success"], 3)
            self.assertEqual(metrics_payload["attributes"]["business_date"], "20260301")
            self.assertEqual(metrics_payload["attributes"]["table_count"], 3)
            self.assertEqual(metrics_payload["attributes"]["auth_requests"], 1)
            self.assertEqual(metrics_payload["attributes"]["token_refreshes"], 1)
            self.assertEqual(metrics_payload["attributes"]["api_retries"], 1)
            self.assertEqual(
                metrics_payload["attributes"]["control_count_customers"],
                {"expected_rows": 1, "actual_rows": 1},
            )

            recorded_steps = {entry["step"] for entry in metrics_payload["timings"]}
            self.assertIn("fetch_token", recorded_steps)
            self.assertIn("ensure_hdfs_dir", recorded_steps)
            self.assertIn("download_csv", recorded_steps)
            self.assertIn("validate_control_count", recorded_steps)
            self.assertIn("upload_hdfs", recorded_steps)
            self.assertIn("cleanup_file", recorded_steps)
            self.assertIn("cleanup_run_dir", recorded_steps)

            self.assertFalse(any(work_dir.rglob("*.csv")))

            log_path = log_dir / "orchestrator.log"
            self.assertTrue(log_path.exists())
            log_entries = [
                json.loads(line)
                for line in log_path.read_text(encoding="utf-8").splitlines()
                if line.strip()
            ]
            messages = [entry["message"] for entry in log_entries]
            self.assertIn("Starting orchestration run", messages)
            self.assertIn("Processing table", messages)
            self.assertIn("Run completed", messages)

    def test_main_returns_failure_on_control_count_mismatch_when_strict(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            work_dir = tmp_path / "downloads"
            log_dir = tmp_path / "logs"
            metrics_dir = tmp_path / "metrics"
            config_path = self._write_mock_config(
                tmp_path, work_dir, log_dir, metrics_dir, control_strict=True
            )

            fake_client = MagicMock()
            fake_client.get_stats.return_value = {
                "auth_requests": 1,
                "token_refreshes": 1,
                "api_retries": 0,
                "api_401_refresh_retries": 0,
            }

            def write_mock_csv(table_name: str, business_date: str, output_file: Path) -> None:
                output_file.parent.mkdir(parents=True, exist_ok=True)
                output_file.write_text(
                    f"id,name\n1,{table_name}_{business_date}\n",
                    encoding="utf-8",
                )

            fake_client.download_csv.side_effect = write_mock_csv
            fake_client.fetch_control_count.side_effect = [99, 1, 1]
            fake_client.count_downloaded_rows.return_value = 1

            with (
                patch.dict(os.environ, {}, clear=False),
                patch.object(
                    sys,
                    "argv",
                    [
                        "orchestrator.main",
                        "--config",
                        str(config_path),
                        "--asofdate",
                        "20260301",
                    ],
                ),
                patch("orchestrator.main.ApiClient", return_value=fake_client),
                patch("orchestrator.main.ensure_hdfs_dir") as ensure_hdfs_dir_mock,
                patch("orchestrator.main.upload_file") as upload_file_mock,
            ):
                return_code = orchestrator_main.main()

            self.assertEqual(return_code, 1)
            ensure_hdfs_dir_mock.assert_called_once_with("/data/raw/internal_api")
            self.assertEqual(fake_client.download_csv.call_count, 3)
            self.assertEqual(fake_client.fetch_control_count.call_count, 3)
            self.assertEqual(upload_file_mock.call_count, 2)

            metrics_files = list(metrics_dir.glob("run_*.json"))
            self.assertEqual(len(metrics_files), 1)
            metrics_payload = json.loads(metrics_files[0].read_text(encoding="utf-8"))

            self.assertEqual(metrics_payload["counters"]["downloads_success"], 3)
            self.assertEqual(metrics_payload["counters"]["uploads_success"], 2)
            self.assertEqual(metrics_payload["counters"]["table_failures"], 1)
            self.assertEqual(metrics_payload["counters"]["control_validation_mismatch"], 1)
            self.assertEqual(metrics_payload["attributes"]["failures"][0]["table"], "customers")
            self.assertIn(
                "Control count mismatch for customers",
                metrics_payload["attributes"]["failures"][0]["error"],
            )
            self.assertEqual(
                metrics_payload["attributes"]["control_count_customers"],
                {"expected_rows": 99, "actual_rows": 1},
            )

            log_path = log_dir / "orchestrator.log"
            self.assertTrue(log_path.exists())
            log_entries = [
                json.loads(line)
                for line in log_path.read_text(encoding="utf-8").splitlines()
                if line.strip()
            ]
            failure_entries = [
                entry for entry in log_entries if entry["message"] == "Table processing failed"
            ]
            self.assertTrue(failure_entries)
            self.assertEqual(failure_entries[0]["context"]["table"], "customers")
            self.assertIn(
                "Control count mismatch for customers",
                failure_entries[0]["context"]["error"],
            )

    def test_main_logs_warning_and_continues_on_control_count_mismatch_when_not_strict(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            work_dir = tmp_path / "downloads"
            log_dir = tmp_path / "logs"
            metrics_dir = tmp_path / "metrics"
            config_path = self._write_mock_config(
                tmp_path, work_dir, log_dir, metrics_dir, control_strict=False
            )

            fake_client = MagicMock()
            fake_client.get_stats.return_value = {
                "auth_requests": 1,
                "token_refreshes": 1,
                "api_retries": 0,
                "api_401_refresh_retries": 0,
            }

            def write_mock_csv(table_name: str, business_date: str, output_file: Path) -> None:
                output_file.parent.mkdir(parents=True, exist_ok=True)
                output_file.write_text(
                    f"id,name\n1,{table_name}_{business_date}\n",
                    encoding="utf-8",
                )

            fake_client.download_csv.side_effect = write_mock_csv
            fake_client.fetch_control_count.side_effect = [99, 1, 1]
            fake_client.count_downloaded_rows.return_value = 1

            with (
                patch.dict(os.environ, {}, clear=False),
                patch.object(
                    sys,
                    "argv",
                    [
                        "orchestrator.main",
                        "--config",
                        str(config_path),
                        "--asofdate",
                        "20260301",
                    ],
                ),
                patch("orchestrator.main.ApiClient", return_value=fake_client),
                patch("orchestrator.main.ensure_hdfs_dir") as ensure_hdfs_dir_mock,
                patch("orchestrator.main.upload_file") as upload_file_mock,
            ):
                return_code = orchestrator_main.main()

            self.assertEqual(return_code, 0)
            ensure_hdfs_dir_mock.assert_called_once_with("/data/raw/internal_api")
            self.assertEqual(fake_client.download_csv.call_count, 3)
            self.assertEqual(fake_client.fetch_control_count.call_count, 3)
            self.assertEqual(upload_file_mock.call_count, 3)

            metrics_files = list(metrics_dir.glob("run_*.json"))
            self.assertEqual(len(metrics_files), 1)
            metrics_payload = json.loads(metrics_files[0].read_text(encoding="utf-8"))

            self.assertEqual(metrics_payload["counters"]["downloads_success"], 3)
            self.assertEqual(metrics_payload["counters"]["uploads_success"], 3)
            self.assertEqual(metrics_payload["counters"]["control_validation_mismatch"], 1)
            self.assertEqual(metrics_payload["counters"]["control_validation_success"], 2)
            self.assertNotIn("table_failures", metrics_payload["counters"])
            self.assertNotIn("failures", metrics_payload["attributes"])
            self.assertEqual(
                metrics_payload["attributes"]["control_count_customers"],
                {"expected_rows": 99, "actual_rows": 1},
            )

            log_path = log_dir / "orchestrator.log"
            self.assertTrue(log_path.exists())
            log_entries = [
                json.loads(line)
                for line in log_path.read_text(encoding="utf-8").splitlines()
                if line.strip()
            ]
            warning_entries = [
                entry for entry in log_entries if entry["message"] == "Control count mismatch"
            ]
            self.assertTrue(warning_entries)
            self.assertEqual(warning_entries[0]["context"]["table"], "customers")
            self.assertEqual(warning_entries[0]["context"]["expected_rows"], 99)
            self.assertEqual(warning_entries[0]["context"]["actual_rows"], 1)


if __name__ == "__main__":
    unittest.main()
