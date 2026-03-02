from __future__ import annotations

import sys
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from orchestrator.hdfs import ensure_hdfs_dir, upload_file


class HdfsTests(unittest.TestCase):
    @patch("orchestrator.hdfs.subprocess.run")
    def test_ensure_hdfs_dir_invokes_hdfs_cli(self, run_mock: MagicMock) -> None:
        ensure_hdfs_dir("/data/raw/internal_api")

        run_mock.assert_called_once_with(
            ["hdfs", "dfs", "-mkdir", "-p", "/data/raw/internal_api"],
            check=True,
            capture_output=True,
            text=True,
        )

    @patch("orchestrator.hdfs.subprocess.run")
    def test_upload_file_invokes_hdfs_put(self, run_mock: MagicMock) -> None:
        upload_file(Path("/tmp/orders.csv"), "/data/raw/internal_api")

        run_mock.assert_called_once_with(
            ["hdfs", "dfs", "-put", "-f", "/tmp/orders.csv", "/data/raw/internal_api"],
            check=True,
            capture_output=True,
            text=True,
        )


if __name__ == "__main__":
    unittest.main()
