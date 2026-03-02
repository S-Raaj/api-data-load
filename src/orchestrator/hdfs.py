from __future__ import annotations

import subprocess
from pathlib import Path


def ensure_hdfs_dir(hdfs_target_dir: str) -> None:
    try:
        subprocess.run(
            ["hdfs", "dfs", "-mkdir", "-p", hdfs_target_dir],
            check=True,
            capture_output=True,
            text=True,
        )
    except subprocess.CalledProcessError as exc:
        raise RuntimeError(exc.stderr.strip() or exc.stdout.strip() or str(exc)) from exc


def upload_file(local_file: Path, hdfs_target_dir: str) -> None:
    try:
        subprocess.run(
            ["hdfs", "dfs", "-put", "-f", str(local_file), hdfs_target_dir],
            check=True,
            capture_output=True,
            text=True,
        )
    except subprocess.CalledProcessError as exc:
        raise RuntimeError(
            f"HDFS upload failed for {local_file.name}: "
            f"{exc.stderr.strip() or exc.stdout.strip() or str(exc)}"
        ) from exc
