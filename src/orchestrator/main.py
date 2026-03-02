from __future__ import annotations

import argparse
import logging
import shutil
import sys
from datetime import datetime
from pathlib import Path

from orchestrator.api_client import ApiClient
from orchestrator.config import Settings
from orchestrator.hdfs import ensure_hdfs_dir, upload_file
from orchestrator.logging_utils import configure_logging
from orchestrator.metrics import MetricsCollector


LOGGER = logging.getLogger("orchestrator")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Internal API to HDFS orchestration framework")
    parser.add_argument("--config", help="YAML config file for API/orchestrator settings")
    parser.add_argument("--tables", nargs="*", default=[], help="List of source table names")
    parser.add_argument("--table-file", help="File with one table name per line")
    parser.add_argument(
        "--asofdate",
        "--business-date",
        dest="asofdate",
        help="As-of date in yyyyMMdd format. Defaults to current date.",
    )
    return parser.parse_args()


def load_tables(args: argparse.Namespace, settings: Settings) -> list[str]:
    tables = list(settings.default_tables)
    tables.extend(args.tables)
    if args.table_file:
        table_file = Path(args.table_file)
        file_tables = [
            line.strip()
            for line in table_file.read_text(encoding="utf-8").splitlines()
            if line.strip() and not line.strip().startswith("#")
        ]
        tables.extend(file_tables)

    unique_tables = []
    seen = set()
    for table in tables:
        if table not in seen:
            seen.add(table)
            unique_tables.append(table)
    return unique_tables


def resolve_business_date(raw_value: str | None) -> str:
    if raw_value:
        datetime.strptime(raw_value, "%Y%m%d")
        return raw_value
    return datetime.now().strftime("%Y%m%d")


def cleanup_run_dir(run_dir: Path) -> None:
    if run_dir.exists():
        shutil.rmtree(run_dir)


def main() -> int:
    args = parse_args()
    settings = Settings.from_sources(args.config)
    log_path = configure_logging(settings.log_dir)

    tables = load_tables(args, settings)
    if not tables:
        raise ValueError("No tables were provided. Use --tables or --table-file.")

    business_date = resolve_business_date(args.asofdate)
    run_id = datetime.now().strftime("%Y%m%d%H%M%S")
    run_dir = settings.work_dir / run_id

    metrics = MetricsCollector(run_id=run_id)
    metrics.record_attribute("business_date", business_date)
    metrics.record_attribute("table_count", len(tables))
    metrics.record_attribute("log_path", str(log_path))

    client = ApiClient(settings)

    LOGGER.info("Starting orchestration run", extra={"context": {"run_id": run_id}})

    try:
        with metrics.track("fetch_token"):
            client.get_valid_token()
        metrics.increment("auth_success")

        with metrics.track("ensure_hdfs_dir", hdfs_target_dir=settings.hdfs_target_dir):
            ensure_hdfs_dir(settings.hdfs_target_dir)

        failures: list[dict[str, str]] = []

        for table in tables:
            file_path = run_dir / f"{table}_{business_date}.csv"
            LOGGER.info(
                "Processing table",
                extra={"context": {"table": table, "business_date": business_date}},
            )
            try:
                with metrics.track("download_csv", table=table):
                    client.download_csv(table, business_date, file_path)
                metrics.increment("downloads_success")

                if settings.control_enabled:
                    with metrics.track("validate_control_count", table=table):
                        expected_rows = client.fetch_control_count(table, business_date)
                        actual_rows = client.count_downloaded_rows(file_path)
                        metrics.record_attribute(
                            f"control_count_{table}",
                            {
                                "expected_rows": expected_rows,
                                "actual_rows": actual_rows,
                            },
                        )
                        if expected_rows != actual_rows:
                            metrics.increment("control_validation_mismatch")
                            message = (
                                f"Control count mismatch for {table}: "
                                f"expected {expected_rows}, actual {actual_rows}"
                            )
                            if settings.control_strict:
                                raise RuntimeError(message)
                            LOGGER.warning(
                                "Control count mismatch",
                                extra={
                                    "context": {
                                        "table": table,
                                        "expected_rows": expected_rows,
                                        "actual_rows": actual_rows,
                                    }
                                },
                            )
                        else:
                            metrics.increment("control_validation_success")

                with metrics.track("upload_hdfs", table=table):
                    upload_file(file_path, settings.hdfs_target_dir)
                metrics.increment("uploads_success")

                if settings.cleanup_local_files:
                    with metrics.track("cleanup_file", table=table):
                        file_path.unlink(missing_ok=True)
                    metrics.increment("cleanup_success")
            except Exception as exc:
                LOGGER.exception(
                    "Table processing failed",
                    extra={"context": {"table": table, "error": str(exc)}},
                )
                metrics.increment("table_failures")
                failures.append({"table": table, "error": str(exc)})
                if settings.fail_fast:
                    raise

        if failures:
            metrics.record_attribute("failures", failures)
            return_code = 1
        else:
            return_code = 0

    except Exception as exc:
        LOGGER.exception("Run failed", extra={"context": {"error": str(exc)}})
        metrics.increment("run_failures")
        return_code = 1
    finally:
        for stat_name, stat_value in client.get_stats().items():
            metrics.record_attribute(stat_name, stat_value)
        if settings.cleanup_local_files:
            with metrics.track("cleanup_run_dir", run_id=run_id):
                cleanup_run_dir(run_dir)
        metrics_path = metrics.write(settings.metrics_dir)
        LOGGER.info(
            "Run completed",
            extra={
                "context": {
                    "run_id": run_id,
                    "metrics_path": str(metrics_path),
                    "return_code": return_code,
                }
            },
        )

    return return_code


if __name__ == "__main__":
    sys.exit(main())
