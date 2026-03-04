from __future__ import annotations

import argparse
import logging
import re
import shutil
import sys
from datetime import datetime, timezone
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


def resolve_business_date(settings: Settings, raw_value: str | None) -> str:
    if raw_value:
        normalized_input = raw_value.strip()
        if _matches_date_format(normalized_input, "%Y%m%d"):
            parsed_date = datetime.strptime(normalized_input, "%Y%m%d")
            return _format_asofdate(settings, parsed_date)

        for input_format in _candidate_asofdate_input_formats(settings):
            if input_format == "%Y%m%d":
                continue
            try:
                datetime.strptime(normalized_input, input_format)
                return (
                    normalized_input.upper()
                    if settings.asofdate_uppercase
                    else normalized_input
                )
            except ValueError:
                continue
        raise ValueError(
            "Invalid --asofdate value. Supported inputs include yyyyMMdd and the configured "
            f"API format {settings.asofdate_format!r}."
        )
    return _format_asofdate(settings, datetime.now())


def _candidate_asofdate_input_formats(settings: Settings) -> list[str]:
    candidates = [
        "%Y%m%d",
        settings.asofdate_format,
        "%d-%b-%Y",
        "%d-%B-%Y",
    ]
    unique_candidates: list[str] = []
    for candidate in candidates:
        if candidate not in unique_candidates:
            unique_candidates.append(candidate)
    return unique_candidates


def _format_asofdate(settings: Settings, value: datetime) -> str:
    formatted = value.strftime(settings.asofdate_format)
    return formatted.upper() if settings.asofdate_uppercase else formatted


def _matches_date_format(value: str, date_format: str) -> bool:
    try:
        datetime.strptime(value, date_format)
        return True
    except ValueError:
        return False


def cleanup_run_dir(run_dir: Path) -> None:
    if run_dir.exists():
        shutil.rmtree(run_dir)


def write_control_metadata_file(
    output_file: Path,
    data_file_path: Path,
    record_count: int,
    business_date: str,
    business_date_format_label: str,
    time_zone: str,
) -> None:
    output_file.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        f"file_name| {data_file_path.name}",
        f"record_count| {record_count}",
        f"business_date| {business_date}",
        f"business_date_format| {business_date_format_label}",
        f"time_zone| {time_zone}",
    ]
    output_file.write_text("\n".join(lines) + "\n", encoding="utf-8")


def resolve_control_file_business_date(
    settings: Settings,
    asofdate_value: str,
    control_date_value: str | None,
) -> str:
    source_value = asofdate_value
    if settings.control_metadata_business_date_source == "control_date":
        source_value = control_date_value or asofdate_value

    parsed_value = _parse_datetime_value(source_value, settings)
    if parsed_value is None:
        raise ValueError(
            f"Unable to parse business date source value for control file: {source_value!r}"
        )
    return parsed_value.strftime(settings.control_metadata_business_date_format)


def resolve_control_file_time_zone(settings: Settings, control_date_value: str | None) -> str:
    if not control_date_value:
        return settings.control_metadata_default_timezone

    bracket_match = re.search(r"\[([^\]]+)\]\s*$", control_date_value)
    if bracket_match:
        return bracket_match.group(1)

    token_match = re.search(r"\b(UTC|GMT|EST|EDT|CST|CDT|MST|MDT|PST|PDT)\b", control_date_value)
    if token_match:
        return token_match.group(1)

    normalized = control_date_value.strip()
    if normalized.endswith("Z") or re.search(r"[+-]\d{2}:\d{2}$", normalized):
        return "UTC"
    if re.search(r"[+-]\d{2}\.\d{2}$", normalized):
        return "UTC"

    return settings.control_metadata_default_timezone


def _parse_datetime_value(raw_value: str, settings: Settings) -> datetime | None:
    candidate = raw_value.strip()
    if not candidate:
        return None

    for date_format in _candidate_asofdate_input_formats(settings):
        try:
            return datetime.strptime(candidate, date_format)
        except ValueError:
            continue

    for date_format in ("%Y-%m-%d", "%Y/%m/%d"):
        try:
            return datetime.strptime(candidate, date_format)
        except ValueError:
            continue

    normalized = re.sub(r"\s*T\s*", "T", candidate)
    if "[" in normalized and normalized.endswith("]"):
        normalized = normalized.split("[", 1)[0]
    if normalized.endswith("Z"):
        normalized = normalized[:-1] + "+00:00"
    if len(normalized) >= 6 and (normalized[-6] in {"+", "-"}) and normalized[-3] == ".":
        normalized = f"{normalized[:-3]}:{normalized[-2:]}"

    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return None

    if parsed.tzinfo is not None:
        return parsed.astimezone(timezone.utc).replace(tzinfo=None)
    return parsed


def main() -> int:
    args = parse_args()
    settings = Settings.from_sources(args.config)
    log_path = configure_logging(settings.log_dir)

    tables = load_tables(args, settings)
    if not tables:
        raise ValueError("No tables were provided. Use --tables or --table-file.")

    business_date = resolve_business_date(settings, args.asofdate)
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

        if settings.hdfs_enabled:
            with metrics.track("ensure_hdfs_dir", hdfs_target_dir=settings.hdfs_target_dir):
                ensure_hdfs_dir(settings.hdfs_target_dir)

        failures: list[dict[str, str]] = []

        for table in tables:
            file_path = run_dir / f"{table}_{business_date}.{settings.data_file_extension}"
            LOGGER.info(
                "Processing table",
                extra={"context": {"table": table, "business_date": business_date}},
            )
            try:
                with metrics.track("download_csv", table=table):
                    client.download_csv(table, business_date, file_path)
                metrics.increment("downloads_success")
                control_validation_succeeded = False
                control_validation_enabled = False
                control_details = None

                if settings.control_enabled:
                    with metrics.track("fetch_control_response", table=table):
                        control_details = client.fetch_control_details(table, business_date)
                    metrics.record_attribute(
                        f"control_response_{table}",
                        {
                            "record_count": control_details.record_count,
                            "record_date": control_details.record_date,
                        },
                    )

                    if settings.control_save_response:
                        control_file_path = (
                            run_dir
                            / f"{table}_{business_date}_control.{settings.control_file_extension}"
                        )
                        with metrics.track("save_control_response", table=table):
                            client.write_control_response(control_details, control_file_path)
                        metrics.increment("control_response_saved")

                    if settings.control_validation_enabled:
                        control_validation_enabled = True
                        with metrics.track("validate_control_count", table=table):
                            expected_rows = control_details.record_count
                            actual_rows = client.count_downloaded_records(file_path)
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
                                control_validation_succeeded = True
                    else:
                        metrics.increment("control_validation_skipped")

                    if settings.control_metadata_file_enabled:
                        should_create_control_file = (
                            control_details is not None
                            and (
                                not control_validation_enabled
                                or control_validation_succeeded
                            )
                        )
                        if should_create_control_file:
                            control_metadata_path = (
                                file_path.parent
                                / f"{file_path.stem}.{settings.control_metadata_file_extension}"
                            )
                            control_file_business_date = resolve_control_file_business_date(
                                settings=settings,
                                asofdate_value=business_date,
                                control_date_value=control_details.record_date,
                            )
                            control_file_time_zone = resolve_control_file_time_zone(
                                settings=settings,
                                control_date_value=control_details.record_date,
                            )
                            with metrics.track("write_control_file", table=table):
                                write_control_metadata_file(
                                    output_file=control_metadata_path,
                                    data_file_path=file_path,
                                    record_count=control_details.record_count,
                                    business_date=control_file_business_date,
                                    business_date_format_label=settings.control_metadata_business_date_format_label,
                                    time_zone=control_file_time_zone,
                                )
                            metrics.increment("control_file_created")
                            LOGGER.info(
                                "Control metadata file created",
                                extra={
                                    "context": {
                                        "table": table,
                                        "control_file": str(control_metadata_path),
                                    }
                                },
                            )
                        else:
                            metrics.increment("control_file_skipped")

                if settings.hdfs_enabled:
                    with metrics.track("upload_hdfs", table=table):
                        upload_file(file_path, settings.hdfs_target_dir)
                    metrics.increment("uploads_success")
                else:
                    metrics.increment("uploads_skipped")

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
