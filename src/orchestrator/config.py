from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urljoin


def _read_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


@dataclass(frozen=True)
class Settings:
    auth_url: str
    data_url: str
    client_id: str
    api_secret: str
    hdfs_target_dir: str
    hdfs_enabled: bool = True
    base_url: str = ""
    auth_path: str = ""
    data_path_template: str = ""
    data_response_format: str = "csv"
    data_file_extension: str = "csv"
    data_csv_delimiter: str = ","
    data_accept_header: str = ""
    control_url: str = ""
    control_path_template: str = ""
    control_enabled: bool = False
    control_method: str = "GET"
    control_response_format: str = "json"
    control_accept_header: str = ""
    control_file_extension: str = "json"
    control_save_response: bool = False
    control_validation_enabled: bool = True
    control_payload_path: str = ""
    control_count_field: str = "lastUploadCount"
    control_date_field: str = "lastUploadDate"
    control_selection_strategy: str = "latest"
    control_text_count_pattern: str = ""
    control_text_date_pattern: str = ""
    control_strict: bool = True
    control_exclude_header: bool = True
    verify_ssl: bool = True
    asofdate_format: str = "%Y%m%d"
    asofdate_uppercase: bool = False
    auth_method: str = "GET"
    data_method: str = "GET"
    secret_header: str = "X-API-SECRET"
    client_id_param: str = "client_id"
    table_param: str = "table_name"
    business_date_param: str = "business_date"
    token_field: str = "token"
    token_expiry_field: str = "expiringAt"
    token_header: str = "Authorization"
    token_prefix: str = "Bearer"
    request_timeout_seconds: int = 60
    max_retries: int = 3
    retry_backoff_seconds: float = 2.0
    retryable_status_codes: tuple[int, ...] = (429, 500, 502, 503, 504)
    token_validity_minutes: int = 60
    token_refresh_buffer_minutes: int = 5
    work_dir: Path = Path("./downloads")
    log_dir: Path = Path("./logs")
    metrics_dir: Path = Path("./metrics")
    cleanup_local_files: bool = True
    fail_fast: bool = False
    default_tables: tuple[str, ...] = ()

    @classmethod
    def from_sources(cls, config_path: str | None = None) -> "Settings":
        file_config = _load_yaml_config(config_path) if config_path else {}
        api_config = file_config.get("api", {})
        auth_config = api_config.get("auth", {})
        data_config = api_config.get("data", {})
        control_config = api_config.get("control", {})
        token_config = file_config.get("token", {})
        retry_config = file_config.get("retry", {})
        orchestrator_config = file_config.get("orchestrator", {})
        hdfs_config = file_config.get("hdfs", {})
        download_dir = orchestrator_config.get("download_dir", orchestrator_config.get("work_dir", "./downloads"))
        hdfs_target_dir = hdfs_config.get("target_dir", orchestrator_config.get("hdfs_target_dir", ""))

        base_url = os.getenv("API_BASE_URL", str(api_config.get("base_url", ""))).strip()
        auth_path = os.getenv("API_AUTH_PATH", str(auth_config.get("path", ""))).strip()
        data_path_template = os.getenv(
            "API_DATA_PATH_TEMPLATE", str(data_config.get("path_template", ""))
        ).strip()
        auth_url = _resolve_url(
            direct_url=os.getenv("API_AUTH_URL", str(auth_config.get("url", ""))).strip(),
            base_url=base_url,
            path=auth_path,
        )
        data_url = _resolve_url(
            direct_url=os.getenv("API_DATA_URL", str(data_config.get("url", ""))).strip(),
            base_url=base_url,
            path="",
        )
        control_url = _resolve_url(
            direct_url=os.getenv("API_CONTROL_URL", str(control_config.get("url", ""))).strip(),
            base_url=base_url,
            path="",
        )

        settings = cls(
            auth_url=auth_url,
            data_url=data_url,
            client_id=os.getenv("API_CLIENT_ID", str(auth_config.get("client_id", ""))),
            api_secret=os.getenv("API_SECRET", str(auth_config.get("secret", ""))),
            hdfs_target_dir=os.getenv("HDFS_TARGET_DIR", str(hdfs_target_dir)),
            hdfs_enabled=_read_bool(
                "HDFS_ENABLED", bool(hdfs_config.get("enabled", True))
            ),
            base_url=base_url,
            auth_path=auth_path,
            data_path_template=data_path_template,
            data_response_format=os.getenv(
                "API_DATA_RESPONSE_FORMAT",
                str(data_config.get("response_format", "csv")),
            ).lower(),
            data_file_extension=os.getenv(
                "API_DATA_FILE_EXTENSION",
                str(data_config.get("file_extension", "")),
            ).lower()
            or _default_file_extension(
                str(data_config.get("response_format", "csv")).lower()
            ),
            data_csv_delimiter=os.getenv(
                "API_DATA_CSV_DELIMITER",
                str(data_config.get("csv_delimiter", ",")),
            ),
            data_accept_header=os.getenv(
                "API_DATA_ACCEPT_HEADER",
                str(
                    data_config.get(
                        "accept_header",
                        _default_accept_header(
                            str(data_config.get("response_format", "csv")).lower()
                        ),
                    )
                ),
            ),
            control_url=control_url,
            control_path_template=os.getenv(
                "API_CONTROL_PATH_TEMPLATE", str(control_config.get("path_template", ""))
            ).strip(),
            control_enabled=_read_bool(
                "API_CONTROL_ENABLED", bool(control_config.get("enabled", False))
            ),
            control_method=os.getenv(
                "API_CONTROL_METHOD", str(control_config.get("method", "GET"))
            ).upper(),
            control_response_format=os.getenv(
                "API_CONTROL_RESPONSE_FORMAT",
                str(control_config.get("response_format", "json")),
            ).lower(),
            control_accept_header=os.getenv(
                "API_CONTROL_ACCEPT_HEADER",
                str(
                    control_config.get(
                        "accept_header",
                        _default_accept_header(
                            str(control_config.get("response_format", "json")).lower()
                        ),
                    )
                ),
            ),
            control_file_extension=os.getenv(
                "API_CONTROL_FILE_EXTENSION",
                str(control_config.get("file_extension", "")),
            ).lower()
            or _default_file_extension(
                str(control_config.get("response_format", "json")).lower()
            ),
            control_save_response=_read_bool(
                "API_CONTROL_SAVE_RESPONSE",
                bool(control_config.get("save_response", False)),
            ),
            control_validation_enabled=_read_bool(
                "API_CONTROL_VALIDATION_ENABLED",
                bool(control_config.get("validation_enabled", True)),
            ),
            control_payload_path=os.getenv(
                "API_CONTROL_PAYLOAD_PATH", str(control_config.get("payload_path", ""))
            ),
            control_count_field=os.getenv(
                "API_CONTROL_COUNT_FIELD", str(control_config.get("count_field", "lastUploadCount"))
            ),
            control_date_field=os.getenv(
                "API_CONTROL_DATE_FIELD", str(control_config.get("date_field", "lastUploadDate"))
            ),
            control_selection_strategy=os.getenv(
                "API_CONTROL_SELECTION_STRATEGY",
                str(control_config.get("selection_strategy", "latest")),
            ),
            control_text_count_pattern=os.getenv(
                "API_CONTROL_TEXT_COUNT_PATTERN",
                str(control_config.get("text_count_pattern", "")),
            ),
            control_text_date_pattern=os.getenv(
                "API_CONTROL_TEXT_DATE_PATTERN",
                str(control_config.get("text_date_pattern", "")),
            ),
            control_strict=_read_bool(
                "API_CONTROL_STRICT", bool(control_config.get("strict", True))
            ),
            control_exclude_header=_read_bool(
                "API_CONTROL_EXCLUDE_HEADER",
                bool(control_config.get("exclude_header", True)),
            ),
            verify_ssl=_read_bool(
                "API_VERIFY_SSL", bool(api_config.get("verify_ssl", True))
            ),
            asofdate_format=os.getenv(
                "API_ASOFDATE_FORMAT", str(api_config.get("asofdate_format", "%Y%m%d"))
            ),
            asofdate_uppercase=_read_bool(
                "API_ASOFDATE_UPPERCASE", bool(api_config.get("asofdate_uppercase", False))
            ),
            auth_method=os.getenv("API_AUTH_METHOD", str(auth_config.get("method", "GET"))).upper(),
            data_method=os.getenv("API_DATA_METHOD", str(data_config.get("method", "GET"))).upper(),
            secret_header=os.getenv(
                "API_SECRET_HEADER", str(auth_config.get("secret_header", "X-API-SECRET"))
            ),
            client_id_param=os.getenv(
                "API_CLIENT_ID_PARAM", str(auth_config.get("client_id_param", "client_id"))
            ),
            table_param=os.getenv(
                "API_TABLE_PARAM", str(data_config.get("table_param", "table_name"))
            ),
            business_date_param=os.getenv(
                "API_BUSINESS_DATE_PARAM",
                str(data_config.get("business_date_param", "business_date")),
            ),
            token_field=os.getenv("API_TOKEN_FIELD", str(token_config.get("field", "token"))),
            token_expiry_field=os.getenv(
                "API_TOKEN_EXPIRY_FIELD", str(token_config.get("expiry_field", "expiringAt"))
            ),
            token_header=os.getenv(
                "API_TOKEN_HEADER", str(token_config.get("header", "Authorization"))
            ),
            token_prefix=os.getenv(
                "API_TOKEN_PREFIX", str(token_config.get("prefix", "Bearer"))
            ),
            request_timeout_seconds=int(
                os.getenv(
                    "REQUEST_TIMEOUT_SECONDS",
                    str(file_config.get("request_timeout_seconds", "60")),
                )
            ),
            max_retries=int(os.getenv("API_MAX_RETRIES", str(retry_config.get("max_retries", "3")))),
            retry_backoff_seconds=float(
                os.getenv(
                    "API_RETRY_BACKOFF_SECONDS",
                    str(retry_config.get("backoff_seconds", "2.0")),
                )
            ),
            retryable_status_codes=tuple(
                int(code.strip())
                for code in os.getenv(
                    "API_RETRYABLE_STATUS_CODES",
                    ",".join(str(code) for code in retry_config.get("retryable_status_codes", [429, 500, 502, 503, 504])),
                ).split(",")
                if code.strip()
            ),
            token_validity_minutes=int(
                os.getenv(
                    "API_TOKEN_VALIDITY_MINUTES",
                    str(token_config.get("validity_minutes", "60")),
                )
            ),
            token_refresh_buffer_minutes=int(
                os.getenv(
                    "API_TOKEN_REFRESH_BUFFER_MINUTES",
                    str(token_config.get("refresh_buffer_minutes", "5")),
                )
            ),
            work_dir=Path(os.getenv("WORK_DIR", str(download_dir))),
            log_dir=Path(os.getenv("LOG_DIR", str(orchestrator_config.get("log_dir", "./logs")))),
            metrics_dir=Path(
                os.getenv("METRICS_DIR", str(orchestrator_config.get("metrics_dir", "./metrics")))
            ),
            cleanup_local_files=_read_bool(
                "CLEANUP_LOCAL_FILES", bool(orchestrator_config.get("cleanup_local_files", True))
            ),
            fail_fast=_read_bool("FAIL_FAST", bool(orchestrator_config.get("fail_fast", False))),
            default_tables=tuple(str(table) for table in file_config.get("tables", [])),
        )
        settings.validate()
        return settings

    def build_auth_url(self) -> str:
        return _resolve_url(self.auth_url, self.base_url, self.auth_path)

    def build_data_request(self, table_name: str, business_date: str) -> tuple[str, dict[str, str]]:
        if self.data_path_template:
            rendered_path = self.data_path_template.format(
                table_name=table_name,
                business_date=business_date,
                asofdate=business_date,
            )
            return _resolve_url("", self.base_url or self.data_url, rendered_path), {}

        return self.data_url, {
            self.table_param: table_name,
            self.business_date_param: business_date,
        }

    def build_control_request(
        self,
        table_name: str,
        business_date: str,
    ) -> tuple[str, dict[str, str]]:
        if self.control_path_template:
            rendered_path = self.control_path_template.format(
                table_name=table_name,
                business_date=business_date,
                asofdate=business_date,
            )
            return _resolve_url("", self.base_url or self.control_url, rendered_path), {}

        return self.control_url, {
            self.table_param: table_name,
            self.business_date_param: business_date,
        }

    def validate(self) -> None:
        missing_fields: list[str] = []
        if not self.client_id:
            missing_fields.append("api.auth.client_id")
        if not self.api_secret:
            missing_fields.append("api.auth.secret")
        if self.hdfs_enabled and not self.hdfs_target_dir:
            missing_fields.append("hdfs.target_dir")
        if not self.auth_url and not (self.base_url and self.auth_path):
            missing_fields.append("api.auth.url or api.base_url + api.auth.path")
        if not self.data_url and not (self.base_url and self.data_path_template):
            missing_fields.append("api.data.url or api.base_url + api.data.path_template")
        if self.data_response_format not in {"csv", "json"}:
            missing_fields.append("api.data.response_format must be 'csv' or 'json'")
        if self.data_response_format == "csv" and len(self.data_csv_delimiter) != 1:
            missing_fields.append("api.data.csv_delimiter must be a single character")
        if self.control_enabled and not self.control_url and not (
            self.base_url and self.control_path_template
        ):
            missing_fields.append(
                "api.control.url or api.base_url + api.control.path_template"
            )

        if missing_fields:
            raise RuntimeError(
                "Missing required orchestrator configuration: " + ", ".join(missing_fields)
            )


def _load_yaml_config(config_path: str) -> dict:
    try:
        import yaml
    except ModuleNotFoundError as exc:
        raise RuntimeError("PyYAML is required for --config support. Install requirements.txt.") from exc

    config_file = Path(config_path)
    payload = yaml.safe_load(config_file.read_text(encoding="utf-8"))
    if payload is None:
        return {}
    if not isinstance(payload, dict):
        raise RuntimeError(f"YAML config must be a mapping at the top level: {config_file}")
    return payload


def _resolve_url(direct_url: str, base_url: str, path: str) -> str:
    if direct_url:
        return direct_url
    if not base_url or not path:
        return direct_url
    return urljoin(_normalize_base_url(base_url), path.lstrip("/"))


def _normalize_base_url(base_url: str) -> str:
    return base_url if base_url.endswith("/") else f"{base_url}/"


def _default_file_extension(response_format: str) -> str:
    if response_format == "json":
        return "json"
    return "csv"


def _default_accept_header(response_format: str) -> str:
    if response_format == "json":
        return "application/json"
    return "text/csv"
