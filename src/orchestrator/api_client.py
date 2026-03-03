from __future__ import annotations

import json
import logging
import random
import re
import time
from csv import reader
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import requests
from requests import HTTPError

from orchestrator.config import Settings


LOGGER = logging.getLogger("orchestrator.api_client")


class RetryableApiError(RuntimeError):
    pass


class UnauthorizedApiError(RuntimeError):
    pass


@dataclass
class TokenState:
    value: str
    expires_at: datetime


@dataclass
class ControlResponseDetails:
    record_count: int
    record_date: str | None
    raw_payload: Any
    response_format: str


class ApiClient:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.session = requests.Session()
        self._token_state: TokenState | None = None
        self._stats: dict[str, int] = {
            "auth_requests": 0,
            "token_refreshes": 0,
            "api_retries": 0,
            "api_401_refresh_retries": 0,
        }

    def get_valid_token(self, force_refresh: bool = False) -> str:
        if force_refresh or self._should_refresh_token():
            self._refresh_token()
        if self._token_state is None:
            raise RuntimeError("Token state was not initialized.")
        return self._token_state.value

    def download_csv(self, table_name: str, business_date: str, output_file: Path) -> None:
        token = self.get_valid_token()
        try:
            self._download_csv_with_token(token, table_name, business_date, output_file)
        except UnauthorizedApiError:
            LOGGER.info(
                "Refreshing token after unauthorized response",
                extra={"context": {"table": table_name}},
            )
            self._stats["api_401_refresh_retries"] += 1
            token = self.get_valid_token(force_refresh=True)
            self._download_csv_with_token(token, table_name, business_date, output_file)

    def fetch_control_count(self, table_name: str, business_date: str) -> int:
        return self.fetch_control_details(table_name, business_date).record_count

    def fetch_control_details(self, table_name: str, business_date: str) -> ControlResponseDetails:
        token = self.get_valid_token()
        try:
            return self._fetch_control_details_with_token(token, table_name, business_date)
        except UnauthorizedApiError:
            LOGGER.info(
                "Refreshing token after unauthorized control response",
                extra={"context": {"table": table_name}},
            )
            self._stats["api_401_refresh_retries"] += 1
            token = self.get_valid_token(force_refresh=True)
            return self._fetch_control_details_with_token(token, table_name, business_date)

    def get_stats(self) -> dict[str, int]:
        return dict(self._stats)

    def _refresh_token(self) -> None:
        token, expires_at = self.fetch_token()
        self._token_state = TokenState(value=token, expires_at=expires_at)
        self._stats["token_refreshes"] += 1
        LOGGER.info(
            "Token refreshed",
            extra={
                "context": {
                    "expires_at": expires_at.isoformat(),
                    "refresh_buffer_minutes": self.settings.token_refresh_buffer_minutes,
                }
            },
        )

    def _should_refresh_token(self) -> bool:
        if self._token_state is None:
            return True
        refresh_deadline = self._token_state.expires_at - timedelta(
            minutes=self.settings.token_refresh_buffer_minutes
        )
        return datetime.now(timezone.utc) >= refresh_deadline

    def fetch_token(self) -> tuple[str, datetime]:
        self._stats["auth_requests"] += 1
        headers = {self.settings.secret_header: self.settings.api_secret}
        params = {self.settings.client_id_param: self.settings.client_id}
        response = self._request(
            method=self.settings.auth_method,
            url=self.settings.build_auth_url(),
            headers=headers,
            params=params,
            request_name="auth",
        )

        try:
            payload: Any = response.json()
        except json.JSONDecodeError as exc:
            raise RuntimeError("Auth response was not valid JSON.") from exc

        token = payload.get(self.settings.token_field)
        if not token:
            raise RuntimeError(
                f"Token field '{self.settings.token_field}' not found in auth response."
            )
        expires_at = self._resolve_expiration(payload)
        return str(token), expires_at

    def _download_csv_with_token(
        self,
        token: str,
        table_name: str,
        business_date: str,
        output_file: Path,
    ) -> None:
        headers = {
            self.settings.token_header: f"{self.settings.token_prefix} {token}".strip()
        }
        if self.settings.data_accept_header:
            headers["Accept"] = self.settings.data_accept_header
        data_url, params = self.settings.build_data_request(table_name, business_date)
        response = self._request(
            method=self.settings.data_method,
            url=data_url,
            headers=headers,
            params=params,
            stream=True,
            request_name="data",
            table_name=table_name,
        )

        output_file.parent.mkdir(parents=True, exist_ok=True)
        bytes_written = self._write_downloaded_content(response, output_file)

        content_type = response.headers.get("Content-Type", "")
        LOGGER.info(
            "Download data response",
            extra={
                "context": {
                    "table": table_name,
                    "url": data_url,
                    "status_code": response.status_code,
                    "content_type": content_type,
                    "response_format": self.settings.data_response_format,
                    "bytes_written": bytes_written,
                    "output_file": str(output_file),
                }
            },
        )
        if bytes_written == 0:
            raise RuntimeError(
                f"Downloaded file was empty for table {table_name}. "
                f"status={response.status_code}, content_type={content_type or 'unknown'}"
            )

    def _write_downloaded_content(self, response: requests.Response, output_file: Path) -> int:
        if self.settings.data_response_format == "json":
            payload = response.json()
            serialized = json.dumps(payload, indent=2)
            output_file.write_text(serialized, encoding="utf-8")
            return len(serialized.encode("utf-8"))

        bytes_written = 0
        with output_file.open("wb") as handle:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    handle.write(chunk)
                    bytes_written += len(chunk)
        return bytes_written

    def _fetch_control_details_with_token(
        self,
        token: str,
        table_name: str,
        business_date: str,
    ) -> ControlResponseDetails:
        headers = {
            self.settings.token_header: f"{self.settings.token_prefix} {token}".strip()
        }
        if self.settings.control_accept_header:
            headers["Accept"] = self.settings.control_accept_header
        control_url, params = self.settings.build_control_request(table_name, business_date)
        response = self._request(
            method=self.settings.control_method,
            url=control_url,
            headers=headers,
            params=params,
            request_name="control",
            table_name=table_name,
        )
        return self._parse_control_details(response)

    def _request(
        self,
        method: str,
        url: str,
        headers: dict[str, str],
        params: dict[str, str],
        stream: bool = False,
        request_name: str = "api",
        table_name: str | None = None,
    ) -> requests.Response:
        max_attempts = self.settings.max_retries + 1
        for attempt in range(1, max_attempts + 1):
            try:
                self._log_request(
                    request_name=request_name,
                    method=method,
                    url=url,
                    params=params,
                    stream=stream,
                    table_name=table_name,
                    attempt=attempt,
                )
                response = self.session.request(
                    method=method,
                    url=url,
                    headers=headers,
                    params=params,
                    timeout=self.settings.request_timeout_seconds,
                    stream=stream,
                    verify=self.settings.verify_ssl,
                )
                response.raise_for_status()
                return response
            except HTTPError as exc:
                if exc.response is not None and exc.response.status_code == 401:
                    raise UnauthorizedApiError("API request failed with status 401.") from exc
                should_retry = (
                    exc.response is not None
                    and exc.response.status_code in self.settings.retryable_status_codes
                    and attempt < max_attempts
                )
                if should_retry:
                    self._retry_sleep(
                        attempt=attempt,
                        url=url,
                        reason=f"status {exc.response.status_code}",
                    )
                    continue
                body = exc.response.text[:500] if exc.response is not None else ""
                raise RuntimeError(
                    f"API request failed for {url} with status "
                    f"{exc.response.status_code if exc.response is not None else 'unknown'}: {body}"
                ) from exc
            except requests.RequestException as exc:
                if attempt < max_attempts:
                    self._retry_sleep(attempt=attempt, url=url, reason=str(exc))
                    continue
                raise RuntimeError(f"API request failed for {url}: {exc}") from exc

        raise RetryableApiError(f"API request exhausted retries for {url}.")

    def _log_request(
        self,
        request_name: str,
        method: str,
        url: str,
        params: dict[str, str],
        stream: bool,
        table_name: str | None,
        attempt: int,
    ) -> None:
        context: dict[str, Any] = {
            "request_name": request_name,
            "method": method,
            "url": url,
            "params": params,
            "stream": stream,
            "attempt": attempt,
        }
        if table_name:
            context["table"] = table_name
        LOGGER.info("Calling API", extra={"context": context})

    def count_downloaded_records(self, output_file: Path) -> int:
        if self.settings.data_response_format == "json":
            payload = json.loads(output_file.read_text(encoding="utf-8"))
            if isinstance(payload, list):
                return len(payload)
            if isinstance(payload, dict):
                return 1
            raise RuntimeError("Unsupported JSON download payload for record counting.")

        with output_file.open("r", encoding="utf-8", newline="") as handle:
            total_rows = sum(
                1 for row in reader(handle, delimiter=self.settings.data_csv_delimiter) if row
            )
        if self.settings.control_exclude_header and total_rows > 0:
            return total_rows - 1
        return total_rows

    def write_control_response(
        self,
        details: ControlResponseDetails,
        output_file: Path,
    ) -> None:
        output_file.parent.mkdir(parents=True, exist_ok=True)
        if details.response_format == "json":
            serialized = json.dumps(details.raw_payload, indent=2)
            output_file.write_text(serialized, encoding="utf-8")
            return

        output_file.write_text(str(details.raw_payload), encoding="utf-8")

    def _retry_sleep(self, attempt: int, url: str, reason: str) -> None:
        delay = self.settings.retry_backoff_seconds * (2 ** (attempt - 1))
        jitter = random.uniform(0, delay * 0.2)
        sleep_seconds = delay + jitter
        self._stats["api_retries"] += 1
        LOGGER.warning(
            "Retrying API request",
            extra={
                "context": {
                    "url": url,
                    "attempt": attempt,
                    "sleep_seconds": round(sleep_seconds, 3),
                    "reason": reason,
                }
            },
        )
        time.sleep(sleep_seconds)

    def _parse_control_details(self, response: requests.Response) -> ControlResponseDetails:
        if self.settings.control_response_format == "text":
            count, record_date = self._parse_control_count_from_text(response.text)
            return ControlResponseDetails(
                record_count=count,
                record_date=record_date,
                raw_payload=response.text,
                response_format="text",
            )

        try:
            payload: Any = response.json()
        except json.JSONDecodeError as exc:
            raise RuntimeError("Control response was not valid JSON.") from exc

        if self.settings.control_payload_path:
            payload = self._extract_by_path(payload, self.settings.control_payload_path)

        if isinstance(payload, int):
            return ControlResponseDetails(
                record_count=payload,
                record_date=None,
                raw_payload=payload,
                response_format="json",
            )
        if isinstance(payload, dict):
            count_value = self._extract_by_path(payload, self.settings.control_count_field)
            if count_value is None:
                raise RuntimeError(
                    f"Control field '{self.settings.control_count_field}' not found in response."
                )
            try:
                record_count = int(count_value)
            except (TypeError, ValueError) as exc:
                raise RuntimeError(
                    f"Control field '{self.settings.control_count_field}' was not numeric."
                ) from exc
            record_date = self._extract_by_path(payload, self.settings.control_date_field)
            return ControlResponseDetails(
                record_count=record_count,
                record_date=str(record_date) if record_date is not None else None,
                raw_payload=payload,
                response_format="json",
            )

        if isinstance(payload, list):
            if not payload:
                raise RuntimeError("Control response list was empty.")
            selected_record = self._select_control_record(payload)
            count_value = self._extract_by_path(
                selected_record, self.settings.control_count_field
            )
            if count_value is None:
                raise RuntimeError(
                    f"Control field '{self.settings.control_count_field}' not found in selected control record."
                )
            try:
                record_count = int(count_value)
            except (TypeError, ValueError) as exc:
                raise RuntimeError(
                    f"Control field '{self.settings.control_count_field}' was not numeric."
                ) from exc
            record_date = self._extract_by_path(
                selected_record, self.settings.control_date_field
            )
            return ControlResponseDetails(
                record_count=record_count,
                record_date=str(record_date) if record_date is not None else None,
                raw_payload=payload,
                response_format="json",
            )

        raise RuntimeError("Unsupported control response payload.")

    def _select_control_record(self, payload: list[Any]) -> dict[str, Any]:
        dict_records = [item for item in payload if isinstance(item, dict)]
        if not dict_records:
            raise RuntimeError("Control response list did not contain JSON objects.")

        if self.settings.control_selection_strategy == "first":
            return dict_records[0]

        if self.settings.control_selection_strategy == "latest":
            dated_records: list[tuple[datetime, dict[str, Any]]] = []
            for record in dict_records:
                raw_date = self._extract_by_path(record, self.settings.control_date_field)
                parsed_date = self._parse_timestamp(raw_date)
                if parsed_date is not None:
                    dated_records.append((parsed_date, record))
            if dated_records:
                dated_records.sort(key=lambda item: item[0], reverse=True)
                return dated_records[0][1]
            return dict_records[0]

        raise RuntimeError(
            f"Unsupported control selection strategy: {self.settings.control_selection_strategy}"
        )

    def _parse_control_count_from_text(self, raw_text: str) -> tuple[int, str | None]:
        count_pattern = self.settings.control_text_count_pattern
        date_pattern = self.settings.control_text_date_pattern

        if not count_pattern:
            try:
                return int(raw_text.strip()), None
            except ValueError as exc:
                raise RuntimeError(
                    "Control text response requires API_CONTROL_TEXT_COUNT_PATTERN "
                    "or must be a plain integer."
                ) from exc

        if date_pattern:
            counts = list(re.finditer(count_pattern, raw_text, re.MULTILINE))
            dates = list(re.finditer(date_pattern, raw_text, re.MULTILINE))
            if not counts:
                raise RuntimeError("Control text count pattern did not match response.")
            if not dates:
                raise RuntimeError("Control text date pattern did not match response.")

            record_count = min(len(counts), len(dates))
            records: list[dict[str, Any]] = []
            for index in range(record_count):
                records.append(
                    {
                        self.settings.control_count_field: counts[index].group(1),
                        self.settings.control_date_field: dates[index].group(1),
                    }
                )
            selected_record = self._select_control_record(records)
            count_value = self._extract_by_path(
                selected_record, self.settings.control_count_field
            )
            record_date = self._extract_by_path(
                selected_record, self.settings.control_date_field
            )
            return int(count_value), str(record_date) if record_date is not None else None

        match = re.search(count_pattern, raw_text, re.MULTILINE)
        if not match:
            raise RuntimeError("Control text count pattern did not match response.")
        return int(match.group(1)), None

    def _extract_by_path(self, payload: Any, path: str) -> Any:
        if not path:
            return payload

        current = payload
        for part in path.split("."):
            if isinstance(current, dict):
                current = current.get(part)
            elif isinstance(current, list) and part.isdigit():
                index = int(part)
                current = current[index] if 0 <= index < len(current) else None
            else:
                return None
            if current is None:
                return None
        return current

    def _resolve_expiration(self, payload: dict[str, Any]) -> datetime:
        raw_expiration = payload.get(self.settings.token_expiry_field)
        if raw_expiration:
            parsed_expiration = self._parse_expiration(raw_expiration)
            if parsed_expiration is not None:
                return parsed_expiration
            LOGGER.warning(
                "Unable to parse token expiration from auth response, using configured fallback",
                extra={
                    "context": {
                        "expiry_field": self.settings.token_expiry_field,
                        "raw_value": str(raw_expiration),
                    }
                },
            )

        return datetime.now(timezone.utc) + timedelta(
            minutes=self.settings.token_validity_minutes
        )

    def _parse_expiration(self, raw_expiration: Any) -> datetime | None:
        return self._parse_timestamp(raw_expiration)

    def _parse_timestamp(self, raw_value: Any) -> datetime | None:
        if isinstance(raw_value, (int, float)):
            return datetime.fromtimestamp(float(raw_value), tz=timezone.utc)

        if isinstance(raw_value, str):
            candidate = raw_value.strip()
            if not candidate:
                return None

            if "[" in candidate and candidate.endswith("]"):
                candidate = candidate.split("[", 1)[0]

            if candidate.endswith("Z"):
                candidate = candidate[:-1] + "+00:00"

            if len(candidate) >= 6 and (candidate[-6] in {"+", "-"}) and candidate[-3] == ".":
                candidate = f"{candidate[:-3]}:{candidate[-2:]}"

            try:
                parsed = datetime.fromisoformat(candidate)
            except ValueError:
                return None

            if parsed.tzinfo is None:
                return parsed.replace(tzinfo=timezone.utc)
            return parsed.astimezone(timezone.utc)

        return None
