from __future__ import annotations

import json
import logging
import random
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
        token = self.get_valid_token()
        try:
            return self._fetch_control_count_with_token(token, table_name, business_date)
        except UnauthorizedApiError:
            LOGGER.info(
                "Refreshing token after unauthorized control response",
                extra={"context": {"table": table_name}},
            )
            self._stats["api_401_refresh_retries"] += 1
            token = self.get_valid_token(force_refresh=True)
            return self._fetch_control_count_with_token(token, table_name, business_date)

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
        headers = {self.settings.token_header: f"{self.settings.token_prefix} {token}".strip()}
        data_url, params = self.settings.build_data_request(table_name, business_date)
        response = self._request(
            method=self.settings.data_method,
            url=data_url,
            headers=headers,
            params=params,
            stream=True,
        )

        output_file.parent.mkdir(parents=True, exist_ok=True)
        with output_file.open("wb") as handle:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    handle.write(chunk)

    def _fetch_control_count_with_token(
        self,
        token: str,
        table_name: str,
        business_date: str,
    ) -> int:
        headers = {self.settings.token_header: f"{self.settings.token_prefix} {token}".strip()}
        control_url, params = self.settings.build_control_request(table_name, business_date)
        response = self._request(
            method=self.settings.control_method,
            url=control_url,
            headers=headers,
            params=params,
        )
        return self._parse_control_count(response)

    def _request(
        self,
        method: str,
        url: str,
        headers: dict[str, str],
        params: dict[str, str],
        stream: bool = False,
    ) -> requests.Response:
        max_attempts = self.settings.max_retries + 1
        for attempt in range(1, max_attempts + 1):
            try:
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

    def count_downloaded_rows(self, output_file: Path) -> int:
        with output_file.open("r", encoding="utf-8", newline="") as handle:
            total_rows = sum(1 for row in reader(handle) if row)
        if self.settings.control_exclude_header and total_rows > 0:
            return total_rows - 1
        return total_rows

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

    def _parse_control_count(self, response: requests.Response) -> int:
        if self.settings.control_response_format == "text":
            try:
                return int(response.text.strip())
            except ValueError as exc:
                raise RuntimeError(
                    f"Control response was not an integer: {response.text[:200]}"
                ) from exc

        try:
            payload: Any = response.json()
        except json.JSONDecodeError as exc:
            raise RuntimeError("Control response was not valid JSON.") from exc

        if isinstance(payload, int):
            return payload
        if isinstance(payload, dict):
            count_value = payload.get(self.settings.control_count_field)
            if count_value is None:
                raise RuntimeError(
                    f"Control field '{self.settings.control_count_field}' not found in response."
                )
            try:
                return int(count_value)
            except (TypeError, ValueError) as exc:
                raise RuntimeError(
                    f"Control field '{self.settings.control_count_field}' was not numeric."
                ) from exc

        if isinstance(payload, list):
            if not payload:
                raise RuntimeError("Control response list was empty.")
            selected_record = self._select_control_record(payload)
            count_value = selected_record.get(self.settings.control_count_field)
            if count_value is None:
                raise RuntimeError(
                    f"Control field '{self.settings.control_count_field}' not found in selected control record."
                )
            try:
                return int(count_value)
            except (TypeError, ValueError) as exc:
                raise RuntimeError(
                    f"Control field '{self.settings.control_count_field}' was not numeric."
                ) from exc

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
                raw_date = record.get(self.settings.control_date_field)
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
