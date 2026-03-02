from __future__ import annotations

import sys
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

try:
    from requests import HTTPError
    from orchestrator.api_client import ApiClient, TokenState
    from orchestrator.config import Settings
    REQUESTS_AVAILABLE = True
except ModuleNotFoundError:
    HTTPError = Exception
    ApiClient = None
    TokenState = None
    Settings = None
    REQUESTS_AVAILABLE = False


class FakeResponse:
    def __init__(
        self,
        *,
        status_code: int = 200,
        json_data: dict | None = None,
        chunks: list[bytes] | None = None,
        text: str = "",
    ) -> None:
        self.status_code = status_code
        self._json_data = json_data or {}
        self._chunks = chunks or []
        self.text = text

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise HTTPError(response=self)

    def json(self) -> dict:
        return self._json_data

    def iter_content(self, chunk_size: int = 8192):
        del chunk_size
        for chunk in self._chunks:
            yield chunk


def build_settings() -> Settings:
    if Settings is None:
        raise RuntimeError("requests dependency is not installed")
    return Settings(
        auth_url="https://example.internal/auth",
        data_url="https://example.internal/export",
        client_id="client-123",
        api_secret="secret-xyz",
        hdfs_target_dir="/tmp/hdfs-target",
        request_timeout_seconds=5,
        max_retries=2,
        retry_backoff_seconds=0.01,
        token_refresh_buffer_minutes=5,
        token_validity_minutes=60,
    )


@unittest.skipUnless(REQUESTS_AVAILABLE, "requests is not installed")
class ApiClientTests(unittest.TestCase):
    def test_fetch_token_parses_java_style_utc_expiration(self) -> None:
        client = ApiClient(build_settings())
        client.session.request = MagicMock(
            return_value=FakeResponse(
                json_data={
                    "token": "token-1",
                    "expiringAt": "2025-09-04T00:54:46.502Z[UTC]",
                }
            )
        )

        token, expires_at = client.fetch_token()

        self.assertEqual(token, "token-1")
        self.assertEqual(
            expires_at,
            datetime(2025, 9, 4, 0, 54, 46, 502000, tzinfo=timezone.utc),
        )

    def test_get_valid_token_refreshes_when_within_buffer(self) -> None:
        client = ApiClient(build_settings())
        near_expiry = datetime.now(timezone.utc) + timedelta(minutes=4)
        client._token_state = TokenState(value="old-token", expires_at=near_expiry)
        client.fetch_token = MagicMock(
            return_value=("new-token", datetime.now(timezone.utc) + timedelta(minutes=60))
        )

        token = client.get_valid_token()

        self.assertEqual(token, "new-token")
        self.assertEqual(client.fetch_token.call_count, 1)

    @patch("orchestrator.api_client.time.sleep", return_value=None)
    def test_download_csv_retries_transient_failure_then_succeeds(self, _sleep: MagicMock) -> None:
        client = ApiClient(build_settings())
        client._token_state = TokenState(
            value="cached-token",
            expires_at=datetime.now(timezone.utc) + timedelta(minutes=30),
        )
        client.session.request = MagicMock(
            side_effect=[
                FakeResponse(status_code=503, text="temporary outage"),
                FakeResponse(chunks=[b"a,b\n", b"1,2\n"]),
            ]
        )

        with tempfile.TemporaryDirectory() as tmp_dir:
            output_file = Path(tmp_dir) / "orders_20260301.csv"
            client.download_csv("orders", "20260301", output_file)

            self.assertEqual(output_file.read_text(encoding="utf-8"), "a,b\n1,2\n")
            self.assertEqual(client.get_stats()["api_retries"], 1)
            self.assertEqual(client.session.request.call_count, 2)

    def test_download_csv_refreshes_token_after_401(self) -> None:
        client = ApiClient(build_settings())
        current_expiry = datetime.now(timezone.utc) + timedelta(minutes=30)
        refreshed_expiry = datetime.now(timezone.utc) + timedelta(minutes=60)
        client.fetch_token = MagicMock(return_value=("refreshed-token", refreshed_expiry))
        client._token_state = TokenState(value="expired-token", expires_at=current_expiry)
        client.session.request = MagicMock(
            side_effect=[
                FakeResponse(status_code=401, text="unauthorized"),
                FakeResponse(chunks=[b"col\n", b"value\n"]),
            ]
        )

        with tempfile.TemporaryDirectory() as tmp_dir:
            output_file = Path(tmp_dir) / "customers_20260301.csv"
            client.download_csv("customers", "20260301", output_file)

            self.assertEqual(output_file.read_text(encoding="utf-8"), "col\nvalue\n")
            self.assertEqual(client.fetch_token.call_count, 1)
            self.assertEqual(client.get_stats()["api_401_refresh_retries"], 1)


if __name__ == "__main__":
    unittest.main()
