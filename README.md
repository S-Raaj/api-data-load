# API to HDFS Orchestration Framework

Simple Python framework for:

1. Fetching an access token from an auth API using `client_id` and `X-API-SECRET`
2. Downloading CSV data for a list of tables using the token and current business date (`yyyyMMdd`)
3. Moving downloaded files to HDFS
4. Recording logs, per-step metrics, and cleaning up local temporary files

## Project layout

- `src/orchestrator/config.py`: runtime configuration
- `src/orchestrator/logging_utils.py`: structured logging setup
- `src/orchestrator/metrics.py`: metrics collector
- `src/orchestrator/api_client.py`: auth and CSV download logic
- `src/orchestrator/hdfs.py`: HDFS upload helper
- `src/orchestrator/main.py`: orchestration entrypoint

## Setup

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Configuration

You can configure the orchestrator either with environment variables or with a YAML config file.

### YAML config

Example [config.yaml](/Users/raaj/Documents/New%20project/config.yaml):

```yaml
api:
  base_url: "https://internal.company.com"
  verify_ssl: false
  asofdate_format: "%d-%b-%Y"
  asofdate_uppercase: true
  auth:
    path: "/auth/token"
    method: "GET"
    client_id: "your-client-id"
    secret: "your-secret"
    client_id_param: "client_id"
    secret_header: "X-API-SECRET"
  data:
    method: "GET"
    path_template: "/data/table={table_name}/asofdate={asofdate}"
  control:
    enabled: false
    method: "GET"
    path_template: "/data/table={table_name}?asofdate={asofdate}"
    response_format: "json"
    count_field: "lastUploadCount"
    date_field: "lastUploadDate"
    selection_strategy: "latest"
    strict: true
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
  download_dir: "./downloads"
  log_dir: "./logs"
  metrics_dir: "./metrics"
  cleanup_local_files: true
  fail_fast: false
hdfs:
  enabled: true
  target_dir: "/data/raw/internal_api"
tables:
  - customers
  - orders
```

A test/demo version is also available at [config.mock.yaml](/Users/raaj/Documents/New%20project/config.mock.yaml).

Run with YAML:

```bash
PYTHONPATH=src python -m orchestrator.main --config config.yaml
```

For a sample dry setup, you can also start from:

```bash
PYTHONPATH=src python -m orchestrator.main --config config.mock.yaml --asofdate 20260301
```

The `data.path_template` can include `{table_name}`, `{business_date}`, and `{asofdate}` placeholders.
`api.asofdate_format: "%d-%b-%Y"` plus `api.asofdate_uppercase: true` produces values like `18-AUG-2025`.
Set `api.verify_ssl: false` to match `curl -k` for internal certificates.
If `api.control.enabled: true`, the orchestrator calls the optional control API with the same bearer token and validates the downloaded row count.
The `tables` list, local `orchestrator.download_dir`, and `hdfs.target_dir` can all be defined in the config file and used without CLI table input.
Set `hdfs.enabled: false` to skip HDFS upload entirely.

### Environment variables

Set these environment variables before running:

```bash
export API_AUTH_URL="https://internal.company.com/auth"
export API_DATA_URL="https://internal.company.com/export"
export API_BASE_URL="https://internal.company.com"
export API_VERIFY_SSL="false"
export API_ASOFDATE_FORMAT="%d-%b-%Y"
export API_ASOFDATE_UPPERCASE="true"
export API_AUTH_PATH="/auth/token"
export API_DATA_PATH_TEMPLATE="/data/table={table_name}/asofdate={asofdate}"
export API_CONTROL_ENABLED="false"
export API_CONTROL_PATH_TEMPLATE="/data/table={table_name}?asofdate={asofdate}"
export API_CONTROL_METHOD="GET"
export API_CONTROL_RESPONSE_FORMAT="json"
export API_CONTROL_COUNT_FIELD="lastUploadCount"
export API_CONTROL_DATE_FIELD="lastUploadDate"
export API_CONTROL_SELECTION_STRATEGY="latest"
export API_CONTROL_STRICT="true"
export API_CONTROL_EXCLUDE_HEADER="true"
export API_CLIENT_ID="your-client-id"
export API_SECRET="your-secret"
export HDFS_ENABLED="true"
export HDFS_TARGET_DIR="/data/raw/internal_api"
```

Optional environment variables:

```bash
export API_SECRET_HEADER="X-API-SECRET"
export API_CLIENT_ID_PARAM="client_id"
export API_BUSINESS_DATE_PARAM="business_date"
export API_TABLE_PARAM="table_name"
export API_TOKEN_FIELD="token"
export API_TOKEN_EXPIRY_FIELD="expiringAt"
export API_AUTH_METHOD="GET"
export API_DATA_METHOD="GET"
export API_TOKEN_HEADER="Authorization"
export API_TOKEN_PREFIX="Bearer"
export REQUEST_TIMEOUT_SECONDS="60"
export API_MAX_RETRIES="3"
export API_RETRY_BACKOFF_SECONDS="2.0"
export API_RETRYABLE_STATUS_CODES="429,500,502,503,504"
export API_TOKEN_VALIDITY_MINUTES="60"
export API_TOKEN_REFRESH_BUFFER_MINUTES="5"
export WORK_DIR="./downloads"
export LOG_DIR="./logs"
export METRICS_DIR="./metrics"
export CLEANUP_LOCAL_FILES="true"
export FAIL_FAST="false"
```

## Resilience behavior

- Transient HTTP failures and network errors are retried with exponential backoff plus jitter
- Default retryable status codes are `429, 500, 502, 503, 504`
- Tokens are cached and refreshed automatically before expiry using `expiringAt` from the auth response
- If a data request returns `401`, the client refreshes the token and retries once

Expected auth response shape:

```json
{
  "secret": "masked-or-ignored",
  "token": "abc123",
  "expiringAt": "2026-03-01T18:30:00Z"
}
```

If `expiringAt` is missing or cannot be parsed, the client falls back to the configured validity window. Example fallback config for a 60-minute token:

```bash
export API_TOKEN_VALIDITY_MINUTES="60"
export API_TOKEN_REFRESH_BUFFER_MINUTES="5"
```

That refreshes the token when less than 5 minutes remain.

## Running

Pass tables directly:

```bash
PYTHONPATH=src python -m orchestrator.main --tables customers orders invoices
```

Or use a file with one table name per line:

```bash
PYTHONPATH=src python -m orchestrator.main --table-file tables.txt
```

Or combine YAML config with CLI table overrides:

```bash
PYTHONPATH=src python -m orchestrator.main --config config.yaml --tables invoices payments
```

If you want the whole run driven by config only, this is enough:

```bash
PYTHONPATH=src python -m orchestrator.main --config config.yaml --asofdate 20260301
```

If you want to skip HDFS movement for a local test run:

```yaml
hdfs:
  enabled: false
```

With the sample config above, `--asofdate 20260301` is rendered to the API as `01-MAR-2026`.
You can also pass the API-formatted value directly, for example `--asofdate 18-AUG-2025`.

Optional control API example matching your curl:

```bash
curl -k 'https://<hostname>/data/table=<Table_Name>?asofdate=<Date>' \
  -H 'Authorization: Bearer <token>'
```

That maps to:

```yaml
api:
  verify_ssl: false
  control:
    enabled: true
    path_template: "/data/table={table_name}?asofdate={asofdate}"
```

Example control response supported now:

```json
[
  {"lastUploadCount": 1, "lastUploadDate": "2025-08-28T17:22:19.000+00.00"},
  {"lastUploadCount": 4562, "lastUploadDate": "2025-07-11T17:22:19.000+00.00"}
]
```

With `selection_strategy: "latest"`, the orchestrator picks the most recent `lastUploadDate` record and compares its `lastUploadCount` to the downloaded CSV row count.

To run for a specific business date instead of today:

```bash
PYTHONPATH=src python -m orchestrator.main --asofdate 20260228 --table-file tables.txt
```

## Outputs

- Logs: `logs/orchestrator.log`
- Metrics JSON: `metrics/run_<timestamp>.json`
- Downloaded CSVs: `downloads/<run_id>/` during processing
- Download location comes from `orchestrator.download_dir` in the YAML config
- HDFS destination comes from `hdfs.target_dir` in the YAML config

If `CLEANUP_LOCAL_FILES=true`, downloaded CSV files are removed after the HDFS upload step completes.

## HDFS dependency

This framework uses the local Hadoop CLI:

```bash
hdfs dfs -mkdir -p <target-dir>
hdfs dfs -put -f <local-file> <target-dir>
```

Make sure `hdfs` is installed and available on the runtime host.

## Tests

Mock-based unit tests are included under `tests/`. They do not call the real API or HDFS.

Run all tests:

```bash
PYTHONPATH=src python3 -m unittest discover -s tests -v
```

Covered examples:

- parsing auth response with `expiringAt: "2025-09-04T00:54:46.502Z[UTC]"`
- proactive token refresh before expiry
- retry on transient API failure like `503`
- refresh and retry after `401`
- HDFS CLI invocation checks with mocks
- end-to-end orchestration run with mocked API, HDFS, metrics, and log verification
