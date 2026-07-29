#!/usr/bin/env python3
"""Smoke tests for FXOpen/TickTrader REST and FIX APIs."""

from __future__ import annotations

import argparse
import base64
import contextlib
import csv
import datetime as dt
import gzip
import hashlib
import hmac
import io
import json
import os
import socket
import ssl
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
import zipfile
import zlib
from dataclasses import dataclass
from typing import Any


SOH = "\x01"
DEFAULT_REST_BASE_URL = "https://marginalttdemowebapi.fxopen.net"
DEFAULT_WS_BASE_URL = "wss://marginalttdemowebapi.fxopen.net"


class SmokeError(RuntimeError):
    pass


@dataclass
class CheckResult:
    name: str
    ok: bool
    detail: str
    fileUrl: str | None = None
    fileName: str | None = None


def load_env(path: str) -> None:
    if not os.path.exists(path):
        return
    with open(path, "r", encoding="utf-8") as env_file:
        for raw_line in env_file:
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip().strip('"').strip("'")
            if key and key not in os.environ:
                os.environ[key] = value


def env_bool(name: str, default: bool = False) -> bool:
    value = os.environ.get(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def env_required(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise SmokeError(f"Missing required env var: {name}")
    return value


def utc_timestamp_ms(value: str | None = None) -> int:
    if not value:
        now = dt.datetime.now(dt.timezone.utc)
        yesterday = now - dt.timedelta(days=1)
        midnight = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)
        return int(midnight.timestamp() * 1000)

    normalized = value.strip()
    if normalized.endswith("Z"):
        normalized = normalized[:-1] + "+00:00"
    parsed = dt.datetime.fromisoformat(normalized)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=dt.timezone.utc)
    return int(parsed.astimezone(dt.timezone.utc).timestamp() * 1000)


def parse_timestamp_ms(value: str) -> int:
    normalized = value.strip()
    if not normalized:
        raise SmokeError("timestamp value is empty")
    if normalized.isdigit():
        return int(normalized)
    if normalized.endswith("Z"):
        normalized = normalized[:-1] + "+00:00"
    parsed = dt.datetime.fromisoformat(normalized)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=dt.timezone.utc)
    return int(parsed.astimezone(dt.timezone.utc).timestamp() * 1000)


def normalize_base_url(base_url: str, port: str | None = None) -> str:
    value = (base_url or DEFAULT_REST_BASE_URL).strip().rstrip("/")
    if "://" not in value:
        value = "https://" + value
    parsed = urllib.parse.urlparse(value)
    selected_port = (port or "").strip()
    if not selected_port:
        return value
    if parsed.port:
        netloc = parsed.hostname or parsed.netloc
    else:
        netloc = parsed.netloc
    if selected_port != "443":
        netloc = f"{netloc}:{selected_port}"
    return urllib.parse.urlunparse((parsed.scheme, netloc, parsed.path, "", "", ""))


def periodicity_to_ms(periodicity: str) -> int:
    mapping = {
        "S1": 1_000,
        "S10": 10_000,
        "M1": 60_000,
        "M5": 5 * 60_000,
        "M15": 15 * 60_000,
        "M30": 30 * 60_000,
        "H1": 60 * 60_000,
        "H4": 4 * 60 * 60_000,
        "D1": 24 * 60 * 60_000,
        "W1": 7 * 24 * 60 * 60_000,
        "MN1": 30 * 24 * 60 * 60_000,
    }
    return mapping.get(periodicity, 60_000)


class TickTraderRestClient:
    def __init__(
        self,
        base_url: str,
        web_api_id: str | None,
        web_api_key: str | None,
        web_api_secret: str | None,
        timeout: float,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.web_api_id = web_api_id or ""
        self.web_api_key = web_api_key or ""
        self.web_api_secret = web_api_secret or ""
        self.timeout = timeout
        self.sent_commands: list[str] = []

    def public_get(self, path: str) -> tuple[int, dict[str, str], bytes]:
        return self._request("GET", path, auth=False)

    def auth_get(self, path: str) -> tuple[int, dict[str, str], bytes]:
        return self._request("GET", path, auth=True)

    def auth_post(self, path: str, body: dict[str, Any]) -> tuple[int, dict[str, str], bytes]:
        content = json.dumps(body, separators=(",", ":"))
        return self._request("POST", path, data=content.encode("utf-8"), content=content, auth=True)

    def auth_put(self, path: str, body: dict[str, Any]) -> tuple[int, dict[str, str], bytes]:
        content = json.dumps(body, separators=(",", ":"))
        return self._request("PUT", path, data=content.encode("utf-8"), content=content, auth=True)

    def auth_delete(self, path: str) -> tuple[int, dict[str, str], bytes]:
        return self._request("DELETE", path, auth=True)

    def _request(
        self,
        method: str,
        path: str,
        data: bytes | None = None,
        content: str = "",
        auth: bool = False,
    ) -> tuple[int, dict[str, str], bytes]:
        if not path.startswith("/"):
            raise ValueError(f"path must start with '/': {path}")

        url = self.base_url + path
        headers = {
            "Content-type": "application/json",
            "Accept": "application/json",
            "Accept-Encoding": "gzip, deflate",
        }
        if auth:
            self._ensure_hmac_credentials()
            headers["Authorization"] = self._hmac_authorization(method, url, content)

        self.sent_commands.append(format_rest_command(method, url, auth, content))
        request = urllib.request.Request(url, data=data, headers=headers, method=method)
        try:
            with urllib.request.urlopen(request, timeout=self.timeout) as response:
                raw = response.read()
                decoded = decode_response_body(raw, response.headers.get("Content-Encoding"))
                return response.status, dict(response.headers.items()), decoded
        except urllib.error.HTTPError as exc:
            raw = exc.read()
            decoded = decode_response_body(raw, exc.headers.get("Content-Encoding"))
            return exc.code, dict(exc.headers.items()), decoded
        except urllib.error.URLError as exc:
            raise SmokeError(f"{method} {url} failed: {exc.reason}") from exc

    def _ensure_hmac_credentials(self) -> None:
        missing = [
            name
            for name, value in {
                "TT_WEB_API_ID": self.web_api_id,
                "TT_WEB_API_KEY": self.web_api_key,
                "TT_WEB_API_SECRET": self.web_api_secret,
            }.items()
            if not value
        ]
        if missing:
            raise SmokeError("Missing REST HMAC credentials: " + ", ".join(missing))

    def _hmac_authorization(self, method: str, url: str, content: str) -> str:
        timestamp = str(int(time.time() * 1000))
        signature = timestamp + self.web_api_id + self.web_api_key + method + url + content
        digest = hmac.new(
            self.web_api_secret.encode("ascii"),
            signature.encode("ascii"),
            hashlib.sha256,
        ).digest()
        encoded = base64.b64encode(digest).decode("ascii")
        return f"HMAC {self.web_api_id}:{self.web_api_key}:{timestamp}:{encoded}"


def decode_response_body(raw: bytes, encoding: str | None) -> bytes:
    if not raw:
        return b""
    normalized = (encoding or "").lower()
    if normalized in {"gzip", "x-gzip"}:
        return gzip.decompress(raw)
    if normalized == "deflate":
        try:
            return zlib.decompress(raw)
        except zlib.error:
            return zlib.decompress(raw, -zlib.MAX_WBITS)
    return raw


def parse_json(body: bytes, name: str) -> Any:
    try:
        return json.loads(body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        preview = body[:200].decode("utf-8", errors="replace")
        raise SmokeError(f"{name} returned non-JSON body: {preview}") from exc


def assert_status(status: int, name: str, expected: set[int] | None = None) -> None:
    expected_statuses = expected or set(range(200, 300))
    if status not in expected_statuses:
        raise SmokeError(f"{name} returned HTTP {status}")


def result(name: str, callback: Any) -> CheckResult:
    try:
        detail = callback()
        return CheckResult(name, True, str(detail))
    except Exception as exc:
        return CheckResult(name, False, str(exc))


def print_results(results: list[CheckResult]) -> int:
    failed = 0
    for check in results:
        status = "PASS" if check.ok else "FAIL"
        print(f"[{status}] {check.name}: {check.detail}")
        if not check.ok:
            failed += 1
    return 1 if failed else 0


def make_rest_client(args: argparse.Namespace) -> TickTraderRestClient:
    return TickTraderRestClient(
        base_url=normalize_base_url(
            os.environ.get("TT_REST_BASE_URL", DEFAULT_REST_BASE_URL),
            os.environ.get("TT_REST_PORT"),
        ),
        web_api_id=os.environ.get("TT_WEB_API_ID"),
        web_api_key=os.environ.get("TT_WEB_API_KEY"),
        web_api_secret=os.environ.get("TT_WEB_API_SECRET"),
        timeout=args.timeout,
    )


def format_rest_command(method: str, url: str, auth: bool, content: str) -> str:
    auth_label = "HMAC masked" if auth else "public"
    body = f" body={content}" if content else ""
    return f"REST SEND {method} {url} auth={auth_label}{body}"


def rest_public_checks(client: TickTraderRestClient, symbol: str) -> list[CheckResult]:
    encoded_symbol = quote_path_component(symbol)

    def trade_session() -> str:
        status, _, body = client.public_get("/api/v1/public/tradesession")
        assert_status(status, "public tradesession")
        payload = parse_json(body, "public tradesession")
        return f"{payload.get('PlatformName', 'TickTrader')} session={payload.get('SessionStatus', 'unknown')}"

    def symbols() -> str:
        status, _, body = client.public_get("/api/v1/public/symbol")
        assert_status(status, "public symbols")
        payload = parse_json(body, "public symbols")
        if not isinstance(payload, list):
            raise SmokeError("public symbols response is not a list")
        sample = payload[0].get("Symbol", "?") if payload else "none"
        return f"{len(payload)} symbols, sample={sample}"

    def symbol_info() -> str:
        status, _, body = client.public_get(f"/api/v1/public/symbol/{encoded_symbol}")
        assert_status(status, "public symbol")
        payload = parse_json(body, "public symbol")
        if isinstance(payload, list):
            if not payload:
                raise SmokeError(f"symbol not found: {symbol}")
            payload = payload[0]
        return f"{payload.get('Symbol', symbol)} precision={payload.get('Precision', 'unknown')}"

    def tick() -> str:
        status, _, body = client.public_get(f"/api/v1/public/tick/{encoded_symbol}")
        assert_status(status, "public tick")
        payload = parse_json(body, "public tick")
        if isinstance(payload, list):
            if not payload:
                raise SmokeError(f"tick not found: {symbol}")
            payload = payload[0]
        bid = payload.get("BestBid", {}).get("Price")
        ask = payload.get("BestAsk", {}).get("Price")
        return f"{symbol} bid={bid} ask={ask}"

    return [
        result("REST public tradesession", trade_session),
        result("REST public symbols", symbols),
        result(f"REST public symbol {symbol}", symbol_info),
        result(f"REST public tick {symbol}", tick),
    ]


def rest_auth_checks(client: TickTraderRestClient, symbol: str) -> list[CheckResult]:
    encoded_symbol = quote_path_component(symbol)

    def account() -> str:
        status, _, body = client.auth_get("/api/v1/account")
        assert_status(status, "account")
        payload = parse_json(body, "account")
        return f"id={payload.get('Id', '?')} type={payload.get('AccountingType', '?')} group={payload.get('Group', '?')}"

    def trade_session() -> str:
        status, _, body = client.auth_get("/api/v1/tradesession")
        assert_status(status, "trade session")
        payload = parse_json(body, "trade session")
        return f"session={payload.get('SessionStatus', 'unknown')}"

    def private_symbol() -> str:
        status, _, body = client.auth_get(f"/api/v1/symbol/{encoded_symbol}")
        assert_status(status, "private symbol")
        payload = parse_json(body, "private symbol")
        if isinstance(payload, list):
            if not payload:
                raise SmokeError(f"symbol not found: {symbol}")
            payload = payload[0]
        return f"{payload.get('Symbol', symbol)} precision={payload.get('Precision', 'unknown')}"

    def private_tick() -> str:
        status, _, body = client.auth_get(f"/api/v1/tick/{encoded_symbol}")
        assert_status(status, "private tick")
        payload = parse_json(body, "private tick")
        if isinstance(payload, list):
            if not payload:
                raise SmokeError(f"tick not found: {symbol}")
            payload = payload[0]
        bid = payload.get("BestBid", {}).get("Price")
        ask = payload.get("BestAsk", {}).get("Price")
        return f"{symbol} bid={bid} ask={ask}"

    def trade_history() -> str:
        request_body = {
            "TimestampTo": int(time.time() * 1000),
            "RequestDirection": "Backward",
            "RequestPageSize": 10,
        }
        status, _, body = client.auth_post("/api/v1/tradehistory", request_body)
        assert_status(status, "trade history")
        payload = parse_json(body, "trade history")
        records = payload.get("Records", [])
        return f"{len(records)} records, last={payload.get('IsLastReport', '?')}"

    return [
        result("REST account", account),
        result("REST trade session", trade_session),
        result(f"REST private symbol {symbol}", private_symbol),
        result(f"REST private tick {symbol}", private_tick),
        result("REST trade history", trade_history),
    ]


def rest_quote_checks(client: TickTraderRestClient, symbol: str) -> list[CheckResult]:
    encoded_symbol = quote_path_component(symbol)

    def private_symbol() -> str:
        status, _, body = client.auth_get(f"/api/v1/symbol/{encoded_symbol}")
        assert_status(status, "private symbol")
        payload = parse_json(body, "private symbol")
        if isinstance(payload, list):
            if not payload:
                raise SmokeError(f"symbol not found: {symbol}")
            payload = payload[0]
        return f"{payload.get('Symbol', symbol)} precision={payload.get('Precision', 'unknown')}"

    def private_tick() -> str:
        status, _, body = client.auth_get(f"/api/v1/tick/{encoded_symbol}")
        assert_status(status, "private tick")
        payload = parse_json(body, "private tick")
        if isinstance(payload, list):
            if not payload:
                raise SmokeError(f"tick not found: {symbol}")
            payload = payload[0]
        bid = payload.get("BestBid", {}).get("Price")
        ask = payload.get("BestAsk", {}).get("Price")
        return f"{symbol} bid={bid} ask={ask}"

    return [
        result(f"REST private symbol {symbol}", private_symbol),
        result(f"REST private tick {symbol}", private_tick),
    ]


def rest_trade_checks(client: TickTraderRestClient) -> list[CheckResult]:
    def account() -> str:
        status, _, body = client.auth_get("/api/v1/account")
        assert_status(status, "account")
        payload = parse_json(body, "account")
        return f"id={payload.get('Id', '?')} type={payload.get('AccountingType', '?')} group={payload.get('Group', '?')}"

    def trade_session() -> str:
        status, _, body = client.auth_get("/api/v1/tradesession")
        assert_status(status, "trade session")
        payload = parse_json(body, "trade session")
        return f"session={payload.get('SessionStatus', 'unknown')}"

    def trade_history() -> str:
        request_body = {
            "TimestampTo": int(time.time() * 1000),
            "RequestDirection": "Backward",
            "RequestPageSize": 10,
        }
        status, _, body = client.auth_post("/api/v1/tradehistory", request_body)
        assert_status(status, "trade history")
        payload = parse_json(body, "trade history")
        records = payload.get("Records", [])
        return f"{len(records)} records, last={payload.get('IsLastReport', '?')}"

    return [
        result("REST account", account),
        result("REST trade session", trade_session),
        result("REST trade history", trade_history),
    ]


def bars_checks(
    client: TickTraderRestClient,
    symbol: str,
    periodicity: str,
    price_type: str,
    timestamp_ms: int | None,
    download_dir: str | None,
    timestamp_to_ms: int | None = None,
    count: int | None = None,
) -> list[CheckResult]:
    encoded_symbol = quote_path_component(symbol)
    checks: list[CheckResult] = []

    if timestamp_ms is not None:
        def bars_json() -> str:
            selected_count = count or calculate_bar_count(timestamp_ms, timestamp_to_ms, periodicity)
            path = (
                f"/api/v2/quotehistory/{encoded_symbol}/{periodicity}/bars/{price_type}"
                f"?timestamp={timestamp_ms}&count={selected_count}"
            )
            if client.web_api_id and client.web_api_key and client.web_api_secret:
                status, _, body = client.auth_get(path)
            else:
                status, _, body = client.public_get("/api/v2/public" + path[len("/api/v2"):])
            assert_status(status, "bars")
            payload = parse_json(body, "bars")
            bars = payload.get("Bars", []) if isinstance(payload, dict) else []
            if not isinstance(bars, list):
                raise SmokeError("bars response has no Bars list")
            first = bars[0].get("Timestamp") if bars else "none"
            last = bars[-1].get("Timestamp") if bars else "none"
            return f"{len(bars)} bars, count={selected_count}, first={first}, last={last}"

        def bars_download_file() -> str:
            path = f"/api/v2/quotehistory/download/bars/{encoded_symbol}/{periodicity}/{price_type}/{timestamp_ms}"
            status, _, body = client.auth_get(path)
            assert_status(status, "bars")
            return summarize_zip_or_binary(body, "bars", download_dir)

        checks.append(result(f"Bars range {symbol} {periodicity} {price_type}", bars_json))
        if download_dir:
            checks.append(result(f"Bars download {symbol} {periodicity} {price_type}", bars_download_file))
    else:
        checks.append(result(f"Bars range {symbol} {periodicity} {price_type}", lambda: "start time is not set"))

    return checks


def quote_history_download_checks(
    client: TickTraderRestClient,
    symbol: str,
    history_kind: str,
    periodicity: str,
    price_type: str,
    timestamp_ms: int,
    timestamp_to_ms: int | None,
    download_dir: str,
    url_prefix: str,
) -> list[CheckResult]:
    def download() -> CheckResult:
        kind = "Bars" if history_kind == "Bars" else "Ticks"
        rows = collect_quote_history(client, symbol, kind, periodicity, price_type, timestamp_ms, timestamp_to_ms)
        os.makedirs(download_dir, exist_ok=True)
        safe_symbol = "".join(ch for ch in symbol if ch.isalnum() or ch in {"_", "-"}).strip() or "symbol"
        stamp = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%d-%H%M%S")
        file_name = f"{safe_symbol}-{kind.lower()}-{stamp}.csv"
        file_path = os.path.join(download_dir, file_name)
        write_quote_csv(file_path, rows, kind)
        first = rows[0].get("Timestamp") if rows else "none"
        last = rows[-1].get("Timestamp") if rows else "none"
        detail = f"{kind.lower()} downloaded: {len(rows)} rows, first={first}, last={last}"
        return CheckResult(
            f"REST quote history {kind.lower()} {symbol}",
            True,
            detail,
            f"{url_prefix.rstrip('/')}/{urllib.parse.quote(file_name)}",
            file_name,
        )

    try:
        return [download()]
    except Exception as exc:
        return [CheckResult(f"REST quote history {history_kind.lower()} {symbol}", False, str(exc))]


def collect_quote_history(
    client: TickTraderRestClient,
    symbol: str,
    kind: str,
    periodicity: str,
    price_type: str,
    timestamp_ms: int,
    timestamp_to_ms: int | None,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    seen_timestamps: set[Any] = set()
    cursor = timestamp_ms
    max_pages = int(os.environ.get("TT_HISTORY_MAX_PAGES", "100") or 100)
    for _ in range(max_pages):
        page = fetch_quote_history_page(client, symbol, kind, periodicity, price_type, cursor, 1000)
        if not page:
            break
        added = 0
        last_timestamp = cursor
        for row in page:
            row_timestamp = row.get("Timestamp")
            if row_timestamp is None:
                continue
            last_timestamp = int(row_timestamp)
            if timestamp_to_ms is not None and last_timestamp > timestamp_to_ms:
                return rows
            if row_timestamp in seen_timestamps:
                continue
            seen_timestamps.add(row_timestamp)
            rows.append(row)
            added += 1
        if timestamp_to_ms is None or added == 0 or last_timestamp <= cursor:
            break
        cursor = last_timestamp
    return rows


def fetch_quote_history_page(
    client: TickTraderRestClient,
    symbol: str,
    kind: str,
    periodicity: str,
    price_type: str,
    timestamp_ms: int,
    count: int,
) -> list[dict[str, Any]]:
    encoded_symbol = quote_path_component(symbol)
    if kind == "Bars":
        path = (
            f"/api/v2/quotehistory/{encoded_symbol}/{periodicity}/bars/{price_type}"
            f"?timestamp={timestamp_ms}&count={count}"
        )
        result_key = "Bars"
        name = "quote history bars"
    else:
        path = f"/api/v2/quotehistory/{encoded_symbol}/ticks?timestamp={timestamp_ms}&count={count}"
        result_key = "Ticks"
        name = "quote history ticks"
    if client.web_api_id and client.web_api_key and client.web_api_secret:
        status, _, body = client.auth_get(path)
    else:
        status, _, body = client.public_get("/api/v2/public" + path[len("/api/v2"):])
    assert_status(status, name)
    payload = parse_json(body, name)
    rows = payload.get(result_key, []) if isinstance(payload, dict) else []
    if not isinstance(rows, list):
        raise SmokeError(f"{name} response has no {result_key} list")
    return rows


def write_quote_csv(path: str, rows: list[dict[str, Any]], kind: str) -> None:
    fieldnames = (
        ["Timestamp", "Open", "High", "Low", "Close", "Volume"]
        if kind == "Bars"
        else ["Timestamp", "BidPrice", "BidVolume", "BidType", "AskPrice", "AskVolume", "AskType"]
    )
    with open(path, "w", encoding="utf-8", newline="") as output:
        writer = csv.DictWriter(output, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(flatten_quote_row(row, kind))


def flatten_quote_row(row: dict[str, Any], kind: str) -> dict[str, Any]:
    if kind == "Bars":
        return {key: row.get(key, "") for key in ["Timestamp", "Open", "High", "Low", "Close", "Volume"]}
    bid = row.get("BestBid", {}) or {}
    ask = row.get("BestAsk", {}) or {}
    return {
        "Timestamp": row.get("Timestamp", ""),
        "BidPrice": bid.get("Price", ""),
        "BidVolume": bid.get("Volume", ""),
        "BidType": bid.get("Type", ""),
        "AskPrice": ask.get("Price", ""),
        "AskVolume": ask.get("Volume", ""),
        "AskType": ask.get("Type", ""),
    }


def calculate_bar_count(timestamp_from_ms: int, timestamp_to_ms: int | None, periodicity: str) -> int:
    if timestamp_to_ms is None or timestamp_to_ms <= timestamp_from_ms:
        return 100
    count = max(1, int((timestamp_to_ms - timestamp_from_ms) / periodicity_to_ms(periodicity)) + 1)
    return min(count, 1000)


def summarize_zip_or_binary(body: bytes, prefix: str, download_dir: str | None) -> str:
    if not body:
        raise SmokeError("empty response body")

    saved = ""
    if download_dir:
        os.makedirs(download_dir, exist_ok=True)
        filename = os.path.join(download_dir, f"{prefix}-{int(time.time())}.zip")
        with open(filename, "wb") as output:
            output.write(body)
        saved = f", saved={filename}"

    if zipfile.is_zipfile(io.BytesIO(body)):
        with zipfile.ZipFile(io.BytesIO(body)) as archive:
            names = archive.namelist()
        return f"zip {len(body)} bytes, files={names[:5]}{saved}"
    return f"binary {len(body)} bytes, starts={body[:8]!r}{saved}"


def order_lifecycle_check(client: TickTraderRestClient, enable_trading_flag: bool) -> list[CheckResult]:
    def lifecycle() -> str:
        if not enable_trading_flag:
            raise SmokeError("trading disabled; enable order trading before running this check")

        symbol = env_required("TT_ORDER_SYMBOL")
        side = os.environ.get("TT_ORDER_SIDE", "Buy").strip() or "Buy"
        order_type = os.environ.get("TT_ORDER_TYPE", "Limit").strip() or "Limit"
        amount = parse_number_env("TT_ORDER_AMOUNT")
        comment_id = f"smoke-{uuid.uuid4()}"

        if side not in {"Buy", "Sell"}:
            raise SmokeError("TT_ORDER_SIDE must be Buy or Sell")
        if order_type not in {"Market", "Limit"}:
            raise SmokeError("TT_ORDER_TYPE must be Market or Limit")

        create_payload = {
            "Type": order_type,
            "Side": side,
            "Symbol": symbol,
            "Amount": amount,
            "Comment": f"{comment_id} create",
        }

        if order_type == "Limit":
            create_payload["Price"] = parse_number_env("TT_ORDER_LIMIT_PRICE")

        status, _, body = client.auth_post("/api/v1/trade", create_payload)
        assert_status(status, f"create {order_type.lower()} order")
        created = parse_json(body, f"create {order_type.lower()} order")
        trade_id = created.get("Id")
        if trade_id is None:
            raise SmokeError(f"create response has no Id: {created}")

        if order_type == "Market":
            return f"market order sent trade id={trade_id}"

        modify_payload = {"Id": trade_id, "Comment": f"{comment_id} modify"}
        status, _, body = client.auth_put("/api/v1/trade", modify_payload)
        assert_status(status, "modify limit order")
        _ = parse_json(body, "modify limit order")

        status, _, _ = client.auth_delete(f"/api/v1/trade?type=Cancel&id={trade_id}")
        assert_status(status, "cancel limit order", expected={200, 202, 204})
        return f"created, modified, canceled trade id={trade_id}"

    return [result("REST order lifecycle", lifecycle)]


def parse_number_env(name: str) -> int | float:
    value = env_required(name)
    try:
        as_float = float(value)
    except ValueError as exc:
        raise SmokeError(f"{name} must be numeric") from exc
    if as_float.is_integer():
        return int(as_float)
    return as_float


def quote_path_component(value: str) -> str:
    return urllib.parse.quote(urllib.parse.quote(value, safe=""), safe="")


def fix_logon_check(args: argparse.Namespace) -> list[CheckResult]:
    def logon() -> str:
        config = FixConfig.from_env(args.channel, args.timeout)
        client = RawFixClient(config)
        client.connect()
        try:
            logon_response = client.logon()
            msg_type = logon_response.get("35", "?")
            if msg_type != "A":
                text = logon_response.get("58", "")
                raise SmokeError(f"expected Logon Ack 35=A, got 35={msg_type} {text}")
            heartbeat_detail = ""
            if args.test_request:
                heartbeat = client.test_request()
                heartbeat_detail = f", heartbeat 35={heartbeat.get('35', '?')}"
            logout_response = client.logout()
            return (
                f"logon accepted by {config.host}:{config.port}, logout 35={logout_response.get('35', '?')}"
                f"{heartbeat_detail}, connection closed\nSENT:\n{client.command_log()}"
            )
        except Exception as exc:
            raise SmokeError(f"{exc}\nSENT:\n{client.command_log()}") from exc
        finally:
            client.close()

    return [result(f"FIX {args.channel} logon", logon)]


def websocket_feed_check(symbol: str, periodicity: str, price_type: str, timestamp_ms: int | None, count: int, timeout: float) -> list[CheckResult]:
    def feed() -> str:
        client = TickTraderWebSocketClient.from_env("feed", timeout)
        try:
            login = client.login()
            symbols = client.request("Symbols", {"Symbol": symbol})
            client.request("FeedSubscribe", {"Subscribe": [{"Symbol": symbol, "BookDepth": 1}]})
            ticks = client.read_feed_ticks(symbol, max_ticks=3, timeout_seconds=min(timeout, 6))
            bars_detail = ""
            if timestamp_ms is not None:
                bars = client.request(
                    "QuoteHistoryBars",
                    {
                        "Symbol": symbol,
                        "Periodicity": periodicity,
                        "PriceType": price_type.lower(),
                        "Timestamp": timestamp_ms,
                        "Count": count,
                    },
                )
                bars_count = len(bars.get("Result", {}).get("Bars", []))
                bars_detail = f", bars={bars_count}"
            symbol_count = len(symbols.get("Result", {}).get("Symbols", []))
            ticks_detail = "; ".join(ticks) if ticks else "no live ticks before timeout"
            return (
                f"login={login.get('Result', {}).get('Info', 'ok')}, symbols={symbol_count}, feed subscribed"
                f"{bars_detail}, ticks=[{ticks_detail}], connection closed\nSENT:\n{client.command_log()}"
            )
        except Exception as exc:
            raise SmokeError(f"{exc}\nSENT:\n{client.command_log()}") from exc
        finally:
            client.close()

    return [result("WebSocket feed", feed)]


def websocket_trade_check(timeout: float) -> list[CheckResult]:
    def trade() -> str:
        client = TickTraderWebSocketClient.from_env("trade", timeout)
        try:
            login = client.login()
            session = client.request("TradeSessionInfo")
            account = client.request("Account")
            platform = session.get("Result", {}).get("PlatformName", "?")
            account_id = account.get("Result", {}).get("Id", "?")
            return (
                f"login={login.get('Result', {}).get('Info', 'ok')}, account={account_id}, platform={platform}, "
                f"connection closed\nSENT:\n{client.command_log()}"
            )
        except Exception as exc:
            raise SmokeError(f"{exc}\nSENT:\n{client.command_log()}") from exc
        finally:
            client.close()

    return [result("WebSocket trade", trade)]


def websocket_order_check(enable_trading_flag: bool, timeout: float) -> list[CheckResult]:
    def order() -> str:
        if not enable_trading_flag:
            raise SmokeError("trading disabled; enable order trading before running this check")
        client = TickTraderWebSocketClient.from_env("trade", timeout)
        try:
            client.login()
            create_params = order_payload_from_env(comment_prefix="websocket")
            created = client.request("TradeCreate", create_params)
            trade = created.get("Result", {}).get("Trade", {})
            trade_id = trade.get("Id")
            if not trade_id:
                raise SmokeError(f"TradeCreate response has no Trade.Id: {created}")
            if create_params["Type"] == "Limit":
                deleted = client.request("TradeDelete", {"Type": "Cancel", "Id": trade_id})
                event = deleted.get("Response", "TradeDelete")
                return f"limit trade id={trade_id}, canceled via {event}, connection closed\nSENT:\n{client.command_log()}"
            return f"market trade sent id={trade_id}, connection closed\nSENT:\n{client.command_log()}"
        except Exception as exc:
            raise SmokeError(f"{exc}\nSENT:\n{client.command_log()}") from exc
        finally:
            client.close()

    return [result("WebSocket order", order)]


def fix_order_check(enable_trading_flag: bool, timeout: float) -> list[CheckResult]:
    def order() -> str:
        if not enable_trading_flag:
            raise SmokeError("trading disabled; enable order trading before running this check")
        config = FixConfig.from_env("trade", timeout)
        client = RawFixClient(config)
        client.connect()
        try:
            logon_response = client.logon()
            if logon_response.get("35") != "A":
                raise SmokeError(f"FIX logon rejected: 35={logon_response.get('35')} {logon_response.get('58', '')}")
            cl_ord_id = client.new_order_single_from_env()
            report = client.read_until({"8", "3", "j"}, timeout)
            if is_fix_reject(report):
                raise SmokeError(f"FIX order rejected: {fix_reject_detail(report)}\nSENT:\n{client.command_log()}")
            detail = f"35=8 execType={report.get('150', '?')} ordStatus={report.get('39', '?')} clOrdId={cl_ord_id}"
            if os.environ.get("TT_ORDER_TYPE", "Limit").strip() == "Limit":
                client.cancel_order(cl_ord_id)
                cancel_report = client.read_until({"8", "3", "j"}, timeout)
                detail += f", cancel 35={cancel_report.get('35', '?')} status={cancel_report.get('39', '?')}"
            client.logout()
            return detail + f", connection closed\nSENT:\n{client.command_log()}"
        except Exception as exc:
            if "SENT:" in str(exc):
                raise
            raise SmokeError(f"{exc}\nSENT:\n{client.command_log()}") from exc
        finally:
            client.close()

    return [result("FIX order", order)]


def is_fix_reject(message: dict[str, str]) -> bool:
    return message.get("35") in {"3", "j"} or message.get("150") == "8" or message.get("39") == "8"


def fix_reject_detail(message: dict[str, str]) -> str:
    parts = []
    if message.get("58"):
        parts.append(message["58"])
    for tag, label in (("371", "RefTagID"), ("372", "RefMsgType"), ("373", "SessionRejectReason"), ("103", "OrdRejReason"), ("39", "OrdStatus"), ("150", "ExecType")):
        if message.get(tag):
            parts.append(f"{label}={message[tag]}")
    return ", ".join(parts) or str(message)


def order_payload_from_env(comment_prefix: str) -> dict[str, Any]:
    symbol = env_required("TT_ORDER_SYMBOL")
    side = os.environ.get("TT_ORDER_SIDE", "Buy").strip() or "Buy"
    order_type = os.environ.get("TT_ORDER_TYPE", "Limit").strip() or "Limit"
    amount = parse_number_env("TT_ORDER_AMOUNT")
    if side not in {"Buy", "Sell"}:
        raise SmokeError("TT_ORDER_SIDE must be Buy or Sell")
    if order_type not in {"Market", "Limit"}:
        raise SmokeError("TT_ORDER_TYPE must be Market or Limit")
    payload: dict[str, Any] = {
        "Type": order_type,
        "Side": side,
        "Symbol": symbol,
        "Amount": amount,
        "Comment": f"{comment_prefix}-{uuid.uuid4()}",
        "ClientId": f"{comment_prefix}-{uuid.uuid4()}",
    }
    if order_type == "Limit":
        payload["Price"] = parse_number_env("TT_ORDER_LIMIT_PRICE")
    return payload


class TickTraderWebSocketClient:
    def __init__(self, address: str, web_api_id: str, web_api_key: str, web_api_secret: str, timeout: float) -> None:
        self.address = address
        self.web_api_id = web_api_id
        self.web_api_key = web_api_key
        self.web_api_secret = web_api_secret
        self.timeout = timeout
        self.socket: socket.socket | ssl.SSLSocket | None = None
        self.buffer = b""
        self.sent_commands: list[str] = []

    @classmethod
    def from_env(cls, channel: str, timeout: float) -> "TickTraderWebSocketClient":
        missing = [
            name
            for name in ("TT_WEB_API_ID", "TT_WEB_API_KEY", "TT_WEB_API_SECRET")
            if not os.environ.get(name, "").strip()
        ]
        if missing:
            raise SmokeError("Missing WebSocket HMAC credentials: " + ", ".join(missing))

        explicit = os.environ.get("TT_WS_TRADE_URL" if channel == "trade" else "TT_WS_FEED_URL", "").strip()
        if explicit:
            address = explicit
        else:
            base_url = os.environ.get("TT_WS_BASE_URL", os.environ.get("TT_WS_HOST", DEFAULT_WS_BASE_URL)).strip() or DEFAULT_WS_BASE_URL
            port = os.environ.get("TT_WS_TRADE_PORT" if channel == "trade" else "TT_WS_FEED_PORT", "443").strip() or "443"
            path = "/trade" if channel == "trade" else "/feed"
            address = build_ws_address(base_url, port, path)

        return cls(
            address=address,
            web_api_id=env_required("TT_WEB_API_ID"),
            web_api_key=env_required("TT_WEB_API_KEY"),
            web_api_secret=env_required("TT_WEB_API_SECRET"),
            timeout=timeout,
        )

    def connect(self) -> None:
        parsed = urllib.parse.urlparse(self.address)
        if parsed.scheme != "wss":
            raise SmokeError("WebSocket address must start with wss://")
        host = parsed.hostname
        if not host:
            raise SmokeError("WebSocket address has no host")
        port = parsed.port or 443
        path = parsed.path or "/"
        if parsed.query:
            path += "?" + parsed.query
        raw = socket.create_connection((host, port), timeout=self.timeout)
        raw.settimeout(self.timeout)
        self.socket = ssl.create_default_context().wrap_socket(raw, server_hostname=host)
        key = base64.b64encode(os.urandom(16)).decode("ascii")
        self.sent_commands.append(f"WS HANDSHAKE GET {self.address}")
        request = (
            f"GET {path} HTTP/1.1\r\n"
            f"Host: {host}:{port}\r\n"
            "Upgrade: websocket\r\n"
            "Connection: Upgrade\r\n"
            f"Sec-WebSocket-Key: {key}\r\n"
            "Sec-WebSocket-Version: 13\r\n\r\n"
        ).encode("ascii")
        self.socket.sendall(request)
        response = self._read_http_response()
        if b" 101 " not in response.split(b"\r\n", 1)[0]:
            preview = response[:200].decode("utf-8", errors="replace")
            raise SmokeError(f"WebSocket handshake failed: {preview}")

    def close(self) -> None:
        if self.socket:
            with contextlib.suppress(Exception):
                self._send_frame(b"", opcode=0x8)
            self.socket.close()
            self.socket = None

    def login(self) -> dict[str, Any]:
        self.connect()
        timestamp = str(int(time.time() * 1000))
        signature = base64.b64encode(
            hmac.new(
                self.web_api_secret.encode("ascii"),
                (timestamp + self.web_api_id + self.web_api_key).encode("ascii"),
                hashlib.sha256,
            ).digest()
        ).decode("ascii")
        request_id = str(uuid.uuid4())
        self.send_json(
            {
                "Id": request_id,
                "Request": "Login",
                "Params": {
                    "AuthType": "HMAC",
                    "WebApiId": self.web_api_id,
                    "WebApiKey": self.web_api_key,
                    "Timestamp": int(timestamp),
                    "Signature": signature,
                    "DeviceId": "CodexLocalGui",
                    "AppSessionId": "ticktrader-smoke",
                },
            }
        )
        response = self.read_json_for_id(request_id)
        if response.get("Response") == "Error":
            raise SmokeError(f"WebSocket login error: {response.get('Error')}")
        return response

    def request(self, request_name: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        request_id = str(uuid.uuid4())
        payload: dict[str, Any] = {"Id": request_id, "Request": request_name}
        if params is not None:
            payload["Params"] = params
        self.send_json(payload)
        response = self.read_json_for_id(request_id)
        if response.get("Response") == "Error":
            raise SmokeError(f"WebSocket {request_name} error: {response.get('Error')}")
        return response

    def send_json(self, payload: dict[str, Any]) -> None:
        self.sent_commands.append("WS SEND " + json.dumps(redact_ws_payload(payload), ensure_ascii=False, separators=(",", ":")))
        body = json.dumps(payload, separators=(",", ":")).encode("utf-8")
        self._send_frame(body, opcode=0x1)

    def read_json_for_id(self, request_id: str) -> dict[str, Any]:
        deadline = time.monotonic() + self.timeout
        last_message: dict[str, Any] | None = None
        while time.monotonic() < deadline:
            message = self.read_json()
            last_message = message
            if message.get("Id") == request_id:
                return message
        raise SmokeError(f"timed out waiting for WebSocket response {request_id}; last={last_message}")

    def read_feed_ticks(self, symbol: str, max_ticks: int, timeout_seconds: float) -> list[str]:
        deadline = time.monotonic() + timeout_seconds
        ticks: list[str] = []
        old_timeout = self.socket.gettimeout() if self.socket else None
        if self.socket:
            self.socket.settimeout(1.0)
        try:
            while len(ticks) < max_ticks and time.monotonic() < deadline:
                try:
                    message = self.read_json()
                except (socket.timeout, TimeoutError):
                    continue
                if message.get("Response") != "FeedTick":
                    continue
                result = message.get("Result", {})
                if result.get("Symbol") != symbol:
                    continue
                bid = result.get("BestBid", {}).get("Price")
                ask = result.get("BestAsk", {}).get("Price")
                timestamp = result.get("Timestamp", "?")
                ticks.append(f"{symbol} bid={bid} ask={ask} ts={timestamp}")
        finally:
            if self.socket:
                self.socket.settimeout(old_timeout)
        return ticks

    def read_json(self) -> dict[str, Any]:
        frame = self._read_frame()
        try:
            return json.loads(frame.decode("utf-8"))
        except json.JSONDecodeError as exc:
            raise SmokeError(f"invalid WebSocket JSON: {frame[:120]!r}") from exc

    def _read_http_response(self) -> bytes:
        if not self.socket:
            raise SmokeError("WebSocket socket is not connected")
        data = b""
        while b"\r\n\r\n" not in data:
            chunk = self.socket.recv(4096)
            if not chunk:
                break
            data += chunk
        return data

    def _send_frame(self, payload: bytes, opcode: int) -> None:
        if not self.socket:
            raise SmokeError("WebSocket socket is not connected")
        header = bytearray()
        header.append(0x80 | opcode)
        length = len(payload)
        if length < 126:
            header.append(0x80 | length)
        elif length < 65536:
            header.extend([0x80 | 126, (length >> 8) & 0xFF, length & 0xFF])
        else:
            header.append(0x80 | 127)
            header.extend(length.to_bytes(8, "big"))
        mask = os.urandom(4)
        masked = bytes(byte ^ mask[index % 4] for index, byte in enumerate(payload))
        self.socket.sendall(bytes(header) + mask + masked)

    def _read_frame(self) -> bytes:
        if not self.socket:
            raise SmokeError("WebSocket socket is not connected")
        while len(self.buffer) < 2:
            self.buffer += self.socket.recv(4096)
        first, second = self.buffer[0], self.buffer[1]
        opcode = first & 0x0F
        masked = bool(second & 0x80)
        length = second & 0x7F
        offset = 2
        if length == 126:
            while len(self.buffer) < offset + 2:
                self.buffer += self.socket.recv(4096)
            length = int.from_bytes(self.buffer[offset:offset + 2], "big")
            offset += 2
        elif length == 127:
            while len(self.buffer) < offset + 8:
                self.buffer += self.socket.recv(4096)
            length = int.from_bytes(self.buffer[offset:offset + 8], "big")
            offset += 8
        mask = b""
        if masked:
            while len(self.buffer) < offset + 4:
                self.buffer += self.socket.recv(4096)
            mask = self.buffer[offset:offset + 4]
            offset += 4
        while len(self.buffer) < offset + length:
            self.buffer += self.socket.recv(4096)
        payload = self.buffer[offset:offset + length]
        self.buffer = self.buffer[offset + length:]
        if masked:
            payload = bytes(byte ^ mask[index % 4] for index, byte in enumerate(payload))
        if opcode == 0x8:
            raise SmokeError("WebSocket closed by remote")
        if opcode == 0x9:
            self._send_frame(payload, opcode=0xA)
            return self._read_frame()
        if opcode != 0x1:
            return self._read_frame()
        return payload

    def command_log(self) -> str:
        return "\n".join(self.sent_commands) if self.sent_commands else "no WebSocket messages sent"


def redact_ws_payload(value: Any) -> Any:
    if isinstance(value, dict):
        result: dict[str, Any] = {}
        for key, nested in value.items():
            if key in {"Signature", "WebApiKey"}:
                result[key] = "***"
            else:
                result[key] = redact_ws_payload(nested)
        return result
    if isinstance(value, list):
        return [redact_ws_payload(item) for item in value]
    return value


def build_ws_address(base_url: str, port: str, path: str) -> str:
    value = (base_url or DEFAULT_WS_BASE_URL).strip().rstrip("/")
    if "://" not in value:
        value = "wss://" + value
    parsed = urllib.parse.urlparse(value)
    if parsed.scheme != "wss":
        raise SmokeError("WebSocket base URL must start with wss://")
    netloc = parsed.hostname or parsed.netloc
    selected_port = (port or "").strip() or "443"
    if selected_port != "443":
        netloc = f"{netloc}:{selected_port}"
    return urllib.parse.urlunparse(("wss", netloc, path, "", "", ""))


@dataclass
class FixConfig:
    host: str
    port: int
    use_tls: bool
    verify_tls: bool
    begin_string: str
    sender_comp_id: str
    target_comp_id: str
    username: str
    password: str
    heartbeat: int
    reset_seq_num: bool
    timeout: float

    @classmethod
    def from_env(cls, channel: str, timeout: float) -> "FixConfig":
        port_name = "TT_FIX_TRADE_PORT" if channel == "trade" else "TT_FIX_FEED_PORT"
        sender = os.environ.get("TT_FIX_SENDER_COMP_ID", "").strip() or "client"
        return cls(
            host=os.environ.get("TT_FIX_HOST", "ttdemomarginal.fxopen.net").strip(),
            port=int(os.environ.get(port_name, "5002" if channel == "trade" else "5001")),
            use_tls=env_bool("TT_FIX_TLS", False),
            verify_tls=env_bool("TT_FIX_VERIFY_TLS", True),
            begin_string=os.environ.get("TT_FIX_BEGIN_STRING", "FIX.4.4").strip(),
            sender_comp_id=sender,
            target_comp_id=os.environ.get("TT_FIX_TARGET_COMP_ID", "EXECUTOR").strip(),
            username=env_required("TT_FIX_USERNAME"),
            password=env_required("TT_FIX_PASSWORD"),
            heartbeat=int(os.environ.get("TT_FIX_HEARTBTINT", "30")),
            reset_seq_num=env_bool("TT_FIX_RESET_SEQ_NUM", False),
            timeout=timeout,
        )


class RawFixClient:
    def __init__(self, config: FixConfig) -> None:
        self.config = config
        self.socket: socket.socket | ssl.SSLSocket | None = None
        self.out_seq = 1
        self.buffer = b""
        self.sent_commands: list[str] = []

    def connect(self) -> None:
        raw = socket.create_connection((self.config.host, self.config.port), timeout=self.config.timeout)
        raw.settimeout(self.config.timeout)
        if self.config.use_tls:
            if self.config.verify_tls:
                context = ssl.create_default_context()
            else:
                context = ssl._create_unverified_context()
            self.socket = context.wrap_socket(raw, server_hostname=self.config.host)
        else:
            self.socket = raw

    def close(self) -> None:
        if self.socket:
            self.socket.close()
            self.socket = None

    def logon(self) -> dict[str, str]:
        fields = [
            ("98", "0"),
            ("108", str(self.config.heartbeat)),
            ("553", self.config.username),
            ("554", self.config.password),
        ]
        if self.config.reset_seq_num:
            fields.append(("141", "Y"))
        self.send("A", fields)
        return self.read_message()

    def test_request(self) -> dict[str, str]:
        test_id = f"test-{int(time.time())}"
        self.send("1", [("112", test_id)])
        deadline = time.monotonic() + self.config.timeout
        while time.monotonic() < deadline:
            message = self.read_message()
            if message.get("35") == "0" and message.get("112") == test_id:
                return message
        raise SmokeError("no heartbeat response for TestRequest")

    def logout(self) -> dict[str, str]:
        self.send("5", [("58", "smoke logout")])
        try:
            return self.read_message()
        except SmokeError as exc:
            if is_remote_close_error(exc):
                return {"35": "closed"}
            raise

    def new_order_single_from_env(self) -> str:
        payload = order_payload_from_env(comment_prefix="fix")
        cl_ord_id = str(payload["ClientId"])
        fields = [
            ("11", cl_ord_id),
            ("55", str(payload["Symbol"])),
            ("54", "1" if payload["Side"] == "Buy" else "2"),
            ("38", str(payload["Amount"])),
            ("40", "1" if payload["Type"] == "Market" else "2"),
            ("60", fix_utc_timestamp()),
        ]
        if payload["Type"] == "Limit":
            fields.append(("44", str(payload["Price"])))
        self.send("D", fields)
        return cl_ord_id

    def cancel_order(self, orig_cl_ord_id: str) -> str:
        symbol = env_required("TT_ORDER_SYMBOL")
        side = os.environ.get("TT_ORDER_SIDE", "Buy").strip() or "Buy"
        cl_ord_id = f"cancel-{uuid.uuid4()}"
        self.send(
            "F",
            [
                ("11", cl_ord_id),
                ("41", orig_cl_ord_id),
                ("55", symbol),
                ("54", "1" if side == "Buy" else "2"),
                ("60", fix_utc_timestamp()),
            ],
        )
        return cl_ord_id

    def market_data_subscribe(self, symbol: str) -> str:
        request_id = f"md-{uuid.uuid4()}"
        self.send(
            "V",
            [
                ("262", request_id),
                ("263", "1"),
                ("264", "1"),
                ("265", "1"),
                ("267", "2"),
                ("269", "0"),
                ("269", "1"),
                ("146", "1"),
                ("55", symbol),
            ],
        )
        return request_id

    def market_data_unsubscribe(self, request_id: str, symbol: str) -> None:
        self.send(
            "V",
            [
                ("262", request_id),
                ("263", "2"),
                ("264", "1"),
                ("267", "2"),
                ("269", "0"),
                ("269", "1"),
                ("146", "1"),
                ("55", symbol),
            ],
        )

    def send(self, msg_type: str, fields: list[tuple[str, str]]) -> None:
        if not self.socket:
            raise SmokeError("FIX socket is not connected")
        header_fields = [
            ("35", msg_type),
            ("49", self.config.sender_comp_id),
            ("56", self.config.target_comp_id),
            ("34", str(self.out_seq)),
            ("52", fix_utc_timestamp()),
        ]
        self.out_seq += 1
        self.sent_commands.append(format_fix_command(self.config.begin_string, header_fields + fields))
        payload = build_fix_message(self.config.begin_string, header_fields + fields)
        self.socket.sendall(payload)

    def read_message(self) -> dict[str, str]:
        if not self.socket:
            raise SmokeError("FIX socket is not connected")
        deadline = time.monotonic() + self.config.timeout
        while time.monotonic() < deadline:
            parsed = pop_fix_message(self.buffer)
            if parsed:
                raw, self.buffer = parsed
                return parse_fix_fields(raw)
            try:
                chunk = self.socket.recv(4096)
            except socket.timeout:
                continue
            except (ConnectionResetError, ConnectionAbortedError, OSError) as exc:
                raise SmokeError(f"FIX socket closed by remote host: {exc}") from exc
            if not chunk:
                raise SmokeError("FIX socket closed by remote")
            self.buffer += chunk
        raise SmokeError("timed out waiting for FIX message")

    def read_until(self, message_types: set[str], timeout: float) -> dict[str, str]:
        deadline = time.monotonic() + timeout
        last: dict[str, str] | None = None
        while time.monotonic() < deadline:
            message = self.read_message()
            last = message
            if message.get("35") in message_types:
                return message
        raise SmokeError(f"timed out waiting for FIX {message_types}; last={last}")

    def command_log(self) -> str:
        return "\n".join(self.sent_commands) if self.sent_commands else "no FIX messages sent"


def is_remote_close_error(exc: Exception) -> bool:
    text = str(exc).lower()
    return "closed by remote" in text or "forcibly closed" in text or "connection reset" in text


def fix_utc_timestamp() -> str:
    return dt.datetime.now(dt.timezone.utc).strftime("%Y%m%d-%H:%M:%S.%f")[:-3]


def format_fix_command(begin_string: str, fields: list[tuple[str, str]]) -> str:
    masked = []
    for tag, value in fields:
        shown = "***" if tag in {"553", "554"} else value
        masked.append(f"{tag}={shown}")
    return f"FIX SEND 8={begin_string}|{'|'.join(masked)}"


def summarize_fix_market_data(message: dict[str, str], symbol: str) -> str:
    msg_type = message.get("35", "?")
    if msg_type in {"W", "X"}:
        bid = message.get("270") if message.get("269") == "0" else ""
        ask = message.get("270") if message.get("269") == "1" else ""
        price = f" price={message.get('270')}" if message.get("270") else ""
        size = f" size={message.get('271')}" if message.get("271") else ""
        return f"FIX tick {symbol} 35={msg_type}{price}{size}"
    text = f" text={message.get('58')}" if message.get("58") else ""
    return f"FIX message 35={msg_type}{text}"


def build_fix_message(begin_string: str, fields: list[tuple[str, str]]) -> bytes:
    body = SOH.join(f"{tag}={value}" for tag, value in fields) + SOH
    prefix = f"8={begin_string}{SOH}9={len(body.encode('ascii'))}{SOH}"
    without_checksum = (prefix + body).encode("ascii")
    checksum = sum(without_checksum) % 256
    return without_checksum + f"10={checksum:03d}{SOH}".encode("ascii")


def pop_fix_message(buffer: bytes) -> tuple[bytes, bytes] | None:
    start = buffer.find(b"8=FIX")
    if start < 0:
        return None
    if start > 0:
        buffer = buffer[start:]

    marker = b"\x019="
    body_len_start = buffer.find(marker)
    if body_len_start < 0:
        return None
    body_len_start += len(marker)
    body_len_end = buffer.find(b"\x01", body_len_start)
    if body_len_end < 0:
        return None
    try:
        body_length = int(buffer[body_len_start:body_len_end])
    except ValueError:
        raise SmokeError("invalid FIX BodyLength")

    body_start = body_len_end + 1
    checksum_start = body_start + body_length
    total_end = checksum_start + len(b"10=000\x01")
    if len(buffer) < total_end:
        return None
    message = buffer[:total_end]
    return message, buffer[total_end:]


def parse_fix_fields(raw_message: bytes) -> dict[str, str]:
    fields: dict[str, str] = {}
    for item in raw_message.decode("ascii", errors="replace").split(SOH):
        if not item or "=" not in item:
            continue
        tag, value = item.split("=", 1)
        fields[tag] = value
    return fields


def run_command(args: argparse.Namespace) -> int:
    load_env(args.env)
    symbol = args.symbol or os.environ.get("TT_SYMBOL", "EURUSD")
    client = make_rest_client(args)
    results: list[CheckResult] = []

    if args.command in {"public", "all"}:
        results.extend(rest_public_checks(client, symbol))

    if args.command in {"rest-auth", "all"}:
        results.extend(rest_auth_checks(client, symbol))

    if args.command in {"bars", "all"}:
        from_value = getattr(args, "bars_from", None) or os.environ.get("TT_BARS_FROM_UTC") or os.environ.get("TT_BARS_TIMESTAMP_UTC")
        to_value = getattr(args, "bars_to", None) or os.environ.get("TT_BARS_TO_UTC")
        timestamp_ms = None if args.no_bars_file else utc_timestamp_ms(from_value)
        timestamp_to_ms = parse_timestamp_ms(to_value) if to_value else None
        count = getattr(args, "bars_count", None) or int(os.environ.get("TT_BARS_COUNT", "0") or 0) or None
        periodicity = args.periodicity or os.environ.get("TT_PERIODICITY", "M1")
        price_type = args.price_type or os.environ.get("TT_PRICE_TYPE", "Bid")
        download_dir = args.download_dir if args.download else None
        results.extend(bars_checks(client, symbol, periodicity, price_type, timestamp_ms, download_dir, timestamp_to_ms, count))

    if args.command == "order-lifecycle":
        results.extend(order_lifecycle_check(client, args.enable_trading))

    if args.command == "fix-logon":
        results.extend(fix_logon_check(args))

    if args.command == "ws-feed":
        from_value = getattr(args, "bars_from", None) or os.environ.get("TT_BARS_FROM_UTC") or os.environ.get("TT_BARS_TIMESTAMP_UTC")
        timestamp_ms = utc_timestamp_ms(from_value)
        periodicity = args.periodicity or os.environ.get("TT_PERIODICITY", "M1")
        price_type = args.price_type or os.environ.get("TT_PRICE_TYPE", "Bid")
        count = args.bars_count or int(os.environ.get("TT_BARS_COUNT", "100") or 100)
        results.extend(websocket_feed_check(symbol, periodicity, price_type, timestamp_ms, count, args.timeout))

    if args.command == "ws-trade":
        results.extend(websocket_trade_check(args.timeout))

    if args.command == "ws-order":
        results.extend(websocket_order_check(args.enable_trading, args.timeout))

    if args.command == "fix-order":
        results.extend(fix_order_check(args.enable_trading, args.timeout))

    return print_results(results)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="FXOpen/TickTrader API smoke tests")
    parser.add_argument("--env", default=".env", help="path to .env file")
    parser.add_argument("--timeout", type=float, default=15.0, help="network timeout in seconds")
    parser.add_argument("--symbol", help="symbol for checks, default TT_SYMBOL or EURUSD")

    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("public", help="run public REST checks")
    subparsers.add_parser("rest-auth", help="run authenticated REST checks")

    bars = subparsers.add_parser("bars", help="download/check quote history bars")
    bars.add_argument("--periodicity", choices=["S1", "S10", "M1", "M5", "M15", "M30", "H1", "H4", "D1", "W1", "MN1"], help="bars periodicity")
    bars.add_argument("--price-type", choices=["Ask", "Bid"], help="bars price type")
    bars.add_argument("--bars-from", help="UTC start time, e.g. 2026-05-01T00:00:00Z")
    bars.add_argument("--bars-to", help="UTC end time, e.g. 2026-05-01T01:00:00Z")
    bars.add_argument("--bars-count", type=int, help="bar count, 1..1000 or negative")
    bars.add_argument("--no-bars-file", action="store_true", help="only check bars info endpoint")
    bars.add_argument("--download", action="store_true", help="save returned ZIP responses")
    bars.add_argument("--download-dir", default="downloads", help="directory for --download")

    order = subparsers.add_parser("order-lifecycle", help="create/modify/cancel a pending limit order")
    order.add_argument("--enable-trading", action="store_true", help="required guard for order placement")

    fix = subparsers.add_parser("fix-logon", help="run raw FIX logon/logout smoke check")
    fix.add_argument("--channel", choices=["trade", "feed"], default="trade", help="FIX channel")
    fix.add_argument("--test-request", action="store_true", help="send FIX TestRequest after logon")

    fix_order = subparsers.add_parser("fix-order", help="send a guarded test order through FIX")
    fix_order.add_argument("--enable-trading", action="store_true", help="required guard for order placement")

    ws_feed = subparsers.add_parser("ws-feed", help="run WebSocket feed smoke check")
    ws_feed.add_argument("--periodicity", choices=["S1", "S10", "M1", "M5", "M15", "M30", "H1", "H4", "D1", "W1", "MN1"], help="bars periodicity")
    ws_feed.add_argument("--price-type", choices=["Ask", "Bid"], help="bars price type")
    ws_feed.add_argument("--bars-from", help="UTC start time, e.g. 2026-05-01T00:00:00Z")
    ws_feed.add_argument("--bars-count", type=int, default=100, help="bar count")

    subparsers.add_parser("ws-trade", help="run WebSocket trade smoke check")

    ws_order = subparsers.add_parser("ws-order", help="send a guarded test order through WebSocket trade")
    ws_order.add_argument("--enable-trading", action="store_true", help="required guard for order placement")

    all_cmd = subparsers.add_parser("all", help="run public, authenticated, and bars REST checks")
    all_cmd.add_argument("--periodicity", choices=["S1", "S10", "M1", "M5", "M15", "M30", "H1", "H4", "D1", "W1", "MN1"], help="bars periodicity")
    all_cmd.add_argument("--price-type", choices=["Ask", "Bid"], help="bars price type")
    all_cmd.add_argument("--bars-from", help="UTC start time, e.g. 2026-05-01T00:00:00Z")
    all_cmd.add_argument("--bars-to", help="UTC end time, e.g. 2026-05-01T01:00:00Z")
    all_cmd.add_argument("--bars-count", type=int, help="bar count, 1..1000 or negative")
    all_cmd.add_argument("--no-bars-file", action="store_true", help="only check bars info endpoint")
    all_cmd.add_argument("--download", action="store_true", help="save returned ZIP responses")
    all_cmd.add_argument("--download-dir", default="downloads", help="directory for --download")
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    try:
        return run_command(args)
    except KeyboardInterrupt:
        print("Interrupted", file=sys.stderr)
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
