#!/usr/bin/env python3
"""Local browser UI for TickTrader smoke checks."""

from __future__ import annotations

import argparse
import contextlib
import csv
import json
import mimetypes
import os
import socket
import threading
import time
import urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import yaml

import ticktrader_smoke as smoke


ROOT = Path(__file__).resolve().parent
WEB_ROOT = ROOT / "web"
DOWNLOAD_ROOT = ROOT / "downloads"
DEFAULT_REST_BASE_URL = "https://marginalttdemowebapi.fxopen.net"
DEFAULT_REST_PORT = "443"
DEFAULT_WS_BASE_URL = "wss://marginalttdemowebapi.fxopen.net"
ENV_LOCK = threading.Lock()
STREAM_LOCK = threading.Lock()
STREAMS: dict[str, dict[str, Any]] = {}
STREAM_MAX_EVENTS = 250

YAML_ENV_MAP: dict[str, str] = {
    "rest.address": "TT_REST_BASE_URL",
    "rest.port": "TT_REST_PORT",
    "rest.web_api_id": "TT_WEB_API_ID",
    "rest.web_api_key": "TT_WEB_API_KEY",
    "rest.web_api_secret": "TT_WEB_API_SECRET",
    "fix.host": "TT_FIX_HOST",
    "fix.trade_port": "TT_FIX_TRADE_PORT",
    "fix.feed_port": "TT_FIX_FEED_PORT",
    "fix.tls": "TT_FIX_TLS",
    "fix.reset_seq_num": "TT_FIX_RESET_SEQ_NUM",
    "fix.sender_comp_id": "TT_FIX_SENDER_COMP_ID",
    "fix.target_comp_id": "TT_FIX_TARGET_COMP_ID",
    "fix.login": "TT_FIX_USERNAME",
    "fix.password": "TT_FIX_PASSWORD",
    "websocket.address": "TT_WS_BASE_URL",
    "websocket.feed_port": "TT_WS_FEED_PORT",
    "websocket.trade_port": "TT_WS_TRADE_PORT",
    "symbol.name": "TT_SYMBOL",
}

_active_profile: str = ""
_config_env: dict[str, str] = {}
_all_profiles: dict[str, dict[str, str]] = {}
_available_profiles: list[str] = []
_config_path: Path = Path("configDocker/config.yaml")


def load_yaml_config(path: Path) -> tuple[str, dict[str, str], dict[str, dict[str, str]], list[str]]:
    with path.open("r", encoding="utf-8") as fh:
        data = yaml.safe_load(fh)

    if not isinstance(data, dict):
        raise ValueError(f"config.yaml must be a mapping, got {type(data).__name__}")

    active_name = str(data.get("active", "demo")).strip()
    profiles: dict[str, dict[str, str]] = {}
    for name, block in data.items():
        if name == "active" or not isinstance(block, dict):
            continue
        env_vars: dict[str, str] = {}
        for dotted_key, env_key in YAML_ENV_MAP.items():
            value = _deep_get(block, dotted_key)
            if value is not None:
                if isinstance(value, bool):
                    env_vars[env_key] = "true" if value else "false"
                else:
                    env_vars[env_key] = str(value)
        profiles[name] = env_vars

    if active_name not in profiles:
        raise ValueError(f"profile '{active_name}' not found in config.yaml. Available: {list(profiles.keys())}")

    return active_name, profiles[active_name], profiles, list(profiles.keys())


def switch_profile(name: str) -> None:
    global _active_profile, _config_env  # noqa: PLW0603
    if name not in _all_profiles:
        raise ValueError(f"profile '{name}' not found. Available: {_available_profiles}")
    for stream_name in list(STREAMS.keys()):
        with contextlib.suppress(Exception):
            stop_stream(stream_name)
    _active_profile = name
    _config_env = dict(_all_profiles[name])
    apply_config_to_env(_config_env)


def _deep_get(data: dict, dotted_key: str) -> Any:
    parts = dotted_key.split(".")
    node: Any = data
    for part in parts:
        if not isinstance(node, dict):
            return None
        node = node.get(part)
    return node


def apply_config_to_env(env_vars: dict[str, str]) -> None:
    for key, value in env_vars.items():
        os.environ[key] = value


@contextlib.contextmanager
def patched_environment(values: dict[str, str]):
    previous: dict[str, str | None] = {key: os.environ.get(key) for key in values}
    try:
        for key, value in values.items():
            os.environ[key] = value
        yield
    finally:
        for key, value in previous.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value


def bool_from_env(values: dict[str, str], name: str, default: bool = False) -> bool:
    value = values.get(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def safe_console_log(message: str) -> None:
    with contextlib.suppress(Exception):
        print(message, flush=True)


def credential_status(overrides: dict[str, str] | None = None) -> dict[str, Any]:
    values = dict(_config_env)
    if overrides:
        values.update(overrides)
    if not values.get("TT_SYMBOL", "").strip():
        values["TT_SYMBOL"] = "EURUSD"
    if not values.get("TT_ORDER_SYMBOL", "").strip() and values.get("TT_SYMBOL", "").strip():
        values["TT_ORDER_SYMBOL"] = values["TT_SYMBOL"]
    hmac_keys = ["TT_WEB_API_ID", "TT_WEB_API_KEY", "TT_WEB_API_SECRET"]
    fix_keys = ["TT_FIX_USERNAME", "TT_FIX_PASSWORD"]
    order_type = values.get("TT_ORDER_TYPE", "Limit").strip() or "Limit"
    order_keys = ["TT_ORDER_SYMBOL", "TT_ORDER_AMOUNT"]
    if order_type == "Limit":
        order_keys.append("TT_ORDER_LIMIT_PRICE")

    def missing(keys: list[str]) -> list[str]:
        return [key for key in keys if not values.get(key, "").strip()]

    return {
        "profile": _active_profile,
        "availableProfiles": _available_profiles,
        "restBaseUrl": values.get("TT_REST_BASE_URL", DEFAULT_REST_BASE_URL),
        "restPort": values.get("TT_REST_PORT", DEFAULT_REST_PORT),
        "symbol": values.get("TT_SYMBOL", "EURUSD"),
        "quoteDownloadApi": values.get("TT_QUOTE_DOWNLOAD_API", "REST"),
        "quoteHistoryKind": values.get("TT_QUOTE_HISTORY_KIND", "Ticks"),
        "periodicity": values.get("TT_PERIODICITY", "M1"),
        "priceType": values.get("TT_PRICE_TYPE", "Bid"),
        "barsFromUtc": values.get("TT_BARS_FROM_UTC", values.get("TT_BARS_TIMESTAMP_UTC", "")),
        "barsToUtc": values.get("TT_BARS_TO_UTC", ""),
        "fixHost": values.get("TT_FIX_HOST", "ttdemomarginal.fxopen.net"),
        "fixTradePort": values.get("TT_FIX_TRADE_PORT", "5002"),
        "fixFeedPort": values.get("TT_FIX_FEED_PORT", "5001"),
        "wsBaseUrl": values.get("TT_WS_BASE_URL", values.get("TT_WS_HOST", DEFAULT_WS_BASE_URL)),
        "wsFeedPort": values.get("TT_WS_FEED_PORT", "443"),
        "wsTradePort": values.get("TT_WS_TRADE_PORT", "443"),
        "hmac": {
            "ready": not missing(hmac_keys),
            "missing": missing(hmac_keys),
        },
        "fix": {
            "ready": not missing(fix_keys),
            "missing": missing(fix_keys),
            "targetCompId": values.get("TT_FIX_TARGET_COMP_ID", "EXECUTOR"),
            "senderCompId": values.get("TT_FIX_SENDER_COMP_ID", "client"),
            "senderCompIdSet": bool(values.get("TT_FIX_SENDER_COMP_ID", "").strip()),
            "tls": bool_from_env(values, "TT_FIX_TLS", False),
            "resetSeqNum": bool_from_env(values, "TT_FIX_RESET_SEQ_NUM", False),
        },
        "order": {
            "armed": bool_from_env(values, "TT_ENABLE_TRADING"),
            "ready": bool_from_env(values, "TT_ENABLE_TRADING") and not missing(order_keys),
            "missing": missing(order_keys),
            "symbol": values.get("TT_ORDER_SYMBOL", values.get("TT_SYMBOL", "")),
            "side": values.get("TT_ORDER_SIDE", "Buy"),
            "type": order_type,
            "amount": values.get("TT_ORDER_AMOUNT", ""),
            "limitPrice": values.get("TT_ORDER_LIMIT_PRICE", ""),
        },
    }


def request_env_updates(payload: dict[str, Any]) -> dict[str, str]:
    updates = dict(_config_env)
    optional_map = {
        "symbol": "TT_SYMBOL",
        "quoteDownloadApi": "TT_QUOTE_DOWNLOAD_API",
        "quoteHistoryKind": "TT_QUOTE_HISTORY_KIND",
        "periodicity": "TT_PERIODICITY",
        "priceType": "TT_PRICE_TYPE",
        "barsFromUtc": "TT_BARS_FROM_UTC",
        "barsToUtc": "TT_BARS_TO_UTC",
        "restPort": "TT_REST_PORT",
        "fixTradePort": "TT_FIX_TRADE_PORT",
        "fixFeedPort": "TT_FIX_FEED_PORT",
        "wsFeedPort": "TT_WS_FEED_PORT",
        "wsTradePort": "TT_WS_TRADE_PORT",
        "orderSide": "TT_ORDER_SIDE",
        "orderType": "TT_ORDER_TYPE",
        "orderAmount": "TT_ORDER_AMOUNT",
        "orderLimitPrice": "TT_ORDER_LIMIT_PRICE",
    }
    for payload_key, env_key in optional_map.items():
        value = str(payload.get(payload_key, "")).strip()
        if value:
            updates[env_key] = value
    boolean_map = {
        "fixTls": "TT_FIX_TLS",
        "fixResetSeqNum": "TT_FIX_RESET_SEQ_NUM",
    }
    for payload_key, env_key in boolean_map.items():
        if payload_key in payload:
            updates[env_key] = "true" if bool(payload.get(payload_key)) else "false"
    if payload.get("symbol"):
        updates["TT_ORDER_SYMBOL"] = str(payload.get("symbol")).strip()
    if payload.get("enableTrading"):
        updates["TT_ENABLE_TRADING"] = "true"
    check = str(payload.get("check", "")).strip()
    quote_api = str(payload.get("quoteDownloadApi", "")).strip()
    if check in {"fix-feed", "fix-feed-open", "fix-feed-close"} or (check == "bars" and quote_api == "FIX"):
        feed_port = updates.get("TT_FIX_FEED_PORT", "").strip()
        tls = str(updates.get("TT_FIX_TLS", "")).strip().lower() in {"1", "true", "yes", "y", "on"}
        if not feed_port:
            updates["TT_FIX_FEED_PORT"] = "5003" if tls else "5001"
    return updates


def append_stream_event(name: str, text: str) -> None:
    with STREAM_LOCK:
        session = STREAMS.get(name)
        if not session:
            return
        events = session.setdefault("events", [])
        events.append({"time": time.time(), "text": text})
        if len(events) > STREAM_MAX_EVENTS:
            del events[: len(events) - STREAM_MAX_EVENTS]


def stop_stream(name: str) -> smoke.CheckResult:
    with STREAM_LOCK:
        session = STREAMS.get(name)
    if not session:
        return smoke.CheckResult(stream_title(name), True, "stream already closed")

    session["stop"].set()
    client = session.get("client")
    if name == "fix-feed" and session.get("requestId") and hasattr(client, "market_data_unsubscribe"):
        with contextlib.suppress(Exception):
            client.market_data_unsubscribe(session["requestId"], session.get("symbol", ""))
    with contextlib.suppress(Exception):
        client.close()
    session["running"] = False
    append_stream_event(name, f"{stream_title(name)} closed")
    return smoke.CheckResult(stream_title(name), True, "stream closed")


def stream_title(name: str) -> str:
    return "FIX quote stream" if name == "fix-feed" else "WebSocket quote stream"


def stream_status(name: str) -> dict[str, Any]:
    with STREAM_LOCK:
        session = STREAMS.get(name)
        if not session:
            return {"name": name, "running": False, "events": []}
        return {
            "name": name,
            "running": bool(session.get("running")),
            "events": list(session.get("events", [])),
        }


def start_ws_feed_stream(symbol: str, timeout: float) -> list[smoke.CheckResult]:
    stop_stream("ws-feed")
    client = smoke.TickTraderWebSocketClient.from_env("feed", timeout)
    login = client.login()
    symbols = client.request("Symbols", {"Symbol": symbol})
    client.request("FeedSubscribe", {"Subscribe": [{"Symbol": symbol, "BookDepth": 1}]})
    stop_event = threading.Event()
    session = {"client": client, "stop": stop_event, "running": True, "events": [], "symbol": symbol}
    with STREAM_LOCK:
        STREAMS["ws-feed"] = session
    append_stream_event("ws-feed", "WS feed subscribed")

    def read_loop() -> None:
        try:
            while not stop_event.is_set():
                ticks = client.read_feed_ticks(symbol, max_ticks=1, timeout_seconds=1.5)
                for tick in ticks:
                    append_stream_event("ws-feed", "WS TICK " + tick)
        except Exception as exc:
            if not stop_event.is_set():
                append_stream_event("ws-feed", f"WS stream stopped: {exc}")
        finally:
            session["running"] = False

    threading.Thread(target=read_loop, name="ws-feed-stream", daemon=True).start()
    symbol_count = len(symbols.get("Result", {}).get("Symbols", []))
    detail = (
        f"login={login.get('Result', {}).get('Info', 'ok')}, symbols={symbol_count}, stream opened\n"
        f"SENT:\n{client.command_log()}"
    )
    return [smoke.CheckResult("WebSocket quote stream", True, detail)]


def start_fix_feed_stream(symbol: str, timeout: float, test_request: bool) -> list[smoke.CheckResult]:
    stop_stream("fix-feed")
    config = smoke.FixConfig.from_env("feed", timeout)
    client = smoke.RawFixClient(config)
    client.connect()
    try:
        logon_response = client.logon()
        if logon_response.get("35") != "A":
            raise smoke.SmokeError(f"FIX feed logon rejected: 35={logon_response.get('35')} {logon_response.get('58', '')}")
        heartbeat_detail = ""
        if test_request:
            heartbeat = client.test_request()
            heartbeat_detail = f", heartbeat 35={heartbeat.get('35', '?')}"
        request_id = client.market_data_subscribe(symbol)
        initial_messages = read_fix_feed_start_messages(client, symbol, timeout_seconds=2.0)
    except Exception:
        client.close()
        raise
    stop_event = threading.Event()
    session = {
        "client": client,
        "stop": stop_event,
        "running": True,
        "events": [],
        "symbol": symbol,
        "requestId": request_id,
    }
    with STREAM_LOCK:
        STREAMS["fix-feed"] = session
    append_stream_event("fix-feed", f"FIX MarketDataRequest opened id={request_id}")
    for message in initial_messages:
        append_stream_event("fix-feed", smoke.summarize_fix_market_data(message, symbol))

    def read_loop() -> None:
        old_timeout = client.config.timeout
        client.config.timeout = 1.0
        try:
            while not stop_event.is_set():
                try:
                    message = client.read_message()
                except smoke.SmokeError as exc:
                    if "timed out waiting" in str(exc):
                        continue
                    raise
                append_stream_event("fix-feed", smoke.summarize_fix_market_data(message, symbol))
        except Exception as exc:
            if not stop_event.is_set():
                append_stream_event("fix-feed", f"FIX stream stopped: {exc}")
        finally:
            client.config.timeout = old_timeout
            session["running"] = False

    threading.Thread(target=read_loop, name="fix-feed-stream", daemon=True).start()
    detail = f"logon accepted by {config.host}:{config.port}{heartbeat_detail}, stream opened\nSENT:\n{client.command_log()}"
    return [smoke.CheckResult("FIX quote stream", True, detail)]


def read_fix_feed_start_messages(client: smoke.RawFixClient, symbol: str, timeout_seconds: float) -> list[dict[str, str]]:
    messages: list[dict[str, str]] = []
    deadline = time.monotonic() + timeout_seconds
    old_timeout = client.config.timeout
    client.config.timeout = 0.7
    try:
        while time.monotonic() < deadline and len(messages) < 3:
            try:
                message = client.read_message()
            except smoke.SmokeError as exc:
                if "timed out waiting" in str(exc):
                    break
                raise
            messages.append(message)
            if is_console_fix_reject(message):
                raise smoke.SmokeError(
                    f"FIX feed rejected subscription: {smoke.summarize_fix_market_data(message, symbol)}\n"
                    f"SENT:\n{client.command_log()}"
                )
            if message.get("35") in {"W", "X", "S", "i", "f"}:
                break
    finally:
        client.config.timeout = old_timeout
    return messages


def run_console(payload: dict[str, Any]) -> dict[str, Any]:
    api = str(payload.get("consoleApi", "rest")).strip().lower()
    channel = str(payload.get("consoleChannel", "feed")).strip().lower()
    timeout = float(payload.get("timeout") or 15)
    started = time.time()
    request_text = str(payload.get("consoleRequest", "")).strip()
    if not request_text:
        raise smoke.SmokeError("Console request is empty")

    env_payload = dict(payload)
    if api == "fix" and channel == "feed":
        env_payload["check"] = "fix-feed"
    env_updates = request_env_updates(env_payload)

    with ENV_LOCK:
        with patched_environment(env_updates):
            if api == "rest":
                result = run_rest_console(request_text, bool(payload.get("consoleAuth")), timeout)
            elif api == "fix":
                result = run_fix_console(request_text, channel, timeout)
            elif api == "ws":
                result = run_ws_console(request_text, channel, timeout)
            else:
                raise smoke.SmokeError(f"Unknown console API: {api}")

    elapsed_ms = int((time.time() - started) * 1000)
    result["elapsedMs"] = elapsed_ms
    result["status"] = credential_status(env_updates)
    return result


def run_rest_console(request_text: str, auth: bool, timeout: float) -> dict[str, Any]:
    method, path, content = parse_rest_console_request(request_text)
    client = smoke.make_rest_client(SimpleNamespace(timeout=timeout))
    data = content.encode("utf-8") if content else None
    status, headers, body = client._request(method, path, data=data, content=content, auth=auth)  # noqa: SLF001
    body_text = body.decode("utf-8", errors="replace")
    pretty_body = pretty_json_text(body_text)
    detail = f"HTTP {status}\n{pretty_body}"
    return {
        "ok": 200 <= status < 400,
        "commands": list(client.sent_commands),
        "detail": detail,
        "response": {
            "status": status,
            "headers": headers,
            "body": pretty_body,
        },
    }


def parse_rest_console_request(request_text: str) -> tuple[str, str, str]:
    lines = request_text.replace("\r\n", "\n").split("\n")
    first = lines[0].strip()
    if first.startswith("{"):
        payload = json.loads(request_text)
        method = str(payload.get("method", "GET")).upper()
        path = normalize_console_rest_path(str(payload.get("path") or payload.get("url") or ""))
        body_value = payload.get("body", "")
        content = json.dumps(body_value, separators=(",", ":")) if isinstance(body_value, (dict, list)) else str(body_value or "")
        return method, path, content
    parts = first.split(maxsplit=1)
    if len(parts) != 2:
        raise smoke.SmokeError("REST request first line must be like: GET /api/v1/public/tick/EURUSD")
    method = parts[0].upper()
    path = normalize_console_rest_path(parts[1].strip())
    content = "\n".join(lines[1:]).strip()
    return method, path, content


def normalize_console_rest_path(value: str) -> str:
    if not value:
        raise smoke.SmokeError("REST path is empty")
    if value.startswith("http://") or value.startswith("https://"):
        parsed = urllib.parse.urlparse(value)
        path = parsed.path or "/"
        if parsed.query:
            path += "?" + parsed.query
        return path
    if not value.startswith("/"):
        value = "/" + value
    return value


def run_fix_console(request_text: str, channel: str, timeout: float) -> dict[str, Any]:
    if channel not in {"trade", "feed"}:
        raise smoke.SmokeError("FIX channel must be trade or feed")
    msg_type, fields = parse_fix_console_request(request_text)
    config = smoke.FixConfig.from_env(channel, timeout)
    client = smoke.RawFixClient(config)
    client.connect()
    responses: list[dict[str, str]] = []
    try:
        logon = client.logon()
        responses.append(logon)
        if logon.get("35") != "A":
            raise smoke.SmokeError(f"FIX logon rejected: {format_fix_dict(logon)}")
        client.send(msg_type, fields)
        responses.extend(read_fix_console_responses(client, timeout))
        with contextlib.suppress(Exception):
            client.logout()
    finally:
        client.close()

    response_text = "\n".join(f"FIX RECV {format_fix_dict(item)}" for item in responses)
    return {
        "ok": not any(is_console_fix_reject(item) for item in responses),
        "commands": list(client.sent_commands),
        "detail": response_text,
        "response": {"messages": responses},
    }


def parse_fix_console_request(request_text: str) -> tuple[str, list[tuple[str, str]]]:
    normalized = request_text.replace("\x01", "|").replace("\r", "\n").replace("\n", "|")
    fields: list[tuple[str, str]] = []
    msg_type = ""
    skip_tags = {"8", "9", "10", "34", "35", "49", "52", "56"}
    for item in normalized.split("|"):
        item = item.strip()
        if not item or "=" not in item:
            continue
        tag, value = item.split("=", 1)
        tag = tag.strip()
        value = value.strip()
        if tag == "35":
            msg_type = value
            continue
        if tag in skip_tags:
            continue
        fields.append((tag, value))
    if not msg_type:
        raise smoke.SmokeError("FIX request must contain 35=<MsgType>")
    return msg_type, fields


def read_fix_console_responses(client: smoke.RawFixClient, timeout: float) -> list[dict[str, str]]:
    responses: list[dict[str, str]] = []
    deadline = time.monotonic() + max(0.8, min(timeout, 5.0))
    old_timeout = client.config.timeout
    client.config.timeout = 0.8
    try:
        while time.monotonic() < deadline and len(responses) < 8:
            try:
                message = client.read_message()
            except smoke.SmokeError as exc:
                if "timed out waiting" in str(exc):
                    break
                raise
            responses.append(message)
            if is_console_fix_reject(message):
                break
    finally:
        client.config.timeout = old_timeout
    return responses


def is_console_fix_reject(message: dict[str, str]) -> bool:
    return message.get("35") in {"3", "j", "Y"} or message.get("58", "").lower().startswith("unsupported")


def format_fix_dict(message: dict[str, str]) -> str:
    return "|".join(f"{tag}={value}" for tag, value in message.items())


def run_ws_console(request_text: str, channel: str, timeout: float) -> dict[str, Any]:
    if channel not in {"trade", "feed"}:
        raise smoke.SmokeError("WebSocket channel must be trade or feed")
    request_name, params = parse_ws_console_request(request_text)
    client = smoke.TickTraderWebSocketClient.from_env(channel, timeout)
    try:
        login = client.login()
        response = client.request(request_name, params)
    finally:
        client.close()
    detail = json.dumps({"login": login, "response": response}, ensure_ascii=False, indent=2)
    return {
        "ok": response.get("Response") != "Error",
        "commands": list(client.sent_commands),
        "detail": detail,
        "response": {"login": login, "message": response},
    }


def parse_ws_console_request(request_text: str) -> tuple[str, dict[str, Any] | None]:
    text = request_text.strip()
    if text.startswith("{"):
        payload = json.loads(text)
        request_name = str(payload.get("Request") or payload.get("request") or "").strip()
        if not request_name:
            raise smoke.SmokeError("WebSocket JSON must contain Request")
        params = payload.get("Params", payload.get("params"))
        if params is not None and not isinstance(params, dict):
            raise smoke.SmokeError("WebSocket Params must be an object")
        return request_name, params
    first, _, rest = text.partition("\n")
    parts = first.strip().split(maxsplit=1)
    request_name = parts[0] if parts else ""
    if not request_name:
        raise smoke.SmokeError("WebSocket request name is empty")
    raw_params = parts[1] if len(parts) > 1 else rest.strip()
    params = json.loads(raw_params) if raw_params else None
    if params is not None and not isinstance(params, dict):
        raise smoke.SmokeError("WebSocket params must be a JSON object")
    return request_name, params


def pretty_json_text(value: str) -> str:
    try:
        return json.dumps(json.loads(value), ensure_ascii=False, indent=2)
    except Exception:
        return value


def quote_download_file_name(symbol: str, api: str, kind: str) -> tuple[str, str]:
    os.makedirs(DOWNLOAD_ROOT, exist_ok=True)
    safe_symbol = "".join(ch for ch in symbol if ch.isalnum() or ch in {"_", "-"}).strip() or "symbol"
    stamp = time.strftime("%Y%m%d-%H%M%S", time.gmtime())
    file_name = f"{safe_symbol}-{api.lower()}-{kind.lower()}-{stamp}.csv"
    return file_name, str(DOWNLOAD_ROOT / file_name)


def ws_quote_download_checks(
    symbol: str,
    history_kind: str,
    periodicity: str,
    price_type: str,
    timestamp_ms: int,
    timestamp_to_ms: int | None,
    timeout: float,
) -> list[smoke.CheckResult]:
    kind = "Bars" if history_kind == "Bars" else "Ticks"

    def download() -> smoke.CheckResult:
        client = smoke.TickTraderWebSocketClient.from_env("feed", timeout)
        try:
            client.login()
            if kind == "Bars":
                count = smoke.calculate_bar_count(timestamp_ms, timestamp_to_ms, periodicity)
                response = client.request(
                    "QuoteHistoryBars",
                    {
                        "Symbol": symbol,
                        "Periodicity": periodicity,
                        "PriceType": price_type.lower(),
                        "Timestamp": timestamp_ms,
                        "Count": count,
                    },
                )
                rows = response.get("Result", {}).get("Bars", [])
                if not isinstance(rows, list):
                    raise smoke.SmokeError("WebSocket bars response has no Bars list")
                file_name, file_path = quote_download_file_name(symbol, "ws", kind)
                smoke.write_quote_csv(file_path, rows, kind)
                first = rows[0].get("Timestamp") if rows else "none"
                last = rows[-1].get("Timestamp") if rows else "none"
                detail = f"WS bars downloaded: {len(rows)} rows, first={first}, last={last}\nSENT:\n{client.command_log()}"
            else:
                client.request("FeedSubscribe", {"Subscribe": [{"Symbol": symbol, "BookDepth": 1}]})
                rows = collect_ws_live_ticks(client, symbol, min(max(timeout, 3.0), 12.0), timestamp_to_ms)
                file_name, file_path = quote_download_file_name(symbol, "ws", kind)
                smoke.write_quote_csv(file_path, rows, kind)
                first = rows[0].get("Timestamp") if rows else "none"
                last = rows[-1].get("Timestamp") if rows else "none"
                detail = f"WS ticks downloaded: {len(rows)} rows, first={first}, last={last}\nSENT:\n{client.command_log()}"
            return smoke.CheckResult(f"WS quote download {kind.lower()} {symbol}", True, detail, f"/downloads/{urllib.parse.quote(file_name)}", file_name)
        finally:
            client.close()

    try:
        return [download()]
    except Exception as exc:
        return [smoke.CheckResult(f"WS quote download {kind.lower()} {symbol}", False, str(exc))]


def collect_ws_live_ticks(
    client: smoke.TickTraderWebSocketClient,
    symbol: str,
    timeout_seconds: float,
    timestamp_to_ms: int | None,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    deadline = time.monotonic() + timeout_seconds
    old_timeout = client.socket.gettimeout() if client.socket else None
    if client.socket:
        client.socket.settimeout(1.0)
    try:
        while time.monotonic() < deadline and len(rows) < 1000:
            try:
                message = client.read_json()
            except (socket.timeout, TimeoutError):
                continue
            if message.get("Response") != "FeedTick":
                continue
            row = message.get("Result", {})
            if row.get("Symbol") != symbol:
                continue
            timestamp = row.get("Timestamp")
            if timestamp_to_ms is not None and isinstance(timestamp, int) and timestamp > timestamp_to_ms:
                break
            rows.append(row)
    finally:
        if client.socket:
            client.socket.settimeout(old_timeout)
    return rows


def fix_quote_download_checks(
    symbol: str,
    history_kind: str,
    timeout: float,
    test_request: bool,
) -> list[smoke.CheckResult]:
    kind = "Bars" if history_kind == "Bars" else "Ticks"
    if kind == "Bars":
        return [smoke.CheckResult("FIX quote download bars", False, "FIX feed downloads live ticks only. Choose Ticks or use REST/WS for bars history.")]

    def download() -> smoke.CheckResult:
        config = smoke.FixConfig.from_env("feed", timeout)
        client = smoke.RawFixClient(config)
        client.connect()
        try:
            logon = client.logon()
            if logon.get("35") != "A":
                raise smoke.SmokeError(f"FIX feed logon rejected: 35={logon.get('35')} {logon.get('58', '')}")
            if test_request:
                client.test_request()
            request_id = client.market_data_subscribe(symbol)
            rows = collect_fix_live_ticks(client, symbol, min(max(timeout, 3.0), 12.0))
            with contextlib.suppress(Exception):
                client.market_data_unsubscribe(request_id, symbol)
            file_name, file_path = quote_download_file_name(symbol, "fix", kind)
            write_fix_tick_csv(file_path, rows)
            detail = f"FIX ticks downloaded: {len(rows)} rows\nSENT:\n{client.command_log()}"
            return smoke.CheckResult(f"FIX quote download ticks {symbol}", True, detail, f"/downloads/{urllib.parse.quote(file_name)}", file_name)
        finally:
            client.close()

    try:
        return [download()]
    except Exception as exc:
        return [smoke.CheckResult(f"FIX quote download ticks {symbol}", False, str(exc))]


def collect_fix_live_ticks(client: smoke.RawFixClient, symbol: str, timeout_seconds: float) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    deadline = time.monotonic() + timeout_seconds
    old_timeout = client.config.timeout
    client.config.timeout = 0.8
    try:
        while time.monotonic() < deadline and len(rows) < 1000:
            try:
                message = client.read_message()
            except smoke.SmokeError as exc:
                if "timed out waiting" in str(exc):
                    continue
                raise
            if is_console_fix_reject(message):
                raise smoke.SmokeError(f"FIX feed rejected subscription: {smoke.summarize_fix_market_data(message, symbol)}")
            if message.get("35") in {"W", "X", "S", "i"}:
                rows.append(
                    {
                        "Timestamp": message.get("52", ""),
                        "MsgType": message.get("35", ""),
                        "Symbol": message.get("55", symbol),
                        "MDEntryType": message.get("269", ""),
                        "Price": message.get("270", ""),
                        "Size": message.get("271", ""),
                        "Raw": smoke.format_fix_command("FIX.4.4", list(message.items())),
                    }
                )
    finally:
        client.config.timeout = old_timeout
    return rows


def write_fix_tick_csv(path: str, rows: list[dict[str, Any]]) -> None:
    with open(path, "w", encoding="utf-8", newline="") as output:
        writer = csv.DictWriter(output, fieldnames=["Timestamp", "MsgType", "Symbol", "MDEntryType", "Price", "Size", "Raw"])
        writer.writeheader()
        writer.writerows(rows)


def run_check(payload: dict[str, Any]) -> dict[str, Any]:
    check = str(payload.get("check", "")).strip()
    timeout = float(payload.get("timeout") or 15)
    started = time.time()
    env_updates = request_env_updates(payload)

    with ENV_LOCK:
        with patched_environment(env_updates):
            symbol = str(payload.get("symbol") or os.environ.get("TT_SYMBOL", "EURUSD")).strip()
            args = SimpleNamespace(timeout=timeout)
            client = smoke.make_rest_client(args)

            if check == "public":
                results = smoke.rest_public_checks(client, symbol)
            elif check == "rest-auth":
                results = smoke.rest_auth_checks(client, symbol)
            elif check == "rest-quotes":
                results = smoke.rest_quote_checks(client, symbol)
            elif check == "rest-trade":
                results = smoke.rest_trade_checks(client)
            elif check == "bars":
                quote_api = str(payload.get("quoteDownloadApi") or os.environ.get("TT_QUOTE_DOWNLOAD_API", "REST")).strip()
                quote_history_kind = str(payload.get("quoteHistoryKind") or os.environ.get("TT_QUOTE_HISTORY_KIND", "Ticks"))
                periodicity = str(payload.get("periodicity") or os.environ.get("TT_PERIODICITY", "M1"))
                price_type = str(payload.get("priceType") or os.environ.get("TT_PRICE_TYPE", "Bid"))
                from_value = os.environ.get("TT_BARS_FROM_UTC") or os.environ.get("TT_BARS_TIMESTAMP_UTC")
                to_value = os.environ.get("TT_BARS_TO_UTC", "")
                timestamp_ms = smoke.utc_timestamp_ms(from_value)
                timestamp_to_ms = smoke.parse_timestamp_ms(to_value) if to_value else None
                if quote_api == "WS":
                    results = ws_quote_download_checks(symbol, quote_history_kind, periodicity, price_type, timestamp_ms, timestamp_to_ms, timeout)
                elif quote_api == "FIX":
                    results = fix_quote_download_checks(symbol, quote_history_kind, timeout, bool(payload.get("testRequest")))
                else:
                    results = smoke.quote_history_download_checks(
                        client,
                        symbol,
                        quote_history_kind,
                        periodicity,
                        price_type,
                        timestamp_ms,
                        timestamp_to_ms,
                        str(DOWNLOAD_ROOT),
                        "/downloads",
                    )
            elif check == "rest-bundle":
                periodicity = str(payload.get("periodicity") or os.environ.get("TT_PERIODICITY", "M1"))
                price_type = str(payload.get("priceType") or os.environ.get("TT_PRICE_TYPE", "Bid"))
                from_value = os.environ.get("TT_BARS_FROM_UTC") or os.environ.get("TT_BARS_TIMESTAMP_UTC")
                timestamp_ms = smoke.utc_timestamp_ms(from_value)
                results = []
                results.extend(smoke.rest_public_checks(client, symbol))
                results.extend(smoke.rest_auth_checks(client, symbol))
                results.extend(smoke.bars_checks(client, symbol, periodicity, price_type, timestamp_ms, None))
            elif check in {"fix-trade", "fix-feed"}:
                fix_args = SimpleNamespace(
                    channel="trade" if check == "fix-trade" else "feed",
                    timeout=timeout,
                    test_request=bool(payload.get("testRequest")),
                )
                results = smoke.fix_logon_check(fix_args)
            elif check == "fix-feed-open":
                results = start_fix_feed_stream(symbol, timeout, bool(payload.get("testRequest")))
            elif check == "fix-feed-close":
                results = [stop_stream("fix-feed")]
            elif check == "order-rest":
                results = smoke.order_lifecycle_check(client, bool(payload.get("enableTrading")))
            elif check == "order-fix":
                results = smoke.fix_order_check(bool(payload.get("enableTrading")), timeout)
            elif check == "order-ws":
                results = smoke.websocket_order_check(bool(payload.get("enableTrading")), timeout)
            elif check == "ws-feed":
                periodicity = str(payload.get("periodicity") or os.environ.get("TT_PERIODICITY", "M1"))
                price_type = str(payload.get("priceType") or os.environ.get("TT_PRICE_TYPE", "Bid"))
                from_value = os.environ.get("TT_BARS_FROM_UTC") or os.environ.get("TT_BARS_TIMESTAMP_UTC")
                to_value = os.environ.get("TT_BARS_TO_UTC", "")
                timestamp_ms = smoke.utc_timestamp_ms(from_value)
                timestamp_to_ms = smoke.parse_timestamp_ms(to_value) if to_value else None
                count = smoke.calculate_bar_count(timestamp_ms, timestamp_to_ms, periodicity)
                results = smoke.websocket_feed_check(symbol, periodicity, price_type, timestamp_ms, count, timeout)
            elif check == "ws-feed-open":
                results = start_ws_feed_stream(symbol, timeout)
            elif check == "ws-feed-close":
                results = [stop_stream("ws-feed")]
            elif check == "ws-trade":
                results = smoke.websocket_trade_check(timeout)
            else:
                raise smoke.SmokeError(f"Unknown check: {check}")

    elapsed_ms = int((time.time() - started) * 1000)
    return {
        "ok": all(item.ok for item in results),
        "elapsedMs": elapsed_ms,
        "commands": list(getattr(client, "sent_commands", [])),
        "results": [item.__dict__ for item in results],
        "status": credential_status(env_updates),
    }


class GuiHandler(BaseHTTPRequestHandler):
    server_version = "TickTraderGui/1.0"

    def do_GET(self) -> None:
        parsed_url = urllib.parse.urlparse(self.path)
        if parsed_url.path == "/api/status":
            self.send_json(credential_status())
            return
        if parsed_url.path == "/api/stream-status":
            query = urllib.parse.parse_qs(parsed_url.query)
            name = query.get("name", [""])[0]
            self.send_json(stream_status(name))
            return

        path = parsed_url.path
        if path.startswith("/downloads/"):
            raw_name = urllib.parse.unquote(path.removeprefix("/downloads/"))
            if "/" in raw_name or "\\" in raw_name or not raw_name:
                self.send_error(404)
                return
            file_path = (DOWNLOAD_ROOT / raw_name).resolve()
            if not str(file_path).startswith(str(DOWNLOAD_ROOT.resolve())) or not file_path.exists():
                self.send_error(404)
                return
            data = file_path.read_bytes()
            self.send_response(200)
            self.send_header("Content-Type", mimetypes.guess_type(str(file_path))[0] or "text/csv")
            self.send_header("Content-Disposition", f'attachment; filename="{file_path.name}"')
            self.send_header("Content-Length", str(len(data)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(data)
            return

        if path == "/":
            path = "/index.html"
        file_path = (WEB_ROOT / path.lstrip("/")).resolve()
        if not str(file_path).startswith(str(WEB_ROOT.resolve())) or not file_path.exists():
            self.send_error(404)
            return
        content_type = mimetypes.guess_type(str(file_path))[0] or "application/octet-stream"
        data = file_path.read_bytes()
        self.send_response(200)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(data)

    def do_POST(self) -> None:
        if self.path == "/api/switch-profile":
            try:
                length = int(self.headers.get("Content-Length", "0"))
                payload = json.loads(self.rfile.read(length).decode("utf-8") or "{}")
                profile = str(payload.get("profile", "")).strip()
                if not profile:
                    raise ValueError("profile is required")
                with ENV_LOCK:
                    switch_profile(profile)
                safe_console_log(f"Switched to profile: {profile}")
                self.send_json({"ok": True, "status": credential_status()})
            except Exception as exc:
                self.send_json({"ok": False, "error": str(exc)}, status=400)
            return
        if self.path not in {"/api/check", "/api/console"}:
            self.send_error(404)
            return
        try:
            length = int(self.headers.get("Content-Length", "0"))
            payload = json.loads(self.rfile.read(length).decode("utf-8") or "{}")
            if self.path == "/api/console":
                response = run_console(payload)
            else:
                response = run_check(payload)
            self.send_json(response)
        except Exception as exc:
            self.send_json(
                {
                    "ok": False,
                    "elapsedMs": 0,
                    "results": [{"name": "GUI request", "ok": False, "detail": str(exc)}],
                    "status": credential_status(),
                },
                status=500,
            )

    def log_message(self, format: str, *args: Any) -> None:
        safe_console_log(f"{self.address_string()} - {format % args}")

    def send_json(self, payload: dict[str, Any], status: int = 200) -> None:
        data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(data)


class TickTraderGuiServer(ThreadingHTTPServer):
    def __init__(self, server_address: tuple[str, int], handler: type[BaseHTTPRequestHandler]):
        super().__init__(server_address, handler)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Local TickTrader smoke-test GUI")
    parser.add_argument("--host", default="127.0.0.1", help="bind host")
    parser.add_argument("--port", type=int, default=8765, help="bind port")
    parser.add_argument("--config", default="configDocker/config.yaml", help="path to config.yaml")
    return parser


def main() -> int:
    global _active_profile, _config_env, _all_profiles, _available_profiles, _config_path  # noqa: PLW0603

    args = build_parser().parse_args()
    config_path = (ROOT / args.config).resolve() if not os.path.isabs(args.config) else Path(args.config).resolve()
    _config_path = config_path

    _active_profile, _config_env, _all_profiles, _available_profiles = load_yaml_config(config_path)
    apply_config_to_env(_config_env)

    safe_console_log(f"TickTrader GUI running at http://{args.host}:{args.port}")
    safe_console_log(f"Config: {config_path} (profile: {_active_profile}, available: {_available_profiles})")

    server = TickTraderGuiServer((args.host, args.port), GuiHandler)
    server.serve_forever()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
