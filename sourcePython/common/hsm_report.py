from __future__ import annotations

from pathlib import Path
from datetime import datetime, timezone
from typing import Dict, Any, Optional
import os
import sys

import logging
import requests


def _detect_swap_updater_root() -> Path:
    # 1) Если явно передан root через env — используем его
    env_root = os.environ.get("SWAP_UPDATER_ROOT")
    if env_root:
        return Path(env_root).resolve()

    # 2) Локальная структура репозитория:
    # .../sourcePython/common/hsm_report.py
    # .../sourcePython/swap-updater
    local_candidate = Path(__file__).resolve().parents[1] / "swap-updater"
    if local_candidate.exists():
        return local_candidate

    # 3) Docker fallback
    return Path("/app").resolve()


# common/hsm_report.py
SWAP_UPDATER_ROOT = _detect_swap_updater_root()

# Чтобы hsm_report.py видел модули из swap-updater
if str(SWAP_UPDATER_ROOT) not in sys.path:
    sys.path.insert(0, str(SWAP_UPDATER_ROOT))

from swap_markup import load_config


logger = logging.getLogger(__name__)


def _iso_timestamp_ms() -> str:
    now = datetime.now(timezone.utc)
    return now.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]


def _find_difference_in_today(today_root: Path) -> Optional[Path]:
    date_str = datetime.now().strftime("%d-%m-%Y")
    p1 = today_root / date_str / "difference.csv"
    if p1.exists():
        return p1

    dd = datetime.now().strftime("%d")
    mm = datetime.now().strftime("%m")
    yyyy = datetime.now().strftime("%Y")
    p2 = today_root / dd / mm / yyyy / "difference.csv"
    if p2.exists():
        return p2

    candidates = list(today_root.rglob("difference.csv"))
    if not candidates:
        return None

    candidates.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    return candidates[0]


def _get_today_difference_path(config: Dict[str, Any]) -> Path:
    paths_cfg = config.get("paths", {})

    today_dir_cfg = paths_cfg.get("today_dir", "dataDocker/today")
    today_root = SWAP_UPDATER_ROOT / Path(today_dir_cfg)

    diff_path = _find_difference_in_today(today_root)
    if not diff_path or not diff_path.exists():
        raise FileNotFoundError(
            f"Today difference file not found under: {today_root}\n"
            f"Expected one of:\n"
            f" - {today_root}/DD-MM-YYYY/difference.csv\n"
            f" - {today_root}/DD/MM/YYYY/difference.csv\n"
            f"Also tried recursive search for latest difference.csv."
        )

    return diff_path


def send_file_to_hsm(
    config: Dict[str, Any],
    file_path: Path,
    comment: str = "OK",
    status: int = 1,
) -> None:
    hsm_cfg = config.get("hsm_setting", {})
    server = hsm_cfg.get("server")
    port = hsm_cfg.get("port")
    path = hsm_cfg.get("path")
    product_key = hsm_cfg.get("product_key")

    if not all([server, port, path, product_key]):
        raise ValueError("HSM settings are incomplete in config.yaml (hsm_setting).")

    url = f"https://{server}:{port}/api/Sensors/file"

    file_bytes = file_path.read_bytes()
    bytes_list = list(file_bytes)

    body = {
        "key": product_key,
        "path": path,
        "time": _iso_timestamp_ms(),
        "comment": comment,
        "status": status,
        "extension": file_path.suffix.lstrip(".") or "csv",
        "name": file_path.name,
        "value": bytes_list,
    }

    logger.info(
        "Sending file to HSM %s (path=%s, file=%s, size=%d bytes)",
        url,
        path,
        file_path.name,
        len(file_bytes),
    )

    resp = requests.post(
        url,
        json=body,
        timeout=30,
        verify=False,
    )

    if resp.status_code != 200:
        logger.error("HSM file upload failed: %s %s", resp.status_code, resp.text)
        raise RuntimeError(f"HSM file upload failed with code {resp.status_code}")

    logger.info("HSM file upload succeeded.")


def send_status_to_hsm(
    config: Dict[str, Any],
    value: str,
    status: int = 1,
    status_path: str = "SwapUpdaterAuto/Status",
) -> None:
    hsm_cfg = config.get("hsm_setting", {})
    server = hsm_cfg.get("server")
    port = hsm_cfg.get("port")
    product_key = hsm_cfg.get("product_key")

    if not all([server, port, product_key]):
        raise ValueError("HSM settings are incomplete in config.yaml (hsm_setting).")

    url = f"https://{server}:{port}/api/Sensors/string"

    body = {
        "key": product_key,
        "path": status_path,
        "time": _iso_timestamp_ms(),
        "comment": "",
        "status": status,
        "value": value,
    }

    logger.info("Sending HSM status '%s' to %s (path=%s)", value, url, status_path)

    resp = requests.post(
        url,
        json=body,
        timeout=30,
        verify=False,
    )

    if resp.status_code != 200:
        logger.error("HSM status update failed: %s %s", resp.status_code, resp.text)
        raise RuntimeError(f"HSM status update failed with code {resp.status_code}")

    logger.info("HSM status update succeeded.")


def send_hsm_report(config_path: str | Path = "configDocker/config.yaml") -> None:
    config = load_config(Path(config_path))

    diff_path = _get_today_difference_path(config)
    logger.info("Found today's difference CSV: %s", diff_path)

    try:
        send_file_to_hsm(config, diff_path, comment="Difference CSV uploaded", status=1)
        result_msg = "SUCCESS: difference CSV uploaded to HSM"
        st = 1
    except Exception as e:
        logger.exception("HSM upload error")
        result_msg = f"ERROR: {e}"
        st = 2

    try:
        send_status_to_hsm(config, result_msg, status=st)
    except Exception:
        logger.exception("HSM status update error")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s.%(msecs)03d - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    send_hsm_report()