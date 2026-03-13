from __future__ import annotations

from pathlib import Path
from datetime import datetime
import logging
import shutil
import subprocess
import sys
import io
import contextlib
import warnings

import pandas as pd
from urllib3.exceptions import InsecureRequestWarning

# Базовые пути проекта
PROJECT_ROOT = Path(__file__).resolve().parent
SOURCEPYTHON_ROOT = PROJECT_ROOT.parent
COMMON_DIR = SOURCEPYTHON_ROOT / "common"

if str(COMMON_DIR) not in sys.path:
    sys.path.insert(0, str(COMMON_DIR))

from swap_markup import load_config, generate_swaps
from diff import (
    generate_difference,
    build_diff_thresholds,
    save_diff_thresholds,
    apply_threshold_flags,
)
from hsm_report import send_hsm_report


def setup_logging(log_dir: Path) -> logging.Logger:
    log_dir.mkdir(parents=True, exist_ok=True)

    log_filename = datetime.now().strftime("%Y-%m-%d") + ".log"
    log_path = log_dir / log_filename

    fmt = logging.Formatter(
        fmt="%(asctime)s.%(msecs)03d - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    root = logging.getLogger()
    root.setLevel(logging.INFO)

    for h in list(root.handlers):
        root.removeHandler(h)

    fh = logging.FileHandler(log_path, encoding="utf-8")
    fh.setLevel(logging.INFO)
    fh.setFormatter(fmt)
    root.addHandler(fh)

    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch.setFormatter(fmt)

    class OnlyMainLoggerFilter(logging.Filter):
        def filter(self, record: logging.LogRecord) -> bool:
            return record.name == "swaps_updater"

    ch.addFilter(OnlyMainLoggerFilter())
    root.addHandler(ch)

    logger = logging.getLogger("swaps_updater")
    logger.setLevel(logging.INFO)
    return logger


def run_quietly_to_log(func, log_name: str, *args, **kwargs):
    buf_out = io.StringIO()
    buf_err = io.StringIO()

    with contextlib.redirect_stdout(buf_out), contextlib.redirect_stderr(buf_err):
        result = func(*args, **kwargs)

    other_logger = logging.getLogger(log_name)

    out = buf_out.getvalue()
    if out:
        for line in out.splitlines():
            line = line.strip()
            if line:
                other_logger.info(line)

    err = buf_err.getvalue()
    if err:
        for line in err.splitlines():
            line = line.strip()
            if line:
                other_logger.warning(line)

    return result


def clear_today_root(today_root: Path) -> None:
    if not today_root.exists():
        return

    for entry in today_root.iterdir():
        if entry.is_dir():
            shutil.rmtree(entry)
        else:
            entry.unlink()


def build_storage_run_dir(storage_root: Path, date_str: str) -> Path:
    storage_root.mkdir(parents=True, exist_ok=True)

    existing = [
        p.name for p in storage_root.iterdir()
        if p.is_dir() and p.name.startswith(date_str)
    ]

    if date_str not in existing:
        folder_name = date_str
    else:
        suffixes = [0]
        for name in existing:
            if name == date_str:
                suffixes.append(0)
            elif name.startswith(date_str + "-"):
                try:
                    suffixes.append(int(name.split("-")[-1]))
                except ValueError:
                    continue
        next_suffix = max(suffixes) + 1
        folder_name = f"{date_str}-{next_suffix}"

    run_dir = storage_root / folder_name
    run_dir.mkdir(parents=True, exist_ok=True)
    return run_dir


def _log_process_output(logger_name: str, stdout: str | None, stderr: str | None) -> None:
    lg = logging.getLogger(logger_name)

    skip_patterns = [
        "Microsoft.Common.CurrentVersion.targets",
        "MSB3277",
        "System.Threading.Tasks.Dataflow",
    ]

    if stdout:
        for line in stdout.splitlines():
            line = line.strip()
            if not line:
                continue
            if any(p in line for p in skip_patterns):
                continue
            lg.info(line)

    if stderr:
        for line in stderr.splitlines():
            line = line.strip()
            if not line:
                continue
            if any(p in line for p in skip_patterns):
                continue
            lg.warning(line)


def _rel_to_root(swaps_root: Path, p: Path) -> str:
    try:
        return str(p.resolve().relative_to(swaps_root.resolve()))
    except Exception:
        return str(p.resolve())


def run_tts_fetcher(
    *,
    swaps_root: Path,
    tts_fetcher_csproj: Path,
) -> None:
    if not tts_fetcher_csproj.exists():
        raise FileNotFoundError(f"TtsSwapsFetcher.csproj not found: {tts_fetcher_csproj}")

    cmd = [
        "dotnet",
        "run",
        "--project",
        _rel_to_root(swaps_root, tts_fetcher_csproj),
    ]

    completed = subprocess.run(
        cmd,
        cwd=str(swaps_root),
        capture_output=True,
        text=True,
    )

    _log_process_output("tts_fetcher", completed.stdout, completed.stderr)

    if completed.returncode != 0:
        raise RuntimeError(
            "TTS fetcher failed.\n"
            f"CMD: {' '.join(cmd)}\n"
            f"ReturnCode: {completed.returncode}\n"
            f"STDOUT:\n{completed.stdout}\n"
            f"STDERR:\n{completed.stderr}\n"
        )


def run_tts_uploader(
    *,
    swaps_root: Path,
    tts_uploader_csproj: Path,
    config_yaml: Path,
    mapping_csv: Path,
    libs_tts_dir: Path,
    swaps_csv: Path,
    dry_run: bool,
    only_if_different: bool,
) -> None:
    if not tts_uploader_csproj.exists():
        raise FileNotFoundError(f"TtsSwapsUploader.csproj not found: {tts_uploader_csproj}")

    cmd = [
        "dotnet",
        "run",
        "--project",
        _rel_to_root(swaps_root, tts_uploader_csproj),
        "--",
        "--config",
        _rel_to_root(swaps_root, config_yaml),
        "--mapping",
        _rel_to_root(swaps_root, mapping_csv),
        "--libs",
        _rel_to_root(swaps_root, libs_tts_dir),
        "--swaps",
        _rel_to_root(swaps_root, swaps_csv),
        "--dryRun",
        "true" if dry_run else "false",
        "--onlyIfDifferent",
        "true" if only_if_different else "false",
    ]

    completed = subprocess.run(
        cmd,
        cwd=str(swaps_root),
        capture_output=True,
        text=True,
    )

    _log_process_output("tts_uploader", completed.stdout, completed.stderr)

    if completed.returncode != 0:
        raise RuntimeError(
            "TTS uploader failed.\n"
            f"CMD: {' '.join(cmd)}\n"
            f"ReturnCode: {completed.returncode}\n"
            f"STDOUT:\n{completed.stdout}\n"
            f"STDERR:\n{completed.stderr}\n"
        )


def main():
    swaps_root = PROJECT_ROOT

    config_path = swaps_root / "configDocker" / "config.yaml"
    config = load_config(config_path)

    paths_cfg = config.get("paths", {})
    data_dir = swaps_root / Path(paths_cfg.get("data_dir", "dataDocker"))
    log_dir = swaps_root / Path(paths_cfg.get("log_dir", data_dir / "log"))
    storage_dir = swaps_root / Path(paths_cfg.get("storage_dir", data_dir / "storage"))
    today_dir_root = swaps_root / Path(paths_cfg.get("today_dir", data_dir / "today"))

    mapping_csv = swaps_root / Path(
        paths_cfg.get("mapping_csv", paths_cfg.get("mapping_config", "configDocker/mapping.csv"))
    )

    tts_fetcher_csproj = swaps_root / Path(
        paths_cfg.get("tts_fetcher_csproj", "TtsSwapsFetcher/TtsSwapsFetcher.csproj")
    )
    tts_uploader_csproj = swaps_root / Path(
        paths_cfg.get("tts_uploader_csproj", "TtsSwapsUploader/TtsSwapsUploader.csproj")
    )

    libs_root = swaps_root / Path(paths_cfg.get("tts_libs_dir", "configDocker/libs"))
    libs_tts_dir = libs_root / "tts"

    uploader_cfg = config.get("tts_uploader", {})
    dry_run = bool(uploader_cfg.get("dry_run", False))
    only_if_different = bool(uploader_cfg.get("only_if_different", True))

    logger = setup_logging(log_dir)

    warnings.simplefilter("ignore", InsecureRequestWarning)

    logger.info("Starting swaps updater")

    date_str = datetime.now().strftime("%d-%m-%Y")

    run_storage_dir = build_storage_run_dir(storage_dir, date_str)

    clear_today_root(today_dir_root)

    today_dir = today_dir_root / date_str
    today_dir.mkdir(parents=True, exist_ok=True)

    logger.info("Running TTS fetcher (C#) ...")
    run_tts_fetcher(
        swaps_root=swaps_root,
        tts_fetcher_csproj=tts_fetcher_csproj,
    )

    tts_csv_today = today_dir / "tts_swaps_current.csv"
    if not tts_csv_today.exists():
        raise FileNotFoundError(f"TTS output not found after fetcher run: {tts_csv_today}")

    tts_csv_storage = run_storage_dir / "tts_swaps_current.csv"
    shutil.copy2(tts_csv_today, tts_csv_storage)
    logger.info("Saved TTS swaps CSV to storage: %s", tts_csv_storage)

    result_df: pd.DataFrame = run_quietly_to_log(generate_swaps, "swap_markup", config)

    if result_df.empty:
        logger.info("Finished swaps updater")
        return

    storage_csv = run_storage_dir / "swaps.csv"
    result_df.to_csv(storage_csv, index=False)
    logger.info("Saved storage CSV to %s", storage_csv)

    today_csv = today_dir / "swaps.csv"
    result_df.to_csv(today_csv, index=False)
    logger.info("Saved today CSV to %s", today_csv)

    df_diff = run_quietly_to_log(
        generate_difference,
        "diff",
        config=config,
        mapping_csv=mapping_csv,
        tts_swaps_current_csv=tts_csv_today,
        swap_csv=today_csv,
    )

    df_thr = run_quietly_to_log(build_diff_thresholds, "diff", config)
    run_quietly_to_log(save_diff_thresholds, "diff", config, df_thr)

    df_diff_flagged = run_quietly_to_log(
        apply_threshold_flags,
        "diff",
        df_diff,
        df_thr,
        decimals=6,
        use_abs=True,
    )

    diff_today = today_dir / "difference.csv"
    diff_storage = run_storage_dir / "difference.csv"
    df_diff_flagged.to_csv(diff_today, index=False)
    df_diff_flagged.to_csv(diff_storage, index=False)

    logger.info("Saved difference CSV to %s", diff_today)
    logger.info("Saved difference CSV to %s", diff_storage)

    logger.info("Running TTS uploader (C#) ...")
    run_tts_uploader(
        swaps_root=swaps_root,
        tts_uploader_csproj=tts_uploader_csproj,
        config_yaml=config_path,
        mapping_csv=mapping_csv,
        libs_tts_dir=libs_tts_dir,
        swaps_csv=today_csv,
        dry_run=dry_run,
        only_if_different=only_if_different,
    )
    logger.info("Finished TTS upload step")

    logger.info("Sending HSM report ...")
    run_quietly_to_log(send_hsm_report, "hsm_report", config_path)
    logger.info("Finished HSM report")

    logger.info("Finished swaps updater")


if __name__ == "__main__":
    main()