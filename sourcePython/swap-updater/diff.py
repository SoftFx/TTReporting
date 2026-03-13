from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, Optional, Tuple

import pandas as pd
import yaml
import re

# Глобальные настройки
# Количество знаков после запятой для diff-значений
DIFF_DECIMALS = 6

# Индексы в TTS начинаются с символа '#'
# Используем это, чтобы отличать индексы от валют и прочих инструментов
INDEX_SYMBOL_RE = re.compile(r"^#")

# Порог отклонения от OLD swap в процентах
# 20% означает диапазон [old * 0.8 ; old * 1.2]
THRESHOLD_PCT = 20.0

@dataclass
class OutputPaths:
    """
    Просто удобный контейнер для путей вывода.
    """
    today_dir: Path
    storage_dir: Path

# Базовые утилиты
def load_config(path: Path) -> Dict[str, Any]:
    """
    Загружаем config.yaml.
    Без него скрипт не имеет смысла, поэтому если файла нет — сразу падаем.
    """
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {path}")
    with path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def _ensure_dir(p: Path) -> None:
    """
    Создаёт директорию, если она ещё не существует.
    """
    p.mkdir(parents=True, exist_ok=True)

def resolve_output_paths(config: Dict[str, Any]) -> OutputPaths:
    """
    Забираем пути из config.yaml.
    Если их нет — используем дефолтные значения.
    """
    paths_cfg = config.get("paths", {}) or {}
    today_dir = Path(paths_cfg.get("today_dir", "dataDocker/today"))
    storage_dir = Path(paths_cfg.get("storage_dir", "dataDocker/storage"))
    return OutputPaths(today_dir=today_dir, storage_dir=storage_dir)

def _today_folder(today_dir: Path) -> Path:
    """
    Формируем папку текущего дня в формате DD-MM-YYYY.
    """
    folder = today_dir / datetime.now().strftime("%d-%m-%Y")
    _ensure_dir(folder)
    return folder

def _norm_cols(df: pd.DataFrame) -> pd.DataFrame:
    """
    Приводим названия колонок к нижнему регистру
    и убираем лишние пробелы — чтобы не зависеть от формата CSV.
    """
    df = df.copy()
    df.columns = [str(c).strip().lower() for c in df.columns]
    return df

def _is_index_symbol(sym: str) -> bool:
    """
    Проверяем, является ли символ индексом (начинается с '#').
    """
    return bool(INDEX_SYMBOL_RE.match(str(sym)))

def _thr_digits_for_symbol(sym: str) -> int:
    """
    Для индексов используем 6 знаков,
    для остальных инструментов — 4.
    """
    return 6 if _is_index_symbol(sym) else 4

# Загрузка mapping.csv
def load_mapping(mapping_csv: Path) -> Tuple[Dict[str, str], Dict[str, str]]:
    """
    Загружаем mapping.csv и строим два словаря:

    1) lp_symbol_to_tts:
       LP symbol -> TTS symbol
       Используется для сопоставления новых свопов с текущими.

    2) tts_to_lp_value:
       TTS symbol -> LP / provider (для колонки lp в отчёте).

    Если колонка provider (или её аналоги) есть — используем её,
    иначе в качестве lp просто берём lp_symbol.
    """
    if not mapping_csv.exists():
        raise FileNotFoundError(f"mapping.csv not found: {mapping_csv}")

    df = pd.read_csv(mapping_csv, sep=None, engine="python")
    df = _norm_cols(df)

    if "tts" not in df.columns:
        raise KeyError(
            f"{mapping_csv.name} must contain column 'tts'. "
            f"Detected columns: {list(df.columns)}"
        )

    # Пытаемся найти колонку с LP-символом
    lp_symbol_col: Optional[str] = None
    for cand in ["lp_symbol", "lpsymbol", "lp", "symbol", "symbols"]:
        if cand in df.columns:
            lp_symbol_col = cand
            break

    if lp_symbol_col is None:
        raise KeyError(
            f"{mapping_csv.name} must contain lp symbol column and 'tts'. "
            f"Detected columns: {list(df.columns)}"
        )

    # Пытаемся найти колонку с названием LP / провайдера
    provider_col: Optional[str] = None
    for cand in ["provider", "lp_name", "source", "liquidity_provider", "lp_provider"]:
        if cand in df.columns:
            provider_col = cand
            break

    df["tts"] = df["tts"].astype(str).str.strip()
    df[lp_symbol_col] = df[lp_symbol_col].astype(str).str.strip()

    if provider_col is not None:
        df[provider_col] = df[provider_col].astype(str).str.strip()

    df = df[(df["tts"] != "") & (df[lp_symbol_col] != "")]
    if df.empty:
        return {}, {}

    lp_symbol_to_tts: Dict[str, str] = {}
    tts_to_lp_value: Dict[str, str] = {}

    for _, r in df.iterrows():
        lp_symbol = str(r[lp_symbol_col]).strip()
        tts = str(r["tts"]).strip()

        if lp_symbol and tts and lp_symbol not in lp_symbol_to_tts:
            lp_symbol_to_tts[lp_symbol] = tts

        if tts and tts not in tts_to_lp_value:
            if provider_col is not None:
                lp_value = str(r[provider_col]).strip()
                tts_to_lp_value[tts] = lp_value if lp_value else lp_symbol
            else:
                tts_to_lp_value[tts] = lp_symbol

    return lp_symbol_to_tts, tts_to_lp_value

# Чтение входных CSV
def _read_tts_current(tts_swaps_current_csv: Path) -> pd.DataFrame:
    """
    Загружаем текущие свопы из TTS.
    Это наши OLD значения, с которыми будем сравнивать новые.
    """
    if not tts_swaps_current_csv.exists():
        raise FileNotFoundError(f"tts_swaps_current.csv not found: {tts_swaps_current_csv}")

    df = pd.read_csv(tts_swaps_current_csv)
    df = _norm_cols(df)

    required = {"tts", "swap_long", "swap_short"}
    missing = required - set(df.columns)
    if missing:
        raise KeyError(f"{tts_swaps_current_csv.name} missing columns: {missing}")

    df["tts"] = df["tts"].astype(str).str.strip()
    df["swap_long"] = pd.to_numeric(df["swap_long"], errors="coerce")
    df["swap_short"] = pd.to_numeric(df["swap_short"], errors="coerce")

    out = pd.DataFrame()
    out["symbol"] = df["tts"]
    out["oldswap_long"] = df["swap_long"]
    out["oldswap_short"] = df["swap_short"]
    out = out[out["symbol"] != ""]

    return out


def _read_swap_csv(swap_csv: Path) -> pd.DataFrame:
    """
    Загружаем swap.csv с новыми рассчитанными свопами от LP.
    """
    if not swap_csv.exists():
        raise FileNotFoundError(f"swap.csv not found: {swap_csv}")

    df = pd.read_csv(swap_csv)
    df_lower = _norm_cols(df)

    col_symbol = None
    for cand in ["symbol", "symbols"]:
        if cand in df_lower.columns:
            col_symbol = cand
            break

    if col_symbol is None:
        raise KeyError(f"{swap_csv.name} must contain 'Symbol' column")

    if "newswaplong" not in df_lower.columns or "newswapshort" not in df_lower.columns:
        raise KeyError(f"{swap_csv.name} must contain 'NewSwapLong' and 'NewSwapShort' columns")

    out = pd.DataFrame()
    out["lp_symbol"] = df_lower[col_symbol].astype(str).str.strip()

    out["rawswap_long"] = (
        pd.to_numeric(df_lower["rawswaplong"], errors="coerce")
        if "rawswaplong" in df_lower.columns else pd.NA
    )
    out["rawswap_short"] = (
        pd.to_numeric(df_lower["rawswapshort"], errors="coerce")
        if "rawswapshort" in df_lower.columns else pd.NA
    )

    out["newswap_long"] = pd.to_numeric(df_lower["newswaplong"], errors="coerce")
    out["newswap_short"] = pd.to_numeric(df_lower["newswapshort"], errors="coerce")

    out = out[out["lp_symbol"] != ""]
    return out

# Формирование difference.csv
def generate_difference(
    config: Dict[str, Any],
    mapping_csv: Path,
    tts_swaps_current_csv: Path,
    swap_csv: Path,
) -> pd.DataFrame:
    """
    Формируем основной отчёт difference.csv.

    Логика:
      difference_long  = oldswap_long  - newswap_long
      difference_short = oldswap_short - newswap_short

    Также добавляем колонку lp в начало таблицы.
    """
    lp_symbol_to_tts, tts_to_lp_value = load_mapping(mapping_csv)

    df_old = _read_tts_current(tts_swaps_current_csv)
    df_new = _read_swap_csv(swap_csv)

    # Привязываем LP symbol к TTS symbol
    df_new["symbol"] = df_new["lp_symbol"].map(lp_symbol_to_tts)
    df_new = df_new.dropna(subset=["symbol"])
    df_new["symbol"] = df_new["symbol"].astype(str).str.strip()

    df_new = df_new[
        ["symbol", "rawswap_long", "rawswap_short", "newswap_long", "newswap_short"]
    ]

    merged = df_old.merge(df_new, on="symbol", how="left")

    # Добавляем колонку lp самой первой
    merged.insert(0, "lp", merged["symbol"].map(tts_to_lp_value))

    # Округляем все входные значения
    for col in [
        "oldswap_long", "oldswap_short",
        "rawswap_long", "rawswap_short",
        "newswap_long", "newswap_short",
    ]:
        merged[col] = pd.to_numeric(merged[col], errors="coerce").round(DIFF_DECIMALS)

    # OLD - NEW
    merged["difference_long"] = (merged["oldswap_long"] - merged["newswap_long"]).round(DIFF_DECIMALS)
    merged["difference_short"] = (merged["oldswap_short"] - merged["newswap_short"]).round(DIFF_DECIMALS)

    return merged[
        [
            "lp",
            "symbol",
            "oldswap_long",
            "oldswap_short",
            "rawswap_long",
            "rawswap_short",
            "newswap_long",
            "newswap_short",
            "difference_long",
            "difference_short",
        ]
    ]

# Threshold-логика (±20% от OLD swap)
def build_diff_thresholds(
    config: Dict[str, Any],
    *,
    last_n_days: int = 3,
    threshold_pct: float = 10.0,
    decimals: int = 6,
) -> pd.DataFrame:
    """
    История больше не используется.
    Пороги считаются напрямую от OLD swap.

    Функция оставлена для совместимости с main.py.
    """
    return pd.DataFrame(columns=["symbol", "diff_long_thrsh", "diff_short_thrsh"])


def _compute_delta_threshold_from_old(old_value: float, sym: str) -> float:
    """
    Считаем дельта-порог:
      abs(old) * 20%

    Округление:
      индексы  -> 6 знаков
      остальное -> 4 знака
    """
    if pd.isna(old_value):
        return float("nan")

    digits = _thr_digits_for_symbol(sym)
    delta = abs(float(old_value)) * (THRESHOLD_PCT / 100.0)
    return round(delta, digits)


def apply_threshold_flags(
    difference_df: pd.DataFrame,
    thresholds_df: pd.DataFrame,
    *,
    decimals: int = DIFF_DECIMALS,
    use_abs: bool = True,
) -> pd.DataFrame:
    """
    Добавляем threshold-колонки и флаги value_long / value_short.

    Логика:
      low  = old * 0.8
      high = old * 1.2

      если new < low или new > high → value = 1
      иначе → value = 0

    Для отрицательных OLD диапазон корректно нормализуется через min/max.
    """
    df = _norm_cols(difference_df).copy()

    for col in ["oldswap_long", "oldswap_short", "newswap_long", "newswap_short"]:
        df[col] = pd.to_numeric(df.get(col), errors="coerce")

    df["diff_long_thrsh"] = [
        _compute_delta_threshold_from_old(v, s)
        for v, s in zip(df["oldswap_long"], df["symbol"])
    ]
    df["diff_short_thrsh"] = [
        _compute_delta_threshold_from_old(v, s)
        for v, s in zip(df["oldswap_short"], df["symbol"])
    ]

    # Минимальный threshold = ±1 чтобы избежать ложных нотификаций около нуля
    df["diff_long_thrsh"] = df.apply(
        lambda r: r["diff_long_thrsh"]
        if _is_index_symbol(r["symbol"])
        else (1.0 if pd.notna(r["diff_long_thrsh"]) and r["diff_long_thrsh"] < 1 else r["diff_long_thrsh"]),
        axis=1
    )

    df["diff_short_thrsh"] = df.apply(
        lambda r: r["diff_short_thrsh"]
        if _is_index_symbol(r["symbol"])
        else (1.0 if pd.notna(r["diff_short_thrsh"]) and r["diff_short_thrsh"] < 1 else r["diff_short_thrsh"]),
        axis=1
    )

    df["difference_long"] = df["difference_long"].round(decimals)
    df["difference_short"] = df["difference_short"].round(decimals)

    def _value_flag(old_v: float, new_v: float) -> int:
        if pd.isna(old_v) or pd.isna(new_v):
            return 0

        low = old_v * 0.8
        high = old_v * 1.2
        lo, hi = min(low, high), max(low, high)

        return 1 if (new_v < lo or new_v > hi) else 0

    df["value_long"] = [
        _value_flag(o, n) for o, n in zip(df["oldswap_long"], df["newswap_long"])
    ]
    df["value_short"] = [
        _value_flag(o, n) for o, n in zip(df["oldswap_short"], df["newswap_short"])
    ]

    return df[
        [
            "lp",
            "symbol",
            "oldswap_long",
            "oldswap_short",
            "rawswap_long",
            "rawswap_short",
            "newswap_long",
            "newswap_short",
            "difference_long",
            "difference_short",
            "diff_long_thrsh",
            "diff_short_thrsh",
            "value_long",
            "value_short",
        ]
    ]

# Сохранение CSV
def save_difference(
    config: Dict[str, Any],
    difference_df: pd.DataFrame,
    filename: str = "difference.csv",
) -> Dict[str, Path]:
    """
    Сохраняем difference.csv:
      - в папку текущего дня
      - в storage (для истории)
    """
    paths = resolve_output_paths(config)

    today_folder = _today_folder(paths.today_dir)
    out_today = today_folder / filename
    difference_df.to_csv(out_today, index=False)

    storage_day = paths.storage_dir / datetime.now().strftime("%d-%m-%Y")
    _ensure_dir(storage_day)
    out_storage = storage_day / filename
    difference_df.to_csv(out_storage, index=False)

    return {"today": out_today, "storage": out_storage}


def save_diff_thresholds(
    config: Dict[str, Any],
    thresholds_df: pd.DataFrame,
    *,
    filename: str = "diff_threshold.csv",
) -> Dict[str, Path]:
    """
    Оставлено для совместимости.
    Может быть пустым — это нормально.
    """
    paths = resolve_output_paths(config)

    today_folder = _today_folder(paths.today_dir)
    out_today = today_folder / filename
    thresholds_df.to_csv(out_today, index=False)

    storage_day = paths.storage_dir / datetime.now().strftime("%d-%m-%Y")
    _ensure_dir(storage_day)
    out_storage = storage_day / filename
    thresholds_df.to_csv(out_storage, index=False)

    return {"today": out_today, "storage": out_storage}
