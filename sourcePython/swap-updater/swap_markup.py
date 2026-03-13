from pathlib import Path
from typing import Dict, Any, List, Optional

import logging
from decimal import Decimal, ROUND_FLOOR, ROUND_CEILING, InvalidOperation

import pandas as pd
import requests
import yaml


logger = logging.getLogger(__name__)


def load_config(path: Path) -> Dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {path}")
    with path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f)


# Конвертируем float в Decimal через строку, чтобы избежать float-артефактов
def _to_decimal(x: float) -> Decimal:
    return Decimal(str(x))


# Очищаем float от бинарных артефактов и приводим к фиксированному числу знаков
# (например, 0.819999999 -> 0.82) ДО брокерского округления
def clean_float(value: float, digits: int) -> float:
    if pd.isna(value):
        return value
    try:
        d = _to_decimal(float(value))
        q = Decimal("1").scaleb(-digits)
        d = d.quantize(q)
        return float(d)
    except (InvalidOperation, ValueError, TypeError):
        return float(value)


# Выполняем округление в нашу сторону
# положительные значения — ROUND_FLOOR (0.8199999 -> 0.81)
# отрицательные — ROUND_CEILING (-0.8199999 -> -0.82)
def round_broker_side(value: float, digits: int = 6) -> float:
    if pd.isna(value):
        return value

    value = clean_float(float(value), digits + 2)

    d = _to_decimal(value)
    q = Decimal("1").scaleb(-digits)

    if d >= 0:
        out = d.quantize(q, rounding=ROUND_FLOOR)
    else:
        out = d.quantize(q, rounding=ROUND_CEILING)

    return float(out)


# Считаем вариант значения свопа на основе процентного markup
def _percent_candidate(raw_value: float, markup_percent: float) -> float:
    if pd.isna(raw_value) or pd.isna(markup_percent):
        return raw_value
    if raw_value == 0:
        return 0.0

    factor = 1.0 + (markup_percent / 100.0) * (1 if raw_value > 0 else -1)
    return raw_value * factor


# Применяем markup по принципу "лучше для нас"
# выбираем минимум между процентным markup и минимальным markup в пунктах
def apply_markup_best_for_us(raw_value: float, markup_percent: float, min_markup: Optional[float]) -> float:
    if pd.isna(raw_value):
        return raw_value

    if pd.isna(markup_percent):
        cand_percent = float(raw_value)
    else:
        cand_percent = _percent_candidate(float(raw_value), float(markup_percent))

    candidates = [cand_percent]

    if min_markup is not None and not pd.isna(min_markup):
        cand_min = float(raw_value) + float(min_markup)
        candidates.append(cand_min)

    best = min(candidates)
    return float(best)


# Загружаем свопы от LP через HTTP API и приводим ответ к DataFrame
def fetch_swaps(base_url: str, endpoint: str, data_source: str, verify_ssl: bool) -> pd.DataFrame:
    url = base_url.rstrip("/") + "/" + endpoint.lstrip("/")
    resp = requests.get(
        url,
        params={"dataSource": data_source},
        timeout=30,
        verify=verify_ssl,
    )
    resp.raise_for_status()

    data = resp.json()
    df = pd.DataFrame(data)
    df.columns = [c.lower() for c in df.columns]

    required = {"symbol", "swaplong", "swapshort"}
    missing = required - set(df.columns)
    if missing:
        raise KeyError(f"Swaps API response is missing columns: {missing}")

    df["symbol"] = df["symbol"].astype(str).str.strip()
    return df


# Загружаем CSV-конфиг свопов (symbols, markup, group, min_markup, mt4/mt5/tts)
# Поддерживаем разные варианты названий колонок
def load_swaps_config(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path, sep=";")

    if df.empty:
        raise ValueError(f"Swaps config is empty: {path}")

    df.columns = [str(c).strip() for c in df.columns]
    lower_map = {c.lower().strip(): c for c in df.columns}

    sym_col = lower_map.get("symbols") or lower_map.get("symbol") or lower_map.get("lp")
    markup_col = lower_map.get("markup")
    group_col = lower_map.get("group") or lower_map.get("security")
    source_col = lower_map.get("source")

    minmarkup_col = (
        lower_map.get("min markup pips")
        or lower_map.get("min markup")
        or lower_map.get("minmarkup")
        or lower_map.get("min_markup")
        or lower_map.get("min mark")
    )

    if sym_col is None or markup_col is None:
        raise KeyError(f"{path.name} must contain columns 'symbols'/'symbol'/'lp' and 'markup' (case-insensitive).")

    result = pd.DataFrame()
    result["symbol"] = df[sym_col].astype(str).str.strip()
    result["markup"] = pd.to_numeric(df[markup_col], errors="coerce")

    if group_col:
        result["group"] = df[group_col].astype(str).str.strip()
    else:
        result["group"] = pd.NA

    if source_col:
        result["source"] = df[source_col].astype(str).str.strip().str.lower()
    else:
        result["source"] = pd.NA

    if minmarkup_col:
        raw_min = df[minmarkup_col].astype(str).str.strip().replace({"": pd.NA})
        raw_min = raw_min.str.replace(",", ".", regex=False)
        result["min_markup"] = pd.to_numeric(raw_min, errors="coerce")
    else:
        result["min_markup"] = pd.NA

    mt4_col = lower_map.get("mt4")
    mt5_col = lower_map.get("mt5")
    tts_col = lower_map.get("tts")

    if mt4_col:
        result["mt4"] = df[mt4_col].astype(str).str.strip()
    if mt5_col:
        result["mt5"] = df[mt5_col].astype(str).str.strip()
    if tts_col:
        result["tts"] = df[tts_col].astype(str).str.strip()

    result = result[result["symbol"] != ""]
    if result.empty:
        raise ValueError(f"No non-empty symbols found in {path}")

    return result


# Загружаем текущие TTS swaps из CSV, чтобы использовать их как fallback
def load_tts_swaps_current(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"TTS swaps current CSV not found: {path}")

    df = pd.read_csv(path)

    if df.empty:
        raise ValueError(f"TTS swaps current CSV is empty: {path}")

    df.columns = [str(c).strip().lower() for c in df.columns]

    required = {"tts", "swap_long", "swap_short"}
    missing = required - set(df.columns)
    if missing:
        raise KeyError(f"{path.name} must contain columns {sorted(required)}. Missing: {sorted(missing)}")

    result = pd.DataFrame()
    result["tts"] = df["tts"].astype(str).str.strip()
    result["swap_long"] = pd.to_numeric(df["swap_long"], errors="coerce")
    result["swap_short"] = pd.to_numeric(df["swap_short"], errors="coerce")

    result = result[result["tts"] != ""]
    result = result.dropna(subset=["swap_long", "swap_short"])

    if result.empty:
        raise ValueError(f"No valid rows found in {path}")

    return result


# Формируем fallback-строку из текущего TTS swap
def _build_fallback_row(
    row: pd.Series,
    tts_swaps_df: pd.DataFrame,
    provider_key: str,
    data_source: str,
    out_digits: int,
) -> Optional[Dict[str, Any]]:
    internal_symbol = str(row["symbol"]).strip()
    tts_symbol = str(row.get("tts", "")).strip()
    lp_symbol = str(row.get("lp_symbol", "")).strip() if pd.notna(row.get("lp_symbol", pd.NA)) else "UNKNOWN"
    group_val = str(row.get("group", "")).strip()

    if not tts_symbol:
        logger.error(
            "FALLBACK FAILED [%s]: internal_symbol=%s | lp_symbol=%s | source=%s | reason=empty_tts_symbol_in_mapping",
            provider_key,
            internal_symbol,
            lp_symbol,
            data_source,
        )
        return None

    tts_match = tts_swaps_df[tts_swaps_df["tts"] == tts_symbol]
    if tts_match.empty:
        logger.error(
            "FALLBACK FAILED [%s]: internal_symbol=%s | tts_symbol=%s | lp_symbol=%s | source=%s | reason=tts_symbol_not_found_in_tts_swaps_current",
            provider_key,
            internal_symbol,
            tts_symbol,
            lp_symbol,
            data_source,
        )
        return None

    tts_row = tts_match.iloc[0]

    raw_long = float(tts_row["swap_long"])
    raw_short = float(tts_row["swap_short"])

    raw_long = round_broker_side(raw_long, out_digits)
    raw_short = round_broker_side(raw_short, out_digits)

    logger.warning(
        "LP SWAP NOT RECEIVED [%s]: internal_symbol=%s | tts_symbol=%s | lp_symbol=%s | source=%s | fallback=use_old_tts_swap | oldLong=%s | oldShort=%s",
        provider_key,
        internal_symbol,
        tts_symbol,
        lp_symbol,
        data_source,
        raw_long,
        raw_short,
    )

    return {
        "Symbol": internal_symbol,
        "RawSwapLong": raw_long,
        "NewSwapLong": raw_long,
        "RawSwapShort": raw_short,
        "NewSwapShort": raw_short,
        "Source": data_source,
        "Group": group_val,
    }


# Обрабатываем одного LP-провайдера
def _process_provider(
    cfg: Dict[str, Any],
    symbols_df: pd.DataFrame,
    provider_key: str,
    tts_swaps_df: pd.DataFrame,
) -> pd.DataFrame:
    base_url = cfg.get("base_url")
    endpoint = cfg.get("swaps_endpoint", "/api/swaps/latest")
    data_source = cfg.get("data_source", provider_key)
    verify_ssl = bool(cfg.get("verify_ssl", False))

    # Группы, которые не маркапим
    no_markup_groups = set(cfg.get("no_markup_groups", ["CFD Index 1"]))

    # Группы, где свопы в процентах (надо /100)
    percent_groups = set(cfg.get("percent_groups", ["CFD Index 1"]))

    # Максимальное количество дигитов
    out_digits = int(cfg.get("out_digits", 6))

    if not base_url:
        raise ValueError(f"Provider '{provider_key}': base_url is missing in config")

    df_sym = symbols_df.copy()
    df_sym["lp_symbol"] = df_sym["symbol"].astype(str).str.strip()
    df_sym = df_sym[df_sym["lp_symbol"] != ""]

    if df_sym.empty:
        logger.warning("No symbols to process for provider %s", provider_key)
        return pd.DataFrame(
            columns=[
                "Symbol",
                "RawSwapLong",
                "NewSwapLong",
                "RawSwapShort",
                "NewSwapShort",
                "Source",
                "Group",
            ]
        )

    swaps_df = fetch_swaps(base_url, endpoint, data_source, verify_ssl)
    logger.info(
        "Loaded %d swap records from API %s (dataSource=%s)",
        len(swaps_df),
        base_url,
        data_source,
    )

    if swaps_df.empty:
        logger.error(
            "LP SWAPS RESPONSE EMPTY [%s]: API returned 0 records | base_url=%s | dataSource=%s",
            provider_key,
            base_url,
            data_source,
        )
        logger.error(
            "Skipping swap calculation for provider %s. Fallback to old TTS swaps will be used.",
            provider_key,
        )

        fallback_results: List[Dict[str, Any]] = []
        for _, row in df_sym.iterrows():
            fallback_row = _build_fallback_row(
                row=row,
                tts_swaps_df=tts_swaps_df,
                provider_key=provider_key,
                data_source=data_source,
                out_digits=out_digits,
            )
            if fallback_row is not None:
                fallback_results.append(fallback_row)

        return pd.DataFrame(fallback_results)

    swaps_df = swaps_df.rename(columns={"symbol": "lp_symbol"})
    merged = df_sym.merge(swaps_df, on="lp_symbol", how="left")

    missing_mask = merged["swaplong"].isna() | merged["swapshort"].isna()
    fallback_results: List[Dict[str, Any]] = []

    if missing_mask.any():
        missing_rows = merged.loc[missing_mask].copy()

        for _, miss_row in missing_rows.iterrows():
            fallback_row = _build_fallback_row(
                row=miss_row,
                tts_swaps_df=tts_swaps_df,
                provider_key=provider_key,
                data_source=data_source,
                out_digits=out_digits,
            )
            if fallback_row is not None:
                fallback_results.append(fallback_row)

    merged = merged.dropna(subset=["swaplong", "swapshort"])

    results: List[Dict[str, Any]] = []
    for _, row in merged.iterrows():
        internal_symbol = str(row["symbol"]).strip()
        group_val = str(row.get("group", "")).strip()

        raw_long = float(row["swaplong"])
        raw_short = float(row["swapshort"])

        skip_markup = group_val in no_markup_groups

        if skip_markup:
            new_long_raw = raw_long
            new_short_raw = raw_short
            markup_percent = pd.NA
            min_markup = pd.NA
        else:
            markup_percent = row.get("markup", pd.NA)
            min_markup = row.get("min_markup", pd.NA)
            new_long_raw = apply_markup_best_for_us(raw_long, markup_percent, min_markup)
            new_short_raw = apply_markup_best_for_us(raw_short, markup_percent, min_markup)

        # Округление (убираем 0.819999999 и т.п.)
        new_long = round_broker_side(new_long_raw, out_digits)
        new_short = round_broker_side(new_short_raw, out_digits)

        # для CFD Index 1 (и любых percent_groups) приводим к долям
        # Чтобы дальше в uploader/manager service не было спец-логики.
        if group_val in percent_groups:
            raw_long = raw_long / 100.0
            raw_short = raw_short / 100.0
            new_long = new_long / 100.0
            new_short = new_short / 100.0

            # и ещё раз брокер-округление после деления (чтобы не было артефактов)
            raw_long = round_broker_side(raw_long, out_digits)
            raw_short = round_broker_side(raw_short, out_digits)
            new_long = round_broker_side(new_long, out_digits)
            new_short = round_broker_side(new_short, out_digits)

        logger.info(
            "SUCCESS [%s]: %s | group=%s | RawLong=% .6f -> NewLong=% .6f | "
            "RawShort=% .6f -> NewShort=% .6f | markup=%s min_markup=%s",
            provider_key,
            internal_symbol,
            group_val,
            raw_long,
            new_long,
            raw_short,
            new_short,
            str(markup_percent),
            str(min_markup),
        )

        results.append(
            {
                "Symbol": internal_symbol,
                "RawSwapLong": raw_long,
                "NewSwapLong": new_long,
                "RawSwapShort": raw_short,
                "NewSwapShort": new_short,
                "Source": data_source,
                "Group": group_val,
            }
        )

    if fallback_results:
        logger.warning(
            "Provider %s used TTS fallback for %d symbols",
            provider_key,
            len(fallback_results),
        )

    final_results = results + fallback_results
    return pd.DataFrame(final_results)


# Главная функция расчёта новых свопов
def generate_swaps(config: Dict[str, Any]) -> pd.DataFrame:
    paths_cfg = config.get("paths", {})
    api_cfg = config.get("api", {})

    all_results: List[pd.DataFrame] = []

    mapping_path = Path(paths_cfg.get("mapping_config", "configDocker/mapping.csv"))
    mapping_df = load_swaps_config(mapping_path)
    logger.info("Loaded %d symbols from %s", len(mapping_df), mapping_path)

    tts_swaps_current_path = Path(paths_cfg.get("today_dir", "dataDocker/today"))
    tts_swaps_current_path = tts_swaps_current_path / pd.Timestamp.now().strftime("%d-%m-%Y") / "tts_swaps_current.csv"
    tts_swaps_df = load_tts_swaps_current(tts_swaps_current_path)
    logger.info("Loaded %d current TTS swaps from %s", len(tts_swaps_df), tts_swaps_current_path)

    if "isprime" in api_cfg:
        isprime_df = mapping_df[mapping_df["source"].astype(str).str.lower() == "isprime"].copy()
        logger.info("Loaded %d isprime symbols from %s", len(isprime_df), mapping_path)
        logger.info("Processing provider: isprime")
        df_isprime = _process_provider(api_cfg["isprime"], isprime_df, "isprime", tts_swaps_df)
        if not df_isprime.empty:
            all_results.append(df_isprime)

    if "velocity" in api_cfg:
        velocity_df = mapping_df[mapping_df["source"].astype(str).str.lower() == "velocity"].copy()
        logger.info("Loaded %d velocity symbols from %s", len(velocity_df), mapping_path)
        logger.info("Processing provider: velocity")
        df_velocity = _process_provider(api_cfg["velocity"], velocity_df, "velocity", tts_swaps_df)
        if not df_velocity.empty:
            all_results.append(df_velocity)

    if not all_results:
        return pd.DataFrame(
            columns=[
                "Symbol",
                "RawSwapLong",
                "NewSwapLong",
                "RawSwapShort",
                "NewSwapShort",
                "Source",
                "Group",
            ]
        )

    return pd.concat(all_results, ignore_index=True)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s.%(msecs)03d - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    pass