# fx_correlation_mataf_service.py

from __future__ import annotations

import csv
import io
import logging
import os
from datetime import datetime, timezone
from typing import List, Tuple, Set

import psycopg2
from psycopg2.extras import execute_values
import requests
from dotenv import load_dotenv

from telegram_notifier import notify_telegram, ChatType, start_telegram_notifier, close_telegram_notifier, ChatType
import public_module



load_dotenv()
logger = logging.getLogger(__name__)


# ---------------------------------------------------------
# Universe of symbols we request from Mataf
# NOTE: Mataf expects them WITHOUT slash: "EURUSD", "GBPUSD", ...
# ---------------------------------------------------------
MATAF_SYMBOLS: List[str] = [
    # majors
    "EURUSD", "GBPUSD", "NZDUSD", "NZDCAD", "GBPCAD", "AUDUSD", "GBPAUD",
    "GBPCHF", "EURCAD", "GBPJPY", "NZDCHF", "NZDJPY", "AUDCAD", "EURAUD",
    "CHFJPY", "GBPNZD", "EURJPY", "AUDJPY", "AUDCHF", "EURCHF", "CADJPY",
    "CADCHF", "USDJPY", "EURNZD", "AUDNZD", "EURGBP", "USDCAD", "USDCHF",
    # you can keep extending this list with more crosses if you want
]


# ---------------------------------------------------------
# DB connection helper
# ---------------------------------------------------------
def _get_db_connection():
    """
    Create a PostgreSQL connection using either DATABASE_URL
    or individual POSTGRES_* env vars.
    """
    dsn = os.getenv("DATABASE_URL")
    if dsn:
        conn = psycopg2.connect(dsn)
        conn.autocommit = True
        return conn

    conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        dbname=os.getenv("POSTGRES_DB"),
    )
    conn.autocommit = True
    return conn

def _build_corr_matrix_message(
    conn,
    as_of_time: datetime,
    timeframe: str,
    num_period: int,
) -> str:
    """
    Build an ASCII correlation matrix for preferred symbols in public_module.symbols
    and return it as a text block ready to send via Telegram.
    """
    preferred_symbols = getattr(public_module, "symbols", [])
    if not preferred_symbols:
        return (
            "🔃 QuantFlow_FX_Correlation\n"
            f"as_of_time: {as_of_time}\n"
            f"Timeframe: {timeframe}, periods: {num_period}\n"
            "No preferred symbols configured in public_module.symbols."
        )

    # Sort for stable row/column order
    symbols = sorted(preferred_symbols)

    # Fetch correlation rows for this snapshot / timeframe / period
    placeholders = ", ".join(["%s"] * len(symbols))
    sql = f"""
        SELECT base_symbol, ref_symbol, corr_value
        FROM fx_correlation_mataf
        WHERE as_of_time = %s
          AND timeframe = %s
          AND num_period = %s
          AND base_symbol IN ({placeholders})
          AND ref_symbol IN ({placeholders})
    """

    params = [as_of_time, timeframe, num_period] + symbols + symbols

    with conn.cursor() as cur:
        cur.execute(sql, params)
        rows = cur.fetchall()

    # Build symmetric map: (A,B) -> corr_value, with sorted key
    corr_map: dict[tuple[str, str], float] = {}
    for base, ref, val in rows:
        key = tuple(sorted((base, ref)))
        corr_map[key] = float(val)

    # Compute column widths
    sym_width = max(len(s) for s in symbols)
    cell_width = 6  # width for each correlation cell

    lines: list[str] = []

    # Header row
    header = " " * (sym_width + 1)
    for sym in symbols:
        header += "  " + sym.rjust(cell_width + 1)
    lines.append(header)

    # Matrix rows
    for row_sym in symbols:
        line = row_sym.ljust(sym_width)
        for col_sym in symbols:
            if row_sym == col_sym:
                cell = " 100 "  # diagonal
            else:
                key = tuple(sorted((row_sym, col_sym)))
                val = corr_map.get(key)
                if val is None:
                    cell = "   ."
                else:
                    # Show as integer percent with sign, e.g. +75, -23
                    cell = f"{int(round(val)):>4d}"
            line += " " + cell
        lines.append(line)

    matrix_block = "\n".join(lines)

    msg = (
        "🔃 QuantFlow_FX_Correlation\n"
        f"as_of_time: {as_of_time}\n"
        f"Timeframe: {timeframe}, periods: {num_period}\n\n"
        "Corr matrix (%):\n"
        f"{matrix_block}"
    )
    return msg


def _ensure_table_exists(conn) -> None:
    ddl = """
    CREATE TABLE IF NOT EXISTS public.fx_correlation_mataf
    (
        id           bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        base_symbol  text   NOT NULL,
        ref_symbol   text   NOT NULL,
        timeframe    text   NOT NULL,
        num_period   integer NOT NULL,
        corr_value   numeric(7,2) NOT NULL,
        as_of_time   timestamp(0) without time zone NOT NULL,
        source       text   NOT NULL DEFAULT 'MATAF',
        record_time  timestamp(0) without time zone NOT NULL DEFAULT now(),
        UNIQUE (base_symbol, ref_symbol, timeframe, num_period, as_of_time)
    );
    """
    with conn.cursor() as cur:
        cur.execute(ddl)


# ---------------------------------------------------------
# Helpers
# ---------------------------------------------------------
def _normalize_symbol(sym: str) -> str:
    """
    Mataf uses 'EURUSD'; DB uses 'EUR/USD'.
    """
    s = sym.strip().upper().replace(" ", "").replace("/", "")
    if len(s) == 6:
        return f"{s[0:3]}/{s[3:6]}"
    return s


def _detect_delimiter(sample: str) -> str:
    sniffer = csv.Sniffer()
    try:
        dialect = sniffer.sniff(sample, delimiters=",;")
        return dialect.delimiter
    except Exception:
        return ","


def _map_timeframe(raw: str) -> str | None:
    """
    Map Mataf column names to DB timeframe labels.
    """
    r = raw.strip().lower()
    if r in ("5min", "5 mn", "5 m"):
        return "5m"
    if r in ("15min", "15 mn", "15 m"):
        return "15m"
    if r in ("1h", "1 h"):
        return "1H"
    if r in ("4h", "4 h"):
        return "4H"
    if r in ("day", "daily"):
        return "1D"
    if r in ("week", "weekly"):
        return "1W"
    return None


def _parse_as_of_time(row: List[str]) -> datetime:
    """
    Parse the as-of timestamp from the row just before the 'pair1' header.

    Example row (cells):
        ["Wed", "10 Dec 2025 19:11:33 +0000", ...]
    We want to read "10 Dec 2025 19:11:33 +0000" and store it as UTC naive.
    """
    # First: try to parse each non-empty cell individually
    for cell in row:
        txt = cell.strip()
        if not txt:
            continue

        for fmt in (
            "%d %b %Y %H:%M:%S %z",      # "10 Dec 2025 19:11:33 +0000"
            "%a %d %b %Y %H:%M:%S %z",   # "Wed 10 Dec 2025 19:11:33 +0000"
        ):
            try:
                dt = datetime.strptime(txt, fmt)
                return dt.astimezone(timezone.utc).replace(tzinfo=None, microsecond=0)
            except Exception:
                continue

    # Fallback: join all non-empty cells and try again
    candidates = [c.strip() for c in row if c.strip()]
    if candidates:
        txt_all = " ".join(candidates)
        for fmt in (
            "%a %d %b %Y %H:%M:%S %z",
            "%d %b %Y %H:%M:%S %z",
        ):
            try:
                dt = datetime.strptime(txt_all, fmt)
                return dt.astimezone(timezone.utc).replace(tzinfo=None, microsecond=0)
            except Exception:
                continue

    # Final fallback: use current UTC time
    return datetime.utcnow().replace(microsecond=0)



def _build_mataf_url_and_params(
    symbols: List[str],
    num_period: int,
) -> tuple[str, dict]:
    """
    Build the Mataf CSV URL + query params.

    Pattern:
      https://www.mataf.io/api/tools/csv/correl/snapshot/forex/{n}/correlation.csv
        ?symbol=EURUSD|GBPUSD|...

    where n = len(symbols).
    """
    n = len(symbols)
    base_url = (
        "https://www.mataf.io/api/tools/csv/correl/"
        "snapshot/forex/{n}/correlation.csv"
    )
    url = base_url.format(n=n)

    symbol_param = "|".join(symbols)
    params = {"symbol": symbol_param}

    return url, params


# ---------------------------------------------------------
# Main public function
# ---------------------------------------------------------
def update_mataf_correlation_from_csv(
    *,
    num_period: int = 50,
    symbols: List[str] | None = None,
) -> None:
    """
    Download Mataf correlation CSV for a whole universe of symbols
    and store it into fx_correlation_mataf.

    Rules:
      - `symbols`: list of pairs without slash (e.g. "EURUSD").
        If None, MATAF_SYMBOLS will be used.
      - Ignore rows where base_symbol == ref_symbol.
      - Treat (A,B) and (B,A) as the same unordered pair.
      - Batch insert using execute_values for performance.
    """
    if symbols is None:
        symbols = MATAF_SYMBOLS

    url, params = _build_mataf_url_and_params(symbols, num_period)

    logger.info(f"[mataf] Downloading CSV from {url} (n={len(symbols)})")
    resp = requests.get(url, params=params, timeout=20)
    resp.raise_for_status()

    text = resp.text
    lines = text.splitlines()
    if len(lines) < 5:
        logger.error("[mataf] CSV too short; unexpected format.")
        return

    sample = "\n".join(lines[:5])
    delimiter = _detect_delimiter(sample)

    reader = csv.reader(io.StringIO(text), delimiter=delimiter)
    rows: List[List[str]] = list(reader)

    if len(rows) < 5:
        logger.error("[mataf] CSV too short after parsing.")
        return

    # Locate header row: the one whose first cell is 'pair1'
    header_row = None
    header_index = None
    for idx, r in enumerate(rows):
        if r and r[0].strip().lower() == "pair1":
            header_row = r
            header_index = idx
            break

    if header_row is None or header_index is None:
        logger.error("[mataf] Could not find 'pair1' header row.")
        return

    # As-of time from row before header if possible
    if header_index >= 1:
        as_of_row = rows[header_index - 1]
        as_of_time = _parse_as_of_time(as_of_row)
    else:
        as_of_time = datetime.utcnow().replace(microsecond=0)

    # Determine timeframe columns
    tf_columns: List[Tuple[int, str]] = []
    for idx, name in enumerate(header_row):
        if idx < 2:
            continue  # pair1, pair2
        tf = _map_timeframe(name)
        if tf is not None:
            tf_columns.append((idx, tf))

    if not tf_columns:
        logger.error("[mataf] No timeframe columns detected in header.")
        return

    logger.info(
        f"[mataf] Detected timeframe columns: {tf_columns}, "
        f"as_of_time={as_of_time}, num_period={num_period}"
    )

    with _get_db_connection() as conn:
        _ensure_table_exists(conn)

        with conn.cursor() as cur:
            # Collect all rows and then batch-insert them
            records_to_upsert: List[Tuple[str, str, str, int, float, datetime]] = []

            # To deduplicate symmetric pairs A/B vs B/A
            seen_pairs: Set[Tuple[str, str]] = set()

            # Iterate over data rows (after header)
            for row in rows[header_index + 1 :]:
                if len(row) < 2:
                    continue

                raw_p1 = row[0].strip()
                raw_p2 = row[1].strip()
                if not raw_p1 or not raw_p2:
                    continue

                base_symbol = _normalize_symbol(raw_p1)
                ref_symbol = _normalize_symbol(raw_p2)

                # Canonical ordering to avoid duplicates like A/B vs B/A
                bs, rs = sorted([base_symbol, ref_symbol])

                # Ignore same-symbol pairs (EUR/EUR, USD/USD, etc.)
                if bs == rs:
                    continue

                # Deduplicate symmetric pairs: if (A,B) already seen, skip (B,A)
                pair_key = (bs, rs)
                if pair_key in seen_pairs:
                    continue
                seen_pairs.add(pair_key)

                # Collect all timeframe values for this pair
                for col_idx, tf_label in tf_columns:
                    if col_idx >= len(row):
                        continue
                    cell = row[col_idx].strip()
                    if cell == "":
                        continue

                    try:
                        value_raw = float(cell)
                    except ValueError:
                        continue

                    corr_value = round(value_raw, 2)  # -100..100

                    records_to_upsert.append(
                        (bs, rs, tf_label, num_period, corr_value, as_of_time)
                    )

            print(as_of_time)
            # Batch insert with execute_values
            if records_to_upsert:
                sql = """
                    INSERT INTO fx_correlation_mataf
                      (base_symbol, ref_symbol, timeframe, num_period, corr_value, as_of_time, source)
                    VALUES %s

                """


                '''
                    INSERT INTO fx_correlation_mataf
                      (base_symbol, ref_symbol, timeframe, num_period, corr_value, as_of_time, source)
                    VALUES %s
                    ON CONFLICT (base_symbol, ref_symbol, timeframe, num_period, as_of_time)
                    DO UPDATE SET
                        corr_value  = EXCLUDED.corr_value,
                        record_time = now()
                '''
                execute_values(
                    cur,
                    sql,
                    records_to_upsert,
                    template="(%s, %s, %s, %s, %s, %s, 'MATAF')",
                )

        logger.info(
            f"[mataf] Imported Mataf correlations: num_period={num_period}, "
            f"as_of_time={as_of_time}, "
            f"rows={len(records_to_upsert)}, "
            f"unique_pairs={len(seen_pairs)}"
        )

        tf_for_alert = "5m"

        try:
            msg = _build_corr_matrix_message(
                conn=conn,
                as_of_time=as_of_time,
                timeframe=tf_for_alert,
                num_period=num_period,
            )
        except Exception as e:
            msg = (
                "🔃 QuantFlow_FX_Correlation\n"
                f"as_of_time: {as_of_time}\n"
                f"Timeframe: {tf_for_alert}, periods: {num_period}\n"
                f"Error building matrix: {e}"
            )

        notify_telegram(msg, ChatType.ALERT)


        tf_for_alert = "15m"

        try:
            msg = _build_corr_matrix_message(
                conn=conn,
                as_of_time=as_of_time,
                timeframe=tf_for_alert,
                num_period=num_period,
            )
        except Exception as e:
            msg = (
                "🔃 QuantFlow_FX_Correlation\n"
                f"as_of_time: {as_of_time}\n"
                f"Timeframe: {tf_for_alert}, periods: {num_period}\n"
                f"Error building matrix: {e}"
            )

        notify_telegram(msg, ChatType.ALERT)

# Manual test
if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    update_mataf_correlation_from_csv(num_period=50)
