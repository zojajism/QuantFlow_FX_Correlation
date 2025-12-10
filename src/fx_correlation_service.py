# fx_correlation_service.py

from __future__ import annotations

import logging
import os
from typing import Dict

import psycopg  # psycopg3
import pandas as pd
from dotenv import load_dotenv

from public_module import config_data, symbols  # shared config + symbol list

load_dotenv()
logger = logging.getLogger(__name__)


def _get_db_connection():
    """
    Create a PostgreSQL connection using either DATABASE_URL
    or individual POSTGRES_* env vars.
    """
    dsn = os.getenv("DATABASE_URL")
    if dsn:
        return psycopg.connect(dsn)

    return psycopg.connect(
        host=os.getenv("POSTGRES_HOST"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        dbname=os.getenv("POSTGRES_DB"),
        autocommit=True,
    )


def update_fx_correlations_for_timeframe(timeframe: str) -> None:
    """
    Main public function.

    Uses shared config from public_module:

      - EXCHANGE: string, e.g. "OANDA"
      - CORRELATION_SYMBOLS: list of symbols (already loaded as `symbols`)
      - CORRELATION_TIMEFRAMES: mapping { "5m": 80, "15m": 64, ... }
      - MIN_ROWS_FOR_CORR (optional): minimum aligned rows for returns

    Steps:
      * Load last N candles for each symbol from `candles` (given timeframe)
      * Build a DataFrame of closes aligned on `close_time`
      * Drop timestamps where any symbol is missing (df_price.dropna())
      * Compute:
          - correlation on price (closes)
          - correlation on returns (pct_change of closes)
          - correlation on tick returns (pct_change of resampled 1s ticks)
      * Insert/Update values in `fx_correlation` table:
          corr_value_by_price, corr_value_by_return, corr_value_by_tick
    """
    cfg = config_data or {}

    exchange: str = str(cfg.get("EXCHANGE", "OANDA"))

    if not symbols or len(symbols) < 2:
        logger.debug(f"[fx_corr] Not enough symbols configured. Skipping. timeframe={timeframe}")
        return

    # CORRELATION_TIMEFRAMES is a dict: { "5m": 80, "15m": 64, ... }
    tf_map: Dict[str, int] = cfg.get("CORRELATION_TIMEFRAMES", {}) or {}
    window_size = int(tf_map.get(timeframe, 100))  # fallback 100 if not defined
    min_rows_for_corr: int = int(cfg.get("MIN_ROWS_FOR_CORR", 30))

    with _get_db_connection() as conn:
        with conn.cursor() as cur:
            # --------------------------------------------------
            # 1) Load candles for each symbol
            # --------------------------------------------------
            series_by_symbol_price: Dict[str, pd.Series] = {}

            for symbol in symbols:
                cur.execute(
                    """
                    SELECT close_time, close
                    FROM candles
                    WHERE exchange = %s
                      AND symbol = %s
                      AND timeframe = %s
                    ORDER BY close_time DESC
                    LIMIT %s
                    """,
                    (exchange, symbol, timeframe, window_size),
                )
                rows = cur.fetchall()
                if len(rows) < 2:
                    logger.debug(f"[fx_corr] Not enough candles for {symbol} ({timeframe}).")
                    continue

                # rows are in DESC order, reverse them for ascending time
                rows.reverse()

                times = []
                closes = []
                for close_time, close in rows:
                    if close is None:
                        continue
                    times.append(close_time)
                    closes.append(float(close))

                if len(closes) < 2:
                    logger.debug(f"[fx_corr] After filtering NULLs, not enough data for {symbol} ({timeframe}).")
                    continue

                s = pd.Series(data=closes, index=pd.to_datetime(times))
                series_by_symbol_price[symbol] = s

            if len(series_by_symbol_price) < 2:
                logger.debug(
                    f"[fx_corr] Not enough symbols with valid candle data to compute correlations. "
                    f"timeframe={timeframe}"
                )
                return

            # --------------------------------------------------
            # 2) Price DF and return DF (candle-based)
            # --------------------------------------------------
            df_price = pd.DataFrame(series_by_symbol_price)
            df_price = df_price.dropna()

            if df_price.shape[0] < 2:
                logger.debug(
                    f"[fx_corr] Not enough aligned price rows after dropna(). "
                    f"rows={df_price.shape[0]}, timeframe={timeframe}"
                )
                return

            df_ret = df_price.pct_change().dropna()

            if df_ret.shape[0] < min_rows_for_corr:
                logger.debug(
                    f"[fx_corr] Not enough aligned return rows after dropna(). "
                    f"rows={df_ret.shape[0]}, min_required={min_rows_for_corr}, timeframe={timeframe}"
                )
                return

            # Time window determined by candle returns
            first_ts = df_ret.index.min()
            last_ts = df_ret.index.max()

            # fx_correlation.as_of_time is TIMESTAMP WITHOUT TIME ZONE (0 fractional seconds)
            if last_ts.tzinfo is not None:
                last_ts_utc = last_ts.tz_convert("UTC")
            else:
                last_ts_utc = last_ts

            as_of_time = last_ts_utc.replace(microsecond=0, tzinfo=None)

            # --------------------------------------------------
            # 3) Candle-based correlation matrices
            # --------------------------------------------------
            corr_price = df_price.corr()  # Pearson on price level
            corr_ret = df_ret.corr()      # Pearson on returns

            # --------------------------------------------------
            # 4) Tick-based returns & correlation
            #    Use ticks in [first_ts, last_ts], resampled to 1-second grid.
            # --------------------------------------------------
            tick_ret_by_symbol: Dict[str, pd.Series] = {}

            for symbol in symbols:
                cur.execute(
                    """
                    SELECT tick_time, last_price
                    FROM tickers
                    WHERE exchange = %s
                      AND symbol = %s
                      AND tick_time >= %s
                      AND tick_time <= %s
                    ORDER BY tick_time ASC
                    """,
                    (exchange, symbol, first_ts, last_ts),
                )
                trows = cur.fetchall()
                if len(trows) < 5:
                    logger.debug(f"[fx_corr] Not enough ticks for {symbol} in [{first_ts}, {last_ts}].")
                    continue

                t_times = []
                t_prices = []
                for tick_time, last_price in trows:
                    if last_price is None:
                        continue
                    t_times.append(tick_time)
                    t_prices.append(float(last_price))

                if len(t_prices) < 5:
                    logger.debug(f"[fx_corr] After filtering NULL tick prices, not enough data for {symbol}.")
                    continue

                s_tick_price = pd.Series(data=t_prices, index=pd.to_datetime(t_times))

                # Resample to 1-second grid, forward-fill to align symbols
                s_tick_price = (
                    s_tick_price
                    .resample("1s")
                    .last()
                    .ffill()
                )

                s_tick_ret = s_tick_price.pct_change().dropna()

                if len(s_tick_ret) < min_rows_for_corr:
                    logger.debug(
                        f"[fx_corr] Not enough tick-return rows for {symbol}. "
                        f"rows={len(s_tick_ret)}, min_required={min_rows_for_corr}"
                    )
                    continue

                tick_ret_by_symbol[symbol] = s_tick_ret

            corr_tick = None
            if len(tick_ret_by_symbol) >= 2:
                df_tick_ret = pd.DataFrame(tick_ret_by_symbol).dropna()
                if df_tick_ret.shape[0] >= min_rows_for_corr:
                    corr_tick = df_tick_ret.corr()
                else:
                    logger.debug(
                        f"[fx_corr] Not enough aligned tick-return rows after dropna(). "
                        f"rows={df_tick_ret.shape[0]}, min_required={min_rows_for_corr}, timeframe={timeframe}"
                    )
            else:
                logger.debug(
                    f"[fx_corr] Not enough symbols with valid tick-return data to compute tick correlations. "
                    f"timeframe={timeframe}"
                )

            # --------------------------------------------------
            # 5) Insert/Update fx_correlation
            #    Only store each unordered pair once (base_symbol < ref_symbol)
            # --------------------------------------------------
            for base_symbol in symbols:
                if (
                    base_symbol not in corr_price.columns
                    or base_symbol not in corr_ret.columns
                ):
                    continue

                for ref_symbol in symbols:
                    if (
                        ref_symbol not in corr_price.columns
                        or ref_symbol not in corr_ret.columns
                    ):
                        continue
                    if base_symbol == ref_symbol:
                        continue

                    # Avoid duplicates: only store base < ref lexicographically
                    if base_symbol >= ref_symbol:
                        continue

                    # Candle-based price & return correlations
                    price_val = float(corr_price.loc[base_symbol, ref_symbol])
                    ret_val = float(corr_ret.loc[base_symbol, ref_symbol])

                    corr_price_rounded = round(price_val, 2)  # numeric(18,2)
                    corr_ret_rounded = round(ret_val, 2)

                    # Tick-based correlation (may be None)
                    tick_val_rounded = None
                    if corr_tick is not None:
                        if (
                            base_symbol in corr_tick.columns
                            and ref_symbol in corr_tick.columns
                        ):
                            tick_val = float(corr_tick.loc[base_symbol, ref_symbol])
                            tick_val_rounded = round(tick_val, 2)

                    cur.execute(
                        """
                        INSERT INTO fx_correlation
                          (exchange,
                           base_symbol,
                           ref_symbol,
                           timeframe,
                           window_size,
                           corr_value_by_price,
                           corr_value_by_return,
                           corr_value_by_tick,
                           as_of_time)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (exchange, base_symbol, ref_symbol, timeframe, as_of_time)
                        DO UPDATE SET
                            corr_value_by_price  = EXCLUDED.corr_value_by_price,
                            corr_value_by_return = EXCLUDED.corr_value_by_return,
                            corr_value_by_tick   = EXCLUDED.corr_value_by_tick,
                            window_size          = EXCLUDED.window_size
                        """,
                        (
                            exchange,
                            base_symbol,
                            ref_symbol,
                            timeframe,          # e.g. "5m" / "15m"
                            window_size,
                            corr_price_rounded,
                            corr_ret_rounded,
                            tick_val_rounded,
                            as_of_time,
                        ),
                    )

            logger.info(
                f"[fx_corr] Updated correlations for timeframe={timeframe} "
                f"as_of_time={as_of_time} "
                f"rows_price={df_price.shape[0]} rows_ret={df_ret.shape[0]}"
            )


# Optional manual test
if __name__ == "__main__":
    # Example: run once for "5m"
    update_fx_correlations_for_timeframe("5m")
