from pathlib import Path

import yaml



CONFIG_PATH = Path("/data/config.yaml")
if not CONFIG_PATH.exists():
    CONFIG_PATH = Path(__file__).resolve().parent / "data" / "config.yaml"

if not CONFIG_PATH.exists():
    raise FileNotFoundError(f"Config file not found: {CONFIG_PATH}")

with CONFIG_PATH.open("r", encoding="utf-8") as f:
    config_data = yaml.safe_load(f) or {}

symbols = [str(s) for s in config_data.get("CORRELATION_SYMBOLS", [])]
timeframes = [str(t) for t in config_data.get("CORRELATION_TIMEFRAMES", [])]


RUN_INTERVALS_MINUTE = int(config_data.get("MIN_ROWS_FOR_CORR", 30))
