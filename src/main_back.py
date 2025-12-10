# main.py

import asyncio
from pathlib import Path
import time
from fx_correlation_service import update_fx_correlations_for_timeframe
from telegram_notifier import notify_telegram, ChatType, start_telegram_notifier, close_telegram_notifier, ChatType
from logger_config import setup_logger
from dotenv import load_dotenv
from public_module import timeframes   # e.g. ["5m", "15m"]

async def main():

    await start_telegram_notifier()   
    

    try:


        # Try the Docker volume location first
        env_path = Path("/data/.env")
        # Fallback for local dev
        if not env_path.exists():
            env_path = Path(__file__).resolve().parent / "data" / ".env"
        load_dotenv(dotenv_path=env_path)

        logger = setup_logger()
        logger.info("Starting QuantFlow_Fx_DecEng_1m system...")


        notify_telegram(f"❇️ QuantFlow_FX_Correlation started....", ChatType.ALERT)
     
        if not timeframes:
            logger.error("[main] No timeframes configured in CORRELATION_TIMEFRAMES. Exiting.")
            return

        logger.info(f"[main] Starting correlation loop for timeframes: {timeframes}")


        while True:
            try:
                for tf in timeframes:
                    update_fx_correlations_for_timeframe(tf)

            except Exception as e:
                # Basic error logging; replace with proper logger if you have one
                logger.error(f"[main] Error while updating correlations: {e}")

            # Sleep 60 seconds before next round
            time.sleep(60)

    finally:
        notify_telegram(f"⛔️ QuantFlow_FX_Correlation App stopped.", ChatType.ALERT)
        await close_telegram_notifier()

if __name__ == "__main__":
    asyncio.run(main())
