
import asyncio
from pathlib import Path
import time
from datetime import datetime, timedelta
from fx_correlation_mataf_service import update_mataf_correlation_from_csv
from telegram_notifier import notify_telegram, ChatType, start_telegram_notifier, close_telegram_notifier, ChatType
from logger_config import setup_logger
from dotenv import load_dotenv

def run_job():
    print(f"[{datetime.now()}] Running Mataf update...")
    update_mataf_correlation_from_csv(num_period=50)


def next_run_at_xx_10(now: datetime) -> datetime:
    """
    Return the next wall-clock time at HH:10:00.
    If it is before HH:10 in the current hour, use this hour.
    Otherwise use the next hour.
    """
    candidate = now.replace(minute=10, second=0, microsecond=0)

    if now < candidate:
        # Next run is this hour at :10
        return candidate
    else:
        # Already passed :10 → schedule next hour at :10
        return candidate + timedelta(hours=1)
    
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
     
        # 1) Run once on startup
        run_job()
            
        # 2) Then repeat aligned to the clock
        while True:
            now = datetime.now()
            next_run = next_run_at_xx_10(now)
            wait_seconds = (next_run - now).total_seconds()

            print(f"Next run at {next_run}, sleeping {wait_seconds:.1f} seconds...")
            if wait_seconds > 0:
                await asyncio.sleep(wait_seconds)

            run_job()

    finally:
        notify_telegram(f"⛔️ QuantFlow_FX_Correlation App stopped.", ChatType.ALERT)
        await close_telegram_notifier()

if __name__ == "__main__":
   asyncio.run(main())


