import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path

from main import main as ingest
from process_sample import main as process


PROJECT_ROOT = Path(__file__).resolve().parent
LOG_DIRECTORY = PROJECT_ROOT / "logs"


def configure_logging():
    LOG_DIRECTORY.mkdir(parents=True, exist_ok=True)

    file_handler = RotatingFileHandler(
        LOG_DIRECTORY / "marketflow.log",
        maxBytes=5 * 1024 * 1024,
        backupCount=3,
        encoding="utf-8",
    )

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        handlers=[
            logging.StreamHandler(),
            file_handler,
        ],
        force=True,
    )


def main():
    configure_logging()
    logger = logging.getLogger(__name__)
    logger.info("MarketFlow pipeline started.")

    try:
        logger.info("Fetching and saving market data.")
        if not ingest():
            raise RuntimeError("Ingestion returned no data.")

        logger.info("Processing candles and loading PostgreSQL.")
        process()

    except Exception:
        logger.exception("MarketFlow pipeline failed.")
        raise

    logger.info("MarketFlow pipeline completed successfully.")


if __name__ == "__main__":
    main()