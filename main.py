import logging

from ingestion import binance_client, raw_storage


logger = logging.getLogger(__name__)


def main():
    candles = binance_client.get_candles("BTCUSDT", "1m", 100)

    if not candles:
        logger.warning("No candle data was retrieved.")
        return False

    raw_storage.save_data(candles)
    logger.info("Saved raw candles: %s", len(candles))
    return True


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )
    main()