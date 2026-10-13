from ingestion import binance_client,raw_storage

def main():
    retrieved_data = binance_client.get_candles('BTCUSDT',"1m",100)
    if retrieved_data in (None,[]):
        print("No candle data was retrieved.")
        return

    raw_storage.save_data(retrieved_data)
    print(f"Saved {len(retrieved_data)} candles to raw storage.")


     

if __name__ == "__main__":
    main()