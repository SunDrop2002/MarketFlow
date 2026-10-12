from ingestion import raw_storage

def main():
    candles_data = raw_storage.read_data()
    if candles_data in (None,[]):
        print("No candle data was loaded.")
        return
    
    candle_count = len(candles_data)
    fields_per_candle = len(candles_data[0])
    print(f"Loaded candles: {candle_count}")
    print(f"Fields in first candle: {fields_per_candle}")
       
    


if __name__ == "__main__":
    main()