from ingestion import binance_client,raw_storage

def main():
    retrieved_data = binance_client.get_candles('BTCUSDT',"1m",5)
    if retrieved_data not in (None,[]):
        print(f"number of candles : {len(retrieved_data)}")
        # candle candles_records much more constructed using list of dictionaries
        keys = ["Timestamp", "Open", "High","Low", "Close","Volume"]
        candles_records = [dict(zip(keys, row)) for row in retrieved_data]

        # First candle retrieved_data
        print("First Candle: ")
        print(f"Open:  {candles_records[0]['Open']}")
        print(f"High:  {candles_records[0]['High']}")
        print(f"Low:  {candles_records[0]['Low']}")
        print(f"Close:  {candles_records[0]['Close']}")
        print(f"Volume: {candles_records[0]['Volume']}")

        print(candles_records)
        print(type(retrieved_data))
        print(type(retrieved_data[0]))
        print(type(retrieved_data[0][4]))

        #saving data in json file
        raw_storage.save_data(retrieved_data)

        # reading saved data from json file
        local_data = raw_storage.read_data()

        print("candles data saved from file : ")
        print(local_data)

        print(f"Saved data matches original: {local_data == retrieved_data}")
        print(f"Loaded candles: {len(local_data)}")



    else:
        print("No candle data was retrieved.")
     

if __name__ == "__main__":
    main()