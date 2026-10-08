from ingestion import binance_client

def main():
    data = binance_client.get_candles('BTCUSDT',"1m",5)
    if data not in (None,[]):
        print(f"number of candles : {len(data)}")
        # candle records much more constructed using list of dictionaries
        keys = ["Timestamp", "Open", "High","Low", "Close","Volume"]
        records = [dict(zip(keys, row)) for row in data]

        # First candle data
        print("First Candle: ")
        print(f"Open:  {records[0]['Open']}")
        print(f"High:  {records[0]['High']}")
        print(f"Low:  {records[0]['Low']}")
        print(f"Close:  {records[0]['Close']}")
        print(f"Volume: {records[0]['Volume']}")
        print(type(data))
        print(type(data[0]))
        print(type(data[0][4]))

    
if __name__ == "__main__":
    main()