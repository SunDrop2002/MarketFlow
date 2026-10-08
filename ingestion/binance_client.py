import requests


def get_candles(symbol,interval,limit):
    # data for constructing my http request 
    url = "https://data-api.binance.vision/api/v3/klines"
    query_param = {
        "symbol": symbol,
        "interval": interval,
        "limit": limit
    }
    try:
        # making the request
        response = requests.get(
            url,
            params=query_param,
            timeout=10
        )
        # it basically throws an error in case of a status code 4xx or 5xx
        response.raise_for_status()

        # retrieving data baby ! woooo !
        data = response.json()
        return data
    
    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error occurred >.< : {http_err}")  # e.g., 404 Not Found or 500 Server Error
    except requests.exceptions.Timeout:
        print("I'm tired boss :'( .")
    except requests.exceptions.RequestException as err:
        print(f"A critical error occurred: {err}")

