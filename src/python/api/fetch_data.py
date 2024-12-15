import requests
import time

# Binance API Configuration
API_URL = "https://api.binance.com/api/v3"
SYMBOLS = ["BTCUSDT", "ETHUSDT", "LTCUSDT", "XRPUSDT"]


def fetch_data_from_api(endpoint, params=None):
    """
    Generic function to fetch data from a Binance API endpoint.

    Args:
        endpoint (str): The API endpoint to query.
        params (dict): Optional parameters for the API call.

    Returns:
        dict/list: The JSON response from the API or an empty list in case of errors.
    """
    try:
        response = requests.get(f"{API_URL}/{endpoint}", params=params)
        response.raise_for_status()  # Raise an HTTPError for bad responses (4xx and 5xx)
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data from {endpoint}: {e}")
        return []


def fetch_price_data():
    """
    Fetch the latest price data for all symbols.

    Returns:
        list: A list of price data dictionaries for the specified symbols.
    """
    data = []
    for symbol in SYMBOLS:
        data.append(fetch_data_from_api("ticker/price", {"symbol": symbol}))
        time.sleep(1)  # Pause to avoid hitting rate limits
    return data


def fetch_klines_data():
    """
    Fetch candlestick (Klines) data for all symbols with a 1-minute interval.

    Returns:
        list: A list of dictionaries containing OHLCV data for each symbol.
    """
    data = []
    for symbol in SYMBOLS:
        klines = fetch_data_from_api("klines", {"symbol": symbol, "interval": "1m"})
        for kline in klines:
            # Map Kline data to a structured dictionary
            data.append({
                "symbol": symbol,
                "open_time": kline[0],
                "open_price": kline[1],
                "high_price": kline[2],
                "low_price": kline[3],
                "close_price": kline[4],
                "volume": kline[5],
                "close_time": kline[6],
                "quote_asset_volume": kline[7],
                "number_of_trades": kline[8],
                "taker_buy_base_asset_volume": kline[9],
                "taker_buy_quote_asset_volume": kline[10]
            })
        time.sleep(1)
    return data


def fetch_order_book_data():
    """
    Fetch order book data (top 10 bids and asks) for all symbols.

    Returns:
        list: A list of dictionaries containing order book data for each symbol.
    """
    data = []
    for symbol in SYMBOLS:
        order_book = fetch_data_from_api("depth", {"symbol": symbol, "limit": 10})
        data.append({
            "symbol": symbol,
            "last_update_id": order_book["lastUpdateId"],
            "bids": order_book["bids"],
            "asks": order_book["asks"]
        })
        time.sleep(1)
    return data


def fetch_trades_data():
    """
    Fetch recent trade data (last 10 trades) for all symbols.

    Returns:
        list: A list of dictionaries containing trade data for each symbol.
    """
    data = []
    for symbol in SYMBOLS:
        trades = fetch_data_from_api("trades", {"symbol": symbol, "limit": 10})
        for trade in trades:
            data.append({
                "symbol": symbol,
                "id": trade["id"],
                "price": trade["price"],
                "qty": trade["qty"],
                "quote_qty": trade["quoteQty"],
                "time": trade["time"],
                "is_buyer_maker": trade["isBuyerMaker"],
                "is_best_match": trade["isBestMatch"]
            })
        time.sleep(1)
    return data


def fetch_market_info_data():
    """
    Fetch 24-hour market statistics for all symbols.

    Returns:
        list: A list of dictionaries containing market info data for each symbol.
    """
    data = []
    for symbol in SYMBOLS:
        market_info = fetch_data_from_api("ticker/24hr", {"symbol": symbol})
        data.append(market_info)
        time.sleep(1)
    return data


def fetch_data():
    """
    Fetch all types of data (price, klines, order book, trades, and market info).

    Returns:
        dict: A dictionary containing all fetched data grouped by type.
    """
    price_data = fetch_price_data()
    klines_data = fetch_klines_data()
    order_book_data = fetch_order_book_data()
    trades_data = fetch_trades_data()
    market_info_data = fetch_market_info_data()
    return {
        "price_data": price_data,
        "klines_data": klines_data,
        "order_book_data": order_book_data,
        "trades_data": trades_data,
        "market_info_data": market_info_data
    }


if __name__ == "__main__":
    # Main execution: Fetch data and print it to the console
    data = fetch_data()
    print(data)

