from prometheus_client import start_http_server
from prometheus_client import Gauge
from binance.spot import Spot
import pandas as pd
import time

def top5_btc_by_volume(clent,symbols):
    quoteBTC = symbols[symbols.str.endswith("BTC")].reset_index(drop=True).copy()
    BTCvolume = client.ticker_24hr(symbols=quoteBTC.tolist())
    quoteBTCvol = pd.DataFrame(BTCvolume)[["symbol", "quoteVolume"]]
    quoteBTCvol['quoteVolume'] = quoteBTCvol['quoteVolume'].astype('float64')
    btc_top5 = quoteBTCvol.nlargest(n=5, columns=["quoteVolume"])["symbol"].tolist()
    print("Top 5 symbols with quote asset BTC and the highest volume over the last 24 hours: ", btc_top5)
    return btc_top5

def top5_usdt_by_trade(clent,symbols):
    quoteUSDT = symbols[symbols.str.endswith("USDT")].reset_index(drop=True).copy()
    USDTvolume = client.ticker_24hr(symbols=quoteUSDT.tolist())
    quoteUSDTvol = pd.DataFrame(USDTvolume)[["symbol", "count"]]
    quoteUSDTvol['count'] = quoteUSDTvol['count'].astype('int64')
    usdt_top5 = quoteUSDTvol.nlargest(n=5, columns=["count"])["symbol"].tolist()
    print("Top 5 symbols with quote asset USDT and the highest number of trades over the last 24 hours: ", usdt_top5)
    return usdt_top5

def total_notional_200_b_a(client,symlist):
    print('Total notional value of the top 200 bids-and-asks:')
    for s in symlist:
        baa = client.depth(s,limit=200)
        bid_df = pd.DataFrame(baa["bids"],columns=["price","qty"],dtype="float64")
        bid_df["total"] = bid_df["price"]*bid_df["qty"]
        ask_df = pd.DataFrame(baa["asks"], columns=["price", "qty"], dtype="float64")
        ask_df["total"] = ask_df["price"] * ask_df["qty"]
        print(f'{s}: bids_notion_value={bid_df["total"].sum()}, asks_notion_value={ask_df["total"].sum()}')

def price_spread(client,symlist):
    ticker_data = client.ticker_24hr(symbols=symlist)
    spread_df = pd.DataFrame(ticker_data)[["symbol", "bidPrice", "askPrice"]]
    spread_df['bidPrice'] = spread_df['bidPrice'].astype('float64')
    spread_df['askPrice'] = spread_df['askPrice'].astype('float64')
    spread_df["priceSpread"]=spread_df["askPrice"] - spread_df["bidPrice"]
    return spread_df


if __name__ == "__main__":
    start_http_server(8080)
    print("Prometheus metrics available on port 8080 /metrics")
    client = Spot()
    exchange_info = client.exchange_info()
    symbols = pd.DataFrame(exchange_info["symbols"])["symbol"]
    btc_sym_list = top5_btc_by_volume(client,symbols)
    usdt_sym_list = top5_usdt_by_trade(client,symbols)
    total_notional_200_b_a(client,btc_sym_list)
    gauge_list_spread = []
    gauge_list_spread_delta = []
    for s in usdt_sym_list:
        gauge_list_spread.append(Gauge(f'metric_price_spread_{s}', f'Price Spread {s}'))
        gauge_list_spread_delta.append(Gauge(f'metric_price_spread_{s}_delta', f'Price Spread {s} delta'))
    starttime = time.time()
    metrics_df=pd.DataFrame(0, index=usdt_sym_list, columns=["priceSpread", "priceDelta"])
    while True:
        time.sleep(10.0 - ((time.time() - starttime) % 10.0))
        metrics_df["prev_priceSpread"]=metrics_df["priceSpread"]
        metrics_df.update(price_spread(client,usdt_sym_list).set_index('symbol'))
        metrics_df["priceDelta"]=abs(metrics_df["priceSpread"]-metrics_df["prev_priceSpread"])
        for g, v in zip(gauge_list_spread,metrics_df["priceSpread"].tolist()):
            g.set(v)
        for g, v in zip(gauge_list_spread_delta,metrics_df["priceDelta"].tolist()):
            g.set(v)
        print("Price spread and absolute delta:\n", metrics_df[["priceSpread", "priceDelta"]])


