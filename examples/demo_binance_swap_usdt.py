'''
Copyright (C) 2017-2020  Bryant Moscon - bmoscon@gmail.com

Please see the LICENSE file for the terms and conditions
associated with this software.
'''
import os,sys
curDir = os.path.dirname(__file__)
sys.path.append(os.path.abspath(os.path.dirname(__file__)+'/../'))
sys.path.append(os.path.abspath(os.path.dirname(__file__)+'/'))
cur_dir = os.path.dirname( os.path.abspath(__file__)) or os.getcwd()
sys.path.append(cur_dir)
sys.path.append("..")



from cryptofeed.callback import TickerCallback, TradeCallback, BookCallback
from cryptofeed import FeedHandler
# from cryptofeed.exchanges import HuobiDM
# from cryptofeed.exchanges import HuobiSwap
from cryptofeed.exchanges import BinanceFutures
from cryptofeed.pairs import binance_futures_pairs

from cryptofeed.defines import L2_BOOK_SWAP, L2_BOOK,L3_BOOK, BOOK_DELTA,BID, ASK, TRADES, TRADES_SWAP, OPEN_INTEREST, FUNDING, TICKER_SWAP

import redis
import json
import time
import requests

redis_ip = '127.0.0.1'
redis_port = 6379

pool = redis.ConnectionPool(host=redis_ip, port=redis_port)
r = redis.Redis(connection_pool=pool)

# Timestamp: 1608857271.874 Feed: HUOBI_SWAP_USDT Pair: BTC-USDT ID: 52743207850002 Side: sell Amount: 600 Price: 23608
async def trade(feed, pair, order_id, timestamp, side, amount, price, receipt_timestamp):
    # print(f"Timestamp: {timestamp} Feed: {feed} Pair: {pair} ID: {order_id} Side: {side} Amount: {amount} Price: {price}")
    pair_key = f"binance_swap_{pair.lower()}"
    # print(f"set {pair_key}")
    j = {}
    j['ask0'] = float(price)
    j['bid0'] = float(price)
    j['update_timestamp'] = time.time()
    print(f"set {pair_key} {j}")
    r.set(pair_key,json.dumps(j))


async def book(feed, pair, book, timestamp, receipt_timestamp):
    # print(f'Timestamp: {timestamp} Feed: {feed} Pair: {pair} Book Bid Size is {len(book[BID])} Ask Size is {len(book[ASK])}')
    # print(f'Timestamp: {timestamp} Feed: {feed} Pair: {pair} Book Bid Size is {len(book[BID])} Ask Size is {len(book[ASK])}, Bid is {book[BID]}, Ask is {book[ASK]}')
    bids = book[BID]
    bid0_price = 0
    ask0_price = 0
    for price in bids:
        bid0_price = float(price)
        # print(f"bid0 price is {price}, amount {bids[price]}")
        break
    asks = book[ASK]
    for price in asks:
        ask0_price = float(price)
        # print(f"ask0 price is {price}, amount {asks[price]}")
        break
    pair_key = f"hbdm_swap_{pair.lower()}"
    # print(f"set {pair_key}")
    j = {}
    j['ask0'] = ask0_price
    j['bid0'] = bid0_price
    j['update_timestamp'] = time.time()
    print(f"set {pair_key} {j}")
    r.set(pair_key,json.dumps(j))


    # if((not pair_key in last_update_timestamp_dict) or ( (pair_key in last_update_timestamp_dict) and (time.time() - last_update_timestamp_dict[pair_key] > 300))):
    #     last_update_timestamp_dict[pair_key] = time.time()
    #     response = requests.get(server_record_redis_url + f"?key={pair_key}&value={json.dumps(j)}")
    #     if response.status_code == 200:
    #         print(f"set {pair_key} {response.text}")
    #     else:
    #         print(f"{response.status_code}")

async def open_interest(feed, pair, open_interest, timestamp, receipt_timestamp):
    print(f'Timestamp: {timestamp} Feed: {feed} Pair: {pair} open interest: {open_interest}')


async def funding(**kwargs):
    print(f"Funding: {kwargs}")

async def ticker(**kwargs):
    print(f"Tikcer: {kwargs}")



def get_trade_symbol_list():
    '''
    // 20210214003858
        // https://fapi.binance.com/fapi/v1/exchangeInfo

        {
          "timezone": "UTC",
          "serverTime": 1613234338179,
          "futuresType": "U_MARGINED",
          "rateLimits": [
            {
              "rateLimitType": "REQUEST_WEIGHT",
              "interval": "MINUTE",
              "intervalNum": 1,
              "limit": 2400
            },
            {
              "rateLimitType": "ORDERS",
              "interval": "MINUTE",
              "intervalNum": 1,
              "limit": 1200
            }
          ],
          "exchangeFilters": [

          ],
          "symbols": [
            {
              "symbol": "BTCUSDT",
              "pair": "BTCUSDT",
              "contractType": "PERPETUAL",
              "deliveryDate": 4133404800000,
              "onboardDate": 1569398400000,
              "status": "TRADING",
              "maintMarginPercent": "2.5000",
              "requiredMarginPercent": "5.0000",
              "baseAsset": "BTC",
              "quoteAsset": "USDT",
              "marginAsset": "USDT",
              "pricePrecision": 2,
              "quantityPrecision": 3,
              "baseAssetPrecision": 8,
              "quotePrecision": 8,
              "underlyingType": "COIN",
              "underlyingSubType": [

              ],
              "settlePlan": 0,
              "triggerProtect": "0.0500",
              "filters": [
                {
                  "minPrice": "0.01",
                  "maxPrice": "1000000",
                  "filterType": "PRICE_FILTER",
                  "tickSize": "0.01"
                },
                {
                  "stepSize": "0.001",
                  "filterType": "LOT_SIZE",
                  "maxQty": "1000",
                  "minQty": "0.001"
                },
                {
                  "stepSize": "0.001",
                  "filterType": "MARKET_LOT_SIZE",
                  "maxQty": "1000",
                  "minQty": "0.001"
                },
                {
                  "limit": 200,
                  "filterType": "MAX_NUM_ORDERS"
                },
                {
                  "limit": 100,
                  "filterType": "MAX_NUM_ALGO_ORDERS"
                },
                {
                  "notional": "1",
                  "filterType": "MIN_NOTIONAL"
                },
                {
                  "multiplierDown": "0.8500",
                  "multiplierUp": "1.1500",
                  "multiplierDecimal": "4",
                  "filterType": "PERCENT_PRICE"
                }
              ],
              "orderTypes": [
                "LIMIT",
                "MARKET",
                "STOP",
                "STOP_MARKET",
                "TAKE_PROFIT",
                "TAKE_PROFIT_MARKET",
                "TRAILING_STOP_MARKET"
              ],
              "timeInForce": [
                "GTC",
                "IOC",
                "FOK",
                "GTX"
              ]
            },
            {
              "symbol": "ETHUSDT",
              "pair": "ETHUSDT",
              "contractType": "PERPETUAL",
              "deliveryDate": 4133404800000,
              "onboardDate": 1569398400000,
              "status": "TRADING",
              "maintMarginPercent": "2.5000",
              "requiredMarginPercent": "5.0000",
              "baseAsset": "ETH",
              "quoteAsset": "USDT",
              "marginAsset": "USDT",
              "pricePrecision": 2,
              "quantityPrecision": 3,
              "baseAssetPrecision": 8,
              "quotePrecision": 8,
              "underlyingType": "COIN",
              "underlyingSubType": [

              ],
              "settlePlan": 0,
              "triggerProtect": "0.0500",
              "filters": [
                {
                  "minPrice": "0.01",
                  "maxPrice": "100000",
                  "filterType": "PRICE_FILTER",
                  "tickSize": "0.01"
                },
                {
                  "stepSize": "0.001",
                  "filterType": "LOT_SIZE",
                  "maxQty": "10000",
                  "minQty": "0.001"
                },
                {
                  "stepSize": "0.001",
                  "filterType": "MARKET_LOT_SIZE",
                  "maxQty": "10000",
                  "minQty": "0.001"
                },
                {
                  "limit": 200,
                  "filterType": "MAX_NUM_ORDERS"
                },
                {
                  "limit": 100,
                  "filterType": "MAX_NUM_ALGO_ORDERS"
                },
                {
                  "notional": "1",
                  "filterType": "MIN_NOTIONAL"
                },
                {
                  "multiplierDown": "0.8500",
                  "multiplierUp": "1.1500",
                  "multiplierDecimal": "4",
                  "filterType": "PERCENT_PRICE"
                }
              ],
              "orderTypes": [
                "LIMIT",
                "MARKET",
                "STOP",
                "STOP_MARKET",
                "TAKE_PROFIT",
                "TAKE_PROFIT_MARKET",
                "TRAILING_STOP_MARKET"
              ],
              "timeInForce": [
                "GTC",
                "IOC",
                "FOK",
                "GTX"
              ]
            }
          ]
        }

    '''

    symbol_list = []
    basic_symbol_url = "https://fapi.binance.com/fapi/v1/exchangeInfo"

    res = requests.get(basic_symbol_url)
    res_json = res.json()

    for info in res_json['symbols']:
        if(info['quoteAsset'] == 'USDT'):
            symbol_list.append(info['symbol'])

    return symbol_list


def main():
    fh = FeedHandler()
    # pairs_list = ['BTC-USDT', 'ETH-USDT', 'XRP-USDT', 'LTC-USDT', 'LINK-USDT', 'TRX-USDT', 'DOT-USDT', 'ADA-USDT', 'EOS-USDT',
    #  'BCH-USDT', 'BSV-USDT', 'YFI-USDT', 'UNI-USDT', 'FIL-USDT', 'YFII-USDT', 'SNX-USDT', 'BNB-USDT', 'ZEC-USDT',
    #  'DASH-USDT', 'ETC-USDT', 'THETA-USDT', 'KSM-USDT', 'ATOM-USDT', 'AAVE-USDT', 'XLM-USDT', 'SUSHI-USDT', 'CRV-USDT',
    #  'WAVES-USDT', 'KAVA-USDT', 'RSR-USDT', 'NEO-USDT', 'XMR-USDT', 'ALGO-USDT', 'VET-USDT', 'XTZ-USDT', 'COMP-USDT',
    #  'OMG-USDT', 'XEM-USDT', 'ONT-USDT', 'ZIL-USDT', 'AVAX-USDT', 'BAND-USDT', 'GRT-USDT', '1INCH-USDT', 'DOGE-USDT',
    #  'MATIC-USDT', 'LRC-USDT', 'SOL-USDT', 'IOTA-USDT']

    # pairs_list = get_trade_symbol_list()
    pairs_list = binance_futures_pairs()
    print(pairs_list)

    # fh.add_feed(OKEx(pairs=['EOS-USD-SWAP'], channels=[TRADES_SWAP, L2_BOOK_SWAP, OPEN_INTEREST, FUNDING], callbacks={FUNDING: funding, OPEN_INTEREST: open_interest, TRADES: TradeCallback(trade), L2_BOOK: BookCallback(book), TICKER_SWAP:TickerCallback(ticker)}))
    #fh.add_feed(OKEx(pairs=['EOS-USD-SWAP'], channels=[ TRADES_SWAP,L2_BOOK_SWAP], callbacks={FUNDING: funding, OPEN_INTEREST: open_interest, TRADES_SWAP: TradeCallback(trade), L2_BOOK: BookCallback(book)}))
    # fh.add_feed(HuobiDM(pairs=['BTC'], channels=[ TRADES], callbacks={FUNDING: funding, OPEN_INTEREST: open_interest, TRADES_SWAP: TradeCallback(trade), L2_BOOK: BookCallback(book)}))
    # fh.add_feed(HuobiDM(pairs=['BTC_CQ'], channels=[ TRADES,L2_BOOK], callbacks={FUNDING: funding, OPEN_INTEREST: open_interest, TRADES: TradeCallback(trade), L2_BOOK: BookCallback(book)}))

    # fh.add_feed(HuobiDM(max_depth=1, pairs=['BTC_CQ'], channels=[ L2_BOOK], callbacks={ L2_BOOK: BookCallback(book)}))

    # fh.add_feed(HuobiSwapUsdt(max_depth=1, pairs=pairs_list, channels=[ L2_BOOK], callbacks={ L2_BOOK: BookCallback(book)}))
    fh.add_feed(BinanceFutures(max_depth=1, pairs=pairs_list, channels=[ TRADES], callbacks={ TRADES: TradeCallback(trade)}))
    # fh.add_feed(HuobiSwap(pairs=['BTC-USDT'], channels=[ L2_BOOK], callbacks={ L2_BOOK: BookCallback(book)}))

    fh.run()


# last_update_timestamp_dict = {}
# server_record_redis_url = json.load(open("./url_conf.json","r"))['set_redis_key'] #"http://127.0.0.1:6007/api/set_redis_key"

if __name__ == '__main__':
    main()
