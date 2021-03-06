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
from cryptofeed.exchanges import HuobiDM
from cryptofeed.exchanges import HuobiSwap
from cryptofeed.defines import L2_BOOK_SWAP, L2_BOOK,L3_BOOK, BOOK_DELTA,BID, ASK, TRADES, TRADES_SWAP, OPEN_INTEREST, FUNDING, TICKER_SWAP

import redis
import json
import time
import requests

redis_ip = '127.0.0.1'
redis_port = 6379

pool = redis.ConnectionPool(host=redis_ip, port=redis_port)
r = redis.Redis(connection_pool=pool)


async def trade(feed, pair, order_id, timestamp, side, amount, price, receipt_timestamp):
    # print(f"Timestamp: {timestamp} Feed: {feed} Pair: {pair} ID: {order_id} Side: {side} Amount: {amount} Price: {price}")
    pair_key = f"hbdm_swap_{pair.lower()}"
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


def main():
    fh = FeedHandler()
    pairs_list = ['BTC-USD', 'ETH-USD', 'LINK-USD', 'FIL-USD', 'UNI-USD', 'DOT-USD', 'EOS-USD', 'ADA-USD', 'LTC-USD', 'BAND-USD',
                  'WAVES-USD', 'JST-USD', 'ATOM-USD', 'ZEC-USD', 'BCH-USD', 'BSV-USD', 'XRP-USD', 'ETC-USD', 'TRX-USD', 'QTUM-USD',
                  'IOTA-USD', 'NEO-USD', 'ONT-USD', 'XLM-USD', 'XMR-USD', 'XTZ-USD', 'DASH-USD', 'ALGO-USD', 'VET-USD', 'ZRX-USD',
                  'DOGE-USD', 'THETA-USD', 'REN-USD', 'IOST-USD', 'BTM-USD', 'COMP-USD', 'SNX-USD', 'OMG-USD', 'ANT-USD', 'CRV-USD',
                  'KSM-USD', 'RSR-USD', 'STORJ-USD', 'YFI-USD', 'SUSHI-USD', 'YFII-USD', 'TRB-USD', 'SUN-USD', 'AAVE-USD', 'NEAR-USD',
                  'GRT-USD']

    # 'YFV-USD',

    # 'LEND-USD',


    # fh.add_feed(OKEx(pairs=['EOS-USD-SWAP'], channels=[TRADES_SWAP, L2_BOOK_SWAP, OPEN_INTEREST, FUNDING], callbacks={FUNDING: funding, OPEN_INTEREST: open_interest, TRADES: TradeCallback(trade), L2_BOOK: BookCallback(book), TICKER_SWAP:TickerCallback(ticker)}))
    #fh.add_feed(OKEx(pairs=['EOS-USD-SWAP'], channels=[ TRADES_SWAP,L2_BOOK_SWAP], callbacks={FUNDING: funding, OPEN_INTEREST: open_interest, TRADES_SWAP: TradeCallback(trade), L2_BOOK: BookCallback(book)}))
    # fh.add_feed(HuobiDM(pairs=['BTC'], channels=[ TRADES], callbacks={FUNDING: funding, OPEN_INTEREST: open_interest, TRADES_SWAP: TradeCallback(trade), L2_BOOK: BookCallback(book)}))
    # fh.add_feed(HuobiDM(pairs=['BTC_CQ'], channels=[ TRADES,L2_BOOK], callbacks={FUNDING: funding, OPEN_INTEREST: open_interest, TRADES: TradeCallback(trade), L2_BOOK: BookCallback(book)}))

    # fh.add_feed(HuobiDM(max_depth=1, pairs=['BTC_CQ'], channels=[ L2_BOOK], callbacks={ L2_BOOK: BookCallback(book)}))

    # fh.add_feed(HuobiSwap(max_depth=1, pairs=pairs_list, channels=[ L2_BOOK], callbacks={ L2_BOOK: BookCallback(book)}))
    fh.add_feed(HuobiSwap(max_depth=1, pairs=pairs_list, channels=[ TRADES], callbacks={ TRADES: TradeCallback(trade)}))
    # fh.add_feed(HuobiSwap(pairs=['BTC-USD'], channels=[ L2_BOOK], callbacks={ L2_BOOK: BookCallback(book)}))

    fh.run()


# last_update_timestamp_dict = {}
# server_record_redis_url = json.load(open("./url_conf.json","r"))['set_redis_key'] #"http://127.0.0.1:6007/api/set_redis_key"

if __name__ == '__main__':
    main()
