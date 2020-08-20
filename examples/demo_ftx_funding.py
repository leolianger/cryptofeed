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

from cryptofeed import FeedHandler
from cryptofeed.callback import FundingCallback
from cryptofeed.defines import FUNDING
from cryptofeed.exchanges import FTX

from cryptofeed.callback import BookCallback, TickerCallback, TradeCallback
from cryptofeed.defines import BID, ASK, L2_BOOK, TICKER, TRADES


import redis
import json
import time
import arrow

redis_ip = '127.0.0.1'
redis_port = 6379

pool = redis.ConnectionPool(host=redis_ip, port=redis_port)
r = redis.Redis(connection_pool=pool)

# Examples of some handlers for different updates. These currently don't do much.
# Handlers should conform to the patterns/signatures in callback.py
# Handlers can be normal methods/functions or async. The feedhandler is paused
# while the callbacks are being handled (unless they in turn await other functions or I/O)
# so they should be as lightweight as possible

async def funding(**kwargs):
    print(f"Funding Update for {kwargs['feed']}")
    print(kwargs)




async def book(feed, pair, book, timestamp, receipt_timestamp):
    # print(f'Timestamp: {timestamp} Feed: {feed} Pair: {pair} Book Bid Size is {len(book[BID])} Ask Size is {len(book[ASK])}')
    # print(f'Timestamp: {timestamp} Feed: {feed} Pair: {pair} Book Bid Size is {len(book[BID])} Ask Size is {len(book[ASK])}, Bid is {book[BID]}, Ask is {book[ASK]}')
    order_book_j = {}
    order_book_j['asks'] = []
    order_book_j['bids'] = []

    bid0_price = 0
    bids = book[BID]
    for price in bids:
        bid_price = float(price)
        bid_amount = float(bids[price])
        order_book_j['bids'].insert(0,{"price":bid_price, "amount":bid_amount})
        # print(f"bid0 price is {price}, amount {bids[price]}")
        if(bid0_price ==0):
            bid0_price = bid_price

    ask0_price = 0
    asks = book[ASK]
    for price in asks:
        ask_price = float(price)
        ask_amount = float(asks[ask_price])
        order_book_j['asks'].append({"price":ask_price, "amount":ask_amount})
        if(ask0_price ==0):
            ask0_price = ask_price

    pair_key = f"ftx_{pair.lower()}_book"
    # print(f"set {pair_key}")
    order_book_j['update_timestamp'] = time.time()
    order_book_j['update_timestamp_s'] = arrow.get(order_book_j['update_timestamp']).format()
    print(f"set {pair_key} ask0: {ask0_price}, bid0: {bid0_price}, diff {ask0_price - bid0_price}")
    r.set(pair_key,json.dumps(order_book_j))


def main():
    f = FeedHandler()
    # f.add_feed(FTX(max_depth=10,pairs=['BTC-PERP'], channels=[FUNDING,L2_BOOK], callbacks={L2_BOOK: BookCallback(book),FUNDING: FundingCallback(funding)}))
    f.add_feed(FTX(max_depth=10,pairs=['BTC-PERP'], channels=[L2_BOOK], callbacks={L2_BOOK: BookCallback(book),FUNDING: FundingCallback(funding)}))


    f.run()


if __name__ == '__main__':
    main()
