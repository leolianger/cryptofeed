import os,sys
curDir = os.path.dirname(__file__)
sys.path.append(os.path.abspath(os.path.dirname(__file__)+'/../'))
sys.path.append(os.path.abspath(os.path.dirname(__file__)+'/'))
cur_dir = os.path.dirname( os.path.abspath(__file__)) or os.getcwd()
sys.path.append(cur_dir)
sys.path.append("..")


from cryptofeed import FeedHandler
from cryptofeed.callback import BookCallback, TickerCallback, TradeCallback
from cryptofeed.defines import BID, ASK, FUNDING, L2_BOOK, OPEN_INTEREST, TICKER, TRADES
from cryptofeed.exchanges import Deribit
import time
import redis
import json
from datetime import date
import calendar
import operator



redis_ip = '127.0.0.1'
redis_port = 6379

pool = redis.ConnectionPool(host=redis_ip, port=redis_port)
r = redis.Redis(connection_pool=pool)

# async def trade(feed, pair, order_id, timestamp, side, amount, price):
#     print(f"Timestamp: {timestamp} Feed: {feed} Pair: {pair} ID: {order_id} Side: {side} Amount: {amount} Price: {price}")

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

async def ticker(feed, pair, bid, ask, timestamp):
    print(f'Feed: {feed} Pair: {pair} Bid: {bid} Ask: {ask}')


# async def book(feed, pair, book, timestamp):
#     print(f'Timestamp: {timestamp} Feed: {feed} Pair: {pair} Book Bid Size is {len(book[BID])} Ask Size is {len(book[ASK])}')

async def book(feed, pair, book, timestamp, receipt_timestamp):
    # print(f"{pair} {book}")
    # print(f'Timestamp: {timestamp} Feed: {feed} Pair: {pair} Book Bid Size is {len(book[BID])} Ask Size is {len(book[ASK])}')
    # print(f'Timestamp: {timestamp} Feed: {feed} Pair: {pair} Book Bid Size is {len(book[BID])} Ask Size is {len(book[ASK])}, Bid is {book[BID]}, Ask is {book[ASK]}')
    bids = book[BID]
    bid0_price = 0
    ask0_price = 0
    list_bids =   list(bids)
    if(len(list_bids) > 0):
        bid0_price = float(list_bids[-1])
    # for price in bids:
    #     bid0_price = float(price)
    #     # print(f"bid0 price is {price}, amount {bids[price]}")
    #     break
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

async def funding(**kwargs):
    print(f'Funding {kwargs}')


async def oi(feed, pair, open_interest, timestamp):
    print(f'Timestamp: {timestamp} Feed: {feed} Pair: {pair} open interest: {open_interest}')

def get_expiry_date_of_instrument(instrument_name):
  months = ['JAN', 'FEB', 'MAR', 'APR', 'MAY', 'JUN', 'JUL', 'AUG', 'SEP', 'OCT', 'NOV', 'DEC']
  date1 = instrument_name.split('-')[1]
  if date1 == 'PERPETUAL':
    return None
  date2 = date1
  i = 0
  for member in months:
    if member in date1:
      month = member
    date2 = date2.replace(member, 'MONTH')
  day = date2.split('MONTH')[0]
  year = '20' + date2.split('MONTH')[1]
  month = months.index(month) + 1
  result = date(year=int(year), month=int(month), day=int(day))
  timestamp = calendar.timegm(result.timetuple())
  return timestamp


def main():
    f = FeedHandler()

    deribit_ex = Deribit(['BTC-PERPETUAL','ETH-PERPETUAL'])
    instruments = deribit_ex.get_instruments()
    timestamps = [get_expiry_date_of_instrument(x) for x in instruments]
    timestamps = [x for x in timestamps if x != None]
    timestamps_set_list = list(set(timestamps))
    timestamps_set_list.sort()
    result = list(zip(instruments, timestamps))
    result = [x for x in result if not x[1] == None]
    result = sorted(result, key=operator.itemgetter(1))
    first_timestamp = result[0][1]
    result_latest_3 = [x for x in result if x[1] in timestamps_set_list[0:3]]
    instruments_lastest_3 = [x[0] for x in result_latest_3]
    # timestamps = [x[1] for x in result]


    # Deribit can't handle 400+ simultaneous requests, so if all
    # instruments are needed they should be fed in the different calls

    # config = {TRADES: ["BTC-PERPETUAL"], TICKER: ['ETH-PERPETUAL'], FUNDING: ['ETH-PERPETUAL'], OPEN_INTEREST: ['ETH-PERPETUAL']}
    # f.add_feed(Deribit(config=config, callbacks={OPEN_INTEREST: oi, FUNDING: funding, TICKER: TickerCallback(ticker), TRADES: TradeCallback(trade)}))
    # f.add_feed(Deribit(pairs=['BTC-PERPETUAL'], channels=[L2_BOOK], callbacks={L2_BOOK: BookCallback(book)}))

    # f.add_feed(Deribit(pairs=['BTC-PERPETUAL', 'BTC-27DEC20-26500-C'], channels=[TICKER], callbacks={TICKER: TickerCallback(ticker)}))
    f.add_feed(Deribit(pairs=['BTC-PERPETUAL','ETH-PERPETUAL'], channels=[TRADES], callbacks={ TRADES: TradeCallback(trade)} ))
    # f.add_feed(Deribit(pairs=['BTC-28DEC20-26500-C'], channels=[L2_BOOK], callbacks={L2_BOOK: BookCallback(book)} ))
    f.add_feed(Deribit(pairs=instruments_lastest_3, channels=[L2_BOOK], callbacks={L2_BOOK: BookCallback(book)} ))

    f.run()


if __name__ == '__main__':
    main()
