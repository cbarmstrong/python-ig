#!/usr/bin/python3.6

import datetime
import traceback

from elasticsearch import Elasticsearch
import sys
from trading_ig import IGService
import ig_config
from pymongo import MongoClient
import time

es = Elasticsearch()
c = MongoClient()
ftse_db = c.ftse_db
epics = ftse_db.epics

ig = IGService(ig_config.ig_usr, ig_config.ig_pwd, ig_config.ig_key, 'live')
ig.create_session()

datatype = 'es'

def get_epics(stocks):
    r_epics = {}

    for stock in stocks:

        p_epics = []

        if datatype == "mongo":
            s_epic = epics.find_one({'stock': stock})
        else:
            s_epic = get_es_epic(stock)
        if s_epic:
            r_epics[stock] = s_epic['epic']
            continue

        s_epics = ig.search_markets(stock)
        time.sleep(0.5)

        for index, epic in s_epics.iterrows():
            if epic['expiry'] == 'DFB' and epic['epic'][:2] == "KA":
                p_epics.append({'epic': epic['epic'], 'name': epic['instrumentName']})

        if len(p_epics) == 1:
            choice = 0
            r_epics[stock] = p_epics[choice]
            p_epics[choice].update({'stock': stock})
            print(f"Selected {p_epics[0]['epic']} for {stock}")
        else:
            choice = -1
            print(f"Choose for {stock}")
            while len(p_epics) > 0 and (choice < 0 or choice > len(p_epics)):
                for index, p in enumerate(p_epics):
                    print(f"{index} - {p['epic']:30} {p['name']}")
                choice = input('--> ')
                try:
                    choice = int(choice)
                except Exception as e:
                    pass
            if len(p_epics) > 0:
                print(f"Selected {p_epics[choice]['epic']} for {stock}")
                p_epics[choice].update({'stock': stock})
                r_epics[stock] = p_epics[choice]
        if len(p_epics) > 0 and choice > -1:
            print(f"Inserting {p_epics[choice]}")
            if datatype == "es":
                es.index(index="epics", doc_type="epic", id=stock, body=p_epics[choice])
            else:
                epics.insert_one(p_epics[choice])

    return r_epics


def to_ymd(date):
    return date.strftime("%Y-%m-%d")


def to_secs(date):
    epoch = datetime.datetime.utcfromtimestamp(0)
    return (date - epoch).total_seconds()


def from_ymd(date):
    return datetime.datetime.strptime(date, "%Y-%m-%d")


def from_secs(secs):
    return datetime.datetime.utcfromtimestamp(secs)


def find_gaps(epic, date, lookback):
    gaps = {}
    date_from_date = date
    date_to_date = date_from_date - datetime.timedelta(days=lookback)

    q = {
        "query": {
            "bool": {
                "must": [
                    {
                        "query_string": {
                            "query": f"epic:/{epic}/ AND "
                                     f"date:<={to_secs(date_from_date)*1000} AND "
                                     f"date:>{to_secs(date_to_date)*1000}"
                        }
                    }
                ]
            }
        }
    }
    res = es.count(index='stocks', doc_type='ohlc', body=q)
    count = res["count"]

    print(f"Got {count} data points between {to_ymd(date_from_date)} and {to_ymd(date_to_date)} for {epic}")
    if count == lookback:
        return gaps
    s_date = to_secs(date_from_date)
    run_start = s_date
    while date_from_date > date_to_date:
        # print(f"Searching for data on {date_from_date}")
        count = count_es_ohlc_on_date(epic, s_date * 1000)
        date_from_date -= datetime.timedelta(days=1)
        s_date = to_secs(date_from_date)
        if count == 0:
            # print("No data")
            if run_start not in gaps.keys():
                gaps[run_start] = 0
            gaps[run_start] += 1
        else:
            if run_start in gaps.keys():
                print(f"Gap to {to_ymd(from_secs(run_start))}: {gaps[run_start]}")
            run_start = s_date
    if run_start in gaps.keys():
        print(f"Gap to {to_ymd(from_secs(run_start))}: {gaps[run_start]}")
    return gaps


def pull_prices(date, epic):
    gaps = find_gaps(epic, date, 100)
    for gap, length in gaps.items():
        print(f"Found Gap from {to_ymd(from_secs(gap))} for {length} day(s) for {epic}")
        populate_ohlc(epic, to_ymd(from_secs(gap)), to_ymd(from_secs(gap) - datetime.timedelta(days=length - 1)))


def get_ohlcs(epic, date, lookback):
    date_from_date = date
    date_to_date = date_from_date - datetime.timedelta(days=lookback)
    date_from_ms = to_secs(date_from_date) * 1000
    date_to_ms = to_secs(date_to_date) * 1000
    data = es.count(index='stocks', doc_type='ohlc', body={
        "query": {
            "bool": {
                "must": [
                    {
                        "query_string": {
                            "query": f"epic:{epic} AND "
                                     f"date:>{date_from_ms} AND "
                                     f"date:<={date_to_ms}"
                        }
                    }
                ]
            }
        }
    })
    from pprint import pprint
    pprint(data)
    return data


def count_es_ohlc_on_date(epic, date_ms):
    count = es.count(index='stocks', doc_type='ohlc', body={
        "query": {
            "bool": {
                "must": [
                    {
                        "query_string": {
                            "query": f"epic:/{epic}/ AND "
                                     f"date:{date_ms}"
                        }
                    }
                ]
            }
        }
    })
    return count['count']


def get_es_epic(stock):
    data = es.search(index='epics', doc_type='epic', body={
        "query": {
            "match_phrase": {
                "stock":  stock
            }
        }
    })
    if data['hits']['total']>0:
        return data['hits']['hits'][0]['_source']
    else:
        return None


def add_stock_to_es(ident, data):
    try:
        res = es.index(index='stocks', doc_type='ohlc', id=ident, body=data)
        print(f"Result: {res['result']}, Version: {res['_version']}")
    except Exception as e:
        print(f"Result: Failed to add {data} as {ident}, Version N/A")


def migrate_epics_to_es():
    from pprint import pprint
    for epic in epics.find({}):
        epic.pop("_id", None)
#         try:
#             res = es.delete(index='stocks', doc_type='ohlc', id=epic['stock'])
#             pprint(res)
#         except Exception as e:
#             pass
        res = es.index(index='epics', doc_type='epic', id=epic['stock'], body=epic)
        print(f"Inserted {epic['stock']} - result: {res['result']}")

def populate_ohlc(epic, date_from, date_to):
    global ig
    try:
        time.sleep(2)
        print(f"Pulling data from {date_from} 00:00:00 to {date_to} 01:00:00")
        prices = ig.fetch_historical_prices_by_epic_and_date_range(epic, 'D', date_to + ' 00:00:00',
                                                                   date_from + ' 01:00:00')
        ohlcs = prices['prices']
        # print(f"{prices}")
        allowance = prices['allowance']['remainingAllowance']
        restart = prices['allowance']['allowanceExpiry']
        reset_on = datetime.datetime.now() + datetime.timedelta(seconds=restart)
        resets = reset_on.strftime("%d/%m/%Y %H:%M:%S")
        print(f"Allowance remaining: {allowance} - resets at: {resets}")
        from_date = from_ymd(date_from)
        to_date = from_ymd(date_to)
        index = from_date.strftime('%Y:%m:%d-00:00:00')
        while from_date >= to_date:
            secs = to_secs(from_date) * 1000
            ident = f"{secs}-{epic}"
            count = count_es_ohlc_on_date(epic, secs)
            if count == 0:
                if index in ohlcs.index:
                    ohlc = ohlcs.loc[index]
                    print(f"Inserting price data on {from_date} ({secs})")
                    insert = {
                        'date': secs,
                        'epic': epic,
                        'bid': {
                            'Open': float(ohlc[('bid', 'Open')]),
                            'High': float(ohlc[('bid', 'High')]),
                            'Low': float(ohlc[('bid', 'Low')]),
                            'Close': float(ohlc[('bid', 'Close')])
                        },
                        'ask': {
                            'Open': float(ohlc[('ask', 'Open')]),
                            'High': float(ohlc[('ask', 'High')]),
                            'Low': float(ohlc[('ask', 'Low')]),
                            'Close': float(ohlc[('ask', 'Close')])
                        },
                        'volume': int(ohlc[('last', 'Volume')])
                    }
                    # print(f"{insert}")
                else:
                    print(f"Inserting zero data on {from_date} ({secs})")
                    insert = {
                        'date': secs,
                        'epic': epic,
                        'bid': {
                            'Open': 0,
                            'High': 0,
                            'Low': 0,
                            'Close': 0
                        },
                        'ask': {
                            'Open': 0,
                            'High': 0,
                            'Low': 0,
                            'Close': 0
                        },
                        'volume': 0
                    }

                add_stock_to_es(ident, insert)
            else:
                print(f"Not inserting data on {from_date} ({secs})")
            from_date -= datetime.timedelta(days=1)
            index = from_date.strftime('%Y:%m:%d-00:00:00')
    except Exception as e:
        if type(e) == Exception and e.args[0] == 'error.public-api.exceeded-account-allowance':
            print(e.args[0])
            time.sleep(60)
            populate_ohlc(epic, date_to, date_to)
            return True
        elif type(e) == Exception and e.args[0] == 'error.security.client-token-invalid':
            print(e.args[0])
            ig = IGService(ig_config.ig_usr, ig_config.ig_pwd, ig_config.ig_key, 'live')
            ig.create_session()
            populate_ohlc(epic, date_to, date_to)
            return True
        elif type(e) == Exception and \
                        e.args[0] == 'error.public-api.exceeded-account-historical-data-allowance':
            print(e.args[0])
            return False
        else:
            info = sys.exc_info()
            print(f"Exception: {info[0]}/n{traceback.print_tb(info[2])}")
            return False
