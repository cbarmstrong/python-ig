#!/usr/bin/python3.6

import datetime
import traceback

import sys
from trading_ig import IGService
import ig_config
from pymongo import MongoClient
import time

c = MongoClient()
ftse_db = c.ftse_db
epics = ftse_db.epics
ohlc_data = ftse_db.prices

ig = IGService(ig_config.ig_usr, ig_config.ig_pwd, ig_config.ig_key, 'live')
ig.create_session()


def get_epics(stocks):
    r_epics = {}

    for stock in stocks:

        p_epics = []

        s_epic = epics.find_one({'stock': stock})
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
                except:
                    pass
            if len(p_epics) > 0:
                print(f"Selected {p_epics[choice]['epic']} for {stock}")
                p_epics[choice].update({'stock': stock})
                r_epics[stock] = p_epics[choice]
        if len(p_epics) > 0 and choice > -1:
            print(f"Inserting {p_epics[choice]}")
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


def clean_data(epic):
    data = ohlc_data.find({'epic': epic})
    x_data = {}
    for line in data:
        if line['date'] not in x_data.keys():
            x_data[line['date']] = 0
        x_data[line['date']] += 1
    for date, count in x_data.items():
        if count > 1:
            print(f"Found {count} items on {date} for {epic}")
            ohlc_data.remove({'epic': epic, 'date': date})


def find_gaps(epic, date, lookback):
    gaps = {}
    date_from_date = date
    date_to_date = date_from_date - datetime.timedelta(days=lookback)
    count = ohlc_data.find(
        {'epic': epic, 'date': {'$lte': to_secs(date_from_date), '$gt': to_secs(date_to_date)}}).count()
    print(f"Got {count} data points between {to_ymd(date_from_date)} and {to_ymd(date_to_date)} for {epic}")
    if count == lookback:
        return gaps
    s_date = to_secs(date_from_date)
    run_start = s_date
    while date_from_date > date_to_date:
        # print(f"Searching for data on {date_from_date}")
        count = ohlc_data.find({'epic': epic, 'date': s_date}).count()
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
    ohlcs = []
    date_from_date = date
    date_to_date = date_from_date - datetime.timedelta(days=lookback)
    date_from_secs = to_secs(date_from_date)
    date_to_secs = to_secs(date_to_date)
    data = ohlc_data.find({'epic': epic, 'date': {'$lte': date_to_secs, '$gt': date_from_secs}})
    if data is None or data.count() != lookback:
        if populate_ohlc(epic, to_ymd(date_from_date), to_ymd(date_to_date)):
            pass
        else:
            return []
    else:
        ohlcs.append(data)
    return ohlcs


def populate_ohlc(epic, date_from, date_to):
    global ig
    try:
        time.sleep(2)
        print(f"Pulling data from {date_from} 00:00:00 to {date_to} 01:00:00")
        prices = ig.fetch_historical_prices_by_epic_and_date_range(epic, 'D', date_to + ' 00:00:00',
                                                                   date_from + ' 01:00:00')
        ohlcs = prices['prices']
        #print(f"{prices}")
        allowance = prices['allowance']['remainingAllowance']
        restart = prices['allowance']['allowanceExpiry']
        reset_on = datetime.datetime.now() + datetime.timedelta(seconds=restart)
        resets = reset_on.strftime("%d/%m/%Y %H:%M:%S")
        print(f"Allowance remaining: {allowance} - resets at: {resets}")
        from_date = from_ymd(date_from)
        to_date = from_ymd(date_to)
        index = from_date.strftime('%Y:%m:%d-00:00:00')
        while from_date >= to_date:
            secs = to_secs(from_date)
            if ohlc_data.find({'epic': epic, 'date': secs}).count() == 0:
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
                    #print(f"{insert}")
                    ohlc_data.insert_one(insert)
                else:
                    print(f"Inserting zero data on {from_date} ({secs})")
                    ohlc_data.insert_one({
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
                    })
            else:
                print(f"Not inserting data on {from_date} ({secs})")
            from_date -= datetime.timedelta(days=1)
            index = from_date.strftime('%Y:%m:%d-00:00:00')
    except Exception as e:
        x = e.args[0]
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
