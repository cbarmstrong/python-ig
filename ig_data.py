import datetime

import exceptions
from trading_ig import IGService
import ig_config
from pymongo import MongoClient
import time
import json

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
            print("Selected {} for {}".format(p_epics[0]['epic'], stock))
        else:
            choice = -1
            print("Choose for {}".format(stock))
            while len(p_epics) > 0 and (choice < 0 or choice > len(p_epics)):
                for index, p in enumerate(p_epics):
                    print("{} - {:30} {}".format(index, p['epic'], p['name']))
                choice = raw_input('--> ')
                try:
                    choice = int(choice)
                except:
                    pass
            if len(p_epics) > 0:
                print("Selected {} for {}".format(p_epics[choice]['epic'], stock))
                p_epics[choice].update({'stock': stock})
                r_epics[stock] = p_epics[choice]
        if len(p_epics) > 0 and choice > -1:
            print("Inserting {}".format(p_epics[choice]))
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
            print("Found {} items on {} for {}".format(count, date, epic))
            ohlc_data.remove({'epic': epic, 'date': date})


def find_gaps(epic, date, lookback):
    gaps={}
    date_from_date = date
    date_to_date = date_from_date - datetime.timedelta(days=lookback)
    count=ohlc_data.find({ 'epic': epic, 'date': {'$lte': to_secs(date_from_date), '$gt': to_secs(date_to_date)}}).count()
    print("Got {} data points between {} and {} for {}".format(count, to_ymd(date_from_date), to_ymd(date_to_date), epic))
    if count == lookback:
        return gaps
    s_date = to_secs(date_from_date)
    run_start = s_date
    while date_from_date>date_to_date:
        #print("Searching for data on {}".format(date_from_date))
        count=ohlc_data.find({ 'epic': epic, 'date': s_date }).count()
        date_from_date -= datetime.timedelta(days=1)
        s_date = to_secs(date_from_date)
        if count == 0:
            #print("No data")
            if run_start not in gaps.keys():
                gaps[run_start]=0
            gaps[run_start]+=1
        else:
            if run_start in gaps.keys():
                print("Gap to {}: {}".format(to_ymd(datetime.datetime.utcfromtimestamp(run_start)), gaps[run_start]))
            run_start = s_date
    if run_start in gaps.keys():
        print("Gap to {}: {}".format(to_ymd(datetime.datetime.utcfromtimestamp(run_start)), gaps[run_start]))
    return gaps


def pull_prices(date, epic):
    gaps = find_gaps(epic, date, 100)
    for gap, length in gaps.items():
        print("Found Gap from {} for {} day(s) for {}".format(to_ymd(from_secs(gap)), length, epic))
        populate_ohlc(epic,to_ymd(from_secs(gap)), to_ymd(from_secs(gap)-datetime.timedelta(days=length-1)))

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
        print("Pulling data from {} 00:00:00 to {} 01:00:00".format(date_from, date_to))
        prices = ig.fetch_historical_prices_by_epic_and_date_range(epic, 'D', date_to + ' 00:00:00',
                                                                   date_from + ' 01:00:00')
        ohlcs = prices['prices']
        print("{}".format(prices))
        allowance = prices['allowance']['remainingAllowance']
        restart = prices['allowance']['allowanceExpiry']
        reset_on = datetime.datetime.now() + datetime.timedelta(seconds=restart)
        resets = reset_on.strftime("%d/%m/%Y %H:%M:%S")
        print("Allowance remaining: {} - resets at: {}".format(allowance,resets))
        from_date = from_ymd(date_from)
        to_date = from_ymd(date_to)
        index = from_date.strftime('%Y:%m:%d-00:00:00')
        while from_date >= to_date:
            secs = to_secs(from_date)
            if ohlc_data.find({'epic': epic, 'date': secs}).count() == 0:
                if index in ohlcs.index:
                    ohlc = ohlcs.loc[index]
                    print("Inserting price data on {}({})".format(from_date, secs))
                    ohlc_data.insert_one({
                        'date': secs,
                        'epic': epic,
                        'bid': {
                            'Open': ohlc[('bid', 'Open')],
                            'High': ohlc[('bid', 'High')],
                            'Low': ohlc[('bid', 'Low')],
                            'Close': ohlc[('bid', 'Close')]
                        },
                        'ask': {
                            'Open': ohlc[('ask', 'Open')],
                            'High': ohlc[('ask', 'High')],
                            'Low': ohlc[('ask', 'Low')],
                            'Close': ohlc[('ask', 'Close')]
                        },
                        'volume': ohlc[('last', 'Volume')]
                    })
                else:
                    print("Inserting zero data on {}({})".format(from_date, secs))
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
                print("Not inserting data on {}({})".format(to_ymd(from_date),secs))
            from_date -= datetime.timedelta(days=1)
            index = from_date.strftime('%Y:%m:%d-00:00:00')
    except Exception as e:
        x = e.args[0]
        if type(e) == exceptions.Exception and e.args[0] == 'error.public-api.exceeded-account-allowance':
            print(e.args[0])
            time.sleep(60)
            populate_ohlc(epic, date_to, date_to)
            return True
        elif type(e) == exceptions.Exception and e.args[0] == 'error.security.client-token-invalid':
            print(e.args[0])
            ig = IGService(ig_config.ig_usr, ig_config.ig_pwd, ig_config.ig_key, 'live')
            ig.create_session()
            populate_ohlc(epic, date_to, date_to)
            return True
        elif type(e) == exceptions.Exception and \
                        e.args[0] == 'error.public-api.exceeded-account-historical-data-allowance':
            print(e.args[0])
            return False
        else:
            print("Exception: {}".format(type(e)))
            print("Args: {}".format(e.args))
            print(e)
            return False
