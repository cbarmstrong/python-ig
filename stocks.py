#!/usr/bin/python3.6

from ftse_data import FtseData
import datetime
import ig_data
import sys

ftse_data = FtseData()
ftse = ftse_data.get_ftse()
change_dates = ftse_data.get_ftse_changes()
ftse_changes = ftse_data.match_stock_changes(ftse, change_dates)
current_date = datetime.date.today().strftime("%Y-%m-%d")
view = ftse_data.get_constituents_on(current_date, ftse.copy(), ftse_changes)
epics = ig_data.get_epics(view)

def pull_prices_to_date(date=""):
    try:
        stop_date = datetime.datetime.strptime(date, "%Y-%m-%d")
    except:
        print(f"Provided date string not parseable: {date}")
        return

    cur_date = ig_data.from_ymd(ig_data.to_ymd(datetime.datetime.today()))
    while cur_date > stop_date:
        view = ftse_data.get_constituents_on(ig_data.to_ymd(cur_date), ftse.copy(), ftse_changes)
        epics = ig_data.get_epics(view)
        for e, epic in epics.items():
            ig_data.pull_prices(cur_date, epic)
        cur_date -= datetime.timedelta(days=1)


def clean_data():
    for e, epic in epics.items():
        ig_data.clean_data(epic)


if __name__ == '__main__':
    # clean_data()
    pull_prices_to_date(sys.argv[1])
