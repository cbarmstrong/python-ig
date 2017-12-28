#!/usr/bin/python3.6

from ftse_data import FtseData
import datetime
import ig_data
import json
import sys

class stocks():

    ftse_data = FtseData()
    ftse = ftse_data.get_ftse()
    change_dates = ftse_data.get_ftse_changes()
    from pprint import pprint
    # pprint(change_dates)
    # pprint(ftse)
    ftse_changes = ftse_data.match_stock_changes(ftse, change_dates)
    current_date = datetime.date.today().strftime("%Y-%m-%d")
    view = ftse_data.get_constituents_on(current_date, ftse.copy(), ftse_changes)

    prompt = f"{current_date} >>"
    epics = ig_data.get_epics(view)

    def do_print_current_list(self,line):
        print(json.dumps(self.view,indent=2))

    def do_pull_prices_to_date(self,line=""):
        try:
            stop_date=datetime.datetime.strptime(line,"%Y-%m-%d")
        except:
            print(f"Provided date string not parseable: {line}")
            return

        cur_date = ig_data.from_ymd(ig_data.to_ymd(datetime.datetime.today()))
        while cur_date > stop_date:
            view = self.ftse_data.get_constituents_on(ig_data.to_ymd(cur_date), self.ftse.copy(), self.ftse_changes)
            self.epics = ig_data.get_epics(view)
            for e, epic in self.epics.items():
                ig_data.pull_prices(cur_date, epic)
            cur_date-=datetime.timedelta(days=25)

    def do_clean_data(self,line):
        for e,epic in self.epics.items():
            ig_data.clean_data(epic)


if __name__ == '__main__':
    s = stocks()
    s.do_pull_prices_to_date(sys.argv[1])
