from __future__ import print_function
import datetime
import PyPDF2
import re
import urllib2

class FtseData:

    def add_new_line(self,match):
        return "\n" + match.group(0)

    def get_ftse(self):
        known_substitutions = [
            [r'^\s+', ''],
            [r'\s+$', ''],
            [r'Scottish Mortgage Inv Tst', 'Scottish Mortgage Investment Trust'],
            [r'Carphone Warehouse Group', 'Carphone Warehouse'],
            [r'Rolls-Royce Holdings', 'Rolls Royce'],
            ['London Stock Exchange Group', 'London Stock Exchange'],
            ['HSBC Hldgs', 'HSBC'],
            ['Worldpay Group', 'Worldpay'],
            ['Royal Mail', 'NMC Health'],
            ['Provident Financial', 'Berkeley Group Holdings'],
            [r'Capita Group', 'Capita']
        ]

        ftse100_file = '/tmp/ftse100.pdf'
        f = open(ftse100_file, 'wb')
        response = urllib2.urlopen(
            'http://www.ftse.com/analytics/factsheets/Home/DownloadConstituentsWeights/?indexdetails=UKX')
        f.write(response.read())
        f.close()
        f = open(ftse100_file, 'rb')
        r = PyPDF2.PdfFileReader(f)

        ftse = {}

        print("Extracting FTSE Constituent List")

        for i, page in enumerate(r.pages):

            text = page.extractText()
            text = re.sub(r"\n", "", text)
            text = re.sub(r" UNITED KINGDOM ", "\n", text)
            text = re.sub(r" Country ", "\n", text)
            groups = re.findall(r".* \d\.\d", text)

            for j, group in enumerate(groups):
                group = re.sub(r"\n", "", group)
                extract = re.match(r"^(.*) (\d\.\d)", group)
                stock = extract.group(1)
                stock = self.clean_data(stock, known_substitutions)
                weight = extract.group(2)
                if float(weight) != 0.0:
                    ftse[stock] = weight

            if 'Royal Dutch Shell A' in ftse.keys() and 'Royal Dutch Shell B' in ftse.keys():
                ftse['Royal Dutch Shell A&B'] = float(ftse['Royal Dutch Shell A']) + float(ftse['Royal Dutch Shell B'])
                del ftse['Royal Dutch Shell A']
                del ftse['Royal Dutch Shell B']

        if (len(ftse) != 101):
            print("Warning, found {} stocks".format(len(ftse)))
        else:
            print("Returning {} stocks".format(len(ftse)))

        return ftse


    def get_ftse_changes(self):
        known_substitutions = [
            [u'James\u2122s', 'James\'s'],
            ['Cable and Wireless Worldwide', 'Vodafone Group'],
            ['Home Retail Group', 'Sainsbury (J)'],
            ['Melrose Industries', 'Melrose'],
            ['Worldpay Group', 'Worldpay'],
            ['Essar energy', 'Essar Energy'],
            ['HSBC Hldgs', 'HSBC'],
            ['Corporate Event.*', ''],
            ['Transfer of listing.*', ''],
            ['London Stock Exchange Group', 'London Stock Exchange'],
            ['Fast Entry', ''],
            ['Col Inv Trust', 'Colonial Investment Trust'],
            ['Col Invest Trust', 'Colonial Investment Trust'],
            [' \(Wm\.\) ', ' (Wm) '],
            ['Amec', 'AMEC'],
            ['British Energy Group', 'Centrica'],
            ['Cable & Wireless', 'Vodafone Group'],
            ['Alliance Unichem', 'Alliance Boots'],
            ['Xstrata', 'Glencore'],
            ['Friends Provident', 'Resolution'],
            ['Innogy Hldgs', 'Innogy Holdings'],
            ['GKN PLC', 'GKN'],
            ['Capita Group', 'Capita'],
            ['HBOS', 'Lloyds Banking Group'],
            ['Granada', 'ITV'],
            ['Experian Group', 'Experian'],
            ['\s+$', ''],
            ['^\s+', '']
        ]

        add_del_file = '/tmp/ftse_adds_and_deletes.pdf'
        f = open(add_del_file, 'wb')
        response = urllib2.urlopen('http://www.ftse.com/products/downloads/FTSE_100_Constituent_history.pdf')
        f.write(response.read())
        f.close()
        f = open(add_del_file, 'rb')
        r = PyPDF2.PdfFileReader(f)

        change_dates = {}

        print("Extracting FTSE Add/Remove text")

        all_groups = []

        for i, page in enumerate(r.pages):
            text = re.sub(r"\n", "", page.extractText())
            text = re.sub(r"\d{1,2}-[A-Za-z]{3}-\d{1,2}", self.add_new_line, text)
            groups = re.findall(r"\d{1,2}-[A-Za-z]{3}-\d{1,2}.*", text)

            all_groups.extend(groups)

        all_groups.sort()
        for j, group in enumerate(all_groups):
            group = re.sub(r"\n", "", group)
            group = re.sub(r" - .*", "", group)
            if re.search(r"No Constituent Changes", group):
                continue
            extract = re.match(r"(\d{1,2}-\w{3}-\d{1,2}) (.*)", group)
            date_string = extract.group(1)
            stocks = extract.group(2)
            stocks = self.clean_data(stocks, known_substitutions)
            date = datetime.datetime.strptime(date_string, "%d-%b-%y")
            if date not in change_dates.keys():
                change_dates[date] = []
            change_dates[date].append(stocks)
            #           print("Extracted on {}: {}".format(date,stocks))

        return change_dates


    def match_stock_changes(self,ftse, change_dates):
        print("Attempting to extract stock identification")

        ftse_changes = {}
        dates = change_dates.keys()
        dates.sort(reverse=True)
        ftse = ftse.copy()

        for date in dates:
            stock_list = change_dates[date]
            if date.year < 2005: continue
            #       print("Processing changes on {}".format(date))
            for stock_change in stock_list:

                #            print(" - {}".format(stock_change))
                matched = False
                stocks = ftse.keys()
                stocks.sort()

                for stock in stocks:
                    search_stock = re.escape(stock)

                    # print("Searching for {} in {}".format(search_stock,stock_change))

                    guess = re.search(r"{} +(.+)".format(search_stock), stock_change)
                    if guess is not None:
                        to_add = stock
                        to_del = guess.group(1)
                        self.process_add_del(ftse, ftse_changes, date, to_add, to_del)
                        # print("Matched first  '{}' with '{}'".format(stock_change,search_stock))
                        matched = True
                        break

                    guess = re.search(r"(.+) +{}".format(search_stock), stock_change)
                    if guess is not None:
                        to_add = guess.group(1)
                        to_del = stock
                        self.process_add_del(ftse, ftse_changes, date, to_add, to_del)
                        # print("Matched second '{}' with '{}'".format(stock_change,search_stock))
                        matched = True
                        break

                if not matched:
                    poss = ftse.keys()
                    poss.sort()
                    for stock in poss:
                        stock = re.escape(stock)
                        print("Warning - {} unable to find '{}' in '{}'".format(date, stock, stock_change))
                    return
        return ftse_changes


    def process_add_del(self,ftse, ftse_changes, date, a, d):
        known_subs = [
            ['^\s+', ''],
            ['\s+$', '']
        ]

        if date not in ftse_changes.keys():
            ftse_changes[date] = []

        to_add = self.clean_data(a, known_subs)
        to_del = self.clean_data(d, known_subs)

        for x in [to_add, to_del]:
            if x not in ftse.keys():
                ftse[x] = 0

        ftse_changes[date].append({'add': to_add, 'del': to_del})


    def clean_data(self,stock, subs, debug=False):
        if debug:
            print(u"Transitioned from '{}' ".format(stock), end='')
        for sub in subs:
            stock = re.sub(sub[0], sub[1], stock)
        if debug:
            print("to '{}'".format(stock))
        return stock


    def get_constituents_on(self,target_date, ftse, ftse_changes):
        target_date = datetime.datetime.strptime(target_date, '%Y-%m-%d')
        dates = ftse_changes.keys()
        dates.sort(reverse=True)
        for date in dates:
            if date < target_date:
                break
            for change in ftse_changes[date]:
                print("Removed {}, Added {} on {}".format(change['add'], change['del'], date))
                del ftse[change['add']]
                ftse[change['del']] = 1
        return ftse
