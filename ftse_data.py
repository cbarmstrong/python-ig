import datetime
import PyPDF2
import re
import urllib3
from elasticsearch import Elasticsearch

class FtseData:
    http = urllib3.PoolManager()

    def add_new_line(self, match):
        return "\n" + match.group(0)

    def get_ftse(self):
        known_substitutions = [
            [r'^\s+', ''],
            [r'\s+$', ''],
        ]

        ftse100_file = '/tmp/ftse100.pdf'
        response = \
            self.http.request('GET',
                              'http://www.ftse.com/analytics/factsheets/'
                              'Home/DownloadConstituentsWeights/'
                              '?indexdetails=UKX')
        with open(ftse100_file, 'wb') as f:
            f.write(response.data)

        with open(ftse100_file, 'rb') as f:
            r = PyPDF2.PdfFileReader(f)

            if r.isEncrypted:
                r.decrypt('')
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
                    # print(f"'{stock}': '{weight}'")
                    if float(weight) != 0.0:
                        ftse[stock] = weight

            print("Returning {} stocks".format(len(ftse)))
        return ftse

    def get_ftse_changes(self):
        es = Elasticsearch()
        res = es.search(index='ftse_changes', doc_type='change', body={ 'size': 1000 })
        changes = {}
        for change in res['hits']['hits']:
            dt = datetime.datetime.strptime(change['_source']['date'], '%d-%m-%Y').strftime('%s')
            if dt not in changes.keys():
                changes[dt] = []
            changes[dt].append(change['_source'])
        return changes

    def match_stock_changes(self, ftse, change_dates):
        print("Attempting to extract stock identification")
        return change_dates

    def process_add_del(self, ftse, ftse_changes, date, a, d):
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

    def clean_data(self, stock, subs, debug=False):
        if debug:
            print(u"Transitioned from '{}' ".format(stock), end='')
        for sub in subs:
            stock = re.sub(sub[0], sub[1], stock)
        if debug:
            print("to '{}'".format(stock))
        return stock

    def get_constituents_on(self, target_date, ftse, ftse_changes):
        target_date = datetime.datetime.strptime(target_date, '%Y-%m-%d')
        dates = list(ftse_changes.keys())
        dates.sort(reverse=True)
        for date in dates:
            change_date = datetime.datetime.utcfromtimestamp(float(date))
            print(date)
            print(ftse_changes[date])
            if change_date < target_date:
                break
            for change in ftse_changes[date]:
                print("Removed {}, Added {} on {}".format(change['add'], change['del'], date))
                del ftse[change['add']]
                ftse[change['del']] = 1
        return ftse
