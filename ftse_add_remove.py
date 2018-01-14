#!/usr/bin/python3.6

from elasticsearch import Elasticsearch
import sys, datetime

def print_usage():
    print(f"Usage: {sys.argv[0]} <add|del> <dd-mm-yyyy> <add_stock> <del_stock>")
    sys.exit(8)

if len(sys.argv) != 5:
    print_usage()

if sys.argv[1] not in ['add', 'del']:
    print_usage()

try:
    day = datetime.datetime.strptime(sys.argv[2], "%d-%m-%Y")
except:
    print_usage()

ident = "-".join(sys.argv[2:])
doc = { 'date': sys.argv[2], 'add': sys.argv[3], 'del': sys.argv[4] }
es = Elasticsearch()
if sys.argv[1] == "add":
    res = es.index(index='ftse_changes', doc_type='change', id=ident, body=doc)
else:
    res = es.delete(index='ftse_changes', doc_type='change', id=ident)

print(f"{res['result']} record on {sys.argv[2]}: Add {sys.argv[3]}, Del: {sys.argv[4]}")

sys.exit()
