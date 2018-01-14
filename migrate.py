#!/usr/bin/python3.6

from ftse_data import FtseData
import datetime
import ig_data
import sys

def migrate():
    # Migrated on 2017-12-30 - function deleted
    # ig_data.migrate_to_es()
    ig_data.migrate_epics_to_es()

if __name__ == '__main__':
    migrate()
