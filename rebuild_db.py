#!/usr/bin/python
"""
n1mm_view rebuild_view_db
This program reads an active N1MM+ contest database and uses it to rebuild
the n1mm_view database tables. 

CAVEAT EMPTOR: This program was written *the day before* Field Day 2017 to provide
a recovery mechanism for collector.py or network failures.  It can build a new
n1mm_view DB from a live N1MM+ contest database while the contest is running.
It is last minute and largely untested.  Use at your own risk!!

USAGE:

1. Set the N1MM_LOG_FILE_NAME value in the n1mm_view_config.py file to point to
   your actual N1MM+ database file.  For example:
   N1MM_LOG_FILE_NAME = '/Users/Field Day/Documents/N1MM Logger+/Databases/MYDB.s3db'

2. Rename your existing DATABASE_FILENAME file.  This file is usually called
   n1mm_view.db  

3. Run the rebuild_db.py program.

4. Restart the collector.py program.

NOTE: the sqlite3 dll that ships with windows python won't read the N1MM+ log file.
You must get the latest sqlite3 dll from https://www.sqlite.org/download.html and
replace the version in your python dlls folder.  I've not tried this on Linux.
Make sure your download the version for the same architecture as your python
installation (32- or. 64-bit.)

Based on replayer.py and collector.py programs written by Jeffrey B. Otterson, N1KDO.
"""

import calendar
import logging
import sqlite3
import time

from n1mm_view_constants import *
from n1mm_view_config import *

__author__ = 'Sheldon Hartling, VE1GPY'
__copyright__ = 'Copyright 2016 Sheldon Hartling'
__license__ = 'Simplified BSD'


logging.basicConfig(format='%(asctime)s.%(msecs)03d %(levelname)-8s %(message)s', datefmt='%Y-%m-%d %H:%M:%S',
                    level=LOG_LEVEL)
logging.Formatter.converter = time.gmtime


class Operators:
    operators = {}
    db = None
    cursor = None

    def __init__(self, db, cursor):
        self.db = db
        self.cursor = cursor
        # load operators
        self.cursor.execute('SELECT id, name FROM operator;')
        for row in self.cursor:
            self.operators[row[1]] = row[0]

    def lookup_operator_id(self, operator):
        """
        lookup the operator id for the supplied operator text.
        if the operator is not found, create it.
        """
        oid = self.operators.get(operator)
        if oid is None:
            self.cursor.execute("insert into operator (name) values (?);", (operator,))
            self.db.commit()
            oid = self.cursor.lastrowid
            self.operators[operator] = oid
        return oid


class Stations:
    stations = {}
    db = None
    cursor = None

    def __init__(self, db, cursor):
        self.db = db
        self.cursor = cursor
        self.cursor.execute('SELECT id, name FROM station;')
        for row in self.cursor:
            self.stations[row[1]] = row[0]

    def lookup_station_id(self, station):
        sid = self.stations.get(station)
        if sid is None:
            self.cursor.execute("insert into station (name) values (?);", (station,))
            self.db.commit()
            sid = self.cursor.lastrowid
            self.stations[station] = sid
        return sid


def create_tables(db, cursor):
    """
    set up the n1mm_view database tables
    """
    cursor.execute('CREATE TABLE IF NOT EXISTS operator\n'
                   '    (id INTEGER PRIMARY KEY NOT NULL, \n'
                   '    name char(12) NOT NULL);')
    cursor.execute('CREATE INDEX IF NOT EXISTS operator_name ON operator(name);')

    cursor.execute('CREATE TABLE IF NOT EXISTS station\n'
                   '    (id INTEGER PRIMARY KEY NOT NULL, \n'
                   '    name char(12) NOT NULL);')
    cursor.execute('CREATE INDEX IF NOT EXISTS station_name ON station(name);')

    cursor.execute('CREATE TABLE IF NOT EXISTS qso_log\n'
                   #'    (id INTEGER PRIMARY KEY NOT NULL,\n'
                   '     (timestamp INTEGER NOT NULL,\n'
                   '     mycall char(12) NOT NULL,\n'
                   '     band_id INTEGER NOT NULL,\n'
                   '     mode_id INTEGER NOT NULL,\n'
                   '     operator_id INTEGER NOT NULL,\n'
                   '     station_id INTEGER NOT NULL,\n'
                   '     rx_freq INTEGER NOT NULL,\n'
                   '     tx_freq INTEGER NOT NULL,\n'
                   '     callsign char(12) NOT NULL,\n'
                   '     rst_sent char(3),\n'
                   '     rst_recv char(3),\n'
                   '     exchange char(4),\n'
                   '     section char(4),\n'
                   '     comment TEXT);')
    cursor.execute('CREATE INDEX IF NOT EXISTS qso_log_band_id ON qso_log(band_id);')
    cursor.execute('CREATE INDEX IF NOT EXISTS qso_log_mode_id ON qso_log(mode_id);')
    cursor.execute('CREATE INDEX IF NOT EXISTS qso_log_operator_id ON qso_log(operator_id);')
    cursor.execute('CREATE INDEX IF NOT EXISTS qso_log_station_id ON qso_log(station_id);')
    cursor.execute('CREATE INDEX IF NOT EXISTS qso_log_section ON qso_log(section);')
    db.commit()


def convert_timestamp(s):
    """
    convert the N1MM+ timestamp into a python time object.
    """
    return time.strptime(s, '%Y-%m-%d %H:%M:%S')


def convert_band(band):
    if band == 1.8:
        return '1.8'
    elif band == 3.5:
        return '3.5'
    else:
        return '%d' % band


def record_contact(db, cursor, operators, stations,
                   timestamp, mycall, band, mode, operator, station,
                   rx_freq, tx_freq, callsign, rst_sent, rst_recv,
                   exchange, section, comment):

    #record the results of a contact_message

    band_id = Bands.get_band_number(band)
    mode_id = Modes.get_mode_number(mode)
    operator_id = operators.lookup_operator_id(operator)
    station_id = stations.lookup_station_id(station)

    logging.info('QSO: %s %6s %4s %-6s %-12s %-12s %10d %10d %-6s %3s %3s %3s %-3s %-3s' % (
        time.strftime('%Y-%m-%d %H:%M:%S', timestamp),
        mycall, band,
        mode, operator,
        station, rx_freq, tx_freq, callsign, rst_sent,
        rst_recv, exchange, section, comment))

    cursor.execute(
        'insert into qso_log \n'
        '    (timestamp, mycall, band_id, mode_id, operator_id, station_id , rx_freq, tx_freq, \n'
        '     callsign, rst_sent, rst_recv, exchange, section, comment)\n'
        '    values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
        (calendar.timegm(timestamp), mycall, band_id, mode_id, operator_id, station_id, rx_freq, tx_freq,
         callsign, rst_sent, rst_recv, exchange, section, comment))

    db.commit()


def main():
    logging.info('Database rebuild started...')
    
    # Open the actual N1MM+ contest database, (we're just looking).

    n1mm_db = sqlite3.connect(N1MM_LOG_FILE_NAME)
    n1mm_cursor = n1mm_db.cursor()
    
    # Create n1mm_view database.

    view_db = sqlite3.connect(DATABASE_FILENAME)
    view_cursor = view_db.cursor()
    create_tables(view_db, view_cursor)
    
    # Instantiate 'operators' and 'stations' objects
    operators = Operators(view_db, view_cursor)
    stations = Stations(view_db, view_cursor)

    # Rebuild the n1mm_view database from the N1MM+ database.

    # Read all of the "FD" contest entries in the DXLOG table.
    n1mm_cursor.execute('SELECT TS, StationPrefix, band, Mode, Operator, NetBiosName, Freq, QSXFreq, Call, \n'
                        'SNT, RCV, Exchange1, Sect, Comment \n'
                        'FROM DXLOG WHERE ContestName=\'FD\' order by TS;')
    qso_number = 0
    for row in n1mm_cursor:
        timestamp = convert_timestamp(row[0])
        mycall = row[1]
        band = convert_band(row[2])
        mode = row[3] 
        operator = row[4]
        station = row[5]
        rx_freq = row[6] * 100
        tx_freq = row[7] * 100
        callsign = row[8]
        rst_sent = row[9]
        rst_recv = row[10]
        exchange = row[11]
        section = row[12]
        comment = row[13]

        record_contact(view_db, view_cursor, operators, stations,
                       timestamp, mycall, band, mode, operator, station,
                       rx_freq, tx_freq, callsign, rst_sent, rst_recv,
                       exchange, section, comment)
        qso_number += 1

    # Close databases and exit. 
    n1mm_db.close()
    view_db.close()

    logging.info("Database rebuild finished... %d QSOs added." % qso_number)


if __name__ == '__main__':
    main()
