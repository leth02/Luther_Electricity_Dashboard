#!/usr/bin/python

import psycopg2
import csv
from datetime import datetime

def dt_obj(d_str):
    return datetime.strptime(d_str, "%m/%d/%Y").date()

def parse_file(fileName, lst):
    with open(fileName, 'r', newline='') as csvfile:
        reader = csv.DictReader(csvfile)

        for row in reader:
            lst.append((
                dt_obj(row['TREND_DATE']),
                row['METER_ID'],
                row['CONSUMPTION'],
                row['CONSUMPTION_GAP'],
                row['PEAK_DEMAND'] if row['PEAK_DEMAND'] != 'NULL' else '-1',
                #row['PEAK_DEMD_DT_TM'] if row['PEAK_DEMD_DT_TM'] != 'NULL' else '-1',
                row['CONSUMPTION_OCC'],
                row['SUM_DAILY_HIGH'],
                row['SUM_DAILY_LOW'],
                row['OCC_MINUTES'],
                row['TOT_HEATING_DEG'],
                row['TOT_COOLING_DEG']
            ))

    lst.sort()
    print('list sorted')

def db(lst):
    commands = (
        """DROP TABLE IF EXISTS METERS_MLY_DATA""",
        """
        CREATE TABLE METERS_MLY_DATA (
            trend_date DATE NOT NULL,
            meter_id INTEGER REFERENCES METERS(meter_id),
            consumption REAL,
            consumption_gap REAL,
            peak_demand REAL,
            consumption_occ REAL,
            sum_daily_high REAL,
            sum_daily_low REAL,
            occ_minutes INTEGER,
            tot_heating_deg REAL,
            tot_cooling_deg REAL,
            PRIMARY KEY (trend_date, meter_id)
        )
        """
    )

    conn = None

    try:
        conn = psycopg2.connect(host="localhost",database="energy", user="thopo", password="SHAMD5*TLS")
        cur = conn.cursor()

        '''create table'''
        for command in commands:
            cur.execute(command)

        insertion_query = 'INSERT INTO METERS_MLY_DATA VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'
        for r in lst:
            cur.execute(insertion_query, r)

        cur.close()

        conn.commit()
    
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

def main():
    lst = []
    parse_file('METERS_MLY_DATA.csv', lst)
    db(lst)


if __name__ == '__main__':
    main()