#!/usr/bin/python

import psycopg2
import csv
from datetime import datetime

def dt_obj(d_str):
    return datetime.strptime(d_str, "%Y-%m-%d").date()

def parse_file_to_list(file_name, the_list):
    with open(file_name, 'r', newline='') as csvfile:
        reader = csv.DictReader(csvfile)

        for row in reader:

            the_list.append((dt_obj(row['TREND_DATE']), row['METER_ID'], row['YEAR_VAL'], \
                row['MONTH_OF_YEAR'], row['DAY_NO'], row['CONSUMPTION'], row['CONSUMPTION_GAP'], \
                row['PEAK_DEMAND'] if row['PEAK_DEMAND'] != 'NULL' else '-1', \
                row['PEAK_DEMD_DT_TM'] if row['PEAK_DEMD_DT_TM'] != 'NULL' else '-1', 
                row['CONSUMPTION_OCC'], row['DAILY_HIGH_TEMP'], row['DAILY_LOW_TEMP'], row['OCC_MINUTES'] ))

        print("successfully inserting records to list!!!")    
        
    the_list.sort()
    print("list sorted")

def export_to_db(lst):

    commands = (
        """
        DROP TABLE IF EXISTS METERS_DLY_DATA
        """,
        """
        CREATE TABLE METERS_DLY_DATA (
            trend_date DATE NOT NULL,
            meter_id INTEGER REFERENCES METERS(meter_id),
            year_val INTEGER NOT NULL,
            month_of_year INTEGER NOT NULL,
            day_no INTEGER NOT NULL,
            consumption REAL,
            consumption_gap REAL,
            peak_demand REAL,
            peak_demd_dt_tm REAL,
            consumption_occ REAL,
            daily_hight_temp REAL,
            daily_low_temp REAL,
            occ_minutes INTEGER,
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

        insertion_query = 'INSERT INTO METERS_DLY_DATA VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'
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
    daily = []
    parse_file_to_list('METERS_DLY_DATA.csv', daily)

    export_to_db(daily)

    


if __name__ == '__main__':
    main()
