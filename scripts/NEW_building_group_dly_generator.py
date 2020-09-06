#!/usr/bin/python

import psycopg2
import csv
from datetime import datetime

def dt_obj(d_str):
    return datetime.strptime(d_str, "%Y-%m-%d").date()

def parse_file(fileName, lst):
    with open(fileName, 'r', newline='') as csvfile:
        reader = csv.DictReader(csvfile)

        for row in reader:
            lst.append((
                dt_obj(row['TREND_DATE']),
                row['BUILDING_GROUP'],
                row['CONSUMPTION'],
                
            ))

    lst.sort()
    print(lst[-5:-1])
    print('list sorted')

def db(lst):
    commands = (
        """DROP TABLE IF EXISTS BUILDING_GROUP_DAILY""",
        """
        CREATE TABLE BUILDING_GROUP_DAILY (
            trend_date DATE NOT NULL,
            building_group VARCHAR(100),
            consumption REAL
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

        insertion_query = 'INSERT INTO BUILDING_GROUP_DAILY VALUES(%s, %s, %s)'
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
    parse_file('vertical_group.csv', lst)
    db(lst)


if __name__ == '__main__':
    main()