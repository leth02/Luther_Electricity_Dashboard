#!/usr/bin/python

'''
This file transforms records from the csv file into tuples which are stored in a Python list.
Then the list is sorted by Start_date.
Finally, insert each tutple as a new record into the CALENDAR table in the database.
'''

import csv
from datetime import datetime
import psycopg2

'''Return a datetime object under YYYY-MM-DD format given a date string of MM-DD-YYYY format'''
def make_dt_object(date_str):
    return datetime.strptime(date_str, '%m/%d/%Y').date()


def parse_file(lst):
    with open('CALENDAR.csv', 'r', newline = '') as file:
        reader = csv.DictReader(file)
        # for r in reader:
        #     record = (r['Start_date'], r['End_date'])
        for r in reader:
            start = make_dt_object(r['Start_date'])
            end = make_dt_object(r['End_date'])
            session = (start, end, r['Fiscal_Year'], r['Session_Name'], r['Session'], r['Session_Type'], r['Term'])

            lst.append(session)

        '''
        Add sessions starting from summer 2020 until 2021 (tentative, check for any changes)
        '''
        lst.append((make_dt_object('6/1/2020'), make_dt_object('6/26/2020'),'2019-2020', \
            'Summer Session I 2020','Yes','Summer','Summer'))
        lst.append((make_dt_object('6/27/2020'), make_dt_object('6/28/2020'),'2019-2020', \
            'Summer Break I 2020','No','Break','Summer'))
        lst.append((make_dt_object('6/29/2020'), make_dt_object('7/24/2020'),'2019-2020', \
            'Summer Session II 2020','Yes','Summer','Summer'))
        lst.append((make_dt_object('7/25/2020'), make_dt_object('9/1/2020'),'2019-2020', \
            'Summer Break II 2020','No','Break','Summer'))
        lst.append((make_dt_object('9/2/2020'), make_dt_object('9/25/2020'),'2020-2021', \
            'September Semester 2020','Yes','Fall','Fall'))

        lst.sort()


def export_to_database(lst):

    commands = (
        """
        DROP TABLE IF EXISTS CALENDAR
        """
        ,
        """
        CREATE TABLE CALENDAR (
            start_date DATE NOT NULL,
            end_date DATE NOT NULL,
            fiscal_year VARCHAR(10),
            session_name VARCHAR(100),
            in_session BOOLEAN,
            session_type VARCHAR(50),
            term VARCHAR(50),
            PRIMARY KEY(start_date, end_date)
        )
        """
    )

    conn = None

    try:
        conn = psycopg2.connect(host="localhost",database="energy", user="thopo", password="SHAMD5*TLS")
        cur = conn.cursor()

        '''create table calendar'''
        for command in commands:
            cur.execute(command)

        '''Add records to the table'''
        insert_query = "INSERT INTO CALENDAR VALUES (%s, %s, %s, %s, %s, %s, %s)"
        cur.executemany(insert_query, lst)

        cur.close()

        conn.commit()

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

def main():
    session_list = []
    parse_file(session_list)
    export_to_database(session_list)


if __name__ == '__main__':
    main()
