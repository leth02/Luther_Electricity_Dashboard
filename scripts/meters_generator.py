#!/usr/bin/python

import csv
import psycopg2

def export_to_database(file_name):
    
    commands = (
        '''DROP TABLE IF EXISTS METERS''',
        '''CREATE TABLE METERS (
            meter_id INTEGER PRIMARY KEY,
            meter_name VARCHAR(200),
            meter_desc VARCHAR(200),
            meter_type_id INTEGER,
            is_active BOOLEAN
        )'''
    )

    insertion_query = 'INSERT INTO METERS VALUES(%s, %s, %s, %s, %s)'

    conn = None

    try:
        conn = psycopg2.connect(host="localhost",database="energy", user="thopo", password="SHAMD5*TLS")
        cur = conn.cursor()

        #create table meters
        for command in commands:
            cur.execute(command)
        
        #Add records to the table
        with open(file_name,'r', newline = '') as csvfile:
            reader = csv.DictReader(csvfile)

            for row in reader:
                cur.execute(insertion_query, (row['METER_ID'], row['METER_NAME'], \
                    row['METER_DESC'], row['METER_TYPE_ID'], row['IS_ACTIVE']))


        cur.close()
        conn.commit()

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

def main():
    export_to_database('METERS2.csv')


if __name__ == '__main__':
    main()

