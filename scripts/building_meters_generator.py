#!/usr/bin/python

'''
CREATE AND GENERATE RECORDS FOR TABLE BUILDING_METERS IN THE ENERGY DATABASE
'''

import csv
import psycopg2

def getpass():
    password = input("Enter your password")
    return password

def export_to_database(file_name):
    
    commands = (
        '''DROP TABLE IF EXISTS BUILDING_METERS''',
        '''CREATE TABLE BUILDING_METERS (
            building_id INTEGER,
            meter_id INTEGER,
            FOREIGN KEY (building_id) REFERENCES BUILDINGS(building_id),
            FOREIGN KEY (meter_id) REFERENCES METERS(meter_id),
            PRIMARY KEY (building_id, meter_id)
        )'''
    )

    insertion_query = 'INSERT INTO BUILDING_METERS VALUES(%s, %s)'

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
                cur.execute(insertion_query, (row['BUILDING_ID'], row['METER_ID']))


        cur.close()
        conn.commit()

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

def main():
    export_to_database('BUILDING_METERS.csv')


if __name__ == '__main__':
    main()

