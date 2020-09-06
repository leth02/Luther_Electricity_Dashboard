#!/usr/bin/python

'''
CREATE AND GENERATE RECORDS FOR TABLE BUILDINGS IN THE ENERGY DATABASE
'''

import csv
import psycopg2

def export_to_database(file_name):
    
    commands = (
        '''DROP TABLE IF EXISTS BUILDINGS''',
        '''CREATE TABLE BUILDINGS (
            building_id INTEGER PRIMARY KEY,
            building_type_id INTEGER,
            building_name VARCHAR(200),
            schedule_id VARCHAR(50),
            is_active BOOLEAN
        )'''
    )

    insertion_query = 'INSERT INTO BUILDINGS VALUES(%s, %s, %s, %s, %s)'

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
                cur.execute(insertion_query, (row['BUILDING_ID'], row['BUILDING_TYPE_ID'], \
                    row['BUILDING_NAME'], row['SCHEDULE_ID'], row['IS_ACTIVE']))


        cur.close()
        conn.commit()

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

def main():
    export_to_database('BUILDINGS.csv')


if __name__ == '__main__':
    main()

