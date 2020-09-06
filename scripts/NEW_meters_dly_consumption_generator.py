#!/usr/bin/python

import psycopg2
import csv
from datetime import datetime

def datetime_object_generator(datetime_string):
  return datetime.strptime(datetime_string, "%Y-%m-%d").date()

def parse_file_to_list(file_name, object_list):
  with open(file_name, 'r', newline='') as csvfile:
    reader = csv.DictReader(csvfile)

    for record in reader:
      object_list.append((
        datetime_object_generator(record['TREND_DATE']),
        record['METER_ID'],
        record['year'],
        record['month'],
        record['day'],
        record['weekday'],
        record['Fiscal_Year'],
        record['Session_Name'],
        record['Session'],
        record['Session_Type'],
        record['Term'],
        record['METER_CODE'],
        record['CONSUMPTION'],
        record['METER_NAME'],
        record['BUILDING_GROUP']
      ))

  object_list.sort()
  print("List of tuples of meters' daily consumption data generated and sorted!")


def export_to_database(object_list):

  commands = (
    """
    DROP TABLE IF EXISTS METER_DAILY
    """
    ,
    """
    CREATE TABLE METER_DAILY (
      trend_date DATE NOT NULL,
      meter_id INTEGER REFERENCES METERS(meter_id),
      year INTEGER NOT NULL,
      month INTEGER NOT NULL,
      day INTEGER NOT NULL,
      weekday VARCHAR(10),
      fiscal_year VARCHAR(20),
      session_name VARCHAR(20),
      session VARCHAR(5),
      session_type VARCHAR(20),
      term VARCHAR(20),
      meter_code VARCHAR(10),
      consumption REAL,
      meter_name VARCHAR(50),
      building_group VARCHAR(30),
      PRIMARY KEY (trend_date, meter_id)
    )
    """
  )

  connection = None
  try:
    connection = psycopg2.connect(host="localhost", database="energy", user="thopo", password="SHAMD5*TLS")
    cursor = connection.cursor()

    # create the table
    for command in commands:
      cursor.execute(command)

    # insert records from object list to the table
    insertion_query = 'INSERT INTO METER_DAILY VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'

    for record in object_list:
      cursor.execute(insertion_query, record)

    cursor.close()

    connection.commit()

  except (Exception, psycopg2.DatabaseError) as error:
    print(error)
  finally:
    if connection is not None:
      connection.close()

def main():
  meter_daily_consumption_list = []
  parse_file_to_list('vertical_data.csv', meter_daily_consumption_list)
  export_to_database(meter_daily_consumption_list)


if __name__ == '__main__':
  main()
