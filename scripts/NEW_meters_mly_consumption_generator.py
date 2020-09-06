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
        record['month'],
        record['year'],
        record['METER_ID'],
        record['BUILDING_GROUP'],
        record['CONSUMPTION']
      ))

  object_list.sort()
  print("List of tuples of meters' daily consumption data generated and sorted!")


def export_to_database(object_list):

  commands = (
    """
    DROP TABLE IF EXISTS METER_MONTHLY
    """
    ,
    """
    CREATE TABLE METER_MONTHLY (
      month INTEGER NOT NULL,
      year INTEGER NOT NULL,
      meter_id INTEGER REFERENCES METERS(meter_id),
      building_group VARCHAR(30), 
      consumption REAL,
      PRIMARY KEY (month, year, meter_id)
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
    insertion_query = 'INSERT INTO METER_MONTHLY VALUES (%s, %s, %s, %s, %s)'

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
  meter_monthly_consumption_list = []
  parse_file_to_list('vertical_monthly_data.csv', meter_monthly_consumption_list)
  export_to_database(meter_monthly_consumption_list)


if __name__ == '__main__':
  main()
