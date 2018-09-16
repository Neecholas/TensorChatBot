import sqlite3
import json
from datetime import datetime

timeframe = '2015-05'
  # gets the file name and saves it to a variable
sql_transaction = []

connection = sqlite3.connect('{}.db'.format(timeframe))
  #connects the db to the file with timeframe.db as it's name
c = connection.cursor()
  #allows the database to have SQL commands executed on it directly

def create_table():
  c.execute("""CREATE TABLE IF NOT EXISTS parent_reply(
    parent_id TEXT PRIMARY KEY, comment.id TEXT UNIQUE,
    parent TEXT, comment TEXT, subreddit TEXT, unit INT, score INT
    )""")
  #creates a table and sets the variables within and their data type

if __name__ == "__main__":
  create_table()
