import sqlite3
import json
from datetime import datetime

timeframe = '2015-05'
sql_transaction = []

connection = sqlite3.connect('{}.db'.format(timeframe))
c = connection.cursor()

def create_table():
  c.execute("""CREATE TABLE IF NOT EXISTS parent_reply(
      parent_id TEXT PRIMARY KEY, comment.id TEXT UNIQUE,
      parent TEXT, comment TEXT, subreddit TEXT, unit INT, score INT
      )""")

if __name__ == "__main__":
  create_table()
