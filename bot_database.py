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
    parent_id TEXT PRIMARY KEY,
    comment_id TEXT UNIQUE,
    parent TEXT,
    comment TEXT,
    subreddit TEXT,
    unit INT,
    score INT
    )""")
  #creates a table and sets the variables within and their data type

def format_data(data):
  data = data.replace("\n"," newlinechar ").replace("\r"," newlinechar ").replace('"', "'")
  #normalises the data and returns it
  return data

def find_parent(pid):
  try:
    sql = "SELECT comment FROM parent_reply WHERE comment_id = '{}' LIMIT 1".format(pid)
    c.execute(sql)
    result = c.fetchone()
    if result != None:
      return result[0]
    else: return False
  except Exception as e:
    #print("find_parent", e)
    return False
    #finds the parent id by getting the id of the parent and seeing if it corresponds to the comment

if __name__ == "__main__":
  create_table()
  row_counter = 0
  paired_rows = 0
  with open("/mnt/c/Users/right/Downloads/Chatbot_data/reddit_data/{}/RC_{}".format(timeframe.split('-')[0], timeframe)) as f:
  #opens the file and takes the timeframe by splitting the initial timeframe string
  #and taking the first part (array index 0)
  #then takes the full timeframe and has it as the second variable
    for row in f:
      row_counter += 1
      row = json.loads(row)
      parent_id = row['parent_id']
      body = format_data(row['body'])
      created_utc = row['created_utc']
      score = row['score']
      subreddit = row['subreddit']
      #takes the JSON rows and assigns the important attributes to variables


