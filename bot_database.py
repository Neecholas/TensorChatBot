import sqlite3
import json
from datetime import datetime

timeframe = '2011-12'
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

def find_existing_score(pid):
  try:
    sql = "SELECT score FROM parent_reply WHERE parent_id = '{}' LIMIT 1".format(pid)
    c.execute(sql)
    result = c.fetchone()
    if result != None:
      return result[0]
    else: return False
  except Exception as e:
    return False

def acceptable(data):
  if len(data.split(' ')) > 50 or len(data) < 1:
    return False
  elif len(data) > 1000:
    return False
  elif data == '[deleted]' or data == '[removed]':
    return False
  else:
    return True

def transaction_bldr(sql):
  global sql_transaction
  sql_transaction.append(sql)
  if len(sql_transaction) > 1000:
    c.execute('BEGIN TRANSACTION')
    for s in sql_transaction:
      try:
        c.execute(s)
      except:
        pass
    connection.commit()
    sql_transaction = []


def sql_insert_replace_comment(commentid, parentid, parent, comment, subreddit, unit, score):
  try:
    sql = """UPDATE parent_reply SET parent_id = ?, comment_id = ?, parent = ?, comment = ?, subreddit = ?, unit = ?, score = ? WHERE parent_id = ?;""".format(parentid, commentid, parent, comment, subreddit, unit, score)
    transaction_bldr(sql)
  except Exception as e:
    print('s-UPDATE insertion',str(e))

def sql_insert_has_parent(commentid, parentid, parent,comment, subreddit, time, score):
  try:
    sql = """INSERT INTO parent_reply (parent_id, comment_id, parent, comment, subreddit, unit, score) VALUES ("{}","{}","{}","{}","{}","{}","{}");""".format(parentid, commentid, parent, comment, subreddit, unit, score)
    transaction_bldr(sql)
  except Exception as e:
    print('s-PARENT insertion', str(e))

def sql_insert_no_parent(commentid, parentid, comment, subreddit, time, score):
  try:
    sql = """INSERT INTO parent_reply () VALUES ("{}", "{}", "{}", "{}", "{}", "{}")""".format(commentid, parentid, comment, subreddit, time, score)
    transaction_bldr(sql)
  except Exception as e:
    print('s-NO PARENT insertion', str(e))


if __name__ == "__main__":
  create_table()
  row_counter = 0
  paired_rows = 0
  with open("/mnt/c/Users/right/Downloads/Chatbot_data/reddit_data/{}/RC_{}".format(timeframe.split('-')[0], timeframe), buffering=1000) as f:
  #opens the file and takes the timeframe by splitting the initial timeframe string
  #and taking the first part (array index 0)
  #then takes the full timeframe and has it as the second variable
    for row in f:
      row_counter += 1
      row = json.loads(row)
      parent_id = row['parent_id']
      comment_id = row['link_id']
      body = format_data(row['body'])
      created_utc = row['created_utc']
      score = row['score']
      subreddit = row['subreddit']
      parent_data = find_parent(parent_id)

      #takes the JSON rows and assigns the important attributes to variables

      if score >= 2:
        if acceptable(body):
          existing_comment_score = find_existing_score(parent_id)
          if existing_comment_score:
            if score > existing_comment_score:
              print('works')
              sql_insert_replace_comment(comment_id, parent_id, parent_data, body, subreddit, created_utc, score)
          else:
            #it gets to here
            if parent_data:
              #does not get here
              sql_insert_has_parent(comment_id, parent_id, parent_data, body, subreddit, created_utc, score)
              paired_rows += 1
            else:
              #gets here
              sql_insert_no_parent(comment_id, parent_id, body, subreddit, created_utc, score)
      if row_counter % 100000 == 0:
        print("Total rows read: {}, paired rows: {}, time: {}".format(row_counter, paired_rows, str(datetime.now())))


