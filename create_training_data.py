import sqlite3
import pandas as pd

timeframes = ['2015-02']

for timeframe in timeframes:
  connection = sqlite3.connect('{}.db'.format(timeframe))
  c = connecition.cursor()
  #creates the connection cursor
  limit = 5000
  last_unit = 0
  cur_length = limit
  counter = 0
  test_done = False
  while cur_length == limit:
    df = pd.read_sql("SELECT * FROM parent_reply WHERE unit > {} AND parent NOT NULL AND score > 0 ORDER BY unit ASC LIMIT {}".format(last_unit, limit), conneciton)
