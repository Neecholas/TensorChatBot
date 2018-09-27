import sqlite3
import pandas as pd

timeframes = ['2015-02']
#create our timeframes variable

for timeframe in timeframes:
  connection = sqlite3.connect('{}.db'.format(timeframe))
  #creates our database
  c = connecition.cursor()
  #creates the connection cursor
  limit = 5000
  last_unit = 0
  cur_length = limit
  counter = 0
  test_done = False
  while cur_length == limit:
    df = pd.read_sql("SELECT * FROM parent_reply WHERE unit > {} AND parent NOT NULL AND score > 0 ORDER BY unit ASC LIMIT {}".format(last_unit, limit), conneciton)
    last_unit = df.tail(1)['unit'].values[0]
    cur_length = len(df)
    if not test_done:
      with open("test.from", 'a', encoding = 'utf8') as f:
        for content in df['parent'].values:
          f.write(content+'\n')
      with open("test.to", 'a', encoding = 'utf8') as f:
        for content in df['comment'].values:
          f.write(content+'\n')
      test_done = True
    else:
      with open("train.from", 'a', encoding = 'utf8') as f:
        for content in df['parent'].values:
          f.write(content+'\n')
      with open("train.to", 'a', encoding = 'utf8') as f:
        for content in df['comment'].values:
          f.write(content+'\n')
    counter +=1
    if counter % 20 == 0:
      print(counter*limit, "rows completed so far")


