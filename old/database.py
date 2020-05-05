import sqlite3
import json

def read_json(filename):
    filename_ext = filename + '.json'
    with open(filename_ext, 'r') as f:
        dict = json.load(f)
    return dict

#Warning : not protected against SQL injections
def create_table(table,cursor):
    table_db ="""
    CREATE TABLE IF NOT EXISTS %s(
         tstamp INTEGER PRIMARY KEY,
         pair_id TXT,
         open REAL,
         high REAL,
         low REAL,
         close REAL,
         volume REAL
    )
    """%(table)
    cursor.execute(table_db)
    print('table : %s, created with sucess'%(table))

def pair_to_DB(pair_id, list, cursor):
    buffer = []
    print(list)
    for i in list:
        i.append(pair_id)
        buffer.append(i)
    print (buffer)
    cursor.executemany("""INSERT INTO binance(tstamp, open, high, low, close, volume, pair_id) VALUES(?, ?, ?, ?, ?, ?, ?)""", list)
conn = sqlite3.connect('arbot.db')
cursor = conn.cursor()


# cursor.execute("""
# DROP TABLE binance
# """)

table = 'binance'
create_table(table,cursor)
BTCUSDT = read_json('BTCUSDT_1h')
pair_to_DB('BTCUSDT',BTCUSDT,cursor)




#Save arbot database changes
conn.commit()

#Closes arbot database
conn.close()