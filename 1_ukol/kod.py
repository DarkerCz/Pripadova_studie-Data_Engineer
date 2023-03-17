import shutil
import time
import sqlite3
import pandas as pd

shutil.copy('../test.db', 'db_1_ukol_sqlite.db')

# SQLite3
try:
    start = time.time()
    con = sqlite3.connect("db_1_ukol_sqlite.db")

    cur = con.cursor()
    cur.execute("UPDATE smlouvy set platnost_do = DATE(substr(platnost_do, 0,5) || '-' || substr(platnost_do, 9,2) || '-' || substr(platnost_do, 6,2));")
    con.commit()
    con.close()
    konec = time.time()
    print("SQL: Pocet zmenenych radku: {} ({} sec)".format(cur.rowcount, round(konec - start,2)))

except Exception as e:
    print("SQL: Chyba pri zpracovani: {}".format(e))


shutil.copy('../test.db', 'db_1_ukol_pandas.db')

# Pandas
try:
    start = time.time()
    con = sqlite3.connect("db_1_ukol_pandas.db")
    smlouvy_df = pd.read_sql_query("select * from smlouvy", con)
    smlouvy_df["platnost_do"] = pd.to_datetime(smlouvy_df["platnost_do"], format='%Y-%d-%m').dt.strftime('%Y-%m-%d')
    smlouvy_df.to_sql(name='smlouvy', con=con, if_exists='replace')
    con.close()
    konec = time.time()
    print("PANDAS: Pocet zmenenych radku: {} ({} sec)".format(len(smlouvy_df.axes[0]), round(konec - start,2)))

except Exception as e:
    print("PANDAS: Chyba pri zpracovani: {}".format(e))
