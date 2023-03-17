import time
import sqlite3
import pandas as pd

vystup = open('vystup.txt', 'a')

# SQLite3
try:
    start = time.time()
    con = sqlite3.connect("../test.db")

    cur = con.cursor()
    cur.execute("select sum(spotreba_mwh)/1000 from spotreba where id_smlouvy=1234 and strftime('%m', dates) = \"06\" and strftime('%Y', dates) = \"2022\";")
    spotreba = cur.fetchone()[0]
    con.close()
    konec = time.time()
    print("SQL: Celkova spotreba pro cerven je {} GWh ({} sec)".format(spotreba, round(konec - start,2)))
    vystup.write("SQL: Celkova spotreba pro cerven je {} GWh ({} sec)\n".format(spotreba, round(konec - start,2)))

except Exception as e:
    print("SQL: Chyba pri zpracovani: {}".format(e))

# Pandas

try:
    start = time.time()
    con = sqlite3.connect("../test.db")
    spotreba_df = pd.read_sql_query("select * from spotreba", con)
    spotreba_df["dates"] = pd.to_datetime(spotreba_df["dates"])
    spotreba = spotreba_df[(spotreba_df["dates"].dt.month==6) & (spotreba_df["dates"].dt.year==2022) & (spotreba_df["id_smlouvy"] == 1234)].mode(numeric_only=True).sum()["spotreba_mwh"]/1000
    con.close()
    konec = time.time()
    print("PANDAS: Celkova spotreba pro cerven je {} GWh ({} sec)".format(spotreba, round(konec - start,2)))
    vystup.write("PANDAS: Celkova spotreba pro cerven je {} GWh ({} sec)\n".format(spotreba, round(konec - start,2)))

except Exception as e:
    print("PANDAS: Chyba pri zpracovani: {}".format(e))

vystup.close()