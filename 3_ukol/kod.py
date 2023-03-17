import csv
import time
import sqlite3
import pandas as pd

soubor_nazev = 'sql_vysledek.csv'
f = open(soubor_nazev, 'a', newline='')
writer = csv.writer(f)
writer.writerow(('id_smlouvy', 'celkova_spotreba', 'cena_za_mwh', 'mena', 'celkova_cena v CZK'))
# SQLite3
try:
    start = time.time()
    con = sqlite3.connect("../test.db")

    cur = con.cursor()
    cur.execute(
        "select smlouvy.id as \"id_smlouvy\", \
            sum(spotreba.spotreba_mwh) as \"celkova_spotreba\", \
            smlouvy.cena_za_mwh as \"cena_za_mwh\", \
            smlouvy.mena as \"mena\",\
            (CASE \
                WHEN smlouvy.mena = 'CZK' \
                    THEN smlouvy.cena_za_mwh*sum(spotreba.spotreba_mwh) \
                    ELSE (smlouvy.cena_za_mwh/25)*sum(spotreba.spotreba_mwh)\
            END) as \"celkova_cena\"\
        from smlouvy \
        LEFT JOIN  spotreba on smlouvy.id = spotreba.id_smlouvy \
        group by id_smlouvy;")
    rows = cur.fetchall()
    for row in rows:
        writer.writerow(row)
    con.close()
    f.close()
    konec = time.time()
    print("SQL: Vyexportovano do souboru: {} ({} sec)".format(soubor_nazev, round(konec - start,2)))

except Exception as e:
    print("SQL: Chyba pri zpracovani: {}".format(e))


# Pandas
soubor_nazev = 'pandas_vysledek.csv'
f = open(soubor_nazev, 'a', newline='')
writer = csv.writer(f)
writer.writerow(('id_smlouvy', 'celkova_spotreba', 'cena_za_mwh', 'mena', 'celkova_cena v CZK'))
try:
    start = time.time()
    con = sqlite3.connect("../test.db")
    spotreba_df = pd.read_sql_query("select * from spotreba", con)
    smlouvy_df = pd.read_sql_query("select * from smlouvy", con)
    spotreby = dict(spotreba_df.groupby('id_smlouvy')['spotreba_mwh'].sum())
    for smlouva in smlouvy_df.iterrows():
        kurz = 1 if smlouva[1]['mena'] == 'CZK' else 25
        writer.writerow((smlouva[1]['id'], spotreby[smlouva[1]['id']], smlouva[1]['cena_za_mwh'], smlouva[1]['mena'], spotreby[smlouva[1]['id']]*smlouva[1]['cena_za_mwh']/kurz))
    con.close()
    f.close()
    konec = time.time()
    print("PANDAS: Vyexportovano do souboru: {} ({} sec)".format(soubor_nazev, round(konec - start,2)))
except Exception as e:
    print("PANDAS: Chyba pri zpracovani: {}".format(e))
