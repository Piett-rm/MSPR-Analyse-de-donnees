import sqlite3
import random

import pandas as pd


def data_aleatoire():
    conn = sqlite3.connect('DW_all_data.sqlite')
    cur = conn.cursor()
    index_ville =set()
    dfs = []
    cur.execute("SELECT DISTINCT libelle_commune from DW")
    villes = cur.fetchall()

    # print(resultat)
    while len(index_ville) < 2:
        random_row_number = random.randint(0, len(villes) - 1)
        index_ville.add(random_row_number)

    # #
    for index in index_ville:
        print(index)
        df_dw = pd.read_sql_query(f"SELECT * FROM DW WHERE libelle_commune='{str(villes[index][0])}'", conn)
        dfs.append(df_dw)


    df = pd.concat(dfs, ignore_index=True)
    chemin_fichier_excel = "donnees.csv"
    df.to_csv(chemin_fichier_excel, index=False)
