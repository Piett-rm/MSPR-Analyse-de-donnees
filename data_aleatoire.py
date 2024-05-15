import sqlite3
import random

import pandas as pd


def data_aleatoire():
    conn = sqlite3.connect('DW_all_data.sqlite')
    cur = conn.cursor()
    index_ville =set()
    dfs = []
    df = pd.read_sql_query("SELECT * FROM {}".format("DW"), conn)
    df.to_csv("all_donnees.csv", index=False)

    cur.execute("SELECT DISTINCT libelle_commune from DW")
    villes = cur.fetchall()

    while len(index_ville) < 350:
        random_row_number = random.randint(0, len(villes) - 1)
        index_ville.add(random_row_number)

    for index in index_ville:
        ville = villes[index][0]
        query = "SELECT * FROM DW WHERE libelle_commune=?"
        df_dw = pd.read_sql_query(query, conn, params=(ville,))
        dfs.append(df_dw)

    # df.sample(n=300)

    df = pd.concat(dfs, ignore_index=True)
    chemin_fichier_excel = "donnees_aleatoires.csv"
    df.to_csv(chemin_fichier_excel, index=False)
