import pandas as pd
import sqlite3

path_election = './data/presidentielle-2022-communes-t1.csv'
path_crime = './data/crimes_communes_2022.csv'
path_pauvrete = './data/indic-stat-circonscriptions-legislatives-2022.xlsx'
path_chomage = './data/famille_TAUX-CHOMAGE_20122023.xlsx'
path_circo_composition = './data/circo_composition.xlsx'

df_election = pd.read_csv(path_election, delimiter=",", low_memory=False)
df_securite = pd.read_csv(path_crime, delimiter=";", low_memory=False)
df_pauvrete = pd.read_excel(path_pauvrete, header=7)
df_chomage = pd.read_excel(path_chomage)
df_circo_composition = pd.read_excel(path_circo_composition, header=0, sheet_name="table")


def insert_to_db():

    conn = sqlite3.connect('ODS.sqlite')
    df_chomage.drop('Période', axis=1, inplace=True)
    df_chomage_finales = df_chomage.melt(id_vars=['Libellé', 'idBank', 'Dernière mise à jour'], value_name='Taux')
    df_chomage_finales[['Annee', 'Trimestre']] = df_chomage_finales['variable'].str.split('-', expand=True)
    df_chomage_finales.drop('variable', axis=1, inplace=True)
    df_chomage_finales = df_chomage_finales.rename(columns={'Dernière mise à jour': 'MiseAJour', 'Libellé': 'Libelle'})

    df_election.to_sql("election", conn, if_exists='append', index=False)
    df_securite.to_sql("securite", conn, if_exists='append', index=False)
    df_pauvrete.to_sql("pauvrete", conn, if_exists='append', index=False)
    df_chomage_finales.to_sql("chomage", conn, if_exists='append', index=False)
    df_circo_composition.to_sql("composition", conn, if_exists='append', index=False)
    conn.close()