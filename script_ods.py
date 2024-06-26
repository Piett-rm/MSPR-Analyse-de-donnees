import pandas as pd
import sqlite3

def securite_annee(annee: str):
    """
        Parcourt et remet en forme le fichier sur les données crimes
    """
    path = './data/donnee-data.gouv-2023-geographie2023-produit-le2024-03-07.csv'
    df = pd.read_csv(path, delimiter=";", low_memory=False)

    df = df.query('annee == 23').query('valeur_publiée == "diff"').reset_index()
    df['date'] = df['annee'].apply(lambda x: f"20{x}-01-01")
    df['tauxpourmille'] = df['tauxpourmille'].apply(lambda x: float(str(x).replace(',', '.')) if x==x else 0)
    df['complementinfoval'] = df['complementinfoval'].apply(lambda x: float(str(x).replace(',', '.')) if x==x else 0)
    df['complementinfotaux'] = df['complementinfotaux'].apply(lambda x: float(str(x).replace(',', '.')) if x==x else 0)

    df = df[['CODGEO_2023', 'date', 'classe', 'unité_de_compte', 'valeur_publiée', 'tauxpourmille', 'complementinfoval', 'complementinfotaux']]
    df = df.rename(columns={"CODGEO_2023": "code_insee", "unité_de_compte": "unite_de_compte", "valeur_publiée": "valeur_publiee"})

    return df
"""
    Chargement en RAM des données
"""
path_election = './data/presidentielle-2022-communes-t1.csv'
path_crime = './data/crimes_communes_2022.csv'
path_pauvrete = './data/indic-stat-circonscriptions-legislatives-2022.xlsx'
path_chomage = './data/famille_TAUX-CHOMAGE_20122023.xlsx'
path_circo_composition = './data/circo_composition.xlsx'

df_election = pd.read_csv(path_election, delimiter=",", low_memory=False)

df_securite = pd.read_csv(path_crime, delimiter=";", low_memory=False)
df_securite_2023 = securite_annee("23")
df_securite = pd.concat([df_securite, df_securite_2023])

df_pauvrete = pd.read_excel(path_pauvrete, header=7)
df_chomage = pd.read_excel(path_chomage)
df_circo_composition = pd.read_excel(path_circo_composition, header=0, sheet_name="table")


def insert_to_db():
    """
        Insersion des données brut dans l'ODS
    """
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