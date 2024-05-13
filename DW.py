import pandas as pd
import sqlite3


def jointure_table_pour_DW():
    conn = sqlite3.connect('ODS.sqlite')
    
    df_chomage = pd.read_sql_query("SELECT * FROM {}".format("chomage"), conn)
    df_composition = pd.read_sql_query("SELECT * FROM {}".format("composition"), conn)
    df_election = pd.read_sql_query("SELECT * FROM {}".format("election"), conn)
    df_securite = pd.read_sql_query("SELECT * FROM {}".format("securite"), conn)
    df_pauvrete = pd.read_sql_query("SELECT * FROM {}".format("pauvrete"), conn)
    
    conn.close()
    
    df_election['code_commune'] = df_election['code_commune'].apply(lambda x: str(x).zfill(3))
    df_election['CODGEOGRA'] = df_election['code_departement'].str.cat(df_election['code_commune'], sep='')

    df_pauvrete_new = df_pauvrete.loc[:,['circo','part_pauvres_diff']]
    df_pauvrete_new['part_pauvres_diff'] = pd.to_numeric(df_pauvrete_new['part_pauvres_diff'], errors='coerce')

    df_filtre_chomage_par_departement = df_chomage[df_chomage['Libelle'].str.contains(
        'département - Nord| département - Pas-de-Calais| département - Somme| département - Oise| département - Aisne')].copy()
    df_filtre_chomage_par_departement_annee_2022 =  df_filtre_chomage_par_departement[df_filtre_chomage_par_departement['Annee'].str.contains(
        '2022')].copy()
    df_filtre_chomage_par_departement_annee_2022.loc[:, 'Libelle'] = df_filtre_chomage_par_departement['Libelle'].str.split(' - ').str[-1]
    df_filtre_chomage_par_departement_annee_2022['Taux'] = pd.to_numeric(df_filtre_chomage_par_departement_annee_2022['Taux'])
    df_chomage_taux_moyenne_annee = df_filtre_chomage_par_departement_annee_2022.groupby('Libelle')['Taux'].mean().reset_index()

    df_securite = df_securite[df_securite['date'].str.contains('2021-01-01')]
    df_securite['classe'] = df_securite['classe'].str.replace(r'.*coups.*', 'coups', regex=True)
    df_securite['classe'] = df_securite['classe'].str.replace(r'.*Coups.*', 'coups', regex=True)

    df_securite['classe'] = df_securite['classe'].str.replace(r'.*Vols.*', 'vols', regex=True)
    df_securite['classe'] = df_securite['classe'].str.replace(r'.*vols.*', 'vols', regex=True)

    
    print("election")
    print(df_election.columns.tolist())

    print("pauvrete")
    print(df_pauvrete_new.columns.tolist())

    print("chomage")
    print(df_filtre_chomage_par_departement_annee_2022.columns.tolist())

    print("securite")
    print(df_securite.columns.tolist())

    print("composition")
    print(df_composition.columns.tolist())

    df_commune=df_composition[['circo','LIBcom','COMMUNE_RESID', 'DEP']]
    df_commune.rename(columns={'circo': 'codeINSEE', 'LIBcom': 'libelleCommune','COMMUNE_RESID':'codePostal','DEP':'codeDepartement'}, inplace=True)
    df_commune = pd.merge(df_commune, df_pauvrete_new, left_on='codeINSEE', right_on='circo', how='left')
    df_commune.drop(columns=['circo'], inplace=True)
    df_commune.rename(columns={'part_pauvres_diff': 'part_pauvres'}, inplace=True)

    df_departement=df_composition[['DEP','libdep']].drop_duplicates()
    df_departement.rename(columns={'DEP':'codeDepartement','libdep':'nomDepartement'}, inplace=True)

    df_chomage_dw = df_filtre_chomage_par_departement_annee_2022[['Annee','Trimestre','Taux','Libelle']]
    dep_to_number = pd.Series(df_departement.codeDepartement.values, index=df_departement.nomDepartement).to_dict()
    df_chomage_dw['Libelle'] = df_chomage_dw['Libelle'].map(dep_to_number)
    df_chomage_dw.rename(columns={'Libelle':'codeDepartement'}, inplace=True)

    df_crime = df_securite[['date','tauxpourmille','classe','code_insee']]
    df_crime.rename(columns={'code_insee':'codeINSEE'}, inplace=True)


    print("commune")
    print(df_commune.head(2))
    print("departement")
    print(df_departement.head(2))
    print("chomage")
    print(df_chomage_dw.head(2))
    print("crime")
    print(df_crime.head(2))


    """
    jointure_composition_pauvrete = pd.merge(df_composition,df_pauvrete_new,on="circo",how='inner').drop(columns=["type_com","LIBcom","DEP","REG","libreg"])
    jointure = pd.merge(jointure_composition_pauvrete,df_election,left_on="COMMUNE_RESID", right_on="CODGEOGRA",how='inner').drop(columns=["CODGEOGRA","code_departement","libelle_departement"])
    jointure = pd.merge(jointure,df_securite,left_on="COMMUNE_RESID", right_on="code_insee",how='inner').drop(columns=["COMMUNE_RESID","complementinfoval","complementinfotaux","valeur_publiee"])
    jointure = pd.merge(jointure,df_chomage_taux_moyenne_annee,left_on="libdep",right_on="Libelle",how='inner').drop(columns=["code_insee", "code_commune"])

    conn = sqlite3.connect("DW_all_data.sqlite")

    jointure.to_sql('DW', conn, if_exists='append', index=True, index_label='id')
    """

    """
    df_commune.to_sql('Commune', conn, if_exists='append', index=True, index_label='id')
    """
    conn.close()
