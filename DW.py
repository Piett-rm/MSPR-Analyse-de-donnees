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
    df_securite = df_securite[df_securite['date'].str.contains('2021')]
    df_securite = df_securite.drop(columns=['unite_de_compte','valeur_publiee'])

    df_securite = df_securite.pivot_table(index=['code_insee', 'date'],
                              columns='classe',
                              values='tauxpourmille',
                              aggfunc='first').reset_index()
    df_securite['insecurite'] = (
            df_securite['Autres coups et blessures volontaires'] +
            df_securite['Cambriolages de logement'] +
            df_securite['Coups et blessures volontaires'] +
            df_securite['Coups et blessures volontaires intrafamiliaux'] +
            df_securite['Violences sexuelles'] +
            df_securite['Vols avec armes'] +
            df_securite["Vols d'accessoires sur véhicules"] +
            df_securite['Vols dans les véhicules'] +
            df_securite['Vols de véhicules'] +
            df_securite['Vols sans violence contre des personnes'] +
            df_securite['Vols violents sans arme']
    )

    df_securite = df_securite[['code_insee','insecurite','date']]

    df_election = df_election.drop(columns=["prenom"])
    df_election['code_commune'] = df_election['code_commune'].apply(lambda x: str(x).zfill(3))
    df_election['CODGEOGRA'] = df_election['code_departement'].str.cat(df_election['code_commune'], sep='')

    votes_par_ville = df_election.groupby('libelle_commune')['voix'].sum().reset_index()
    votes_par_ville['voix'] = votes_par_ville['voix'].astype(int)

    bins = [0, 999, 4999, 9999, float('inf')]
    labels = ['Petite ville', 'Petite moyenne ville', 'Grande ville', 'Métropole']

    votes_par_ville['catégorie'] = pd.cut(votes_par_ville['voix'], bins=bins, labels=labels, right=False)

    votes_par_ville.rename(columns={'voix': 'total_voix_par_ville'}, inplace=True)

    valeurs_indesirables = (df_election['libelle_commune'] == 'Besmé') | (df_election['libelle_commune'] == 'Wail')| (df_election['libelle_commune'] == 'Ponchon') | (df_election['libelle_commune'] == "Hodenc-l'Evêque")| (df_election['libelle_commune'] == "Maysel")| (df_election['libelle_commune'] == "Blérancourt")
    df_election = df_election[~valeurs_indesirables]

    df_election = pd.merge(votes_par_ville,df_election, on='libelle_commune')
    df_election['pourcentage_voix'] = (df_election['voix'] / df_election['total_voix_par_ville']) * 100
    df_election.drop(columns=['total_voix_par_ville','voix'])

    df_pauvrete_new = df_pauvrete.loc[:,['circo','part_pauvres_diff','part_modestes_diff','part_medians_diff','part_plutot_aises_diff','part_aises_diff','actemp','actcho','inactret','age_moyen']]
    df_pauvrete_new['part_pauvres_diff'] = pd.to_numeric(df_pauvrete_new['part_pauvres_diff'], errors='coerce')

    df_filtre_chomage_par_departement = df_chomage[df_chomage['Libelle'].str.contains(
        'département - Nord| département - Pas-de-Calais| département - Somme| département - Oise| département - Aisne')].copy()
    df_filtre_chomage_par_departement_annee_2022 =  df_filtre_chomage_par_departement[df_filtre_chomage_par_departement['Annee'].str.contains(
        '2022')].copy()
    df_filtre_chomage_par_departement_annee_2022.loc[:, 'Libelle'] = df_filtre_chomage_par_departement['Libelle'].str.split(' - ').str[-1]
    df_filtre_chomage_par_departement_annee_2022['taux_chomage_moyen'] = pd.to_numeric(df_filtre_chomage_par_departement_annee_2022['Taux'])
    df_chomage_taux_moyenne_annee = df_filtre_chomage_par_departement_annee_2022.groupby('Libelle')['taux_chomage_moyen'].mean().reset_index()

    jointure_composition_pauvrete = pd.merge(df_composition,df_pauvrete_new,on="circo",how='inner').drop(columns=["type_com","LIBcom","DEP","REG","libreg"])
    jointure = pd.merge(jointure_composition_pauvrete,df_election,left_on="COMMUNE_RESID", right_on="CODGEOGRA",how='inner').drop(columns=["CODGEOGRA","code_departement","libelle_departement"])
    jointure = pd.merge(jointure,df_securite,left_on="COMMUNE_RESID", right_on="code_insee",how='inner').drop(columns=["COMMUNE_RESID"])
    jointure = pd.merge(jointure,df_chomage_taux_moyenne_annee,left_on="libdep",right_on="Libelle",how='inner').drop(columns=["code_commune","Libelle","date"])
    conn = sqlite3.connect("DW_all_data.sqlite")

    jointure.to_sql('DW', conn, if_exists='append', index=True, index_label='id')

    conn.close()
