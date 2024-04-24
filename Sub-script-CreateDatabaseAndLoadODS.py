import sqlite3
import pandas as panda
import os
from datetime import datetime


def createDatabase(databasePath, scriptPath):
    connexion = sqlite3.connect(databasePath)
    creationCursor = connexion.cursor()
    with open(scriptPath, 'r') as sql_file:
            sql_script = sql_file.read()
    creationCursor.executescript(sql_script)
    connexion.commit()
    creationCursor.close()
    connexion.close()

if not os.path.exists('ODS.db'):
    createDatabase('ODS.db', './Script SQLite ODS.sql')
if not os.path.exists('DWH.db'):
    createDatabase('DWH.db', './DWH.sql')


today_date = datetime.now()
today_string = today_date.strftime('%Y-%m-%d')
circo = panda.read_excel('./data/circo_composition.xlsx', sheet_name="table")
circo['date_collecte'] = today_string


circo_types = {
    'DEP': 'string',
    'libdep': 'string',
    'REG': 'string',
    'libreg': 'string',
    'COMMUNE_RESID': 'string',
    'LIBcom': 'string',
    'circo': 'string',
    'type_com': 'string',
    'date_collecte': 'string'
}
circo = circo.astype(circo_types)


crime = panda.read_csv('./data/crimes_communes_2022.csv', sep=";")
crime['date_collecte'] = today_string
crime.rename(columns={'date': 'date_crime'}, inplace=True)
crime_types = {
    'code_insee': 'string',
    'date_crime': 'string',
    'classe': 'string',
    'unite_de_compte': 'string',
    'valeur_publiee': 'string', 
    'tauxpourmille': 'float',
    'complementinfoval': 'float',
    'complementinfotaux': 'float',
    'date_collecte': 'string'
}
crime = crime.astype(crime_types)


famille_original = panda.read_excel('./data/famille_TAUX-CHOMAGE_20122023.xlsx', sheet_name="valeurs_trimestrielles")
famille_original['date_collecte'] = today_string
famille_original.rename(columns={'Libellé': 'Libelle', 'Dernière mise à jour': 'Derniere_mise_a_jour'}, inplace=True)
famille = panda.DataFrame(data=None, columns=['idBank', 'Libelle', 'Derniere_mise_a_jour', 'annee', 'timestre', 'taux', 'date_collecte'])

for col in famille_original.columns:
    col_split = col.split('-')
    if len(col_split) == 1:
        continue
    for index, row in famille_original.iterrows():
        new_row = panda.Series({'idBank': row['idBank'], 'Libelle': row['Libelle'], 'Derniere_mise_a_jour': row['Derniere_mise_a_jour'],'annee': col_split[0], 'timestre' : col_split[1], 'taux': row[col], 'date_collecte': row['date_collecte']})
        famille.loc[len(famille)] = new_row

famille_types = {
    'idBank': 'string',
    'Libelle': 'string',
    'Derniere_mise_a_jour': 'string',
    'annee': 'string',
    'timestre': 'string',
    'taux': 'string',
    'date_collecte': 'string'
}
famille = famille.astype(famille_types)

stat = panda.read_excel('./data/indic-stat-circonscriptions-legislatives-2022.xlsx', sheet_name="indicateurs_circonscriptions", skiprows=7, header=0, na_values=['nd'])
stat['date_collecte'] = today_string
stat.rename(columns={'Nom de la circonscription': 'Nom_de_la_circonscription', 'pop_légal_19': 'pop_legal_19', 'pop_légal_13': 'pop_legal_13'}, inplace=True)
stat.replace({'nd': panda.NA}, inplace=True)
stat_types = {
    'circo': 'string',
    'Nom_de_la_circonscription': 'string',
    'Inscrit_22': 'Int64',
    'pop_legal_19': 'Int64',
    'pop_legal_13': 'Int64',
    'tvar_pop': 'float',
    'pop_pole_aav': 'float',
    'pop_cour_aav': 'float',
    'pop_horsaav': 'float',
    'pop_urb': 'float',
    'pop_rur_periu': 'float',
    'pop_rur_non_periu': 'float',
    'age_moyen': 'float',
    'dec90': 'Int64',
    'dec75': 'Int64',
    'dec50': 'Int64',
    'dec25': 'Int64',
    'dec10': 'Int64',
    'actemp': 'float',
    'actcho': 'float',
    'inactret': 'float',
    'inactetu': 'float',
    'inactm14': 'float',
    'inactaut': 'float',
    'actemp_hom': 'float',
    'actcho_hom': 'float',
    'inactret_hom': 'float',
    'inactetu_hom': 'float',
    'inactm14_hom': 'float',
    'inactaut_hom': 'float',
    'actemp_fem': 'float',
    'actcho_fem': 'float',
    'inactret_fem': 'float',
    'inactetu_fem': 'float',
    'inactm14_fem': 'float',
    'inactaut_fem': 'float',
    'actdip_PEU': 'float',
    'actdip_CAP': 'float',
    'actdip_BAC': 'float',
    'actdip_BAC2': 'float',
    'actdip_BAC3': 'float',
    'actdip_BAC5': 'float',
    'actdip_BAC3P': 'float',
    'act_agr': 'float',
    'act_art': 'float',
    'act_cad': 'float',
    'act_int': 'float',
    'act_emp': 'float',
    'act_ouv': 'float',
    'act_cho': 'float',
    'log_res': 'float',
    'log_sec': 'float',
    'log_vac': 'float',
    'proprio': 'float',
    'locatai': 'float',
    'gratuit': 'float',
    'maison': 'float',
    'ach90': 'float',
    'mfuel': 'float',
    'men_seul': 'float',
    'men_coupae': 'float',
    'men_coupse': 'float',
    'men_monop': 'float',
    'men_sfam': 'float',
    'men_seul_com': 'float',
    'men_coupse_com': 'float',
    'men_coupae_com': 'float',
    'men_monop_com': 'float',
    'men_complexe_com': 'float',
    'seul_hom1829': 'float',
    'seul_hom3059': 'float',
    'seul_hom6074': 'float',
    'seul_hom7584': 'float',
    'seul_hom85': 'float',
    'seul_fem1829': 'float',
    'seul_fem3059': 'float',
    'seul_fem6074': 'float',
    'seul_fem7584': 'float',
    'seul_fem85': 'float',
    'iranr_log': 'float',
    'iranr_com': 'float',
    'iranr_dep': 'float',
    'iranr_fra': 'float',
    'iranr_etr': 'float',
    'mobresid': 'float',
    'ilt_com': 'float',
    'ilt_dep': 'float',
    'ilt_fra': 'float',
    'ilt_etr': 'float',
    'mobtrav': 'float',
    'modtrans_aucun': 'float',
    'modtrans_pied': 'float',
    'modtrans_velo': 'float',
    'modtrans_moto': 'float',
    'modtrans_voit': 'float',
    'modtrans_commun': 'float',
    'tx_pauvrete60_diff': 'float',
    'nivvie_median_diff': 'float',
    'part_pauvres_diff': 'float',
    'part_modestes_diff': 'float',
    'part_medians_diff': 'float',
    'part_plutot_aises_diff': 'float',
    'part_aises_diff': 'float',
    'D1_diff': 'Int64',
    'D9_diff': 'Int64',
    'rpt_D9_D1_diff': 'float',
    'tx_pauvrete60_diff_trageRF1': 'float',
    'tx_pauvrete60_diff_trageRF2': 'float',
    'tx_pauvrete60_diff_trageRF3': 'float',
    'tx_pauvrete60_diff_trageRF4': 'float',
    'tx_pauvrete60_diff_trageRF5': 'float',
    'tx_pauvrete60_diff_trageRF6': 'float',
    'PACT': 'float',
    'PPEN': 'float',
    'PPAT': 'float',
    'PPSOC': 'float',
    'PIMPOT': 'float',
    'acc_ecole': 'float',
    'acc_college': 'float',
    'acc_lycee': 'float',
    'acc_medecin': 'float',
    'acc_dentiste': 'float',
    'acc_pharmacie': 'float',
    'part_eloig': 'float',
    'date_collecte': 'string'
}
stat = stat.astype(stat_types)

presidentielleT1 = panda.read_csv('./data/presidentielle-2022-communes-t1.csv')
presidentielleT1['date_collecte'] = today_string
presidentielleT1['date_election'] = '2022'
presidentielleT1_type = {
    'code_departement': 'string',
    'libelle_departement': 'string',
    'code_commune': 'string',
    'libelle_commune': 'string',
    'prenom': 'string',
    'nom': 'string',
    'voix': 'Int64',
    'date_election': 'string',
    'date_collecte': 'string'
}
presidentielleT1 = presidentielleT1.astype(presidentielleT1_type)

presidentielleT2_headers = [
    'Code_du_departement', 'Code_de_la_commune', 'Libelle_du_departement', 'Libelle_de_la_commune',
    'Etat_saisie', 'Inscrits', 'Abstentions', '_Abs_Ins', 'Votants', '_Vot_Ins', 'Blancs',
    '_Blancs_Ins', '_Blancs_Vot', 'Nuls', '_Nuls_Ins', '_Nuls_Vot', 'Exprimes', '_Exp_Ins', '_Exp_Vot',
    'N_Panneau', 'Sexe', 'Nom', 'Prenom', 'Voix', '_Voix_Ins', '_Voix_Exp',
    'N_Panneau_2', 'Sexe_2', 'Nom_2', 'Prenom_2', 'Voix_2', '_Voix_Ins_2', '_Voix_Exp_2'
]
presidentielleT2 = panda.read_excel('./data/resultats-par-niveau-subcom-t2-france-entiere.xlsx', skiprows=1, header=None, names=presidentielleT2_headers)


columns_presidentielleT2_commun = [
    'Code_du_departement', 'Libelle_du_departement', 'Code_de_la_commune', 'Libelle_de_la_commune',
    'Etat_saisie', 'Inscrits', 'Abstentions', '_Abs_Ins', 'Votants', '_Vot_Ins', 'Blancs',
    '_Blancs_Ins', '_Blancs_Vot', 'Nuls', '_Nuls_Ins', '_Nuls_Vot', 'Exprimes', '_Exp_Ins', '_Exp_Vot'
]
columns_presidentielleT2_candidat = [
    'N_Panneau', 'Sexe', 'Nom', 'Prenom', 'Voix', '_Voix_Ins', '_Voix_Exp', 'Code_du_departement', 'Code_de_la_commune'
]

columns_presidentielleT2_candidat_2 = [
    'N_Panneau_2', 'Sexe_2', 'Nom_2', 'Prenom_2', 'Voix_2', '_Voix_Ins_2', '_Voix_Exp_2', 'Code_du_departement', 'Code_de_la_commune'
]

presidentielleT2_commun = presidentielleT2[columns_presidentielleT2_commun]
presidentielleT2_candidat = presidentielleT2[columns_presidentielleT2_candidat]
presidentielleT2_candidat_2 = presidentielleT2[columns_presidentielleT2_candidat_2]
presidentielleT2_candidat_2.columns = columns_presidentielleT2_candidat
presidentielleT2_candidat = panda.concat([presidentielleT2_candidat, presidentielleT2_candidat_2], ignore_index=True)

presidentielleT2_commun['date_collecte'] = today_string
presidentielleT2_commun_types = {
    'Code_du_departement': 'string',
    'Code_de_la_commune': 'string',
    'Libelle_du_departement': 'string',
    'Libelle_de_la_commune': 'string',
    'Etat_saisie': 'string',
    'Inscrits': 'Int64',
    'Abstentions': 'Int64',
    '_Abs_Ins': 'float',
    'Votants': 'Int64',
    '_Vot_Ins': 'float',
    'Blancs': 'Int64',
    '_Blancs_Ins': 'float',
    '_Blancs_Vot': 'float',
    'Nuls': 'Int64',
    '_Nuls_Ins': 'float',
    '_Nuls_Vot': 'float',
    'Exprimes': 'Int64',
    '_Exp_Ins': 'float',
    '_Exp_Vot': 'float',
    'date_collecte': 'string'
}
presidentielleT2_commun = presidentielleT2_commun.astype(presidentielleT2_commun_types)

presidentielleT2_candidat_types = {
    'N_Panneau': 'Int64',
    'Sexe': 'string',
    'Nom': 'string',
    'Prenom': 'string',
    'Voix': 'Int64',
    '_Voix_Ins': 'float',
    '_Voix_Exp': 'float',
    'Code_du_departement': 'string',
    'Code_de_la_commune': 'string'
}
presidentielleT2_candidat = presidentielleT2_candidat.astype(presidentielleT2_candidat_types)

connexion = sqlite3.connect('ODS.db')

circo.to_sql('circo_composition', connexion, if_exists='append', index=False)
crime.to_sql('crimes_communes_2022', connexion, if_exists='append', index=False)
famille.to_sql('famile_TAUX_CHOMAGE_valeurs_trimestrielles', connexion, if_exists='append', index=False)
stat.to_sql('indic_stat_circonscriptions_legislatives_2022', connexion, if_exists='append', index=False)
presidentielleT1.to_sql('presidentielle_2022_communes_t1', connexion, if_exists='append', index=False)
presidentielleT2_commun.to_sql('resultats_par_niveau_subcom_t2_france_entiere_commun', connexion, if_exists='append', index=False)
presidentielleT2_candidat.to_sql('resultats_par_niveau_subcom_t2_france_entiere_candidat', connexion, if_exists='append', index=False)

connexion.commit()
connexion.close()