from data_aleatoire import data_aleatoire
from data_prediction import data_prediction
from script_ods import insert_to_db
from DW import jointure_table_pour_DW
import os

if __name__ == "__main__":
    """
        Préparation et stockage des données
        Insersion des données brut dans l'ODS
        Mise en qualité et insersion des données propre dans le DW
        Selection des données 2022 des haut de france en deux csv prêt à entrainement
        Selection des données 2023 des haut de france en un csv
    """
    insert_to_db()
    jointure_table_pour_DW()
    data_aleatoire()
    data_prediction()
