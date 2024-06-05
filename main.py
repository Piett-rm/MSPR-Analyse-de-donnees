from data_aleatoire import data_aleatoire
from data_prediction import data_prediction
from script_ods import insert_to_db
from DW import jointure_table_pour_DW
import os

if __name__ == "__main__":

    insert_to_db()
    jointure_table_pour_DW()
    data_aleatoire()
    data_prediction()
