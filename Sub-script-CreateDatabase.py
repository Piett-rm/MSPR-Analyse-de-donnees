import sqlite3

def createDatabase(databasePath, scriptPath):
    connexion = sqlite3.connect(databasePath)
    creationCursor = connexion.cursor()
    with open(scriptPath, 'r') as sql_file:
            sql_script = sql_file.read()
    creationCursor.executescript(sql_script)
    connexion.commit()
    creationCursor.close()
    connexion.close()

createDatabase('ODS.db', './Script SQLite ODS.sql')

createDatabase('DWH.db', './DWH.sql')