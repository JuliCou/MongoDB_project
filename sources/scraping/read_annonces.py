import pymongo
import pandas as pd
import argparse


# Arguments d'entrée
parser = argparse.ArgumentParser()
parser.add_argument('collection', type=str, help='Collection mongoDB')
args = parser.parse_args()
collection_args = args.collection

# Connexion à la base de données
# connex = pymongo.MongoClient("mongodb://127.0.0.1:27017/")
connex = pymongo.MongoClient("mongodb://35.180.225.211:28015")
connex.admin.authenticate("julie", "initi@l1")
db = connex.LBC_db1
collection = db[collection_args]

# Initialisation liste des vendeurs
pages_html = []
titre = []

# Recherche sur toutes les annonces
annonces = collection.find().sort([("verification_date", pymongo.ASCENDING), ("index_date", pymongo.ASCENDING)])
for annonce in annonces:
    pages_html.append(annonce["url"])
    titre.append(annonce["subject"])

# Ecriture de la liste des propriétaires dans fichier csv
annonces = pd.DataFrame({"url":pages_html, "titre": titre})
annonces = annonces.drop_duplicates()

# Sauvegarde des données
annonces.to_csv("C:\\Users\\juliette.courgibet\\dataLBC\\annonces.csv", index=False, header=True)