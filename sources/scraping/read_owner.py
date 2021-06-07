import pymongo
import pandas as pd


# Connexion à la base de données
connex = pymongo.MongoClient("mongodb://127.0.0.1:27017/")
connex = pymongo.MongoClient("mongodb://35.180.225.211:28015/")
db = connex.LBC_db1

# Initialisation liste des vendeurs
vendeurs = []
type_vendeur = []

# Recherche sur toutes les collections
for cltn in db.list_collection_names():
    collection = db[cltn]
    if collection.name != "vendeur":
        for vd in collection.find():
            vendeurs.append(vd["owner"]["user_id"])
            type_vendeur.append(vd["owner"]["type"])

# Ecriture de la liste des propriétaires dans fichier csv
owner = pd.DataFrame({"owner_id":vendeurs, "type": type_vendeur})
owner = owner.drop_duplicates()

# Recherche des vendeurs déjà dans la base
for vd in db["vendeur"].find():
    vd_ = vd["owner"]["userId"]
    if vd_ in vendeurs:
        owner = owner[owner.owner_id != vd_]

# Sauvegarde des données
owner.to_csv("C:\\Users\\juliette.courgibet\\dataLBC\\vendeurs.csv", index=False, header=True)