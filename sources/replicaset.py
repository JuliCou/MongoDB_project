import pymongo
import pandas as pd


# Connexion à la base de données
connex = pymongo.MongoClient("mongodb://127.0.0.1:27017/")
connex2 = pymongo.MongoClient("mongodb://127.0.0.1:27018/")

db = connex.LBC_db1
db2 = connex2.LBC

for cltn in db.list_collection_names():
    collection = db[cltn]
    for ad in collection.find():
        db2[cltn].insert_one(ad)