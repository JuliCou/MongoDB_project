from bs4 import BeautifulSoup
import pymongo
import argparse
from datetime import datetime


# Arguments d'entrée
parser = argparse.ArgumentParser()
parser.add_argument('collection', type=str, help='Collection mongoDB')
args = parser.parse_args()

# Connexion à mongoDB
# connex = pymongo.MongoClient("mongodb://127.0.0.1:27017/")
connex = pymongo.MongoClient("mongodb://35.180.225.211:28015")
connex.admin.authenticate("julie", "initi@l1")
db = connex.LBC_db1
old_db = connex.anciennes_annonces
collection_args = args.collection

# Récupération de la collection existante
collection = db[collection_args]
collection_old = old_db[collection_args]

# Ouverture fichier html
with open("C:\\Users\\juliette.courgibet\\dataLBC\\annonce.htm", "r", encoding='utf-8') as f:
    data = f.read()

soup = BeautifulSoup(data)

donnees = ""
for index, p in enumerate(soup.find_all('script')):
    if p.string != None :
        ix = index
        txt = str(p.string)
        donnees = txt

donnees = donnees.replace("null", "None")
donnees = donnees.replace("\\n", " ")
donnees = donnees.replace("false", "False")
donnees = donnees.replace("true", "True")
donnees = eval(donnees)


try:
    ad = donnees["props"]["pageProps"]["ad"]
    ad["_id"] = collection.name + str(ad.pop("list_id"))
except:
    ad_ = donnees["query"]
    ad = {}
    ad["_id"] = ad_["cat"] + ad_["id"]


res = collection.find({"_id":ad["_id"]})
try:
    res = res[0]
    date_previous = res["index_date"]
    if "subject" in ad.keys():
        testTitreIdentique = (ad["subject"] == res["subject"])
    else:
        testTitreIdentique = False
except:
    res = ad
    testTitreIdentique = False
    pass


if len(ad.keys()) == 1 or not(testTitreIdentique):
    res["suppression_date"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    res["id"] = res.pop("_id")
    collection_old.insert_one(res)
    collection.delete_one({"_id":ad["_id"]})
else:
    maj = {'$set': {}}
    maj["$set"]["verification_date"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")         
    a = collection.find_one_and_update({'_id': ad["_id"]}, maj)


# for ad in collection.find():
#     maj = {'$set': {}}
#     maj["$set"]["nb_posts"] = 1
#     date_post = ad['index_date']
#     maj["$set"]["price_calendar"] = [{"date" : date_post, "price": ad["price"][0]}]
#     a = collection.find_one_and_update({'_id': ad["_id"]}, maj)
# ['livres', 'foire', 'vetements', 'voitures', 'ameublement', 'vendeur']

# col_name = 'vetements'
# collection = db[col_name]
# resultats = collection.find()
# for res in resultats:
#     _id = res["_id"]
#     if not col_name in str(_id):
#         res["_id"] = col_name + str(_id)
#         collection.insert_one(res)
#         collection.remove({"_id":_id})
#     if col_name*2 in str(_id):
#         res["_id"] = res["_id"].replace(col_name*2, col_name)
#         collection.insert_one(res)
#         collection.remove({"_id":_id})