from bs4 import BeautifulSoup
import pymongo
import argparse

# Arguments d'entrée
parser = argparse.ArgumentParser()
parser.add_argument('collection', type=str, help='Collection mongoDB')
args = parser.parse_args()

# Connexion à mongoDB
# connex = pymongo.MongoClient("mongodb://127.0.0.1:27017/")
connex = pymongo.MongoClient("mongodb://15.188.64.30:28015")
connex.admin.authenticate("julie", "initi@l1")
db = connex.LBC_db1
collection_args = args.collection

# Récupération de la collection existante
if collection_args in db.list_collection_names():
    collection = db[collection_args]
# Création d'une nouvelle collection
else:
    collection = db[collection_args]

# Ouverture fichier html
with open("C:\\Users\\juliette.courgibet\\dataLBC\\dataLBC.htm", "r", encoding='utf-8') as f:
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

for ad in donnees["props"]["pageProps"]["listingData"]["ads"]:
    ad["_id"] = collection.name + str(ad.pop("list_id"))
    ad["nb_posts"] = 1
    ad["verification_date"] = ad["index_date"]
    date_post = ad['index_date']
    try:
        ad["price_calendar"] = [{"date" : date_post, "price": ad["price"][0]}]
        collection.insert_one(ad)
    except:
        print("erreur")
        print(ad["_id"])
        res = collection.find({"_id":ad["_id"]})
        if res.count(True)==1:
            for r in res:
                date_previous = r["index_date"]
                if date_previous != date_post:
                    maj = {'$set': {}}
                    maj["$set"]["nb_posts"] = r["nb_posts"] + 1
                    maj["$set"]["verification_date"] = ad["index_date"]
                    maj["$set"]["price_calendar"] = r["price_calendar"].append({"date" : date_post, "price": ad["price"][0]})
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