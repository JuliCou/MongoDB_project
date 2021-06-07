from bs4 import BeautifulSoup
import pymongo
import copy

connex = pymongo.MongoClient("mongodb://127.0.0.1:27017/")
db = connex.LBC_db1
vendeur_collection = db["vendeur"]
foire_collection = db["foire"]

with open("C:\\Users\\juliette.courgibet\\dataLBC\\vendeur.htm", "r", encoding='utf-8') as f:
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

# Données vendeurs
vendeur = copy.deepcopy(donnees["props"]["pageProps"]["data"])
vendeur["ads"] = {"total": donnees["props"]["pageProps"]["adsData"]["total"]}
vendeur["_id"] = vendeur["owner"]["userId"]

try:
    vendeur_collection.insert_one(vendeur)
except:
    print("erreur")

# Données annonces
for ad in donnees["props"]["pageProps"]["adsData"]["ads"]:
    ad["_id"] = ad.pop("list_id")
    try:
        foire_collection.insert_one(ad)
    except:
        print("erreur")
