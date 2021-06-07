import pymongo
import pprint
from bson.code import Code
from bson.son import SON
import pandas as pd
import seaborn
import matplotlib.pyplot as plt
import numpy as np

# connex = pymongo.MongoClient("mongodb://127.0.0.1:27017/")
# db = connex.LBC_db1
connex = pymongo.MongoClient("mongodb://35.180.30.62:28015")
connex.admin.authenticate("julie", "initi@l1")
# connex2 = pymongo.MongoClient("mongodb://127.0.0.1:27017/")
db = connex.LBC_db1

list_collections = db.list_collection_names()
for collection in list_collections:
    print(collection, " : ", db[collection].estimated_document_count())
    pprint.pprint(db[collection].find_one())


collection = list_collections[0]

for key, item in db[collection].find_one().items():
    print(key, " : ", str(len(db[collection].distinct(key))))
# collection = db.vehicules

# Nombre de vendeurs différents dans une collection
collection = "foire"
len(db[collection].distinct("owner.user_id"))

# Liste des vendeurs
for vendeur in db[collection].distinct("owner.user_id"):
    print(vendeur)

# res = db[collection].aggregate({"cuisine": "Soups"}, { "_id": 0, "name": 1, "borough": 1})
# print(list(res))

# Prix égal à 2€
res = db[collection].find({"price":2})
res.count()
for r in res:
    pprint.pprint(r)

# Prix supérieur à une certaine borne
collection="voitures"
res = db[collection].find({"price":{"$gte":150000}})
res.count()
for r in res:
    print(r["subject"], " : ", r["price"][0])

# Moyenne des prix
collection = "voitures"
res = db[collection].aggregate([
    {
        "$unwind" : "$price"
    },
    {
        "$group": {
            "_id" : "$_id",
            "price0" : {"$first" : "$price"}
        }
    },
    {
        "$group": {
            "_id": 0,
            "nb": {"$sum": 1},
            "prix_moyen" : {"$avg" : "$price0"},
            "prix min" : {"$min" : "$price0"},
            "prix max" : {"$max" : "$price0"}
        }
    }])


def affiche(res):
    for r in res:
        pprint.pprint(r)


# Liste des marques
# Liste des marques et nombre de voitures par marque
collection = "voitures"
map = Code("function () {"
            "    for(var i=0;i<this.attributes.length;i++){"
            "           if(this.attributes[i].key == 'brand') {"
            "                   emit(this.attributes[i].value, 1);}"
            "   }"
            "}")
reduce = Code("function (key, values) {"
               "  return values.length;"
               "}")
result = db[collection].map_reduce(map, reduce, "map_reduce_brand")
affiche(db.map_reduce_brand.find().sort([("value", pymongo.DESCENDING)]))
nb = 0
for res in db.map_reduce_brand.find():
    nb += res["value"]
# Map reduce - Prix moyen des marques de voitures
collection = "voitures"
map = Code("function () {"
            "    for(var i=0;i<this.attributes.length;i++){"
            "           if(this.attributes[i].key == 'brand') {"
            "                   emit(this.attributes[i].value, this.price[0]);}"
            "   }"
            "}")
reduce = Code("function (key, values) {"
               "  var total = 0;"
               "  for (var i = 0; i < values.length; i++) {"
               "    total += values[i];"
               "  }"
               "  return total/values.length;"
               "}")
result = db[collection].map_reduce(map, reduce, "map_reduce_price_per_brand")
affiche(db.map_reduce_price_per_brand.find().sort([("value", pymongo.DESCENDING)]))
df = pd.DataFrame(db.map_reduce_price_per_brand.find()).round(0)
df = df.sort_values(by='value', ascending=False)

# Map reduce - Prix moyen par rapport à l'âge de la voiture
collection = "voitures"
map = Code("function () {"
            "    for(var i=0;i<this.attributes.length;i++){"
            "           if(this.attributes[i].key == 'regdate') {"
            "                   emit(this.attributes[i].value, this.price[0]);}"
            "   }"
            "}")
reduce = Code("function (key, values) {"
               "  var total = 0;"
               "  for (var i = 0; i < values.length; i++) {"
               "    total += values[i];"
               "  }"
               "  return total/values.length;"
               "}")
result = db[collection].map_reduce(map, reduce, "map_reduce_price_per_year")
affiche(db.map_reduce_price_per_year.find().sort([("_id", pymongo.DESCENDING)]))
df = pd.DataFrame(db.map_reduce_price_per_year.find()).round(0)
df = df.sort_values(by='_id', ascending=False)
# Prix - distribution, moyenne, médiane, description
# Marque de voiture, prix par rapport aux marques

# Collection vêtements
# Map-reduce catégorie de vêtements (femme, enfants, homme, maternité)
collection = "vetements"
map = Code("function () {"
            "    for(var i=0;i<this.attributes.length;i++){"
            "           if(this.attributes[i].key == 'clothing_type') {"
            "                   emit(this.attributes[i].value_label, this.price[0]);}"
            "   }"
            "}")
reduce = Code("function (key, values) {"
               "  return values.length;"
               "}")
result = db[collection].map_reduce(map, reduce, "vetement_categorie_nb")
affiche(db.vetement_categorie_nb.find())
# Map-reduce catégorie de vêtements (femme, enfants, homme, maternité) - moyenne des prix
collection = "vetements"
map = Code("function () {"
            "    for(var i=0;i<this.attributes.length;i++){"
            "           if(this.attributes[i].key == 'clothing_type') {"
            "                   emit(this.attributes[i].value_label, this.price[0]);}"
            "   }"
            "}")
reduce = Code("function (key, values) {"
               "  var total = 0;"
               "  for (var i = 0; i < values.length; i++) {"
               "    total += values[i];"
               "  }"
               "  return total/values.length;"
               "}")
result = db[collection].map_reduce(map, reduce, "vetement_categorie_price")
affiche(db.vetement_categorie_price.find())

# Map-reduce type de vêtements
collection = "vetements"
map = Code("function () {"
            "    for(var i=0;i<this.attributes.length;i++){"
            "           if(this.attributes[i].key == 'clothing_category') {"
            "                   emit(this.attributes[i].value_label, this.price[0]);}"
            "   }"
            "}")
reduce = Code("function (key, values) {"
               "  return values.length;"
               "}")
result = db[collection].map_reduce(map, reduce, "vetement_type_nb")
affiche(db.vetement_type_nb.find().sort([("value", pymongo.DESCENDING)]))
# Prix des vêtements selon leur type
reduce = Code("function (key, values) {"
               "  var total = 0;"
               "  for (var i = 0; i < values.length; i++) {"
               "    total += values[i];"
               "  }"
               "  return total/values.length;"
               "}")
result = db[collection].map_reduce(map, reduce, "vetement_type_prix")
affiche(db.vetement_type_prix.find().sort([("value", pymongo.DESCENDING)]))


# Distribution des prix
collection = "voitures"
map = Code("function () {"
            "   emit(this.price[0], 1);}")
reduce = Code("function (key, values) {"
               "  return values.length;"
               "}")
result = db[collection].map_reduce(map, reduce, "dist_prix_voitures")
prix = []
qte = []
for res in db.dist_prix_voitures.find():
    prix.append(res["_id"])
    qte.append(res["value"])
prix = []
for res in db["voitures"].find():
    prix.append(res["price"][0])
prix_DF = pd.DataFrame({"price": prix})
fig = plt.figure(1)
seaborn.distplot(prix, kde=False)
plt.xlabel("Prix (€)")
plt.ylabel("Nombre de voitures")
plt.title("Distribution des prix des voitures")
plt.savefig("dist_prix_voiture.png")
plt.show()

# Sans les outliers
def reject_outliers(data, m = 2.):
    d = np.abs(data - np.median(data))
    mdev = np.median(d)
    s = d/mdev if mdev else 0.
    return np.array(data)[np.array(s<m)]

prix_nettoye = reject_outliers(prix, 5)
fig = plt.figure(1)
seaborn.distplot(prix_nettoye, kde=False)
plt.xlabel("Prix (€)")
plt.ylabel("Nombre de voitures")
plt.title("Distribution des prix des voitures (sans outliers)")
plt.savefig("dist_prix_outliers_voiture.png")
plt.show()

# Distribution des prix pour vetements
prix = []
for res in db["vetements"].find():
    prix.append(res["price"][0])

fig = plt.figure(1)
seaborn.distplot(prix, kde=False)
plt.xlabel("Prix (€)")
plt.ylabel("Nombre de vêtements")
plt.title("Distribution des prix des vêtements")
plt.savefig("dist_prix_vetement.png")
plt.show()

# Sans les outliers
def reject_outliers(data, m = 2.):
    d = np.abs(data - np.median(data))
    mdev = np.median(d)
    s = d/mdev if mdev else 0.
    return np.array(data)[np.array(s<m)]

prix_nettoye = reject_outliers(prix, 10)
fig = plt.figure(1)
seaborn.distplot(prix_nettoye, kde=False)
plt.xlabel("Prix (€)")
plt.ylabel("Nombre de vêtements")
plt.title("Distribution des prix des vêtements (sans outliers)")
plt.savefig("dist_prix_outliers_vetements.png")
plt.show()