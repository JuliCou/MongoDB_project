from elasticsearch import Elasticsearch
import pymongo

# Connexion à mongoDB
connex = pymongo.MongoClient("mongodb://CLOUD/")
connex.admin.authenticate("julie", "MDP")
db = connex.LBC_db1
collection = db['vendeur']

# db.list_collection_names()
# ['livres', 'foire', 'vetements', 'voitures', 'ameublement', 'vendeur']

# Elasticsearch
es = Elasticsearch(['localhost'], port=9200)

# Récupération des documents
res_mongo = collection.find()
for i, doc in enumerate(res_mongo):
    if "_id" in doc.keys():
        doc_id = doc.pop("_id")
    res = es.search(index=collection.name, body={"query": {"match":{"_id":doc_id}}})
    res = es.search(index=collection.name, body={"query": {"match_all":{"subject":"Twingo ess moteur a chaîne 102000km 1350"}}})
    nb_res = res["hits"]["total"]["value"]
    res = es.get(index=collection.name, id=doc_id)
    res = es.index(index=collection.name, id=doc_id, body=doc)
    print(res["result"])

es.count(index=collection.name)["count"]

res = es.get(index="index_mapreduce_test", id=1)
print(res['_source'])
