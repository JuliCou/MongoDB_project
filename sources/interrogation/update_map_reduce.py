import pymongo
from bson.code import Code
from bson.son import SON
import pandas as pd

connex = pymongo.MongoClient("mongodb://127.0.0.1:27017/")
db = connex.LBC_db1

# map-reduce_brand
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

# Prix des vêtements selon leur type
reduce = Code("function (key, values) {"
               "  var total = 0;"
               "  for (var i = 0; i < values.length; i++) {"
               "    total += values[i];"
               "  }"
               "  return total/values.length;"
               "}")
result = db[collection].map_reduce(map, reduce, "vetement_type_prix")