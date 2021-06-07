# Projet universitaire
Projet de base de données noSQL, exploration de MongoDB.

Le projet consiste à récupérer les données d'annonces en ligne, du site Le Bon Coin, et de les insérer dans une base de données MongoDB.

La base de données MongoDB a pu être utilisée en local ou Cloud.

## Présentation du projet
Le projet est présenté dans le fichier def_projet.pdf  

## Première partie : Scraping des données
Code accessible sur sources/scraping

### Données des annonces
Les requêtes URL sont rejetées.  
Pour scrapper les données, un robot est utilisé (UiPath) qui ouvre des pages internet et enregistre les pages.  
Le robot lance un script python qui lit le fichier htm enregistré pour récupérer les données de toutes les annonces présentes sur une page de recherche et les insère dans la base de données MongoDB précédemment crées.

Démonstration visible sur documentation/démos/recuperation_donnees_annonces.gif

### Données des vendeurs
On peut aussi récupérer les données des pages LBC vendeurs à partir des identifiants des vendeurs obtenus avec les annonces.
On récupère ainsi la liste des vendeurs en lisant les annonces enregistrées dans la base de données LBC et on interroge la page URL (robot UiPath). Cette partie n'est intéressante que pour les vendeurs professionnels (pas d'information pour les vendeurs particuliers).

Démonstration visible sur documentation/démos/recuperation_donnees_vendeurs.gif

### Existence de la page LBC
Pour vérifier que l'annonce est encore en ligne, un autre script UiPath (verification_annonce.xaml) permet de lire les url des annonces enregistrées dans la base de données et de tester l'existence de la page.

### Résultat visible
Dans l'interface Compass. Des captures d'écran sont visibles documentation/démos/captures_ecran_interface_compass


## Deuxième partie : interrogation de la base de données
### Requêtes
Code accessibles sources/interrogation/requetes.py

### Map-reduce
Code accessibles sources/interrogation/update_map_reduce.py

## Data viz
Graphiques dans sources/interrogation/graphiques

## Troisième partie : Elastic Search
Exploration de l'utilisation de Elastic Search

Code : sources/elasticsearch_insert.py

## Quatrième partie : noSQL, ReplicaSet, Distribution
Voir la documentation :  
- Résistance aux pannes.docx
- Système distribué avec mongoDB.docx

Et vidéo démo dans le dossier démos :
- demo_replicaSet.pptx
- demo_sharding.pptx