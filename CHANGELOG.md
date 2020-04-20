data_france
===========

Version 0.9.1
-------------

* Rajoute `__str__` aux collectivités départementales et régionales.
* Rajoute le champ de recherche à l'admin des régions et départements

Version 0.9.0
-------------

* Ajoute collectivités départementales, y compris la métropole de Lyon
* Ajoute collectivités régionales, y compris les collectivités territoriales uniques comme la Corse ou la Martinique
* Nettoyage de l'admin

Version 0.8.3
-------------

* Ajout des codes postaux
* Import un peu plus rapide (en segmentant en plusieurs transactions)

Version 0.8.2
-------------

* Régle le bug de plantage de la vue de recherche de commune lorsque celle-ci
  renvoyait une commune déléguée ou associée comme un des résultats.
* Les régions sont maintenant ordonnées par défaut dans l'ordre alphabétique.

Version 0.8.1
-------------

* Corrige la vue d'admin des communes

Version 0.8.0
-------------

Principaux changements :

* `data_france` n'est maintenant explicitement compatible qu'avec PostGIS
* Ajout des régions et des départements
* Les données ne sont plus importées dans le cadre d'une migration, cela prenait
  trop de temps à chaque création de la base de test et ralentissait donc les 
  tests de toute application Django qui intégrerait ce paquet. À la place, une
  commande de management `update_data_france` a été ajoutée.

Autres changements :

* Ajoute un module `urls.py` avec un chemin vers défaut vers la vue de recherche
* La mise à jour des données a été grandement accéléré en utilisant la commande
  `COPY` pour importer les données dans une table temporaire, puis une insertion
  utilisant l'option `ON COMMIT`. Ces deux fonctionnalités sont spécifiques à
  PostgreSQL.
* Les `id` des différentes entités géographiques sont maintenant assurées de rester
  stable à l'avenir.