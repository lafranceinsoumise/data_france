data_france
===========

Version 0.13.5
--------------

* Mise à jour du RNE à décembre 2021
* Ajout des géométries des cantons
* Ajout de la nomenclature des catégories socio-professionnelles

Version 0.13.4
--------------

* Ajouts d'index de recherche à toutes les catégories d'élus

Version 0.13.3
--------------

* Correction d'un bug d'affichage des élus régionaux

Version 0.13.2
-----------

* Ajout des conseiller·ères régionaux·ales
* Ajout des député·es européen·nes

Les conseillers régionaux sont pour le moment reliés directement aux régions plutôt qu'aux
collectivités régionales, mais c'est une mesure temporaire, par simplicité.

Version 0.13.1
--------------

* Ajout des conseillers départementaux
* Ajout de l'administration pour les cantons

Version 0.13.0
--------------

* Documentation légèrement enrichie (on y est pas encore)
* Retravail complet de la liste des collectivités à rôle départemental pour les
  inclure toutes, même celle qui étaient inclues ailleurs, avec les codes
  utilisés par l'INSEE.

Version 0.12.2
--------------

* Ajout de l'admin pour les circonscriptions législatives

Version 0.12.1
--------------

Nouvelles fonctionnalités
* Ajout des circonscriptions législatives et des députés
* Les populations sont recalculées pour les communes modifiées (fusions et
  rétablissements) après 2019

Mises à jour :
* Mise à jour du COG et d'admin-express à la version de 2021

Résolutions de bugs:
* Les mairies Corses n'étaient pas importées

**Attention** : la version 0.12.0 a été publiée incorrectement avec une dépendance inutile.

Version 0.11.5
--------------

* Ajout des circonscriptions consulaires

Version 0.11.4
--------------

* Mise à jour du RNE avec la version d'avril 2021

Version 0.11.3
--------------

* Mise à jour des codes postaux

Version 0.11.2
--------------

* Ajout de l'information des parrainages 2017 aux élus municipaux
* Passage à *poetry* pour la gestion des dépendances et le paquetage
* Les fichiers sources bruts sont maintenant sauvegardées dans le dépôt git avec le code.
  Cela permet de reconstruire le paquet sans rien télécharger.
* Ajout de la normalisation sur la recherche par texte

**Attention** : la version 0.11.1 a été incorrectement publiée.

Version 0.11.0
--------------

* Mise à jour des EPCI pour la liste 2020
* Ajout des mandats EPCI aux élus municipaux
* Conseils municipaux et d'EPCI affichés sur les pages respectives
* Gestion de l'ordre des adjoints

**Attention** : les version 0.10.9 et 0.10.10 ont été incorrectement publiées et ne sont pas fonctionnelles.

Version 0.10.8
--------------

* Ajout des informations liées aux mairies pour chaque commune
* Correction d'un bug sur la vue `DepartementParCodeView`

Version 0.10.7
--------------

* Les noms des collectivités de niveau départemental ne comportent plus la partie "Conseil département de",
  mais reprennent juste le nom du département ; pour Lyon, le nom est "Métropole de Lyon"
* Idem pour les régions, avec comme exceptions les assemblées uniques ou le nom entier est repris (par exemple
  "Assemblée de Corse")
* Les attributs `nom_complet` (avec l'article) et `nom_avec_charniere` sont maintenant présents sur tous
  les modèles.

Version 0.10.6
--------------

* Interprétation correcte des dates saisies dans un format non standard dans le
  répertoire national des élus (par exemple 03/07/20 au lieu de 03/07/2020)
* Correction d'un bug dans le backend, sans aucune conséquence sur les versions
  publiées

Version 0.10.5
--------------

* Ajout de la recherche plein texte sur les élus municipaux

Version 0.10.4
--------------

* Mise à jour du référentiel COG vers la version de décembre 2020.
* Ajout de l'ensemble des élus municipaux depuis le Répertoire National des Élus

Version 0.10.3
--------------

* Remplacement de la mention obsolète de « conseil général » par « conseil départemental »
* Mise à jour des codes postaux

Version 0.10.2
--------------

* Version corrective de la précédente, avec les données.

Version 0.10.1
--------------

* Compatibilité Django 3
* Ajout de la liste des cantons (sans géométrie pour le moment)

Ne pas utiliser : version publié par accident sans les fichiers de données.

Version 0.10.0
--------------

Breaking changes :
* Les URLs par défaut des vues ont changé

Autres changements :
* Ajout de vues d'affichage 

Version 0.9.2
-------------

* Rajoute des vues JSON de récupération pour les régions et départements
* Les trois vues JSON ont un mode GeoJSON pour récupérer les géométries.

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
