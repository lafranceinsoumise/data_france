DATA-FRANCE
===========

Un ensemble de données administratives et géographiques pour la France. Elle double comme application Django
pour permettre l'intégration aisée de ces données.


Importer les données
--------------------

Pour importer les données, appliquez les migrations et utilisez la commande de management::

    ./manage.py update_data_france


Modèles
-------

Pour le moment, j'ai inclus :

* Les communes
    * Inclut les communes délégués / communes associées / arrondissements PLM
* Les EPCI
    * CA, CC, CU et métropoles
    * N'inclut pas encore les EPT du Grand Paris
* Les départements
* Les régions

Toutes ces entités viennent avec une géometrie et les articles + charnière.

Vues
----

Une vue de recherche renvoyant les résultats en JSON est disponible, par défaut
à l'URL `chercher/communes/` (en utilisant le paramètre GET `q`).


Autres remarques
----------------

**ATTENTION** : Ce paquet ne fonctionne que si votre projet Django utilise **PostGIS.**
