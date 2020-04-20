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
* Les départements et les conseils départementaux
* Les régions et les conseils régionaux
* Les collectivités territoriales aux statut particulier : métropole de Lyon (aux compétences départementales) avec les
  conseils départementaux, les collectivités territoriales uniques (qui cumulent compétences départementales et
  régionales) avec les conseils régionaux
* Les codes postaux

Toutes ces entités (sauf les codes postaux, et les collectivités régionales, dont la géométrie est systématique celle
de la région correspondante) viennent avec une géometrie et les articles + charnière.

Vues
----

Une vue de recherche renvoyant les résultats en JSON est disponible, par défaut à l'URL `chercher/communes/` (en
utilisant le paramètre GET `q`). Il est possible d'obtenir les résultats au format geojson en ajoutant le paramètre GET
`geojson` à une valeur non vide.


Autres remarques
----------------

**ATTENTION** : Ce paquet ne fonctionne que si votre projet Django utilise **PostGIS.**
