data-france
=============

Un ensemble de données administratives et géographiques pour la France. Elle double comme application Django
pour permettre l'intégration aisée de ces données.


Installer le paquet
-------------------

Installez ce paquet avec pip::

  pip install data-france


Importer les données
--------------------

Pour importer les données, appliquez les migrations et utilisez la commande de management::

  ./manage.py update_data_france


Modèles
--------

Circonscriptions administratives et collectivités locales
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

L'application django comporte les modèles suivants :

* `Commune`

  * Inclut les communes délégués / communes associées / arrondissements PLM /
    secteurs électoraux PLM
  * Les différents types d'entités sont différenciés par le champ `type`

* `EPCI`

  * Il s'agit des EPCI à fiscalité propre : CA, CC, CU et métropoles
  * N'inclut pas encore les EPT du Grand Paris

* Canton

  * N'inclut pas encore les géométries

* `Departement` et `Region` pour les départements et régions comme
  circonscriptions administratives de l'État

* `CollectiviteDepartementale` et `CollectiviteRegionale` pour les départements
  et régions comme collectivités territoriales :

  * La métropole de Lyon (aux compétences départementales) est référencée comme
    une collectivité départementale ;
  * les collectivités territoriales uniques (par exemple l'Assemblée de Corse)
    sont référencées comme des collectivités régionales (cela inclut, de façon
    contre-intuitive, le département de Mayotte) ;
  * À noter que comme le conseil de Paris est déjà référencé comme une
    `Commune`, il n'est pas référencé de nouveau comme collectivité
    départementale.

* Les codes postaux

* Circonscriptions législatives

* Cisconscriptions consulaires

Toutes ces entités (sauf les codes postaux, les cantons, les circonscriptions
consulaires, et les collectivités régionales, dont la géométrie est
systématiquement celle de la région correspondante) viennent avec une géometrie
et les articles + charnière.

Élu·es
~~~~~~

Les fichiers suivants du répertoire national des élus sont importés et
disponibles sous forme de modèle Django :

* Les élus municipaux

* Les députés


Vues JSON
----------

Recherche de communes
~~~~~~~~~~~~~~~~~~~~~

Une vue de recherche renvoyant les résultats en JSON est disponible, à l'URL
`chercher/communes/` si vous importez `data_france.urls` (en utilisant le
paramètre GET `q`). Il est possible d'obtenir les résultats au format geojson en
ajoutant le paramètre GET `geojson` à une valeur non vide.

Recherche de circonscriptions consulaires
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Une vue de recherche des circonscriptions consulaires, à l'adresse
`circonscriptions-consulaires/chercher/`, en utilisant le paramètre `q`.

Des vues d'affichage par code
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Des vues existent pour afficher une des entités suivantes en la référençant par son code INSEE usuel :

* Les communes, `communes/par-code/`
* Les epci, `epci/par-code/`

  * Il faut utiliser les codes SIREN

* Les départements, `departements/par-code/`
* Les régions, `regions/par-code/`
* Les codes postaux, `code-postal/par-code/`
* Les collectivités de niveau départemental, `collectivite-departementale/par-code/`

  * Le code du département considéré comme une collectivité départementale
    plutôt que comme une circonscription administrative de l'État est
    généralement `<code dep>D`.

* Les collectivités de niveau régional, `collectivite-regionale/par-code/`

  * Généralement

Autres remarques
----------------

**ATTENTION** : Ce paquet ne fonctionne que si votre projet Django utilise
**PostGIS** car il utilise certaines fonctionnalités propres à PostgreSQL.
