import json

from django.http import QueryDict
from django.test import TestCase, RequestFactory

from data_france.models import Commune, Departement
from data_france.views import CommuneJSONView, DepartementJSONView


class ViewTestCase(TestCase):
    view_class = None

    def setUp(self) -> None:
        self.factory = RequestFactory()
        self.view = self.view_class.as_view()

    def query_builder(self, d):
        q = QueryDict(mutable=True)
        q.update(d)
        return q.urlencode()

    def json_content(self, res):
        self.assertEqual("application/json", res["Content-Type"])
        try:
            return json.loads(res.content.decode("utf-8"))
        except ValueError:
            self.fail(f"Devrait renvoyer un JSON valide en UTF-8: '{res.content}'")


class CommuneSearchViewTestCase(ViewTestCase):
    view_class = CommuneJSONView

    def test_ne_renvoie_rien_sans_requete(self):
        req = self.factory.get("/communes/")
        res = self.view(req)

        self.assertEqual({"results": []}, self.json_content(res))

    def test_recherche_simple(self):

        req = self.factory.get(f"/communes/?{self.query_builder({'q': 'etalans'})}")
        res = self.view(req)

        self.assertEqual(
            {
                "results": [
                    {
                        "code": "25222",
                        "code_departement": "25",
                        "nom": "Étalans",
                        "type": "COM",
                    }
                ]
            },
            self.json_content(res),
        )

    def test_recherche_en_geojson(self):
        req = self.factory.get(
            f"/communes/?{self.query_builder({'q': 'etalans', 'geojson': 'O'})}"
        )
        res = self.view(req)
        results = self.json_content(res)

        features = results.pop("features")
        self.assertEqual(results, {"type": "FeatureCollection"})

        self.assertEqual(len(features), 1)
        feature = features[0]

        self.assertEqual(feature["type"], "Feature")

        self.assertEqual(
            feature["properties"],
            {
                "code": "25222",
                "code_departement": "25",
                "nom": "Étalans",
                "type": "COM",
            },
        )

        etalans = Commune.objects.get(type="COM", code="25222")
        expected_geometry = json.loads(etalans.geometry.geojson)

        self.assertEqual(feature["geometry"], expected_geometry)


class DepartementViewTestCase(ViewTestCase):
    view_class = DepartementJSONView

    def test_obtenir_departement_specifique(self):
        d = Departement.objects.order_by("?").select_related("chef_lieu").first()

        req = self.factory.get(f"/departements/?{self.query_builder({'code': d.code})}")
        res = self.view(req)

        results = self.json_content(res)

        self.assertEqual(
            results,
            {
                "code": d.code,
                "nom": d.nom,
                "population": d.population,
                "chefLieu": {"nom": d.chef_lieu.nom_complet, "code": d.chef_lieu.code},
            },
        )

    def test_obtenir_departement_specifique_geojson(self):
        d = Departement.objects.order_by("?").select_related("chef_lieu").first()

        req = self.factory.get(
            f"/departements/?{self.query_builder({'code': d.code, 'geojson': 'O'})}"
        )
        res = self.view(req)

        results = self.json_content(res)

        self.assertEqual(
            results,
            {
                "type": "Feature",
                "properties": {
                    "code": d.code,
                    "nom": d.nom,
                    "population": d.population,
                    "chefLieu": {
                        "nom": d.chef_lieu.nom_complet,
                        "code": d.chef_lieu.code,
                    },
                },
                "geometry": json.loads(d.geometry.geojson),
            },
        )

    def test_obtenir_departements(self):
        qs = Departement.objects.select_related("chef_lieu")

        req = self.factory.get("/departements/")
        res = self.view(req)
        results = self.json_content(res)

        self.assertEqual(
            results,
            {
                "results": [
                    {
                        "code": d.code,
                        "nom": d.nom,
                        "population": d.population,
                        "chefLieu": {
                            "nom": d.chef_lieu.nom_complet,
                            "code": d.chef_lieu.code,
                        },
                    }
                    for d in qs
                ]
            },
        )

        req = self.factory.get("/departements/?geojson=O")
        res = self.view(req)

        self.assertEqual(res.status_code, 400)
