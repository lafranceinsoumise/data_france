import json

from django.http import QueryDict
from django.test import TestCase, RequestFactory

from data_france.models import Commune, Departement
from data_france.views import (
    RechercheCommuneView,
    CommuneParCodeView,
    DepartementParCodeView,
)


class ViewTestCase(TestCase):
    view_class = None

    def setUp(self) -> None:
        self.factory = RequestFactory()
        self.view = self.view_class.as_view()

    def query_builder(self, d):
        q = QueryDict(mutable=True)
        if isinstance(d, dict):
            q.update(d)
        else:
            for k, v in d:
                q.appendlist(k, v)
        return q.urlencode()

    def get_status_json(self, res):
        self.assertEqual("application/json", res["Content-Type"])
        try:
            return res.status_code, json.loads(res.content.decode("utf-8"))
        except ValueError:
            self.fail(f"Devrait renvoyer un JSON valide en UTF-8: '{res.content}'")


class CommuneSearchViewTestCase(ViewTestCase):
    view_class = RechercheCommuneView

    def test_ne_renvoie_rien_sans_requete(self):
        req = self.factory.get("/communes/")
        res = self.view(req)

        status, results = self.get_status_json(res)

        self.assertEqual(status, 400)
        self.assertIn("errors", results)
        self.assertCountEqual(["q"], results["errors"])

    def test_recherche_simple(self):
        req = self.factory.get(f"/communes/?{self.query_builder({'q': 'etalans'})}")
        res = self.view(req)

        self.assertEqual(
            (
                200,
                {
                    "results": [
                        {
                            "code": "25222",
                            "code_departement": "25",
                            "nom": "Étalans",
                            "nom_departement": "Doubs",
                            "type": "COM",
                        }
                    ]
                },
            ),
            self.get_status_json(res),
        )

    def test_rechercher_avec_type(self):
        req = self.factory.get(f"/communes/?{self.query_builder({'q': 'Paris 13'})}")
        res = self.view(req)
        status, results = self.get_status_json(res)

        self.assertEqual(status, 200)
        self.assertCountEqual(
            [c["code"] for c in results["results"]], ["75113", "75056SR13"]
        )

        req = self.factory.get(
            f"/communes/?{self.query_builder([('q', 'Paris 13'), ('type', 'SRM'), ('type', 'COM')])}"
        )
        res = self.view(req)
        status, results = self.get_status_json(res)

        self.assertEqual(status, 200)
        self.assertCountEqual([c["code"] for c in results["results"]], ["75056SR13"])

    def test_recherche_en_geojson(self):
        req = self.factory.get(
            f"/communes/?{self.query_builder({'q': 'etalans', 'geojson': 'true'})}"
        )
        res = self.view(req)
        status, results = self.get_status_json(res)

        self.assertEqual(status, 200)

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
                "nom_departement": "Doubs",
                "type": "COM",
            },
        )

        etalans = Commune.objects.get(type="COM", code="25222")
        expected_geometry = json.loads(etalans.geometry.geojson)

        self.assertEqual(feature["geometry"], expected_geometry)


class CommuneParCodeViewTestCase(ViewTestCase):
    view_class = CommuneParCodeView

    def test_obtenir_commune_specifique(self):
        c = Commune.objects.order_by("?").filter(type="COM").first()

        req = self.factory.get(
            f"/communes/par-code/?{self.query_builder({'code': c.code, 'type': 'COM'})}"
        )
        res = self.view(req)

        status, results = self.get_status_json(res)

        self.assertEqual(status, 200)
        self.assertEqual(
            results,
            {
                "code": c.code,
                "type": c.type,
                "nom": c.nom_complet,
                "code_departement": c.code_departement,
                "nom_departement": c.nom_departement,
            },
        )


class DepartementViewTestCase(ViewTestCase):
    view_class = DepartementParCodeView

    def test_obtenir_departement_specifique(self):
        d = Departement.objects.order_by("?").select_related("chef_lieu").first()

        req = self.factory.get(f"/departements/?{self.query_builder({'code': d.code})}")
        res = self.view(req)

        status, results = self.get_status_json(res)

        self.assertEqual(status, 200)
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
            f"/departements/?{self.query_builder({'code': d.code, 'geojson': 'true'})}"
        )
        res = self.view(req)

        status, results = self.get_status_json(res)

        self.assertEqual(status, 200)
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
        status, results = self.get_status_json(res)

        self.assertEqual(status, 400)
        self.assertIn("errors", results)
        self.assertCountEqual(results["errors"], ["code"])

        req = self.factory.get("/departements/?geojson=true")
        res = self.view(req)

        self.assertEqual(res.status_code, 400)
