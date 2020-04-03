import json

from django.http import QueryDict
from django.test import TestCase, RequestFactory

from data_france.views import CommuneSearchView


class CommuneSearchViewTestCase(TestCase):
    def setUp(self) -> None:
        self.factory = RequestFactory()
        self.view = CommuneSearchView.as_view()

    def query_builder(self, d):
        q = QueryDict(mutable=True)
        q.update(d)
        return q.urlencode()

    def test_ne_renvoie_rien_sans_requete(self):
        req = self.factory.get("/chercher")
        res = self.view(req)

        self.assertEqual(res["Content-Type"], "application/json; charset=utf-8")
        self.assertEqual(json.loads(res.content.decode("utf-8")), {"results": []})

    def test_recherche_simple(self):

        req = self.factory.get(f"/chercher?q={self.query_builder({'q': 'etalans'})}")
        res = self.view(req)

        self.assertEqual(json.loads(res.content.decode("utf-8")), {"results": []})
