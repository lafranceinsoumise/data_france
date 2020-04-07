from django.http import JsonResponse
from django.views import View

from data_france.models import Commune


class CommuneSearchView(View):
    def get(self, request, *args, **kwargs):
        q = request.GET.get("q")

        if q:
            qs = Commune.objects.search(q).select_related("departement")[:10]
            res = [
                {
                    "code": c.code,
                    "type": c.type,
                    "nom": c.nom_complet,
                    "code_departement": c.departement.code,
                }
                for c in qs
            ]
        else:
            res = []

        return JsonResponse({"results": res},)
