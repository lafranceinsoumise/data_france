import json

from django.http import JsonResponse
from django.shortcuts import get_object_or_404
from django.views import View

from data_france.models import Commune, Departement, Region


class CommuneJSONView(View):
    def get_props_from_instance(self, commune):
        return {
            "code": commune.code,
            "type": commune.type,
            "nom": commune.nom_complet,
            "code_departement": commune.code_departement,
        }

    def get(self, request, *args, **kwargs):
        q = request.GET.get("q")
        geojson = request.GET.get("geojson", False)

        if q:
            qs = Commune.objects.search(q).select_related(
                "departement", "commune_parent__departement"
            )[:10]
        else:
            qs = Commune.objects.none()

        res = [self.get_props_from_instance(c) for c in qs]

        if geojson:
            features = [
                {
                    "type": "Feature",
                    "properties": r,
                    "geometry": json.loads(c.geometry.geojson),
                }
                for r, c in zip(res, qs)
            ]
            return JsonResponse({"type": "FeatureCollection", "features": features})
        else:
            return JsonResponse({"results": res},)


class BaseJSONView(View):
    queryset = None

    def get_props_from_instance(self, instance):
        raise NotImplementedError()

    def get_queryset(self):
        return self.queryset.all()

    def get(self, request, *args, **kwargs):
        code = request.GET.get("code")
        geojson = request.GET.get("geojson", False)
        qs = self.get_queryset()

        if code:
            instance = get_object_or_404(qs, code=code)
            props = self.get_props_from_instance(instance)

            if geojson:
                return JsonResponse(
                    {
                        "type": "Feature",
                        "properties": props,
                        "geometry": json.loads(instance.geometry.geojson),
                    }
                )
            else:
                return JsonResponse(props)

        props = [self.get_props_from_instance(d) for d in qs]

        if geojson:
            return JsonResponse(
                {"error": "impossible de récupérer tous les éléments en GEOJson"},
                status=400,
            )
        else:
            return JsonResponse({"results": props},)


class DepartementJSONView(BaseJSONView):
    queryset = Departement.objects.select_related("chef_lieu")

    def get_props_from_instance(self, departement):
        return {
            "code": departement.code,
            "nom": departement.nom,
            "population": departement.population,
            "chefLieu": {
                "nom": departement.chef_lieu.nom_complet,
                "code": departement.chef_lieu.code,
            },
        }


class RegionJSONView(BaseJSONView):
    queryset = Region.objects.select_related("chef_lieu")

    def get_props_from_instance(self, region):
        return {
            "code": region.code,
            "nom": region.nom,
            "population": region.population,
            "chefLieu": {
                "nom": region.chef_lieu.nom_complet,
                "code": region.chef_lieu.code,
            },
        }
