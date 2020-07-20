import json

from django.http import JsonResponse
from django.shortcuts import get_object_or_404
from django.views import View

from data_france.forms import (
    CommuneParametresForm,
    ParCodeParametresForm,
    CommuneParCodeParametresForm,
)
from data_france.models import Commune, Departement, Region, EPCI


class CommuneMixin:
    def get_props_from_instance(self, commune):
        return {
            "code": commune.code,
            "type": commune.type,
            "nom": commune.nom_complet,
            "code_departement": commune.code_departement,
        }


class RechercheCommuneView(CommuneMixin, View):
    def get(self, request, *args, **kwargs):
        params = CommuneParametresForm(data=request.GET)

        if params.is_valid():
            q = params.cleaned_data["q"]
            types = params.cleaned_data.get("type") or [
                t for t, _ in Commune.TYPE_CHOICES
            ]

            geojson = params.cleaned_data["geojson"]

            qs = (
                Commune.objects.search(q)
                .filter(type__in=types)
                .select_related("departement", "commune_parent__departement")[:10]
            )

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

        return JsonResponse({"errors": params.errors}, status=400)


class BaseParCodeView(View):
    queryset = None
    form_class = ParCodeParametresForm

    def get_props_from_instance(self, instance):
        return instance.as_dict()

    def get_queryset(self):
        return self.queryset.all()

    def get(self, request, *args, **kwargs):
        params = self.form_class(data=request.GET)

        if not params.is_valid():
            return JsonResponse({"errors": params.errors}, status=400)

        geojson = params.cleaned_data.get("geojson", False)
        other_params = {k: v for k, v in params.cleaned_data.items() if k != "geojson"}

        qs = self.get_queryset()

        instance = get_object_or_404(qs, **other_params)
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


class CommuneParCodeView(CommuneMixin, BaseParCodeView):
    queryset = Commune.objects.select_related(
        "departement", "commune_parent__departement"
    )
    form_class = CommuneParCodeParametresForm


class EPCIParCodeView(BaseParCodeView):
    queryset = EPCI.objects.all()


class DepartementParCodeView(BaseParCodeView):
    queryset = Departement.objects.select_related("chef_lieu")


class RegionParCodeView(BaseParCodeView):
    queryset = Region.objects.select_related("chef_lieu")
