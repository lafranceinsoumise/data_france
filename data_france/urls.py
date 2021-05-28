from django.urls import path

from data_france import views

app_name = "data_france"
urlpatterns = [
    path(
        "communes/chercher/",
        views.RechercheCommuneView.as_view(),
        name="communes-chercher",
    ),
    path(
        "communes/par-code/",
        views.CommuneParCodeView.as_view(),
        name="communes-par-code",
    ),
    path("epci/par-code/", views.EPCIParCodeView.as_view(), name="epci-par-code"),
    path(
        "departements/par-code/",
        views.DepartementParCodeView.as_view(),
        name="departements-par-code",
    ),
    path(
        "regions/par-code/", views.RegionParCodeView.as_view(), name="regions-par-code"
    ),
    path(
        "code-postal/par-code/",
        views.CodePostalParCodeView.as_view(),
        name="code-postal-par-code",
    ),
    path(
        "collectivite-departementale/par-code/",
        views.CollectiviteDepartementaleParCodeView.as_view(),
        name="collectivite-departementale-par-code",
    ),
    path(
        "collectivite-regionale/par-code/",
        views.CollectiviteRegionaleParCodeView.as_view(),
        name="collectivite-regionale-par-code",
    ),
    path(
        "circonscription-consulaire/chercher/",
        views.RechercheCirconscriptionConsulaireView.as_view(),
        name="circonscriptions-consulaires-par-code",
    ),
]
