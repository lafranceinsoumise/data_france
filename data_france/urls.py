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
]
