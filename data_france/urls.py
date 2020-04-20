from django.urls import path

from data_france import views

app_name = "data_france"
urlpatterns = [
    path("communes/", views.CommuneJSONView.as_view(), name="communes"),
    path("departements/", views.DepartementJSONView.as_view(), name="departements"),
    path("regions/", views.RegionJSONView.as_view(), name="regions"),
]
