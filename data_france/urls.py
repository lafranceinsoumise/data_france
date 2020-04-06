from django.urls import path

from data_france import views

app_name = "data_france"
urlpatterns = [
    path(
        "chercher/communes/", views.CommuneSearchView.as_view(), name="chercher_commune"
    )
]
