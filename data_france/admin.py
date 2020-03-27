from django.contrib import admin
from django.urls import reverse
from django.utils.html import format_html, format_html_join
from django.utils.safestring import mark_safe

from data_france.models import Commune, EPCI


@admin.register(Commune)
class CommuneAdmin(admin.ModelAdmin):
    readonly_fields = (
        "code",
        "type",
        "nom_complet",
        "code_departement",
        "lien_epci",
        "population_municipale",
        "population_cap",
        "commune_parent",
        "geometry",
    )

    fieldsets = (
        (None, {"fields": ("code", "nom_complet")}),
        (
            "Relations avec d'autres entités",
            {"fields": ("type", "code_departement", "commune_parent", "lien_epci")},
        ),
        ("Population", {"fields": ("population_municipale", "population_cap")}),
    )

    list_display = ("code", "type", "nom_complet", "lien_epci", "population_municipale")

    search_fields = ("code", "nom")

    def nom_complet(self, obj):
        return obj.nom_complet

    nom_complet.short_description = "Nom complet"
    nom_complet.admin_order_field = "nom"

    def lien_epci(self, obj):
        if obj.epci:
            return format_html(
                '<a href="{}">{}</a>',
                reverse("admin:data_france_epci_change", args=[obj.epci_id]),
                str(obj.epci),
            )
        return "-"

    lien_epci.short_description = "EPCI"


@admin.register(EPCI)
class EPCIAdmin(admin.ModelAdmin):
    readonly_fields = ("code", "type", "nom", "communes")
    fields = readonly_fields
    list_display = (
        "code",
        "nom",
        "type",
    )

    def communes(self, obj):
        return format_html_join(
            mark_safe("<br>"),
            '<a href="{}">{}</a>',
            (
                (
                    reverse("admin:data_france_commune_change", args=[commune.id]),
                    str(commune),
                )
                for commune in obj.communes.all()
            ),
        )

    communes.short_description = "Communes membres"

    search_fields = ("code", "nom")
