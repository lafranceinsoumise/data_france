from django.contrib import admin
from django.urls import reverse
from django.utils.html import format_html, format_html_join, mark_safe

from data_france.admin.utils import list_of_links, ImmutableModelAdmin
from data_france.models import (
    Commune,
    EPCI,
    Departement,
    Region,
    CodePostal,
    Canton,
    CollectiviteDepartementale,
    CollectiviteRegionale,
    CirconscriptionConsulaire,
    CirconscriptionLegislative,
    EluMunicipal,
    EluDepartemental,
    EluRegional,
    Depute,
    DeputeEuropeen,
)
from data_france.typologies import Fonction


@admin.register(Commune)
class CommuneAdmin(ImmutableModelAdmin):
    fieldsets = (
        (None, {"fields": ("code", "nom_complet", "geometry_as_widget")}),
        (
            "Mairie",
            {
                "fields": (
                    "mairie_adresse",
                    "mairie_localisation_as_widget",
                    "mairie_accessibilite",
                    "mairie_accessibilite_details",
                    "mairie_horaires_display",
                    "mairie_telephone",
                    "mairie_email",
                    "mairie_site",
                )
            },
        ),
        (
            "Relations avec d'autres entités",
            {
                "fields": (
                    "type",
                    "departement_link",
                    "commune_parent_link",
                    "epci_link",
                    "codes_postaux_list",
                )
            },
        ),
        ("Population", {"fields": ("population_municipale", "population_cap")}),
        ("Conseil municipal", {"fields": ("maire", "adjoints", "conseillers")}),
    )

    list_display = ("code", "type", "nom_complet", "epci_link", "population_municipale")

    search_fields = (
        "code",
        "nom",
    )  # doit être "truthy" pour afficher le champ de recherche

    def get_queryset(self, request):
        qs = super(CommuneAdmin, self).get_queryset(request)
        if request.resolver_match.url_name == "data_france_commune_change":
            return qs.select_related("departement", "commune_parent", "epci")
        return qs

    def get_search_results(self, request, queryset, search_term):
        use_distinct = False
        if search_term:
            return queryset.search(search_term), use_distinct
        return queryset, use_distinct

    def nom_complet(self, obj):
        return obj.nom_complet

    nom_complet.short_description = "Nom complet"
    nom_complet.admin_order_field = "nom"

    def maire(self, obj):
        if obj.id:
            return list_of_links(
                obj.elus.filter(fonction=Fonction.MAIRE),
                "admin:data_france_elumunicipal_change",
            )
        return "-"

    def adjoints(self, obj):
        if obj.id:
            return list_of_links(
                obj.elus.filter(fonction=Fonction.MAIRE_ADJOINT).order_by(
                    "ordre_fonction"
                ),
                "admin:data_france_elumunicipal_change",
            )
        return "-"

    def conseillers(self, obj):
        if obj.id:
            return list_of_links(
                obj.elus.filter(fonction="").order_by("nom", "prenom"),
                "admin:data_france_elumunicipal_change",
            )
        return "-"


@admin.register(EPCI)
class EPCIAdmin(ImmutableModelAdmin):
    fieldsets = (
        (
            None,
            {
                "fields": (
                    "code",
                    "type",
                    "communes_list",
                    "geometry_as_widget",
                )
            },
        ),
        ("Conseil", {"fields": ("president", "vice_presidents", "conseillers")}),
    )

    list_display = (
        "code",
        "nom",
        "type",
    )

    search_fields = ("code", "nom")

    def get_queryset(self, request):
        qs = super().get_queryset(request)
        if request.resolver_match.url_name == "data_france_epci_change":
            return qs.prefetch_related("communes")
        return qs

    def president(self, obj):
        if obj.id:
            return list_of_links(
                EluMunicipal.objects.filter(
                    fonction_epci=Fonction.PRESIDENT, commune__epci=obj
                ),
                "admin:data_france_elumunicipal_change",
            )
        return "-"

    def vice_presidents(self, obj):
        if obj.id:
            return list_of_links(
                EluMunicipal.objects.filter(
                    fonction_epci=Fonction.VICE_PRESIDENT, commune__epci=obj
                ).order_by("nom", "prenom"),
                "admin:data_france_elumunicipal_change",
            )
        return "-"

    def conseillers(self, obj):
        if obj.id:
            return list_of_links(
                EluMunicipal.objects.filter(
                    fonction_epci="",
                    date_debut_mandat_epci__isnull=False,
                    commune__epci=obj,
                ).order_by("nom", "prenom"),
                "admin:data_france_elumunicipal_change",
            )
        return "-"


@admin.register(Departement)
class DepartementAdmin(ImmutableModelAdmin):
    list_display = ("code", "nom", "region_link", "chef_lieu_link")
    fields = list_display + (
        "population",
        "geometry_as_widget",
    )
    search_fields = ("code", "nom")

    def get_queryset(self, request):
        return super().get_queryset(request).select_related("region", "chef_lieu")


@admin.register(CollectiviteDepartementale)
class CollectiviteDepartementaleAdmin(ImmutableModelAdmin):
    list_display = ("code", "nom", "region_link")
    fields = list_display + ("population", "geometry_as_widget", "voir_aussi")
    search_fields = ("code", "nom")

    def get_queryset(self, request):
        return super().get_queryset(request).select_related("region")

    def voir_aussi(self, obj):
        if not obj:
            return "-"
        if (
            obj.code[-1] in "D"
            or obj.code.startswith("97")
            or obj.code in ["20R", "6AE"]
        ):
            if obj.code == "20R":
                deps = Departement.objects.filter(code__in=["2A", "2B"])
            elif obj.code == "6AE":
                deps = Departement.objects.filter(code__in=["67", "68"])
            else:
                deps = Departement.objects.filter(code=obj.code[:-1])

            links = [
                (
                    reverse("admin:data_france_departement_change", args=(dep.id,)),
                    f"Département {dep.nom_avec_charniere}",
                )
                for dep in deps
            ]

        elif obj.code == "75C":
            com = Commune.objects.get(code="75056")
            links = [
                (
                    reverse("admin:data_france_commune_change", args=(com.id,)),
                    "Commune de Paris (commune)",
                )
            ]

        elif obj.code == "69M":
            met = EPCI.objects.get(code="200046977")
            links = [
                (
                    reverse("admin:data_france_epci_change", args=(met.id,)),
                    "Métropole de Lyon (EPCI)",
                )
            ]

        return format_html_join(mark_safe("<br>"), '<a href="{}">{}</a>', links)

    voir_aussi.short_description = "Voir aussi"


@admin.register(Region)
class RegionAdmin(ImmutableModelAdmin):
    list_display = ("code", "nom", "chef_lieu_link")
    fields = list_display + (
        "departements_list",
        "population",
        "geometry_as_widget",
    )
    search_fields = ("nom",)

    def get_queryset(self, request):
        qs = super().get_queryset(request).select_related("chef_lieu")
        if request.resolver_match.url_name == "data_france_region_change":
            return qs.prefetch_related("departements")
        return qs


@admin.register(CollectiviteRegionale)
class CollectiviteRegionaleAdmin(ImmutableModelAdmin):
    list_display = ("code", "nom", "region_link")
    fields = list_display
    search_fields = ("nom",)

    def get_queryset(self, request):
        return super().get_queryset(request).select_related("region")


@admin.register(CodePostal)
class CodePostalAdmin(ImmutableModelAdmin):
    list_display = (
        "code",
        "communes_list",
    )
    fields = (
        "code",
        "communes_list",
    )
    search_fields = ("code",)

    def get_search_results(self, request, queryset, search_term):
        use_distinct = False
        if search_term:
            return queryset.filter(code__startswith=search_term), use_distinct
        return queryset, use_distinct

    def get_queryset(self, request):
        return super().get_queryset(request=request).prefetch_related("communes")


@admin.register(Canton)
class CantonAdmin(ImmutableModelAdmin):
    list_display = ("code", "nom_complet", "departement", "type")

    fieldsets = (
        (
            None,
            {
                "fields": [
                    "code",
                    "nom_complet",
                    "type",
                    "composition",
                    "geometry_as_widget",
                    "elus_list",
                    "departement_link",
                    "bureau_centralisateur_link",
                ]
            },
        ),
    )


@admin.register(CirconscriptionConsulaire)
class CirconscriptionConsulaireAdmin(ImmutableModelAdmin):
    list_display = (
        "nom",
        "consulats",
        "nombre_conseillers",
    )

    fields = ("nom", "consulats", "nombre_conseillers")

    search_fields = ("nom", "consulats")

    def get_search_results(self, request, queryset, search_term):
        use_distinct = False
        if search_term:
            return queryset.search(search_term), use_distinct
        return queryset, use_distinct


@admin.register(CirconscriptionLegislative)
class CirconscriptionLegislativeAdmin(ImmutableModelAdmin):
    list_display = (
        "code",
        "departement",
    )

    fields = ("code", "departement_link", "geometry_as_widget", "deputes")

    search_fields = ("departement__nom", "code")

    def deputes(self, obj):
        if obj and obj.id:
            return format_html_join(
                mark_safe("<br>"),
                '<a href="{}">{}</a>',
                (
                    (reverse("admin:data_france_depute_change", args=(d.id,)), str(d))
                    for d in obj.deputes.all()
                ),
            )
        return "-"

    deputes.short_description = "Députés"


class RNEAdmin(ImmutableModelAdmin):
    def nom_complet(self, obj):
        return f"{obj.nom.upper()}, {obj.prenom}"

    nom_complet.short_description = "Nom complet"
    nom_complet.admin_order_field = "nom"


@admin.register(EluMunicipal)
class EluMunicipalAdmin(RNEAdmin):
    search_fields = ("nom", "prenom")

    list_display = ("nom_complet", "commune", "sexe", "libelle_fonction", "actif")
    list_filter = ("actif",)

    fieldsets = (
        (
            "Identité",
            {
                "fields": [
                    "nom",
                    "prenom",
                    "sexe",
                    "date_naissance",
                    "nationalite",
                    "profession",
                ]
            },
        ),
        (
            "Mandat",
            {
                "fields": [
                    "commune_link",
                    "actif",
                    "date_debut_mandat",
                    "libelle_fonction",
                    "date_debut_fonction",
                ]
            },
        ),
        (
            "Mandat intercommunal",
            {
                "fields": [
                    "epci_link",
                    "date_debut_mandat_epci",
                    "fonction_epci",
                    "date_debut_fonction_epci",
                ]
            },
        ),
    )

    def epci_link(self, obj):
        if obj.elu_epci and obj.commune.epci:
            return format_html(
                '<a href="{}">{}</a>',
                reverse("admin:data_france_epci_change", args=(obj.commune.epci.id,)),
                obj.commune.epci.nom,
            )
        return "Pas de mandat intercommunal"

    epci_link.short_description = "EPCI"

    def get_queryset(self, request):
        qs = super(EluMunicipalAdmin, self).get_queryset(request)
        return qs.select_related("commune")

    def get_search_results(self, request, queryset, search_term):
        use_distinct = False
        if search_term:
            return queryset.search(search_term), use_distinct
        return queryset, use_distinct


@admin.register(EluDepartemental)
class EluDepartementalAdmin(RNEAdmin):
    search_fields = ("nom", "prenom")

    list_display = ("nom_complet", "canton", "libelle_fonction", "actif")
    list_filter = ("actif",)

    fieldsets = (
        (
            "Identité",
            {"fields": ["nom", "prenom", "sexe", "date_naissance", "profession"]},
        ),
        (
            "Mandat",
            {
                "fields": [
                    "canton_link",
                    "actif",
                    "date_debut_mandat",
                    "libelle_fonction",
                    "date_debut_fonction",
                ]
            },
        ),
    )


@admin.register(EluRegional)
class EluRegionalAdmin(RNEAdmin):
    search_fields = ("nom", "prenom")

    list_display = ("nom_complet", "region", "libelle_fonction", "actif")
    list_filter = ("actif",)

    fieldsets = (
        (
            "Identité",
            {"fields": ["nom", "prenom", "sexe", "date_naissance", "profession"]},
        ),
        (
            "Mandat",
            {
                "fields": [
                    "region_link",
                    "date_debut_mandat",
                    "libelle_fonction",
                    "date_debut_fonction",
                ]
            },
        ),
    )


@admin.register(Depute)
class DeputeAdmin(RNEAdmin):
    search_fields = ("nom", "prenom")
    list_display = (
        "nom_complet",
        "circonscription",
        "sexe",
        "groupe",
        "relation",
        "actif",
    )
    list_filter = ("actif",)

    fieldsets = (
        (
            "Identité",
            {
                "fields": [
                    "nom",
                    "prenom",
                    "sexe",
                    "date_naissance",
                ]
            },
        ),
        (
            "Mandat",
            {
                "fields": [
                    "legislature",
                    "code",
                    "circonscription_link",
                    "actif",
                    "date_debut_mandat",
                    "date_fin_mandat",
                ]
            },
        ),
        (
            "Groupe et parti",
            {"fields": ["groupe", "relation", "parti"]},
        ),
    )


@admin.register(DeputeEuropeen)
class DeputeEuropeenAdmin(RNEAdmin):
    search_fields = ("nom", "prenom")
    list_display = ("nom_complet", "sexe", "actif")
    list_filter = ("actif",)

    fields = [
        "nom",
        "prenom",
        "sexe",
        "date_naissance",
        "actif",
        "date_debut_mandat",
    ]
