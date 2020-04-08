from functools import partial

from django.contrib import admin
from django.contrib.gis.admin import OSMGeoAdmin
from django.db.models import (
    ForeignObject,
    OneToOneRel,
    ManyToManyRel,
    ManyToOneRel,
    ManyToManyField,
)
from django.db.models.fields.related import RelatedField
from django.db.models.fields.reverse_related import ForeignObjectRel
from django.urls import reverse
from django.utils.html import format_html, format_html_join
from django.utils.safestring import mark_safe

from data_france.models import Commune, EPCI, Departement, Region


class AddRelatedLinkMixin:
    @classmethod
    def get_link(cls, obj, attr_name, view_name):
        if hasattr(obj, attr_name) and getattr(obj, attr_name, None) is not None:
            value = getattr(obj, attr_name)
            return format_html(
                '<a href="{link}">{text}</a>',
                link=reverse(view_name, args=(value.pk,)),
                text=str(value),
            )
        return "-"

    @classmethod
    def get_list(cls, obj, attr_name, view_name):
        qs = getattr(obj, attr_name).all()
        if not qs.exists():
            return "-"
        return format_html_join(
            mark_safe("<br>"),
            '<a href="{}">{}</a>',
            ((reverse(view_name, args=[rel.id]), str(rel),) for rel in qs),
        )

    def __init__(self, model, admin_site):
        super().__init__(model, admin_site)

        self.readonly_fields = list(self.readonly_fields)

        for f in model._meta.get_fields():
            if (
                isinstance(f, (RelatedField, ForeignObjectRel))
                and f.related_model is not None
            ):
                if isinstance(f, ForeignObjectRel):
                    attr_name = f.get_accessor_name()
                    verbose_name = f.related_model._meta.verbose_name_plural
                else:
                    attr_name = f.name

                view_name = "admin:%s_%s_change" % (
                    f.related_model._meta.app_label,
                    f.related_model._meta.model_name,
                )

                if isinstance(f, (ForeignObject, OneToOneRel)):
                    get_link = partial(
                        self.get_link, attr_name=attr_name, view_name=view_name
                    )
                    get_link.short_description = f.verbose_name
                    get_link.admin_order_field = f.name

                    link_attr_name = f"{attr_name}_link"
                    setattr(self, link_attr_name, get_link)
                    self.readonly_fields.append(link_attr_name)

                elif isinstance(f, (ManyToOneRel, ManyToManyRel, ManyToManyField)):
                    get_list = partial(
                        self.get_list, attr_name=attr_name, view_name=view_name
                    )
                    get_list.short_description = (
                        f.related_model._meta.verbose_name_plural
                    )

                    link_attr_name = f"{attr_name}_list"

                    setattr(self, link_attr_name, get_list)
                    self.readonly_fields.append(link_attr_name)


class ImmutableModelAdmin(AddRelatedLinkMixin, OSMGeoAdmin):
    modifiable = False

    def has_add_permission(self, request, obj=None):
        return False

    def has_delete_permission(self, request, obj=None):
        return False


@admin.register(Commune)
class CommuneAdmin(ImmutableModelAdmin):
    readonly_fields = (
        "code",
        "type",
        "nom_complet",
        "population_municipale",
        "population_cap",
    )

    fieldsets = (
        (None, {"fields": ("code", "nom_complet", "geometry")}),
        (
            "Relations avec d'autres entités",
            {
                "fields": (
                    "type",
                    "departement_link",
                    "commune_parent_link",
                    "epci_link",
                )
            },
        ),
        ("Population", {"fields": ("population_municipale", "population_cap")}),
    )

    list_display = ("code", "type", "nom_complet", "epci_link", "population_municipale")

    search_fields = (
        "code",
        "nom",
    )  # doit être "truthy" pour afficher le champ de recherche

    def get_search_results(self, request, queryset, search_term):
        use_distinct = False
        if search_term:
            return queryset.search(search_term), use_distinct
        return queryset, use_distinct

    def nom_complet(self, obj):
        return obj.nom_complet

    nom_complet.short_description = "Nom complet"
    nom_complet.admin_order_field = "nom"


@admin.register(EPCI)
class EPCIAdmin(ImmutableModelAdmin):
    readonly_fields = ("code", "type", "nom")
    fields = (
        "code",
        "type",
        "communes_list",
        "geometry",
    )
    list_display = (
        "code",
        "nom",
        "type",
    )

    search_fields = ("code", "nom")


@admin.register(Departement)
class DepartementAdmin(ImmutableModelAdmin):
    modifiable = False  # pour GeoModelAdmin
    readonly_fields = ("code", "nom", "population")

    list_display = ("code", "nom", "region_link", "chef_lieu_link")
    fields = list_display + ("population", "geometry",)


@admin.register(Region)
class RegionAdmin(ImmutableModelAdmin):
    modifiable = False  # pour GeoModelAdmin
    readonly_fields = ("code", "nom", "population")

    list_display = ("code", "nom", "chef_lieu_link")
    fields = list_display + ("population", "geometry",)
