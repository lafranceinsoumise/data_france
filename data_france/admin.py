from functools import partial

from django.contrib import admin
from django.contrib.admin import ModelAdmin
from django.contrib.gis.admin import OSMGeoAdmin, GeoModelAdmin
from django.contrib.gis.db.models import GeometryField
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

from data_france.models import (
    Commune,
    EPCI,
    Departement,
    Region,
    CodePostal,
    CollectiviteDepartementale,
    CollectiviteRegionale,
    EluMunicipal,
)


class ReadOnlyGeometryMixin(GeoModelAdmin):
    def __init__(self, model, admin_site):
        super().__init__(model, admin_site)

        self._additional_geometry_widgets = []

        for f in model._meta.get_fields():
            if isinstance(f, GeometryField):
                verbose_name = f.verbose_name
                new_name = f"{f.name}_as_widget"

                method = partial(self._display_geometry_field_with_widget, field=f)
                method.short_description = verbose_name

                setattr(self, new_name, method)

    def get_readonly_fields(self, request, obj=None):
        return super().get_readonly_fields(request, obj) + tuple(
            self._additional_geometry_widgets
        )

    def _display_geometry_field_with_widget(self, obj, *, field: GeometryField):
        value = getattr(obj, field.name, None)
        if value is None:
            return "-"

        widget = self.get_map_widget(field)
        widget.params["modifiable"] = False
        form_field = field.formfield(widget=widget)
        return form_field.widget.render(
            field.name + "_as_widget", value, attrs={"id": f"id_{field.name}"}
        )


class AddRelatedLinkMixin(ModelAdmin):
    def __init__(self, model, admin_site):
        super().__init__(model, admin_site)

        self._additional_related_fields = []

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
                    verbose_name = f.verbose_name

                view_name = "admin:%s_%s_change" % (
                    f.related_model._meta.app_label,
                    f.related_model._meta.model_name,
                )

                if isinstance(f, (ForeignObject, OneToOneRel)):
                    get_link = partial(
                        self._get_link, attr_name=attr_name, view_name=view_name
                    )
                    get_link.short_description = verbose_name
                    get_link.admin_order_field = f.name

                    link_attr_name = f"{attr_name}_link"
                    setattr(self, link_attr_name, get_link)
                    self._additional_related_fields.append(link_attr_name)

                elif isinstance(f, (ManyToOneRel, ManyToManyRel, ManyToManyField)):
                    get_list = partial(
                        self._get_list, attr_name=attr_name, view_name=view_name
                    )

                    get_list.short_description = verbose_name

                    link_attr_name = f"{attr_name}_list"

                    setattr(self, link_attr_name, get_list)
                    self._additional_related_fields.append(link_attr_name)

    def get_readonly_fields(self, request, obj=None):
        return super().get_readonly_fields(request, obj) + tuple(
            self._additional_related_fields
        )

    def _get_link(self, obj, *, attr_name, view_name):
        if hasattr(obj, attr_name) and getattr(obj, attr_name, None) is not None:
            value = getattr(obj, attr_name)
            return format_html(
                '<a href="{link}">{text}</a>',
                link=reverse(view_name, args=(value.pk,)),
                text=str(value),
            )
        return "-"

    def _get_list(self, obj, *, attr_name, view_name):
        qs = getattr(obj, attr_name).all()
        if not qs.exists():
            return "-"
        return format_html_join(
            mark_safe("<br>"),
            '<a href="{}">{}</a>',
            ((reverse(view_name, args=[rel.id]), str(rel),) for rel in qs),
        )


class ImmutableModelAdmin(AddRelatedLinkMixin, ReadOnlyGeometryMixin, OSMGeoAdmin):
    def has_add_permission(self, request, obj=None):
        return False

    def has_delete_permission(self, request, obj=None):
        return False

    def has_change_permission(self, request, obj=None):
        return False


@admin.register(Commune)
class CommuneAdmin(ImmutableModelAdmin):
    fieldsets = (
        (None, {"fields": ("code", "nom_complet", "geometry_as_widget")}),
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


@admin.register(EPCI)
class EPCIAdmin(ImmutableModelAdmin):
    fields = (
        "code",
        "type",
        "communes_list",
        "geometry_as_widget",
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


@admin.register(Departement)
class DepartementAdmin(ImmutableModelAdmin):
    list_display = ("code", "nom", "region_link", "chef_lieu_link")
    fields = list_display + ("population", "geometry_as_widget",)
    search_fields = ("code", "nom")

    def get_queryset(self, request):
        return super().get_queryset(request).select_related("region", "chef_lieu")


@admin.register(CollectiviteDepartementale)
class CollectiviteDepartementaleAdmin(ImmutableModelAdmin):
    list_display = ("code", "nom", "departement_link")
    fields = list_display + ("population", "geometry_as_widget")
    search_fields = ("code", "nom")

    def get_queryset(self, request):
        return super().get_queryset(request).select_related("departement")


@admin.register(Region)
class RegionAdmin(ImmutableModelAdmin):
    list_display = ("code", "nom", "chef_lieu_link")
    fields = list_display + ("departements_list", "population", "geometry_as_widget",)
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


@admin.register(EluMunicipal)
class EluMunicipalAdmin(ImmutableModelAdmin):
    search_fields = ("nom", "prenom")

    list_display = ("nom_complet", "commune", "sexe", "fonction")
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
                    "date_debut_mandat",
                    "fonction",
                    "date_debut_fonction",
                ]
            },
        ),
    )

    def nom_complet(self, obj):
        return f"{obj.nom}, {obj.prenom}"

    nom_complet.short_description = "Nom complet"
    nom_complet.admin_order_field = "nom"

    def get_queryset(self, request):
        qs = super(EluMunicipalAdmin, self).get_queryset(request)
        return qs.select_related("commune")

    def get_search_results(self, request, queryset, search_term):
        use_distinct = False
        if search_term:
            return queryset.search(search_term), use_distinct
        return queryset, use_distinct
