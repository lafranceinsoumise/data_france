from functools import partial

from django.contrib.admin import ModelAdmin
from django.contrib.gis.admin import GeoModelAdmin, OSMGeoAdmin
from django.contrib.gis.db.models import GeometryField
from django.db.models import (
    ForeignObjectRel,
    ForeignObject,
    OneToOneRel,
    ManyToOneRel,
    ManyToManyRel,
    ManyToManyField,
)
from django.db.models.fields.related import RelatedField
from django.urls import reverse
from django.utils.html import format_html_join, format_html
from django.utils.safestring import mark_safe


def list_of_links(qs, view_name):
    return format_html_join(
        mark_safe("<br>"),
        '<a href="{}">{}</a>',
        ((reverse(view_name, args=(obj.pk,)), str(obj)) for obj in qs),
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
        return list_of_links(qs, view_name)


class ImmutableModelAdmin(AddRelatedLinkMixin, ReadOnlyGeometryMixin, OSMGeoAdmin):
    def has_add_permission(self, request, obj=None):
        return False

    def has_delete_permission(self, request, obj=None):
        return False

    def has_change_permission(self, request, obj=None):
        return False
