from django.contrib.gis.db.models import GeometryField, MultiPolygonField
from django.db import models

from data_france.type_noms import TYPE_NOM_ARTICLE


class Commune(models.Model):
    TYPE_COMMUNE = "COM"
    TYPE_COMMUNE_DELEGUEE = "COMD"
    TYPE_COMMUNE_ASSOCIEE = "COMA"
    TYPE_ARRONDISSEMENT_PLM = "ARM"
    TYPE_SECTEUR_PLM = "SEC"
    TYPE_CHOICES = (
        (TYPE_COMMUNE, "Commune"),
        (TYPE_COMMUNE_DELEGUEE, "Commune déléguée"),
        (TYPE_COMMUNE_ASSOCIEE, "Commune associée"),
        (TYPE_ARRONDISSEMENT_PLM, "Arrondissement de Paris/Lyon/Marseille"),
        (TYPE_SECTEUR_PLM, "Secteur électoral de Paris/Lyon/Marseille"),
    )

    code = models.CharField("Code de la commune", max_length=10, editable=False)
    type = models.CharField(
        "Type de commune",
        max_length=4,
        blank=False,
        editable=False,
        null=False,
        default="COM",
        choices=TYPE_CHOICES,
    )

    nom = models.CharField(
        "Nom de la commune", max_length=200, blank=False, editable=False, null=False,
    )

    type_nom = models.PositiveSmallIntegerField(
        "Type de nom de la commune", blank=False, editable=False, null=False
    )

    code_departement = models.CharField(
        "Code du département", max_length=5, editable=False
    )

    epci = models.ForeignKey(
        "data_france.EPCI",
        on_delete=models.SET_NULL,
        related_name="communes",
        related_query_name="commune",
        null=True,
        editable=False,
    )

    population_municipale = models.PositiveIntegerField(
        "Population municipale", null=True, editable=False
    )
    population_cap = models.PositiveIntegerField(
        "Population comptée à part", null=True, editable=False
    )

    commune_parent = models.ForeignKey(
        "Commune",
        on_delete=models.CASCADE,
        related_name="composants",
        related_query_name="composant",
        null=True,
        editable=False,
    )

    geometry = MultiPolygonField(
        "Géométrie", geography=True, srid=4326, null=True, editable=False
    )

    @property
    def nom_complet(self):
        return f"{TYPE_NOM_ARTICLE[self.type_nom].title()}{self.nom}"

    def __str__(self):
        return f"{self.nom_complet} ({self.code})"

    class Meta:
        verbose_name = "Commune"
        verbose_name_plural = "Communes"

        ordering = ("code", "nom", "type")

        constraints = (
            models.CheckConstraint(
                check=(
                    models.Q(type="COM", commune_parent__isnull=True)
                    | (models.Q(commune_parent__isnull=False) & ~models.Q(type="COM"))
                ),
                name="commune_deleguees_associees_constraint",
            ),
            models.UniqueConstraint(
                fields=["code"],
                name="commune_unique_code",
                condition=models.Q(type="COM"),
            ),
        )


class EPCI(models.Model):
    TYPE_CA = "CA"
    TYPE_CC = "CC"
    TYPE_CU = "CU"
    TYPE_METROPOLE = "ME"

    TYPE_CHOICES = (
        (TYPE_CA, "Communauté d'agglomération"),
        (TYPE_CC, "Communauté de communes",),
        (TYPE_CU, "Communauté urbaine"),
        (TYPE_METROPOLE, "Métropole"),
    )

    code = models.CharField("Code", max_length=10, editable=False, unique=True,)

    type = models.CharField("Type d'EPCI", max_length=2, choices=TYPE_CHOICES)

    nom = models.CharField("Nom de l'EPCI", max_length=300)

    def __str__(self):
        return f"{self.nom} ({self.code})"

    class Meta:
        verbose_name = "EPCI"
        verbose_name_plural = "EPCIs"

        ordering = ("code", "nom")
