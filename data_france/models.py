from django.contrib.gis.db.models import MultiPolygonField
from django.contrib.postgres.search import SearchVector, SearchQuery, SearchRank
from django.db import models

from data_france.search import PrefixSearchQuery
from data_france.type_noms import TypeNomMixin


class CommuneQueryset(models.QuerySet):
    def search(self, search_terms):
        vector = SearchVector(
            models.F("nom"), config="data_france_search", weight="A"
        ) + SearchVector(models.F("code"), config="data_france_search", weight="B")

        query = PrefixSearchQuery(search_terms, config="data_france_search")

        return (
            self.annotate(search=vector)
            .filter(search=query)
            .annotate(rank=SearchRank(vector, query))
            .order_by("-rank")
        )


class Commune(TypeNomMixin, models.Model):
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

    objects = CommuneQueryset.as_manager()

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

    departement = models.ForeignKey(
        "Departement",
        verbose_name="Département",
        on_delete=models.PROTECT,
        related_name="communes",
        related_query_name="commune",
        null=True,
        editable=False,
    )

    @property
    def code_departement(self):
        if self.commune_parent_id:
            return self.commune_parent.departement.code
        return self.departement.code

    epci = models.ForeignKey(
        "EPCI",
        verbose_name="EPCI",
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

    geometry = MultiPolygonField("Géométrie", geography=True, srid=4326, null=True)

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

    population = models.PositiveIntegerField("Population", null=True)

    geometry = MultiPolygonField("Géométrie", geography=True, srid=4326, null=True)

    def __str__(self):
        return f"{self.nom} ({self.code})"

    class Meta:
        verbose_name = "EPCI"
        verbose_name_plural = "EPCI"

        ordering = ("code", "nom")


class Departement(TypeNomMixin, models.Model):
    code = models.CharField("Code", max_length=3, editable=False, unique=True)
    nom = models.CharField("Nom du département", max_length=200, editable=False)
    type_nom = models.PositiveSmallIntegerField(
        "Type de nom du département", blank=False, editable=False, null=False
    )

    chef_lieu = models.ForeignKey(
        "Commune",
        verbose_name="Chef-lieu",
        on_delete=models.PROTECT,
        related_name="+",
        related_query_name="chef_lieu_de",
    )
    region = models.ForeignKey(
        "Region",
        verbose_name="Région",
        on_delete=models.PROTECT,
        related_name="departements",
        related_query_name="departement",
    )

    population = models.PositiveIntegerField("Population", null=True)

    geometry = MultiPolygonField("Géométrie", geography=True, srid=4326, null=True)

    def __str__(self):
        return f"{self.nom} ({self.code})"

    class Meta:
        verbose_name = "Département"
        ordering = ("code",)


class Region(TypeNomMixin, models.Model):
    code = models.CharField("Code", max_length=3, editable=False, unique=True)
    nom = models.CharField("Nom de la région", max_length=200, editable=False)
    type_nom = models.PositiveSmallIntegerField(
        "Type de nom", blank=False, editable=False, null=False
    )

    chef_lieu = models.ForeignKey(
        "Commune",
        verbose_name="Chef-lieu",
        on_delete=models.PROTECT,
        related_name="+",
        related_query_name="chef_lieu_de",
    )

    population = models.PositiveIntegerField("Population", null=True)

    geometry = MultiPolygonField("Géométrie", geography=True, srid=4326, null=True)

    def __str__(self):
        return self.nom

    class Meta:
        verbose_name = "Région"
        ordering = ("nom",)  # personne ne connait les codes de région
