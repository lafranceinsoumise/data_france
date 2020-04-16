from django.contrib.gis.db.models import MultiPolygonField
from django.contrib.postgres.search import SearchRank, SearchVectorField
from django.db import models

from data_france.search import PrefixSearchQuery
from data_france.type_noms import TypeNomMixin


class CommuneQueryset(models.QuerySet):
    def search(self, search_terms):
        query = PrefixSearchQuery(search_terms, config="data_france_search")

        return (
            self.filter(search=query)
            .annotate(rank=SearchRank(models.F("search"), query))
            .order_by("-rank")
        )


class Commune(TypeNomMixin, models.Model):
    TYPE_COMMUNE = "COM"
    TYPE_COMMUNE_DELEGUEE = "COMD"
    TYPE_COMMUNE_ASSOCIEE = "COMA"
    TYPE_ARRONDISSEMENT_PLM = "ARM"
    TYPE_SECTEUR_PLM = "SRM"
    TYPE_CHOICES = (
        (TYPE_COMMUNE, "Commune"),
        (TYPE_COMMUNE_DELEGUEE, "Commune déléguée"),
        (TYPE_COMMUNE_ASSOCIEE, "Commune associée"),
        (TYPE_ARRONDISSEMENT_PLM, "Arrondissement de Paris/Lyon/Marseille"),
        (TYPE_SECTEUR_PLM, "Secteur électoral de Paris/Lyon/Marseille"),
    )

    objects = CommuneQueryset.as_manager()

    code = models.CharField("Code INSEE", max_length=10, editable=False)
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

    search = SearchVectorField("Champ de recherche", null=True, editable=False)

    @property
    def avec_conseil(self):
        return self.type in (self.TYPE_COMMUNE, self.TYPE_SECTEUR_PLM)

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
            models.CheckConstraint(
                check=(
                    models.Q(type="COM", departement_id__isnull=False)
                    | (models.Q(departement__isnull=True) & ~models.Q(type="COM"))
                ),
                name="commune_departement_constraint",
            ),
            models.UniqueConstraint(
                fields=["type", "code"], name="commune_unique_code",
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

    code = models.CharField("Code SIREN", max_length=10, editable=False, unique=True,)

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
    code = models.CharField("Code INSEE", max_length=3, editable=False, unique=True)
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
    code = models.CharField("Code INSEE", max_length=3, editable=False, unique=True)
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


class CodePostal(models.Model):
    code = models.CharField("Code postal", max_length=5, editable=False, unique=True)

    communes = models.ManyToManyField(
        to="Commune",
        verbose_name="Communes dans ce code postal",
        related_name="codes_postaux",
        related_query_name="codes_postaux",
    )

    def __str__(self):
        return self.code

    class Meta:
        verbose_name = "Code postal"
        verbose_name_plural = "Codes postaux"
        ordering = ("code",)


class CollectiviteDepartementale(models.Model):
    TYPE_CONSEIL_DEPARTEMENTAL = "D"
    TYPE_CONSEIL_METROPOLE = "M"
    TYPE_CHOICES = (
        (TYPE_CONSEIL_DEPARTEMENTAL, "Conseil départemental"),
        (TYPE_CONSEIL_METROPOLE, "Conseil de métropole"),
    )

    code = models.CharField("Code INSEE", max_length=4, unique=True)
    type = models.CharField(
        "Type de collectivité départementale", max_length=1, choices=TYPE_CHOICES
    )
    actif = models.BooleanField("En cours d'existence", default=True)

    nom = models.CharField("Nom", max_length=200)
    departement = models.ForeignKey(
        "Departement",
        on_delete=models.PROTECT,
        verbose_name="Circonscription administrative correspondante",
    )

    population = models.PositiveIntegerField("Population", null=True)

    geometry = MultiPolygonField("Géométrie", geography=True, srid=4326, null=True)

    class Meta:
        verbose_name = "Collectivité à compétences départementales"
        verbose_name_plural = "Collectivités à compétences départementales"
        ordering = ("code",)


class CollectiviteRegionale(models.Model):
    TYPE_CONSEIL_REGIONAL = "R"
    TYPE_COLLECTIVITE_UNIQUE = "U"
    TYPE_CHOICES = (
        (TYPE_CONSEIL_REGIONAL, "Conseil régional"),
        (TYPE_COLLECTIVITE_UNIQUE, "Collectivité territoriale unique"),
    )

    code = models.CharField("Code INSEE", max_length=4, unique=True)
    type = models.CharField(
        "Type de collectivité départementale", max_length=1, choices=TYPE_CHOICES
    )

    actif = models.BooleanField("En cours d'existence", default=True)

    nom = models.CharField("Nom", max_length=200)
    region = models.OneToOneField(
        to="Region",
        on_delete=models.PROTECT,
        related_name="collectivite",
        verbose_name="Circonscription administrative correspondante",
    )

    class Meta:
        verbose_name = "Collectivité à compétences régionales"
        verbose_name_plural = "Collectivités à compétences régionales"
        ordering = ("nom",)
