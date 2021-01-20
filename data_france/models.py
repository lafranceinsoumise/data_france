from django.contrib.gis.db.models import MultiPolygonField
from django.contrib.postgres.search import SearchRank, SearchVectorField
from django.db import models

from data_france.search import PrefixSearchQuery
from data_france.type_noms import TypeNomMixin


class SearchQueryset(models.QuerySet):
    def search(self, search_terms):
        query = PrefixSearchQuery(search_terms, config="data_france_search")

        return (
            self.filter(search=query)
            .annotate(rank=SearchRank(models.F("search"), query))
            .order_by("-rank")
        )


class Commune(TypeNomMixin, models.Model):
    class TypeCommune(models.TextChoices):
        COMMUNE = "COM", "Commune"
        COMMUNE_DELEGUEE = "COMD", "Commune déléguée"
        COMMUNE_ASSOCIEE = "COMA", "Commune associée"
        ARRONDISSEMENT_PLM = "ARM", "Arrondissement de Paris/Lyon/Marseille"
        SECTEUR_PLM = "SRM", "Secteur électoral de Paris/Lyon/Marseille"

    TYPE_COMMUNE = TypeCommune.COMMUNE
    TYPE_COMMUNE_DELEGUEE = TypeCommune.COMMUNE_DELEGUEE
    TYPE_COMMUNE_ASSOCIEE = TypeCommune.COMMUNE_ASSOCIEE
    TYPE_ARRONDISSEMENT_PLM = TypeCommune.ARRONDISSEMENT_PLM
    TYPE_SECTEUR_PLM = TypeCommune.SECTEUR_PLM

    objects = SearchQueryset.as_manager()

    code = models.CharField("Code INSEE", max_length=10, editable=False)
    type = models.CharField(
        "Type de commune",
        max_length=4,
        blank=False,
        editable=False,
        null=False,
        default="COM",
        choices=TypeCommune.choices,
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
        return self.type in (self.TypeCommune.COMMUNE, self.TypeCommune.SECTEUR_PLM)

    def __str__(self):
        return f"{self.nom_complet} ({self.code})"

    def as_dict(self):
        return {
            "code": self.code,
            "type": self.type,
            "nom": self.nom_complet,
            "code_departement": self.code_departement,
        }

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
    class TypeEPCI(models.TextChoices):
        CA = "CA", "Communauté d'agglomération"
        CC = "CC", "Communauté de communes"
        CU = "CU", "Communauté urbaine"
        METROPOLE = "ME", "Métropole"

    TYPE_CA = "CA"
    TYPE_CC = "CC"
    TYPE_CU = "CU"
    TYPE_METROPOLE = "ME"

    code = models.CharField("Code SIREN", max_length=10, editable=False, unique=True,)

    type = models.CharField("Type d'EPCI", max_length=2, choices=TypeEPCI.choices)

    nom = models.CharField("Nom de l'EPCI", max_length=300)

    population = models.PositiveIntegerField("Population", null=True)

    geometry = MultiPolygonField("Géométrie", geography=True, srid=4326, null=True)

    def __str__(self):
        return f"{self.nom} ({self.code})"

    def as_dict(self):
        return {
            "code": self.code,
            "nom": self.nom,
            "type": self.type,
            "population": self.population,
            "communes": {c.code: c.nom for c in self.communes.all()},
        }

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

    def as_dict(self):
        return {
            "code": self.code,
            "nom": self.nom,
            "population": self.population,
            "chefLieu": {
                "nom": self.chef_lieu.nom_complet,
                "code": self.chef_lieu.code,
            },
        }

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

    def get_props_from_instance(self):
        return {
            "code": self.code,
            "nom": self.nom,
            "population": self.population,
            "chefLieu": {
                "nom": self.chef_lieu.nom_complet,
                "code": self.chef_lieu.code,
            },
        }

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

    def as_dict(self):
        return {
            "code": self.code,
            "communes": {c.code: c.nom_complet for c in self.communes.all()},
        }

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

    def __str__(self):
        return f"{self.nom} ({self.code})"


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

    def __str__(self):
        return self.nom


class Canton(models.Model):
    TYPE_CANTON = "C"
    TYPE_CANTON_VILLE = "V"
    TYPE_CANTON_FICTIF = "N"
    TYPE_CHOICES = (
        (TYPE_CANTON, "Canton"),
        (TYPE_CANTON_VILLE, "Canton-Ville (ou pseudo-canton)"),
        (TYPE_CANTON_FICTIF, "Canton « fictif » pour communes nouvelles"),
    )

    COMPOSITION_COMMUNE_ENTIERE = 1
    COMPOSITION_FRACTION_ET_COMMUNES = 2
    COMPOSITION_FRACTIONS_ET_COMMUNES = 3
    COMPOSITION_FRACTION = 4
    COMPOSITION_FRACTIONS = 5
    COMPOSITION_CHOICES = (
        (COMPOSITION_COMMUNE_ENTIERE, "Canton composé de commune(s) entière(s)"),
        (
            COMPOSITION_FRACTION_ET_COMMUNES,
            "Canton composé d'une fraction d'une commune et de commune(s) entière(s)",
        ),
        (
            COMPOSITION_FRACTIONS_ET_COMMUNES,
            "Canton composé de fractions de plusieurs communes et de commune(s) entière(s)",
        ),
        (COMPOSITION_FRACTION, "Canton composé d'une fraction de commune"),
        (COMPOSITION_FRACTIONS, "Canton composé de fractions de plusieurs communes"),
    )

    code = models.CharField("Code INSEE", max_length=5, unique=True)
    type = models.CharField(
        "Type de canton",
        max_length=1,
        choices=TYPE_CHOICES,
        blank=False,
        editable=False,
    )
    composition = models.IntegerField(
        "Composition du canton", choices=COMPOSITION_CHOICES, editable=False, null=True
    )

    nom = models.CharField(
        "Nom du canton", max_length=200, blank=False, editable=False, null=False,
    )

    type_nom = models.PositiveSmallIntegerField(
        "Type de nom du canton", blank=False, editable=False, null=False
    )

    departement = models.ForeignKey(
        "Departement",
        verbose_name="Département",
        on_delete=models.PROTECT,
        related_name="cantons",
        related_query_name="canton",
    )

    bureau_centralisateur = models.ForeignKey(
        "Commune",
        verbose_name="Bureau centralisateur",
        null=True,
        on_delete=models.PROTECT,
        related_name="+",
        related_query_name="bureau_centralisateur_de",
    )


class EluMunicipal(models.Model):
    objects = SearchQueryset.as_manager()

    class CodeSexe(models.TextChoices):
        MASCULIN = "M"
        FEMININ = "F"

    commune = models.ForeignKey(
        to="Commune",
        on_delete=models.PROTECT,
        related_name="elus",
        related_query_name="elu",
        editable=False,
    )

    nom = models.CharField(
        verbose_name="Nom de famille", editable=False, max_length=200
    )
    prenom = models.CharField(
        verbose_name="Prénom", editable=False, blank=True, max_length=200
    )
    sexe = models.CharField(
        verbose_name="Sexe à l'état civil",
        choices=CodeSexe.choices,
        max_length=1,
        editable=True,
    )
    date_naissance = models.DateField(verbose_name="Date de naissance", editable=False)
    profession = models.SmallIntegerField(
        verbose_name="Profession", editable=False, null=True
    )

    date_debut_mandat = models.DateField(
        verbose_name="Date de début du mandat", editable=False
    )

    fonction = models.CharField(
        verbose_name="Fonction", editable=False, blank=True, max_length=50
    )
    date_debut_fonction = models.DateField(
        verbose_name="Date de début de la fonction", editable=True, null=True
    )

    nationalite = models.CharField(
        verbose_name="Nationalité", editable=False, max_length=30
    )

    search = SearchVectorField(verbose_name="Champ de recherche", null=True)

    def __str__(self):
        return f"{self.nom}, {self.prenom} ({self.commune.nom_complet})"

    class Meta:
        verbose_name = "Élu⋅e municipal⋅e"
        verbose_name_plural = "Élu⋅es municipaux⋅les"
        ordering = ("commune", "nom", "prenom", "date_naissance")
