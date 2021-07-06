from django.contrib.gis.db.models import MultiPolygonField, PointField
from django.contrib.postgres.search import SearchRank, SearchVectorField
from django.db import models
from django.utils.html import format_html_join

from data_france.search import PrefixSearchQuery
from data_france.typologies import (
    CodeSexe,
    Fonction,
    NOMS_COM,
    ORDINAUX_LETTRES,
    RelationGroupe,
    TypeNom,
)
from django.contrib.postgres.fields.array import ArrayField
from django.contrib.postgres.indexes import GinIndex

__all__ = [
    "Commune",
    "EPCI",
    "Canton",
    "Departement",
    "Region",
    "CodePostal",
    "CollectiviteDepartementale",
    "CollectiviteRegionale",
    "CirconscriptionLegislative",
    "CirconscriptionConsulaire",
    "Depute",
    "EluMunicipal",
]

JOURS_SEMAINE = [
    "lundi",
    "mardi",
    "mercredi",
    "jeudi",
    "vendredi",
    "samedi",
    "dimanche",
]


def _horaires_sort_key(j):
    return (JOURS_SEMAINE.index(j[0]), JOURS_SEMAINE.index(j[1]), tuple(j[2]))


class TypeNomMixin(models.Model):
    """Mixin de modèle pour ajouter les bons articles et charnières aux noms de lieux

    Ce mixin ajoute un champ de modèle :py:attr:`type_nom` pour sauvegarder le
    type de nom selon la nomenclature de l'INSEE
    """

    type_nom = models.PositiveSmallIntegerField(
        "Type de nom",
        blank=False,
        editable=False,
        null=False,
        choices=TypeNom.choices,
    )

    @property
    def nom_complet(self):
        """Retourne le nom complet de l'entité, avec l'article éventuel.

        :return: Le nom complet de l'entité, avec l'article si approprié.
        """
        return f"{TypeNom(self.type_nom).article}{self.nom}"

    @property
    def nom_avec_charniere(self):
        """Retourne le nom de l'entité précédé de la charnière (forme possessive).

        :return: le nom avec la charniere (par exemple "de la Charente")
        """
        return f"{TypeNom(self.type_nom).charniere}{self.nom}"

    class Meta:
        abstract = True


class IdentiteMixin(models.Model):
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

    class Meta:
        abstract = True


class SearchQueryset(models.QuerySet):
    def search(self, termes: str):
        """Réalise une recherche plein texte dans le queryset

        :param termes: Les termes à rechercher
        :return: le queryset filtré et ordonné selon les termes à rechercher
        """
        query = PrefixSearchQuery(termes, config="data_france_search")

        return (
            self.filter(search=query)
            .annotate(rank=SearchRank(models.F("search"), query, normalization=8))
            .order_by("-rank")
        )


class Commune(TypeNomMixin, models.Model):
    class TypeCommune(models.TextChoices):
        """Enum des différents types d'entité référencées comme communes"""

        COMMUNE = "COM", "Commune"
        COMMUNE_DELEGUEE = "COMD", "Commune déléguée"
        COMMUNE_ASSOCIEE = "COMA", "Commune associée"
        ARRONDISSEMENT_PLM = "ARM", "Arrondissement PLM"
        SECTEUR_PLM = "SRM", "Secteur électoral PLM"

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
        "Nom de la commune",
        max_length=200,
        blank=False,
        editable=False,
        null=False,
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
    def code_departement(self) -> str:
        """Renvoie le code du département contenant cette commune

        :return: le code à 2 ou 3 caractères du département
        """
        if self.commune_parent_id:
            return self.commune_parent.departement.code
        return self.departement.code

    @property
    def nom_departement(self) -> str:
        """Renvoie le nom du département contenant cette commune."""
        if self.commune_parent_id:
            return self.commune_parent.departement.nom
        return self.departement.nom

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

    geometry = MultiPolygonField(
        "Géométrie", geography=True, srid=4326, null=True, spatial_index=True
    )

    ACCESSIBILITE_CHOICES = (
        ("ACC", "Accessible"),
        ("DEM", "Sur demande préalable"),
        ("NAC", "Non accessible"),
    )

    mairie_adresse = models.TextField("Adresse de la mairie", blank=True)
    mairie_accessibilite = models.CharField(
        "Accessibilité de la mairie",
        choices=ACCESSIBILITE_CHOICES,
        max_length=3,
        default="NAC",
    )
    mairie_accessibilite_details = models.TextField(
        "Accessibilité de la mairie (détails)", blank=True
    )
    mairie_localisation = PointField(
        "Localisation de la mairie", null=True, geography=True, srid=4326
    )
    mairie_horaires = models.JSONField("Horaires d'ouverture", default=list)
    mairie_email = models.EmailField("Email de la mairie", blank=True)
    mairie_telephone = models.CharField(
        "Numéro de téléphone de la mairie", blank=True, max_length=20
    )
    mairie_site = models.URLField("Site web de la mairie", blank=True)

    search = SearchVectorField("Champ de recherche", null=True, editable=False)

    @property
    def avec_conseil(self) -> bool:
        """Indique si cette entité de type commune a un conseil municipal.

        Cela concerne les communes à proprement parler, ainsi que les secteurs
        électoraux de Paris, Lyon et Marseille

        :return: Si cette entité a un conseil municipal
        """
        return self.type in (self.TypeCommune.COMMUNE, self.TypeCommune.SECTEUR_PLM)

    def __str__(self):
        return f"{self.nom_complet} ({self.code})"

    def __repr__(self):
        return f"<{self.get_type_display()}: {self}>"

    def as_dict(self):
        """Sérialise l'instance (compatible JSON)"""
        return {
            "code": self.code,
            "type": self.type,
            "nom": self.nom_complet,
            "code_departement": self.code_departement,
            "nom_departement": self.nom_departement,
        }

    def mairie_horaires_display(self):
        return format_html_join(
            "",
            "<div style='margin-bottom: 10px;'><strong>{}</strong>{}</div>",
            (
                (
                    j[0] if j[0] == j[1] else f"Du {j[0]} au {j[1]}",
                    format_html_join(
                        "", "<div>{}</div>", ((f"De {h[0]} à {h[1]}",) for h in j[2])
                    ),
                )
                for j in sorted(self.mairie_horaires, key=_horaires_sort_key)
            ),
        )

    mairie_horaires_display.short_description = "Horaires d'ouverture"

    class Meta:
        verbose_name = "Commune"
        verbose_name_plural = "Communes"

        ordering = ("code", "nom", "type")

        indexes = (GinIndex(fields=["search"]),)
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
                fields=["type", "code"],
                name="commune_unique_code",
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

    code = models.CharField("Code SIREN", max_length=10, editable=False, unique=True)

    type = models.CharField("Type d'EPCI", max_length=2, choices=TypeEPCI.choices)

    nom = models.CharField("Nom de l'EPCI", max_length=300)

    population = models.PositiveIntegerField("Population", null=True)

    geometry = MultiPolygonField(
        "Géométrie", geography=True, srid=4326, null=True, spatial_index=True
    )

    def __str__(self):
        return f"{self.nom} ({self.code})"

    def as_dict(self):
        """Sérialise l'instance (compatible JSON)"""
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

    geometry = MultiPolygonField(
        "Géométrie", geography=True, srid=4326, null=True, spatial_index=True
    )

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


class CollectiviteDepartementale(TypeNomMixin, models.Model):
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

    geometry = MultiPolygonField(
        "Géométrie", geography=True, srid=4326, null=True, spatial_index=True
    )

    class Meta:
        verbose_name = "Collectivité à compétences départementales"
        verbose_name_plural = "Collectivités à compétences départementales"
        ordering = ("code",)

    def __str__(self):
        return f"{self.nom} ({self.code})"

    def as_dict(self):
        return {
            "code": self.code,
            "nom": self.nom_complet,
            "population": self.population,
            "type": self.type,
        }


class CollectiviteRegionale(TypeNomMixin, models.Model):
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

    def as_dict(self):
        return {"code": self.code, "nom": self.nom_complet, "type": self.type}


class Canton(TypeNomMixin, models.Model):
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
        "Nom du canton",
        max_length=200,
        blank=False,
        editable=False,
        null=False,
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


class CirconscriptionLegislative(models.Model):
    code = models.CharField(
        verbose_name="Numéro de la circonscription",
        max_length=10,
        blank=False,
        editable=False,
        null=False,
    )

    departement = models.ForeignKey(
        Departement,
        null=True,
        on_delete=models.CASCADE,
    )

    geometry = MultiPolygonField("Géométrie", geography=True, srid=4326, null=True)

    def __str__(self):
        code_dep, num = self.code.split("-")
        num = int(num)
        if num == 1:
            ordinal = "1ère"
        else:
            ordinal = f"{num}ème"

        if self.departement:
            return f"{ordinal} {self.departement.nom_avec_charniere}"
        elif code_dep == "99":
            return f"{ordinal} des Français de l'Étranger"
        elif code_dep == "977":
            return f"{ordinal} de Saint-Barthélémy et Saint-Martin"
        else:
            return f"{ordinal} {NOMS_COM[code_dep].nom_avec_charniere}"

    class Meta:
        verbose_name = "Circonscription législative"
        verbose_name_plural = "Circonscriptions législatives"
        ordering = ("code",)


class CirconscriptionConsulaire(models.Model):
    objects = SearchQueryset.as_manager()

    nom = models.CharField(
        verbose_name="Nom de la circonscription",
        max_length=300,
        blank=False,
        editable=False,
        null=False,
    )

    consulats = ArrayField(
        verbose_name="Consulats inclus",
        base_field=models.CharField(max_length=200),
        blank=False,
        null=False,
        editable=False,
    )

    nombre_conseillers = models.PositiveSmallIntegerField(
        verbose_name="Nombre de conseillers", blank=False, null=False, editable=False
    )

    search = SearchVectorField(verbose_name="Champ de recherche", null=True)

    def as_dict(self):
        return {
            "nom": self.nom,
            "consulats": self.consulats,
            "nombre_conseillers": self.nombre_conseillers,
        }

    def __str__(self):
        return f"Circonscription «\u00A0{self.nom}\u00A0»"

    class Meta:
        verbose_name_plural = "circonscriptions consulaires"
        indexes = (GinIndex(fields=["search"]),)


class Depute(IdentiteMixin, models.Model):
    objects = SearchQueryset.as_manager()

    code = models.CharField(
        verbose_name="Identifiant AN",
        max_length=50,
        editable=False,
    )

    circonscription = models.ForeignKey(
        CirconscriptionLegislative,
        on_delete=models.CASCADE,
        related_name="deputes",
        related_query_name="depute",
        editable=False,
    )

    groupe = models.CharField(
        verbose_name="Groupe parlementaire",
        max_length=200,
        blank=True,
        editable=False,
    )

    relation = models.CharField(
        verbose_name="Type de relation au groupe",
        max_length=1,
        choices=RelationGroupe.choices,
        blank=True,
        editable=False,
    )

    parti = models.CharField(
        verbose_name="Parti d'affiliation", max_length=200, blank=True, editable=False
    )

    legislature = models.PositiveSmallIntegerField(
        verbose_name="Législature", null=False, editable=False
    )

    date_debut_mandat = models.DateField(
        verbose_name="Date de début du mandat", editable=False
    )

    date_fin_mandat = models.DateField(
        verbose_name="Date de fin du mandat", null=True, editable=False
    )

    def __str__(self):
        return f"{self.nom}, {self.prenom} ({self.circonscription})"

    class Meta:
        verbose_name = "Député⋅e"
        ordering = ("nom", "prenom")


class EluMunicipal(IdentiteMixin, models.Model):
    objects = SearchQueryset.as_manager()

    commune = models.ForeignKey(
        to="Commune",
        on_delete=models.PROTECT,
        related_name="elus",
        related_query_name="elu",
        editable=False,
    )

    date_debut_mandat = models.DateField(
        verbose_name="Date de début du mandat", editable=False
    )

    fonction = models.CharField(
        verbose_name="Fonction",
        editable=False,
        blank=True,
        max_length=50,
        choices=Fonction.choices,
    )
    ordre_fonction = models.PositiveSmallIntegerField(
        verbose_name="Ordre de la fonction", editable=False, null=True
    )

    date_debut_fonction = models.DateField(
        verbose_name="Date de début de la fonction", editable=True, null=True
    )

    date_debut_mandat_epci = models.DateField(
        verbose_name="Date de début du mandat auprès de l'EPCI",
        editable=False,
        null=True,
    )

    fonction_epci = models.CharField(
        verbose_name="Fonction auprès de l'EPCI",
        editable=False,
        blank=True,
        max_length=60,
    )

    date_debut_fonction_epci = models.DateField(
        verbose_name="Date de début de la fonction auprès de l'EPCI",
        editable=False,
        null=True,
    )

    nationalite = models.CharField(
        verbose_name="Nationalité", editable=False, max_length=30
    )

    parrainage2017 = models.CharField(
        verbose_name="Personne parrainée aux Présidentielles de 2017",
        max_length=80,
        editable=False,
        blank=True,
    )

    search = SearchVectorField(verbose_name="Champ de recherche", null=True)

    @property
    def libelle_fonction(self):
        display = self.get_fonction_display()
        if self.ordre_fonction:
            return f"{ORDINAUX_LETTRES[self.ordre_fonction-1]} {display.lower()}"
        return display

    @property
    def elu_epci(self):
        return self.date_debut_mandat_epci is not None

    def __str__(self):
        return f"{self.nom}, {self.prenom} ({self.commune.nom_complet})"

    class Meta:
        verbose_name = "Élu⋅e municipal⋅e"
        verbose_name_plural = "Élu⋅es municipaux⋅les"
        ordering = ("commune", "nom", "prenom", "date_naissance")
        indexes = (GinIndex(fields=["search"]),)
