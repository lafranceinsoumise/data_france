from django.db.models import TextChoices, IntegerChoices


class TypeNom(IntegerChoices):
    """Nomenclature des noms propres utilisés par l'INSEE pour les noms de lieu

    Cette nomenclature permet de savoir quels sont les articles et les charnières applicables
    aux noms propres de communes, départements et régions.

    Référence :
    https://www.insee.fr/fr/information/2560684#tncc
    """

    def __new__(cls, value, article, charniere):
        obj = int.__new__(cls, value)
        obj._value_ = value
        obj.article = article
        obj.charniere = charniere
        return obj

    CONSONNE = 0, "", "de ", "Pas d'article, commence par une consonne (sauf H muet)"
    VOYELLE = 1, "", "d'", "Pas d'article, commence par une voyelle (ou H muet)"
    ARTICLE_LE = 2, "le ", "du ", "Article = LE"
    ARTICLE_LA = 3, "la ", "de la ", "Article = LA"
    ARTICLE_LES = 4, "les ", "des ", "Article = LES"
    ARTICLE_L = 5, "l'", "de l'", "Article = L'"
    ARTICLE_AUX = 6, "aux", "des ", "Article = AUX"
    ARTICLE_LAS = 7, "las ", "de las ", "Article = LAS"
    ARTICLE_LOS = 8, "los ", "de los ", "Article = LOS"


class CodeSexe(TextChoices):
    MASCULIN = "M"
    FEMININ = "F"


class Fonction(TextChoices):
    MAIRE = "MAI", "Maire"
    MAIRE_DELEGUE = "MDA", "Maire délégué⋅e"
    VICE_PRESIDENT = "VPE", "Vice-Président⋅e"
    PRESIDENT = "PRE", "Président⋅e"
    MAIRE_ADJOINT = "ADJ", "Adjoint⋅e au maire"
    AUTRE_MEMBRE_COM = "AMC", "Autre membre de la commission permanente"


ORDINAUX_LETTRES = [
    "premier",
    "second",
    "troisième",
    "quatrième",
    "cinquième",
    "sixième",
    "septième",
    "huitième",
    "neuvième",
    "dixième",
    "onzième",
    "douzième",
    "treizième",
    "quatorzième",
    "quinzième",
    "seizième",
    "dix-septième",
    "dix-huitième",
    "dix-neuvième",
    "vingtième",
    "vingt-et-unième",
    "vingt-deuxième",
    "vingt-troisième",
    "vingt-quatrième",
    "vingt-cinquième",
    "vingt-sixième",
    "vingt-septième",
    "vingt-huitième",
    "vingt-neuvième",
    "trentième",
    "trente-et-unième",
    "trente-deuxième",
    "trente-troisième",
    "trente-quatrième",
    "trente-cinquième",
    "trente-sixième",
    "trente-septième",
    "trente-huitième",
    "trente-neuvième",
]
