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


ORDINAUX = [
    "Premier",
    "Second",
    "Troisième",
    "Quatrième",
    "Cinquième",
    "Sixième",
    "Septième",
    "Huitième",
    "Neuvième",
    "Dixième",
    "Onzième",
    "Douzième",
    "Treizième",
    "Quatorzième",
    "Quinzième",
    "Seizième",
    "Dix-septième",
    "Dix-huitième",
    "Dix-neuvième",
    "Vingtième",
    "Vingt-et-unième",
    "Vingt-deuxième",
    "Vingt-troisième",
    "Vingt-quatrième",
    "Vingt-cinquième",
    "Vingt-sixième",
    "Vingt-septième",
    "Vingt-huitième",
    "Vingt-neuvième",
    "Trentième",
    "Trente-et-unième",
    "Trente-deuxième",
    "Trente-troisième",
    "Trente-quatrième",
    "Trente-cinquième",
    "Trente-sixième",
    "Trente-septième",
    "Trente-huitième",
    "Trente-neuvième",
]
