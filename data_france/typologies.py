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


class Fonction(TextChoices):
    MAIRE = "MAI", "Maire"
    MAIRE_DELEGUE = "MDA", "Maire délégué⋅e"
    VICE_PRESIDENT = "VPE", "Vice-Président⋅e"
    PRESIDENT = "PRE", "Président⋅e"
    ADJOINT_01 = (
        "A01",
        "Premier adjoint⋅e",
    )
    ADJOINT_02 = (
        "A02",
        "Second adjoint⋅e",
    )
    ADJOINT_03 = (
        "A03",
        "Troisième adjoint⋅e",
    )
    ADJOINT_04 = (
        "A04",
        "Quatrième adjoint⋅e",
    )
    ADJOINT_05 = (
        "A05",
        "Cinquième adjoint⋅e",
    )
    ADJOINT_06 = (
        "A06",
        "Sixième adjoint⋅e",
    )
    ADJOINT_07 = (
        "A07",
        "Septième adjoint⋅e",
    )
    ADJOINT_08 = (
        "A08",
        "Huitième adjoint⋅e",
    )
    ADJOINT_09 = (
        "A09",
        "Neuvième adjoint⋅e",
    )
    ADJOINT_10 = (
        "A10",
        "Dixième adjoint⋅e",
    )
    ADJOINT_11 = (
        "A11",
        "Onzième adjoint⋅e",
    )
    ADJOINT_12 = (
        "A12",
        "Douzième adjoint⋅e",
    )
    ADJOINT_13 = (
        "A13",
        "Treizième adjoint⋅e",
    )
    ADJOINT_14 = (
        "A14",
        "Quatorzième adjoint⋅e",
    )
    ADJOINT_15 = (
        "A15",
        "Quinzième adjoint⋅e",
    )
    ADJOINT_16 = (
        "A16",
        "Seizième adjoint⋅e",
    )
    ADJOINT_17 = (
        "A17",
        "Dix-septième adjoint⋅e",
    )
    ADJOINT_18 = (
        "A18",
        "Dix-huitième adjoint⋅e",
    )
    ADJOINT_19 = (
        "A19",
        "Dix-neuvième adjoint⋅e",
    )
    ADJOINT_20 = (
        "A20",
        "Vingtième adjoint⋅e",
    )
    ADJOINT_21 = (
        "A21",
        "Vingt-et-unième adjoint⋅e",
    )
    ADJOINT_22 = (
        "A22",
        "Vingt-deuxième adjoint⋅e",
    )
    ADJOINT_23 = (
        "A23",
        "Vingt-troisième adjoint⋅e",
    )
    ADJOINT_24 = (
        "A24",
        "Vingt-quatrième adjoint⋅e",
    )
    ADJOINT_25 = (
        "A25",
        "Vingt-cinquième adjoint⋅e",
    )
    ADJOINT_26 = (
        "A26",
        "Vingt-sixième adjoint⋅e",
    )
    ADJOINT_27 = (
        "A27",
        "Vingt-septième adjoint⋅e",
    )
    ADJOINT_28 = (
        "A28",
        "Vingt-huitième adjoint⋅e",
    )
    ADJOINT_29 = (
        "A29",
        "Vingt-neuvième adjoint⋅e",
    )
    ADJOINT_30 = (
        "A30",
        "Trentième adjoint⋅e",
    )
    ADJOINT_31 = (
        "A31",
        "Trente-et-unième adjoint⋅e",
    )
    ADJOINT_32 = (
        "A32",
        "Trente-deuxième adjoint⋅e",
    )
    ADJOINT_33 = (
        "A33",
        "Trente-troisième adjoint⋅e",
    )
    ADJOINT_34 = (
        "A34",
        "Trente-quatrième adjoint⋅e",
    )
    ADJOINT_35 = (
        "A35",
        "Trente-cinquième adjoint⋅e",
    )
    ADJOINT_36 = (
        "A36",
        "Trente-sixième adjoint⋅e",
    )
    ADJOINT_37 = (
        "A37",
        "Trente-septième adjoint⋅e",
    )
    ADJOINT_38 = (
        "A38",
        "Trente-huitième adjoint⋅e",
    )
    ADJOINT_39 = (
        "A39",
        "Trente-neuvième adjoint⋅e",
    )


Fonction.ADJOINTS = [
    value
    for name, value in zip(Fonction.names, Fonction.values)
    if name.startswith("ADJOINT_")
]
