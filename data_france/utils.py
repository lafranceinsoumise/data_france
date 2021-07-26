from dataclasses import dataclass

from django.db.models import IntegerChoices

from data_france.typologies import CodeSexe

JOURS_SEMAINE = [
    "lundi",
    "mardi",
    "mercredi",
    "jeudi",
    "vendredi",
    "samedi",
    "dimanche",
]

POINTS_MEDIAN = [
    "‧",  # point d'hyphénation
    "⋅",  # opérateur 'dot'
    "·",  # point du milieu
]

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


@dataclass
class NomType:
    nom: str
    type_nom: TypeNom

    @property
    def nom_complet(self):
        return f"{self.type_nom.article}{self.nom}"

    @property
    def nom_avec_charniere(self):
        return f"{self.type_nom.charniere}{self.nom}"


def genrer_mot_inclusif(mot, genre):
    if genre not in CodeSexe.values:
        return mot

    for point in POINTS_MEDIAN:
        if point in mot:
            break
    else:
        return mot

    # on sépare la racine de la partie féminine
    racine, ext = mot.split(point, 1)

    # seules peuvent être déclinées automatiquement les mots dont la forme féminine finit par "e", éventuellement
    # au pluriel.
    if ext[-1] != "e" and ext[-2:] != "es":
        raise ValueError(
            "Seules les terminaisons en e ou es peuvent utiliser la forme à 2 arguments."
        )

    pluriel_commun = "s" if ext[-1:] == "s" and racine[-1] not in "sx" else ""
    if pluriel_commun:
        ext = ext[:-1]

    # l'heuristique utilisée part du principe que la terminaison féminine a
    # généralement un caractère de plus que la terminaison masculine.
    # Surprenamment, ça marche dans un très grand nombre de cas. Ça ne marche
    # pas dans les cas où il y a doublement de la consonne finale de la forme
    # masculine, quand elle se termine par un n par exemple
    tronque = len(racine) - len(ext) + 1

    if racine[-1] in "n":
        tronque += 1

    if genre == CodeSexe.MASCULIN:
        return f"{racine}{pluriel_commun}"
    return f"{racine[:tronque]}{ext}{pluriel_commun}"


def genrer(genre, *args):
    """Genrer correctement une expression

    Il y a deux façons d'appeler cette fonction : avec 2 arguments, et avec 4.

    Le premier argument est toujours le genre de destination.

    Dans la version à deux arguments, le deuxième argument doit être un mot sous forme inclusive, écrit avec un
    point médian (par exemple "insoumis⋅e"), et il doit s'agir d'un mot dont la version féminine est en "e".
    Dans ces cas, la fonction est généralement capable de décliner le mot correspondant, mais peut échouer avec certains
    mots.

    Dans la version à 4 arguments, les 3 derniers arguments doivent être la forme masculine, féminine, et épicène, dans
    cet ordre.

    :param genre: le genre dans lequel il faut décliner le mot
    :param args: soit un unique argument sous forme inclusive (avec point médian), soit les formes masculine, féminine,
    et épicène, dans cet ordre
    :return: le mot décliné selon le bon genre
    """
    if len(args) not in (1, 3):
        raise TypeError

    if len(args) == 1:
        return " ".join(genrer_mot_inclusif(mot, genre) for mot in args[0].split())

    return args[
        0 if genre == CodeSexe.MASCULIN else 1 if genre == CodeSexe.FEMININ else 2
    ]
