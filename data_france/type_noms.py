TYPE_NOM_CONSONNE = 0
TYPE_NOM_VOYELLE = 1
TYPE_NOM_ARTICLE_LE = 2
TYPE_NOM_ARTICLE_LA = 3
TYPE_NOM_ARTICLE_LES = 4
TYPE_NOM_ARTICLE_L = 5
TYPE_NOM_ARTICLE_AUX = 6
TYPE_NOM_ARTICLE_LAS = 7
TYPE_NOM_ARTICLE_LOS = 8
TYPE_NOM_CHOICES = (
    (TYPE_NOM_CONSONNE, "Pas d'article, commence par une consonne (sauf H muet)"),
    (TYPE_NOM_VOYELLE, "Pas d'article, commence par une voyelle (ou H muet)"),
    (TYPE_NOM_ARTICLE_LE, "Article = LE"),
    (TYPE_NOM_ARTICLE_LA, "Article = LA"),
    (TYPE_NOM_ARTICLE_LES, "Article = LES"),
    (TYPE_NOM_ARTICLE_L, "Article = L'"),
    (TYPE_NOM_ARTICLE_AUX, "Article = AUX"),
    (TYPE_NOM_ARTICLE_LAS, "Article = LAS"),
    (TYPE_NOM_ARTICLE_LOS, "Article = LOS"),
)
TYPE_NOM_ARTICLE = {
    TYPE_NOM_CONSONNE: "",
    TYPE_NOM_VOYELLE: "",
    TYPE_NOM_ARTICLE_LE: "le ",
    TYPE_NOM_ARTICLE_LA: "la ",
    TYPE_NOM_ARTICLE_LES: "les ",
    TYPE_NOM_ARTICLE_L: "l'",
    TYPE_NOM_ARTICLE_AUX: "aux ",
    TYPE_NOM_ARTICLE_LAS: "las ",
    TYPE_NOM_ARTICLE_LOS: "los ",
}
TYPE_NOM_CHARNIERE = {
    TYPE_NOM_CONSONNE: "de ",
    TYPE_NOM_VOYELLE: "d'",
    TYPE_NOM_ARTICLE_LE: "du ",
    TYPE_NOM_ARTICLE_LA: "de la ",
    TYPE_NOM_ARTICLE_LES: "des ",
    TYPE_NOM_ARTICLE_L: "de l'",
    TYPE_NOM_ARTICLE_AUX: "des ",
    TYPE_NOM_ARTICLE_LAS: "de las ",
    TYPE_NOM_ARTICLE_LOS: "de los ",
}


class TypeNomMixin:
    @property
    def nom_complet(self):
        return f"{TYPE_NOM_ARTICLE[self.type_nom].title()}{self.nom}"

    @property
    def nom_avec_charniere(self):
        return f"{TYPE_NOM_CHARNIERE[self.type_nom]}{self.nom}"
