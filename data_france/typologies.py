from dataclasses import dataclass

from django.db.models import TextChoices, IntegerChoices


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


class RelationGroupe(TextChoices):
    PRESIDENT = "P", "Président⋅e de groupe"
    MEMBRE = "M", "Membre"
    APPARENTE = "A", "Membre apparenté au groupe"
