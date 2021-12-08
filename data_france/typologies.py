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


class CSP(IntegerChoices):
    AGE = 1, "Agriculteurs exploitants"
    AGE_E = 10, "Agriculteurs exploitants"
    AGE_PE = 11, "Agriculteurs sur petite exploitation"
    AGE_ME = 12, "Agriculteurs sur moyenne exploitation"
    AGE_GE = 13, "Agriculteurs sur grande exploitation"

    ACC = 2, "Artisans, commerçants et chefs d'entreprise"
    ACC_ART = 21, "Artisans"
    ACC_COM = 22, "Commerçants et assimilés"
    ACC_CHE = 23, "Chefs d'entreprise de 10 salariés ou plus"

    CIS = 3, "Cadres et professions intellectuelles supérieures"
    CIS_LIB = 31, "Professions libérales et assimilés"
    CIS_CFPPIA = (
        32,
        "Cadres de la fonction publique, professions intellectuelles et  artistiques",
    )
    CIS_CFP = 33, "Cadres de la fonction publique"
    CIS_PPS = 34, "Professeurs, professions scientifiques"
    CIS_PIAS = 35, "Professions de l'information, des arts et des spectacles"
    CIS_CE = 36, "Cadres d'entreprise"
    CIS_CACE = 37, "Cadres administratifs et commerciaux d'entreprise"
    CIS_ICT = 38, "Ingénieurs et cadres techniques d'entreprise"

    PI = 4, "Professions Intermédiaires"
    PI_ESFP = (
        41,
        "Professions intermédiaires de l'enseignement, de la santé, de la fonction publique et assimilés",
    )
    PI_PEI = 42, "Professeurs des écoles, instituteurs et assimilés"
    PI_STS = 43, "Professions intermédiaires de la santé et  du travail social"
    PI_CR = 44, "Clergé, religieux"
    PI_AFP = 45, "Professions intermédiaires administratives de la fonction publique"
    PI_ACE = (
        46,
        "Professions intermédiaires administratives et commerciales des entreprises",
    )
    PI_TECH = 47, "Techniciens"
    PI_CAM = 48, "Contremaîtres, agents de maîtrise"

    EMP = 5, "Employés"
    EMP_FP = 51, "Employés de la fonction publique"
    EMP_CASFP = 52, "Employés civils et agents de service de la fonction publique"
    EMP_PM = 53, "Policiers et militaires"
    EMP_AE = 54, "Employés administratifs d'entreprise"
    EMP_COM = 55, "Employés de commerce"
    EMP_PSDP = 56, "Personnels des services directs aux particuliers"

    OUV = 6, "Ouvriers"
    OUV_Q = 61, "Ouvriers qualifiés"
    OUV_QI = 62, "Ouvriers qualifiés de type industriel"
    OUV_QA = 63, "Ouvriers qualifiés de type artisanal"
    OUV_CHA = 64, "Chauffeurs"
    OUV_QMMT = 65, "Ouvriers qualifiés de la manutention, du magasinage et du transport"
    OUV_NQ = 66, "Ouvriers non qualifiés"
    OUV_NQI = 67, "Ouvriers non qualifiés de type industriel"
    OUV_NQIA = 68, "Ouvriers non qualifiés de type artisanal"
    OUV_A = 69, "Ouvriers agricoles"

    RET = 7, "Retraités"
    RET_AGE = 71, "Anciens agriculteurs exploitants"
    RET_ACC = 72, "Anciens artisans, commerçants, chefs d'entreprise"
    RET_CPI = 73, "Anciens cadres et professions intermédiaires"
    RET_CAD = 74, "Anciens cadres"
    RET_PI = 75, "Anciennes professions intermédiaires"
    RET_EO = 76, "Anciens employés et ouvriers"
    RET_EMP = 77, "Anciens employés"
    RET_OUV = 78, "Anciens ouvriers"

    SAP = 8, "Autres personnes sans activité professionnelle"
    SAP_CJT = 81, "Chômeurs n'ayant jamais travaillé"
    SAP_ID = 82, "Inactifs divers (autres que retraités)"
    SAP_MC = 83, "Militaires du contingent"
    SAP_EE = 84, "Elèves, étudiants"
    SAP_DJ = (
        85,
        "Personnes diverses sans activité  professionnelle de moins de 60 ans (sauf retraités)",
    )
    SAP_DV = (
        86,
        "Personnes diverses sans activité professionnelle de 60 ans et plus (sauf retraités)",
    )
