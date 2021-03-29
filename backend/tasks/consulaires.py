import pandas as pd
import requests
from bs4 import BeautifulSoup
from doit.tools import create_folder, run_once

from sources import PREPARE_DIR


__all__ = ["task_recuperer_conseillers_afe"]


AFE_DOMAIN = "https://www.assemblee-afe.fr/"
AFE_LISTE = f"{AFE_DOMAIN}-annuaire-des-conseillers-.html"

correspondance = {
    "Profession ou qualité": "profession",
    "Élu(e) en": "dates_election",
    "Chef-lieu de circonscription": "circonscription_chef_lieu",
    "Circonscription": "circonscription",
    "Commission": "commission",
    "Groupe à l'assemblée": "groupe",
    "Email AFE": "email_principal",
    "Autre adresse email": "email_autre",
    "Courrier valise diplomatique": "adresse_diplomatique",
    "Adresse à l'étranger": "adresse_etranger",
    "Adresse en France": "adresse_france",
    "Courrier local": "adresse_locale",
    "Téléphone cellulaire à l'étranger": "telephone_mobile_etranger",
    "Téléphone cellulaire en France": "telephone_mobile_france",
    "Téléphone fixe à l'étranger": "telephone_fixe_etranger",
    "Téléphone fixe en France": "telephone_fixe_france",
    "Page web personnelle": "site_personnel",
    "Mandat particulier": "mandat_particulier",
}


def task_recuperer_conseillers_afe():
    target = PREPARE_DIR / "consulaires" / "afe.csv"
    return {
        "uptodate": [run_once],
        "targets": [target],
        "actions": [
            (create_folder, (target.parent,)),
            (recuperer_conseillers_afe, (target,)),
        ],
    }


def recuperer_conseillers_afe(dest):
    pages = recuperer_listes_pages()

    res = pd.DataFrame([recuperer_infos_conseiller(l) for l in pages])
    res.to_csv(dest, index=False)


def recuperer_listes_pages():
    res = requests.get(AFE_LISTE)
    res.raise_for_status()

    soup = BeautifulSoup(res.content, "lxml")
    return [a.attrs["href"] for a in soup("a", "tt-upp")]


def recuperer_infos_conseiller(url):
    res = requests.get(f"{AFE_DOMAIN}{url}")
    res.raise_for_status()

    soup = BeautifulSoup(res.content, "lxml")
    header = soup.article.header

    titre, prenom = header.h1.contents[0].strip().split(" ", 1)
    nom = header.h1.contents[2]

    # on écarte le premier qui est traité à part (date et lieu de naissance)
    fields = [li for li in soup.article("li") if len(li.contents) == 2 and li.strong][
        1:
    ]

    infos = {
        correspondance[li.contents[0].text.strip(": ")]: li.contents[1] for li in fields
    }

    bloc_naissance = header.ul("li")[0]
    if len(bloc_naissance.contents) > 1:
        infos["date_naissance"], infos["lieu_naissance"] = bloc_naissance.contents[
            1
        ].split(" à ")

    infos["email_principal"] = infos["email_principal"].text.replace(" chez ", "@")
    if "email_autre" in infos:
        infos["email_autre"] = infos["email_autre"].text.replace(" chez ", "@")

    return {
        "titre": titre,
        "prenom": prenom,
        "nom": nom,
        **{
            k: v.strip() if isinstance(v, str) else v.text.strip()
            for k, v in infos.items()
        },
    }
