# Conversion des identifiants utilisés par le ministère de l'intérieur pour
# l'outremer
MINISTERE_VERS_INSEE = {
    "ZA": "971",
    "ZB": "972",
    "ZC": "973",
    "ZD": "974",
    "ZM": "976",
    "ZN": "988",
    "ZP": "987",
    "ZS": "975",
    "ZW": "986",
    "ZX": "97",
}


def convertir_code_bureau(s):
    outremer = s.str.slice(0, 2).isin(MINISTERE_VERS_INSEE)
    prefixe = s[outremer].str.slice(0, 2).map(MINISTERE_VERS_INSEE)
    suffixe = (
        s[outremer].str.slice(3).where(prefixe.str.len() == 3, s[outremer].str.slice(2))
    )
    return s.where(~outremer, prefixe + suffixe)
