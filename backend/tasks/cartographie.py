import subprocess

import geopandas as gpd


def generer_metropole(shp_departements, topology):
    df = gpd.read_file(shp_departements)

    df = df.rename(columns={"NOM": "nom", "INSEE_DEP": "code"})
    df = df.loc[df.code.str.len() == 2, ["code", "nom", "geometry"]].to_crs(
        2154
    )  # LAMBERT93

    with open(topology, "wb") as fd:
        proc = subprocess.Popen(
            ["geo2topo", "data=-"],
            stdin=subprocess.PIPE,
            stdout=fd,
        )

        proc.communicate(df.to_json().encode())


def generer_miniature(topology, departement):
    return f"""
    topomerge data=data -f 'd.properties.code == "{departement}"' < {topology}
    | toposimplify -P .1
    | topo2geo data=-
    | geoproject 'd3.geoIdentity().reflectY(true).fitSize([500,500], d)'
    | geo2svg -w 500 -h 500
    """
