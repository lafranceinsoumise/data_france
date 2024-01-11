import subprocess

import geopandas as gpd


def generer_metropole(shp_departements, geojson):
    df = gpd.read_file(shp_departements)

    df = df.rename(columns={"NOM": "nom", "INSEE_DEP": "code"})
    df = df.loc[df.code.str.len() == 2, ["code", "geometry"]].to_crs(2154)  # LAMBERT93

    df.to_file(geojson, driver="GeoJSON")


def generer_miniature(topology, departement):
    return f"""
    jq '.features[] | select( .properties.code == "{departement}" )' < {topology} \
    | geoproject 'd3.geoIdentity().reflectY(true).fitSize([500,500], d)' \
    | geo2topo data=- \
    | toposimplify -p 64 \
    | topo2geo data=- \
    | geo2svg -w 500 -h 500
    """
