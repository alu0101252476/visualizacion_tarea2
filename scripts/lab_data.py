import geopandas as gpd
import os
import pandas as pd
import subprocess
from dagster import asset


BRANCH = "practica-final"
REPO_PATH = "."


# Pull del repositorio.
@asset
def git_pull():
    if not os.path.exists(REPO_PATH):
        raise FileNotFoundError(f"El repositorio no existe: {REPO_PATH}")
    try:
        subprocess.run(["git", "-C", REPO_PATH, "checkout", BRANCH], check=True)
        result = subprocess.run(
            ["git", "-C", REPO_PATH, "pull", "origin", BRANCH],
            capture_output=True, text=True, check=True,
        )
        print(f"Git pull realizado correctamente: {result.stdout}")
    except subprocess.CalledProcessError as e:
        print(f"Error haciendo git pull: {e.stderr}")
        raise


# Dataset de distribución de renta por fuente de ingresos y sección.
@asset(deps=[git_pull])
def distribucion_renta_ingresos_csv():
    file_path = "./data/distribucion-renta-ingresos.csv"
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"No se encontró el archivo: {file_path}")
    df = pd.read_csv(file_path)
    df["OBS_VALUE"] = (
        df["OBS_VALUE"].astype(str).str.replace(",", ".", regex=False)
    )
    df["OBS_VALUE"] = pd.to_numeric(df["OBS_VALUE"], errors="coerce")
    return df


# Dataset de ocupación por sección censal, año y sexo.
@asset(deps=[git_pull])
def ocupacion_csv():
    file_path = "./data/ocupacion-sc-3.csv"
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"No se encontró el archivo: {file_path}")
    df = pd.read_csv(file_path)
    df["ocupacion"] = df["ocupacion"].replace({
        "Directores/gerentes y profesionales/técnicos de nivel medio o alto": "Directores y técnicos de nivel alto",
        "Trabajadores cualificados y oficiales/operarios de nivel bajo": "Técnicos de nivel bajo"
    })
    return df


# Dataset de renta bruta media por sección censal.
@asset(deps=[git_pull])
def rentamedia_csv():
    file_path = "./data/rentamedia-sc-3.csv"
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"No se encontró el archivo: {file_path}")
    df = pd.read_csv(file_path)
    return df


# Dataset de actividad económica por sección censal, año y sexo.
@asset(deps=[git_pull])
def actividad_csv():
    file_path = "./data/actividad-sc-3.csv"
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"No se encontró el archivo: {file_path}")
    df = pd.read_csv(file_path)
    return df


# Dataset de mapa por sección censal y año.
@asset(deps=[git_pull])
def secciones_geojson():
    años = [2021, 2022, 2023, 2024]
    gdfs = []
    for año in años:
        path = f"./data/secciones_{año}0101_tenerife.json"
        if not os.path.exists(path):
            raise FileNotFoundError(f"No se encontró el archivo: {path}")
        gdf = gpd.read_file(path)
        gdf["año"] = año
        gdfs.append(gdf)
    combined = gpd.GeoDataFrame(
        pd.concat(gdfs, ignore_index=True),
        crs=gdfs[0].crs,
    )
    return combined
