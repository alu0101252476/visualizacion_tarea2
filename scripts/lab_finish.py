import os
import subprocess
from dagster import asset
from .lab_graph1 import (
    imagen_actividad_mun_dom,
    imagen_actividad_mun_sexo,
    imagen_actividad_sec_dom,
    imagen_actividad_sec_sexo,
    imagen_renta_mun_canaria,
    imagen_renta_mun_nacional,
    imagen_renta_sec_canaria,
    imagen_renta_sec_nacional,
)
from .lab_graph2 import (
    imagen_evolucion_municipios_media,
    imagen_evolucion_secciones_media,
    imagen_ocupacion_mapa_calor,
    imagen_ocupacion_provincia_sexo,
    imagen_ocupacion_isla_sexo,
)
from .lab_graph3 import (
    imagen_distribucion_renta_top_bottom,
    imagen_distribucion_renta_isla,
    imagen_ocupacion_top_bottom,
    imagen_ocupacion_isla,
    imagen_actividad_isla_ano,
)


BRANCH = "practica-final"
REPO_PATH = "."

# Push del repositorio.
@asset(
    deps=[
        imagen_actividad_mun_dom,
        imagen_actividad_mun_sexo,
        imagen_actividad_sec_dom,
        imagen_actividad_sec_sexo,
        imagen_renta_mun_canaria,
        imagen_renta_mun_nacional,
        imagen_renta_sec_canaria,
        imagen_renta_sec_nacional,
        imagen_evolucion_municipios_media,
        imagen_evolucion_secciones_media,
        imagen_ocupacion_mapa_calor,
        imagen_ocupacion_provincia_sexo,
        imagen_ocupacion_isla_sexo,
        imagen_distribucion_renta_top_bottom,
        imagen_distribucion_renta_isla,
        imagen_ocupacion_top_bottom,
        imagen_ocupacion_isla,
        imagen_actividad_isla_ano,
    ]
)
def git_push():
  commit_msg = "Actualización desde Dagster"
  if not os.path.exists(REPO_PATH):
    raise FileNotFoundError(f"El repositorio no existe: {REPO_PATH}")
  try:
    subprocess.run(["git", "-C", REPO_PATH, "checkout", BRANCH], check=True)
    # Agregamos todos los cambios
    subprocess.run(["git", "-C", REPO_PATH, "add", "."], check=True)
    # Hacemos un commit
    subprocess.run(["git", "-C", REPO_PATH, "commit", "-m", commit_msg], check=True)
    # Hacemos un push
    result = subprocess.run(["git", "-C", REPO_PATH, "push", "origin", BRANCH], capture_output=True, text=True, check=True)
    print(f"Git push realizado correctamente: {result.stdout}")
  except subprocess.CalledProcessError as e:
    print(f"Error haciendo git push: {e.stderr}")
    raise
