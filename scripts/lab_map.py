import os
import pandas as pd
import geopandas as gpd
from plotnine import *
from dagster import asset
from .lab_renta import git_pull

##############################
# PASO 1: CARGAMOS LOS DATOS #
##############################

# Cargamos los datos desde el repositorio
@asset(deps=[git_pull])
def municipios_json():
  file_path = "./data/municipios-2024.json"
  if not os.path.exists(file_path):
    raise FileNotFoundError(f"No se encontró el archivo: {file_path}")
  gdf = gpd.read_file(file_path)
  return gdf

################################
# PASO 1: GENERAMOS EL GRÁFICO #
################################

# Generamos el gráfico de tasa de empleo
@asset
def visualizacion_mapa(municipios_json):
    indicador="tsal_t"
    polygons = []
    # Iteramos sobre cada municipio
    for idx, row in municipios_json.iterrows():
        label = row.get('label', f'Municipio {idx}')
        valor = row.get(indicador, 0)
        geom = row.geometry
        if geom.geom_type == 'Polygon':
            polys = [geom]
        elif geom.geom_type == 'MultiPolygon':
            polys = list(geom.geoms)
        else:
            continue
        # Guardamos los datos
        for poly_idx, poly in enumerate(polys):
            coords = list(poly.exterior.coords)
            for x, y in coords:
                polygons.append({
                    'poly_id': f"{idx}_{poly_idx}",
                    'label': label,
                    'x': x,
                    'y': y,
                    'valor': valor
                })
    # Convertimos los datos en un dataframe
    geo_df = pd.DataFrame(polygons)
    # Realizamos el gráfico
    mapa_plotnine = (
        ggplot(geo_df, aes(x='x', y='y', group='poly_id', fill='valor'))
        + geom_polygon(color='black', size=0.5)
        + coord_fixed()
        + scale_fill_distiller(type='div', palette='Spectral')
        + labs(title="Tasa de empleo por municipio en Canarias en 2024",
               fill="Porcentaje")
        + theme_void()
        + theme(plot_title=element_text(weight="bold", size=14))
    )
    # Guardamos el gráfico
    mapa_plotnine.save("./images/mapa_municipios.png", dpi=300, width=10, height=10, units="in")
