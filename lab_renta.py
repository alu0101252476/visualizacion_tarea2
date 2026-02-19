import os
import subprocess
import pandas as pd
import geopandas as gpd
from dagster import asset, job
from plotnine import *
from plotnine.themes import theme_minimal, theme_void

@asset
def git_pull():
  repo_path = "."
  if not os.path.exists(repo_path):
    raise FileNotFoundError(f"El repositorio no existe: {repo_path}")
  try:
    # Hacemos un pull
    result = subprocess.run(["git", "-C", repo_path, "pull"], capture_output=True, text=True, check=True)
    print(f"Git pull realizado correctamente: {result.stdout}")
  except subprocess.CalledProcessError as e:
    print(f"Error haciendo git pull: {e.stderr}")
    raise

@asset(deps=[git_pull])
def codislas_csv():
  # Cargamos codislas.csv desde el repositorio
  file_path = "data/codislas.csv"
  if not os.path.exists(file_path):
    raise FileNotFoundError(f"No se encontró el archivo: {file_path}")
  df = pd.read_csv(file_path, sep=";")
  return df

@asset(deps=[git_pull])
def distribucion_renta_canarias_csv():
  # Cargamos distribucion-renta-canarias.csv desde el repositorio
  file_path = "data/distribucion-renta-canarias.csv"
  if not os.path.exists(file_path):
    raise FileNotFoundError(f"No se encontró el archivo: {file_path}")
  df = pd.read_csv(file_path)
  return df

@asset(deps=[git_pull])
def municipios_geojson():
  # Cargamos municipios.geojson desde el repositorio
  file_path = "data/municipios.geojson"
  if not os.path.exists(file_path):
    raise FileNotFoundError(f"No se encontró el archivo: {file_path}")
  gdf = gpd.read_file(file_path)
  return gdf

@asset(deps=[distribucion_renta_canarias_csv])
def imagen_1(distribucion_renta_canarias_csv):
  # Cargamos los datos del CSV
  df = distribucion_renta_canarias_csv.copy()
  # Renombramos las columnas
  df = df.rename(columns={"TIME_PERIOD_CODE": "año", "MEDIDAS#es": "tipo_renta",
                          "OBS_VALUE": "valor", "TERRITORIO#es": "territorio"})
  # Filtramos por "Canarias"
  df = df[df["territorio"] == "Canarias"]
  # Eliminamos el resto de columnas
  df = df[["año", "tipo_renta", "valor", "territorio"]]
  # Eliminamos valores faltantes
  df = df.dropna(subset=["valor"])
  # Convertimos año a "entero"
  df["año"] = df["año"].astype(int)
  # Generamos el gráfico de líneas
  grafico = (ggplot(df, aes(x="año", y="valor", color="tipo_renta", group="tipo_renta"))
    + geom_line(size=1.5)
    + geom_point(size=3)
    + labs(title="Distribución de renta por tipo y año en Canarias",
           x="Año", y="Porcentaje", color="Tipo de renta")
    + theme_minimal()
    + scale_color_brewer(type="qual", palette="Set1")
    + theme(figure_size=(10, 6), plot_title=element_text(weight="bold"))
    )
  # Guardamos el gráfico en PNG
  grafico.save("images/distrenta_img1", dpi=300)

@asset(deps=[distribucion_renta_canarias_csv])
def imagen_2(distribucion_renta_canarias_csv):
  # Cargamos los datos del CSV
  df = distribucion_renta_canarias_csv.copy()
  # Renombramos las columnas
  df = df.rename(columns={"TIME_PERIOD_CODE": "año", "MEDIDAS#es": "tipo_renta",
                          "OBS_VALUE": "valor", "TERRITORIO#es": "territorio"})
  # Filtramos por "Canarias"
  df = df[df["territorio"] == "Canarias"]
  # Eliminamos el resto de columnas
  df = df[["año", "tipo_renta", "valor", "territorio"]]
  # Eliminamos valores faltantes
  df = df.dropna(subset=["valor"])
  # Convertimos año a "string"
  df["año"] = df["año"].astype(str)
  # Generamos el gráfico de barras agrupadas
  grafico = (ggplot(df, aes(x="año", y="valor", fill="tipo_renta"))
    + geom_col(position="dodge")
    + labs(title="Distribución de renta por tipo y año en Canarias",
           x="Año", y="Porcentaje", fill="Tipo de renta")
    + theme_minimal()
    + scale_fill_brewer(type="qual", palette="Set1")
    + theme(figure_size=(10, 6), plot_title=element_text(weight="bold"))
    )
  # Guardamos el gráfico en PNG
  grafico.save("images/distrenta_img2", dpi=300)

@asset(deps=[distribucion_renta_canarias_csv, municipios_geojson])
def imagen_3(distribucion_renta_canarias_csv, municipios_geojson):
  # Cargamos los datos geográficos
  gdf = municipios_geojson.copy()
  gdf = gdf.explode(index_parts=False).reset_index(drop=True)
  gdf['poly_id'] = gdf.index.astype(str)
  # Recorremos cada polígono y extraemos las coordenadas
  polygons = []
  for _, row in gdf.iterrows():
    geom = row.geometry 
    if geom is None:
      continue
    if geom.geom_type == 'Polygon':
      polys = [geom]
    elif geom.geom_type == 'MultiPolygon':
      polys = list(geom.geoms)
    else:
      continue
    for p in polys:
      coords = list(p.exterior.coords)
      for x, y in coords:
        polygons.append({'poly_id': row['poly_id'], 'codigo': row['codigo'],
                         'nombre': row['nombre'], 'x': x, 'y': y})
  # Convertimos a un "dataframe"
  geo_df = pd.DataFrame(polygons)
  # Cargamos los datos de rentas
  df = distribucion_renta_canarias_csv.copy()
  # Seleccionamos el año
  año = 2023
  # Creamos una lista con las categorías
  categorias = ["Otras prestaciones", "Otros ingresos", "Pensiones",
                "Prestaciones por desempleo", "Sueldos y salarios"]
  mapa_df_list = []
  for tipo_sel in categorias:
    # Filtramos por año y tipo de renta
    df_cat = df[(df["TIME_PERIOD_CODE"] == año) & (df["MEDIDAS#es"] == tipo_sel)]
    # Renombramos las columnas
    df_cat = df_cat.rename(columns={"TERRITORIO_CODE": "codigo", "OBS_VALUE": "valor"})
    # Limpiamos el código territorial
    df_cat['codigo'] = (df_cat['codigo'].astype(str).str.split('_').str[0])
    # Eliminamos los datos agregados
    df_cat = df_cat[~df_cat['codigo'].str.startswith("ES")].copy()
    # Convertimos a "entero"
    df_cat['codigo'] = df_cat['codigo'].astype(int)
    # Unimos con el dataframe geográfico
    merged = geo_df.merge(df_cat[['codigo', 'valor']], on='codigo', how='left')
    # Añadimos la columna de categoría
    merged['categoria'] = tipo_sel
    mapa_df_list.append(merged)
  # Unimos todas las categorías en un único DataFrame
  mapa_df = pd.concat(mapa_df_list, ignore_index=True)
  # Generamos el gráfico de polígonos
  mapa_plotnine = (ggplot(mapa_df, aes(x='x', y='y', group='poly_id', fill='valor'))
    + geom_polygon(color='black', size=0.5)
    + coord_fixed()
    + facet_wrap('~categoria', scales='free')
    + scale_fill_distiller(type='div', palette='Spectral')
    + labs(title=f"Distribución de renta por municipio en {año}",
           fill="Porcentaje")
    + theme_void()
    + theme(plot_title=element_text(weight="bold", size=14))
  )
  # Guardamos el archivo en PNG
  mapa_plotnine.save("images/distrenta_img3", dpi=300, width=14, height=6, units="in")

@asset(deps=[distribucion_renta_canarias_csv, codislas_csv])
def imagen_4(distribucion_renta_canarias_csv, codislas_csv):
  # Seleccionamos la isla y el año
  isla_sel = "Gran Canaria"
  año_sel = 2023
  # Cargamos los datos de los municipios
  codislas = codislas_csv.copy()
  codislas_isla = codislas[codislas["ISLA"] == isla_sel].copy()
  municipios_isla = codislas_isla["NOMBRE"].tolist()
  # Cargamos los datos de la renta 
  df = distribucion_renta_canarias_csv.copy()
  # Renombramos las columnas
  df = df.rename(columns={"TIME_PERIOD_CODE": "año", "MEDIDAS#es": "tipo_renta",
                          "OBS_VALUE": "valor", "TERRITORIO#es": "municipio"})
  # Filtramos por año y municipios de la isla
  df = df[(df["año"] == año_sel) & (df["municipio"].isin(municipios_isla))].copy()
  # Eliminamos valores faltantes
  df = df.dropna(subset=["valor"])
  # Generamos el gráfico de barras apiladas
  grafico = (ggplot(df, aes(x="municipio", y="valor", fill="tipo_renta"))
    + geom_col(position="stack")
    + coord_flip()
    + labs(title=f"Distribución de renta por municipio de {isla_sel} en {año_sel}",
           x="Municipio", y="Porcentaje", fill="Tipo de renta")
    + theme_minimal()
    + scale_fill_brewer(type="qual", palette="Set1")
    + theme(figure_size=(10, 8), plot_title=element_text(weight="bold"))
  )
  # Guardamos el archivo en PNG
  grafico.save("images/distrenta_img4", dpi=300)

@asset(deps=[imagen_1, imagen_2, imagen_3, imagen_4])
def git_push():
  repo_path = "."
  commit_msg = "Actualización desde Dagster"
  if not os.path.exists(repo_path):
    raise FileNotFoundError(f"El repositorio no existe: {repo_path}")
  try:
    # Agregamos todos los cambios
    subprocess.run(["git", "-C", repo_path, "add", "."], check=True)
    # Hacemos un commit
    subprocess.run(["git", "-C", repo_path, "commit", "-m", commit_msg], check=True)
    # Hacemos un push
    result = subprocess.run(["git", "-C", repo_path, "push"], capture_output=True, text=True, check=True)
    print(f"Git push realizado correctamente: {result.stdout}")
  except subprocess.CalledProcessError as e:
    print(f"Error haciendo git push: {e.stderr}")
    raise
