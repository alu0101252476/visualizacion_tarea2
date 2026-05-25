import pandas as pd
import os
import textwrap
from dagster import asset
from plotnine import *
from .lab_data import actividad_csv, rentamedia_csv, secciones_geojson


# Medias de renta (https://www.ine.es/jaxiT3/Datos.htm?t=53689).
MEDIA_NACIONAL = {2021: 41264, 2022: 43827, 2023: 47046}
MEDIA_CANARIA  = {2021: 36406, 2022: 39572, 2023: 43499}

# Año del GeoJSON para renta media.
RENTA_GEO_AÑO = {2021: 2022, 2022: 2023, 2023: 2024}

# Año para los gráficos de actividades económicas.
AÑO = 2021

# Colores fijos para sexo dominante.
COLORES_SEXO = {"Hombres": "#42A5F5", "Mujeres": "#EC407A"}

# Colores fijos para comparación con media de referencia.
COLORES_RENTA = {"Por encima": "#4ABD7A", "Por debajo": "#E74C3C"}


# Función que ajusta un texto a varias líneas para etiquetas de facets/leyenda.
def _wrap(s, width=30):
    return "\n".join(textwrap.wrap(str(s), width))


# Función que convierte un GeoDataFrame en un DataFrame plano de coordenadas.
def _poly_df(gdf, keep_cols):
    gdf = gdf[keep_cols + ["geometry"]].copy()
    gdf = gdf.explode(index_parts=False).reset_index(drop=True)
    gdf["poly_id"] = gdf.index.astype(str)
    rows = []
    for _, row in gdf.iterrows():
        geom = row.geometry
        if geom is None:
            continue
        polys = (
            [geom] if geom.geom_type == "Polygon"
            else list(geom.geoms) if geom.geom_type == "MultiPolygon"
            else []
        )
        for p in polys:
            for x, y in p.exterior.coords:
                entry = {c: row[c] for c in keep_cols + ["poly_id"]}
                entry["x"], entry["y"] = x, y
                rows.append(entry)
    return pd.DataFrame(rows)


# Función que filtra el GeoDataFrame para un año y devuelve solo las columnas útiles.
def _secciones_año(secciones_geojson, año):
    return (
        secciones_geojson[secciones_geojson["año"] == año]
        [["geocode", "gcd_municipio", "geometry"]]
        .copy()
    )


# Función que pasa un GeoDataFrame de secciones a municipios.
def _municipios_gdf(gdf_sec):
    return (
        gdf_sec.dissolve(by="gcd_municipio")
        .reset_index()[["gcd_municipio", "geometry"]]
    )


# Función que pivota 'num_casos' por Sexo y calcula la columna 'domina'.
def _pivot_sexo(df, key):
    df = df.copy()
    df["num_casos"] = pd.to_numeric(df["num_casos"], errors="coerce").fillna(0)
    piv = (
        df.pivot_table(
            index=[key, "Actividad económica"],
            columns="Sexo", values="num_casos", aggfunc="sum",
        )
        .reset_index()
    )
    piv.columns.name = None
    for s in ("Hombres", "Mujeres"):
        if s not in piv.columns:
            piv[s] = 0
    piv["domina"] = piv.apply(
        lambda r: "Hombres" if r["Hombres"] > r["Mujeres"] else "Mujeres", axis=1
    )
    piv["actividad"] = piv["Actividad económica"].apply(_wrap)
    return piv


# Función que devuelve la actividad económica con mayor suma de casos por agrupación.
def _actividad_dominante(df, key):
    df = df.copy()
    df["num_casos"] = pd.to_numeric(df["num_casos"], errors="coerce").fillna(0)
    agg = df.groupby([key, "Actividad económica"])["num_casos"].sum().reset_index()
    dom = agg.loc[agg.groupby(key)["num_casos"].idxmax(), [key, "Actividad económica"]].copy()
    dom["actividad"] = dom["Actividad económica"].apply(_wrap)
    return dom.drop(columns=["Actividad económica"])


# Función que genera un mapa rojo/verde comparando renta media con el valor de referencia.
def _mapa_renta(rentamedia_csv, secciones_geojson, media_ref, level, title, path):
    frames = []
    for año in (2021, 2022, 2023):
        df = rentamedia_csv[rentamedia_csv["año"] == año].copy()
        df = df[df["MEDIDAS_CODE"] == "RENTA_BRUTA_MEDIA_HOGAR"].copy()
        umbral = media_ref[año]
        gdf = _secciones_año(secciones_geojson, RENTA_GEO_AÑO[año])

        if level == "seccion":
            df = df.rename(columns={"TERRITORIO_CODE": "geocode"})
            merged = gdf.merge(df[["geocode", "OBS_VALUE"]], on="geocode", how="left")
            id_col = "geocode"
        else:
            df["gcd_municipio"] = df["TERRITORIO_CODE"].str.split("_").str[1]
            df_mun = df.groupby("gcd_municipio")["OBS_VALUE"].mean().reset_index()
            merged = _municipios_gdf(gdf).merge(df_mun, on="gcd_municipio", how="left")
            id_col = "gcd_municipio"

        merged["color"] = merged["OBS_VALUE"].apply(
            lambda v: "Por debajo" if pd.notna(v) and v < umbral
            else ("Por encima" if pd.notna(v) else None)
        )
        merged = merged.dropna(subset=["color"])
        merged["año"] = str(año)
        frames.append(_poly_df(merged, [id_col, "color", "año"]))

    combined = pd.concat(frames, ignore_index=True)
    figsize = (18, 7) if level == "seccion" else (18, 8)

    g = (
        ggplot(combined, aes("x", "y", group="poly_id", fill="color"))
        + geom_polygon(color="grey", size=0.05)
        + coord_fixed()
        + facet_wrap("~año", ncol=3)
        + scale_fill_manual(values=COLORES_RENTA, na_value="grey")
        + labs(title=title, fill="La media está")
        + theme_void()
        + theme(figure_size=figsize, plot_title=element_text(weight="bold", size=14))
    )
    os.makedirs(os.path.dirname(path), exist_ok=True)
    g.save(path, dpi=150)


# Mapa por secciones censales del sexo dominante por actividad económica.
@asset(deps=[actividad_csv, secciones_geojson])
def imagen_actividad_sec_sexo(actividad_csv, secciones_geojson):
    df = actividad_csv[actividad_csv["Periodo"] == AÑO].copy()
    piv = _pivot_sexo(df, "geocode")

    gdf = _secciones_año(secciones_geojson, AÑO)
    merged = gdf.merge(piv, on="geocode", how="left")
    poly = _poly_df(merged, ["geocode", "actividad", "domina"])

    g = (
        ggplot(poly, aes("x", "y", group="poly_id", fill="domina"))
        + geom_polygon(color="grey", size=0.05)
        + coord_fixed()
        + facet_wrap("~actividad", ncol=3)
        + scale_fill_manual(values=COLORES_SEXO, na_value="grey")
        + labs(title=f"Sexo dominante por actividad económica (secciones) para {AÑO}",
               fill="Sexo dominante")
        + theme_void()
        + theme(figure_size=(18, 14), plot_title=element_text(weight="bold", size=14),
                strip_text=element_text(size=10))
    )
    g.save("./images/actividad_sec_sexo.png", dpi=150)


# Mapa por municipios del sexo dominante por actividad económica.
@asset(deps=[actividad_csv, secciones_geojson])
def imagen_actividad_mun_sexo(actividad_csv, secciones_geojson):
    df = actividad_csv[actividad_csv["Periodo"] == AÑO].copy()
    df["cod_municipio"] = df["cod_municipio"].astype(str)
    piv = _pivot_sexo(df, "cod_municipio")

    gdf = _secciones_año(secciones_geojson, AÑO)
    gdf_mun = _municipios_gdf(gdf)
    merged = gdf_mun.merge(piv, left_on="gcd_municipio", right_on="cod_municipio", how="left")
    poly = _poly_df(merged, ["gcd_municipio", "actividad", "domina"])

    g = (
        ggplot(poly, aes("x", "y", group="poly_id", fill="domina"))
        + geom_polygon(color="grey", size=0.2)
        + coord_fixed()
        + facet_wrap("~actividad", ncol=3)
        + scale_fill_manual(values=COLORES_SEXO, na_value="grey")
        + labs(title=f"Sexo dominante por actividad económica (municipios) para {AÑO}",
               fill="Sexo dominante")
        + theme_void()
        + theme(figure_size=(18, 14), plot_title=element_text(weight="bold", size=14),
                strip_text=element_text(size=10))
    )
    g.save("./images/actividad_mun_sexo.png", dpi=150)


# Mapa por secciones censales de la actividad económica dominante.
@asset(deps=[actividad_csv, secciones_geojson])
def imagen_actividad_sec_dom(actividad_csv, secciones_geojson):
    df = actividad_csv[actividad_csv["Periodo"] == AÑO].copy()
    dom = _actividad_dominante(df, "geocode")

    gdf = _secciones_año(secciones_geojson, AÑO)
    merged = gdf.merge(dom, on="geocode", how="left")
    poly = _poly_df(merged, ["geocode", "actividad"])

    g = (
        ggplot(poly, aes("x", "y", group="poly_id", fill="actividad"))
        + geom_polygon(color="grey", size=0.05)
        + coord_fixed()
        + scale_fill_brewer(type="qual", palette="Paired", na_value="grey")
        + labs(title=f"Actividad económica dominante (secciones) para {AÑO}",
               fill="Actividad")
        + theme_void()
        + theme(figure_size=(14, 10), plot_title=element_text(weight="bold", size=14),
                legend_text=element_text(size=10))
    )
    g.save("./images/actividad_sec_dom.png", dpi=150)


# Mapa por municipios de la actividad económica dominante.
@asset(deps=[actividad_csv, secciones_geojson])
def imagen_actividad_mun_dom(actividad_csv, secciones_geojson):
    df = actividad_csv[actividad_csv["Periodo"] == AÑO].copy()
    df["cod_municipio"] = df["cod_municipio"].astype(str)
    dom = _actividad_dominante(df, "cod_municipio")

    gdf = _secciones_año(secciones_geojson, AÑO)
    gdf_mun = _municipios_gdf(gdf)
    merged = gdf_mun.merge(dom, left_on="gcd_municipio", right_on="cod_municipio", how="left")
    poly = _poly_df(merged, ["gcd_municipio", "actividad"])

    g = (
        ggplot(poly, aes("x", "y", group="poly_id", fill="actividad"))
        + geom_polygon(color="grey", size=0.2)
        + coord_fixed()
        + scale_fill_brewer(type="qual", palette="Paired", na_value="grey")
        + labs(title=f"Actividad económica dominante (municipio) para {AÑO}",
               fill="Actividad")
        + theme_void()
        + theme(figure_size=(14, 10), plot_title=element_text(weight="bold", size=14),
                legend_text=element_text(size=10))
    )
    g.save("./images/actividad_mun_dom.png", dpi=150)


# Mapa por secciones de la renta media sobre/bajo la media nacional.
@asset(deps=[rentamedia_csv, secciones_geojson])
def imagen_renta_sec_nacional(rentamedia_csv, secciones_geojson):
    _mapa_renta(
        rentamedia_csv, secciones_geojson, MEDIA_NACIONAL,
        level="seccion",
        title="Renta bruta media por hogar vs. media nacional (secciones)",
        path="./images/renta_sec_nacional.png",
    )


# Mapa por municipios de la renta media sobre/bajo la media nacional.
@asset(deps=[rentamedia_csv, secciones_geojson])
def imagen_renta_mun_nacional(rentamedia_csv, secciones_geojson):
    _mapa_renta(
        rentamedia_csv, secciones_geojson, MEDIA_NACIONAL,
        level="municipio",
        title="Renta bruta media por hogar vs. media nacional (municipios)",
        path="./images/renta_mun_nacional.png",
    )


# Mapa por secciones de la renta media sobre/bajo la media canaria.
@asset(deps=[rentamedia_csv, secciones_geojson])
def imagen_renta_sec_canaria(rentamedia_csv, secciones_geojson):
    _mapa_renta(
        rentamedia_csv, secciones_geojson, MEDIA_CANARIA,
        level="seccion",
        title="Renta bruta media por hogar vs. media canaria (secciones)",
        path="./images/renta_sec_canaria.png",
    )


# Mapa por municipios de la renta media sobre/bajo la media canaria.
@asset(deps=[rentamedia_csv, secciones_geojson])
def imagen_renta_mun_canaria(rentamedia_csv, secciones_geojson):
    _mapa_renta(
        rentamedia_csv, secciones_geojson, MEDIA_CANARIA,
        level="municipio",
        title="Renta bruta media por hogar vs. media canaria (municipios)",
        path="./images/renta_mun_canaria.png",
    )
