import os
import pandas as pd
from PIL import Image
import tempfile
import textwrap
from dagster import asset
from plotnine import *
from .lab_data import rentamedia_csv, ocupacion_csv, secciones_geojson
from .lab_graph1 import _poly_df, _wrap


# Medias de renta (https://www.ine.es/jaxiT3/Datos.htm?t=53689).
MEDIA_NACIONAL = {2021: 41264, 2022: 43827, 2023: 47046}
MEDIA_CANARIA  = {2021: 36406, 2022: 39572, 2023: 43499}

# Año para los gráficos de actividades económicas.
AÑO = 2021

# Colores fijos para sexo dominante.
COLORES_SEXO = {"Hombres": "#1565C0", "Mujeres": "#C2185B"}

# Nombre de la provincia.
PROVINCIA_NOMBRE = "S/C de Tenerife"

# Islas de Santa Cruz de Tenerife.
ISLA_NOMBRES = {
    "ES709": "Tenerife",
    "ES707": "La Palma",
    "ES706": "La Gomera",
    "ES703": "El Hierro",
}


# Función que filtra el GeoDataFrame combinado para un año concreto.
def _secciones_año(secciones_geojson, año):
    return (
        secciones_geojson[secciones_geojson["año"] == año]
        [["geocode", "gcd_municipio", "gcd_isla", "geometry"]]
        .copy()
    )


# Función que devuelve el mapa en 2021.
def _mun_isla_map(secciones_geojson):
    return (
        secciones_geojson[secciones_geojson["año"] == 2021]
        [["gcd_municipio", "gcd_isla"]]
        .drop_duplicates()
    )


# Gráfico de barras agrupado por número de municipios y si superan o no la renta media.
@asset(deps=[rentamedia_csv])
def imagen_evolucion_municipios_media(rentamedia_csv):
    df = rentamedia_csv[rentamedia_csv["MEDIDAS_CODE"] == "RENTA_BRUTA_MEDIA_HOGAR"].copy()
    df["gcd_municipio"] = df["TERRITORIO_CODE"].str.split("_").str[1]

    rows = []
    for año in [2021, 2022, 2023]:
        df_y = df[df["año"] == año]
        mun_media = df_y.groupby("gcd_municipio")["OBS_VALUE"].mean()
        for ref, umbral in [
            ("Media Nacional", MEDIA_NACIONAL[año]),
            ("Media Canaria",  MEDIA_CANARIA[año]),
        ]:
            rows.append({
                "Año":        str(año),
                "Referencia": ref,
                "Municipios": int((mun_media > umbral).sum()),
            })

    data = pd.DataFrame(rows)

    g = (
        ggplot(data, aes("Año", "Municipios", fill="Referencia"))
        + geom_bar(stat="identity", position=position_dodge(width=0.7), width=0.65)
        + geom_text(
            aes(label="Municipios"),
            position=position_dodge(width=0.7),
            va="bottom", size=10,
        )
        + scale_fill_manual(values={"Media Nacional": "#1565C0", "Media Canaria": "#2E7D32"})
        + labs(
            title="Municipios con renta media bruta por hogar\npor encima de la media de referencia.",
            x="Año", y="Número de municipios", fill="Referencia",
        )
        + theme_classic()
        + theme(figure_size=(10, 6), plot_title=element_text(weight="bold", size=14))
    )
    os.makedirs("./images", exist_ok=True)
    g.save("./images/evolucion_mun_media.png", dpi=150)


# Gráfico de barras agrupado por número de secciones y si superan o no la renta media.
@asset(deps=[rentamedia_csv])
def imagen_evolucion_secciones_media(rentamedia_csv):
    df = rentamedia_csv[rentamedia_csv["MEDIDAS_CODE"] == "RENTA_BRUTA_MEDIA_HOGAR"].copy()

    rows = []
    for año in [2021, 2022, 2023]:
        df_y = df[df["año"] == año]
        for ref, umbral in [
            ("Media Nacional", MEDIA_NACIONAL[año]),
            ("Media Canaria",  MEDIA_CANARIA[año]),
        ]:
            rows.append({
                "Año":      str(año),
                "Referencia": ref,
                "Secciones": int((df_y["OBS_VALUE"] > umbral).sum()),
            })

    data = pd.DataFrame(rows)

    g = (
        ggplot(data, aes("Año", "Secciones", fill="Referencia"))
        + geom_bar(stat="identity", position=position_dodge(width=0.7), width=0.65)
        + geom_text(
            aes(label="Secciones"),
            position=position_dodge(width=0.7),
            va="bottom", size=10,
        )
        + scale_fill_manual(values={"Media Nacional": "#1565C0", "Media Canaria": "#2E7D32"})
        + labs(
            title="Secciones censales con renta media bruta por hogar\npor encima de la media de referencia.",
            x="Año", y="Número de secciones", fill="Referencia",
        )
        + theme_classic()
        + theme(figure_size=(10, 6), plot_title=element_text(weight="bold", size=14))
    )
    os.makedirs("./images", exist_ok=True)
    g.save("./images/evolucion_sec_media.png", dpi=150)


# Mapas de calor por ocupación.
@asset(deps=[ocupacion_csv, secciones_geojson])
def imagen_ocupacion_mapa_calor(ocupacion_csv, secciones_geojson):
    df = ocupacion_csv[ocupacion_csv["año"] == AÑO].copy()
    df["num_casos"] = pd.to_numeric(df["num_casos"], errors="coerce").fillna(0)
    agg = df.groupby(["geocode", "ocupacion"])["num_casos"].sum().reset_index()
    gdf = _secciones_año(secciones_geojson, 2021)
    ocupaciones = sorted(agg["ocupacion"].unique())
 
    tmp_paths = []
    for ocu in ocupaciones:
        df_ocu = agg[agg["ocupacion"] == ocu][["geocode", "num_casos"]]
        merged  = gdf.merge(df_ocu, on="geocode", how="left")
        poly    = _poly_df(merged, ["geocode", "num_casos"])
 
        v_min = poly["num_casos"].min(skipna=True)
        v_max = poly["num_casos"].max(skipna=True)
        if v_min == v_max:
            v_max = v_min + 1
 
        titulo = "\n".join(textwrap.wrap(ocu, 32))
 
        g = (
            ggplot(poly, aes("x", "y", group="poly_id", fill="num_casos"))
            + geom_polygon(color="white", size=0.1)
            + coord_fixed()
            + scale_fill_gradient(
                low="#FFF9C4", high="#BF360C",
                limits=(v_min, v_max),
                name="Casos",
                na_value="lightgrey",
            )
            + labs(title=titulo)
            + theme_void()
            + theme(
                figure_size=(5, 4.5),
                plot_title=element_text(weight="bold", size=14, ha="center"),
                legend_title=element_text(size=12),
                legend_text=element_text(size=10),
            )
        )
 
        with tempfile.NamedTemporaryFile(suffix=".png", delete=False) as f:
            tmp_path = f.name
        g.save(tmp_path, dpi=130, verbose=False)
        tmp_paths.append(tmp_path)

    imgs  = [Image.open(p) for p in tmp_paths]
    ncols = 2
    nrows = (len(imgs) + ncols - 1) // ncols
    w, h  = imgs[0].size

    canvas = Image.new("RGB", (w * ncols, h * nrows), color="white")
    for i, img in enumerate(imgs):
        row, col = divmod(i, ncols)
        canvas.paste(img, (col * w, row * h))

    for img in imgs:
        img.close()
    for p in tmp_paths:
        os.unlink(p)
 
    os.makedirs("./images", exist_ok=True)
    canvas.save("./images/ocupacion_mapa_calor.png", dpi=(150, 150))
    canvas.close()


# Gráfico de barras agrupado de hombres y mujeres por ocupación.
@asset(deps=[ocupacion_csv])
def imagen_ocupacion_provincia_sexo(ocupacion_csv):
    df = ocupacion_csv.copy()
    df["num_casos"] = pd.to_numeric(df["num_casos"], errors="coerce").fillna(0)

    agg = (
        df.groupby(["ocupacion", "sexo"])["num_casos"]
        .sum()
        .reset_index()
    )
    agg["ocupacion_w"] = agg["ocupacion"].apply(lambda s: _wrap(s, 32))

    g = (
        ggplot(agg, aes("ocupacion_w", "num_casos", fill="sexo"))
        + geom_bar(stat="identity", position=position_dodge(width=0.75), width=0.7)
        + scale_fill_manual(values=COLORES_SEXO)
        + coord_flip()
        + labs(
            title="Número de hombres y mujeres por ocupación",
            x="Ocupación", y="Número de casos", fill="Sexo",
        )
        + theme_classic()
        + theme(
            figure_size=(14, 8),
            plot_title=element_text(weight="bold", size=14),
            axis_text_y=element_text(size=10),
        )
    )
    os.makedirs("./images", exist_ok=True)
    g.save("./images/ocupacion_prov_sexo.png", dpi=150)


# Gráfico de barras agrupado por ocupación de todas las islas.
@asset(deps=[ocupacion_csv, secciones_geojson])
def imagen_ocupacion_isla_sexo(ocupacion_csv, secciones_geojson):
    df = ocupacion_csv.copy()
    df["num_casos"] = pd.to_numeric(df["num_casos"], errors="coerce").fillna(0)
    df["cod_municipio"] = df["code_municipio"].astype(str)

    # Incorporar isla a cada fila.
    mun_isla = _mun_isla_map(secciones_geojson)
    df = df.merge(mun_isla, left_on="cod_municipio", right_on="gcd_municipio", how="left")
    df["isla"] = df["gcd_isla"].map(ISLA_NOMBRES)
    df = df.dropna(subset=["isla"])

    agg = (
        df.groupby(["isla", "ocupacion", "sexo"])["num_casos"]
        .sum()
        .reset_index()
    )
    agg["ocupacion_w"] = agg["ocupacion"].apply(lambda s: _wrap(s, 28))

    g = (
        ggplot(agg, aes("ocupacion_w", "num_casos", fill="sexo"))
        + geom_bar(stat="identity", position=position_dodge(width=0.75), width=0.7)
        + scale_fill_manual(values=COLORES_SEXO)
        + coord_flip()
        + facet_wrap("~isla", ncol=2, scales="free_x")
        + labs(
            title="Número de hombres y mujeres por ocupación e isla",
            x="Ocupación", y="Número de casos", fill="Sexo",
        )
        + theme_classic()
        + theme(
            figure_size=(18, 12),
            plot_title=element_text(weight="bold", size=14),
            strip_text=element_text(size=10, weight="bold"),
            axis_text_y=element_text(size=10),
        )
    )
    os.makedirs("./images", exist_ok=True)
    g.save("./images/ocupacion_isla_sexo.png", dpi=150)
