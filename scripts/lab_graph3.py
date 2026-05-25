import os
import pandas as pd
from dagster import asset
from plotnine import *
from .lab_data import (
    distribucion_renta_ingresos_csv,
    ocupacion_csv,
    actividad_csv,
    rentamedia_csv,
    secciones_geojson,
)
from .lab_graph1 import _wrap


# Colores para los grupos top/bottom de municipios.
COLORES_GRUPO = {
    "Top 10 (renta alta)":  "#C62828",
    "Bottom 10 (renta baja)": "#1565C0",
}

# Colores para islas.
COLORES_ISLA = {
    "Tenerife":    "#1565C0",
    "La Palma":    "#2E7D32",
    "La Gomera":   "#E65100",
    "El Hierro":   "#6A1B9A",
}

# Nombres de las islas.
ISLA_NOMBRES = {
    "ES709": "Tenerife",
    "ES707": "La Palma",
    "ES706": "La Gomera",
    "ES703": "El Hierro",
}

# Año para los gráficos de actividades económicas.
AÑO = 2021


# Función que devuelve el mapa en 2021.
def _mun_isla_map(secciones_geojson):
    return (
        secciones_geojson[secciones_geojson["año"] == 2021]
        [["gcd_municipio", "gcd_isla"]]
        .drop_duplicates()
    )


# Función que calcula los n municipios con mayor y menor renta.
def _top_bottom_municipios(rentamedia_csv, n=10):
    df = rentamedia_csv[rentamedia_csv["MEDIDAS_CODE"] == "RENTA_BRUTA_MEDIA_HOGAR"].copy()
    df["gcd_municipio"] = df["TERRITORIO_CODE"].str.split("_").str[1]

    result = {}
    for año in sorted(df["año"].unique()):
        mun_media = (
            df[df["año"] == año]
            .groupby("gcd_municipio")["OBS_VALUE"]
            .mean()
        )
        result[año] = {
            "top":    set(mun_media.nlargest(n).index),
            "bottom": set(mun_media.nsmallest(n).index),
        }
    return result


# Función que construye las medidas.
def _label_medidas(df):
    return (
        df[["MEDIDAS_CODE", "MEDIDAS#es"]]
        .drop_duplicates()
        .assign(**{"MEDIDAS#es": lambda d: d["MEDIDAS#es"].str.strip()})
        .set_index("MEDIDAS_CODE")["MEDIDAS#es"]
        .apply(lambda s: _wrap(s, 22))
        .to_dict()
    )


# Diagrama de líneas para cada tipo de renta por los 10 municipios con mayor y menor renta.
@asset(deps=[distribucion_renta_ingresos_csv, rentamedia_csv])
def imagen_distribucion_renta_top_bottom(
    distribucion_renta_ingresos_csv, rentamedia_csv
):
    df = distribucion_renta_ingresos_csv.copy()
    df["gcd_municipio"] = df["TERRITORIO_CODE"].str.split("_").str[1]

    label_map = _label_medidas(df)
    tb = _top_bottom_municipios(rentamedia_csv)
    años = sorted(tb.keys())

    rows = []
    for año in años:
        df_y = df[df["año"] == año]
        for grupo, muns in [
            ("Top 10 (renta alta)",   tb[año]["top"]),
            ("Bottom 10 (renta baja)", tb[año]["bottom"]),
        ]:
            agg = (
                df_y[df_y["gcd_municipio"].isin(muns)]
                .groupby("MEDIDAS_CODE")["OBS_VALUE"]
                .mean()
                .reset_index()
            )
            agg["año"]   = str(año)
            agg["grupo"] = grupo
            rows.append(agg)

    data = pd.concat(rows, ignore_index=True)
    data["medida_w"] = data["MEDIDAS_CODE"].map(label_map)

    g = (
        ggplot(data, aes("año", "OBS_VALUE", color="grupo", group="grupo"))
        + geom_line(size=1.2)
        + geom_point(size=3)
        + facet_wrap("~medida_w", ncol=2, scales="free_y")
        + scale_color_manual(values=COLORES_GRUPO)
        + labs(
            title="Distribución de renta por tipo y grupo de municipios",
            x="Año", y="Valor medio (%)", color="Grupo de municipios",
        )
        + theme_classic()
        + theme(
            figure_size=(16, 12),
            plot_title=element_text(weight="bold", size=14),
            strip_text=element_text(size=10),
            legend_text=element_text(size=12),
        )
    )
    os.makedirs("./images", exist_ok=True)
    g.save("./images/distribucion_renta_tb.png", dpi=150)


# Diagrama de líneas para cada tipo de renta por isla.
@asset(deps=[distribucion_renta_ingresos_csv, secciones_geojson])
def imagen_distribucion_renta_isla(
    distribucion_renta_ingresos_csv, secciones_geojson
):
    df = distribucion_renta_ingresos_csv.copy()
    df["gcd_municipio"] = df["TERRITORIO_CODE"].str.split("_").str[1]

    label_map = _label_medidas(df)

    mun_isla = _mun_isla_map(secciones_geojson)
    df = df.merge(mun_isla, on="gcd_municipio", how="left")
    df["isla"] = df["gcd_isla"].map(ISLA_NOMBRES)
    df = df.dropna(subset=["isla"])

    agg = (
        df.groupby(["año", "isla", "MEDIDAS_CODE"])["OBS_VALUE"]
        .sum()
        .reset_index()
    )
    agg["año"]      = agg["año"].astype(str)
    agg["medida_w"] = agg["MEDIDAS_CODE"].map(label_map)

    g = (
        ggplot(agg, aes("año", "OBS_VALUE", color="isla", group="isla"))
        + geom_line(size=1.2)
        + geom_point(size=3)
        + facet_wrap("~medida_w", ncol=2, scales="free_y")
        + scale_color_manual(values=COLORES_ISLA)
        + labs(
            title="Distribución de renta por tipo e isla",
            x="Año", y="Valor (%)", color="Isla",
        )
        + theme_classic()
        + theme(
            figure_size=(16, 12),
            plot_title=element_text(weight="bold", size=14),
            strip_text=element_text(size=10),
            legend_text=element_text(size=12),
        )
    )
    os.makedirs("./images", exist_ok=True)
    g.save("./images/distribucion_renta_isla.png", dpi=150)


# Diagrama de líneas de ocupaciones por los 10 municipios con mayor y menor renta.
@asset(deps=[ocupacion_csv, rentamedia_csv])
def imagen_ocupacion_top_bottom(ocupacion_csv, rentamedia_csv):
    """
    Diagrama de líneas: para cada ocupación (facet), evolución del número
    total de casos en los 10 municipios con mayor renta (rojo) y en los 10
    con menor renta (azul), agregando ambos sexos.
    """
    df = ocupacion_csv.copy()
    df["num_casos"] = pd.to_numeric(df["num_casos"], errors="coerce").fillna(0)
    df["gcd_municipio"] = df["code_municipio"].astype(str)

    tb   = _top_bottom_municipios(rentamedia_csv)
    años = sorted(df["año"].unique())

    rows = []
    for año in años:
        if año not in tb:
            continue
        df_y = df[df["año"] == año]
        for grupo, muns in [
            ("Top 10 (renta alta)", tb[año]["top"]),
            ("Bottom 10 (renta baja)", tb[año]["bottom"]),
        ]:
            agg = (
                df_y[df_y["gcd_municipio"].isin(muns)]
                .groupby("ocupacion")["num_casos"]
                .sum()
                .reset_index()
            )
            agg["año"] = str(año)
            agg["grupo"] = grupo
            rows.append(agg)

    data = pd.concat(rows, ignore_index=True)
    data["ocupacion_w"] = data["ocupacion"].apply(lambda s: _wrap(s, 28))

    g = (
        ggplot(data, aes("año", "num_casos", color="grupo", group="grupo"))
        + geom_line(size=1.2)
        + geom_point(size=3)
        + facet_wrap("~ocupacion_w", ncol=2, scales="free_y")
        + scale_color_manual(values=COLORES_GRUPO)
        + labs(
            title="Ocupación por tipo y grupo de municipios",
            x="Año", y="Total de casos", color="Grupo de municipios",
        )
        + theme_classic()
        + theme(
            figure_size=(16, 12),
            plot_title=element_text(weight="bold", size=14),
            strip_text=element_text(size=10),
            legend_text=element_text(size=12),
        )
    )
    os.makedirs("./images", exist_ok=True)
    g.save("./images/ocupacion_tb.png", dpi=150)


# Diagrama de líneas de ocupaciones por isla.
@asset(deps=[ocupacion_csv, secciones_geojson])
def imagen_ocupacion_isla(ocupacion_csv, secciones_geojson):
    """
    Diagrama de líneas: para cada ocupación (facet), evolución del número
    total de casos (ambos sexos) por isla.
    """
    df = ocupacion_csv.copy()
    df["num_casos"]    = pd.to_numeric(df["num_casos"], errors="coerce").fillna(0)
    df["gcd_municipio"] = df["code_municipio"].astype(str)

    mun_isla = _mun_isla_map(secciones_geojson)
    df = df.merge(mun_isla, on="gcd_municipio", how="left")
    df["isla"] = df["gcd_isla"].map(ISLA_NOMBRES)
    df = df.dropna(subset=["isla"])

    agg = (
        df.groupby(["año", "isla", "ocupacion"])["num_casos"]
        .sum()
        .reset_index()
    )
    agg["año"]        = agg["año"].astype(str)
    agg["ocupacion_w"] = agg["ocupacion"].apply(lambda s: _wrap(s, 28))

    g = (
        ggplot(agg, aes("año", "num_casos", color="isla", group="isla"))
        + geom_line(size=1.2)
        + geom_point(size=3)
        + facet_wrap("~ocupacion_w", ncol=2, scales="free_y")
        + scale_color_manual(values=COLORES_ISLA)
        + labs(
            title="Ocupación por tipo e isla",
            x="Año", y="Total de casos", color="Isla",
        )
        + theme_classic()
        + theme(
            figure_size=(16, 12),
            plot_title=element_text(weight="bold", size=14),
            strip_text=element_text(size=10),
            legend_text=element_text(size=12),
        )
    )
    os.makedirs("./images", exist_ok=True)
    g.save("./images/ocupacion_isla.png", dpi=150)


# Gráfico de barras de actividad económica para un año.
@asset(deps=[actividad_csv, secciones_geojson])
def imagen_actividad_isla_ano(actividad_csv, secciones_geojson):
    df = actividad_csv.copy()
    df["num_casos"] = pd.to_numeric(df["num_casos"], errors="coerce").fillna(0)
    df = df[df["Periodo"] == AÑO].copy()
    df["gcd_municipio"] = df["cod_municipio"].astype(str)
    mun_isla = _mun_isla_map(secciones_geojson)
    df = df.merge(mun_isla, on="gcd_municipio", how="left")
    df["isla"] = df["gcd_isla"].map(ISLA_NOMBRES)
    df = df.dropna(subset=["isla"])

    agg = (
        df.groupby(["isla", "Actividad económica"])["num_casos"]
        .sum()
        .reset_index()
    )

    # Calcular porcentaje dentro de cada isla.
    total_por_isla = agg.groupby("isla")["num_casos"].transform("sum")
    agg["pct"] = agg["num_casos"] / total_por_isla * 100

    agg["actividad_w"] = agg["Actividad económica"].apply(lambda s: _wrap(s, 30))

    g = (
        ggplot(agg, aes("isla", "pct", fill="actividad_w"))
        + geom_bar(stat="identity", position="stack", width=0.6)
        + scale_y_continuous(labels=lambda l: [f"{v:.0f}%" for v in l])
        + scale_fill_brewer(type="qual", palette="Set1")
        + labs(
            title=f"Actividad económica por isla ({AÑO})",
            x="Isla", y="Porcentaje (%)", fill="Actividad económica",
        )
        + theme_classic()
        + theme(
            figure_size=(14, 8),
            plot_title=element_text(weight="bold", size=14),
            axis_text_x=element_text(size=10),
            legend_text=element_text(size=10),
            legend_title=element_text(size=12),
        )
    )

    os.makedirs("./images", exist_ok=True)
    g.save("./images/actividad_isla.png", dpi=150)
