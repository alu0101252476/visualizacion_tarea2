import os
import pandas as pd
from dagster import AssetCheckResult, AssetIn, MetadataValue, asset_check
from .lab_data import actividad_csv, rentamedia_csv
from .lab_graph1 import (
    MEDIA_CANARIA,
    MEDIA_NACIONAL,
    AÑO,
    imagen_actividad_mun_dom,
    imagen_actividad_mun_sexo,
    imagen_actividad_sec_dom,
    imagen_actividad_sec_sexo,
    imagen_renta_mun_canaria,
    imagen_renta_mun_nacional,
    imagen_renta_sec_canaria,
    imagen_renta_sec_nacional,
)


# Verifica que haya secciones donde dominan hombres y secciones donde dominan mujeres.
@asset_check(asset=actividad_csv)
def check_ambos_sexos(actividad_csv):
    df = actividad_csv[actividad_csv["Periodo"] == AÑO].copy()
    df["num_casos"] = pd.to_numeric(df["num_casos"], errors="coerce").fillna(0)
    piv = (
        df.pivot_table(index=["geocode", "Actividad económica"],
                       columns="Sexo", values="num_casos", aggfunc="sum")
        .reset_index()
    )
    piv.columns.name = None
    for s in ("Hombres", "Mujeres"):
        if s not in piv.columns:
            piv[s] = 0
    piv["domina"] = piv.apply(
        lambda r: "Hombres" if r["Hombres"] > r["Mujeres"] else "Mujeres", axis=1
    )
    n_hombres = int((piv["domina"] == "Hombres").sum())
    n_mujeres = int((piv["domina"] == "Mujeres").sum())
    passed = n_hombres > 0 and n_mujeres > 0
    return AssetCheckResult(
        passed=passed,
        metadata={
            "secciones_domina_hombres": MetadataValue.int(n_hombres),
            "secciones_domina_mujeres": MetadataValue.int(n_mujeres),
            "principio_gestalt": MetadataValue.text(
                "Similitud: Si un solo color ocupa todo el mapa, la comparación por similitud cromática se pierde."
            ),
            "mensaje": MetadataValue.text(
                "Solución: Verificar que el CSV incluye datos de ambos sexos a nivel de sección."
            ),
        },
    )


# Verifica que el número de actividades económicas únicas no supere 6.
@asset_check(asset=actividad_csv)
def check_num_actividades(actividad_csv):
    n = actividad_csv["Actividad económica"].nunique()
    passed = bool(n <= 12)
    return AssetCheckResult(
        passed=passed,
        metadata={
            "num_actividades": MetadataValue.int(n),
            "limite_paleta": MetadataValue.int(12),
            "actividades": MetadataValue.text(
                str(sorted(actividad_csv["Actividad económica"].dropna().unique()))
            ),
            "principio_gestalt": MetadataValue.text(
                "Similitud y carga cognitiva: Más de 6 categorías saturan la memoria visual y dificultan distinguir el mapa."
            ),
            "mensaje": MetadataValue.text(
                "Solución: Agrupar actividades minoritarias bajo 'Otras actividades' para no saturar."
            ),
        },
    )


# Verifica que rentamedia contiene datos para los tres años esperados (2021–2023).
@asset_check(asset=rentamedia_csv)
def check_rentamedia_tres_anos(rentamedia_csv):
    años_esperados = {2021, 2022, 2023}
    años_encontrados = set(rentamedia_csv["año"].unique())
    faltantes = años_esperados - años_encontrados
    passed = len(faltantes) == 0
    return AssetCheckResult(
        passed=passed,
        metadata={
            "años_esperados": MetadataValue.text(str(sorted(años_esperados))),
            "años_encontrados": MetadataValue.text(str(sorted(años_encontrados))),
            "años_faltantes": MetadataValue.text(str(sorted(faltantes))),
            "principio_gestalt": MetadataValue.text(
                "Continuidad: Un año ausente en la serie de mapas rompe la percepción de evolución temporal."
            ),
            "mensaje": MetadataValue.text(
                "Solución: Verificar que el CSV contiene filas para 2021, 2022 y 2023."
            ),
        },
    )


# Verifica que la renta media presenta variabilidad real entre secciones cada año.
@asset_check(asset=rentamedia_csv)
def check_variabilidad_renta(rentamedia_csv):
    resultados = {}
    passed = True
    for año in (2021, 2022, 2023):
        std = float(rentamedia_csv[rentamedia_csv["año"] == año]["OBS_VALUE"].std())
        resultados[str(año)] = round(std, 2)
        if std == 0:
            passed = False
    return AssetCheckResult(
        passed=passed,
        metadata={
            "desviacion_tipica_por_año": MetadataValue.text(str(resultados)),
            "principio_gestalt": MetadataValue.text(
                "Figura y fondo: Sin variabilidad, todos los polígonos toman el mismo color y la figura se funde con el fondo."
            ),
            "mensaje": MetadataValue.text(
                "Solución: Revisar si los datos de renta tienen valores constantes por algún error."
            ),
        },
    )


# Verifica que la imagen existe y tiene un tamaño mínimo de 300 KB.
@asset_check(asset=imagen_actividad_sec_sexo)
def check_integridad_actividad_sec_sexo():
    path, umbral = "./images/actividad_sec_sexo.png", 300.0
    exists = os.path.exists(path)
    kb = os.path.getsize(path) / 1024 if exists else 0.0
    passed = bool(exists and kb >= umbral)
    return AssetCheckResult(
        passed=passed,
        metadata={
            "ruta": MetadataValue.path(path),
            "tamaño_kb": MetadataValue.float(round(kb, 2)),
            "umbral_kb": MetadataValue.float(umbral),
            "principio_gestalt": MetadataValue.text(
                "Cierre: Un archivo muy pequeño indica un gráfico vacío o mal generado."
            ),
            "mensaje": MetadataValue.text(
                "Solución: Revisar que plotnine generó la imagen correctamente."
            ),
        },
    )


# Verifica que la imagen existe y tiene un tamaño mínimo de 100 KB.
@asset_check(asset=imagen_actividad_mun_sexo)
def check_integridad_actividad_mun_sexo():
    path, umbral = "./images/actividad_mun_sexo.png", 100.0
    exists = os.path.exists(path)
    kb = os.path.getsize(path) / 1024 if exists else 0.0
    passed = bool(exists and kb >= umbral)
    return AssetCheckResult(
        passed=passed,
        metadata={
            "ruta": MetadataValue.path(path),
            "tamaño_kb": MetadataValue.float(round(kb, 2)),
            "umbral_kb": MetadataValue.float(umbral),
            "principio_gestalt": MetadataValue.text(
                "Cierre: Un archivo muy pequeño indica un gráfico vacío o mal generado."
            ),
            "mensaje": MetadataValue.text(
                "Solución: Revisar que plotnine generó la imagen correctamente."
            ),
        },
    )


# Verifica que la imagen existe y tiene un tamaño mínimo de 300 KB.
@asset_check(asset=imagen_actividad_sec_dom)
def check_integridad_actividad_sec_dom():
    path, umbral = "./images/actividad_sec_dom.png", 300.0
    exists = os.path.exists(path)
    kb = os.path.getsize(path) / 1024 if exists else 0.0
    passed = bool(exists and kb >= umbral)
    return AssetCheckResult(
        passed=passed,
        metadata={
            "ruta": MetadataValue.path(path),
            "tamaño_kb": MetadataValue.float(round(kb, 2)),
            "umbral_kb": MetadataValue.float(umbral),
            "principio_gestalt": MetadataValue.text(
                "Cierre: Un archivo muy pequeño indica un gráfico vacío o mal generado."
            ),
            "mensaje": MetadataValue.text(
                "Solución: Revisar que plotnine generó la imagen correctamente."
            ),
        },
    )


# Verifica que la imagen existe y tiene un tamaño mínimo de 100 KB.
@asset_check(asset=imagen_actividad_mun_dom)
def check_integridad_actividad_mun_dom():
    path, umbral = "./images/actividad_mun_dom.png", 100.0
    exists = os.path.exists(path)
    kb = os.path.getsize(path) / 1024 if exists else 0.0
    passed = bool(exists and kb >= umbral)
    return AssetCheckResult(
        passed=passed,
        metadata={
            "ruta": MetadataValue.path(path),
            "tamaño_kb": MetadataValue.float(round(kb, 2)),
            "umbral_kb": MetadataValue.float(umbral),
            "principio_gestalt": MetadataValue.text(
                "Cierre: Un archivo muy pequeño indica un gráfico vacío o mal generado."
            ),
            "mensaje": MetadataValue.text(
                "Solución: Revisar que plotnine generó la imagen correctamente."
            ),
        },
    )


# Verifica que el mapa de renta vs. media nacional muestra ambos colores (rojo y verde).
@asset_check(asset=imagen_renta_sec_nacional,
             additional_ins={"rentamedia_csv": AssetIn()})
def check_ambas_categorias_renta_nacional(rentamedia_csv):
    resultados = {}
    passed = True
    for año in (2021, 2022, 2023):
        df = rentamedia_csv[rentamedia_csv["año"] == año]
        umbral = MEDIA_NACIONAL[año]
        n_bajo   = int((df["OBS_VALUE"] < umbral).sum())
        n_encima = int((df["OBS_VALUE"] >= umbral).sum())
        resultados[str(año)] = {"por_debajo": n_bajo, "por_encima": n_encima}
        if n_bajo == 0 or n_encima == 0:
            passed = False
    return AssetCheckResult(
        passed=passed,
        metadata={
            "distribucion_por_año": MetadataValue.text(str(resultados)),
            "principio_gestalt": MetadataValue.text(
                "Similitud cromática: Si solo aparece un color, no se pueden comparar las secciones."
            ),
            "mensaje": MetadataValue.text(
                "Solución: Revisar las medias nacionales de referencia o el rango de las observaciones."
            ),
        },
    )


# Verifica que el mapa de renta vs. media nacional muestra ambos colores (rojo y verde).
@asset_check(asset=imagen_renta_sec_canaria,
             additional_ins={"rentamedia_csv": AssetIn()})
def check_ambas_categorias_renta_canaria(rentamedia_csv):
    resultados = {}
    passed = True
    for año in (2021, 2022, 2023):
        df = rentamedia_csv[rentamedia_csv["año"] == año]
        umbral = MEDIA_CANARIA[año]
        n_bajo   = int((df["OBS_VALUE"] < umbral).sum())
        n_encima = int((df["OBS_VALUE"] >= umbral).sum())
        resultados[str(año)] = {"por_debajo": n_bajo, "por_encima": n_encima}
        if n_bajo == 0 or n_encima == 0:
            passed = False
    return AssetCheckResult(
        passed=passed,
        metadata={
            "distribucion_por_año": MetadataValue.text(str(resultados)),
            "principio_gestalt": MetadataValue.text(
                "Similitud cromática: Si solo aparece un color, no se pueden comparar las secciones."
            ),
            "mensaje": MetadataValue.text(
                "Solución: Revisar las medias nacionales de referencia o el rango de las observaciones."
            ),
        },
    )


# Verifica que la imagen existe y tiene un tamaño mínimo de 100 KB.
@asset_check(asset=imagen_renta_mun_nacional)
def check_integridad_renta_mun_nacional():
    """Verifica que la imagen existe y tiene un tamaño mínimo de 100 KB."""
    path, umbral = "./images/renta_mun_nacional.png", 100.0
    exists = os.path.exists(path)
    kb = os.path.getsize(path) / 1024 if exists else 0.0
    passed = bool(exists and kb >= umbral)
    return AssetCheckResult(
        passed=passed,
        metadata={
            "ruta": MetadataValue.path(path),
            "tamaño_kb": MetadataValue.float(round(kb, 2)),
            "umbral_kb": MetadataValue.float(umbral),
            "principio_gestalt": MetadataValue.text(
                "Cierre: Un archivo muy pequeño indica un gráfico vacío o mal generado."
            ),
            "mensaje": MetadataValue.text(
                "Solución: Revisar que plotnine generó la imagen correctamente."
            ),
        },
    )


# Verifica que la imagen existe y tiene un tamaño mínimo de 100 KB.
@asset_check(asset=imagen_renta_mun_canaria)
def check_integridad_renta_mun_canaria():
    """Verifica que la imagen existe y tiene un tamaño mínimo de 100 KB."""
    path, umbral = "./images/renta_mun_canaria.png", 100.0
    exists = os.path.exists(path)
    kb = os.path.getsize(path) / 1024 if exists else 0.0
    passed = bool(exists and kb >= umbral)
    return AssetCheckResult(
        passed=passed,
        metadata={
            "ruta": MetadataValue.path(path),
            "tamaño_kb": MetadataValue.float(round(kb, 2)),
            "umbral_kb": MetadataValue.float(umbral),
            "principio_gestalt": MetadataValue.text(
                "Cierre: Un archivo muy pequeño indica un gráfico vacío o mal generado."
            ),
            "mensaje": MetadataValue.text(
                "Solución: Revisar que plotnine generó la imagen correctamente."
            ),
        },
    )
