import os
import pandas as pd
from dagster import AssetCheckResult, AssetIn, MetadataValue, asset_check
from .lab_data import distribucion_renta_ingresos_csv, ocupacion_csv, actividad_csv, rentamedia_csv
from .lab_graph3 import (
    ISLA_NOMBRES,
    AÑO,
    _top_bottom_municipios,
    imagen_distribucion_renta_top_bottom,
    imagen_distribucion_renta_isla,
    imagen_ocupacion_top_bottom,
    imagen_ocupacion_isla,
    imagen_actividad_isla_ano,
)


# Verifica que hay al menos 10 municipios distintos con datos de renta para construir top/bottom.
@asset_check(asset=rentamedia_csv)
def check_graph3_municipios_suficientes(rentamedia_csv):
    df = rentamedia_csv[rentamedia_csv["MEDIDAS_CODE"] == "RENTA_BRUTA_MEDIA_HOGAR"].copy()
    df["gcd_municipio"] = df["TERRITORIO_CODE"].str.split("_").str[1]
    resultados = {}
    passed = True
    for año in (2021, 2022, 2023):
        n = int(df[df["año"] == año]["gcd_municipio"].nunique())
        resultados[str(año)] = n
        if n < 10:
            passed = False
    return AssetCheckResult(
        passed=passed,
        metadata={
            "municipios_por_año": MetadataValue.text(str(resultados)),
            "minimo_requerido": MetadataValue.int(10),
            "principio_gestalt": MetadataValue.text(
                "Figura y fondo: Con menos de 10 municipios no se pueden construir grupos top/bottom significativos."
            ),
            "mensaje": MetadataValue.text(
                "Solución: Verificar que rentamedia_csv contiene al menos 10 municipios distintos por año."
            ),
        },
    )


# Verifica que el top-10 y el bottom-10 no se solapan en ningún año.
@asset_check(asset=rentamedia_csv)
def check_graph3_top_bottom_sin_solapamiento(rentamedia_csv):
    tb = _top_bottom_municipios(rentamedia_csv, n=10)
    solapamientos = {}
    passed = True
    for año, datos in tb.items():
        solapamiento = datos["top"] & datos["bottom"]
        solapamientos[str(año)] = len(solapamiento)
        if solapamiento:
            passed = False
    return AssetCheckResult(
        passed=passed,
        metadata={
            "solapamiento_por_año": MetadataValue.text(str(solapamientos)),
            "principio_gestalt": MetadataValue.text(
                "Similitud: Si un municipio aparece en los dos grupos, las líneas de color rojo y azul se cruzan en el mismo punto y la comparación pierde sentido."
            ),
            "mensaje": MetadataValue.text(
                "Solución: Revisar la función '_top_bottom_municipios', con más de 20 municipios no debería haber solapamiento."
            ),
        },
    )


# Verifica que distribucion_renta_ingresos_csv contiene los tres años esperados.
@asset_check(asset=distribucion_renta_ingresos_csv)
def check_graph3_distribucion_tres_anos(distribucion_renta_ingresos_csv):
    años_esperados   = {2021, 2022, 2023}
    años_encontrados = set(distribucion_renta_ingresos_csv["año"].unique())
    faltantes = años_esperados - años_encontrados
    passed = len(faltantes) == 0
    return AssetCheckResult(
        passed=passed,
        metadata={
            "años_esperados":   MetadataValue.text(str(sorted(años_esperados))),
            "años_encontrados": MetadataValue.text(str(sorted(años_encontrados))),
            "años_faltantes":   MetadataValue.text(str(sorted(faltantes))),
            "principio_gestalt": MetadataValue.text(
                "Continuidad: Un año ausente en el diagrama de líneas rompe la trayectoria visual y da la impresión de datos faltantes."
            ),
            "mensaje": MetadataValue.text(
                "Solución: Verificar que el CSV contiene filas para 2021, 2022 y 2023."
            ),
        },
    )


# Verifica que distribucion_renta_ingresos_csv contiene más de una medida.
@asset_check(asset=distribucion_renta_ingresos_csv)
def check_graph3_num_medidas_distribucion(distribucion_renta_ingresos_csv):
    medidas = sorted(distribucion_renta_ingresos_csv["MEDIDAS_CODE"].dropna().unique())
    n = len(medidas)
    passed = n > 1
    return AssetCheckResult(
        passed=passed,
        metadata={
            "num_medidas": MetadataValue.int(n),
            "medidas":     MetadataValue.text(str(medidas)),
            "principio_gestalt": MetadataValue.text(
                "Región común: Con una sola medida no hay facets diferenciados y el gráfico no aporta información comparativa."
            ),
            "mensaje": MetadataValue.text(
                "Solución: Revisar que el CSV incluye varias categorías en la columna 'MEDIDAS_CODE'."
            ),
        },
    )


# Verifica que ocupacion_csv tiene datos de ambos sexos para el análisis top/bottom.
@asset_check(asset=ocupacion_csv)
def check_graph3_ambos_sexos_ocupacion(ocupacion_csv):
    sexos = set(ocupacion_csv["sexo"].dropna().unique())
    n_hombres = int((ocupacion_csv["sexo"] == "Hombres").sum())
    n_mujeres = int((ocupacion_csv["sexo"] == "Mujeres").sum())
    passed = "Hombres" in sexos and "Mujeres" in sexos
    return AssetCheckResult(
        passed=passed,
        metadata={
            "sexos_presentes": MetadataValue.text(str(sorted(sexos))),
            "filas_hombres":   MetadataValue.int(n_hombres),
            "filas_mujeres":   MetadataValue.int(n_mujeres),
            "principio_gestalt": MetadataValue.text(
                "Similitud: Si falta un sexo, la agregación total por ocupación subestima el número real de casos."
            ),
            "mensaje": MetadataValue.text(
                "Solución: Verificar que el CSV incluye registros con sexo 'Hombres' y 'Mujeres'."
            ),
        },
    )


# Verifica que actividad_csv contiene datos para el año 'AÑO' y tiene casos positivos.
@asset_check(asset=actividad_csv)
def check_graph3_actividad_ano_datos(actividad_csv):
    df = actividad_csv[actividad_csv["Periodo"] == AÑO].copy()
    df["num_casos"] = pd.to_numeric(df["num_casos"], errors="coerce").fillna(0)
    n_filas    = len(df)
    total_casos = float(df["num_casos"].sum())
    actividades = sorted(df["Actividad económica"].dropna().unique())
    passed = n_filas > 0 and total_casos > 0
    return AssetCheckResult(
        passed=passed,
        metadata={
            "año_analizado": MetadataValue.int(AÑO),
            "filas_encontradas": MetadataValue.int(n_filas),
            "total_casos": MetadataValue.float(round(total_casos, 2)),
            "actividades": MetadataValue.text(str(actividades)),
            "principio_gestalt": MetadataValue.text(
                "Figura y fondo: Sin datos para el año seleccionado, el gráfico de barras apiladas aparece vacío y no hay figura."
            ),
            "mensaje": MetadataValue.text(
                f"Solución: Verificar que actividad_csv contiene filas en el año seleccionado."
            ),
        },
    )


# Verifica que todas las islas esperadas están presentes en actividad_csv tras el merge.
@asset_check(asset=actividad_csv,
             additional_ins={"secciones_geojson": AssetIn()})
def check_graph3_islas_actividad(actividad_csv, secciones_geojson):
    df = actividad_csv[actividad_csv["Periodo"] == AÑO].copy()
    df["gcd_municipio"] = df["cod_municipio"].astype(str)
    mun_isla = (
        secciones_geojson[secciones_geojson["año"] == 2021]
        [["gcd_municipio", "gcd_isla"]]
        .drop_duplicates()
    )
    merged = df.merge(mun_isla, on="gcd_municipio", how="left")
    merged["isla"] = merged["gcd_isla"].map(ISLA_NOMBRES)
    islas_encontradas = set(merged["isla"].dropna().unique())
    islas_esperadas   = set(ISLA_NOMBRES.values())
    faltantes = islas_esperadas - islas_encontradas
    passed = len(faltantes) == 0
    return AssetCheckResult(
        passed=passed,
        metadata={
            "islas_esperadas": MetadataValue.text(str(sorted(islas_esperadas))),
            "islas_encontradas": MetadataValue.text(str(sorted(islas_encontradas))),
            "islas_faltantes": MetadataValue.text(str(sorted(faltantes))),
            "principio_gestalt": MetadataValue.text(
                "Región común: Una isla sin datos genera una barra vacía que rompe la comparación entre islas."
            ),
            "mensaje": MetadataValue.text(
                "Solución: Verificar que actividad_csv contiene municipios de todas las islas de S/C de Tenerife."
            ),
        },
    )


# Verifica que la imagen existe y tiene un tamaño mínimo de 200 KB.
@asset_check(asset=imagen_distribucion_renta_top_bottom)
def check_integridad_distribucion_renta_top_bottom():
    path, umbral = "./images/distribucion_renta_tb.png", 200.0
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


# Verifica que la imagen existe y tiene un tamaño mínimo de 200 KB.
@asset_check(asset=imagen_distribucion_renta_isla)
def check_integridad_distribucion_renta_isla():
    path, umbral = "./images/distribucion_renta_isla.png", 200.0
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


# Verifica que la imagen existe y tiene un tamaño mínimo de 200 KB.
@asset_check(asset=imagen_ocupacion_top_bottom)
def check_integridad_ocupacion_top_bottom():
    path, umbral = "./images/ocupacion_tb.png", 200.0
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


# Verifica que la imagen existe y tiene un tamaño mínimo de 200 KB.
@asset_check(asset=imagen_ocupacion_isla)
def check_integridad_ocupacion_isla():
    path, umbral = "./images/ocupacion_isla.png", 200.0
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
                "Solución: Verificar que secciones_geojson contiene los códigos de isla y municipio correctos."
            ),
        },
    )


# Verifica que la imagen existe y tiene un tamaño mínimo de 100 KB.
@asset_check(asset=imagen_actividad_isla_ano)
def check_integridad_actividad_isla_ano():
    path, umbral = "./images/actividad_isla.png", 100.0
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
