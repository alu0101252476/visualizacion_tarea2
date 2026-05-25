import os
import pandas as pd
from dagster import AssetCheckResult, MetadataValue, asset_check
from .lab_data import rentamedia_csv, ocupacion_csv
from .lab_graph2 import (
    MEDIA_NACIONAL,
    MEDIA_CANARIA,
    ISLA_NOMBRES,
    AÑO,
    imagen_evolucion_municipios_media,
    imagen_evolucion_secciones_media,
    imagen_ocupacion_mapa_calor,
    imagen_ocupacion_provincia_sexo,
    imagen_ocupacion_isla_sexo,
)


# Verifica que 'rentamedia' contiene los tres años esperados (2021–2023).
@asset_check(asset=rentamedia_csv)
def check_graph2_rentamedia_tres_anos(rentamedia_csv):
    años_esperados  = {2021, 2022, 2023}
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
                "Continuidad: Un año ausente en la serie de barras rompe la percepción de evolución temporal."
            ),
            "mensaje": MetadataValue.text(
                "Solución: Verificar que el CSV contiene filas para 2021, 2022 y 2023."
            ),
        },
    )


# Verifica que al menos un municipio supera la media nacional y otro no, en cada año.
@asset_check(asset=rentamedia_csv)
def check_graph2_variabilidad_municipios_nacional(rentamedia_csv):
    df = rentamedia_csv[rentamedia_csv["MEDIDAS_CODE"] == "RENTA_BRUTA_MEDIA_HOGAR"].copy()
    df["gcd_municipio"] = df["TERRITORIO_CODE"].str.split("_").str[1]
    resultados = {}
    passed = True
    for año in (2021, 2022, 2023):
        mun_media = df[df["año"] == año].groupby("gcd_municipio")["OBS_VALUE"].mean()
        umbral    = MEDIA_NACIONAL[año]
        n_sobre   = int((mun_media > umbral).sum())
        n_bajo    = int((mun_media <= umbral).sum())
        resultados[str(año)] = {"sobre_media": n_sobre, "bajo_media": n_bajo}
        if n_sobre == 0 or n_bajo == 0:
            passed = False
    return AssetCheckResult(
        passed=passed,
        metadata={
            "distribucion_por_año": MetadataValue.text(str(resultados)),
            "principio_gestalt": MetadataValue.text(
                "Similitud cromática: Si todos los municipios están en el mismo grupo, las barras no permiten comparación."
            ),
            "mensaje": MetadataValue.text(
                "Solución: Revisar los valores de MEDIA_NACIONAL o el rango de OBS_VALUE del CSV."
            ),
        },
    )


# Verifica que al menos un municipio supera la media canaria y otro no, en cada año.
@asset_check(asset=rentamedia_csv)
def check_graph2_variabilidad_municipios_canaria(rentamedia_csv):
    df = rentamedia_csv[rentamedia_csv["MEDIDAS_CODE"] == "RENTA_BRUTA_MEDIA_HOGAR"].copy()
    df["gcd_municipio"] = df["TERRITORIO_CODE"].str.split("_").str[1]
    resultados = {}
    passed = True
    for año in (2021, 2022, 2023):
        mun_media = df[df["año"] == año].groupby("gcd_municipio")["OBS_VALUE"].mean()
        umbral    = MEDIA_CANARIA[año]
        n_sobre   = int((mun_media > umbral).sum())
        n_bajo    = int((mun_media <= umbral).sum())
        resultados[str(año)] = {"sobre_media": n_sobre, "bajo_media": n_bajo}
        if n_sobre == 0 or n_bajo == 0:
            passed = False
    return AssetCheckResult(
        passed=passed,
        metadata={
            "distribucion_por_año": MetadataValue.text(str(resultados)),
            "principio_gestalt": MetadataValue.text(
                "Similitud cromática: Si todos los municipios caen en el mismo grupo, la barra agrupada pierde sentido comparativo."
            ),
            "mensaje": MetadataValue.text(
                "Solución: Revisar los valores de MEDIA_CANARIA o el rango de OBS_VALUE del CSV."
            ),
        },
    )


# Verifica que ocupacion_csv contiene datos de ambos sexos.
@asset_check(asset=ocupacion_csv)
def check_graph2_ambos_sexos_ocupacion(ocupacion_csv):
    sexos = set(ocupacion_csv["sexo"].dropna().unique())
    n_hombres = int((ocupacion_csv["sexo"] == "Hombres").sum())
    n_mujeres = int((ocupacion_csv["sexo"] == "Mujeres").sum())
    passed = "Hombres" in sexos and "Mujeres" in sexos
    return AssetCheckResult(
        passed=passed,
        metadata={
            "sexos_presentes": MetadataValue.text(str(sorted(sexos))),
            "filas_hombres": MetadataValue.int(n_hombres),
            "filas_mujeres": MetadataValue.int(n_mujeres),
            "principio_gestalt": MetadataValue.text(
                "Similitud: Si falta un sexo, las barras agrupadas aparecen incompletas y la comparación visual se rompe."
            ),
            "mensaje": MetadataValue.text(
                "Solución: Verificar que el CSV incluye registros con sexo 'Hombres' y 'Mujeres'."
            ),
        },
    )


# Verifica que ocupacion_csv contiene más de una ocupación única.
@asset_check(asset=ocupacion_csv)
def check_graph2_num_ocupaciones(ocupacion_csv):
    ocupaciones = sorted(ocupacion_csv["ocupacion"].dropna().unique())
    n = len(ocupaciones)
    passed = n > 1
    return AssetCheckResult(
        passed=passed,
        metadata={
            "num_ocupaciones": MetadataValue.int(n),
            "ocupaciones": MetadataValue.text(str(ocupaciones)),
            "principio_gestalt": MetadataValue.text(
                "Región común: Con una sola ocupación no hay facets diferenciados ni comparación posible entre grupos."
            ),
            "mensaje": MetadataValue.text(
                "Solución: Revisar que el CSV contiene varias categorías en la columna 'ocupacion'."
            ),
        },
    )


# Verifica que ocupacion_csv contiene los códigos de isla esperados.
@asset_check(asset=ocupacion_csv)
def check_graph2_islas_presentes(ocupacion_csv):
    codigos_esperados  = set(ISLA_NOMBRES.keys())
    tiene_municipio = "code_municipio" in ocupacion_csv.columns
    passed = tiene_municipio
    return AssetCheckResult(
        passed=passed,
        metadata={
            "columna_code_municipio_presente": MetadataValue.bool(tiene_municipio),
            "islas_esperadas": MetadataValue.text(str(sorted(codigos_esperados))),
            "principio_gestalt": MetadataValue.text(
                "Región común: Sin la columna de municipio no se puede asignar isla y los facets por isla quedan vacíos."
            ),
            "mensaje": MetadataValue.text(
                "Solución: Verificar que ocupacion_csv incluye la columna 'code_municipio'."
            ),
        },
    )


# Verifica que la imagen existe y tiene un tamaño mínimo de 50 KB.
@asset_check(asset=imagen_evolucion_municipios_media)
def check_integridad_evolucion_municipios_media():
    path, umbral = "./images/evolucion_mun_media.png", 50.0
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


# Verifica que la imagen existe y tiene un tamaño mínimo de 50 KB.
@asset_check(asset=imagen_evolucion_secciones_media)
def check_integridad_evolucion_secciones_media():
    path, umbral = "./images/evolucion_sec_media.png", 50.0
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
@asset_check(asset=imagen_ocupacion_mapa_calor)
def check_integridad_ocupacion_mapa_calor():
    path, umbral = "./images/ocupacion_mapa_calor.png", 300.0
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
                "Solución: Revisar que el canvas PIL se construyó correctamente y que hay datos por ocupación."
            ),
        },
    )


# Verifica que la imagen existe y tiene un tamaño mínimo de 100 KB.
@asset_check(asset=imagen_ocupacion_provincia_sexo)
def check_integridad_ocupacion_provincia_sexo():
    path, umbral = "./images/ocupacion_prov_sexo.png", 100.0
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
@asset_check(asset=imagen_ocupacion_isla_sexo)
def check_integridad_ocupacion_isla_sexo():
    path, umbral = "./images/ocupacion_isla_sexo.png", 200.0
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


# Verifica que el mapa de calor muestra variabilidad real (no todos los casos son iguales).
@asset_check(asset=ocupacion_csv)
def check_graph2_variabilidad_ocupacion(ocupacion_csv):
    df = ocupacion_csv[ocupacion_csv["año"] == AÑO].copy()
    df["num_casos"] = pd.to_numeric(df["num_casos"], errors="coerce").fillna(0)
    agg = df.groupby(["geocode", "ocupacion"])["num_casos"].sum()
    std   = float(agg.std())
    maximo = float(agg.max())
    passed = std > 0
    return AssetCheckResult(
        passed=passed,
        metadata={
            "desviacion_tipica": MetadataValue.float(round(std, 2)),
            "maximo_casos": MetadataValue.float(round(maximo, 2)),
            "año_analizado": MetadataValue.int(AÑO),
            "principio_gestalt": MetadataValue.text(
                "Figura y fondo: Sin variabilidad en 'num_casos', el mapa de calor muestra un único tono y no hay figura distinguible."
            ),
            "mensaje": MetadataValue.text(
                "Solución: Revisar que los datos de ocupación tienen distribución real entre secciones."
            ),
        },
    )
