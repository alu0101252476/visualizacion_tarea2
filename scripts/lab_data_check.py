import re
from dagster import AssetCheckResult, MetadataValue, asset_check
from .lab_data import (
    actividad_csv,
    distribucion_renta_ingresos_csv,
    ocupacion_csv,
    rentamedia_csv,
    secciones_geojson,
)


# Verifica que la conversión de coma decimal a float no generó nulos inesperados.
@asset_check(asset=distribucion_renta_ingresos_csv)
def check_decimal_renta(distribucion_renta_ingresos_csv):
    nulos = distribucion_renta_ingresos_csv["OBS_VALUE"].isna().sum()
    total = len(distribucion_renta_ingresos_csv)
    porcentaje = float(nulos / total * 100)
    passed = bool(porcentaje < 5.0)
    return AssetCheckResult(
        passed=passed,
        metadata={
            "filas_no_parseables": MetadataValue.int(int(nulos)),
            "total_filas": MetadataValue.int(total),
            "porcentaje_nulos": MetadataValue.float(round(porcentaje, 2)),
            "umbral_maximo": MetadataValue.float(5.0),
            "principio_gestalt": MetadataValue.text(
                "Figura y fondo: Los huecos en una serie interrumpen el patrón y dificultan comparar categorías."
            ),
            "mensaje": MetadataValue.text(
                "Solución: Verificar que se usa siempre coma decimal y la columna no contiene caracteres no numéricos."
            ),
        },
    )


# Verifica que los valores de 'OBS_VALUE' estén dentro del rango [0, 100].
@asset_check(asset=distribucion_renta_ingresos_csv)
def check_porcentaje_renta(distribucion_renta_ingresos_csv):
    df = distribucion_renta_ingresos_csv.dropna(subset=["OBS_VALUE"])
    fuera = df[(df["OBS_VALUE"] < 0) | (df["OBS_VALUE"] > 100)]
    passed = len(fuera) == 0
    return AssetCheckResult(
        passed=passed,
        metadata={
            "valores_fuera_rango": MetadataValue.int(len(fuera)),
            "valor_min": MetadataValue.float(float(df["OBS_VALUE"].min())),
            "valor_max": MetadataValue.float(float(df["OBS_VALUE"].max())),
            "principio_gestalt": MetadataValue.text(
                "Proporcionalidad: Un valor fuera del rango comprime o exagera el resto de segmentos."
            ),
            "mensaje": MetadataValue.text(
                "Solución: Revisar el origen del CSV para corregir valores fuera de [0, 100]."
            ),
        },
    )


# Verifica que la columna 'sexo' solo contiene categorías esperadas.
@asset_check(asset=ocupacion_csv)
def check_sexo_ocupacion(ocupacion_csv):
    categorias_validas = {"Hombres", "Mujeres", "Total"}
    categorias_encontradas = set(ocupacion_csv["sexo"].dropna().unique())
    invalidas = categorias_encontradas - categorias_validas
    passed = len(invalidas) == 0
    return AssetCheckResult(
        passed=passed,
        metadata={
            "categorias_encontradas": MetadataValue.text(str(sorted(categorias_encontradas))),
            "categorias_invalidas": MetadataValue.text(str(sorted(invalidas))),
            "principio_gestalt": MetadataValue.text(
                "Similitud: Una categoría inesperada genera una serie extra que rompe la comparación."
            ),
            "mensaje": MetadataValue.text(
                "Solución: Estandarizar los valores de 'sexo' a 'Hombres', 'Mujeres' o 'Total'."
            ),
        },
    )


# Verifica que el porcentaje de nulos en 'num_casos' no supere el 10%.
@asset_check(asset=actividad_csv)
def check_nulos_actividad(actividad_csv):
    nulos = actividad_csv["num_casos"].isna().sum()
    total = len(actividad_csv)
    porcentaje = float(nulos / total * 100)
    passed = bool(porcentaje < 10.0)
    return AssetCheckResult(
        passed=passed,
        metadata={
            "filas_nulas": MetadataValue.int(int(nulos)),
            "total_filas": MetadataValue.int(total),
            "porcentaje_nulos": MetadataValue.float(round(porcentaje, 2)),
            "umbral_maximo": MetadataValue.float(10.0),
            "principio_gestalt": MetadataValue.text(
                "Figura y fondo: Demasiados huecos en un mapa hacen que el fondo domine sobre el dato real."
            ),
            "mensaje": MetadataValue.text(
                "Solución: Revisar el origen del CSV para corregir valores nulos."
            ),
        },
    )


# Verifica que todos los valores de renta media sean positivos.
@asset_check(asset=rentamedia_csv)
def check_renta_positiva(rentamedia_csv):
    negativos = rentamedia_csv[rentamedia_csv["OBS_VALUE"] < 0]
    passed = len(negativos) == 0
    return AssetCheckResult(
        passed=passed,
        metadata={
            "filas_negativas": MetadataValue.int(len(negativos)),
            "valor_min": MetadataValue.float(float(rentamedia_csv["OBS_VALUE"].min())),
            "valor_max": MetadataValue.float(float(rentamedia_csv["OBS_VALUE"].max())),
            "principio_gestalt": MetadataValue.text(
                "Proporcionalidad: Un valor negativo invierte la escala de color y engaña la percepción."
            ),
            "mensaje": MetadataValue.text(
                "Solución: Revisar el origen del CSV para corregir valores negativos."
            ),
        },
    )


# Verifica que el GeoDataFrame combinado contiene los cuatro años esperados.
@asset_check(asset=secciones_geojson)
def check_anos_geojson(secciones_geojson):
    años_esperados = {2021, 2022, 2023, 2024}
    años_encontrados = set(secciones_geojson["año"].unique())
    años_faltantes = años_esperados - años_encontrados
    passed = len(años_faltantes) == 0
    return AssetCheckResult(
        passed=passed,
        metadata={
            "años_esperados": MetadataValue.text(str(sorted(años_esperados))),
            "años_encontrados": MetadataValue.text(str(sorted(años_encontrados))),
            "años_faltantes": MetadataValue.text(str(sorted(años_faltantes))),
            "principio_gestalt": MetadataValue.text(
                "Continuidad: Una serie con un año ausente rompe el flujo y hace pensar que no hubo cambio."
            ),
            "mensaje": MetadataValue.text(
                "Solución: Verificar que los cuatro archivos GeoJSON de secciones existen."
            ),
        },
    )

# Verifica que 'geocode' sigue el formato YYYYMMDD_XXXXX_DXX_SXXX.
@asset_check(asset=secciones_geojson)
def check_geocode_geojson(secciones_geojson):
    patron = re.compile(r"^\d{8}_\d{5}_D\d{2}_S\d{3}$")
    muestra = secciones_geojson["geocode"].dropna()
    invalidos = muestra[~muestra.str.match(patron)]
    passed = len(invalidos) == 0
    return AssetCheckResult(
        passed=passed,
        metadata={
            "geocodes_invalidos": MetadataValue.int(len(invalidos)),
            "ejemplos_invalidos": MetadataValue.text(
                str(invalidos.head(5).tolist()) if not invalidos.empty else "—"
            ),
            "patron_esperado": MetadataValue.text("YYYYMMDD_XXXXX_DXX_SXXX"),
            "principio_gestalt": MetadataValue.text(
                "Cierre: Un geocode malformado deja secciones sin color en el mapa, como formas sin cerrar."
            ),
            "mensaje": MetadataValue.text(
                "Solución: Revisar los GeoJSONs para corregir geocodes que no sigan el patrón estándar."
            ),
        },
    )
