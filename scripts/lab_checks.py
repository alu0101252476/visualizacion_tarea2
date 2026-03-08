import os
# import pandas as pd
# import matplotlib.pyplot as plt
from dagster import asset_check, AssetCheckResult, MetadataValue, AssetIn
from .lab_renta import codislas_csv, distribucion_renta_canarias_csv, nivelestudios_xlsx, imagen_2, imagen_3

##################################
# CHECKS - ETAPA 1 — CARGA (RAW) #
##################################

# Verifica que los nombres de las islas están estandarizados.
@asset_check(asset=codislas_csv)
def check_estandarizacion(codislas_csv):
    # Fallo en el check:
    # fila_sucia = pd.DataFrame({"CPRO":[38],"CMUN":[999],"NOMBRE":["Prueba"],"ISLA":["tenerife"]})
    # codislas_csv = pd.concat([codislas_csv, fila_sucia], ignore_index=True)
    originales = codislas_csv["ISLA"].nunique()
    normalizadas = codislas_csv["ISLA"].str.strip().str.title().nunique()
    passed = originales == normalizadas
    return AssetCheckResult(
        passed=passed,
        metadata={
            "categorias_originales":  MetadataValue.int(originales),
            "categorias_normalizadas": MetadataValue.int(normalizadas),
            "islas_detectadas": MetadataValue.text(str(sorted(codislas_csv["ISLA"].dropna().unique()))),
            "principio_gestalt": MetadataValue.text(
                "Similitud: Una misma isla con dos nombres distintos genera duplicados en la leyenda."
            ),
            "mensaje": MetadataValue.text(
                "Solución: Estandarizar con .str.strip().str.title() antes de hacer el gráfico."
            ),
        },
    )

# Verifica la presencia de valores nulos en los datos (permite hasta un 5%).
@asset_check(asset=distribucion_renta_canarias_csv)
def check_nulos(distribucion_renta_canarias_csv):
    # Fallo en el check:
    # distribucion_renta_canarias_csv["OBS_VALUE"] = None
    nulos = distribucion_renta_canarias_csv["OBS_VALUE"].isna().mean()
    filas = int(distribucion_renta_canarias_csv["OBS_VALUE"].isna().sum())
    passed = bool(nulos < 0.05)
    return AssetCheckResult(
        passed=passed,
        metadata={
            "porcentaje_nulos": MetadataValue.float(round(float(nulos * 100), 2)),
            "filas_afectadas":  MetadataValue.int(filas),
            "umbral_maximo": MetadataValue.float(5.0),
            "principio_gestalt": MetadataValue.text(
                "Figura y fondo: Los huecos inesperados en una serie temporal rompen la forma percibida."
            ),
            "mensaje": MetadataValue.text(
                "Solución: Revisar el origen del CSV para solucionar los valores nulos."
            ),
        },
    )

###############################################
# CHECKS - ETAPA 2 — TRANSFORMACIÓN (CURATED) #
###############################################

# Verifica que el número de categorías únicas no supere las 9.
@asset_check(asset=nivelestudios_xlsx)
def check_num_categorias(nivelestudios_xlsx):
    # Fallo en el check:
    # extras = pd.DataFrame({"Nivel de estudios en curso": [f"Nivel_{i}" for i in range(10)]})
    # nivelestudios_xlsx = pd.concat([nivelestudios_xlsx, extras], ignore_index=True)
    col = "Nivel de estudios en curso"
    excluir = ["Total", "No cursa estudios"]
    df = nivelestudios_xlsx[~nivelestudios_xlsx[col].isin(excluir)]
    n_categorias = df[col].nunique()
    passed = bool(n_categorias <= 9)
    return AssetCheckResult(
        passed=passed,
        metadata={
            "num_categorias": MetadataValue.int(n_categorias),
            "limite_visual": MetadataValue.int(9),
            "categorias": MetadataValue.text(str(sorted(df[col].dropna().unique()))),
            "principio_gestalt": MetadataValue.text(
                "Carga Cognitiva y similitud: Más de 9 categorías superan la memoria visual de la persona."
            ),
            "mensaje": MetadataValue.text(
                "Solución: Agrupar niveles minoritarios bajo una sola categoría."
            ),
        },
    )

# Verifica que los porcentajes están entre 0 y 100.
@asset_check(asset=distribucion_renta_canarias_csv)
def check_rango_porcentaje(distribucion_renta_canarias_csv):
    # Fallo en el check:
    # fila_sucia = pd.DataFrame({"OBS_VALUE": [-5.0]})
    # distribucion_renta_canarias_csv = pd.concat([distribucion_renta_canarias_csv, fila_sucia], ignore_index=True)
    fuera = distribucion_renta_canarias_csv[
        (distribucion_renta_canarias_csv["OBS_VALUE"] < 0) | (distribucion_renta_canarias_csv["OBS_VALUE"] > 100)
    ]
    passed = len(fuera) == 0
    return AssetCheckResult(
        passed=passed,
        metadata={
            "valores_fuera_rango": MetadataValue.int(len(fuera)),
            "valor_min": MetadataValue.float(float(distribucion_renta_canarias_csv["OBS_VALUE"].min())),
            "valor_max": MetadataValue.float(float(distribucion_renta_canarias_csv["OBS_VALUE"].max())),
            "principio_gestalt": MetadataValue.text(
                "Proporcionalidad: Un valor fuera de [0, 100] comprime o exagera el resto de barras."
            ),
            "mensaje": MetadataValue.text(
                "Solución: Revisar el origen del CSV para solucionar valores fuera de [0, 100]."
            ),
        },
    )

############################################
# CHECKS - ETAPA 3 — VISUALIZACIÓN (ASSET) #
############################################

# Verifica que los ejes de la imagen parten desde cero.
@asset_check(asset=imagen_2, additional_ins={"distribucion_renta_canarias_csv": AssetIn()})
def check_desde_cero(distribucion_renta_canarias_csv):
    # Fallo en el check:
    # fila_sucia = pd.DataFrame({"OBS_VALUE": [-3.5], "MEDIDAS#es": ["Pensiones"], "TERRITORIO#es": ["Canarias"], "TIME_PERIOD_CODE": [2023]})
    # distribucion_renta_canarias_csv = pd.concat([distribucion_renta_canarias_csv, fila_sucia], ignore_index=True)
    df = distribucion_renta_canarias_csv.copy()
    df = df[df["TERRITORIO#es"] == "Canarias"].dropna(subset=["OBS_VALUE"])
    valor_minimo = float(df["OBS_VALUE"].min())
    # Los gráficos de barra en plotnine comienzan desde 0 si no se especifica lo contrario o hay valores nulos.
    hay_negativos = valor_minimo < 0
    passed = not hay_negativos
    return AssetCheckResult(
        passed=passed,
        metadata={
            "valor_min": MetadataValue.float(round(valor_minimo, 4)),
            "hay_negativos": MetadataValue.text("Sí" if hay_negativos else "No"),
            "eje_desde_0": MetadataValue.text("Sí" if passed else "No"),
            "principio_gestalt": MetadataValue.text(
                "Veracidad visual: El eje Y alterado exagera las diferencias pequeñas y engaña la percepción."
            ),
            "mensaje": MetadataValue.text(
                "Solución: Revisar los datos porque hay algún valor negativo que altera el eje."
            ),
        },
    )

# Verifica que la imagen existe y tiene un tamaño mínimo de 80KB.
@asset_check(asset=imagen_3)
def check_integridad_imagen3():
    # Fallo en el check:
    # fig, ax = plt.subplots()
    # ax.axis("off")
    # fig.savefig("./images/distrenta_img3.png", dpi=72, bbox_inches="tight")
    # plt.close(fig)
    path = "./images/distrenta_img3.png"
    umbral = 80.0
    exists = os.path.exists(path)
    tamaño_kb = os.path.getsize(path) / 1024 if exists else 0.0
    passed = bool(exists and tamaño_kb >= umbral)
    return AssetCheckResult(
        passed=passed,
        metadata={
            "ruta": MetadataValue.path(path),
            "tamaño_kb": MetadataValue.float(round(tamaño_kb, 2)),
            "umbral": MetadataValue.float(umbral),
            "principio_gestalt": MetadataValue.text(
                "Cierre: Un gráfico en blanco impide al usuario recibir la información."
            ),
            "mensaje": MetadataValue.text(
                "Solución: Revisar el código porque un archivo muy pequeño indica un error."
            ),
        },
    )
