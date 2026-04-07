import os
from dagster import asset_check, AssetCheckResult, MetadataValue
from .lab_prompt import codigo_ia_1, codigo_ia_2, visualizacion_ia_1, visualizacion_ia_2

# Comprobamos que el código sigue una estructura correcta (imagen 1)
@asset_check(asset=codigo_ia_1)
def check_estructura_cod_1(codigo_ia_1):
    contiene_funcion = "def generar_plot" in codigo_ia_1
    contiene_return = "return" in codigo_ia_1
    passed = bool(contiene_funcion and contiene_return)
    return AssetCheckResult(
        passed=passed,
        metadata={
            "contiene_funcion": MetadataValue.text("Sí" if contiene_funcion else "No"),
            "contiene_return": MetadataValue.text("Sí" if contiene_return else "No"),
            "longitud_codigo": MetadataValue.int(len(codigo_ia_1)),
            "mensaje": MetadataValue.text(
                "Consejo: Ajustar el prompt para forzar la estructura completa de la función."
            ),
        },
    )

# Comprobamos que el código sigue una estructura correcta (imagen 2)
@asset_check(asset=codigo_ia_2)
def check_estructura_cod_2(codigo_ia_2):
    contiene_funcion = "def generar_plot" in codigo_ia_2
    contiene_return = "return" in codigo_ia_2
    passed = bool(contiene_funcion and contiene_return)
    return AssetCheckResult(
        passed=passed,
        metadata={
            "contiene_funcion": MetadataValue.text("Sí" if contiene_funcion else "No"),
            "contiene_return": MetadataValue.text("Sí" if contiene_return else "No"),
            "longitud_codigo": MetadataValue.int(len(codigo_ia_2)),
            "mensaje": MetadataValue.text(
                "Consejo: Ajustar el prompt para forzar la estructura completa de la función."
            ),
        },
    )

# Comprobamos que el código es seguro (imagen 1)
@asset_check(asset=codigo_ia_1)
def check_codigo_seguro_1(codigo_ia_1):
    patrones_peligrosos = ["__", "exec", "eval", "open(", "os.", "sys."]
    encontrados = [p for p in patrones_peligrosos if p in codigo_ia_1]
    passed = len(encontrados) == 0
    return AssetCheckResult(
        passed=passed,
        metadata={
            "patrones_detectados": MetadataValue.text(str(encontrados)),
            "num_patrones": MetadataValue.int(len(encontrados)),
            "mensaje": MetadataValue.text(
                "Consejo: Restringir el prompt o validar el código antes de ejecutarlo."
            ),
        },
    )

# Comprobamos que el código es seguro (imagen 2)
@asset_check(asset=codigo_ia_2)
def check_codigo_seguro_2(codigo_ia_2):
    patrones_peligrosos = ["__", "exec", "eval", "open(", "os.", "sys."]
    encontrados = [p for p in patrones_peligrosos if p in codigo_ia_2]
    passed = len(encontrados) == 0
    return AssetCheckResult(
        passed=passed,
        metadata={
            "patrones_detectados": MetadataValue.text(str(encontrados)),
            "num_patrones": MetadataValue.int(len(encontrados)),
            "mensaje": MetadataValue.text(
                "Consejo: Restringir el prompt o validar el código antes de ejecutarlo."
            ),
        },
    )

# Comprobamos que usa aes (imagen 1)
@asset_check(asset=codigo_ia_1)
def check_uso_aes_1(codigo_ia_1):
    usa_aes = "aes(" in codigo_ia_1
    usa_color = "color=" in codigo_ia_1 or "fill=" in codigo_ia_1
    passed = bool(usa_aes and usa_color)
    return AssetCheckResult(
        passed=passed,
        metadata={
            "usa_aes": MetadataValue.text("Sí" if usa_aes else "No"),
            "usa_color_o_fill": MetadataValue.text("Sí" if usa_color else "No"),
            "mensaje": MetadataValue.text(
                "Consejo: Reforzar en el prompt el uso obligatorio de aes()."
            ),
        },
    )

# Comprobamos que usa aes (imagen 2)
@asset_check(asset=codigo_ia_2)
def check_uso_aes_2(codigo_ia_2):
    usa_aes = "aes(" in codigo_ia_2
    usa_color = "color=" in codigo_ia_2 or "fill=" in codigo_ia_2
    passed = bool(usa_aes and usa_color)
    return AssetCheckResult(
        passed=passed,
        metadata={
            "usa_aes": MetadataValue.text("Sí" if usa_aes else "No"),
            "usa_color_o_fill": MetadataValue.text("Sí" if usa_color else "No"),
            "mensaje": MetadataValue.text(
                "Consejo: Reforzar en el prompt el uso obligatorio de aes()."
            ),
        },
    )

# Comprobamos que es un gráfico de líneas (imagen 1)
@asset_check(asset=codigo_ia_1)
def check_grafico_lineas(codigo_ia_1):
    usa_barras = "geom_col" in codigo_ia_1
    usa_lineas = "geom_line" in codigo_ia_1
    passed = bool(usa_lineas and not usa_barras)
    return AssetCheckResult(
        passed=passed,
        metadata={
            "usa_geom_col": MetadataValue.text("Sí" if usa_barras else "No"),
            "usa_geom_line": MetadataValue.text("Sí" if usa_lineas else "No"),
            "mensaje": MetadataValue.text(
                "Consejo: Asegurar que el tipo de gráfico coincide con la descripción."
            ),
        },
    )

# Comprobamos que es un gráfico de barras (imagen 2)
@asset_check(asset=codigo_ia_2)
def check_grafico_barras(codigo_ia_2):
    usa_barras = "geom_col" in codigo_ia_2
    usa_lineas = "geom_line" in codigo_ia_2
    passed = bool(usa_barras and not usa_lineas)
    return AssetCheckResult(
        passed=passed,
        metadata={
            "usa_geom_col": MetadataValue.text("Sí" if usa_barras else "No"),
            "usa_geom_line": MetadataValue.text("Sí" if usa_lineas else "No"),
            "mensaje": MetadataValue.text(
                "Consejo: Asegurar que el tipo de gráfico coincide con la descripción."
            ),
        },
    )

# Comprobamos que el gráfico no está vacío (imagen 1)
@asset_check(asset=visualizacion_ia_1)
def check_integridad_1():
    path = "./images/ia_img1.png"
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
            "mensaje": MetadataValue.text(
                "Consejo: Revisar el código que genera el gráfico."
            ),
        },
    )

# Comprobamos que el gráfico no está vacío (imagen 2)
@asset_check(asset=visualizacion_ia_2)
def check_integridad_2():
    path = "./images/ia_img2.png"
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
            "mensaje": MetadataValue.text(
                "Consejo: Revisar el código que genera el gráfico."
            ),
        },
    )
