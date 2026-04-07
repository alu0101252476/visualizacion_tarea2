import re
import requests
import pandas as pd
import subprocess
import plotnine
from dagster import asset, Output

#############################
# PASO 1: PLANTILLA PARA IA #
#############################

# Construimos el payload para la IA (imagen 1)
@asset
def plantilla_ia_1(distribucion_renta_canarias_csv):
    df = distribucion_renta_canarias_csv.copy()
    # Renombramos para facilitar el trabajo de la IA
    df = df.rename(columns={"TIME_PERIOD_CODE": "año",
                            "MEDIDAS#es":       "tipo_renta",
                            "OBS_VALUE":        "valor",
                            "TERRITORIO#es":    "territorio"})
    df = df[df["territorio"] == "Canarias"].dropna(subset=["valor"])
    df["año"] = df["año"].astype(int)
    columnas = ", ".join(df.columns)
    tipos_renta = df["tipo_renta"].unique().tolist()
    # Preparamos la plantilla para la IA
    template_tecnico = """
def generar_plot(df):
    # El código debe seguir esta estructura exacta:
    # GRÁFICO DE PLOTNINE
    # plot = (ggplot(df, aes(...))
    #   + geom_...
    #   + ...
    #   )
    return plot
"""
    # Le damos el contexto a la IA
    system_content = (
        "Eres un experto en la gramática de gráficos de Wickham y en Plotnine. "
        "Tu tarea es traducir descripciones en lenguaje natural a código Python ejecutable. "
        f"Usa siempre este template exacto (no cambies el nombre de la función): {template_tecnico} "
        "Reglas de la gramática de gráficos que debes cumplir: "
        "1) Toda variable visual se mapea dentro de aes() (x, y, color, fill, group). "
        "2) Aplica scale_color_manual() cuando se pidan colores específicos. "
        "3) Incluye labs() con title, x e y. "
        "4) Añade theme_minimal() para reducir el ruido visual (principio figura-fondo). "
        "Devuelve EXCLUSIVAMENTE el bloque de código Python, "
        "sin explicaciones, sin comentarios adicionales y sin Markdown."
    )
    # Escribimos la descripción del gráfico
    descripcion_grafico = f"""
- Dataset: df (ya filtrado por territorio = Canarias)
- Columnas disponibles: {columnas}
- Tipos de renta únicos: {tipos_renta}
- Estéticas (aes):
  * Variable 'año' -> eje X (continua, entero)
  * Variable 'valor' -> eje Y (porcentaje)
  * Variable 'tipo_renta' -> color y group (una línea por tipo)
- Geometría:
  * geom_line -> evolución temporal
  * geom_point -> énfasis en cada observación
- Escalas: scale_color_brewer(type='qual', palette='Set1')
- Etiquetas (labs):
  * title: 'Distribución de renta por tipo y año en Canarias'
  * x: 'Año'
  * y: 'Porcentaje (%)'
  * color: 'Tipo de renta'
- Principio Gestalt (Continuidad):
  * Las líneas continuas permiten percibir la tendencia temporal de cada tipo de renta.
"""
    # Preparamos el prompt que va a recibir la IA
    user_content = (
        "Basándote en esta descripción de gramática de gráficos, "
        f"completa el template:\n{descripcion_grafico}"
    )
    return {"model": "ollama/llama3.1:8b",
            "messages": [{"role": "system", "content": system_content},
                         {"role": "user",   "content": user_content}],
            "temperature": 0.1,
            "stream": False}

# Construimos el payload para la IA (imagen 2)
@asset
def plantilla_ia_2(distribucion_renta_canarias_csv):
    df = distribucion_renta_canarias_csv.copy()
    # Renombramos para facilitar el trabajo de la IA
    df = df.rename(columns={"TIME_PERIOD_CODE": "año",
                            "MEDIDAS#es":       "tipo_renta",
                            "OBS_VALUE":        "valor",
                            "TERRITORIO#es":    "territorio"})
    df = df[df["territorio"] == "Canarias"].dropna(subset=["valor"])
    df["año"] = df["año"].astype(str)
    columnas = ", ".join(df.columns)
    tipos_renta = df["tipo_renta"].unique().tolist()
    # Preparamos la plantilla para la IA
    template_tecnico = """
def generar_plot(df):
    # El código debe seguir esta estructura exacta:
    # GRÁFICO DE PLOTNINE
    # plot = (ggplot(df, aes(...))
    #   + geom_...
    #   + ...
    #   )
    return plot
"""
    # Le damos el contexto a la IA
    system_content = (
        "Eres un experto en la gramática de gráficos de Wickham y en Plotnine. "
        "Tu tarea es traducir descripciones en lenguaje natural a código Python ejecutable. "
        f"Usa siempre este template exacto (no cambies el nombre de la función): {template_tecnico} "
        "Reglas de la gramática de gráficos que debes cumplir: "
        "1) Toda variable visual se mapea dentro de aes() (x, y, color, fill, group). "
        "2) Aplica scale_color_manual() cuando se pidan colores específicos. "
        "3) Incluye labs() con title, x e y. "
        "4) Añade theme_minimal() para reducir el ruido visual (principio figura-fondo). "
        "Devuelve EXCLUSIVAMENTE el bloque de código Python, "
        "sin explicaciones, sin comentarios adicionales y sin Markdown."
    )
    # Escribimos la descripción del gráfico
    descripcion_grafico = f"""
- Dataset: df (ya filtrado por territorio = Canarias)
- Columnas disponibles: {columnas}
- Tipos de renta únicos: {tipos_renta}
- Estéticas (aes):
  * Variable 'año' -> eje X (categórica, string)
  * Variable 'valor' -> eje Y (porcentaje)
  * Variable 'tipo_renta' -> fill (una barra por tipo dentro de cada año)
- Geometría:
  * geom_col(position='dodge') -> barras agrupadas, una por tipo de renta
- Escalas:
  * scale_fill_brewer(type='qual', palette='Set1')
- Etiquetas (labs):
  * title: 'Distribución de renta por tipo y año en Canarias'
  * x: 'Año'
  * y: 'Porcentaje'
  * fill: 'Tipo de renta'
- Principio Gestalt (Similitud y Proximidad):
  * Las barras del mismo tipo de renta comparten color (similitud),
    y las barras de un mismo año están agrupadas juntas (proximidad).
"""
    # Preparamos el prompt que va a recibir la IA
    user_content = (
        "Basándote en esta descripción de gramática de gráficos, "
        f"completa el template:\n{descripcion_grafico}"
    )
    return {"model": "ollama/llama3.1:8b",
            "messages": [{"role": "system", "content": system_content},
                         {"role": "user",   "content": user_content}],
            "temperature": 0.1,
            "stream": False}

###########################
# PASO 2: LLAMADA A LA IA #
###########################

# Enviamos el payload a la IA (imagen 1)
@asset
def codigo_ia_1(context, plantilla_ia_1):
    url = "http://gpu1.esit.ull.es:4000/v1/chat/completions"
    headers = {"Authorization": "Bearer sk-1234", "Content-Type": "application/json"}
    try:
        response = requests.post(url, json=plantilla_ia_1, headers=headers, timeout=120)
        response.raise_for_status()
        codigo_raw = response.json()["choices"][0]["message"]["content"]
        context.log.info(f"Respuesta cruda de la IA:\n{codigo_raw}")
        # Extraemos el bloque ```python ...```
        match = re.search(r"```python\s+(.*?)\s*```", codigo_raw, re.DOTALL)
        if match:
            codigo_final = match.group(1)
        else:
            # Descartamos las líneas que no sean código Python
            lineas_validas = []
            for linea in codigo_raw.split("\n"):
                stripped = linea.strip()
                # Descartamos cabeceras Markdown, viñetas y líneas vacías iniciales
                if (stripped.startswith("###")
                        or stripped.startswith("##")
                        or stripped.startswith("- ")
                        or stripped.startswith("* ")
                        or stripped == "```"):
                    continue
                lineas_validas.append(linea)
            codigo_final = "\n".join(lineas_validas)
        # Eliminar posibles bloques ```...``` sin lenguaje
        codigo_final = re.sub(r"```", "", codigo_final).strip()
        # Verificamos que contiene la función pedida
        if "def generar_plot" not in codigo_final:
            raise ValueError("La IA no devolvió la función 'generar_plot'. "
                             f"Código recibido:\n{codigo_final}")
        context.log.info(f"Código limpio:\n{codigo_final}")
        # Devolvemos el resultado
        return Output(value=codigo_final,
                      metadata={"codigo_completo": f"```python\n{codigo_final}\n```"})
    except Exception as e:
        context.log.error(f"Error en la petición a la IA: {e}")
        raise

# Enviamos el payload a la IA (imagen 2)
@asset
def codigo_ia_2(context, plantilla_ia_2):
    url = "http://gpu1.esit.ull.es:4000/v1/chat/completions"
    headers = {"Authorization": "Bearer sk-1234", "Content-Type": "application/json"}
    try:
        response = requests.post(url, json=plantilla_ia_2, headers=headers, timeout=120)
        response.raise_for_status()
        codigo_raw = response.json()["choices"][0]["message"]["content"]
        context.log.info(f"Respuesta cruda de la IA:\n{codigo_raw}")
        # Extraemos el bloque ```python ...```
        match = re.search(r"```python\s+(.*?)\s*```", codigo_raw, re.DOTALL)
        if match:
            codigo_final = match.group(1)
        else:
            # Descartamos las líneas que no sean código Python
            lineas_validas = []
            for linea in codigo_raw.split("\n"):
                stripped = linea.strip()
                # Descartamos cabeceras Markdown, viñetas y líneas vacías iniciales
                if (stripped.startswith("###")
                        or stripped.startswith("##")
                        or stripped.startswith("- ")
                        or stripped.startswith("* ")
                        or stripped == "```"):
                    continue
                lineas_validas.append(linea)
            codigo_final = "\n".join(lineas_validas)
        # Eliminar posibles bloques ```...``` sin lenguaje
        codigo_final = re.sub(r"```", "", codigo_final).strip()
        # Verificamos que contiene la función pedida
        if "def generar_plot" not in codigo_final:
            raise ValueError("La IA no devolvió la función 'generar_plot'. "
                             f"Código recibido:\n{codigo_final}")
        context.log.info(f"Código limpio:\n{codigo_final}")
        # Devolvemos el resultado
        return Output(value=codigo_final,
                      metadata={"codigo_completo": f"```python\n{codigo_final}\n```"})
    except Exception as e:
        context.log.error(f"Error en la petición a la IA: {e}")
        raise

################################
# PASO 3: EJECUCIÓN DEL CÓDIGO #
################################

# Generamos el gráfico proporcionado (imagen 1)
@asset
def visualizacion_ia_1(context, codigo_ia_1, distribucion_renta_canarias_csv):
    # Preparamos el DataFrame
    df = distribucion_renta_canarias_csv.copy()
    df = df.rename(columns={"TIME_PERIOD_CODE": "año",
                            "MEDIDAS#es":       "tipo_renta",
                            "OBS_VALUE":        "valor",
                            "TERRITORIO#es":    "territorio"})
    df = df[df["territorio"] == "Canarias"].dropna(subset=["valor"])
    df["año"] = df["año"].astype(int)
    # Preparamos el entorno de ejecución seguro
    entorno_ejecucion = {}
    entorno_ejecucion["plotnine"] = plotnine
    entorno_ejecucion.update({
        k: v for k, v in plotnine.__dict__.items() if not k.startswith("_")
    })
    entorno_ejecucion["pd"] = pd
    try:
        # Ejecutamos el string generado por la IA
        exec(codigo_ia_1, entorno_ejecucion)
        # Invocamos la función que la IA definió
        grafico = entorno_ejecucion["generar_plot"](df)
        # Guardamos el PNG
        ruta_archivo = "./images/ia_img1.png"
        grafico.save(ruta_archivo, width=10, height=6, dpi=150)
        context.log.info(f"Gráfico guardado en {ruta_archivo}.")
        return Output(
            value=ruta_archivo,
            metadata={
                "ruta":    ruta_archivo,
                "mensaje": "Gráfico generado y guardado correctamente."
            }
        )
    except Exception as e:
        context.log.error(f"Error al renderizar el gráfico: {e}")
        raise

# Generamos el gráfico proporcionado (imagen 2)
@asset
def visualizacion_ia_2(context, codigo_ia_2, distribucion_renta_canarias_csv):
    # Preparamos el DataFrame
    df = distribucion_renta_canarias_csv.copy()
    df = df.rename(columns={"TIME_PERIOD_CODE": "año",
                            "MEDIDAS#es":       "tipo_renta",
                            "OBS_VALUE":        "valor",
                            "TERRITORIO#es":    "territorio"})
    df = df[df["territorio"] == "Canarias"].dropna(subset=["valor"])
    df["año"] = df["año"].astype(str)
    # Preparamos el entorno de ejecución seguro
    entorno_ejecucion = {}
    entorno_ejecucion["plotnine"] = plotnine
    entorno_ejecucion.update({
        k: v for k, v in plotnine.__dict__.items() if not k.startswith("_")
    })
    entorno_ejecucion["pd"] = pd
    try:
        # Ejecutamos el string generado por la IA
        exec(codigo_ia_2, entorno_ejecucion)
        # Invocamos la función que la IA definió
        grafico = entorno_ejecucion["generar_plot"](df)
        # Guardamos el PNG
        ruta_archivo = "./images/ia_img2.png"
        grafico.save(ruta_archivo, width=10, height=6, dpi=150)
        context.log.info(f"Gráfico guardado en {ruta_archivo}.")
        return Output(
            value=ruta_archivo,
            metadata={
                "ruta":    ruta_archivo,
                "mensaje": "Gráfico generado y guardado correctamente."
            }
        )
    except Exception as e:
        context.log.error(f"Error al renderizar el gráfico: {e}")
        raise

###########################
# PASO 4: SUBIDA A GITHUB #
###########################

# Subimos la imagen a GitHub Pages
@asset(deps=[visualizacion_ia_1, visualizacion_ia_2])
def git_push_ia(context):
    ruta = "./images"
    try:
        subprocess.run(["git", "add", ruta], check=True)
        # Comprobamos si hay cambios
        result = subprocess.run(["git", "status", "--porcelain"],
                                capture_output=True, text=True)
        if not result.stdout.strip():
            context.log.info("No hay cambios para subir.")
            return Output(value=None,
                         metadata={"mensaje": "Sin cambios, no se hace commit."})
        # Hacemos push solo si hay cambios
        subprocess.run(["git", "commit", "-m", "Actualización del gráfico de la IA"],
                        check=True)
        subprocess.run(["git", "push"], check=True)
        context.log.info("Imagen subida a GitHub Pages correctamente.")
        return Output(
            value=ruta,
            metadata={
                "ruta": ruta,
                "mensaje": "Imagen subida a GitHub Pages."
            }
        )
    except subprocess.CalledProcessError as e:
        context.log.error(f"Error en git: {e}")
        raise
