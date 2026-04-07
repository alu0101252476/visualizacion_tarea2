# Proyecto "practica-automatizacion"
**Tarea 4 de la asignatura de Visualización**

En este proyecto, extendemos el pipeline de Dagster de la tarea anterior incorporando gráficos generados mediante Inteligencia Artificial (LLM), automatización mediante sensores y generación de un mapa en Plotline.

Primero, se cargan y transforman los datos de distribución de renta por tipo y año, estandarizando las columnas para facilitar su uso por la IA. A continuación, se generan plantillas que permiten al modelo de LLM producir código Python siguiendo la gramática de gráficos de Wickham y aplicando principios Gestalt, obteniendo así gráficos de líneas para mostrar la evolución temporal de la renta y gráficos de barras agrupadas por tipo y año.

Luego, se realizan checks para comprobar que el código generado por la IA y los gráficos asociados están bien estructurados. Paralelamente, se procesa un GeoJSON de municipios para crear un mapa de la tasa de empleo con Plotnine, de manera similar al mapa generado en la tarea anterior.

Todo el pipeline está automatizado mediante un sensor que detecta cambios en la carpeta de datos y ejecuta el job completo. Los gráficos generados se suben automáticamente a GitHub Pages, realizando commit únicamente cuando hay cambios, asegurando que las visualizaciones estén siempre actualizadas.