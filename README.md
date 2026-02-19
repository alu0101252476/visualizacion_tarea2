# Proyecto "visualizacion_tarea2"
**Tarea 2 de la asignatura de Visualización**

En este proyecto, construimos un pipeline utilizando Dagster para automatizar la visualización de datos sobre la distribución de la renta en Canarias.

En primer lugar, se define un asset que ejecuta un git pull para asegurar que el repositorio local esté sincronizado con la versión de GitHub antes de trabajar con los datos.

A continuación, se cargan tres fuentes de datos desde la carpeta data del repositorio. Un archivo CSV con los códigos y nombres de municipios por isla, otro CSV con la distribución de la renta en Canarias y un archivo GeoJSON con la geometría de los municipios para trazar un mapa.

Posteriormente, se generan cuatro visualizaciones que se guardan en la carpeta images del repositorio:
- La primera muestra la evolución temporal de los distintos tipos de renta en Canarias mediante un gráfico de líneas.
- La segunda representa la distribución por año y tipo de renta en Canarias usando un gráfico de barras agrupadas.
- La tercera combina la información estadística con la cartografía municipal para construir mapas por tipo de renta en el año 2023, procesando las geometrías y uniendo los datos por código territorial.
- La cuarta visualización se centra en Gran Canaria en 2023, filtrando sus municipios y mostrando la composición de la renta mediante un gráfico de barras apiladas.

Finalmente, se define un asset que añade los cambios al repositorio, realiza un commit y ejecuta un git push, guardando así todas las visualizaciones en el repositorio de GitHub.