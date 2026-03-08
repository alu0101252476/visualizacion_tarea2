# Proyecto "practica-calidad-checks"
**Tarea 3 de la asignatura de Visualización**

En este proyecto, extendemos el pipeline de Dagster de la tarea anterior incorporando asset checks para validar la calidad de los datos y la integridad de las visualizaciones en cada etapa del proceso.

En la etapa de carga, se definen dos checks. El primero verifica que los nombres de las islas están estandarizados, detectando variantes de mayúsculas o espacios extra que provocarían duplicados en las leyendas de los gráficos. El segundo comprueba que la columna de distribución de renta no supera un umbral del 5% de nulos, ya que una presencia elevada de huecos rompe la continuidad visual de las series temporales.

En la etapa de transformación, se añaden otros dos checks. El primero limita el número de categorías únicas de nivel de estudios a un máximo de 9, umbral que supera la capacidad de la memoria visual del usuario. El segundo valida que todos los valores se encuentran dentro del rango válido de 0 a 100, dado que representan porcentajes y cualquier valor fuera de ese rango distorsiona la escala del eje Y.

En la etapa de visualización, se incorporan dos checks finales. El primero comprueba que el eje Y del gráfico de barras agrupadas parte desde cero, verificando que no existen valores negativos en los datos que desplacen el origen. El segundo valida que el gráfico existe en disco y supera un tamaño mínimo de 80 KB, descartando exportaciones vacías o fallidas.

Además, todos los checks incluyen metadata con el principio de la Gestalt asociado y una inyección de fallo comentada que permite comprobar el error sin modificar los assets originales.