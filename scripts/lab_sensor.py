import os
from dagster import sensor, RunRequest, AssetSelection, define_asset_job

##############################
# PASO 1: CREAMOS EL TRABAJO #
##############################
# Ejecuta todos los assets del proyecto
job_rentas = define_asset_job(
    name="job_rentas",
    selection=AssetSelection.all()
)

#############################
# PASO 1: CREAMOS EL SENSOR #
#############################

# Vigila cambios en la carpeta "data"
@sensor(job=job_rentas, minimum_interval_seconds=30)
def sensor_datos(context):
    carpeta = "./data"
    if not os.path.exists(carpeta):
        context.log.warning(f"Carpeta {carpeta} no encontrada.")
        return
    # Calculamos el mtime máximo de todos los ficheros de "data"
    mtimes = []
    for nombre in os.listdir(carpeta):
        ruta = os.path.join(carpeta, nombre)
        if os.path.isfile(ruta):
            mtimes.append(os.path.getmtime(ruta))
    if not mtimes:
        return
    curr_mtime = str(max(mtimes))
    last_mtime = context.cursor or "0"
    # Lanzamos si detecta un cambio
    if curr_mtime != last_mtime:
        context.log.info(f"Cambio detectado en {carpeta}. Lanzando pipeline...")
        yield RunRequest(run_key=curr_mtime)
        context.update_cursor(curr_mtime)
