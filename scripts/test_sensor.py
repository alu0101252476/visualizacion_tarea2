import os
from dagster import sensor, RunRequest, AssetSelection, define_asset_job

# 1. Definimos el JOB (el pipeline completo)
# Esto le dice a Dagster: "Quiero ejecutar desde el origen hasta el final"
pipeline_islas_job = define_asset_job(
    name="pipeline_islas_job",
    selection=AssetSelection.all() # O puedes especificar assets concretos
)

# 2. Definimos el SENSOR
@sensor(job=pipeline_islas_job)
def my_directory_sensor(context):
    ruta_fichero = "./data/pwbi-1.csv"
    
    # Verificación de seguridad: ¿existe el fichero?
    if not os.path.exists(ruta_fichero):
        context.log.warning(f"Fichero {ruta_fichero} no encontrado.")
        return

    # Lógica del cursor (tu código estaba perfecto aquí)
    last_mtime = context.cursor or "0"
    curr_mtime = str(os.path.getmtime(ruta_fichero))
    
    if curr_mtime != last_mtime:
        yield RunRequest(
            run_key=curr_mtime,
            message=f"Detectado cambio en {ruta_fichero}. Lanzando pipeline..."
        )
        context.update_cursor(curr_mtime)
