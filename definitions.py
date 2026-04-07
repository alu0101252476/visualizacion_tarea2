from dagster import Definitions, load_assets_from_modules, load_asset_checks_from_modules
from scripts import lab_renta, lab_checks, lab_prompt, lab_checks2, lab_map
from scripts.lab_sensor import sensor_datos, job_rentas

defs = Definitions(
    assets=load_assets_from_modules([lab_renta, lab_prompt, lab_map]),
    asset_checks=load_asset_checks_from_modules([lab_checks, lab_checks2]),
    jobs=[job_rentas],
    sensors=[sensor_datos],
)
