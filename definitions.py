from dagster import Definitions, load_assets_from_modules, load_asset_checks_from_modules
from scripts import (
    lab_data,
    lab_data_check,
    lab_graph1,
    lab_graph1_check,
    lab_graph2,
    lab_graph2_check,
    lab_graph3,
    lab_graph3_check,
    lab_finish
)
from scripts.lab_sensor import sensor_data, job_everything


defs = Definitions(
    assets=load_assets_from_modules(
        [
            lab_data,
            lab_graph1,
            lab_graph2,
            lab_graph3,
            lab_finish
        ]
    ),
    asset_checks=load_asset_checks_from_modules(
        [
            lab_data_check,
            lab_graph1_check,
            lab_graph2_check,
            lab_graph3_check
        ]
    ),
    jobs=[job_everything],
    sensors=[sensor_data],
)
