from dagster import Definitions, load_assets_from_modules, load_asset_checks_from_modules
from scripts import lab_renta, lab_checks
# from scripts import test_assets
# from scripts import test_checks

defs = Definitions(
    # assets=load_assets_from_modules([test_assets]),
    assets=load_assets_from_modules([lab_renta]),
    # asset_checks=load_asset_checks_from_modules([test_checks])
    asset_checks=load_asset_checks_from_modules([lab_checks])
)
