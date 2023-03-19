from dagster import Definitions, ScheduleDefinition, load_assets_from_packege_modules, define_asset_job

from . import assets


daily_refresh_schedule = ScheduleDefinition(
    job=define_asset_job(
        name="all_assets_job"
    ), cron_schedule="0 0 * * *"
)

defs = Definitions(
    assets=load_assets_from_packege_modules(assets), schedules=[daily_refresh_schedule]
)