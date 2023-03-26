from dagster import AssetSelection, Definitions, ScheduleDefinition, define_asset_job, load_assets_from_package_module

from . import assets


daily_refresh_schedule = ScheduleDefinition(
    job=define_asset_job(
        name="all_assets_job",
        selection=AssetSelection.groups("hackernews")
    ), cron_schedule="0 0 * * *"
)

defs = Definitions(
    assets=load_assets_from_package_module(assets), schedules=[daily_refresh_schedule]
)