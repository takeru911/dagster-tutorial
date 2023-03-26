import os
from dagster import AssetSelection, Definitions, JobDefinition, ScheduleDefinition, define_asset_job, load_assets_from_package_module, load_assets_from_modules, resource

from . import assets


loaded_assets = load_assets_from_package_module(assets)


all_asset_job_clone = define_asset_job(
        name="all_assets_job_clone"        ,
        config={
            "ops": {"asset_using_config": {"config": {"person_name": "Alice"}}},
        }
)
daily_refresh_schedule = ScheduleDefinition(
    job=all_asset_job_clone, cron_schedule="0 0 * * *"
)

job =  define_asset_job(
    name="config_testing",selection=AssetSelection.groups("test_config"),
    config={
        "ops": {"asset_using_config": {"config": {"person_name": "Alice"}}}        
    }    
)

class TestResource:
    def __init__(self, shared_name: str, shared_value: str, env_value: str):
        self.shared_name = shared_name
        self.shared_value = shared_value
        self.env_value = env_value


@resource(
        config_schema={"shared_name": str, "shared_value": str, "env_value": str}
)
def test_resource(context):
    return {
        "shared_name": context.resource_config["shared_name"],
        "shared_value": context.resource_config["shared_value"],
        "env_value": context.resource_config["env_value"],
    }

# Definitions
## asse
defs = Definitions(
    assets=loaded_assets,
    jobs=[job, all_asset_job_clone],
    schedules=[daily_refresh_schedule],
    resources={
        "test_resource": test_resource.configured(
            {"shared_name": "MAEHARA", "shared_value": "911", "env_value": {"env": "TEST_ENV"}}
        )
    }
    
)
