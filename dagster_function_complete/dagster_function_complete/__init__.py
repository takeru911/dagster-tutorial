from dagster import Definitions, load_assets_from_package_module, load_assets_from_modules, define_asset_job

from .assets import basic, asset_testing, metadata, graph_backed, multi_asset
from .ops import asset_materialization, asset_observation, basic as basic_ops, expectation, failures, hook
from .jobs import basic as basic_jobs, schedule

basic_assets = load_assets_from_modules(
            [basic]
)

testing_asset = load_assets_from_modules(
            [asset_testing], group_name="testing"
)

metadat_asset = load_assets_from_modules(
            [metadata], group_name="metadata"
)

graph_backed_asset = load_assets_from_modules(
            [graph_backed], group_name="graph_backed"
)

multi_asset = load_assets_from_modules(
            [multi_asset], group_name="multi_asset"
)


defs = Definitions(
    assets=[
        *basic_assets, 
        *testing_asset, 
        *metadat_asset, 
        *graph_backed_asset,
        *multi_asset,
        basic_jobs.configurable_asset,
        basic_jobs.config_mapping_asset_1,
        basic_jobs.config_mapping_asset_2,
        basic_jobs.config_mapping_asset_3,
        schedule.schedule_asset
    ] ,
    jobs=[
        asset_materialization.asset_materialization_job,
        asset_observation.asset_observation_job,
        basic_ops.call_basic_op,
        basic_ops.run_dagster_type_op,
        basic_ops.run_multi_output_op,
        basic_ops.run_op_with_config_by_fixed,
        basic_ops.run_op_with_config_by_dynamic,
        expectation.call_expectation_op,
        failures.call_failure_op,
        hook.call_fail_op, hook.call_success_op,
        basic_jobs.call_do_noting_op,
        basic_jobs.basic_asset_job,
        basic_jobs.run_configurable_asset_fixed_value,
        basic_jobs.run_configurable_op_fixed_value,
        basic_jobs.run_configurable_asset_config_mapping,
        schedule.schedule_asset_job
    ],
    schedules=[
        schedule.scheduled_asset_job,
        schedule.configured_scheduled_asset_job
    ]
)

