from dagster import Definitions, load_assets_from_package_module, load_assets_from_modules, define_asset_job

from dagster_function_complete.assets import basic, asset_testing, metadata, graph_backed, multi_asset
from dagster_function_complete.ops import asset_materialization, asset_observation, basic as basic_ops, expectation, failures, hook
from dagster_function_complete.jobs import basic as basic_jobs

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
        *multi_asset
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
        basic_jobs.basic_asset_job
    ]
)

