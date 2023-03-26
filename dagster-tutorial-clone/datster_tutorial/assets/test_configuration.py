import resource
from dagster import OpExecutionContext, asset, job, materialize, op




@asset(config_schema={"person_name": str}, group_name="test_config", required_resource_keys={"test_resource"})
def asset_using_config(context: OpExecutionContext):
    # Note how asset config is also accessed with context.op_config
    return f'hello {context.op_config["person_name"]}, {context.resources.shared_name}'

@asset(group_name="test_config", required_resource_keys={"test_resource"})
def asset_using_config_shared(context: OpExecutionContext, asset_using_config):
    # Note how asset config is also accessed with context.op_config
    context.resou
    return f'hello {asset_using_config} + {context.resources.test_resource.shared_value} - {context.resources.test_resource.shared_name} env: {context.resources.test_resource.env_value}'


@asset
def test(asset_using_config_shared):
    return ""
