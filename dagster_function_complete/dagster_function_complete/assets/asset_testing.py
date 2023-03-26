from typing import List
from dagster import AssetIn, AssetKey, OpExecutionContext, asset


# assetのテストは通常の関数のようにテストを書くことができる
# see: dagster_function_complete_tests/assets/test_asset_testing.py
@asset(
    key_prefix="test"
)
def upstream_asset():
    return [1, 3, 5]

# 一方でupstreamがある場合のテストについては、
# この関数の呼び出す際に引数をただ指定すれば良い
@asset(
        key_prefix="test",
        ins={
            "upstream": AssetIn(key=AssetKey(["test", "upstream_asset"]))
        }
)
def downstream_asset(upstream: List[int]) -> List[int]:
    return upstream + [7]

# contextを含むテストをどう書けばよいだろうか?
# dagsterはcontextのmockを用意している
@asset(
        key_prefix="test",
        ins={
            "upstream": AssetIn(key=AssetKey(["test", "downstream_asset"]))
        },
        config_schema={"test_value": int}
)
def contextual_asset(context: OpExecutionContext, upstream: List[int]) -> List[int]:
    context.log.info("Hello from contextual_asset")
    test_value = context.op_config["test_value"]
    return upstream + [test_value]