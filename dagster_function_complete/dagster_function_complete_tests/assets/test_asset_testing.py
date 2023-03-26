from dagster import build_op_context
from ....dagster_function_complete.dagster_function_complete.assets import asset_testing

# testの実行はpytestを使う

# test for upstream_asset
# 通常のテストのように関数のテストを書く
def test_upstream_asset():
    assert asset_testing.upstream_asset() == [1, 3, 5]

# test for downstream_asset
# upstream_assetの結果をmockとして実行する
def test_downstream_asset():
    assert asset_testing.downstream_asset([1, 2]) == [1, 2, 7]

# test for contextual_asset
# contextをmockとして実行する
# build_op_contextを実行することでmockのcontextを作成する
# 設定はbuild_op_contextの引数で指定する
def test_contextual_asset():
    context = build_op_context(
        op_config={
            "test_value": 9
        }
    )
    assert asset_testing.contextual_asset(context, [1, 2]) == [1, 2, 9]
