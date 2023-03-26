from dagster import AssetSelection, In, define_asset_job, job, op, load_assets_from_modules
from dagster_function_complete.assets import same_group_asset, basic as basic_assets


# dagsterでは実行する対象をジョブとして定義する。
# ジョブは、opに基づいて定義するものや、assetに基づいて定義するものがある。
# それぞれで導入方法が異なる

@op
def input_value():
    return 1

@op
def do_noting_op(num :int):
    num + 1
    return "do nothing"


# opに基づいてジョブを定義する場合は、@jobをつける。
# このとき、opデコレータが付与された、関数を実行することでジョブを形成することができる。
# opの引数に固定値の入力はできないので注意が必要
@job
def call_do_noting_op():
    # これはエラー
    # do_noting_op(1)
    # 動的に値を変えたい場合は、configなどを利用する。
    do_noting_op(input_value())

assets = load_assets_from_modules(
    [same_group_asset]
)


# assetに基づいてジョブを定義する場合は、define_asset_jobを利用する。
# 実行対象とするassetの選択は様々な方法がある。
# - assetを直接指定する
#   - SourceAsset/AssetDefinitionを混ぜられないことに注意
# - AssetSelectionによって指定する
#   - 基本的にこの指定が良い
#   - https://docs.dagster.io/_apidocs/assets#dagster.AssetSelection
basic_asset_job = define_asset_job(
    name="run_basic_assets",
    selection=AssetSelection.groups("default")
)
