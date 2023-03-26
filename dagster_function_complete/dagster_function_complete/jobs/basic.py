from dagster import AssetSelection, Field, In, Int, asset, config_mapping, define_asset_job, job, op, load_assets_from_modules
from ..assets import same_group_asset, basic as basic_assets


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
#   - そのため、SourceとAssetDefinitionは別で定義するのが良い(もしくはgroup_nameの設計戦略を気にするなど)
# - AssetSelectionによって指定する
#   - 基本的にこの指定が良い
#   - https://docs.dagster.io/_apidocs/assets#dagster.AssetSelection
basic_asset_job = define_asset_job(
    name="run_basic_assets",
    selection=AssetSelection.groups("basic")
)


# opで設定値を使う場合は、まずどの値を要求するのかを定義する
# その後、contextを通じて価を取得する
@op(
    config_schema={
        "index": int
    }
)
def configurable_op(context):
    return context.op_config["index"]

# opに設定値を求めらている場合
# いくつかの方法で値を渡すことができる
# 以下は固定値で渡す方法
# jobデコレータのconfigに設定値を渡すことで、このjobが実行する
# opに値を渡すことができる
@job(
    config={
        # opに対する設定(asset含む)
        "ops": {
            # op/assetの名前
            "configurable_op": {
                "config": {
                    # config_schemaで定義した値
                    "index": 1
                }
            }
        }
    }
)
def run_configurable_op_fixed_value():
    configurable_op()


# assetで利用する場合も同様に、
# まず定義をする

# opで設定値を使う場合は、まずどの値を要求するのかを定義する
# その後、contextを通じて価を取得する
@asset(
    config_schema={
        "index": int
    }
)
def configurable_asset(context):
    return context.op_config["index"]

# assetに設定を入れる場合は、
# 同じようにjobの定義で行うことができる
# これらの設定は、ジョブの実行時やassetのmaterialize時に書き換えることもできる
run_configurable_asset_fixed_value = define_asset_job(
    name="run_configurable_asset_fixed_value",
    selection=[configurable_asset],
    config={
        "ops": {
            "configurable_asset": {
                "config": {
                    "index": 1
                }
            }
        }
    }
)

# 通常設定は,各op/assetごとに必要になる。
# が、多くのケースで価を使い回すことがある。
# そこで利用できるのが、config mappingである。
# 以下のassetはすべて同一の設定を利用するとする
@asset(
    config_schema={
        "index": int
    }
)
def config_mapping_asset_1(context):
    context.log.info(f"config_mapping_asset_1: {context.op_config['index']}")
    return context.op_config["index"]

@asset(
    config_schema={
        "index": int
    }
)
def config_mapping_asset_2(context):
    context.log.info(f"config_mapping_asset_2: {context.op_config['index']}")
    return context.op_config["index"]

@asset(
    config_schema={
        "index": int,
    }
)
def config_mapping_asset_3(context):
    context.log.info(f"config_mapping_asset_3: {context.op_config['index']}")
    return context.op_config["index"]

# config_mappingデコレータで、設定を引き受け
# 各op/assetの設定価をここで展開する
# なお、config_schemaにはdefault価も設定が可能で、Fieldを利用することで設定できる。
@config_mapping(
    config_schema={"index": Field(Int, default_value=19910911)}
)
def mapping_index_config(variable):
    return {
        "ops": {
            "config_mapping_asset_1": {
                "config": {
                    "index": variable["index"]
                }
            },
            "config_mapping_asset_2": {
                "config": {
                    "index": variable["index"]
                }
            },
            "config_mapping_asset_3": {
                "config": {
                    "index": variable["index"]
                }
            }
        }
    }

run_configurable_asset_config_mapping = define_asset_job(
    name="run_configurable_asset_config_mapping",
    selection=[
        config_mapping_asset_1, config_mapping_asset_2, config_mapping_asset_3
    ],
    config=mapping_index_config
)