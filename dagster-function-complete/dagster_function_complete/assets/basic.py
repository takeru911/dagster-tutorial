from typing import List
from dagster import AssetIn, AssetKey, OpExecutionContext, SourceAsset, asset, load_assets_from_modules

@asset
def upstream_asset():
    return [1, 3, 5]

@asset
def downstream_asset(upstream_asset: List[int]) -> List[int]:
    return upstream_asset + [7]


# デコレータのinsにあたいをせっていすることで、upstreamのassetの定義が可能となる
# この場合、upstream_assetがupstreamという名前で定義される
# AssetInにより明示的に決定される
@asset(
        ins={"upstream": AssetIn("upstream_asset")}
)
def downstream_asset_with_asset_in(upstream: List[int]) -> List[int]:
    return upstream + [9]

# key_prefixを指定することで、asset名のフォルダ設定ができる
# このときこのassetの参照は、"upstream_asset_with_key_prefix"となるが、full nameはtest/upstream_asset_with_key_prefixとなる
# dagsterの中ではプロジェクトの中でassetの名前が一意である必要がある
# そのためこのkey prefixをつかうといいよ
# この場合downstreamはAssetKeyで指定したり、「upstream_asset_with_key_prefix」で指定できる
@asset(
    key_prefix="test"
)
def upstream_asset_with_key_prefix():
    return [1, 3, 5, 9]

@asset
def downstream_asset_with_key_prefix(upstream_asset_with_key_prefix: List[int]) -> List[int]:
    return upstream_asset_with_key_prefix + [11]

# またAssetInにはkey名での指定も可能
@asset(
        ins={"upstream": AssetIn(key=["test", "upstream_asset_with_key_prefix"])}
)
def downstream_asset_with_asset_in_by_key(upstream: List[int]) -> List[int]:
    return upstream + [11]

# 外部のassetを参照する場合は、SourceAssetを利用することができる
# SourceAssetを使うことで、このcode location外で生成しているassetに対して依存関係を作れる
# 例えば以下のassetはrepo_root/dagster-tutorial-clone/dagster_tutorial/assets/test_dataframe.py
# にある「depended_initial」である
external_depend_initial = SourceAsset(key=AssetKey("depended_initial"))
# なおこのときにAssetKeyに入力した（もとのasset）値がupsreamの名前となることに注意
@asset
def downstream_external(depended_initial):
    return [1]


# データの共有は必要はないが、依存関係を持たせたい場合がある。
# 例えばDWHでのクエリ実行などだ
# (ctasで作ると、dagster内にデータは戻す必要はないよね)
# こういったときにはnon_argument_depsを使ってもいい

@asset
def not_exist_return_upstream_asset() -> None:
    pass

@asset(
    non_argument_deps={"not_exist_return_upstream_asset"}
)
def not_exist_argument_downstream_asst() -> None:
    pass


# 処理の中でジョブ全体のメタデータを参照したいことがある
# 他にもログを送りたいときに使うのがcontextである。
# contextには様々なAPIが生えている
# https://docs.dagster.io/_apidocs/execution#dagster.OpExecutionContext
# また共有設定やasset特有の設定もこのcontextを参照する
# contextはassetの関数の第一引数に指定することで利用できる
@asset
def context_asset(context: OpExecutionContext) -> None:
    context.log.info("logが送れるよ")
    context.log.info(f"run_idがとれるよ: {context.run_id}")


# データはドメインやコンテキストによってグルーピングができることがある
# こういったときにassetがgroupかできると可視化や実行単位の面でメリットが有る
# 例えば以下のようにassetを定義すると、assetのグループが作成される
@asset(
    group_name="sample_group"
)
def grouped_upstream_asset() -> List[int]:
    return [1, 3, 5]
# group_nameが同じである場合、同じグループに属するとみなされる
@asset(
    group_name="sample_group"    
)
def grouped_downstream_asset(grouped_upstream_asset: List[int]) -> List[int]:
    return grouped_upstream_asset + [7]

# 一方で逐一、assetにgroup_nameを指定するのは面倒なので、
# まとめて付与することができる
# load_assets_from_package_moduleによりpython moduleにあるassetをまとめて読み込むことができる
# このときにgroup_nameを指定することで、読み込まれたassetにgroup_nameが付与される
# この設定をする場合に読み込むassetにgroup名が設定されている場合エラーになるため注意
# dagsterではこの方法でgroupを設定することが推奨されている
# なおgroup_nameが指定されていない場合はdefaultという名前のgroupに属する
from dagster_function_complete.assets import same_group_asset
set_group = load_assets_from_modules(
    [same_group_asset], 
    group_name="same_group_asset"
)