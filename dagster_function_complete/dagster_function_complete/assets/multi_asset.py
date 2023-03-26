from typing import Tuple
from dagster import asset, multi_asset, AssetOut

# assetは通常1つの関数に対して1つだけ定義ができる
# 一方で一つの処理で複数のデータが出力されるケースもあるだろう
# 例えば
# - zipファイルを解凍することで複数のファイルを得る
# - jsonファイルの中に複数のコンテキストの値がある
# このようなケースではmulti_assetを使うことで1つの関数で複数のassetを定義することができる

# この関数では、asset_1, asset_2という複数のassetが定義されている
@multi_asset(
    outs={
        "asset_1": AssetOut(),
        "asset_2": AssetOut()
    }
)
def multi_asset() -> Tuple[int, int]:
    return 1, 2


# このとき依存関係としては出力設定(outs)の値を利用する
@asset
def asset_1_downstream(asset_1: int) -> int:
    return asset_1 + 1

@asset
def asset_2_downstream(asset_2: int) -> int:
    return asset_2 + 2

@asset
def asset_1_2_downstream(asset_1: int, asset_2: int) -> int:
    return asset_1 + asset_2