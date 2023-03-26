from typing import List
from dagster import AssetIn, AssetKey, SourceAsset, asset


@asset(
    key_prefix="same_group"    
)
def upstream_asset() -> List[int]:
    return [1, 3, 5]

@asset(
    key_prefix="same_group"
)
def upstream_asset_2() -> List[int]:
    return [2, 4, 6]

@asset(
    key_prefix="same_group",
    ins={
        "upstream_asset": AssetIn(key=AssetKey(["same_group", "upstream_asset"])),
        "upstream_asset_2": AssetIn(key=AssetKey(["same_group", "upstream_asset_2"]))
    }
)
def test_downstream_asset(upstream_asset: List[int], upstream_asset_2: List[int]) -> List[int]:
    return upstream_asset + upstream_asset_2
