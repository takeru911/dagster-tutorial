import random
from typing import List
from dagster import OpExecutionContext, asset, graph_asset, op

# 単一のassetではデータの生成が難しいことがある
# 例えば以下のようなケースを考える
# 1. APIに問い合わせを行いリストを取得
# 2. リストからさらにAPIを問い合わせる
# 3. 2の結果を元にデータを生成する
# この場合、最後の3がassetの結果として出力したいとする
# 1,2を関数ワケすれば良いようにも思うが、この場合処理が3の途中で失敗した場合
# 再実行時に、1,2の結果を再度取得したくない。
# こういった場合にgraph_assetを使うと良い
# 使用感は通常の関数のように呼び出すだけで良い

@op
def fetch_data_list_from_api(context: OpExecutionContext) -> List[int]:
    context.log.info("execute fetch_data_list_from_api")
    return [1, 2, 3]

@op
def fetch_body_from_api(context: OpExecutionContext,data: List[int]) -> List[str]:
    context.log.info("execute fetch_body_from_api")
    return [
        f"body: {d}"
        for d in data
    ]

@graph_asset
def asset_graph():
    return fetch_body_from_api(fetch_data_list_from_api())

# graph_assetを使う場合も通常の依存関係を作れる
# このときlineageはgraph_asset, graph_downstream_assetのみとなり、可視性の面で優れる
@asset
def graph_downstream_asset(asset_graph: List[str]):
    return [
        s + "!!"
        for s in asset_graph
    ]
