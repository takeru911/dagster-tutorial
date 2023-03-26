import random
import pandas as pd
from dagster import OpExecutionContext, job, op, AssetMaterialization

# assetデコレータを関数に被せることでassetの定義ができる
# 一方で実行時には決定できないようなassetも存在する
# 例えば、
#  - APIの結果を受けてデータの取得をおこなうもの
#  - テーブルリストを取得し、それぞれのテーブルをassetにする
# こういった用途のために、動的にAssetの定義を行うことができる
# 一方であまりこの使い方がしっかりまだ腹落ちできていない

@op
def asset_materialization_op(context: OpExecutionContext):
    for i in range(0, 10):
        records = random.randint(1, 100)
        df = pd.DataFrame(
            [ random.randint(1, 100) for j in range(0, records)],
            columns=["value"]
        )
        # contextを通じてassetができた、というイベントを発生させる
        context.log_event(
            AssetMaterialization(
                asset_key=f"asset_materialization_{i}",
                description="generated random"
            )
        )

@job
def asset_materialization_job():
    asset_materialization_op()