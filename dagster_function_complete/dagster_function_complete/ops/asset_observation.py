import random
from dagster import job, op, AssetObservation, OpExecutionContext

# dagsterではデータ品質を観測していくことが可能である。
# データの品質はassetのmaterializeと別で実行することもおおい
# 例えば、
# - DWHのテーブル数
# - テーブルの使用回数
# などmaterializeと独立して実行したいときに使う

@op
def asset_observation_op(context: OpExecutionContext):
    context.log_event(
        AssetObservation(
            asset_key="asset_observation",
            metadata={
                "file_size": random.randint(1, 100),
                "num_rows": random.randint(1, 100),
            }
        )
    )

@job
def asset_observation_job():
    asset_observation_op()