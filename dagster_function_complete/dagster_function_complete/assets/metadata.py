from dagster import asset, MetadataValue, OpExecutionContext
import random

# assetにはその生成処理の中でメタデータを定義することができる
# metadataは主に2種類あり、
# - 定義時に決定するもの
#   - 流入元情報などの決定的なもの
#   - assetデコレータに記述すると良い
# - 実行中に決定するもの
#   - データのレコード数やサイズなどの情報
#   - contextを通じて書き込む
# またmetadataは様々なフォーマットがあるため適宜選択する
#   - markdown
#     - コンソールでレンダリングされる
#   - 数値データ
#     - コンソールでその推移がプロットされる

@asset(
        metadata={
            "from": MetadataValue.md("自作"),
            "更新頻度": MetadataValue.md("**毎日**")
        }
)
def metadata_asset(context: OpExecutionContext) -> int:

    context.add_output_metadata(
        {
            "レコード数": random.randint(1, 100),
            "サイズ": random.random() * 100
        },        
    )
    return 1
