from dagster import Failure, MetadataValue, job, op

# 明示的にエラーを発生させる方法として、Failureをraiseする方法がある
# 通常の例外は、プログラム上で不正な状態であるが、一方でデータや結果に基づいたエラーを送出したい場合もある。
# APIの戻りなどの状況が例。
# このときにエラーの状況をメタデータとして送ることができる。
# 例えば、エラーの詳細や、復旧方法などを送ることができる。
# 意図的なエラーはこのFailureを送るようにするとよい。

@op
def failure_op():
    raise Failure(
        description="サンプルのエラーです。",
        metadata={
            "error_code": "E001",
            "復旧方法": MetadataValue.md("""
# 復旧方法

- このエラーは必ず起きます。
- そのため、復旧方法はありません。
            """)
        }
    )

@job
def call_failure_op():
    failure_op()