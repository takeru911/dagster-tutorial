from typing import List
from dagster import DagsterType, In, OpDefinition, OpExecutionContext, Out, job, op

# opはdagsterにおけるTaskのようなもの
# 生成物がない処理を行うための機能
# 例えば、
# - slackへの通知
# - メタデータの収集など
# 基本的な使い方はassetと同じで、関数定義をおこなってよしなにすればよい
# assetはopのラッパーであり、様々な機能を追加している(逆にassetで使えたが、opではないものが多くあることに注意)


# opもassetと同様に入出力を持てる。
# op(assetも)の入出力は型をつけることで、適切な入出力が行われるかチェックされる
# IOManagerを使うことで、入出力をファイルやDBなどに保存することもできる(デフォルトはローカルの一時ファイルに出力される)
# 例えば、DBに向けることで、テーブルに保存することなども可能である
@op
def basic_op() -> int:
    return 10

# opを実行する際には、jobの定義が必要となる。
# ここがassetとは異なる。
# 詳細はjobsのsampleを参照
@job
def call_basic_op():
    basic_op()

@op
def minus_value() -> int:
    return -10


# 型チェックで値をベースにしたチェックを行いたいことがある
# 例えば、引数は必ず整数になる、などである
# そういった場合は、DagsterTypeを使うことで可能となる

positive_type = DagsterType(type_check_fn=lambda context, value: value >=0, name="PositiveInteger")
@op(
        ins={
            "num": In(dagster_type=positive_type)
        }
)
def must_input_positive_integer(num):
    return num + 10

@job
def run_dagster_type_op():    
    # opに値を渡す場合はopである必要が有ることに注意
    must_input_positive_integer(basic_op())
    # 負の値を渡すとエラーになる
    must_input_positive_integer(minus_value())

# opの出力は通常単一である。(返り値がそのまま出力になる)
# 一方で、単一のopで出力を明示的に複数に分けたいケースもある
# こういったときは、opデコレータに明示すれば良い

@op(
        out={
            "first_output": Out(),
            "second_output": Out()
        }
)
def multi_output_op():
    return 1, 2

@op
def multi_output_op_downstream_first(first_output: int) -> int:
    return first_output * 10

@op
def multi_output_op_downstream_second(second_output: int) -> int:
    return second_output * 20

@job
def run_multi_output_op():
    s = multi_output_op()
    # 複数の出力があるものは、それぞれのoutputを指定して利用できる
    # opの結果は入力できることに注意
    multi_output_op_downstream_first(s.first_output)
    multi_output_op_downstream_second(s.second_output)


# opに対して、何かしらの設定などを行いたい場合は、contextを通じて渡すことができる。
# これにより、動的な実行が可能となる。
# 受け入れ可能な設定は、opデコレータのconfig_schemaで定義する
# 利用のモチベーションとしては、処理の抽象化を行い、設定により詳細を注入するなど。
# 動的な生成というよりはライブラリを通じた処理の再生性というのが適切と思う。

@op(
    config_schema={"name": str}
)
def op_with_config(context: OpExecutionContext) -> str:
    name = context.op_config["name"]
    context.log.info(name)
    return f"{name}だよ～"


@job
def run_op_with_config_by_fixed():    
    # jobの呼び出し時に固定値をいれることもできる。
    # configuredにより、config設定を注入しつつ、別のopとして扱うことができる
    # testという名前opの生成
    op_with_config.configured({"name": "ほげ"}, name="test")()
    # test_2という名前のopの生成
    op_with_config.configured({"name": "maehara"}, name="test_2")()

@job
def run_op_with_config_by_dynamic():
    # 何も指定しない場合だと、実行時にconfigを指定することができる
    # この場合、GUIやCLI時の実行で入力が必要となる
    op_with_config()
