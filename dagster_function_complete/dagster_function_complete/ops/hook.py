from dagster import (
    Failure,
    HookContext,
    Output,
    ResourceDefinition,
    failure_hook,
    file_relative_path,
    graph,
    job,
    op,
    repository,
    success_hook,    
)

# dagsterでは、処理の終了後に特定の条件で処理を実行することができる。
# これがhookだ。
# 現在デフォルトでは、成功時のhookと失敗時のhookがある。
# それぞれ、success_hookとfailure_hookで定義する。
# このとき、contextを通じて、処理の情報を取得することができる。
# 一方で現状はasset-jobに対してのhookは実装されていない。(alert自体は別の機能を用いて可能である)

@success_hook
def when_success(context: HookContext):
    context.log.info("処理が成功しました。")
    context.log.info(
        f"""{context.job_name}
- run_id: {context.run_id}
- op_name: {context.op.name}
"""    
    )


@failure_hook
def when_fail(context: HookContext):
    context.log.info("処理が失敗しました。")
    context.log.info(
        f"""{context.job_name}
- run_id: {context.run_id}
- op_name: {context.op.name}
"""    
    )

@op
def success_op():
    return Output(
        value="成功",
        metadata={
            "exit_code": 200
        }
    )

@op
def fail_op():
    raise Failure(
        description="失敗",
        metadata={
            "exit_code": 400
        }
    )

# hookはジョブに指定する。
# 各jobのhookには、成功時のhookと失敗時のhookを指定する。
@job(
    hooks={when_fail, when_success}
)
def call_success_op():
    success_op()

@job(
    hooks={when_fail, when_success}
)
def call_fail_op():
    fail_op()
