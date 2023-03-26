import random
from dagster import ExpectationResult, OpExecutionContext, job, op

# dagsterではデータ品質のチェックをexpectationとして実行することができる

@op
def exepectation_op(context: OpExecutionContext):
    i = random.randint(1, 100)
    context.log_event(
        ExpectationResult(
            success=i > 50, description="value is greater than 50"
        )
    )
    return i

@job
def call_expectation_op():
    exepectation_op()
