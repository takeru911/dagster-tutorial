import random
import pandas as pd
from dagster import AssetIn, MetadataValue, OpExecutionContext, Output, asset

@asset(
    group_name="conditional_testing",
    compute_kind="native function",
    key_prefix=["conditional_materialization"]
)
def initial_processing() -> pd.DataFrame:
    tmp = [1,2,3,4,5]

    return pd.DataFrame(
        tmp, columns=["tmp"]
    )

@asset(
    group_name="conditional_testing",
    compute_kind="native function",
    output_required=False
)
def may_not_materialize(context: OpExecutionContext) -> Output:
    r = random.randint(0, 10)
    context.log.info(f"rand: {r}")
    if r < 5:
        yield Output([1,3,5])

@asset(
    group_name="conditional_testing",
    compute_kind="native function",
)
def depend_only_may_not_materialize(context: OpExecutionContext ,may_not_materialize: Output) -> list[int]:
    context.log.info("executed")
    context.add_output_metadata(metadata={
            "value": MetadataValue.md(str(may_not_materialize))
        }
    )
    may_not_materialize.append(7)
    return may_not_materialize

@asset(
        group_name="conditional_testing",
        compute_kind="native function",
        ins={"initial_processing": AssetIn(key_prefix="conditional_materialization")},
)
def depend_may_not_initial(
    context: OpExecutionContext,
    initial_processing: pd.DataFrame,
    may_not_materialize: Output
):
    context.log.info("depend_may_not_initial")
    context.add_output_metadata(
        metadata={
            "initial_processing_unconditional": MetadataValue.md(initial_processing.to_markdown()),
            "may_not_materialize": MetadataValue.md(str(may_not_materialize))
        }
    )
