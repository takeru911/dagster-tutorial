import pandas as pd
from dagster import AssetIn, AssetOut, MetadataValue, OpExecutionContext, Output, asset, multi_asset, op, graph_asset, AssetMaterialization


@op
def test_op(initial_processing: pd.DataFrame) -> str:
    return f"opのtestだよ.{initial_processing.shape}"

@op
def test_op2(test_op: str) -> str:    
    return test_op + "ほんとうかね？"


@asset(
    group_name="dataframe",
)
def initial_processing() -> pd.DataFrame:
    tmp = [1,2,3,4,5]  
    
    return pd.DataFrame(
        tmp, columns=["tmp"]
    )


@multi_asset(
    outs={
        "multi_a": AssetOut(is_required=False),
        "multi_b": AssetOut(is_required=False),
    },
    can_subset=True,
    group_name="dataframe",
    compute_kind="native"
)
def multi_asset(context):
    if "multi_a" in context.selected_output_names:
        yield Output(value=123, output_name="multi_a")
    if "multi_b" in context.selected_output_names:
        yield Output(value=456, output_name="multi_b")

@asset(
    group_name="dataframe",
    compute_kind="native",
)
def using_multiasset(multi_b):
    return multi_b

@graph_asset(
    group_name="dataframe",
     
)
def init(initial_processing) -> pd.DataFrame:
    return test_op2(test_op(initial_processing))

@asset(
    group_name="dataframe",
    compute_kind="Pandas"
)
def initial_processing_2() -> pd.DataFrame:
    tmp = [6,7,8,9,10]

    return pd.DataFrame(
        tmp, columns=["tmp2"]
    )

@asset(
    group_name="dataframe",
    compute_kind="Pandas"
)
def depended_initial(context: OpExecutionContext, initial_processing: pd.DataFrame, initial_processing_2: pd.DataFrame) -> pd.DataFrame:
    context.log.info("depend_initial")
    data = pd.concat([initial_processing, initial_processing_2], axis=1)
    context.add_output_metadata(
        {
            "data_name": "tmp+tmp2",
            "preview": MetadataValue.md(data.head().to_markdown())
        }
    )
    return data

@asset(
    group_name="dataframe",
    compute_kind="Pandas",
    metadata={"tag": "test_asset_ins_multiple"},
    ins={"upstream": AssetIn("initial_processing")}
)
def depended_initial_with_ins(context: OpExecutionContext, upstream): 
    context.log.info(upstream)

    context.add_output_metadata(
        {
            "data_name": "upstream",
            "content": MetadataValue(upstream)
        }
    )
