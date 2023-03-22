import pandas as pd
from dagster import AssetIn, MetadataValue, OpExecutionContext, asset


@asset(
    group_name="dataframe",
    compute_kind="Pandas"
)
def initial_processing() -> pd.DataFrame:
    tmp = [1,2,3,4,5]

    return pd.DataFrame(
        tmp, columns=["tmp"]
    )

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
            "content": upstream
        }
    )
