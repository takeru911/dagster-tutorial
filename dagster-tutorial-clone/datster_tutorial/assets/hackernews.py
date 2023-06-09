import base64
import requests
import pandas as pd
import matplotlib.pyplot as plt
from io import BytesIO
from dagster import AssetIn, asset, OpExecutionContext, MetadataValue
from typing import List
from wordcloud import STOPWORDS, WordCloud


# QA: assetのgroup_nameとは
@asset(
        group_name="hackernews", 
        compute_kind="HackerNewsAPI"
)
def hackernews_topstory_ids_clone() -> List[int]:
    """
    Got up to 500 top stories from the HackerNews topstories endpoint.
    API Docs:  https://github.com/HackerNews/API#new-top-and-best-stories
    """

    newstories_url = "https://hacker-news.firebaseio.com/v0/topstories.json"
    top_500_newstries = requests.get(newstories_url).json()

    return top_500_newstries

# QA: contextとは
@asset(
        group_name="hackernews", 
        compute_kind="HackerNews API",
        
)
def hackernews_topstories_clone(
    context: OpExecutionContext, hackernews_topstory_ids_clone: List[int]
) -> pd.DataFrame:
    """
    Get items based on story ids from the HackerNews items endpoint. It may take 1-2 minutes to fetch all 500 items.
    API Docs: https://github.com/HackerNews/API#items
    """    
    results = []
    for item_id in upstream:
        item = requests.get(f"https://hacker-news.firebaseio.com/v0/item/{item_id}.json").json()
        results.append(item)
        if len(results) % 20 == 0:
            context.log.info("Got {len(results)} item so far.")
    df = pd.DataFrame(results)

    context.add_output_metadata(
        {
            "num_records": len(df),
            "preview": MetadataValue.md(df.head().to_markdown()),
        }
    )

    return df

@asset(group_name="hackernews", compute_kind="Plot")
def hackernews_topstories_word_cloud_clone(
    context: OpExecutionContext, hackernews_topstories_clone: pd.DataFrame
) -> bytes:
    """Exploratory analysis: Generate a word cloud from the current top 500 HackerNews top stories.
    Embed the plot into a Markdown metadata for quick view.
    Read more about how to create word clouds in http://amueller.github.io/word_cloud/.
    """
    stopwords = set(STOPWORDS)
    stopwords.update(["Ask", "Show", "HN"])
    titles_text = " ".join([str(item) for item in hackernews_topstories_clone["title"]])
    titles_cloud = WordCloud(stopwords=stopwords, background_color="white").generate(titles_text)

    # Generate the cloud image
    plt.figure(figsize=(8, 8), facecolor=None)
    plt.imshow(titles_cloud, interpolation="bilinear")
    plt.axis("off")
    plt.tight_layout(pad=0)

    # Save the image to buffer and embed the image into Markdown content for quick view
    buffer = BytesIO()
    plt.savefig(buffer, format="png")
    image_data = base64.b64encode(buffer.getvalue())
    md_content = f"![img](data:image/png;base64,{image_data.decode()})"

    # Attach the Markdown content as metadata to the asset
    # Read abount more metadata types in https://docs.dagster.io/_apidocs/ops#metadata-types
    context.add_output_metadata({"plot": MetadataValue.md(md_content)})

    return image_data

