import pandas as pd
from datster_tutorial.assets import test_dataframe

def test_initial_processing():
    assert pd.DataFrame.all(
        test_dataframe.initial_processing()["tmp"] == pd.DataFrame(
        [1, 2, 3, 4, 5], columns=["tmp"]
    )["tmp"])