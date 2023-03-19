from setuptools import find_packages, setup

setup(
    name="datster_tutorial",
    packages=find_packages(exclude=["datster_tutorial_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud",
        "boto3",
        "pandas",
        "matplotlib",
        "textblob",
        "tweepy",
        "wordcloud",
    ],
    extras_require={"dev": ["dagit", "pytest"]},
)
