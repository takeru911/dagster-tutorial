from setuptools import find_packages, setup

setup(
    name="dagster_function_complete",
    packages=find_packages(exclude=["dagster_function_complete_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud"
    ],
    extras_require={"dev": ["dagit", "pytest"]},
)
