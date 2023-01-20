from setuptools import find_packages, setup

setup(
    name="trx_code_challenge",
    packages=find_packages(exclude=["trx_code_challenge_tests"]),
    install_requires=[
        "dagster",
        "pandas",
        "requests",
        "awswrangler",
        "boto3"
    ],
    extras_require={"dev": ["dagit", "pytest"]},
)
