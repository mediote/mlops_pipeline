from setuptools import find_packages, setup

setup(
    name="mlops_pipeline",
    version="0.1",
    packages=find_packages(),
    install_requires=[

    ],
    entry_points={
        "console_scripts": [
            "run_pipeline=mlops_pipeline.main:run_pipeline",
        ],
    },
)
