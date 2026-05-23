from setuptools import setup, find_packages

setup(
    name="forge-cli",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "requests>=2.32.0",
        "pyyaml>=6.0",
        "click>=8.1.0",
    ],
    entry_points={
        "console_scripts": [
            "forge=cli.forge:cli",
        ],
    },
)
