from setuptools import setup, find_packages

setup(
    name="forge-cli",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "bcrypt>=4.3.0",
        "httpx>=0.27.0",
        "PyYAML>=6.0",
    ],
    entry_points={
        "console_scripts": [
            "forge=cli.forge:main",
            "forge-token=registry.auth:main",
        ],
    },
)
