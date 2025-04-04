from setuptools import find_packages, setup

setup(
    name="bluesky5f61dce7fa034",
    version="0.0.4",
    packages=find_packages(),
    install_requires=[
        "exorde_data",
        "aiohttp",
        "requests",
        "websockets==13.1"
    ],
    extras_require={"dev": ["pytest", "pytest-cov", "pytest-asyncio"]},
)
