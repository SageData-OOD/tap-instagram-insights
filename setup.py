#!/usr/bin/env python
from setuptools import setup

setup(
    name="tap-instagram-insights",
    version="0.1.0",
    description="Singer.io tap for extracting data",
    author="Stitch",
    url="http://singer.io",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    py_modules=["tap_instagram_insights"],
    install_requires=[
        # NB: Pin these to a more specific version for tap reliability
        "singer-python==5.13.0",
        "requests",
        "facebook-sdk==3.1.0"
    ],
    entry_points="""
    [console_scripts]
    tap-instagram-insights=tap_instagram_insights:main
    """,
    packages=["tap_instagram_insights"],
    package_data={
        "schemas": ["tap_instagram_insights/schemas/*.json"]
    },
    include_package_data=True,
)
