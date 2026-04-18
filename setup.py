from setuptools import setup

setup(
    name="discohack",
    version="1.0",
    packages=["."],
    install_requires=[
        "requests",
        "PyQt6",
        "watchdog",
    ],
    entry_points={
        "console_scripts": [
            "discohack = main:main"
        ]
    }
)