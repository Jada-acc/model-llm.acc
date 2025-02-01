from setuptools import setup, find_packages

setup(
    name="autonomous-llm-infrastructure",
    version="0.1",
    packages=find_packages(),
    install_requires=[
        'web3',
        'solana',
        'pytest'
    ],
)