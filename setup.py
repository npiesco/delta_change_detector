# setup.py
from setuptools import setup, find_packages

setup(
    name="delta_change_detector",
    version="0.1.1",
    author="Nicholas G. Piesco",
    author_email="ngpiesco@gmail.com",
    description="A package to detect changes in Delta Lake tables.",
    long_description=open('README.md').read(),
    long_description_content_type="text/markdown",
    url="https://github.com/npiesco/delta_change_detector",
    packages=find_packages(),
    install_requires=[
        "deltalake",
        "pyarrow",
        "pandas"
    ],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
)