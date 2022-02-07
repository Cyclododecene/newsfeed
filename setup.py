from setuptools import setup, find_packages
import os

setup(
    name="newsfeed",
    version="0.1.0",
    keywords="GDELT NewsFeed",
    long_description=open(os.path.join(os.path.dirname(__file__),
                                       'README.md')).read(),
    long_description_content_type='text/markdown',
    author="Cyclododecene",
    author_email="terenceliu1012@outlook.com",
    url="https://github.com/Cyclododecene/newsfeed",
    packages=find_packages(exclude=["test", "example"]),
    install_requires=[
        "numpy>=1.15.4",
        "pandas>=0.25",
        "requests>=2.22.0",
        "tqdm>=4.62.1",
        "newspaper3k>=0.2.5",
        "lxml>=4.4.2"
    ],
    license="MIT",
    classifiers=[
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.6",
)