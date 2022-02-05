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
        'tqdm', 'pandas', 'lxml', 'fake-useragent','newspaper3k'
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