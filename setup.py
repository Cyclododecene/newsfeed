from setuptools import setup, find_packages
import os

setup(
    name="newsfeed",
    version="0.1.7.3",
    keywords="GDELT NewsFeed",
    long_description=open(os.path.join(os.path.dirname(__file__),
                                       'README.md')).read(),
    long_description_content_type='text/markdown',
    author="Cyclododecene",
    author_email="terenceliu1012@outlook.com",
    url="https://github.com/Cyclododecene/newsfeed",
    packages=find_packages(exclude=["test", "example"]),
    install_requires=[
        "numpy>=2.4.0",
        "pandas>=2.3.3",
        "requests>=2.32.5",
        "tqdm>=4.67.1",
        "newspaper4k>=0.9.4.1",
        "lxml>=6.0.2",
        "lxml-html-clean>=0.4.3",
        "beautifulsoup4>=4.14.3",
        "fake-useragent>=2.2.0",
        "nltk>=3.9.2",
        "tenacity>=9.1.2",
        "aiohttp>=3.11.0",
        "aiofiles>=24.1.0",
        "pyarrow>=19.0.0",
        "joblib>=1.4.0"
    ],
    entry_points={
        'console_scripts': [
            'newsfeed=newsfeed.__main__:main',
        ],
    },
    license="MIT",
    classifiers=[
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.11",
)
