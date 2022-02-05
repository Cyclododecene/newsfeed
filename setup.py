from setuptools import setup, find_packages
import os

setup(
    name="GNAF-backend",
    version="0.0.1",
    keywords="GDELT News Aggregation and Feed",
    long_description=open(os.path.join(os.path.dirname(__file__),
                                       'README.md')).read(),
    long_description_content_type='text/markdown',
    author="TerenceCKLau",
    author_email="terenceliu1012@outlook.com",
    url="https://github.com/Cyclododecene/GNAF",
    packages=find_packages(),
    install_requires=[
        'tqdm', 'pandas', 'lxml', 'fake-useragent'
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