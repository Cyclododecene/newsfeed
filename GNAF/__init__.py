import sys
import os

__version__ = "0.0.1"
__author__ = "Cyclododecene"


if sys.version_info < (3, 6):
    print(f"GNAF {__version__} requires Python 3.6+")
    sys.exit(1)
del sys

from GNAF import * 
