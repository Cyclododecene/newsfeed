import sys
import os

__VERSION__ = "0.1.0"
__AUTHOR__ = "Cyclododecene"


if sys.version_info < (3, 6):
    print(f"GNAF {__VERSION__} requires Python 3.6+")
    sys.exit(1)
del sys

from newsfeed import * 
