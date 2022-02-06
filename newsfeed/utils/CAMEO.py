import os
import pandas as pd

__FILE_DIR__ = os.path.join(os.path.dirname(__file__), "eventcode.csv")

eventcode = pd.read_csv(__FILE_DIR__, dtype=str)