import nbformat
from nbconvert.preprocessors import ExecutePreprocessor
import subprocess

notebooks = [
    "Page Views and links analysis for LPP RCT.py",
    "Primary outcome measures for LPP RCT.py",
    "Secondary outcome measures for LPP RCT top 3.py",
]

for notebook in notebooks:
    subprocess.check_call(["ipython", "--pylab", "auto", notebook], cwd="outcomes")
