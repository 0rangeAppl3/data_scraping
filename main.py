import os
from pathlib import Path

# Establish ROOT_DIR before doing anything else
ROOT_DIR = Path(os.path.dirname(os.path.abspath(__file__)))
if __name__ == "__main__":
    from pipeline import run_non_sql_pipeline

    run_non_sql_pipeline("remoteok")
