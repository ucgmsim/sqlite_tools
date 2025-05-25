from sqlite_tools import query_sqlite_db
import sqlite3
from pathlib import Path


db_path = Path("/home/arr65/data/nzgd/extracted_nzgd.db")
conn = sqlite3.connect(db_path)

cpt_df = query_sqlite_db.cpt_measurements_for_one_nzgd(1, conn)

print(cpt_df)
