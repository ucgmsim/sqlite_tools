## Introduction

This Python package contains functions and example code to help access the SQLite 
database containing our copy of the New Zealand Geotechnical Database (NZGD).

For a guided walkthrough and interactive examples, please see the Jupyter 
Notebook: [sqlite_tools_usage_guide.ipynb](./sqlite_tools/sqlite_tools_usage_guide.ipynb).

## Installation

To install `sqlite_tools`, you'll first need to clone the repository from GitHub and 
then install it using pip. If you're new to Git or pip, follow these steps:

**1. Clone the Repository:**

   *   Open your terminal or command prompt.
   *   Navigate to the directory where you want to download the project. You can use 
   the `cd` command (e.g., `cd Documents/projects`).
   *   Run the following command to clone the repository. This will download a copy of 
   the `sqlite_tools` project to your computer:
       ```bash
       git clone https://github.com/ucgmsim/sqlite_tools.git
       ```
   *   After the command finishes, a new directory named `sqlite_tools` will be created. 
   Navigate into this directory:
       ```bash
       cd sqlite_tools
       ```

**2. Install the Package:**

   *   Now that you are inside the `sqlite_tools` directory, you can install the package 
   and its dependencies using pip. Pip is the Python package installer. If you have 
   Python installed, you likely have pip as well.
   *   Run the following command in your terminal:
       ```bash
       pip install .
       ```
   *   The `.` tells pip to install the package located in the current directory.

This will install the package and its dependencies.

## How to use

You can import and use the data extraction functions directly in your Python 
scripts. In the following examples, remember to replace placeholder paths and IDs. Some function parameters in [query.py](./sqlite_tools/query.py) can only take the specific values given in [available_options.md](./available_options.md).

```python
import sqlite3
from pathlib import Path
import pandas as pd
from sqlite_tools import query_sqlite_db # Ensure your package is installed or in PYTHONPATH

# Define the path to your SQLite database
db_file_path = Path("/path/to/your/nzgd_database.db") # Replace with your actual path

# Establish a database connection
# It's good practice to use a try/except/finally block to ensure the connection is closed.
conn = None  # Initialize conn to None
try:
    if not db_file_path.exists():
        print(f"Database file not found at: {db_file_path}")
        # Handle error appropriately, e.g., raise FileNotFoundError or exit
    else:
        conn = sqlite3.connect(db_file_path)
        print(f"Successfully connected to {db_file_path}")

        # Example 1: Get CPT measurements for NZGD ID 1
        print("\n--- CPT Measurements (NZGD ID 1) ---")
        cpt_df = query_sqlite_db.cpt_measurements_for_one_nzgd(selected_nzgd_id=1, conn=conn)
        if not cpt_df.empty:
            print(cpt_df.head())
        else:
            print("No CPT measurements found for NZGD ID 1.")

        # Example 2: Get SPT measurements for NZGD ID 2
        print("\n--- SPT Measurements (NZGD ID 2) ---")
        spt_df = query_sqlite_db.spt_measurements_for_one_nzgd(selected_nzgd_id=2, conn=conn)
        if not spt_df.empty:
            print(spt_df.head())
        else:
            print("No SPT measurements found for NZGD ID 2.")

        # Example 3: Get SPT soil types for NZGD ID 3
        print("\n--- SPT Soil Types (NZGD ID 3) ---")
        spt_soil_df = query_sqlite_db.spt_soil_types_for_one_nzgd(selected_nzgd_id=3, conn=conn)
        if not spt_soil_df.empty:
            print(spt_soil_df.head())
        else:
            print("No SPT soil types found for NZGD ID 3.")

        # Example 4: Get CPT Vs30s for NZGD ID 4
        print("\n--- CPT Vs30s (NZGD ID 4) ---")
        cpt_vs30_df = query_sqlite_db.cpt_vs30s_for_one_nzgd_id(selected_nzgd_id=4, conn=conn)
        if not cpt_vs30_df.empty:
            print(cpt_vs30_df.head())
        else:
            print("No CPT Vs30s found for NZGD ID 4.")

        # Example 5: Get SPT Vs30s for NZGD ID 5
        print("\n--- SPT Vs30s (NZGD ID 5) ---")
        spt_vs30_df = query_sqlite_db.spt_vs30s_for_one_nzgd_id(selected_nzgd_id=5, conn=conn)
        if not spt_vs30_df.empty:
            print(spt_vs30_df.head())
        else:
            print("No SPT Vs30s found for NZGD ID 5.")

        # Example 6: Get all Vs30s given specific correlations
        # Replace with actual correlation names present in your database
        print("\n--- All Vs30s (Specific Correlations) ---")
        all_vs30_data_df = query_sqlite_db.all_vs30s_given_correlations(
            selected_vs30_correlation="McGann, Bradley, Cubrinovski et al. (2015)",
            selected_cpt_to_vs_correlation="Robertson (2009)",
            selected_spt_to_vs_correlation="Ohta & Goto (1978)",
            selected_hammer_type="Safety Hammer", # Or "Donut Hammer", "Unknown" etc.
            conn=conn
        )
        if not all_vs30_data_df.empty:
            print(all_vs30_data_df.head())
        else:
            print("No Vs30 data found for the given correlation combination. \n"
                  "Please check that the correlation names and hammer type exist in the database \n"
                  "and that there are precomputed Vs30 estimates for this combination.")

except sqlite3.Error as e:
    print(f"Database error: {e}")
except Exception as e:
    print(f"An unexpected error occurred: {e}")
finally:
    if conn:
        conn.close()
        print("\nDatabase connection closed.")
```