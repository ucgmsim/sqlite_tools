# sqlite_tools

A Python package for querying an NZGD (New Zealand Geotechnical Database) style SQLite database and extracting geotechnical data. It provides both a command-line interface (CLI) and a Python API for accessing various datasets.

For a guided walkthrough and interactive examples, please see the Jupyter Notebook: [sqlite_tools_usage_guide.ipynb](./sqlite_tools/examples/sqlite_tools_usage_guide.ipynb).

## Installation

### From local source

If you have cloned this repository or have access to the source code, you can install the package using pip:

1.  Navigate to the root directory of the `sqlite_tools` project (the one containing `pyproject.toml`).
2.  Run the installation command:

    ```bash
    pip install .
    ```

This will install the package and its dependencies, making the `sqlite-query` command-line tool available in your environment.

## Command-Line Interface (CLI) Usage

The package provides a CLI named `sqlite-query` for direct interaction with the database from your terminal.

**General Syntax:**

```bash
sqlite-query [COMMAND] --db-path /path/to/your/database.db [OTHER_OPTIONS]
```

The output from all commands is printed to the standard output in CSV format. You can easily redirect this output to a file:

```bash
sqlite-query [COMMAND] --db-path /path/to/db.db [OPTIONS] > output.csv
```

### Available Commands:

1.  **`cpt-measurements`**: Extracts Cone Penetration Test (CPT) measurements for a given NZGD ID.
    *   `--db-path TEXT`: Path to the SQLite database file (required).
    *   `--nzgd-id INTEGER`: The NZGD ID to query (required).
    *   Example:
        ```bash
        sqlite-query cpt-measurements --db-path /data/nzgd_database.db --nzgd-id 123
        ```

2.  **`spt-measurements`**: Extracts Standard Penetration Test (SPT) measurements for a given NZGD ID.
    *   `--db-path TEXT`: Path to the SQLite database file (required).
    *   `--nzgd-id INTEGER`: The NZGD ID to query (required).
    *   Example:
        ```bash
        sqlite-query spt-measurements --db-path /data/nzgd_database.db --nzgd-id 456
        ```

3.  **`spt-soil-types`**: Extracts SPT soil type information for a given NZGD ID.
    *   `--db-path TEXT`: Path to the SQLite database file (required).
    *   `--nzgd-id INTEGER`: The NZGD ID to query (required).
    *   Example:
        ```bash
        sqlite-query spt-soil-types --db-path /data/nzgd_database.db --nzgd-id 789
        ```

4.  **`cpt-vs30s`**: Extracts CPT Vs30 (average shear wave velocity in the top 30m) estimates for a given NZGD ID.
    *   `--db-path TEXT`: Path to the SQLite database file (required).
    *   `--nzgd-id INTEGER`: The NZGD ID to query (required).
    *   Example:
        ```bash
        sqlite-query cpt-vs30s --db-path /data/nzgd_database.db --nzgd-id 1011
        ```

5.  **`spt-vs30s`**: Extracts SPT Vs30 estimates for a given NZGD ID.
    *   `--db-path TEXT`: Path to the SQLite database file (required).
    *   `--nzgd-id INTEGER`: The NZGD ID to query (required).
    *   Example:
        ```bash
        sqlite-query spt-vs30s --db-path /data/nzgd_database.db --nzgd-id 1213
        ```

6.  **`all-vs30s`**: Extracts all CPT and SPT Vs30 data based on selected Vs-to-Vs30 correlation, CPT-to-Vs correlation, SPT-to-Vs correlation, and hammer type.
    *   `--db-path TEXT`: Path to the SQLite database file (required).
    *   `--vs30-correlation TEXT`: Selected Vs to Vs30 correlation name (required).
    *   `--cpt-to-vs-correlation TEXT`: Selected CPT to Vs correlation name (required).
    *   `--spt-to-vs-correlation TEXT`: Selected SPT to Vs correlation name (required).
    *   `--hammer-type TEXT`: Selected hammer type name (required).
    *   Example:
        ```bash
        sqlite-query all-vs30s --db-path /data/nzgd_database.db \
            --vs30-correlation "ExampleVs30Method" \
            --cpt-to-vs-correlation "ExampleCPTtoVsMethod" \
            --spt-to-vs-correlation "ExampleSPTtoVsMethod" \
            --hammer-type "Safety Hammer"
        ```

## Python API Usage

You can also import and use the data extraction functions directly within your Python scripts. This provides more flexibility for integrating the data into other workflows.

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

This provides a basic structure for using the functions. Remember to replace placeholder paths and IDs with actual values relevant to your database and use case.
The `all_vs30s_given_correlations` function, in particular, relies on specific string names for correlations and hammer types that must exist in your database tables (`vstovs30correlation`, `cpttovscorrelation`, `spttovscorrelation`, `spttovs30hammertype`).
