## Introduction

This Python package contains functions and example code to help access the SQLite 
database containing our copy of the New Zealand Geotechnical Database (NZGD).


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

For a guided walkthrough and interactive examples, please see the Jupyter 
Notebook: [sqlite_tools_usage_guide.ipynb](./sqlite_tools_usage_guide.ipynb).