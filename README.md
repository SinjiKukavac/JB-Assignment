# README

## Environment
The data analysis was performed using PostgreSQL. Before analyzing the data, it must first be imported using the scripts located in this directory.

## Importing Data
This directory contains all necessary scripts except the script with connection details, which was sent via email due to sensitive information. The missing script contains connection details for the SQL Server database.

## Setup Instructions
1. Install the required libraries listed in `requirements.txt`:
   ```sh
   pip install -r requirements.txt
   ```

2. Before running the data import, create the necessary tables in the PostgreSQL database using the SQL queries found in `tables.txt`.

3. To import the data into PostgreSQL, simply run the `main_file.py` script:
   ```sh
   python main_file.py
   ```
