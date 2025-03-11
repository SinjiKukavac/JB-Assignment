from adyen_csv_data import download_files, insert_adyen_data_to_psql
from sql_server_data import insert_netsuite_data_to_psql
from sqlalchemy import create_engine
from ssms_connection_details import *

# folder with adyen data
csv_folder = "csv_files"

# GIt repo with files
git_url = "https://api.github.com/repos/antontucek/jb-dea-test-assignment/contents/settlement"

# postgresql connection
pg_conn_str = "postgresql://postgres:admin@localhost/jetbrains_analysis"

# SSMS connection, TrustServerCertificate=yes (SSL erros solution)
connection_url = f"mssql+pyodbc://{username}:{password}@{server}:{port}/{database}?driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=yes"

def main(connection_url, git_url, csv_folder):
    """Main function for ETL execution"""
    # PSQL connection
    pg_engine = create_engine(pg_conn_str)

    try:
        insert_netsuite_data_to_psql(connection_url, pg_engine)
        download_files(git_url, csv_folder)
        insert_adyen_data_to_psql(csv_folder, pg_engine)
    finally:
        # Zatvaramo konekcije na kraju
        pg_engine.dispose()
        print("PSQL connection closed.")


if __name__ == "__main__":
    main(connection_url, git_url, csv_folder)
    