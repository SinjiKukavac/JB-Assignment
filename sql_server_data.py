from sqlalchemy import create_engine, text
import pandas as pd

def insert_netsuite_data_to_psql(connection_url, pg_engine):
    """We are using this menthod to insert Netsuite data from SSMS to PSQL. 
    That is the reason why we have two different connection as a parameters."""
    # Connection Engine
    engine = create_engine(connection_url)

    offset = 0 # using for pagination, as a starting point for chunk
    chunk_size=50000

    while True:
        with engine.connect() as connection:
            # Main Query
            query = text(f"""
                select tr.MERCHANT_ACCOUNT, tr.BATCH_NUMBER, tr.TRANDATE, tr.TRANSACTION_TYPE, AMOUNT_FOREIGN as AMOUNT, tr.ORDER_REF
                from dea.netsuite.TRANSACTIONS tr
                join dea.netsuite.TRANSACTION_LINES li on li.TRANSACTION_ID = tr.TRANSACTION_ID
                join dea.netsuite.accounts acc on acc.ACCOUNT_ID = li.ACCOUNT_ID
                where acc.ACCOUNTNUMBER in (315700 , 315710, 315720, 315800, 548201)
                ORDER BY tr.TRANSACTION_ID
                OFFSET {offset} ROWS FETCH NEXT {chunk_size} ROWS ONLY
            """)
            
            # Query execution
            #print(query) 
            df = pd.read_sql(query, connection)

            if df.empty:
                break  # Loop exit, no more data
            
            #print(df.to_csv('netsuite/data.csv'))
            with pg_engine.begin() as conn:
                # Importing data to PSQL
                df.columns = df.columns.str.lower()
                df.to_sql("netsuite_transactions", conn, if_exists="append", index=False)
            offset += chunk_size  