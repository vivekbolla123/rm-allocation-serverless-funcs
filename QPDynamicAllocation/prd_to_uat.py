from sqlalchemy import create_engine, text

# Database connection URLs (replace with your own credentials)
source_db_url = 'mysql+pymysql://rmappuser:p6VeHNdvAHZKkjR@prod-rm-db-replica.cz6mmsw6mnag.ap-south-1.rds.amazonaws.com/QP_DW_RMALLOC'
destination_db_url = 'mysql+pymysql://rm-user:04D4+aw])g4r@uat-esb-db.cfwwkweqog18.ap-south-1.rds.amazonaws.com/QP_DW_RMALLOC'

# Create engine for both source and destination databases
source_engine = create_engine(source_db_url)
destination_engine = create_engine(destination_db_url)

# Function to copy data in chunks from large tables
def copy_table_data(table_name, chunk_size=10000, batch_size=1000):
    try:
        # Connect to the source and destination databases
        with source_engine.connect() as source_conn, destination_engine.connect() as dest_conn:
            print(f"Truncating table {table_name} in destination database...")
            dest_conn.execute(text(f"TRUNCATE TABLE {table_name}"))
            # Get total number of rows in the source table (optional but helpful)
            total_rows = source_conn.execute(text(f"SELECT COUNT(*) FROM {table_name}")).scalar()
            print(f"Total rows in {table_name}: {total_rows}")
            
            # Fetch data in chunks using LIMIT and OFFSET (pagination)
            offset = 0
            while True:
                # Fetch chunk of data
                result = source_conn.execute(text(f"SELECT * FROM {table_name} LIMIT {chunk_size} OFFSET {offset}"))
                
                # Get the data and columns
                rows = result.fetchall()
                if not rows:
                    break  # Stop when no more rows are returned

                columns = result.keys()
                column_list = ', '.join(columns)
                placeholders = ', '.join([f":{col}" for col in columns])

                # Prepare insert query for destination table
                insert_query = text(f"INSERT INTO {table_name} ({column_list}) VALUES ({placeholders})")
                
                # Insert in batches
                batch = []
                for row in rows:
                    batch.append(dict(row))
                    if len(batch) >= batch_size:
                        dest_conn.execute(insert_query, batch)
                        batch = []  # Reset batch
                
                # Insert remaining rows in the batch
                if batch:
                    dest_conn.execute(insert_query, batch)
                
                # Update offset for the next chunk
                offset += chunk_size
                print(f"Copied {offset} rows out of {total_rows}")
            
            print(f"Data successfully copied from {table_name}!")
    
    except Exception as e:
        print(f"Error copying data from {table_name}: {e}")

# Tables to copy
# tables = ['Curves','d1_d2_strategies','Fares','market_list','market_list_international','config_station_master','config_user_sector_map','market_fares']
tables = ['Curves','d1_d2_strategies','Fares','market_list','market_list_international','config_station_master','config_user_sector_map']


# Copy data for each table

for table in tables:
    copy_table_data(table, chunk_size=400000, batch_size=300000)
    print("\n--------------------------------\n")
    
