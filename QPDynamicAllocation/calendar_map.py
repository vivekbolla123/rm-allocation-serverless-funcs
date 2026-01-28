from datetime import datetime, timedelta
from pymongo import MongoClient, UpdateOne

mongo_client = MongoClient("mongodb://localhost:27017")
mongo_db = mongo_client["qp_dw_rmalloc"]

# Create the required index for $merge
print("Creating index on Sector and DepartureDate...")
mongo_db.calendar_af.create_index([('Sector', 1), ('DepartureDate', 1)], unique=True)
print("Index created successfully")

def map_fares_between_months(source_month, source_year, target_month, target_year, num_days=None):
    """
    Map average fares from source month to target month for all sectors.
    
    Args:
        source_month (int): Source month (1-12)
        source_year (int): Source year
        target_month (int): Target month (1-12)
        target_year (int): Target year
        num_days (int, optional): Number of days to map. If None, uses min days of both months
    
    Example:
        map_fares_between_months(3, 2025, 2, 2025, 28)  # March 2025 -> Feb 2025 (28 days)
        map_fares_between_months(4, 2025, 5, 2025)      # April 2025 -> May 2025 (30 days)
    """
    
    # Calculate the last day of each month
    from calendar import monthrange
    
    source_max_days = monthrange(source_year, source_month)[1]
    target_max_days = monthrange(target_year, target_month)[1]
    
    # Determine how many days to map
    if num_days is None:
        num_days = min(source_max_days, target_max_days)
    else:
        num_days = min(num_days, source_max_days, target_max_days)
    
    # Build source date range
    source_start = f"{source_year:04d}-{source_month:02d}-01"
    source_end = f"{source_year:04d}-{source_month:02d}-{num_days:02d}"
    
    print(f"Mapping fares from {source_start} to {source_end}")
    print(f"Target month: {target_year:04d}-{target_month:02d}")
    print(f"Days to map: {num_days}")
    
    # Step 1: Fetch source month data
    source_data = list(mongo_db.calendar_af.find({
        'DepartureDate': {
            '$gte': source_start,
            '$lte': source_end
        }
    }))
    
    if not source_data:
        print(f"No data found for {source_start} to {source_end}")
        return None
    
    print(f"Found {len(source_data)} records in source month")
    
    # Step 2: Create target month documents
    bulk_operations = []
    
    for source_doc in source_data:
        # Extract day from source date
        source_date = source_doc['DepartureDate']  # e.g., "2025-03-15"
        day = int(source_date.split('-')[2])
        
        # Skip if day exceeds target month's max days
        if day > target_max_days:
            continue
        
        # Create corresponding target date
        target_date = f"{target_year:04d}-{target_month:02d}-{day:02d}"
        
        # Prepare update operation
        bulk_operations.append(
            UpdateOne(
                {
                    'Sector': source_doc['Sector'],
                    'DepartureDate': target_date
                },
                {
                    '$set': {
                        'AverageFare': source_doc['AverageFare']
                    }
                },
                upsert=True
            )
        )
    
    # Step 3: Execute bulk write
    if bulk_operations:
        result = mongo_db.calendar_af.bulk_write(bulk_operations)
        print(f"\nResults:")
        print(f"  Matched: {result.matched_count}")
        print(f"  Modified: {result.modified_count}")
        print(f"  Upserted: {result.upserted_count}")
        print(f"  Total operations: {len(bulk_operations)}")
        return result
    else:
        print("No valid operations to perform")
        return None


# Alternative: Using Aggregation Pipeline (more efficient for large datasets)
def map_fares_aggregation(source_month, source_year, target_month, target_year, num_days=None):
    """
    Map average fares using aggregation pipeline.
    More efficient for large datasets.
    """
    from calendar import monthrange
    
    source_max_days = monthrange(source_year, source_month)[1]
    target_max_days = monthrange(target_year, target_month)[1]
    
    if num_days is None:
        num_days = min(source_max_days, target_max_days)
    else:
        num_days = min(num_days, source_max_days, target_max_days)
    
    source_start = f"{source_year:04d}-{source_month:02d}-01"
    source_end = f"{source_year:04d}-{source_month:02d}-{num_days:02d}"
    
    pipeline = [
        # Match source month data
        {
            '$match': {
                'DepartureDate': {
                    '$gte': source_start,
                    '$lte': source_end
                }
            }
        },
        # Extract day and create target date
        {
            '$addFields': {
                'day': {'$substr': ['$DepartureDate', 8, 2]},
                'day_num': {'$toInt': {'$substr': ['$DepartureDate', 8, 2]}}
            }
        },
        # Filter out days that exceed target month's max days
        {
            '$match': {
                'day_num': {'$lte': target_max_days}
            }
        },
        # Create target date as DepartureDate field
        {
            '$addFields': {
                'DepartureDate': {
                    '$concat': [
                        f"{target_year:04d}-{target_month:02d}-",
                        '$day'
                    ]
                }
            }
        },
        # Keep only needed fields
        {
            '$project': {
                'Sector': 1,
                'DepartureDate': 1,
                'AverageFare': 1,
                '_id': 0
            }
        },
        # Merge into collection
        {
            '$merge': {
                'into': 'calendar_af',
                'on': ['Sector', 'DepartureDate'],
                'whenMatched': 'replace',
                'whenNotMatched': 'insert'
            }
        }
    ]
    
    print(f"Mapping {source_start} to {source_end} → {target_year:04d}-{target_month:02d}")
    mongo_db.calendar_af.aggregate(pipeline, allowDiskUse=True)
    print("Mapping completed successfully")
    
map_fares_between_months(3, 2025, 2, 2025, 28)