import pymysql
from datetime import datetime
from davinci.services.auth import get_secret

# Database connection setup (replace with your actual database credentials)
db_connection = pymysql.connect(
    host=get_secret("DB_HOST"),
    user=get_secret("DB_USER"),
    password=get_secret("DB_PASSWORD"),
    database=get_secret("DB_NAME")
)

def get_baseline_values(cursor):
    cursor.execute("SELECT MAX(Created) AS max_created FROM EDW.fact.JDA_OutboundDetail")
    old_max_created = cursor.fetchone()['max_created']

    cursor.execute("SELECT MAX(Modified) AS max_modified FROM EDW.fact.JDA_OutboundDetail")
    old_max_modified = cursor.fetchone()['max_modified']

    cursor.execute("SELECT COUNT(*) AS total_count FROM EDW.fact.JDA_OutboundDetail")
    old_total_count = cursor.fetchone()['total_count']

    return old_max_created, old_max_modified, old_total_count

def get_update_counts(cursor, old_max_created, old_max_modified, old_total_count):
    # Get the current max Created and max Modified datetime from the warehouse table
    cursor.execute("SELECT MAX(Created) AS max_created FROM EDW.fact.JDA_OutboundDetail")
    new_max_created = cursor.fetchone()['max_created']

    cursor.execute("SELECT MAX(Modified) AS max_modified FROM EDW.fact.JDA_OutboundDetail")
    new_max_modified = cursor.fetchone()['max_modified']

    cursor.execute("SELECT COUNT(*) AS total_count FROM EDW.fact.JDA_OutboundDetail")
    new_total_count = cursor.fetchone()['total_count']

    # Calculate the number of created, modified, and deleted records
    query_created = "SELECT COUNT(*) AS created_records FROM EDW.fact.JDA_OutboundDetail WHERE Created > %s"
    cursor.execute(query_created, (old_max_created,))
    created_records = cursor.fetchone()['created_records']

    query_modified = "SELECT COUNT(*) AS modified_records FROM EDW.fact.JDA_OutboundDetail WHERE Modified > %s"
    cursor.execute(query_modified, (old_max_modified,))
    modified_records = cursor.fetchone()['modified_records']

    deleted_records = old_total_count + created_records - new_total_count

    return created_records, modified_records, deleted_records, new_total_count

# Initialize baseline values
with db_connection.cursor(pymysql.cursors.DictCursor) as cursor:
    old_max_created, old_max_modified, old_total_count = get_baseline_values(cursor)

# Get the updated counts
with db_connection.cursor(pymysql.cursors.DictCursor) as cursor:
    created_records, modified_records, deleted_records, new_total_count = get_update_counts(
        cursor, old_max_created, old_max_modified, old_total_count
    )

# Print results for verification
print("Baseline Values:")
print("Old Max Created:", old_max_created)
print("Old Max Modified:", old_max_modified)
print("Old Total Count:", old_total_count)

print("\nUpdated Counts:")
print("Created Records:", created_records)
print("Modified Records:", modified_records)
print("Deleted Records:", deleted_records)
print("New Total Count:", new_total_count)