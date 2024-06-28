import boto3
from boto3.dynamodb.conditions import Key
import pandas as pd
from datetime import datetime,timedelta
import time
from davinci.services.sql import get_sql
from davinci.services.auth import get_secret

# Initialize a session using Amazon DynamoDB
boto3_login = {
    "verify": False,
    "service_name": 'dynamodb',
    "region_name": 'us-east-1',
    "aws_access_key_id": get_secret("AWS_ACCESS_KEY_ID"),
    "aws_secret_access_key": get_secret("AWS_SECRET_ACCESS_KEY")
}
dynamodb = boto3.resource(**boto3_login)

# Define the table name
table_name = 'summary_test_table'
source_table_name = 'EDW.fact.JDA_OutboundDetail'

# Check if the table already exists
existing_tables = [table.name for table in dynamodb.tables.all()]

if table_name not in existing_tables:
    # Create the DynamoDB table
    table = dynamodb.create_table(
        TableName=table_name,
        KeySchema=[
            {
                'AttributeName': 'TableName',
                'KeyType': 'HASH'  # Partition key
            },
            {
                'AttributeName': 'TimeStamp',
                'KeyType': 'RANGE'  # Sort key
            }
        ],
        AttributeDefinitions=[
            {
                'AttributeName': 'TableName',
                'AttributeType': 'S'
            },
            {
                'AttributeName': 'TimeStamp',
                'AttributeType': 'N'
            }
        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 10,
            'WriteCapacityUnits': 10
        }
    )

    # Wait until the table exists.
    table.meta.client.get_waiter('table_exists').wait(TableName=table_name)

else:
    # If the table already exists, get the table resource
    table = dynamodb.Table(table_name)

print("Table status:", table.table_status)


def round_up_seconds(dt):
    if dt.microsecond > 0:
        dt += timedelta(seconds=1)
    return dt.replace(microsecond=0)

# Function to fetch baseline values
def get_baseline_values():
    baseline_created_query = "SELECT MAX(Created) AS max_created FROM EDW.fact.JDA_OutboundDetail;"
    baseline_modified_query = "SELECT MAX(Modified) AS max_modified FROM EDW.fact.JDA_OutboundDetail;"
    baseline_count_query = "SELECT COUNT(*) AS total_count FROM EDW.fact.JDA_OutboundDetail;"

    max_created_df = get_sql(baseline_created_query, db='EDW_SQL_DATABASE')
    max_modified_df = get_sql(baseline_modified_query, db='EDW_SQL_DATABASE')
    total_count_df = get_sql(baseline_count_query, db='EDW_SQL_DATABASE')

    max_created = pd.to_datetime(max_created_df['max_created'].iloc[0])
    max_modified = pd.to_datetime(max_modified_df['max_modified'].iloc[0])
    total_count = int(total_count_df['total_count'].iloc[0])

    return max_created, max_modified, total_count


# Function to update DynamoDB table
def update_dynamodb_table(old_max_id, old_max_modified, old_total_count):
    old_max_modified = round_up_seconds(old_max_modified)
    old_max_modified_str = old_max_modified.strftime('%Y-%m-%dT%H:%M:%S')
    created_query = f"SELECT COUNT(*) AS created_records FROM EDW.fact.JDA_OutboundDetail WHERE ID > {old_max_id};"
    modified_query = f"SELECT COUNT(*) AS modified_records FROM EDW.fact.JDA_OutboundDetail WHERE Modified > '{old_max_modified_str}' AND ID < {old_max_id};"
    current_total_count_query = "SELECT COUNT(*) AS current_total_count FROM EDW.fact.JDA_OutboundDetail;"

    created_df = get_sql(created_query, db='EDW_SQL_DATABASE')
    modified_df = get_sql(modified_query, db='EDW_SQL_DATABASE')
    current_total_count_df = get_sql(current_total_count_query, db='EDW_SQL_DATABASE')

    created_records = int(created_df['created_records'].iloc[0])
    modified_records = int(modified_df['modified_records'].iloc[0])
    current_total_count = int(current_total_count_df['current_total_count'].iloc[0])

    deleted_records = old_total_count + created_records - current_total_count

    # Current timestamp
    current_timestamp = int(datetime.now().strftime("%Y%m%d%H%M"))

    # Max ID and Modified timestamp for the current run
    new_max_id_query = "SELECT MAX(ID) AS max_id FROM EDW.fact.JDA_OutboundDetail;"
    new_max_modified_query = "SELECT MAX(Modified) AS max_modified FROM EDW.fact.JDA_OutboundDetail;"

    new_max_id_df = get_sql(new_max_id_query, db='EDW_SQL_DATABASE')
    new_max_modified_df = get_sql(new_max_modified_query, db='EDW_SQL_DATABASE')

    new_max_id = int(new_max_id_df['max_id'].iloc[0])
    new_max_modified = pd.to_datetime(new_max_modified_df['max_modified'].iloc[0])
    new_max_modified = round_up_seconds(new_max_modified)

    # Put the data into the DynamoDB table
    item = {
        'TableName': source_table_name,
        'TimeStamp': current_timestamp,
        'CreatedRecords': created_records,
        'ModifiedRecords': modified_records,
        'DeletedRecords': deleted_records,
        'MaxID': new_max_id,
        'MaxModified': new_max_modified.isoformat(),
        'TotalCount': current_total_count
    }

    # Print the data to be inserted for verification
    print("Data to be inserted into DynamoDB:")
    for key, value in item.items():
        print(f"{key}: {value}")

    table.put_item(Item=item)


# Fetch previous values from DynamoDB
response = table.query(
    KeyConditionExpression=Key('TableName').eq(source_table_name),
    ScanIndexForward=False,  # Descending order
    Limit=1
)
print(response)

if response['Items']:
    prev_item = response['Items'][0]
    old_max_id = int(prev_item['MaxID'])
    old_max_modified = pd.to_datetime(prev_item['MaxModified'])
    old_total_count = int(prev_item['TotalCount'])
    # Update the table with the latest counts
    update_dynamodb_table(old_max_id, old_max_modified, old_total_count)
else:
    # Initial run, fetch baseline values
    old_max_created, old_max_modified, old_total_count = get_baseline_values()
    old_max_id_query = "SELECT MAX(ID) AS max_id FROM EDW.fact.JDA_OutboundDetail;"
    old_max_id_df = get_sql(old_max_id_query, db='EDW_SQL_DATABASE')
    old_max_id = int(old_max_id_df['max_id'].iloc[0])

    # Initialize DynamoDB with the first set of values
    current_timestamp = int(datetime.now().strftime("%Y%m%d%H%M"))
    item = {
        'TableName': source_table_name,
        'TimeStamp': current_timestamp,
        'CreatedRecords': 0,
        'ModifiedRecords': 0,
        'DeletedRecords': 0,
        'MaxID': old_max_id,
        'MaxModified': old_max_modified.isoformat(),
        'TotalCount': old_total_count
    }

    # Print the data to be inserted for verification
    print("Data to be inserted into DynamoDB (initial):")
    for key, value in item.items():
        print(f"{key}: {value}")

    table.put_item(Item=item)

# Print results for verification
print("Old Values:")
print("Old Max ID:", old_max_id)
print("Old Max Modified:", old_max_modified)
print("Old Total Count:", old_total_count)