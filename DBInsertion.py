import boto3
from boto3.dynamodb.conditions import Key
import pandas as pd
from datetime import datetime
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
table_name = 'Test_Table'
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

# Fetch the baseline values
baseline_created_query = "SELECT MAX(ID) AS max_id FROM EDW.fact.JDA_OutboundDetail;"
baseline_modified_query = "SELECT MAX(Modified) AS max_modified FROM EDW.fact.JDA_OutboundDetail;"
baseline_count_query = "SELECT COUNT(*) AS total_count FROM EDW.fact.JDA_OutboundDetail;"

max_id_df = get_sql(baseline_created_query, db='EDW_SQL_DATABASE')
max_modified_df = get_sql(baseline_modified_query, db='EDW_SQL_DATABASE')
total_count_df = get_sql(baseline_count_query, db='EDW_SQL_DATABASE')

max_id = max_id_df['max_id'].iloc[0]
max_modified = pd.to_datetime(max_modified_df['max_modified'].iloc[0])
total_count = total_count_df['total_count'].iloc[0]

# Current timestamp
current_timestamp = int(time.time())

# Store values in DynamoDB
table.put_item(
    Item={
        'TableName': source_table_name,
        'TimeStamp': current_timestamp,
        'MaxID': max_id,
        'MaxModified': max_modified.isoformat(),
        'TotalCount': total_count
    }
)

# Print results for verification
print("Initial Values Stored in DynamoDB:")
print("Max ID:", max_id)
print("Max Modified:", max_modified)
print("Total Count:", total_count)