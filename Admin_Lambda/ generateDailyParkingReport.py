import json
import boto3
from datetime import datetime, timezone
import os
import io
import csv

# Initialize Boto3 clients
dynamodb = boto3.resource('dynamodb')
s3_client = boto3.client('s3')

# Get configuration from environment variables
LOGS_TABLE_NAME = os.environ.get('HISTORY_TABLE_NAME')
PARKING_TABLE_NAME = os.environ.get('SLOT_TABLE_NAME')
S3_BUCKET_NAME = os.environ.get('REPORTS_BUCKET_NAME')

logs_table = dynamodb.Table(LOGS_TABLE_NAME)
parking_table = dynamodb.Table(PARKING_TABLE_NAME)

def lambda_handler(event, context):
    try:
        # 1. Define Today's Date
        today_date_str = datetime.now(timezone.utc).strftime('%Y-%m-%d')
        report_data = []

        # 2. Fetch Completed Sessions (using 'DateIndex' on logs_table)
        # 2. Fetch Completed Sessions (using 'DateIndex' on logs_table)
        completed_sessions = logs_table.query(
            IndexName='date-exit_timestamp-index',
            KeyConditionExpression='#d = :today', # Use a placeholder #d instead of date
            ExpressionAttributeNames={'#d': 'date'}, # Map the placeholder to the real attribute name
            ExpressionAttributeValues={ ':today': today_date_str }
        )['Items']

        for item in completed_sessions:
            item['status'] = 'COMPLETED'
            report_data.append(item)

        # 3. Fetch In-Progress Sessions (using your existing GSI)
        # This uses your 'status-area_number-index' to get all occupied slots at once.
        active_sessions = parking_table.query(
            IndexName='status-area_number-index',
            KeyConditionExpression='#s = :occupied', # Use a placeholder #s
            ExpressionAttributeNames={'#s': 'status'}, # Map the placeholder to 'status'
            ExpressionAttributeValues={ ':occupied': 'occupied' }
        )['Items']

        for item in active_sessions:

            entry_ts = item.get('entry_timestamp', '') # Safely get the timestamp
            if entry_ts.startswith(today_date_str):
                report_item = {
                    'session_id': 'N/A',
                    'date': item.get('entry_timestamp', '').split('T')[0],
                    'exit_timestamp': '',
                    'duration_minutes': '',
                    'status': 'PARKED',
                    'area_id': item.get('area_number'),
                    'vehicle_id': item.get('vehicle_id'),
                    'parking_id': item.get('parking_id'),
                    'entry_timestamp': item.get('entry_timestamp'),
                    'floor_number': item.get('floor_number'),
                    'email': item.get('email')
                }
                report_data.append(report_item)
        
        # 4. Create and Upload CSV
        if not report_data:
            print(f"No data to report for {today_date_str}.")
            return {'statusCode': 200, 'body': json.dumps('No data, report not generated.')}

        output_stream = io.StringIO()
        headers = [
            'session_id', 'date', 'exit_timestamp', 'status', 'vehicle_id', 'parking_id',
            'area_id', 'floor_number', 'entry_timestamp', 'duration_minutes', 'email'
        ]
        writer = csv.DictWriter(output_stream, fieldnames=headers, extrasaction='ignore')
        writer.writeheader()
        writer.writerows(report_data)

        s3_key = f"reports/{today_date_str}.csv"
        s3_client.put_object(
            Bucket=S3_BUCKET_NAME,
            Key=s3_key,
            Body=output_stream.getvalue().encode('utf-8'),
            ContentType='text/csv'
        )

        return {
            'statusCode': 200,
            'body': json.dumps(f'Successfully created report: s3://{S3_BUCKET_NAME}/{s3_key}')
        }

    except Exception as e:
        print(f"Error: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }
