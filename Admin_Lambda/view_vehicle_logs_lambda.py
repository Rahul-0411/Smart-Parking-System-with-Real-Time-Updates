import json
import os
import boto3
from decimal import Decimal
from boto3.dynamodb.conditions import Key

from admin_module.logging_util import create_admin_log

# Initialize DynamoDB client
dynamodb = boto3.resource('dynamodb')

def decimal_serializer(obj):
    """Custom JSON serializer for Decimal objects."""
    if isinstance(obj, Decimal):
        return int(obj) if obj % 1 == 0 else float(obj)
    raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")

def lambda_handler(event, context):
    """
    Scans the vehicle logs table for records matching a specific date.
    """
    try:
        # Get table name from environment variables
        table_name = os.environ['VEHICLE_LOGS_DATABASE']
        table = dynamodb.Table(table_name)
        
        # Get the target date from the request body
        body = json.loads(event.get('body', '{}'))
        target_date = body['date']
        
        # --- Scan Operation with Filter ---
        # This reads the entire table and then filters, which can be inefficient
        # on very large tables without a GSI on the 'date' attribute.
        scan_kwargs = {
            'FilterExpression': Key('date').eq(target_date)
        }
        
        matching_logs = []
        done = False
        start_key = None
        while not done:
            if start_key:
                scan_kwargs['ExclusiveStartKey'] = start_key
            response = table.scan(**scan_kwargs)
            matching_logs.extend(response.get('Items', []))
            start_key = response.get('LastEvaluatedKey', None)
            done = start_key is None
        # ---------------------------------
        
        # Log the admin action
        log_details = {
            "queried_date": target_date,
            "logs_found": len(matching_logs)
        }
        create_admin_log(action="ViewVehicleLogs", details=log_details)
        
        return {
            'statusCode': 200,
            "headers": {
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Headers": "Content-Type",
                "Access-Control-Allow-Methods": "OPTIONS,POST"
            },
            'body': json.dumps(matching_logs, default=decimal_serializer)
        }

    except (KeyError, TypeError):
        return {
            'statusCode': 400,
            "headers": {
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Headers": "Content-Type",
                "Access-Control-Allow-Methods": "OPTIONS,POST"
            },
            'body': json.dumps({'error': "Bad Request: 'date' is missing or invalid in the request body."})
        }
    except Exception as e:
        print(f"Error: {str(e)}") # Log the full error for debugging
        return {
            'statusCode': 500,
            "headers": {
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Headers": "Content-Type",
                "Access-Control-Allow-Methods": "OPTIONS,POST"
            },
            'body': json.dumps({'error': f"Internal Server Error: {str(e)}"})
        }