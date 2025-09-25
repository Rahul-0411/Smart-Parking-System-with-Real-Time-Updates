import json
import os
import boto3
from datetime import datetime, timezone
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

def format_overdue_time(overdue_delta):
    """Formats a timedelta into a human-readable string like '1h 25m overdue'."""
    total_minutes = int(overdue_delta.total_seconds() / 60)
    if total_minutes < 1:
        return "Less than a minute overdue"
        
    hours, minutes = divmod(total_minutes, 60)
    
    if hours > 0:
        return f"{hours}h {minutes}m overdue"
    else:
        return f"{minutes}m overdue"

def lambda_handler(event, context):
    """
    Scans for occupied slots with an expected exit time in the past and returns them as alerts.
    """
    try:
        table_name = os.environ['DYNAMODB_TABLE_NAME']
        table = dynamodb.Table(table_name)
        
        # --- MODIFIED LOGIC: Using Scan instead of Query ---
        # A scan reads the entire table, which can be inefficient for large tables.
        scan_kwargs = {
            'FilterExpression': Key('status').eq('occupied')
        }
        
        occupied_slots = []
        done = False
        start_key = None
        while not done:
            if start_key:
                scan_kwargs['ExclusiveStartKey'] = start_key
            response = table.scan(**scan_kwargs)
            occupied_slots.extend(response.get('Items', []))
            start_key = response.get('LastEvaluatedKey', None)
            done = start_key is None
        # ----------------------------------------------------
        
        alerts = []
        now = datetime.now(timezone.utc)
        
        for slot in occupied_slots:
            expected_time_str = slot.get('expected_time')
            
            if not expected_time_str:
                continue

            expected_time = datetime.fromisoformat(expected_time_str)
            
            if now > expected_time:
                overdue_delta = now - expected_time
                alert = {
                    "parking_id": slot.get('parking_id'),
                    "vehicle_id": slot.get('vehicle_id'),
                    "email": slot.get('email'),
                    "overdue": format_overdue_time(overdue_delta)
                }
                alerts.append(alert)
        
        create_admin_log(action="ViewAlerts", details={"alerts_found": len(alerts)})
        
        return {
            'statusCode': 200,
            "headers": {
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Headers": "Content-Type",
                "Access-Control-Allow-Methods": "OPTIONS,GET"
            },
            'body': json.dumps(alerts, default=decimal_serializer)
        }
        
    except Exception as e:
        print(f"Error: {str(e)}")
        return {
            'statusCode': 500,
            "headers": {
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Headers": "Content-Type",
                "Access-Control-Allow-Methods": "OPTIONS,GET"
            },
            'body': json.dumps({'error': f"Internal Server Error: {str(e)}"})
        }