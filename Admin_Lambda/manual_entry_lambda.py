import json
import os
import boto3
from decimal import Decimal
from botocore.exceptions import ClientError

from admin_module.manual_override_util import prepare_manual_entry
from admin_module.logging_util import create_admin_log

dynamodb = boto3.resource('dynamodb')

def decimal_serializer(obj):
    """Custom JSON serializer for Decimal objects."""
    if isinstance(obj, Decimal):
        # Convert Decimal to int or float
        if obj % 1 == 0:
            return int(obj)
        else:
            return float(obj)
    raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")


def lambda_handler(event, context):
    """
    Handles the manual entry of a vehicle into a parking slot.
    """
    try:
        table_name = os.environ['DYNAMODB_TABLE_NAME']
        table = dynamodb.Table(table_name)
        
        body = json.loads(event.get('body', '{}'))
        parking_id = body['parking_id']
        email = body['email']
        vehicle_id = body['vehicle_id']
        expected_time_minutes = body['expected_time_minutes']
        
        # Step 1: Fetch current slot data
        response = table.get_item(Key={'parking_id': parking_id})
        slot_data = response.get('Item')

        # Step 2: Prepare update parameters
        update_params = prepare_manual_entry(
            slot_data, email, vehicle_id, expected_time_minutes
        )
        
        # Step 3: Execute the update
        updated_item = table.update_item(
            Key={'parking_id': parking_id},
            **update_params
        )
        
        # Step 4: Log the admin action
        log_details = {
            'parking_id': parking_id,
            'vehicle_id': vehicle_id,
            'email': email
        }
        create_admin_log(action="ManualEntry", details=log_details)

        return {
            'statusCode': 200,
            "headers": {
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Headers": "Content-Type",
                "Access-Control-Allow-Methods": "OPTIONS,POST"
            },
            'body': json.dumps(updated_item.get('Attributes'), default=decimal_serializer)
        }
        
    except (KeyError, TypeError) as e:
        return {
            'statusCode': 400,
            "headers": {
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Headers": "Content-Type",
                "Access-Control-Allow-Methods": "OPTIONS,POST"
            },
            'body': json.dumps({'error': f"Bad Request: Missing or invalid parameter - {str(e)}"}) }
    except ValueError as e:
        return {
            'statusCode': 409,
            "headers": {
                    "Access-Control-Allow-Origin": "*",
                    "Access-Control-Allow-Headers": "Content-Type",
                    "Access-Control-Allow-Methods": "OPTIONS,POST"
                },
            'body': json.dumps({'error': f"Conflict: {str(e)}"}) }
    except ClientError as e:
        return {
            'statusCode': 500,
            "headers": {
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Headers": "Content-Type",
                "Access-Control-Allow-Methods": "OPTIONS,POST"
            },
            'body': json.dumps({'error': f"DynamoDB Error: {e.response['Error']['Message']}"}) }
    except Exception as e:
        return {
            'statusCode': 500,
            "headers": {
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Headers": "Content-Type",
                "Access-Control-Allow-Methods": "OPTIONS,POST"
            },
            'body': json.dumps({'error': f"Internal Server Error: {str(e)}"}) }