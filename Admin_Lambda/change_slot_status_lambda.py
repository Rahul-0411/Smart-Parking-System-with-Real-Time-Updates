import json
import os
import datetime
import boto3
from botocore.exceptions import ClientError
from admin_module.logging_util import create_admin_log

dynamodb = boto3.resource('dynamodb')
PARKING_SLOTS_TABLE_NAME = os.environ['DYNAMODB_TABLE_NAME']
ADMIN_LOGS_TABLE_NAME = os.environ['ADMIN_LOG_TABLE']
slots_table = dynamodb.Table(PARKING_SLOTS_TABLE_NAME)
logs_table = dynamodb.Table(ADMIN_LOGS_TABLE_NAME)

def lambda_handler(event, context):
    """
    Handles changing the status of parking slots to 'maintenance' or 'empty'.
    This function validates input, performs the change for multiple slots,
    and creates a single audit log for the administrative action.

    Expected event body:
    {
        "parking_ids": ["A1F1S1", "A1F1S2", "A3F2S3"],
        "status": "maintenance"  // or "empty"
    }
    """
    try:
        body = json.loads(event.get('body', '{}'))
        parking_ids = body.get('parking_ids')
        new_status = body.get('status')

        if not parking_ids or not isinstance(parking_ids, list):
            return {'statusCode': 400, 'body': json.dumps('Error: "parking_ids" must be a non-empty list.')}
            
        if new_status not in ['maintenance', 'empty']:
            return {'statusCode': 400, 'body': json.dumps('Error: "status" must be either "maintenance" or "empty".')}

        # Determining the action name based on the new status
        action_name = "SlotFlagUp" if new_status == 'maintenance' else "SlotFlagDown"

        for slot_id in parking_ids:
            slots_table.update_item(
                Key={
                    'parking_id': slot_id
                },
                UpdateExpression='SET #st = :s',  # Using placeholders for safety and clarity
                ExpressionAttributeNames={
                    '#st': 'status'  # Maps '#st' to the 'status' attribute name
                },
                ExpressionAttributeValues={
                    ':s': new_status  # Maps ':s' to the new status value
                }
            )
        
        # --- ADMIN LOGGING ---
        log_details = {
            "updated_slots": parking_ids,
            "new_status": new_status
        }
        create_admin_log(action=action_name, details=log_details)
        # ---------------------
        
        return {
            'statusCode': 200,
            "headers": {
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Headers": "Content-Type",
                "Access-Control-Allow-Methods": "OPTIONS,POST"
            },
            'body': json.dumps({
                'message': f"Successfully executed '{action_name}' for {len(parking_ids)} slots.",
                'updated_slots': parking_ids
            })
        }

    except ClientError as e:
        error_code = e.response['Error']['Code']
        error_message = e.response['Error']['Message']
        print(f"Boto3 ClientError: {error_code} - {error_message}")
        return {'statusCode': 500,
        "headers": {
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Headers": "Content-Type",
                "Access-Control-Allow-Methods": "OPTIONS,POST"
            },
        'body': json.dumps(f"Database error: {error_message}")}
    except Exception as e:
        print(f"Error: {e}")
        return {'statusCode': 500,
        "headers": {
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Headers": "Content-Type",
                "Access-Control-Allow-Methods": "OPTIONS,POST"
            },
        'body': json.dumps(f"An internal error occurred: {str(e)}")}