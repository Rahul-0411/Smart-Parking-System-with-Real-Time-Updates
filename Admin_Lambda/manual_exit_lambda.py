import json
import os
import boto3
from botocore.exceptions import ClientError
from datetime import datetime, timezone
from admin_module.manual_override_util import prepare_manual_exit
from admin_module.logging_util import create_admin_log
import uuid 
# Initialize the DynamoDB client once for reuse
dynamodb = boto3.resource('dynamodb')

def lambda_handler(event, context):
    """
    Handles the manual exit of a vehicle from a parking slot.
    """
    try:
        # Get table names from environment variables
        table_name = os.environ['DYNAMODB_TABLE_NAME']
        table = dynamodb.Table(table_name)
        logs_table = dynamodb.Table('ParkingHistory')
        
        # Parse request body
        body = json.loads(event.get('body', '{}'))
        parking_id = body['parking_id']
        
        # Step 1: Fetch current slot data from DynamoDB
        response = table.get_item(Key={'parking_id': parking_id})
        slot_data = response.get('Item')
        
        # Step 2: Call core logic to validate and prepare update parameters
        update_params = prepare_manual_exit(slot_data)
        
        # Step 3: Execute the update in DynamoDB
        table.update_item(
            Key={'parking_id': parking_id},
            **update_params
        )
        if slot_data:
            exit_dt = datetime.now(timezone.utc)
            exit_timestamp = exit_dt.isoformat()
            date = exit_dt.strftime('%Y-%m-%d')
            
            duration_minutes = None
            entry_timestamp_str = slot_data.get('entry_timestamp')
            if entry_timestamp_str:
                duration = exit_dt - datetime.fromisoformat(entry_timestamp_str)
                duration_minutes = int(duration.total_seconds() / 60)

            log_item = {
                'session_id': str(uuid.uuid4()),
                'date': date,
                'exit_timestamp': exit_timestamp,
                'area_id': slot_data.get('area_number'),
                'vehicle_id': slot_data.get('vehicle_id'),
                'parking_id': parking_id,
                'entry_timestamp': entry_timestamp_str,
                'duration_minutes': duration_minutes,
                'floor_number': slot_data.get('floor_number'),
                'email': slot_data.get('email')
            }
            
            # Remove any keys with None values to prevent DynamoDB errors
            final_log_item = {k: v for k, v in log_item.items() if v is not None}
            logs_table.put_item(Item=final_log_item)
        
        # Step 4: Log the admin action
        # We use the vehicle_id from the data we fetched *before* the update
        log_details = {
            'parking_id': parking_id,
            'vehicle_id': slot_data.get('vehicle_id', 'N/A') 
        }
        create_admin_log(action="ManualExit", details=log_details)

        return {
            'statusCode': 200,
            "headers": {
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Headers": "Content-Type",
                "Access-Control-Allow-Methods": "OPTIONS,POST"
            },
            'body': json.dumps({'message': f"Slot '{parking_id}' successfully vacated."})
        }
        
    except (KeyError, TypeError) as e:
        return {'statusCode': 400,
            "headers": {
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Headers": "Content-Type",
                "Access-Control-Allow-Methods": "OPTIONS,POST"
            },
            'body': json.dumps({'error': f"Bad Request: Missing or invalid parameter - {str(e)}"}) }
    except ValueError as e: # Catches validation errors from prepare_manual_exit
        return {'statusCode': 409,
            "headers": {
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Headers": "Content-Type",
                "Access-Control-Allow-Methods": "OPTIONS,POST"
            },
            'body': json.dumps({'error': f"Conflict: {str(e)}"}) }
    except ClientError as e:
        return {'statusCode': 500,
            "headers": {
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Headers": "Content-Type",
                "Access-Control-Allow-Methods": "OPTIONS,POST"
            },
            'body': json.dumps({'error': f"DynamoDB Error: {e.response['Error']['Message']}"}) }
    except Exception as e:
        return {'statusCode': 500,
            "headers": {
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Headers": "Content-Type",
                "Access-Control-Allow-Methods": "OPTIONS,POST"
            },
            'body': json.dumps({'error': f"Internal Server Error: {str(e)}"}) }