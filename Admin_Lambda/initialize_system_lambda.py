import json
import os
from botocore.exceptions import ClientError
from admin_module.parking_crud import _initialize_system, _create_table_if_not_exists
from admin_module.logging_util import create_admin_log

def lambda_handler(event, context):
    """
    Description:
        Handles an API Gateway event to initialize the entire parking system.
        It ensures the DynamoDB table exists (creating it if necessary) and then
        populates it with slots based on the provided layout.

    Expected event input format:
        The event body must be a JSON string containing a 'parking_layout' key.
        {
            "body": "{ \\"parking_layout\\": [ {\\"area\\": 1, \\"floors\\": 5, \\"slots_per_floor\\": 20} ] }"
        }
    """
    try:
        table_name = os.environ['DYNAMODB_TABLE_NAME']
        _create_table_if_not_exists(table_name)

        body = json.loads(event.get('body', '{}'))
        parking_layout = body.get('parking_layout')
        if not parking_layout or not isinstance(parking_layout, list):
            raise ValueError("Input must contain a 'parking_layout' list.")

        created_slots = _initialize_system(table_name, parking_layout)

        # --- ADMIN LOGGING ---
        log_details = {
            "created_count": len(created_slots),
            "input_layout": parking_layout
        }
        create_admin_log(action="SystemInitialize", details=log_details)
        # ---------------------

        return {
            'statusCode': 200,
            "headers": {
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Headers": "Content-Type",
                "Access-Control-Allow-Methods": "OPTIONS,POST"
            },
            'body': json.dumps({
                'message': f"System initialized successfully. {len(created_slots)} slots created.",
                'created_slots': created_slots
            })
        }
    except (ValueError, TypeError) as e:
        return {'statusCode': 400, "headers": {
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Headers": "Content-Type",
                "Access-Control-Allow-Methods": "OPTIONS,POST"
            },
        'body': json.dumps({'error': f"Bad Request: {str(e)}"}) }
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