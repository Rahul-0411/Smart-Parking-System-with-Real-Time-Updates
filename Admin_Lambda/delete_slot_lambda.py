import json
import os
from botocore.exceptions import ClientError
from admin_module.parking_crud import _delete_slots_by_id
from admin_module.logging_util import create_admin_log

def lambda_handler(event, context):
    """
    Description:
        Handles an API Gateway event to delete one or more specific parking
        slots using a list of their unique parking_ids.

    Expected event input format:
        The event body must be a JSON string containing a 'parking_ids' list.
        {
            "body": "{ \\"parking_ids\\": [\\"A1F1S1\\", \\"A1F1S2\\"] }"
        }
    """
    try:
        table_name = os.environ['DYNAMODB_TABLE_NAME']
        body = json.loads(event.get('body', '{}'))

        parking_ids = body.get('parking_ids')
        if not parking_ids or not isinstance(parking_ids, list):
            raise ValueError("Input must contain a 'parking_ids' list.")

        deleted_count = _delete_slots_by_id(table_name, parking_ids)

        # --- ADMIN LOGGING ---
        log_details = {
            "deleted_ids": parking_ids,
            "deleted_count": deleted_count
        }
        create_admin_log(action="DeleteSlots", details=log_details)
        # ---------------------

        return {
            'statusCode': 200,
            "headers": {
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Headers": "Content-Type",
                "Access-Control-Allow-Methods": "OPTIONS,POST"
            },
            'body': json.dumps({
                'message': f"Delete request processed. {deleted_count} slots marked for deletion.",
                'deleted_ids': parking_ids
            })
        }
    except (ValueError, TypeError) as e:
        return {
            'statusCode': 400,
            "headers": {
                    "Access-Control-Allow-Origin": "*",
                    "Access-Control-Allow-Headers": "Content-Type",
                    "Access-Control-Allow-Methods": "OPTIONS,POST"
                },
            'body': json.dumps({'error': f"Bad Request: {str(e)}"}) }
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