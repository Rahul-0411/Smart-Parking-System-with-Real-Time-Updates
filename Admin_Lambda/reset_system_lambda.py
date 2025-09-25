import json
import os
from admin_module.parking_crud import _reset_all_slots
from admin_module.logging_util import create_admin_log
from botocore.exceptions import ClientError

def lambda_handler(event, context):
    """
    Description:
        Handles an API Gateway event to delete all items from the parking table,
        effectively resetting the system to an empty state without deleting the
        table itself.

    Expected event input format:
        No specific input is required in the event body.
        {
            "body": "{}"
        }
    """
    try:
        table_name = os.environ['DYNAMODB_TABLE_NAME']

        deleted_count = _reset_all_slots(table_name)

        # --- ADMIN LOGGING ---
        log_details = {
            "deleted_total": deleted_count,
            "reset_table": table_name
        }
        create_admin_log(action="SystemReset", details=log_details)
        # ---------------------

        return {
            'statusCode': 200,
            "headers": {
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Headers": "Content-Type",
                "Access-Control-Allow-Methods": "OPTIONS,POST"
            },
            'body': json.dumps({
                'message': "Parking system reset successfully.",
                'deleted_total': deleted_count
            })
        }
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