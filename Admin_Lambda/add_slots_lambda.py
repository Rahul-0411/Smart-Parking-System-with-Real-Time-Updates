import os
import json
from decimal import Decimal
from botocore.exceptions import ClientError
from admin_module.parking_crud import _add_new_slots
from admin_module.logging_util import create_admin_log

class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return int(obj)
        return super(DecimalEncoder, self).default(obj)

def lambda_handler(event, context):
    """
    Description:
        Handles an API Gateway event to add a specified number of new parking
        slots to a particular floor within a parking area.

    Expected event input format:
        The event body must be a JSON string with 'area_number', 'floor_number',
        and 'new_slots'.
        {
            "body": "{ \\"area_number\\": 1, \\"floor_number\\": 3, \\"new_slots\\": 5 }"
        }
    """
    try:
        table_name = os.environ['DYNAMODB_TABLE_NAME']
        body = json.loads(event.get('body', '{}'))

        area = int(body['area_number'])
        floor = int(body['floor_number'])
        new_slots_count = int(body['new_slots'])

        added_slots = _add_new_slots(table_name, area, floor, new_slots_count)

        # --- ADMIN LOGGING ---
        log_details = {
            "area_number": area,
            "floor_number": floor,
            "added_count": len(added_slots),
            "added_slot_ids": [slot['parking_id'] for slot in added_slots]
        }
        create_admin_log(action="AddSlots", details=log_details)
        # ---------------------

        return {
            'statusCode': 200,
            "headers": {
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Headers": "Content-Type",
                "Access-Control-Allow-Methods": "OPTIONS,POST"
            },
            'body': json.dumps({
                'message': f"Successfully added {len(added_slots)} new slots to Area {area}, Floor {floor}.",
                'added_slots': added_slots
            }, cls=DecimalEncoder) # Using custom encoder: Decimal->Int
        }
    except (ValueError, TypeError, KeyError) as e:
        return {
            'statusCode': 400,
            "headers": {
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Headers": "Content-Type",
                "Access-Control-Allow-Methods": "OPTIONS,POST"
            },
            'body': json.dumps({'error': f"Bad Request: Invalid input. {str(e)}"}) }
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