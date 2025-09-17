import json
from decimal import Decimal
import slot_utils

class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return int(obj)
        return super(DecimalEncoder, self).default(obj)

def create_response(status_code, body):
    return {
        'statusCode': status_code,
        'headers': {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'GET, OPTIONS',
            'Access-Control-Allow-Headers': 'Content-Type'
        },
        'body': json.dumps(body, cls=DecimalEncoder)
    }

def lambda_handler(event, context):
    try:
        params = event.get('queryStringParameters', {})
        area = params.get('area')
        floor = params.get('floor')

        if not area or not floor:
            return create_response(400, {'status': 'ERROR', 'message': 'Both "area" and "floor" query parameters are required.'})
        
        area_num = int(area)
        floor_num = int(floor)
        
        
        result = slot_utils.get_floor_status(area_num, floor_num)
        
        return create_response(200, result)

    except (ValueError, TypeError):
        return create_response(400, {'status': 'ERROR', 'message': '"area" and "floor" must be valid numbers.'})
    except Exception as e:
        print(f"Error in lambda_handler: {e}")
        return create_response(500, {'status': 'ERROR', 'message': 'An internal error occurred.'})
