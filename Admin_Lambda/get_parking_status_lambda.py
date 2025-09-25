import os
import json
import boto3
from decimal import Decimal


try:
    PARKING_SLOT_TABLE_NAME = os.environ['DYNAMODB_TABLE_NAME']
    dynamodb = boto3.resource('dynamodb')
    PARKING_SLOT_TABLE = dynamodb.Table(PARKING_SLOT_TABLE_NAME)
except KeyError as e:
    # This will cause the function to fail cleanly if an env var is missing
    raise RuntimeError(f"FATAL: Environment variable {e} not set.") from e


class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, Decimal):
            # Convert Decimal to a standard number type
            if o % 1 == 0:
                return int(o)
            else:
                return float(o)
        return super(DecimalEncoder, self).default(o)


def lambda_handler(event, context):
    """
    Fetches all parking slots, calculates overall statistics, and returns
    the data in the format expected by the front-end dashboard.
    """
    try:
        
        response = PARKING_SLOT_TABLE.scan()
        slot_items = response.get('Items', [])
        
        # Handle pagination if the table is large
        while 'LastEvaluatedKey' in response:
            response = PARKING_SLOT_TABLE.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
            slot_items.extend(response.get('Items', []))

        # --- 2. Calculate Statistics ---
        total_spots = len(slot_items)
        occupied_spots = 0
        
        # Count occupied spots by iterating through the fetched items
        for slot in slot_items:
            if slot.get('status') == 'occupied':
                occupied_spots += 1
        
        available_spots = total_spots - occupied_spots
        
        # Calculate occupancy rate, handling the case of zero total spots
        occupancy_rate = (occupied_spots / total_spots * 100) if total_spots > 0 else 0

        # --- 3. Assemble the final data structure ---
        # This structure now matches exactly what the JavaScript expects
        final_data = {
            'total_spots': total_spots,
            'occupied_spots': occupied_spots,
            'available_spots': available_spots,
            'occupancy_rate': occupancy_rate,
            'slots': slot_items  # Include the full list of individual slots
        }

        # --- 4. Return the successful response ---
        return {
            "statusCode": 200,
            "headers": {
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Headers": "Content-Type",
                "Access-Control-Allow-Methods": "OPTIONS,GET"
            },
            # The body is now the correctly structured final_data object
            "body": json.dumps(final_data, cls=DecimalEncoder)
        }

    except Exception as e:
        # Log the full error for debugging purposes
        print(f"An unexpected error occurred: {str(e)}")
        
        # Return a generic error response
        return {
            "statusCode": 500,
            "headers": {
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Headers": "Content-Type",
                "Access-Control-Allow-Methods": "OPTIONS,GET"
            },
            "body": json.dumps({"error": "An internal server error occurred."})
        }