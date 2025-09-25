import json
import os
import boto3
from botocore.exceptions import ClientError

from admin_module.logging_util import create_admin_log

# Initialize DynamoDB client
dynamodb = boto3.resource('dynamodb')

def lambda_handler(event, context):
    """
    Handles POST requests to fetch the status of a single parking slot.
    Fixes the reserved keyword issue for 'status'.
    Input: {"parking_id": "A1F1S3"}
    """
    try:
        # Get table name from environment variables
        table_name = os.environ['DYNAMODB_TABLE_NAME']
        table = dynamodb.Table(table_name)

        # Get parking_id from the request body
        body = json.loads(event.get('body', '{}'))
        parking_id = body['parking_id']

        # Use ExpressionAttributeNames to handle the reserved keyword 'status'
        response = table.get_item(
            Key={'parking_id': parking_id},
            ProjectionExpression="#s",  # Use a placeholder for the attribute name
            ExpressionAttributeNames={"#s": "status"}  # Map the placeholder to the actual name
        )

        item = response.get('Item')
        if not item:
            return {
                'statusCode': 404,
                "headers": {
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Headers": "Content-Type",
                "Access-Control-Allow-Methods": "OPTIONS,POST"
            },
                'body': json.dumps({'error': f"Parking slot '{parking_id}' not found."})
            }

        # --- ADMIN LOGGING ---
        create_admin_log(action  = "ViewSlotStatus", details = {"Parking ID": parking_id, "Result": item["status"]})
        # ---------------------

        return {
            'statusCode': 200,
            "headers": {
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Headers": "Content-Type",
                "Access-Control-Allow-Methods": "OPTIONS,POST"
            },
            'body': json.dumps(item) # DynamoDB correctly returns {"status": "some_value"}
        }

    except (KeyError, TypeError):
        return {
            'statusCode': 400,
            "headers": {
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Headers": "Content-Type",
                "Access-Control-Allow-Methods": "OPTIONS,POST"
            },
            'body': json.dumps({'error': "Bad Request: 'parking_id' is missing or invalid in the request body."})
        }
    except ClientError as e:
        return {
            'statusCode': 500,
            "headers": {
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Headers": "Content-Type",
                "Access-Control-Allow-Methods": "OPTIONS,POST"
            },
            'body': json.dumps({'error': f"DynamoDB Error: {e.response['Error']['Message']}"})
        }
    except Exception as e:
        return {
            'statusCode': 500,
            "headers": {
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Headers": "Content-Type",
                "Access-Control-Allow-Methods": "OPTIONS,POST"
            },
            'body': json.dumps({'error': f"Internal Server Error: {str(e)}"})
        }