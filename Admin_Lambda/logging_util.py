import boto3
import os
import datetime
from botocore.exceptions import ClientError

# Taking the table name from an environment variable.
# Must set `ADMIN_LOG_TABLE` in respective Lambda function's configuration.
LOG_TABLE_NAME = os.environ.get('ADMIN_LOG_TABLE')
dynamodb = boto3.resource('dynamodb')

def create_admin_log(action: str, details: dict):
    """
    Creates a log entry in the Admin Logs DynamoDB table.

    This function constructs a log item with the current date and timestamp
    and writes it to the DynamoDB table specified in the 'ADMIN_LOG_TABLE'
    environment variable. It's designed to be used as a centralized
    logging utility, ideally from a Lambda Layer.

    Args:
        action (str): A short string describing the admin action performed.
                      e.g., "SystemReset", "AddSlot".
        details (dict): A dictionary containing relevant data about the
                        event. This is stored as a DynamoDB map.

    Raises:
        KeyError: If the 'ADMIN_LOG_TABLE' environment variable is not set.
        ClientError: If the DynamoDB put_item call fails for any reason.

    Example Usage in a Lambda function:
        from logging_util import create_admin_log

        def handler(event, context):
            # ... main logic for the function ...
            
            log_details = {"slotId": "A-07", "previousState": "NonOperational"}
            create_admin_log(action="FlagSlotOperational", details=log_details)
            
            return {"statusCode": 200, "body": "Slot status updated."}
    """
    if not LOG_TABLE_NAME:
        print("FATAL: ADMIN_LOG_TABLE environment variable not set.")
        # Failing fast if configuration is missing
        raise KeyError("ADMIN_LOG_TABLE environment variable not set.")

    table = dynamodb.Table(LOG_TABLE_NAME)
    
    # Using UTC for all timestamps to avoid timezone issues
    now_utc = datetime.datetime.now(datetime.timezone.utc)
    
    try:
        item = {
            'logDate'       : now_utc.strftime('%Y-%m-%d'),   # Partition Key
            'eventTimestamp': now_utc.isoformat(),            # Sort Key
            'action'        : action,
            'details'       : details
        }
        
        table.put_item(Item=item)
        print(f"Log successful for action: {action}")

    except ClientError as e:
        # Catching potential AWS errors, like permission issues
        print(f"ERROR: Could not log to DynamoDB. {e.response['Error']['Message']}")
        # Re-raising the exception to let the caller know the log failed
        raise