import json
import boto3
import os
from datetime import datetime, timezone

# --- Configuration ---
AWS_REGION = os.getenv("AWS_REGION", "ap-south-1")

SNS_TOPIC_ARN = "arn:aws:sns:ap-south-1:180651458429:parking-slot-notifications"

sns_client = boto3.client('sns', region_name=AWS_REGION)

def lambda_handler(event, context):
    print(f"Processing {len(event['Records'])} records from DynamoDB stream.")
    for record in event['Records']:
        try:
            if record['eventName'] == 'MODIFY' or record['eventName'] == 'INSERT':
                new_image = record['dynamodb'].get('NewImage')
                old_image = record['dynamodb'].get('OldImage') # May be None for INSERT

                if not new_image:
                    continue # Skip if no new image data

                new_status = new_image.get('status', {}).get('S')
                old_status = old_image.get('status', {}).get('S') if old_image else None

                parking_id = new_image.get('parking_id', {}).get('S')
                area_number = int(new_image.get('area_number', {}).get('N')) # Convert to int
                floor_number = int(new_image.get('floor_number', {}).get('N')) # Convert to int

                if new_status == 'empty' and (old_status != 'empty' or old_status is None):
                    message = (
                        f"🎉 Parking Alert! A slot is now FREE! 🎉\n\n"
                        f"Slot ID: {parking_id}\n"
                        f"Area: {area_number}\n"
                        f"Floor: {floor_number}\n\n"
                        f"Visit our app/website quickly to grab it!\n"
                        f"This message was sent to all subscribers for Area {area_number}, Floor {floor_number}."
                    )
                    subject = f"Parking Slot FREE: Area {area_number}, Floor {floor_number}"
                    
                    sns_client.publish(
                        TopicArn=SNS_TOPIC_ARN,
                        Message=message,
                        Subject=subject,
                        MessageAttributes={
                            'area_number':   {'DataType': 'String', 'StringValue': str(area_number)},
                            'floor_number':  {'DataType': 'String', 'StringValue': str(floor_number)},
                            'status_change': {'DataType': 'String', 'StringValue': 'became_empty'}
                        }
                    )
                    print(f"Published notification for {parking_id} becoming empty (Area {area_number}, Floor {floor_number}).")

            

        except Exception as e:
            print(f"Error processing record: {record}. Error: {e}")

    return {'statusCode': 200, 'body': 'Processed DynamoDB stream records.'}

