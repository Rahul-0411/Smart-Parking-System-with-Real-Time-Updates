import json
import boto3
from datetime import datetime, timedelta, timezone
import uuid
from boto3.dynamodb.conditions import Attr
import os 


dynamodb = boto3.resource('dynamodb')
parking_table = dynamodb.Table('ParkingSlotDatabase')
logs_table = dynamodb.Table('ParkingHistory')
ses_client = boto3.client('ses')

def lambda_handler(event, context):
    # Parse input
    vehicle_id = event.get('vehicle_id')
    event_type = event.get('event')
    expected_time = event.get('expected')
    area = event.get('area')
    floor = event.get('floor')
    email = event.get('email')
    
    try:
        if event_type not in ['entry', 'exit']:
            
            return {
                'statusCode': 400,
                'body': json.dumps({'error': 'Invalid event type. Use "entry" or "exit"'})
            }
            
        if event_type == 'entry':
            if not (vehicle_id and expected_time is not None and area and email):
                return {
                    'statusCode': 400,
                    'body': json.dumps({'error': 'vehicle_id, expected_time (in minutes), area, and email are required for entry'})
                }
            try:
                expected_minutes = int(expected_time)
            except ValueError:
                return {
                    'statusCode': 400,
                    'body': json.dumps({'error': 'expected_time must be a number (minutes)'})
                }
            return handle_entry(vehicle_id, expected_minutes, area, floor, email)
        else:
            if not vehicle_id:
                return {
                    'statusCode': 400,
                    'body': json.dumps({'error': 'vehicle_id is required for exit'})
                }
            return handle_exit(vehicle_id)
            
    except Exception as e:
        
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }


def handle_entry(vehicle_id, expected_minutes, area, floor, email):

    response = parking_table.query(
        IndexName='vehicle_id-index',
        KeyConditionExpression='vehicle_id = :vehicle_id',
        FilterExpression='#status = :occupied',
        ExpressionAttributeNames={'#status': 'status'},
        ExpressionAttributeValues={':vehicle_id': vehicle_id, ':occupied': 'occupied'}
    )

    
    if response['Items']:
        return {
            'statusCode': 409, 
            'body': json.dumps({'error': f'Vehicle {vehicle_id} is already parked.'})
        }

    now_utc = datetime.now(timezone.utc) 
    entry_timestamp = now_utc.isoformat()
    expected_exit_time = (now_utc + timedelta(minutes=expected_minutes)).isoformat()
    
    filter_expression = 'area_number = :area AND #s = :status'
    expression_values = {':area': int(area), ':status': 'empty'}
    expression_names = {'#s': 'status'}
    
    if floor:
        filter_expression += ' AND floor_number = :floor'
        expression_values[':floor'] = int(floor)
    
    response = parking_table.scan(
        FilterExpression=filter_expression,
        ExpressionAttributeValues=expression_values,
        ExpressionAttributeNames=expression_names
    )
    
    if not response['Items']:
        
        return {
            'statusCode': 400,
            'body': json.dumps({'error': f'No available parking slots in area {area}' + (f' floor {floor}' if floor else '')})
        }
    
    slots = sorted(response['Items'], key=lambda x: (int(x['floor_number']), int(x['slot_number'])))
    selected_slot = slots[0]
    parking_id = selected_slot['parking_id']

    # CHANGED: Added entry_timestamp to the update expression
    try:
        parking_table.update_item(
            Key={'parking_id': parking_id},
            UpdateExpression='SET #status = :occupied, vehicle_id = :vehicle_id, expected_time = :expected_time, entry_timestamp = :entry_ts, email = :email',
            # ADD this new line
            ConditionExpression='#status = :empty',
            ExpressionAttributeNames={'#status': 'status'},
            ExpressionAttributeValues={
                ':occupied': 'occupied',
                ':vehicle_id': vehicle_id,
                ':expected_time': expected_exit_time,
                ':entry_ts': entry_timestamp,
                ':email': email,
                ':empty': 'empty'
            }
        )
        
        try:
            ses_client.verify_email_identity(
                EmailAddress=email
            )
            print(f"Successfully sent SES verification request to {email}.")
            message_for_user = (
                f"Vehicle {vehicle_id} assigned to slot {parking_id}. "
                f"A verification email has been sent to {email} to enable notifications."
            )
        except Exception as e:
            print(f"Warning: Parking was assigned, but failed to initiate SES verification for {email}: {e}")
            message_for_user = f"Vehicle {vehicle_id} assigned to slot {parking_id}."
            
    except dynamodb.meta.client.exceptions.ConditionalCheckFailedException:
        
        return {
            'statusCode': 409, 
            'body': json.dumps({'error': 'Sorry, that parking spot was just taken. Please try again.'})
        }
    
    return {
        'statusCode': 200,
        'body': json.dumps({
            'message': f'Vehicle {vehicle_id} assigned to slot {parking_id}', 'parking_id': parking_id,
            'entry_timestamp': entry_timestamp, 'expected_exit_time': expected_exit_time
        })
    }

def handle_exit(vehicle_id):
    
    response = parking_table.query(
        IndexName='vehicle_id-index',
        KeyConditionExpression='vehicle_id = :vehicle_id',
        FilterExpression='#status = :occupied',
        ExpressionAttributeNames={'#status': 'status'},
        ExpressionAttributeValues={':vehicle_id': vehicle_id, ':occupied': 'occupied'}
    )
    
    if not response['Items']:
        
        return {
            'statusCode': 404,
            'body': json.dumps({'error': f'No slot found for vehicle {vehicle_id}'})
        }

    slot_info = response['Items'][0]
    parking_id = slot_info['parking_id']
    
    exit_dt = datetime.now(timezone.utc)
    exit_timestamp = exit_dt.isoformat()
    date = exit_dt.strftime('%Y-%m-%d')
    
    
    duration_minutes = None
    entry_timestamp_str = slot_info.get('entry_timestamp')
    if entry_timestamp_str:
        duration = datetime.fromisoformat(exit_timestamp) - datetime.fromisoformat(entry_timestamp_str)
        duration_minutes = int(duration.total_seconds() / 60)

    
    parking_table.update_item(
        Key={'parking_id': parking_id},
        UpdateExpression='SET #status = :empty REMOVE vehicle_id, expected_time, entry_timestamp, email',
        ExpressionAttributeNames={'#status': 'status'},
        ExpressionAttributeValues={':empty': 'empty'}
    )
    

    
    log_item = {
        'session_id': str(uuid.uuid4()),          
        'date': date,                   
        'exit_timestamp': exit_timestamp,         
        'area_id': slot_info.get('area_number'),
        'vehicle_id': vehicle_id,
        'parking_id': parking_id,
        'entry_timestamp': slot_info.get('entry_timestamp'),
        'duration_minutes': duration_minutes,
        'floor_number': slot_info.get('floor_number'),
        'email': slot_info.get('email') # Make sure to include email
    }

    final_log_item = {k: v for k, v in log_item.items() if v is not None}

    logs_table.put_item(Item=final_log_item)
    
    return {
        'statusCode': 200,
        'body': json.dumps({
            'message': f'Vehicle {vehicle_id} exited from slot {parking_id}', 'parking_id': parking_id,
            'exit_timestamp': exit_timestamp
        })
    }
