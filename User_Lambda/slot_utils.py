import os
import boto3
from boto3.dynamodb.conditions import Key
from datetime import datetime, timezone
import random 


GSI_NAME = "status-area_number-index"


dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('ParkingSlotDatabase')


def get_floor_status(area_number, floor_number):
    """
    Finds and returns all available slots on a given floor.
    If the floor is full, it returns the top 3 upcoming slots.
    This function READS data only; it does not allocate.
    """
    try:
        response = table.query(
            IndexName=GSI_NAME,
            KeyConditionExpression=Key('status').eq('empty') & Key('area_number').eq(area_number),
            FilterExpression=Key('floor_number').eq(floor_number)
        )
        available_slots = sorted(response.get('Items', []), key=lambda x: x['slot_number'])
        
        if available_slots:
            return {
                'status': 'AVAILABLE',
                'message': f"{len(available_slots)} slots found on Floor {floor_number}.",
                'slots': available_slots
            }
        
        # 3. If no 'empty' slots are found, the floor is full. Find the waiting time.
        else:
            return find_waiting_time(area_number, floor_number)

    except Exception as e:
        print(f"Error in get_floor_status: {e}")
        return {'status': 'ERROR', 'message': 'An internal error occurred.'}

def find_waiting_time(area_number, floor_number):
    """
    Finds the top 3 soonest-to-be-vacant slots on a full floor.
    .
    """
    try:
        response = table.scan(
            FilterExpression=Key('area_number').eq(area_number) & Key('floor_number').eq(floor_number) & Key('status').eq('occupied')
        )
        
        upcoming_slots = []
        now_utc = datetime.now(timezone.utc)
        
        for slot in response.get('Items', []):
            if 'expected_time' in slot:
                exit_time = datetime.fromisoformat(slot['expected_time']).astimezone(timezone.utc)
                wait_delta = exit_time - now_utc
                if wait_delta.total_seconds() > 0:
                    upcoming_slots.append({
                        'parking_id': slot['parking_id'],
                        'wait_minutes': int(wait_delta.total_seconds() / 60)
                    })
        
        top_3_slots = sorted(upcoming_slots, key=lambda x: x['wait_minutes'])[:3]
        
        return {
            'status': 'FULL',
            'message': f"Floor {floor_number} in Area {area_number} is full.",
            'upcoming_slots': top_3_slots
        }
    except Exception as e:
        print(f"Error in find_waiting_time: {e}")
        return {'status': 'ERROR', 'message': 'Could not calculate waiting time.'}

