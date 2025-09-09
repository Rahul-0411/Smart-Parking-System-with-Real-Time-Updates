import os
import boto3
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key

# Initializing clients outside handlers for reuse
dynamodb = boto3.resource('dynamodb')
dynamodb_client = boto3.client('dynamodb')


def _create_table_if_not_exists(table_name: str) -> None:
    """
    Description:
        Checks if a DynamoDB table exists. If not, it creates one with the
        primary key ('parking_id') and the 'AreaFloorIndex' Global Secondary Index.
        It uses a waiter to handle race conditions and ensure the table is active.

    Args:
        table_name (str): The name of the DynamoDB table to check/create.
    """
    try:
        dynamodb_client.describe_table(TableName=table_name)
        print(f"Table '{table_name}' already exists.")
    except ClientError as e:
        if e.response['Error']['Code'] == 'ResourceNotFoundException':
            print(f"Table '{table_name}' does not exist. Creating it...")
            try:
                dynamodb_client.create_table(
                    TableName=table_name,
                    AttributeDefinitions=[
                        {'AttributeName': 'parking_id', 'AttributeType': 'S'},
                        {'AttributeName': 'area_number', 'AttributeType': 'N'},
                        {'AttributeName': 'floor_number', 'AttributeType': 'N'},
                    ],
                    KeySchema=[
                        {'AttributeName': 'parking_id', 'KeyType': 'HASH'}, # Partition Key
                    ],
                    GlobalSecondaryIndexes=[
                        {
                            'IndexName': 'AreaFloorIndex',
                            'KeySchema': [
                                {'AttributeName': 'area_number', 'KeyType': 'HASH'}, # GSI Partition Key
                                {'AttributeName': 'floor_number', 'KeyType': 'RANGE'}, # GSI Sort Key
                            ],
                            'Projection': {'ProjectionType': 'ALL'},
                        }
                    ],
                    BillingMode='PAY_PER_REQUEST'
                )
                print(f"Waiting for table '{table_name}' to become active...")
                waiter = dynamodb_client.get_waiter('table_exists')
                waiter.wait(TableName=table_name)
                print(f"Table '{table_name}' created and is now active.")
            except ClientError as ce:
                # Handling race condition where another process creates the table
                if ce.response['Error']['Code'] != 'ResourceInUseException':
                    raise
        else:
            raise


def _initialize_system(table_name: str, parking_layout: list) -> list:
    """
    Description:
        Core logic to initialize parking slots based on a layout with areas,
        floors, and slots. It performs detailed input validation and uses a
        batch_writer for efficient bulk insertion.

    Args:
        table_name (str): The name of the DynamoDB table.
        parking_layout (list): A list of dictionaries, where each dictionary defines
                            the configuration for a parking area.
                            e.g., [{'area': 1, 'floors': 10, 'slots_per_floor': 15}]

    Returns:
        list: A list of all the item dictionaries created and added to the table.

    Raises:
        ValueError: If the 'parking_layout' data is malformed or contains invalid values.
        ClientError: If there's an issue with DynamoDB operations.
    """
    table = dynamodb.Table(table_name)
    created_slots = []

    with table.batch_writer() as batch:
        for area_config in parking_layout:
            # --- Input Validation ---
            if not isinstance(area_config, dict) or not all(k in area_config for k in ['area', 'floors', 'slots_per_floor']):
                raise ValueError(f"Malformed area data entry: {area_config}. Expected {{'area': int, 'floors': int, 'slots_per_floor': int}}")

            area = area_config['area']
            floors = area_config['floors']
            slots_per_floor = area_config['slots_per_floor']

            if not isinstance(area, int) or area <= 0:
                raise ValueError(f"Invalid area number: {area}. Must be a positive integer.")
            if not isinstance(floors, int) or floors <= 0:
                raise ValueError(f"Invalid number of floors for area {area}: {floors}. Must be a positive integer.")
            if not isinstance(slots_per_floor, int) or slots_per_floor <= 0:
                raise ValueError(f"Invalid number of slots_per_floor for area {area}: {slots_per_floor}. Must be a positive integer.")
            
            # Item creation
            for floor in range(1, floors + 1):
                for slot in range(1, slots_per_floor + 1):
                    parking_id = f"A{area}F{floor}S{slot}"
                    item = {
                        'parking_id'   : parking_id,
                        'area_number'  : area,
                        'floor_number' : floor,
                        'slot_number'  : slot,
                        'status'       : 'empty'
                    }
                    batch.put_item(Item=item)
                    created_slots.append(item)

    return created_slots


# In parking_manager.py

from boto3.dynamodb.conditions import Key

def _add_new_slots(table_name: str, area: int, floor: int, new_slots_count: int) -> list:
    """
    Description:
        Adds a specified number of new slots to a given floor in a given area.
        It efficiently finds the last slot number on that floor by querying the
        'AreaFloorIndex' Global Secondary Index.

    Args:
        table_name (str): The name of the DynamoDB table.
        area (int): The area number where slots will be added.
        floor (int): The floor number where slots will be added.
        new_slots_count (int): The number of new slots to create.

    Returns:
        list: A list of the newly created slot item dictionaries.

    Raises:
        ValueError: If 'new_slots_count' is not a positive integer.
    """
    if new_slots_count <= 0:
        raise ValueError("Number of new slots must be a positive integer.")

    table = dynamodb.Table(table_name)

    # Querying the GSI to get all slots for the specified area and floor.
    # Only need the slot_number to find the max, so using ProjectionExpression
    # to make the read operation more efficient.
    response = table.query(
        IndexName='AreaFloorIndex',
        ProjectionExpression='slot_number',
        KeyConditionExpression=Key('area_number').eq(area) & Key('floor_number').eq(floor)
    )

    items = response.get('Items', [])

    # Finding the highest existing slot number from the returned items.
    # If no items are found for that floor, start from 0.
    max_existing_slot = 0
    if items:
        # This correctly finds the maximum value of the 'slot_number' attribute
        # from the list of all slots on that floor.
        max_existing_slot = max(item['slot_number'] for item in items)

    added_slots = []
    with table.batch_writer() as batch:
        # Starting numbering the new slots from max_existing_slot + 1
        for i in range(1, new_slots_count + 1):
            new_slot_number = max_existing_slot + i
            parking_id = f"A{area}F{floor}S{new_slot_number}"
            item = {
                'parking_id'   : parking_id,
                'area_number'  : area,
                'floor_number' : floor,
                'slot_number'  : new_slot_number,
                'status'       : 'empty'
            }
            batch.put_item(Item=item)
            added_slots.append(item)

    return added_slots


def _delete_slots_by_id(table_name: str, parking_id_list: list) -> int:
    """
    Description:
        Deletes one or more parking slots from the table using a list of parking_ids.
        This function uses a batch_writer for efficient bulk deletion.

    Args:
        table_name (str): The name of the DynamoDB table.
        parking_id_list (list): A list of strings, where each string is a
                                'parking_id' to be deleted.

    Returns:
        int: The number of items processed for deletion.

    Raises:
        ValueError: If the provided 'parking_id_list' is empty.
    """
    if not parking_id_list:
        raise ValueError("parking_id list cannot be empty.")
        
    table = dynamodb.Table(table_name)
    
    with table.batch_writer() as batch:
        for parking_id in parking_id_list:
            batch.delete_item(Key={'parking_id': parking_id})
            
    return len(parking_id_list)


def _reset_all_slots(table_name: str) -> int:
    """
    Description:
        Deletes all items from the DynamoDB table, effectively resetting it to an
        empty state. It performs a paginated scan to retrieve all primary keys,
        then uses a batch_writer to delete all items efficiently.

    Args:
        table_name (str): The name of the DynamoDB table to reset.

    Returns:
        int: The total count of items that were deleted.
    """
    table = dynamodb.Table(table_name)
    all_keys = []
    
    # Step 1: Scaning the table to get all primary keys.
    # ProjectionExpression minimizes read cost by only fetching the key.
    scan_kwargs = {'ProjectionExpression': 'parking_id'}
    
    done = False
    start_key = None
    while not done:
        if start_key:
            scan_kwargs['ExclusiveStartKey'] = start_key
        response = table.scan(**scan_kwargs)
        all_keys.extend(response.get('Items', []))
        start_key = response.get('LastEvaluatedKey', None)
        done = start_key is None

    if not all_keys:
        return 0 # Table is already empty

    # Step 2: Batch deleting the all items.
    with table.batch_writer() as batch:
        for key in all_keys:
            batch.delete_item(Key=key)
            
    return len(all_keys)