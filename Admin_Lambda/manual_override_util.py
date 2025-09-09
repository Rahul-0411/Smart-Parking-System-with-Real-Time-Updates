from datetime import datetime, timezone, timedelta

def prepare_manual_entry(slot_data, email, vehicle_id, expected_time_minutes):
    """
    Validates slot data and prepares the parameters for a manual entry DynamoDB update.
    This function is pure Python and does not interact with AWS services.

    Args:
        slot_data (dict): The current data of the parking slot from DynamoDB.
        email (str): The user's email.
        vehicle_id (str): The user's vehicle ID.
        expected_time_minutes (int): The expected parking duration in minutes.

    Raises:
        ValueError: If the slot is not found or is not empty.

    Returns:
        dict: The parameters required for the DynamoDB update_item call.
    """
    if not slot_data:
        raise ValueError("Parking slot data cannot be empty.")
    if slot_data.get('status') != 'empty':
        raise ValueError(f"Slot is not empty. Current status: {slot_data.get('status')}")

    # Calculate timestamps
    entry_time = datetime.now(timezone.utc)
    expected_time = entry_time + timedelta(minutes=int(expected_time_minutes))

    # Prepare DynamoDB update parameters
    params = {
        'UpdateExpression': (
            "SET #status = :occupied, email = :email, vehicle_id = :vid, "
            "entry_timestamp = :entry, expected_time = :expected"
        ),
        'ExpressionAttributeNames': {
            '#status': 'status'
        },
        'ExpressionAttributeValues': {
            ':occupied': 'occupied',
            ':email': email,
            ':vid': vehicle_id,
            ':entry': entry_time.isoformat(),
            ':expected': expected_time.isoformat()
        },
        'ReturnValues': "ALL_NEW"
    }
    return params


def prepare_manual_exit(slot_data):
    """
    Validates slot data and prepares parameters for a manual exit.
    This function is pure Python and does not interact with AWS services.

    Args:
        slot_data (dict): The current data of the parking slot from DynamoDB.

    Raises:
        ValueError: If the slot is not found or is not occupied.

    Returns:
        dict: The parameters required to update the slot item in DynamoDB.
    """
    if not slot_data:
        raise ValueError("Parking slot data cannot be empty.")
    if slot_data.get('status') != 'occupied':
        raise ValueError(f"Slot is not occupied. Current status: {slot_data.get('status')}")

    # Parameters to update the ParkingSlotStatus table
    update_params = {
        'UpdateExpression': "SET #status = :empty REMOVE email, vehicle_id, entry_timestamp, expected_time",
        'ExpressionAttributeNames': {
            '#status': 'status'
        },
        'ExpressionAttributeValues': {
            ':empty': 'empty'
        }
    }

    return update_params