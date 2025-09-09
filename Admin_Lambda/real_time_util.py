from datetime import datetime

def get_occupancy_status(percentage: float) -> str:
    """
    Determines a human-readable status based on occupancy percentage.

    Args:
        percentage: The calculated occupancy percentage.

    Returns:
        A string representing the status (e.g., "Full", "Available").
    """
    if percentage >= 100.0:
        return "Full"
    if percentage > 40.0:
        return "Partially Occupied"
    return "Available"


def _calculate_realtime_occupancy(slot_items: list) -> list:
    """
    Calculates real-time occupancy data from a list of parking slot items.

    This function is pure business logic, taking raw data and transforming
    it into the required summary format. It does not interact with any
    external services.

    Args:
        slot_items: A list of dictionaries, where each dictionary represents
                    a parking slot item from the database.

    Returns:
        A list of dictionaries, where each dictionary contains the
        aggregated occupancy details for a parking area.
    """
    if not slot_items:
        return []

    area_data = {} # Format: { area_number: { 'total_slots': int, 'occupied_slots': int, 'timestamps': [str] } }

    for item in slot_items:
        # Ensuring area_number is treated as an integer
        area_num = int(item['area_number'])
        if area_num not in area_data:
            area_data[area_num] = {'total_slots': 0, 'occupied_slots': 0, 'timestamps': []}
        
        area_data[area_num]['total_slots'] += 1
        if item.get('is_occupied', False):
            area_data[area_num]['occupied_slots'] += 1
        
        # Ensuring timestamp exists before appending
        if 'last_updated_timestamp' in item:
            area_data[area_num]['timestamps'].append(item['last_updated_timestamp'])

    final_results = []
    for area_num, data in area_data.items():
        total_slots = data['total_slots']
        occupied_slots = data['occupied_slots']
        available_slots = total_slots - occupied_slots
        
        # Finding the most recent timestamp for the area
        last_updated = max(data['timestamps']) if data['timestamps'] else datetime.utcnow().isoformat() + 'Z'
        
        occupancy_percentage = round((occupied_slots / total_slots) * 100, 2) if total_slots > 0 else 0.0
        status = get_occupancy_status(occupancy_percentage)

        final_results.append({
            "area_number"          : area_num,
            "total_slots"          : total_slots,
            "occupied_slots"       : occupied_slots,
            "available_slots"      : available_slots,
            "occupancy_percentage" : occupancy_percentage,
            "status"               : status,
            "last_updated"         : last_updated
        })
        
    # Sorting the final result by area number for consistent output
    return sorted(final_results, key=lambda x: x['area_number'])
