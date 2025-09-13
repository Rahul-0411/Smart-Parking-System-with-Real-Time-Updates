from datetime import datetime, timezone
import boto3
 
# Initialize AWS services
dynamodb = boto3.resource('dynamodb')
ses = boto3.client('ses')
table = dynamodb.Table('ParkingSlotDatabase')
 
SENDER_EMAIL = "rjagdale2523@gmail.com"  # Verified sender email
 
def lambda_handler(event, context):
    now = datetime.now(timezone.utc)
    print(f"[DEBUG] Current UTC time: {now}")
 
    try:
        response = table.scan(
            FilterExpression=boto3.dynamodb.conditions.Attr('status').eq('occupied')
        )
    except Exception as e:
        print(f"[ERROR] Failed to scan table: {e}")
        return {'statusCode': 500, 'body': 'DynamoDB scan failed'}
 
    items = response.get('Items', [])
    print(f"[DEBUG] Found {len(items)} occupied slots")
 
    for item in items:
        try:
            parking_id = item.get('parking_id')
            email = item.get('email')
            expected_time_str = item.get('expected_time')
 
            if not all([parking_id, email, expected_time_str]):
                print(f"[WARN] Skipping incomplete item: {item}")
                continue
 
            expiry = datetime.fromisoformat(expected_time_str)
            if expiry.tzinfo is None:
                expiry = expiry.replace(tzinfo=timezone.utc)
 
            diff_mins = (expiry - now).total_seconds() / 60
            msg = None
 
            # ✅ Case 1: Only send if exactly 10 minutes left (within ±30 sec)
            if 9.5 <= diff_mins <= 10.5:
                msg = f"Reminder: Your slot {parking_id} will expire in 10 minutes."
 
            # ✅ Case 2: Send once when 1–3 minutes left
            elif 1 <= round(diff_mins) <= 3:
                msg = f"Final warning: Your slot {parking_id} will expire in {round(diff_mins)} minutes."
 
            # ✅ Case 3: On-time expiry
            elif -0.5 <= diff_mins <= 0.5:
                msg = f"Notice: Your slot {parking_id} has now expired."
 
            # ✅ Case 4: Every 10 minutes after expiry (like 10, 20, 30 min)
            elif diff_mins < 0:
                mins_expired = int(abs(diff_mins))
                if mins_expired in [10, 20, 30]:
                    msg = f"Reminder: Your slot {parking_id} expired {mins_expired} minutes ago."
 
            if msg:
                ses.send_email(
                    Source=SENDER_EMAIL,
                    Destination={'ToAddresses': [email]},
                    Message={
                        'Subject': {'Data': "Smart Parking Expiry Notification"},
                        'Body': {'Text': {'Data': msg}}
                    }
                )
                print(f"[INFO] Email sent to {email} - {msg}")
 
        except Exception as e:
            print(f"[ERROR] Failed to process slot {item.get('parking_id')}: {e}")
 
    return {'statusCode': 200, 'body': 'Notification cycle completed.'}