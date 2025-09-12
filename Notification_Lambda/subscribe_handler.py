import json
import boto3
import os
from datetime import datetime, timezone
 
AWS_REGION = os.getenv("AWS_REGION", "ap-south-1")
SUBSCRIPTION_TABLE_NAME = "UserSubscriptions"
SNS_TOPIC_ARN = "arn:aws:sns:ap-south-1:180651458429:parking-slot-notifications"
 
# Initialize DynamoDB and SNS clients
# dynamodb = boto3.resource('dynamodb', region_name=AWS_REGION)
# subscriptions_table = dynamodb.Table(SUBSCRIPTION_TABLE_NAME)
sns_client = boto3.client('sns', region_name=AWS_REGION)
 
def create_response(status_code, body):
    return {
        'statusCode': status_code,
        'headers': {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'POST,OPTIONS',
            'Access-Control-Allow-Headers': 'Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token'
        },
        'body': json.dumps(body)
    }
 
def lambda_handler(event, context):
    try:
        if event['httpMethod'] == 'OPTIONS':
            return create_response(200, {})
 
        if event['httpMethod'] != 'POST':
            return create_response(405, {'message': 'Method Not Allowed'})
 
        body = json.loads(event['body'])
        email = body.get('email')
        area_number = body.get('area_number')
        floor_number = body.get('floor_number')
 
        if not email or not isinstance(area_number, int) or not isinstance(floor_number, int):
            return create_response(400, {'message': 'Email, area_number (integer), and floor_number (integer) are required.'})
 
        subscription_id = f"{email.lower()}-{area_number}-{floor_number}"
        
        subscribe_response = sns_client.subscribe(
            TopicArn=SNS_TOPIC_ARN,
            Protocol='email',
            Endpoint=email,
            ReturnSubscriptionArn=True
        )
        subscription_arn = subscribe_response['SubscriptionArn']
 
        filter_policy = {
            'area_number': [str(area_number)],  
            'floor_number': [str(floor_number)]
        }
 
        sns_client.set_subscription_attributes(
            SubscriptionArn=subscription_arn,
            AttributeName='FilterPolicy',
            AttributeValue=json.dumps(filter_policy)
        )
        print(f"Set FilterPolicy for {subscription_arn}: {json.dumps(filter_policy)}")

 
        return create_response(200, {
            'message': 'Subscription request sent successfully! Please check your email to confirm your subscription.',
            'subscriptionId': subscription_id,
            'subscription_arn': subscription_arn  # (Can be removed in prod)
        })
 
    except json.JSONDecodeError:
        return create_response(400, {'message': 'Invalid JSON in request body.'})
    except Exception as e:
        print(f"Error in subscribe_handler: {e}")
        return create_response(500, {'message': f'An internal server error occurred: {str(e)}'})