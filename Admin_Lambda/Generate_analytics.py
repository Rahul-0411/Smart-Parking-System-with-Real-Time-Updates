import json
import os
import io
from datetime import datetime, timedelta
import pandas as pd
import boto3


s3_client = boto3.client('s3')
BUCKET_NAME = os.environ.get('BUCKET_NAME')
REPORTS_FOLDER = 'reports/'

def lambda_handler(event, context):
    try:
        
        query_params = event.get('queryStringParameters')
        
        if not query_params or 'startDate' not in query_params:
            return {
                'statusCode': 400,
                'headers': {'Access-Control-Allow-Origin': '*'},
                'body': json.dumps({'message': 'Please provide at least a startDate.'})
            }

        start_date_str = query_params['startDate']
        end_date_str = query_params.get('endDate', start_date_str)
        report_format = query_params.get('format', 'json').lower()
        
        start_date = datetime.strptime(start_date_str, '%Y-%m-%d').date()
        end_date = datetime.strptime(end_date_str, '%Y-%m-%d').date()

        
        all_dfs = []
        current_date = start_date
        while current_date <= end_date:
            date_str = current_date.strftime('%Y-%m-%d')
            file_key = f"{REPORTS_FOLDER}{date_str}.csv"
            try:
                response = s3_client.get_object(Bucket=BUCKET_NAME, Key=file_key)
                daily_df = pd.read_csv(io.StringIO(response['Body'].read().decode('utf-8')))
                all_dfs.append(daily_df)
            except s3_client.exceptions.NoSuchKey:
                print(f"Warning: Report for {date_str} not found. Skipping.")
            current_date += timedelta(days=1)

        if not all_dfs:
            return {
                'statusCode': 404,
                'headers': {'Access-Control-Allow-Origin': '*'},
                'body': json.dumps({'message': f'No reports found in the range {start_date_str} to {end_date_str}.'})
            }

        df = pd.concat(all_dfs, ignore_index=True)

        
        if report_format == 'csv':
            csv_output = df.to_csv(index=False)
            
            if start_date_str == end_date_str:
                filename = f"report_{start_date_str}.csv"
            else:
                filename = f"report_{start_date_str}_to_{end_date_str}.csv"
            
            return {
                'statusCode': 200,
                'headers': {
                    'Access-Control-Allow-Origin': '*',
                    'Content-Type': 'text/csv',
                    'Content-Disposition': f'attachment; filename="{filename}"'
                },
                'body': csv_output
            }
        else:
            
            completed_df = df[df['status'] == 'COMPLETED'].copy()
            parked_df = df[df['status'] == 'PARKED'].copy()

            df['entry_timestamp'] = pd.to_datetime(df['entry_timestamp'], errors='coerce')
            completed_df['exit_timestamp'] = pd.to_datetime(completed_df['exit_timestamp'], errors='coerce')

            total_entries = int(df['entry_timestamp'].count())
            total_exits = int(completed_df['exit_timestamp'].count())
            
            df['entry_hour'] = df['entry_timestamp'].dt.hour
            peak_entry_hour = int(df['entry_hour'].mode()[0]) if not df['entry_hour'].empty else 'N/A'
            hourly_entries = df['entry_hour'].value_counts().sort_index()

            completed_df['exit_hour'] = completed_df['exit_timestamp'].dt.hour
            peak_exit_hour = int(completed_df['exit_hour'].mode()[0]) if not completed_df['exit_hour'].empty else 'N/A'
            hourly_exits = completed_df['exit_hour'].value_counts().sort_index()
            
            trend_hours = list(range(24))
            entry_trend = [int(hourly_entries.get(h, 0)) for h in trend_hours]
            exit_trend = [int(hourly_exits.get(h, 0)) for h in trend_hours]
            
            
            slot_utilization = df['parking_id'].value_counts().to_dict()

            
            df['area_id'] = df['area_id'].astype(str)
            area_utilization = df['area_id'].value_counts().to_dict()
            
            current_occupancy_count = len(parked_df)

            api_response = {
                'reportStartDate': start_date_str,
                'reportEndDate': end_date_str,
                'dailyUsage': {
                    'totalEntries': total_entries,
                    'totalExits': total_exits,
                    'currentlyParked': current_occupancy_count
                },
                'peakHours': {
                    'peakEntryHour': peak_entry_hour,
                    'peakExitHour': peak_exit_hour,
                },
                'vehicleTrends': {
                    'hours': trend_hours,
                    'entries': entry_trend,
                    'exits': exit_trend
                },
                'utilization': {
                    'bySlot': slot_utilization,
                    'byArea': area_utilization
                }
            }

            return {
                'statusCode': 200,
                'headers': {'Access-Control-Allow-Origin': '*'},
                'body': json.dumps(api_response)
            }
            
    except Exception as e:
        print(f"Error: {str(e)}")
        return {
            'statusCode': 500,
            'headers': {'Access-Control-Allow-Origin': '*'},
            'body': json.dumps({'message': f'An error occurred: {str(e)}'})
        }
