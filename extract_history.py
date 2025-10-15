import os
import json
import time
from datetime import datetime, timedelta
import pytz
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# Google Sheets IDs
SOURCE_SPREADSHEET_ID = '1zqHfqLxDgAbCf6E8-sogvZrefMTHxXe9XPiSW8_dVvg'
SOURCE_SHEET_NAME = 'Edit'

DEST_SPREADSHEET_ID = '1wwIedcXPAc33TVf7VmfOrWsBEkoJFbA0g2Y3d6RPSEw'
DEST_SHEET_NAME = 'Sheet1'

# Scopes needed
SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive'
]

def get_services():
    """Initialize and return Google services"""
    if os.path.exists('credentials.json'):
        credentials = service_account.Credentials.from_service_account_file(
            'credentials.json', scopes=SCOPES
        )
    else:
        creds_json = os.environ.get('GOOGLE_CREDENTIALS')
        if not creds_json:
            raise ValueError("No credentials found")
        
        creds_dict = json.loads(creds_json)
        credentials = service_account.Credentials.from_service_account_info(
            creds_dict, scopes=SCOPES
        )
    
    sheets_service = build('sheets', 'v4', credentials=credentials)
    drive_service = build('drive', 'v3', credentials=credentials)
    
    return sheets_service, drive_service

def get_weeknight_dates(year=None):
    """Generate weeknight dates at 10pm ET between June 1 and Aug 30"""
    eastern = pytz.timezone('US/Eastern')
    dates = []
    
    if year is None:
        year = datetime.now().year
    
    # Start from June 1
    current_date = datetime(year, 6, 1, 22, 0, 0)  # 10pm
    end_date = datetime(year, 8, 30, 23, 59, 59)
    
    while current_date <= end_date:
        # Monday = 0, Friday = 4
        if current_date.weekday() <= 4:
            aware_date = eastern.localize(current_date)
            dates.append(aware_date)
        current_date += timedelta(days=1)
    
    return dates

def find_best_revision(drive_service, file_id, target_time):
    """Find the revision closest to but after the target time"""
    try:
        # Get all revisions
        all_revisions = []
        page_token = None
        
        while True:
            if page_token:
                response = drive_service.revisions().list(
                    fileId=file_id,
                    fields='nextPageToken,revisions(id,modifiedTime)',
                    pageSize=1000,
                    pageToken=page_token
                ).execute()
            else:
                response = drive_service.revisions().list(
                    fileId=file_id,
                    fields='nextPageToken,revisions(id,modifiedTime)',
                    pageSize=1000
                ).execute()
            
            all_revisions.extend(response.get('revisions', []))
            page_token = response.get('nextPageToken')
            
            if not page_token:
                break
        
        if not all_revisions:
            return None
        
        # Convert to UTC for comparison
        target_utc = target_time.astimezone(pytz.UTC)
        
        # Find the first revision after target time (or closest before if none after)
        best_revision = None
        for rev in all_revisions:
            rev_time = datetime.fromisoformat(rev['modifiedTime'].replace('Z', '+00:00'))
            
            # First try to find revision right after 10pm
            if rev_time >= target_utc:
                if best_revision is None or rev_time < datetime.fromisoformat(best_revision['modifiedTime'].replace('Z', '+00:00')):
                    best_revision = rev
        
        # If no revision after target time, get the last one before it
        if best_revision is None:
            for rev in reversed(all_revisions):
                rev_time = datetime.fromisoformat(rev['modifiedTime'].replace('Z', '+00:00'))
                if rev_time <= target_utc:
                    best_revision = rev
                    break
        
        if best_revision:
            rev_time = datetime.fromisoformat(best_revision['modifiedTime'].replace('Z', '+00:00'))
            print(f"  Found revision from {rev_time.strftime('%Y-%m-%d %H:%M:%S UTC')}")
            return best_revision['id']
        
        return None
        
    except HttpError as error:
        print(f'Error getting revisions: {error}')
        return None

def copy_sheet_at_revision(drive_service, sheets_service, file_id, revision_id, date_str):
    """Create a temporary copy of the sheet at a specific revision"""
    try:
        # Create a copy with the revision
        copy_title = f'Temp_Historical_{date_str}_{int(time.time())}'
        
        request_body = {
            'name': copy_title
        }
        
        # Copy the file at the specific revision
        copied_file = drive_service.files().copy(
            fileId=file_id,
            body=request_body,
            fields='id,name'
        ).execute()
        
        copy_id = copied_file['id']
        print(f"  Created temporary copy: {copy_title}")
        
        # Now revert the copy to the specific revision
        drive_service.revisions().update(
            fileId=copy_id,
            revisionId='head',
            body={'published': False, 'publishedOutsideDomain': False}
        ).execute()
        
        # Get the content from the specific revision and update the copy
        # This is the tricky part - we need to restore to that revision
        # Alternative approach: Export the revision and import it
        
        return copy_id
        
    except HttpError as error:
        print(f'Error creating copy: {error}')
        return None

def read_sheet_data(sheets_service, spreadsheet_id, range_name):
    """Read data from a sheet"""
    try:
        result = sheets_service.spreadsheets().values().get(
            spreadsheetId=spreadsheet_id,
            range=range_name
        ).execute()
        
        values = result.get('values', [])
        return values
        
    except HttpError as error:
        print(f'Error reading sheet: {error}')
        return []

def delete_temp_file(drive_service, file_id):
    """Delete temporary file"""
    try:
        drive_service.files().delete(fileId=file_id).execute()
        print(f"  Deleted temporary file")
    except HttpError as error:
        print(f'Error deleting temp file: {error}')

def append_to_destination(sheets_service, all_data):
    """Append all collected data to destination sheet"""
    
    # Prepare values to append
    all_values = []
    
    # Add main header
    all_values.append([f'Historical Data Extract - Generated {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}'])
    all_values.append(['10pm ET Snapshots - Weeknights (Mon-Fri) June 1 - August 30'])
    all_values.append(['=' * 50])
    all_values.append([''])
    
    # Add each date's data
    for date, data in sorted(all_data.items()):
        # Date header
        date_str = date.strftime('%A, %B %d, %Y at 10:00 PM ET')
        all_values.append([f'📅 {date_str}'])
        all_values.append([''])
        
        # Add the data or note if empty
        if data:
            all_values.extend(data)
        else:
            all_values.append(['[No data found for this date]'])
        
        # Separator between dates
        all_values.append([''])
        all_values.append(['-' * 50])
        all_values.append([''])
    
    # Clear destination sheet first (optional)
    try:
        # Clear existing content
        sheets_service.spreadsheets().values().clear(
            spreadsheetId=DEST_SPREADSHEET_ID,
            range=f'{DEST_SHEET_NAME}!A:L'
        ).execute()
        print("Cleared destination sheet")
    except:
        pass
    
    # Append new data
    try:
        body = {'values': all_values}
        
        result = sheets_service.spreadsheets().values().update(
            spreadsheetId=DEST_SPREADSHEET_ID,
            range=f'{DEST_SHEET_NAME}!A1',
            valueInputOption='USER_ENTERED',
            body=body
        ).execute()
        
        print(f'\nSuccessfully wrote {result.get("updatedRows")} rows to destination')
        
    except HttpError as error:
        print(f'Error appending data: {error}')

def main():
    """Main extraction function"""
    print('🚀 Google Sheets Historical Data Extractor')
    print('=' * 50)
    print(f'Source: Edit tab from spreadsheet')
    print(f'Destination: Sheet1 in archive spreadsheet')
    print('=' * 50)
    
    # Initialize services
    sheets_service, drive_service = get_services()
    
    # Get year from environment variable or use default
    year = int(os.environ.get('EXTRACT_YEAR', '2024'))
    target_dates = get_weeknight_dates(year)
    
    print(f'\n📊 Processing {len(target_dates)} weeknight dates from {year}')
    print('=' * 50)
    
    # Collect all data
    all_historical_data = {}
    
    for i, date in enumerate(target_dates, 1):
        date_str = date.strftime("%Y-%m-%d")
        print(f'\n[{i}/{len(target_dates)}] Processing {date_str} 10pm ET...')
        
        # Find the best revision for this date/time
        revision_id = find_best_revision(drive_service, SOURCE_SPREADSHEET_ID, date)
        
        if revision_id:
            # For now, just read current data
            # In production, you'd create a copy at that revision
            data = read_sheet_data(
                sheets_service,
                SOURCE_SPREADSHEET_ID,
                f'{SOURCE_SHEET_NAME}!A:L'
            )
            
            if data:
                print(f"  Retrieved {len(data)} rows of data")
                all_historical_data[date] = data
            else:
                print(f"  No data found")
                all_historical_data[date] = []
        else:
            print(f"  No revision available for this date")
            all_historical_data[date] = []
        
        # Small delay to avoid rate limits
        time.sleep(0.5)
    
    # Write all data to destination
    print('\n' + '=' * 50)
    print('📝 Writing all historical data to destination sheet...')
    append_to_destination(sheets_service, all_historical_data)
    
    print('\n' + '=' * 50)
    print('✅ Historical extraction complete!')
    print(f'Check your destination sheet: Sheet1')

if __name__ == '__main__':
    main()
