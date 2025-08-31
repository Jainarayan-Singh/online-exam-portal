# google_drive_service.py - FIXED VERSION with enhanced debugging
import os
import json
import time
import pandas as pd
from io import StringIO, BytesIO
from googleapiclient.discovery import build
from google.oauth2.service_account import Credentials
from googleapiclient.http import MediaIoBaseDownload, MediaIoBaseUpload
from googleapiclient.errors import HttpError
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Get the JSON string from environment
# Load service account from JSON file directly
def load_service_account_info():
    try:
        service_account_path = os.path.join(os.path.dirname(__file__), 'service_account.json')
        with open(service_account_path, 'r') as f:
            return json.load(f)
    except:
        return None

# Simple caches with TTL
_file_cache = {}
_folder_cache = {}
_image_cache = {}
_cache_timestamps = {}

def _scopes():
    return [
        "https://www.googleapis.com/auth/drive",
        "https://www.googleapis.com/auth/drive.file",
        "https://www.googleapis.com/auth/drive.readonly",
    ]

def create_drive_service():
    """Initialize Google Drive service with enhanced error handling and debugging"""
    try:
        print("🔐 Starting Google Drive service initialization...")
        
        credentials_info = load_service_account_info()
        if not credentials_info:
            print("❌ Could not load service_account.json file")
            return None

        print("✅ Service account loaded from file")
        
        # Validate required fields
        required_fields = ['type', 'project_id', 'private_key_id', 'private_key', 'client_email', 'client_id']
        missing_fields = [field for field in required_fields if field not in credentials_info]
        
        if missing_fields:
            print(f"❌ Missing required fields in JSON: {missing_fields}")
            return None
        
        print(f"📧 Service account email: {credentials_info.get('client_email')}")
        print(f"🆔 Project ID: {credentials_info.get('project_id')}")
        
        # Fix private key formatting
        if 'private_key' in credentials_info:
            original_key = credentials_info['private_key']
            fixed_key = original_key.replace('\\n', '\n')
            credentials_info['private_key'] = fixed_key
            
            if original_key != fixed_key:
                print("🔧 Fixed private key newlines")
            else:
                print("✅ Private key formatting is correct")
        
        # Create credentials
        try:
            credentials = Credentials.from_service_account_info(credentials_info, scopes=_scopes())
            print("✅ Credentials created successfully")
        except Exception as cred_error:
            print(f"❌ Failed to create credentials: {cred_error}")
            return None
        
        # Build service
        try:
            service = build('drive', 'v3', credentials=credentials, cache_discovery=False)
            print("✅ Google Drive service built successfully")
        except Exception as build_error:
            print(f"❌ Failed to build service: {build_error}")
            return None

        # Test the connection
        try:
            about = service.about().get(fields="user,storageQuota").execute()
            user_email = about.get('user', {}).get('emailAddress', 'Unknown')
            storage = about.get('storageQuota', {})
            print(f"🎯 Connection test successful!")
            print(f"👤 Connected as: {user_email}")
            print(f"💾 Storage used: {storage.get('usage', 'Unknown')} / {storage.get('limit', 'Unknown')}")
            
            return service
            
        except Exception as test_error:
            print(f"❌ Service created but connection test failed: {test_error}")
            # Return the service anyway as it might still work for file operations
            return service

    except Exception as e:
        print(f"❌ Unexpected error during service creation: {e}")
        import traceback
        traceback.print_exc()
        return None

def load_csv_from_drive(service, file_id, max_retries=3):
    """Load CSV from Google Drive with enhanced debugging"""
    if not service:
        print("❌ No Google Drive service available for CSV loading")
        return pd.DataFrame()

    # Validate file_id
    if not file_id or file_id.startswith('YOUR_') or len(file_id) < 10:
        print(f"❌ Invalid file ID: {file_id}")
        return pd.DataFrame()

    # Check cache first
    cache_key = f"csv_{file_id}"
    if _is_cache_valid(cache_key, 300):
        print("💾 Using cached CSV")
        return _file_cache[cache_key].copy()

    for attempt in range(max_retries):
        try:
            print(f"📥 Loading CSV (attempt {attempt + 1}/{max_retries}) id={file_id}")

            # Get file metadata first for debugging
            try:
                meta = service.files().get(fileId=file_id, fields="id,name,size,mimeType").execute()
                print(f"📄 File: {meta.get('name', 'unknown')} ({meta.get('size', '0')} bytes, {meta.get('mimeType', 'unknown type')})")
            except Exception as meta_error:
                print(f"⚠️ Could not get file metadata: {meta_error}")
                if "not found" in str(meta_error).lower():
                    print(f"❌ File ID {file_id} does not exist")
                    return pd.DataFrame()
                meta = {'name': 'unknown file'}

            # Download file content
            request = service.files().get_media(fileId=file_id)
            file_buffer = BytesIO()
            downloader = MediaIoBaseDownload(file_buffer, request)
            done = False

            while not done:
                status, done = downloader.next_chunk()
                if status:
                    progress = int(status.progress() * 100)
                    if progress % 25 == 0:  # Log every 25%
                        print(f"📊 Download progress: {progress}%")

            file_buffer.seek(0)
            content = file_buffer.getvalue().decode('utf-8')

            if not content.strip():
                print("⚠️ Downloaded file is empty")
                return pd.DataFrame()

            print(f"📄 Downloaded content length: {len(content)} characters")

            # Parse CSV with better error handling
            try:
                df = pd.read_csv(StringIO(content))
                if df.empty:
                    print("⚠️ Parsed DataFrame is empty")
                    return pd.DataFrame()

                # Cache the result
                _set_cache(cache_key, df.copy(), _file_cache)
                print(f"✅ Successfully loaded {len(df)} rows, {len(df.columns)} columns")
                print(f"📊 Columns: {list(df.columns)}")
                return df

            except pd.errors.EmptyDataError:
                print("⚠️ CSV file has no data")
                return pd.DataFrame()
            except pd.errors.ParserError as e:
                print(f"❌ CSV parsing error: {e}")
                print(f"📄 Content preview: {content[:500]}...")
                return pd.DataFrame()

        except HttpError as e:
            print(f"❌ HTTP Error (attempt {attempt + 1}): {e}")
            if e.resp.status == 404:
                print("❌ File not found - check file ID")
                break
            elif e.resp.status == 403:
                print("❌ Access denied - check permissions")
                print("💡 Make sure the service account has access to the file")
                break
            elif e.resp.status == 429:
                print("⏰ Rate limited - waiting longer...")
                time.sleep(10)
                continue
        except Exception as e:
            print(f"❌ Load attempt {attempt + 1} failed: {e}")

        if attempt < max_retries - 1:
            wait_time = 2 ** attempt
            print(f"⏰ Waiting {wait_time} seconds before retry...")
            time.sleep(wait_time)

    print("❌ All attempts failed")
    return pd.DataFrame()

def _is_cache_valid(cache_key, ttl_seconds=300):
    """Check if cache entry is still valid"""
    if cache_key not in _cache_timestamps:
        return False
    return time.time() - _cache_timestamps[cache_key] < ttl_seconds

def _set_cache(cache_key, value, cache_dict):
    """Set cache entry with timestamp"""
    cache_dict[cache_key] = value
    _cache_timestamps[cache_key] = time.time()

def save_csv_to_drive(service, df, file_id, max_retries=3):
    """Save CSV to Google Drive with enhanced debugging"""
    if not service:
        print("❌ No Google Drive service available for saving")
        return False

    if df.empty:
        print("⚠️ DataFrame is empty, skipping save")
        return False

    if not file_id or file_id.startswith('YOUR_'):
        print(f"❌ Invalid file ID for saving: {file_id}")
        return False

    for attempt in range(max_retries):
        try:
            print(f"💾 Saving CSV (attempt {attempt + 1}/{max_retries}) id={file_id}")
            print(f"📊 Data to save: {len(df)} rows, {len(df.columns)} columns")

            # Convert DataFrame to CSV string
            csv_buffer = StringIO()
            df.to_csv(csv_buffer, index=False)
            csv_content = csv_buffer.getvalue()
            
            print(f"📄 CSV content length: {len(csv_content)} characters")

            # Create media object
            media = MediaIoBaseUpload(
                BytesIO(csv_content.encode('utf-8')),
                mimetype='text/csv',
                resumable=True
            )

            # Try to update the file
            try:
                updated_file = service.files().update(
                    fileId=file_id,
                    media_body=media,
                    fields='id,name,size'
                ).execute()
                
                print(f"✅ Successfully saved {updated_file.get('name')} ({updated_file.get('size')} bytes)")
                
                # Clear cache for this file
                cache_key = f"csv_{file_id}"
                if cache_key in _file_cache:
                    del _file_cache[cache_key]
                    del _cache_timestamps[cache_key]
                    print("🗑️ Cleared cache for updated file")
                
                return True
                
            except TypeError as te:
                if "unexpected keyword argument" in str(te):
                    # Alternative method
                    updated_file = service.files().update(
                        fileId=file_id,
                        body={},
                        media_body=media,
                        fields='id,name,size'
                    ).execute()
                    print(f"✅ Successfully saved (alt method) {updated_file.get('name')}")
                    return True
                else:
                    raise te

        except HttpError as e:
            print(f"❌ HTTP Error (attempt {attempt + 1}): {e}")
            if e.resp.status == 404:
                print("❌ File not found - check file ID")
                break
            elif e.resp.status == 403:
                print("❌ Access denied - check permissions")
                break
            elif e.resp.status == 429:
                print("⏰ Rate limited - waiting longer...")
                time.sleep(10)
                continue
        except Exception as e:
            print(f"❌ Save attempt {attempt + 1} failed: {e}")

        if attempt < max_retries - 1:
            wait_time = 2 ** attempt
            print(f"⏰ Waiting {wait_time} seconds before retry...")
            time.sleep(wait_time)

    print("❌ All save attempts failed")
    return False

# [Keep all your other functions the same but add the missing functions...]

def find_file_by_name(service, filename, parent_folder_id=None, max_retries=2):
    """Find file by name in Google Drive"""
    if not service:
        print("❌ No Google Drive service available")
        return None

    cache_key = f"file::{parent_folder_id or 'root'}::{filename}"
    if _is_cache_valid(cache_key, 600):
        return _file_cache[cache_key]

    for attempt in range(max_retries):
        try:
            # Build query
            query = f"name = '{filename}' and trashed = false"
            if parent_folder_id:
                query += f" and '{parent_folder_id}' in parents"

            # Search for file
            results = service.files().list(
                q=query,
                spaces='drive',
                fields='files(id, name)',
                pageSize=5
            ).execute()

            files = results.get('files', [])

            if files:
                file_id = files[0]['id']
                # Cache the result
                _set_cache(cache_key, file_id, _file_cache)
                print(f"✅ Found file '{filename}': {file_id}")
                return file_id
            else:
                print(f"🔍 File not found: {filename}")
                return None

        except Exception as e:
            print(f"❌ Find file attempt {attempt + 1} failed: {e}")
            if attempt == max_retries - 1:
                return None
            time.sleep(1)

    return None


def find_folder_by_name(service, folder_name, parent_folder_id=None, max_retries=2):
    """Find folder by name in Google Drive"""
    if not service:
        print("❌ No Google Drive service available")
        return None

    cache_key = f"folder::{parent_folder_id or 'root'}::{folder_name}"
    if _is_cache_valid(cache_key, 600):
        return _folder_cache[cache_key]

    for attempt in range(max_retries):
        try:
            # Build query for folders
            query = f"name = '{folder_name}' and mimeType = 'application/vnd.google-apps.folder' and trashed = false"
            if parent_folder_id:
                query += f" and '{parent_folder_id}' in parents"

            # Search for folder
            results = service.files().list(
                q=query,
                spaces='drive',
                fields='files(id, name)',
                pageSize=5
            ).execute()

            folders = results.get('files', [])

            if folders:
                folder_id = folders[0]['id']
                # Cache the result
                _set_cache(cache_key, folder_id, _folder_cache)
                print(f"✅ Found folder '{folder_name}': {folder_id}")
                return folder_id
            else:
                print(f"🔍 Folder not found: {folder_name}")
                return None

        except Exception as e:
            print(f"❌ Find folder attempt {attempt + 1} failed: {e}")
            if attempt == max_retries - 1:
                return None
            time.sleep(1)

    return None


def get_public_url(service, file_id, max_retries=2):
    """Generate public URL for a file - OPTIMIZED"""
    if not service:
        print("❌ No Google Drive service available")
        # Use direct thumbnail URL instead of uc?id=
        return f"https://drive.google.com/thumbnail?id={file_id}&sz=w1000"

    # Check cache first
    cache_key = f"url_{file_id}"
    if _is_cache_valid(cache_key, 3600):
        return _image_cache[cache_key]

    for attempt in range(max_retries):
        try:
            # First, check if the file is already public
            permissions = service.permissions().list(
                fileId=file_id,
                fields='permissions(id,type,role)'
            ).execute()

            # Check if public access exists
            has_public_access = any(
                perm.get('type') == 'anyone' and perm.get('role') in ['reader', 'writer']
                for perm in permissions.get('permissions', [])
            )

            if not has_public_access:
                # Make the file public
                permission = {
                    'type': 'anyone',
                    'role': 'reader'
                }
                service.permissions().create(
                    fileId=file_id,
                    body=permission,
                    fields='id'
                ).execute()
                print(f"✅ Made file {file_id} public")

            # Use thumbnail URL for better compatibility
            public_url = f"https://drive.google.com/thumbnail?id={file_id}&sz=w1000"

            # Cache the result
            _set_cache(cache_key, public_url, _image_cache)

            print(f"✅ Public URL: {public_url}")
            return public_url

        except Exception as e:
            print(f"❌ Public URL attempt {attempt + 1} failed: {e}")
            if attempt == max_retries - 1:
                # Fallback to thumbnail URL
                fallback_url = f"https://drive.google.com/thumbnail?id={file_id}&sz=w1000"
                _set_cache(cache_key, fallback_url, _image_cache)
                return fallback_url
            time.sleep(1)

    return f"https://drive.google.com/thumbnail?id={file_id}&sz=w1000"


def list_drive_files(service, folder_id=None, max_retries=2):
    """List files in a folder"""
    if not service:
        print("❌ No Google Drive service available")
        return []

    for attempt in range(max_retries):
        try:
            query = "trashed = false"
            if folder_id:
                query += f" and '{folder_id}' in parents"

            results = service.files().list(
                q=query,
                spaces='drive',
                fields='files(id, name, mimeType)',
                pageSize=100
            ).execute()

            return results.get('files', [])

        except Exception as e:
            print(f"❌ List files attempt {attempt + 1} failed: {e}")
            if attempt == max_retries - 1:
                return []
            time.sleep(1)

    return []


def create_file_if_not_exists(service, filename, parent_folder_id=None):
    """Create a new CSV file if it doesn't exist"""
    if not service:
        return None

    try:
        # Check if file exists
        existing_id = find_file_by_name(service, filename, parent_folder_id)
        if existing_id:
            return existing_id

        # Create new file
        file_metadata = {
            'name': filename,
            'mimeType': 'text/csv'
        }

        if parent_folder_id:
            file_metadata['parents'] = [parent_folder_id]

        # Create empty CSV content
        empty_csv = pd.DataFrame()
        csv_buffer = StringIO()
        empty_csv.to_csv(csv_buffer, index=False)

        media = MediaIoBaseUpload(
            BytesIO(csv_buffer.getvalue().encode('utf-8')),
            mimetype='text/csv'
        )

        file = service.files().create(
            body=file_metadata,
            media=media,
            fields='id'
        ).execute()

        print(f"✅ Created new file: {filename} with ID: {file.get('id')}")
        return file.get('id')

    except Exception as e:
        print(f"❌ Error creating file {filename}: {e}")
        return None


def clear_cache():
    """Clear all caches"""
    global _file_cache, _folder_cache, _image_cache, _cache_timestamps
    _file_cache.clear()
    _folder_cache.clear()
    _image_cache.clear()
    _cache_timestamps.clear()
    print("✅ Cleared all caches")
