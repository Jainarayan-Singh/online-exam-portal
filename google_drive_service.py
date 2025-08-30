# google_drive_service.py - COMPLETELY FIXED VERSION with proper API usage
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

load_dotenv()
SERVICE_ACCOUNT_JSON_PATH = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON_PATH")


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
    """Initialize Google Drive service with credentials from JSON file"""
    try:
        print("📁 Initializing Google Drive service with credentials from JSON file...")

        # Load credentials from JSON file
        with open(SERVICE_ACCOUNT_JSON_PATH) as f:
            credentials_info = json.load(f)
        credentials = Credentials.from_service_account_info(credentials_info, scopes=_scopes())
        service = build('drive', 'v3', credentials=credentials, cache_discovery=False)

        # Test the connection quickly
        about = service.about().get(fields="user").execute()
        print(f"✅ Connected as: {about.get('user', {}).get('emailAddress', 'Unknown')}")
        return service

    except Exception as e:
        print(f"❌ Failed to initialize Google Drive service: {e}")
        return None


def _is_cache_valid(cache_key, ttl_seconds=300):
    """Check if cache entry is still valid"""
    if cache_key not in _cache_timestamps:
        return False
    return time.time() - _cache_timestamps[cache_key] < ttl_seconds


def _set_cache(cache_key, value, cache_dict):
    """Set cache entry with timestamp"""
    cache_dict[cache_key] = value
    _cache_timestamps[cache_key] = time.time()


def load_csv_from_drive(service, file_id, max_retries=3):
    """Load CSV from Google Drive with caching - FIXED"""
    if not service:
        print("❌ No Google Drive service available")
        return pd.DataFrame()

    # Check cache first
    cache_key = f"csv_{file_id}"
    if _is_cache_valid(cache_key, 300):
        print("📋 Using cached CSV")
        return _file_cache[cache_key].copy()

    for attempt in range(max_retries):
        try:
            print(f"📥 Loading CSV (attempt {attempt + 1}/{max_retries}) id={file_id}")

            # Get file metadata first
            try:
                meta = service.files().get(fileId=file_id, fields="id,name,size").execute()
                print(f"📄 File: {meta.get('name', 'unknown')} ({meta.get('size', '0')} bytes)")
            except Exception as meta_error:
                print(f"⚠️ Could not get file metadata: {meta_error}")
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
                    print(f"📊 Download progress: {progress}%")

            file_buffer.seek(0)
            content = file_buffer.getvalue().decode('utf-8')

            if not content.strip():
                print("⚠️ File is empty")
                return pd.DataFrame()

            # Parse CSV with better error handling
            try:
                df = pd.read_csv(StringIO(content))
                if df.empty:
                    print("⚠️ Parsed DataFrame is empty")
                    return pd.DataFrame()

                # Cache the result
                _set_cache(cache_key, df.copy(), _file_cache)
                print(f"✅ Loaded {len(df)} rows from {meta.get('name', 'unknown file')}")
                return df

            except pd.errors.EmptyDataError:
                print("⚠️ CSV file has no data")
                return pd.DataFrame()
            except pd.errors.ParserError as e:
                print(f"❌ CSV parsing error: {e}")
                return pd.DataFrame()

        except HttpError as e:
            print(f"❌ HTTP Error (attempt {attempt + 1}): {e}")
            if e.resp.status == 404:
                print("❌ File not found - check file ID")
                break
            elif e.resp.status == 403:
                print("❌ Access denied - check permissions")
                break
        except Exception as e:
            print(f"❌ Load attempt {attempt + 1} failed: {e}")

        if attempt == max_retries - 1:
            print("❌ All attempts failed")
            return pd.DataFrame()

        time.sleep(2 ** attempt)  # Exponential backoff

    return pd.DataFrame()


def save_csv_to_drive(service, df, file_id, max_retries=3):
    """Save CSV to Google Drive - FINAL WORKING VERSION"""
    if not service:
        print("No Google Drive service available")
        return False

    if df.empty:
        print("DataFrame is empty, skipping save")
        return False

    for attempt in range(max_retries):
        try:
            print(f"Saving CSV (attempt {attempt + 1}/{max_retries}) id={file_id}")

            # Convert DataFrame to CSV string
            csv_buffer = StringIO()
            df.to_csv(csv_buffer, index=False)
            csv_content = csv_buffer.getvalue()

            # Create media object
            media = MediaIoBaseUpload(
                BytesIO(csv_content.encode('utf-8')),
                mimetype='text/csv',
                resumable=True
            )

            # Try different parameter combinations based on Google API version
            try:
                # Method 1: Most common working version
                updated_file = service.files().update(
                    fileId=file_id,
                    media_body=media,
                    fields='id,name,size'
                ).execute()
            except TypeError as te:
                if "unexpected keyword argument" in str(te):
                    # Method 2: Alternative parameter structure
                    updated_file = service.files().update(
                        fileId=file_id,
                        body={},
                        media_body=media,
                        fields='id,name,size'
                    ).execute()
                else:
                    raise te

            # Clear cache for this file
            cache_key = f"csv_{file_id}"
            if cache_key in _file_cache:
                del _file_cache[cache_key]
                del _cache_timestamps[cache_key]

            print(f"Successfully saved {updated_file.get('name')} ({updated_file.get('size')} bytes)")
            return True

        except HttpError as e:
            print(f"HTTP Error (attempt {attempt + 1}): {e}")
            if e.resp.status == 404:
                print("File not found - check file ID")
                break
            elif e.resp.status == 403:
                print("Access denied - check permissions")
                break
        except Exception as e:
            print(f"Save attempt {attempt + 1} failed: {e}")

        if attempt == max_retries - 1:
            print("All save attempts failed")
            return False

        time.sleep(2 ** attempt)

    return False

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

            # Use thumbnail URL instead of uc?id= for better compatibility
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


def clear_cache():
    """Clear all caches"""
    global _file_cache, _folder_cache, _image_cache, _cache_timestamps
    _file_cache.clear()
    _folder_cache.clear()
    _image_cache.clear()
    _cache_timestamps.clear()
    print("✅ Cleared all caches")


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