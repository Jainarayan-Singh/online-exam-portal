# google_drive_service.py — FINAL (fixed create_drive_service_user only)
import os
import json
import time
from io import StringIO, BytesIO
from datetime import datetime
import pandas as pd

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload, MediaIoBaseUpload
from google.oauth2.service_account import Credentials
from dotenv import load_dotenv
from google.oauth2.credentials import Credentials as UserCredentials
from google_auth_oauthlib.flow import InstalledAppFlow  # <-- added
load_dotenv()

# -------------------------------------------------------------------
# Small in-memory caches (CSV/file lookups/URLs) with TTL timestamps
# -------------------------------------------------------------------
_file_cache = {}
_folder_cache = {}
_image_cache = {}
_cache_timestamps = {}

def _is_cache_valid(key: str, ttl_seconds: int) -> bool:
    ts = _cache_timestamps.get(key)
    return bool(ts and (time.time() - ts) < ttl_seconds)

def _set_cache(key: str, value, bucket: dict):
    bucket[key] = value
    _cache_timestamps[key] = time.time()

def clear_cache():
    _file_cache.clear()
    _folder_cache.clear()
    _image_cache.clear()
    _cache_timestamps.clear()
    print("✅ Cleared all caches")

# -------------------------------------------------------------------
# Credentials loader
# -------------------------------------------------------------------
def _load_service_account_info():
    env_value = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")
    try:
        # Case 1: env contains JSON itself
        if env_value and env_value.strip().startswith("{"):
            return json.loads(env_value)

        # Case 2: env contains a file path (recommended)
        if env_value and os.path.exists(env_value):
            with open(env_value, "r", encoding="utf-8") as f:
                return json.load(f)

        # Case 3: fallback file
        fallback = os.path.join(os.path.dirname(__file__), "credentials.json")
        if os.path.exists(fallback):
            with open(fallback, "r", encoding="utf-8") as f:
                return json.load(f)

        print("❌ No service account JSON found. "
              "Set GOOGLE_SERVICE_ACCOUNT_JSON to a JSON string or file path.")
        return None
    except Exception as e:
        print(f"❌ Failed to load service account JSON: {e}")
        return None

def _scopes():
    # Full Drive scope + file scope + readonly (safe for reading CSVs)
    return [
        "https://www.googleapis.com/auth/drive",
        "https://www.googleapis.com/auth/drive.file",
        "https://www.googleapis.com/auth/drive.readonly",
    ]

# -------------------------------------------------------------------
# Service factory
# -------------------------------------------------------------------
def create_drive_service():
    """
    Build a Drive v3 service using the SERVICE ACCOUNT only.
    This is used for all CSV read/write + folder create/rename/delete.
    (Image uploads will keep using get_drive_service_for_upload() separately.)
    """
    try:
        print("🔐 Initializing Google Drive service (SERVICE ACCOUNT only)...")
        info = _load_service_account_info()
        if not info:
            print("❌ No service-account JSON found.")
            return None

        # Normalize key newlines if needed
        if "private_key" in info and "\\n" in info["private_key"]:
            info["private_key"] = info["private_key"].replace("\\n", "\n")

        sa_creds = Credentials.from_service_account_info(info, scopes=_scopes())
        service = build("drive", "v3", credentials=sa_creds, cache_discovery=False)
        try:
            about = service.about().get(fields="user,storageQuota").execute()
            print(f"✅ SA ready as: {about.get('user', {}).get('emailAddress', 'unknown')}")
        except Exception as e:
            print(f"⚠️ SA about() warn: {e}")
        return service
    except Exception as e:
        print(f"❌ create_drive_service error: {e}")
        return None


def clear_csv_cache(file_id: str | None = None):
    """
    Clear CSV cache for a specific file_id (or all if file_id is None).
    Used by main app to ensure fresh reads after save.
    """
    try:
        if file_id:
            ckey = f"csv::{file_id}"
            _file_cache.pop(ckey, None)
            _cache_timestamps.pop(ckey, None)
        else:
            _file_cache.clear()
            _cache_timestamps.clear()
        print(f"✅ Cleared csv cache for {file_id or 'ALL'}")
    except Exception as e:
        print(f"⚠️ clear_csv_cache error: {e}")


# -------------------------------------------------------------------
# CSV helpers
# -------------------------------------------------------------------
# google_drive_service.py
# Replace your existing load_csv_from_drive with this version

def load_csv_from_drive(service, file_id: str, max_retries: int = 3, **kwargs) -> pd.DataFrame:
    if not service:
        print("❌ load_csv_from_drive: service is None")
        return pd.DataFrame()

    if not file_id or len(str(file_id)) < 8:
        print(f"❌ load_csv_from_drive: invalid file_id '{file_id}'")
        return pd.DataFrame()

    cache_key = f"csv::{file_id}"
    if _is_cache_valid(cache_key, 300):
        print("💾 Using cached CSV")
        return _file_cache[cache_key].copy()

    for attempt in range(1, max_retries + 1):
        try:
            print(f"📥 Loading CSV (try {attempt}/{max_retries}) id={file_id}")
            meta = service.files().get(fileId=file_id, fields="id,name,size,mimeType").execute()

            if not isinstance(meta, dict):
                print(f"⚠️ Unexpected meta type ({type(meta)}) for file_id={file_id}")
                raise RuntimeError("Unexpected metadata type returned from Drive API")

            mime = meta.get("mimeType", "")
            print(f"📄 File: {meta.get('name')} ({meta.get('size', '0')} bytes, {mime})")

            if 'folder' in mime:
                print(f"❌ File id {file_id} is a FOLDER. Returning empty DataFrame.")
                return pd.DataFrame()

            size = meta.get("size")
            if size in [None, "0", 0]:
                print(f"⚠️ File id {file_id} has size={size} (empty). Returning empty DataFrame.")
                return pd.DataFrame()

            # Download media
            req = service.files().get_media(fileId=file_id)
            buf = BytesIO()
            downloader = MediaIoBaseDownload(buf, req)
            done = False
            while not done:
                status, done = downloader.next_chunk()
                if status:
                    prog = int(status.progress() * 100)
                    if prog % 25 == 0:
                        print(f"📊 Download progress: {prog}%")
            
            buf.seek(0)
            content = buf.read().decode("utf-8", errors="replace")
            if not content.strip():
                print("⚠️ CSV empty (no textual content)")
                return pd.DataFrame()

            # 🔧 FIX: Handle header-only files properly
            lines = content.strip().split('\n')
            if len(lines) <= 1:
                # Only headers, no data rows
                df = pd.read_csv(StringIO(content))
                print(f"📋 Header-only CSV detected: {list(df.columns)}")
                _set_cache(cache_key, df.copy(), _file_cache)
                return df
            
            df = pd.read_csv(StringIO(content))
            if df is None:
                print("⚠️ Parsed DataFrame is None")
                return pd.DataFrame()

            _set_cache(cache_key, df.copy(), _file_cache)
            print(f"✅ Loaded {len(df)} rows, {len(df.columns)} cols")
            return df

        except HttpError as he:
            status_code = getattr(he.resp, "status", None)
            print(f"❌ HTTP {status_code} on load: {he}")
            if status_code in (403, 404):
                print("⚠️ Received 403/404 from Drive; returning empty DataFrame")
                return pd.DataFrame()
            time.sleep(2 * attempt)

        except Exception as e:
            import ssl
            es = str(e)
            if isinstance(e, ssl.SSLError) or 'WRONG_VERSION_NUMBER' in es or 'SSLError' in es or 'DECRYPTION_FAILED' in es:
                print(f"❌ SSL error while loading CSV (try {attempt}): {e}")
                if attempt < max_retries:
                    time.sleep(3 * attempt)  # Longer delay for SSL issues
                    continue
            else:
                print(f"❌ load error (try {attempt}): {e}")
            time.sleep(1 * attempt)

    print(f"⚠️ All {max_retries} attempts failed for id={file_id}. Returning empty DataFrame.")
    return pd.DataFrame()



def save_csv_to_drive(service, df: pd.DataFrame, file_id: str, max_retries: int = 3) -> bool:
    if not service:
        print("❌ save_csv_to_drive: service is None")
        return False
    if df is None or df.empty:
        print("⚠️ save_csv_to_drive: empty DataFrame")
        return False
    if not file_id or len(str(file_id)) < 8:
        print(f"❌ save_csv_to_drive: invalid file_id '{file_id}'")
        return False

    for attempt in range(1, max_retries + 1):
        try:
            print(f"💾 Saving CSV (try {attempt}/{max_retries}) id={file_id}")
            csv_buf = StringIO()
            df.to_csv(csv_buf, index=False)
            content = csv_buf.getvalue()

            media = MediaIoBaseUpload(
                BytesIO(content.encode("utf-8")),
                mimetype="text/csv",
                resumable=True,
            )

            service.files().update(
                fileId=file_id,
                media_body=media,
                fields="id,name,size"
            ).execute()

            # bust CSV cache
            ckey = f"csv::{file_id}"
            _file_cache.pop(ckey, None)
            _cache_timestamps.pop(ckey, None)
            print("✅ CSV saved & cache cleared")
            return True
        except HttpError as he:
            status_code = getattr(he.resp, "status", None)
            print(f"❌ HTTP {status_code} on save: {he}")
            if status_code in (403, 404):
                break
            time.sleep(2 * attempt)
        except Exception as e:
            print(f"❌ save error (try {attempt}): {e}")
            time.sleep(1 * attempt)
    return False

# -------------------------------------------------------------------
# Drive search helpers
# -------------------------------------------------------------------
def find_file_by_name(service, filename: str, parent_folder_id: str | None = None, max_retries: int = 2):
    if not service:
        print("❌ find_file_by_name: service is None")
        return None
    if not filename:
        return None

    cache_key = f"file::{parent_folder_id or 'root'}::{filename}"
    if _is_cache_valid(cache_key, 600):
        return _file_cache.get(cache_key)

    query = f"name = '{filename}' and trashed = false"
    if parent_folder_id:
        query += f" and '{parent_folder_id}' in parents"

    for attempt in range(1, max_retries + 1):
        try:
            res = service.files().list(
                q=query,
                spaces="drive",
                fields="files(id,name)",
                pageSize=5
            ).execute()
            files = res.get("files", [])
            if files:
                fid = files[0]["id"]
                _set_cache(cache_key, fid, _file_cache)
                print(f"✅ Found file '{filename}': {fid}")
                return fid
            return None
        except Exception as e:
            print(f"❌ find_file_by_name (try {attempt}): {e}")
            time.sleep(1)
    return None

def find_folder_by_name(service, folder_name: str, parent_folder_id: str | None = None, max_retries: int = 2):
    if not service:
        print("❌ find_folder_by_name: service is None")
        return None
    if not folder_name:
        return None

    cache_key = f"folder::{parent_folder_id or 'root'}::{folder_name}"
    if _is_cache_valid(cache_key, 600):
        return _folder_cache.get(cache_key)

    query = (
        f"name = '{folder_name}' and mimeType = 'application/vnd.google-apps.folder' "
        f"and trashed = false"
    )
    if parent_folder_id:
        query += f" and '{parent_folder_id}' in parents"

    for attempt in range(1, max_retries + 1):
        try:
            res = service.files().list(
                q=query,
                spaces="drive",
                fields="files(id,name)",
                pageSize=5
            ).execute()
            folders = res.get("files", [])
            if folders:
                fid = folders[0]["id"]
                _set_cache(cache_key, fid, _folder_cache)
                print(f"✅ Found folder '{folder_name}': {fid}")
                return fid
            return None
        except Exception as e:
            print(f"❌ find_folder_by_name (try {attempt}): {e}")
            time.sleep(1)
    return None

def list_drive_files(service, folder_id: str | None = None, max_retries: int = 2):
    if not service:
        print("❌ list_drive_files: service is None")
        return []

    query = "trashed = false"
    if folder_id:
        query += f" and '{folder_id}' in parents"

    for attempt in range(1, max_retries + 1):
        try:
            res = service.files().list(
                q=query,
                spaces="drive",
                fields="files(id,name,mimeType)",
                pageSize=200
            ).execute()
            return res.get("files", [])
        except Exception as e:
            print(f"❌ list_drive_files (try {attempt}): {e}")
            time.sleep(1)
    return []

# -------------------------------------------------------------------
# Public URL helper (sets 'anyone with link' if needed)
# -------------------------------------------------------------------
def get_public_url(service, file_id: str, max_retries: int = 2):
    if not file_id:
        return None

    cache_key = f"url::{file_id}"
    if _is_cache_valid(cache_key, 3600):
        return _image_cache[cache_key]

    for attempt in range(1, max_retries + 1):
        try:
            if service:
                try:
                    service.permissions().create(
                        fileId=file_id,
                        body={"type": "anyone", "role": "reader"}
                    ).execute()
                    print(f"🔓 Made file public: {file_id}")
                except HttpError as he:
                    print(f"⚠️ permissions.create warn: {he}")

            url = f"https://drive.google.com/thumbnail?id={file_id}&sz=w1000"
            _set_cache(cache_key, url, _image_cache)
            return url
        except Exception as e:
            print(f"❌ get_public_url (try {attempt}): {e}")
            time.sleep(1)

    return f"https://drive.google.com/thumbnail?id={file_id}&sz=w1000"

# -------------------------------------------------------------------
# File/CSV creation helper
# -------------------------------------------------------------------
def create_file_if_not_exists(service, filename: str, parent_folder_id: str | None = None):
    if not service:
        return None
    try:
        existing = find_file_by_name(service, filename, parent_folder_id)
        if existing:
            return existing

        meta = {"name": filename, "mimeType": "text/csv"}
        if parent_folder_id:
            meta["parents"] = [parent_folder_id]

        csv = StringIO()
        pd.DataFrame().to_csv(csv, index=False)
        media = MediaIoBaseUpload(BytesIO(csv.getvalue().encode("utf-8")), mimetype="text/csv")

        f = service.files().create(body=meta, media_body=media, fields="id").execute()
        print(f"✅ Created CSV '{filename}' → {f.get('id')}")
        return f.get("id")
    except Exception as e:
        print(f"❌ create_file_if_not_exists error: {e}")
        return None

# -------------------------------------------------------------------
# Folder creation for Subjects
# -------------------------------------------------------------------
def create_subject_folder(service, subject_name: str):
    if not service:
        raise RuntimeError("create_subject_folder: service is None")

    parent = os.getenv("IMAGES_FOLDER_ID") or os.getenv("ROOT_FOLDER_ID")
    if not parent:
        raise RuntimeError("IMAGES_FOLDER_ID/ROOT_FOLDER_ID not set in environment")

    try:
        q = (
            "mimeType='application/vnd.google-apps.folder' and "
            f"'{parent}' in parents and trashed=false"
        )
        res = service.files().list(q=q, fields="files(id,name)").execute()
        for f in res.get("files", []):
            if f["name"].strip().lower() == subject_name.strip().lower():
                print(f"📂 Reusing existing subject folder: {f['name']} ({f['id']})")
                return f["id"], datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    except Exception as e:
        print(f"⚠️ list existing subject folders warn: {e}")

    meta = {
        "name": subject_name.strip(),
        "mimeType": "application/vnd.google-apps.folder",
        "parents": [parent]
    }
    f = service.files().create(body=meta, fields="id").execute()
    print(f"✅ Created subject folder '{subject_name}' → {f.get('id')}")
    return f.get("id"), datetime.now().strftime("%Y-%m-%d %H:%M:%S")

# -------------------------------------------------------------------
# USER OAuth Drive client for uploads (handles both token.json and client_secret.json)
# -------------------------------------------------------------------
def create_drive_service_user():
    """
    Try to build a Drive service using a user OAuth token file.

    Works with:
    - An existing authorized token (dict with refresh_token, client_id, client_secret, token_uri, ...), OR
    - A client secret JSON ({"installed": {...}} or {"web": {...}}) — will run a one-time browser flow,
      then overwrite the same file with a proper token.json (with refresh_token) for next runs.

    Env: GOOGLE_SERVICE_TOKEN_JSON -> path to the JSON file (defaults to ./token.json).
    """
    token_path = os.getenv("GOOGLE_SERVICE_TOKEN_JSON", "token.json")
    scopes = _scopes()

    try:
        if not token_path or not os.path.exists(token_path):
            print(f"❌ token file not found. Set GOOGLE_SERVICE_TOKEN_JSON (got: {token_path})")
            return None

        with open(token_path, "r", encoding="utf-8") as f:
            raw = f.read().strip()

        # Some people save JSON in one line; that's fine.
        data = json.loads(raw)

        # CASE 1: Already an authorized-user token.json
        if all(k in data for k in ("refresh_token", "token_uri", "client_id", "client_secret")):
            creds = UserCredentials.from_authorized_user_info(data, scopes=scopes)
            service = build("drive", "v3", credentials=creds, cache_discovery=False)
            try:
                about = service.about().get(fields="user").execute()
                print(f"✅ User OAuth client ready (existing token). Acting as: {about.get('user',{}).get('emailAddress','Unknown')}")
            except Exception:
                print("✅ User OAuth client ready (existing token).")
            return service

        # CASE 2: Client secret JSON → run OAuth flow and save proper token.json back to same path
        if "installed" in data or "web" in data:
            client_config = {"installed": data["installed"]} if "installed" in data else {"web": data["web"]}
            flow = InstalledAppFlow.from_client_config(client_config, scopes=scopes)
            creds = flow.run_local_server(port=0, prompt="consent")

            service = build("drive", "v3", credentials=creds, cache_discovery=False)
            try:
                about = service.about().get(fields="user").execute()
                print(f"✅ User OAuth client ready (new token). Acting as: {about.get('user',{}).get('emailAddress','Unknown')}")
            except Exception:
                print("✅ User OAuth client ready (new token).")

            # Persist authorized token in the SAME file for future runs
            token_to_save = {
                "token": creds.token,
                "refresh_token": creds.refresh_token,
                "token_uri": creds.token_uri,
                "client_id": creds.client_id,
                "client_secret": creds.client_secret,
                "scopes": list(creds.scopes or scopes),
                "expiry": creds.expiry.isoformat() if getattr(creds, "expiry", None) else None,
            }
            with open(token_path, "w", encoding="utf-8") as f:
                json.dump(token_to_save, f)
            print(f"💾 Saved authorized token to {token_path}")
            return service

        # CASE 3: Unknown format → cannot proceed
        print("❌ Provided file is neither an authorized token nor a client secret (web/installed).")
        return None

    except Exception as e:
        print(f"❌ Failed to build user OAuth Drive client: {e}")
        return None

def get_drive_service_for_upload():
    """
    Prefer user OAuth client for uploads. If not available, raise a clear error
    (do NOT silently fall back to service account, which has 0 quota on My Drive).
    """
    user_service = create_drive_service_user()
    if user_service:
        return user_service

    raise RuntimeError(
        "Uploads require a user OAuth token (token.json). "
        "Set GOOGLE_SERVICE_TOKEN_JSON to your token.json or client_secret.json path. "
        "If you provide client_secret.json, a one-time browser window will open to create token.json."
    )
