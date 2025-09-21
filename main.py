# main.py - FIXED VERSION with explicit Google Drive initialization
from flask import Flask, render_template, request, redirect, url_for, session, flash, jsonify
import pandas as pd
import os
from datetime import datetime, timezone
from functools import wraps
import json
import time
import secrets
import string
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import secrets
import re
from fpdf import FPDF
from PIL import Image
import requests
from io import BytesIO
import tempfile
from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas
from reportlab.lib.utils import ImageReader
from dotenv import load_dotenv
from admin import admin_bp
import threading
import uuid
from flask_session import Session
import tempfile
from sessions import (
    generate_session_token, save_session_record, 
    invalidate_session, update_last_seen, set_exam_active, 
    require_valid_session, require_user_role, require_admin_role
)
from email_utils import send_credentials_email
import threading
cache_lock = threading.RLock()
import gc
gc.set_threshold(700, 10, 10) 
from flask import Response
from reportlab.lib.utils import simpleSplit 
import math


# CRITICAL: Load environment variables FIRST
load_dotenv()

# CRITICAL: Check if running on Render or local
IS_PRODUCTION = os.environ.get('RENDER') is not None  # Render sets this automatically
if IS_PRODUCTION:
    print("🌐 Running on Render (Production)")
else:
    print("💻 Running locally")

# Import Google Drive service
from google_drive_service import (
    create_drive_service, load_csv_from_drive, save_csv_to_drive,
    find_file_by_name, get_public_url, find_folder_by_name,
    list_drive_files, create_file_if_not_exists
)

app = Flask(__name__)
app.secret_key = os.environ.get('SECRET_KEY')
app.config['SESSION_PERMANENT'] = True
app.config['PERMANENT_SESSION_LIFETIME'] = 7200  # 2 hours


@app.before_request
def before_request_security_check():
    """Minimal security check"""
    
    # Skip static files and auth pages
    skip_paths = [
        '/static/', '/login', '/admin/login', '/admin/admin_login', 
        '/', '/home', '/forgot-password', '/reset-password', 
        '/request-admin-access', '/favicon.ico', '/api/',
        '/dashboard'  # ADD THIS to prevent interference
    ]
    
    if any(request.path.startswith(path) for path in skip_paths):
        return
    
    # Simple portal conflict check ONLY for wrong portal access
    if request.path.startswith('/admin/') and session.get('user_id') and not session.get('admin_id'):
        flash("Please login as Admin to access Admin portal.", "warning")
        return redirect(url_for("login"))


from latex_editor import latex_bp
app.register_blueprint(latex_bp) 

# Use filesystem for Render single-instance free tier. For multi-instance use Redis.
SESSION_TYPE = os.environ.get("SESSION_TYPE", "filesystem")  # default to filesystem
# session files dir
SESSION_FILE_DIR = os.environ.get("SESSION_FILE_DIR",
                                  os.path.join(tempfile.gettempdir(), "flask_session"))
os.makedirs(SESSION_FILE_DIR, exist_ok=True)

app.config['SESSION_TYPE'] = SESSION_TYPE
app.config['SESSION_FILE_DIR'] = SESSION_FILE_DIR
app.config['SESSION_PERMANENT'] = False  # keep sessions non-permanent by default
# set lifetime if you want (seconds) — keep > exam duration, e.g., 3 hours (10800)
from datetime import timedelta
app.config['PERMANENT_SESSION_LIFETIME'] = timedelta(seconds=int(os.environ.get("PERMANENT_SESSION_LIFETIME", 10800)))
# security cookies
app.config['SESSION_COOKIE_HTTPONLY'] = True
# In production set SESSION_COOKIE_SECURE = True if using HTTPS (Render provides HTTPS)
app.config['SESSION_COOKIE_SECURE'] = True if os.environ.get("FORCE_SECURE_COOKIES", "1") == "1" else False

# Initialize server-side session
Session(app)

print(f"✅ Server-side sessions enabled: type={app.config['SESSION_TYPE']}, dir={app.config.get('SESSION_FILE_DIR')}")



# Register admin blueprint
app.register_blueprint(admin_bp, url_prefix="/admin")

# Configuration
USERS_CSV = 'users.csv'
EXAMS_CSV = 'exams.csv'
QUESTIONS_CSV = 'questions.csv'
RESULTS_CSV = 'results.csv'
RESPONSES_CSV = 'responses.csv'

# CRITICAL: Debug environment variables
print("🔍 Checking environment variables...")
required_env_vars = [
    'SECRET_KEY', 'GOOGLE_SERVICE_ACCOUNT_JSON',
    'USERS_FILE_ID', 'EXAMS_FILE_ID', 'QUESTIONS_FILE_ID', 'RESULTS_FILE_ID', 'RESPONSES_FILE_ID'
]

for var in required_env_vars:
    value = os.environ.get(var)
    if value:
        if var == 'GOOGLE_SERVICE_ACCOUNT_JSON':
            print(f"✅ {var}: Present (length: {len(value)} chars)")
        elif var == 'SECRET_KEY':
            print(f"✅ {var}: Present")
        else:
            print(f"✅ {var}: {value}")
    else:
        print(f"❌ {var}: MISSING!")

# Google Drive File IDs
USERS_FILE_ID = os.environ.get('USERS_FILE_ID')
EXAMS_FILE_ID = os.environ.get('EXAMS_FILE_ID')
QUESTIONS_FILE_ID = os.environ.get('QUESTIONS_FILE_ID')
RESULTS_FILE_ID = os.environ.get('RESULTS_FILE_ID')
RESPONSES_FILE_ID = os.environ.get('RESPONSES_FILE_ID')
EXAM_ATTEMPTS_FILE_ID = os.environ.get('EXAM_ATTEMPTS_FILE_ID')
REQUESTS_RAISED_FILE_ID = os.environ.get("REQUESTS_RAISED_FILE_ID")

DRIVE_FILE_IDS = {
    'users': USERS_FILE_ID,
    'exams': EXAMS_FILE_ID,
    'questions': QUESTIONS_FILE_ID,
    'results': RESULTS_FILE_ID,
    'responses': RESPONSES_FILE_ID,
    'exam_attempts': EXAM_ATTEMPTS_FILE_ID,
    'requests_raised': REQUESTS_RAISED_FILE_ID
}

# Google Drive Folder IDs
ROOT_FOLDER_ID = os.environ.get('ROOT_FOLDER_ID')
IMAGES_FOLDER_ID = os.environ.get('IMAGES_FOLDER_ID')
PHYSICS_FOLDER_ID = os.environ.get('PHYSICS_FOLDER_ID')
CHEMISTRY_FOLDER_ID = os.environ.get('CHEMISTRY_FOLDER_ID')
MATH_FOLDER_ID = os.environ.get('MATH_FOLDER_ID')
CIVIL_FOLDER_ID = os.environ.get('CIVIL_FOLDER_ID')

DRIVE_FOLDER_IDS = {
    'root': ROOT_FOLDER_ID,
    'images': IMAGES_FOLDER_ID,
    'physics': PHYSICS_FOLDER_ID,
    'chemistry': CHEMISTRY_FOLDER_ID,
    'math': MATH_FOLDER_ID,
    'civil': CIVIL_FOLDER_ID
}

# Global drive service instance
drive_service = None

# ============================
# Global In-Memory Cache
# ============================
app_cache = {
    'data': {},
    'images': {},
    'timestamps': {},
    'force_refresh': False   # Flag for forcing reload
}

from flask import current_app
# Cache optimization
app_cache['max_size'] = 100  # Limit cache size
app_cache['cleanup_interval'] = 300  # 5 minutes

# Enhanced logging decorator for key functions
def debug_logging(func_name):
    """Decorator to add detailed logging to functions"""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            start_time = time.time()
            print(f"[DEBUG] {func_name} - START - Args: {len(args)}, Kwargs: {list(kwargs.keys())}")
            
            try:
                result = func(*args, **kwargs)
                end_time = time.time()
                
                # Log result summary
                if hasattr(result, '__len__'):
                    result_info = f"Length: {len(result)}"
                elif isinstance(result, tuple) and len(result) == 2:
                    result_info = f"Tuple: ({result[0]}, '{result[1][:50]}...')"
                else:
                    result_info = f"Type: {type(result).__name__}"
                
                print(f"[DEBUG] {func_name} - SUCCESS - {result_info} - Time: {end_time - start_time:.3f}s")
                return result
                
            except Exception as e:
                end_time = time.time()
                print(f"[DEBUG] {func_name} - ERROR - {str(e)} - Time: {end_time - start_time:.3f}s")
                raise
                
        return wrapper
    return decorator 


def clear_user_cache():
    """Enhanced cache clearing for immediate data refresh"""
    global app_cache
    
    try:
        # Clear global app cache
        cache_keys_to_clear = [k for k in app_cache.get('data', {}).keys() if 'users' in k.lower()]
        for key in cache_keys_to_clear:
            app_cache['data'].pop(key, None)
            app_cache['timestamps'].pop(key, None)
        
        # Force refresh flag
        app_cache['force_refresh'] = True
        
        # Clear session cache if available
        try:
            from flask import session
            session_keys_to_clear = [k for k in list(session.keys()) if 'csv_users' in k or 'user_data' in k]
            for k in session_keys_to_clear:
                session.pop(k, None)
        except:
            pass
        
        # Clear Google Drive service cache if available
        try:
            from google_drive_service import clear_csv_cache
            if DRIVE_FILE_IDS.get('users'):
                clear_csv_cache(DRIVE_FILE_IDS['users'])
        except:
            pass
        
        print("Enhanced cache clearing completed")
        
    except Exception as e:
        print(f"Error in enhanced cache clearing: {e}")



# =============================================
# CONCURRENT SAFETY SYSTEM
# =============================================

# Global file locks
file_locks = {}
lock_registry = threading.RLock()

def get_file_lock(file_key):
    """Get or create a lock for a specific file"""
    with lock_registry:
        if file_key not in file_locks:
            file_locks[file_key] = threading.RLock()
        return file_locks[file_key]

def generate_operation_id():
    """Generate unique operation ID"""
    return f"op_{int(time.time())}_{uuid.uuid4().hex[:8]}"

def safe_csv_save_with_retry(df, csv_type, operation_id=None, max_retries=5):
    """Save CSV with retry mechanism - never gives up"""
    if not operation_id:
        operation_id = generate_operation_id()
    
    global drive_service
    file_id = DRIVE_FILE_IDS.get(csv_type)
    
    if not file_id or not drive_service:
        print(f"[{operation_id}] No file ID or drive service for {csv_type}")
        return False
    
    for attempt in range(max_retries):
        try:
            print(f"[{operation_id}] Attempt {attempt + 1} saving {csv_type}")
            success = save_csv_to_drive(drive_service, df, file_id)
            
            if success:
                # Clear cache
                cache_key = f'csv_{csv_type}.csv'
                app_cache['data'].pop(cache_key, None)
                app_cache['timestamps'].pop(cache_key, None)
                print(f"[{operation_id}] Successfully saved {csv_type} on attempt {attempt + 1}")
                return True
            else:
                print(f"[{operation_id}] Save failed for {csv_type} on attempt {attempt + 1}")
                
        except Exception as e:
            print(f"[{operation_id}] Exception on attempt {attempt + 1} for {csv_type}: {e}")
        
        # Wait before retry (exponential backoff)
        if attempt < max_retries - 1:
            wait_time = (2 ** attempt) * 0.5  # 0.5, 1, 2, 4, 8 seconds
            print(f"[{operation_id}] Waiting {wait_time}s before retry...")
            time.sleep(wait_time)
    
    print(f"[{operation_id}] FAILED to save {csv_type} after {max_retries} attempts")
    return False

def safe_csv_load(filename, operation_id=None):
    """Safe CSV loading with file locking"""
    if not operation_id:
        operation_id = generate_operation_id()
    
    file_lock = get_file_lock(filename.replace('.csv', ''))
    
    with file_lock:
        print(f"[{operation_id}] Loading {filename} safely")
        return load_csv_from_drive_direct(filename)

def safe_dual_file_save(results_df, responses_df, new_result, response_records):
    """Atomically save both results and responses with retry"""
    operation_id = generate_operation_id()
    
    # Lock both files together
    with get_file_lock('results'):
        with get_file_lock('responses'):
            print(f"[{operation_id}] Starting dual file save with retry mechanism")
            
            # Prepare dataframes
            new_results_df = pd.concat([results_df, pd.DataFrame([new_result])], ignore_index=True)
            new_responses_df = pd.concat([responses_df, pd.DataFrame(response_records)], ignore_index=True)
            
            # Save results with retry
            print(f"[{operation_id}] Saving results...")
            results_success = safe_csv_save_with_retry(new_results_df, 'results', f"{operation_id}_results")
            
            if results_success:
                print(f"[{operation_id}] Results saved! Now saving responses...")
                # Save responses with retry
                responses_success = safe_csv_save_with_retry(new_responses_df, 'responses', f"{operation_id}_responses")
                
                if responses_success:
                    print(f"[{operation_id}] Both files saved successfully!")
                    return True, "Both results and responses saved successfully"
                else:
                    print(f"[{operation_id}] Responses failed even after retries!")
                    return False, "Failed to save responses after multiple attempts"
            else:
                print(f"[{operation_id}] Results failed even after retries!")
                return False, "Failed to save results after multiple attempts"

def safe_user_register(email, full_name):
    """Safe user registration with retry mechanism"""
    operation_id = generate_operation_id()
    
    with get_file_lock('users'):
        print(f"[{operation_id}] Registering user safely: {email}")
        
        # Load current users
        users_df = safe_csv_load('users.csv', operation_id)
        
        # Check if email exists
        if not users_df.empty and email.lower() in users_df['email'].str.lower().values:
            existing_user = users_df[users_df['email'].str.lower() == email.lower()].iloc[0]
            return False, "exists", {
                'username': existing_user['username'],
                'password': existing_user['password'],
                'full_name': existing_user['full_name']
            }
        
        # Create new user
        existing_usernames = users_df['username'].tolist() if not users_df.empty else []
        username = generate_username(full_name, existing_usernames)
        password = generate_password()
        
        next_id = 1
        if not users_df.empty and 'id' in users_df.columns:
            next_id = int(users_df['id'].fillna(0).astype(int).max()) + 1
        
        new_user = {
            'id': next_id,
            'full_name': full_name,
            'username': username,
            'email': email.lower(),
            'password': password,
            'created_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'role': 'user'
        }
        
        # Prepare new dataframe
        if users_df.empty:
            new_df = pd.DataFrame([new_user])
        else:
            new_df = pd.concat([users_df, pd.DataFrame([new_user])], ignore_index=True)
        
        # Save with retry mechanism
        if safe_csv_save_with_retry(new_df, 'users', operation_id):
            return True, "success", {
                'username': username,
                'password': password,
                'full_name': full_name
            }
        else:
            return False, "save_failed", None


def ensure_required_files():
    """Ensure all required CSV files exist in Google Drive"""
    global drive_service

    if not drive_service:
        print("❌ No Google Drive service for file verification")
        return

    required_files = {
        'users.csv': DRIVE_FILE_IDS['users'],
        'exams.csv': DRIVE_FILE_IDS['exams'],
        'questions.csv': DRIVE_FILE_IDS['questions'],
        'results.csv': DRIVE_FILE_IDS['results'],
        'responses.csv': DRIVE_FILE_IDS['responses']
    }

    for filename, file_id in required_files.items():
        if not file_id or file_id.startswith('YOUR_'):
            print(f"⚠️ {filename}: File ID not configured properly")
            continue
            
        try:
            # Try to get file metadata to check if it exists
            meta = drive_service.files().get(fileId=file_id, fields="id,name,size").execute()
            print(f"✅ Verified {filename}: {meta.get('name')} ({meta.get('size', '0')} bytes)")
        except Exception as e:
            print(f"❌ Error verifying {filename} (ID: {file_id}): {e}")


# -------------------------
# Helper Functions
# -------------------------
def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'user_id' not in session:
            flash('Please login first', 'error')
            return redirect(url_for('login'))
        return f(*args, **kwargs)

    return decorated_function


def parse_correct_answers(correct_answer_str, question_type):
    """Parse correct answers based on question type"""
    if pd.isna(correct_answer_str) or str(correct_answer_str).strip() == '':
        if question_type == 'MSQ':
            return []
        else:
            return None

    if question_type == 'MSQ':
        # Multiple correct answers (comma separated)
        return [ans.strip().upper() for ans in str(correct_answer_str).split(',') if ans.strip()]
    elif question_type == 'NUMERIC':
        # Numerical answer
        try:
            return float(str(correct_answer_str).strip())
        except (ValueError, TypeError):
            return None
    else:  # MCQ
        # Single correct answer
        return str(correct_answer_str).strip().upper()


def init_drive_service():
    """Initialize the Google Drive service"""
    global drive_service
    try:
        print("🔧 Initializing Google Drive service...")
        drive_service = create_drive_service()
        if drive_service:
            print("✅ Google Drive service initialized successfully!")

            # FIXED: Ensure all required files exist
            ensure_required_files()
            return True
        else:
            print("❌ Failed to initialize Google Drive service")
            return False
    except Exception as e:
        print(f"❌ Failed to initialize Google Drive service: {e}")
        return False


def ensure_required_files():
    """Ensure all required CSV files exist in Google Drive"""
    global drive_service

    if not drive_service:
        return

    required_files = {
        'users.csv': DRIVE_FILE_IDS['users'],
        'exams.csv': DRIVE_FILE_IDS['exams'],
        'questions.csv': DRIVE_FILE_IDS['questions'],
        'results.csv': DRIVE_FILE_IDS['results'],
        'responses.csv': DRIVE_FILE_IDS['responses']
    }

    for filename, file_id in required_files.items():
        try:
            # Try to load the file to check if it exists
            test_df = load_csv_from_drive(drive_service, file_id)
            if test_df is not None:
                print(f"✅ Verified {filename} exists")
            else:
                print(f"⚠️ {filename} may not exist, but ID is configured")
        except Exception as e:
            print(f"❌ Error verifying {filename}: {e}")


# main.py - replace load_csv_with_cache with this
@debug_logging("load_csv_with_cache")
def load_csv_with_cache(filename, force_reload=False):
    """Load CSV with smart caching - fixed cache validation and DataFrame consistency."""
    global app_cache

    cache_key = f'csv_{filename}'
    cache_duration = 300  # 5 minutes

    # Force reload conditions
    force_conditions = [
        app_cache.get('force_refresh', False),
        session.get('force_refresh', False),
        force_reload,
        filename == 'exam_attempts.csv'  # Keep attempts fresh
    ]
    
    if any(force_conditions):
        print(f"Force refresh triggered for {filename}")
        app_cache['force_refresh'] = False
        session.pop('force_refresh', None)
        force_reload = True

    # Check cache validity
    if not force_reload and cache_key in app_cache['data']:
        cached_time = app_cache['timestamps'].get(cache_key, 0)
        if time.time() - cached_time < cache_duration:
            cached_df = app_cache['data'][cache_key]
            # CRITICAL: Validate cached DataFrame
            if cached_df is not None and hasattr(cached_df, 'empty'):
                print(f"Using cached data for {filename} ({len(cached_df)} rows)")
                return cached_df.copy()
            else:
                print(f"Invalid cached data for {filename}, reloading...")

    # Load fresh data
    print(f"Loading fresh data for {filename}")
    df = load_csv_from_drive_direct(filename)

    # Validate loaded DataFrame
    if df is None:
        print(f"WARNING: load_csv_from_drive_direct returned None for {filename}")
        df = pd.DataFrame()
    elif not hasattr(df, 'empty'):
        print(f"ERROR: Invalid DataFrame type for {filename}: {type(df)}")
        df = pd.DataFrame()

    # Special handling for exam_attempts
    if filename == 'exam_attempts.csv':
        expected_cols = ['id','student_id','exam_id','attempt_number','status','start_time','end_time']
        if df.empty and len(df.columns) == 0:
            df = pd.DataFrame(columns=expected_cols)
            print(f"Created empty exam_attempts DataFrame with headers")
        elif not df.empty:
            for col in expected_cols:
                if col not in df.columns:
                    df[col] = pd.NA

    # Cache the validated result
    try:
        with cache_lock:
            app_cache['data'][cache_key] = df.copy()
            app_cache['timestamps'][cache_key] = time.time()
        print(f"Cached {len(df)} records for {filename}")
    except Exception as e:
        print(f"Error caching {filename}: {e}")

    return df




def load_csv_from_drive_direct(filename):
    """
    Robust loader: use safe_drive_csv_load -> fallback to app_cache -> fallback to local file.
    Keeps the server stable on transient Drive/SSL errors.
    """
    global drive_service, app_cache

    cache_key = f'csv_{filename}'
    file_id_key = filename.replace('.csv', '')
    file_id = DRIVE_FILE_IDS.get(file_id_key)

    # 1) If no drive service or no file id, try cache/local and return empty DF if nothing found
    if drive_service is None or not file_id:
        print(f"load_csv_from_drive_direct: No drive_service or file_id for {filename}")
        # try in-memory cache
        try:
            cached = app_cache.get('data', {}).get(cache_key)
            if cached is not None:
                print(f"📋 Returning cached copy for {filename} (drive unavailable).")
                return cached.copy()
        except Exception:
            pass
        # try local file fallback
        local_path = os.path.join(os.getcwd(), filename)
        if os.path.exists(local_path):
            try:
                df_local = pd.read_csv(local_path, dtype=str)
                df_local.columns = df_local.columns.str.strip()
                print(f"📥 Loaded local fallback for {filename} ({len(df_local)} rows).")
                return df_local
            except Exception as e:
                print(f"❌ Failed to read local fallback {filename}: {e}")
        return pd.DataFrame()

    # 2) Try safe drive loader (itself defensive/retries)
    try:
        df = safe_drive_csv_load(drive_service, file_id, friendly_name=filename, max_retries=3)
        if df is not None and not df.empty:
            # canonicalize column names and cache a copy
            try:
                df.columns = df.columns.str.strip()
            except Exception:
                pass
            print(f"Successfully loaded {len(df)} rows from {filename}")
            # update in-memory cache (defensive)
            try:
                app_cache.setdefault('data', {})[cache_key] = df.copy()
                app_cache.setdefault('timestamps', {})[cache_key] = time.time()
                print(f"💾 Cached {len(df)} records for {filename}")
            except Exception:
                pass
            return df
        # If df is empty but has columns treat as valid header-only
        if df is not None and hasattr(df, "columns") and len(df.columns) > 0:
            try:
                df.columns = df.columns.str.strip()
            except Exception:
                pass
            print(f"Loaded header-only or empty data for {filename} (0 rows, {len(df.columns)} cols).")
            try:
                app_cache.setdefault('data', {})[cache_key] = df.copy()
                app_cache.setdefault('timestamps', {})[cache_key] = time.time()
            except Exception:
                pass
            return df
    except Exception as e:
        err = str(e).lower()
        print(f"Error loading {filename} from Drive: {e}")
        # If SSL/connection transient error, prefer cached copy rather than crash/retry loop
        if 'ssl' in err or 'wrong version number' in err or 'sslv3' in err:
            print(f"⚠️ Transient SSL/Drive error while loading {filename}. Will fallback to cache/local.")
        # fall through to fallback logic

    # 3) If we reached here, Drive fetch failed or returned empty — fallback to cache/local
    try:
        cached = app_cache.get('data', {}).get(cache_key)
        if cached is not None:
            print(f"📋 Falling back to cached copy for {filename} ({len(cached)} rows).")
            return cached.copy()
    except Exception:
        pass

    # 4) Last resort: local file if present
    try:
        local_path = os.path.join(os.getcwd(), filename)
        if os.path.exists(local_path):
            df_local = pd.read_csv(local_path, dtype=str)
            try:
                df_local.columns = df_local.columns.str.strip()
            except Exception:
                pass
            print(f"📥 Loaded local fallback for {filename} ({len(df_local)} rows).")
            # cache it for later
            try:
                app_cache.setdefault('data', {})[cache_key] = df_local.copy()
                app_cache.setdefault('timestamps', {})[cache_key] = time.time()
            except Exception:
                pass
            return df_local
    except Exception as e:
        print(f"❌ Local fallback read failed for {filename}: {e}")

    # 5) nothing found — return empty DataFrame but avoid crashing
    print(f"⚠️ Returning empty DataFrame for {filename} after failures.")
    return pd.DataFrame()



def process_question_image_fixed_ssl_safe(question):
    """Process image path using subjects.csv with SSL-safe retries and fallbacks"""
    global drive_service, app_cache

    image_path = question.get("image_path")

    if (
        image_path is None
        or pd.isna(image_path)
        or str(image_path).strip() in ["", "nan", "NaN", "null", "None"]
    ):
        return False, None

    image_path = str(image_path).strip()
    if not image_path:
        return False, None

    # Cache check first
    cache_key = f"image_{image_path}"
    if cache_key in app_cache["images"]:
        cached_time = app_cache["timestamps"].get(cache_key, 0)
        if time.time() - cached_time < 3600:  # 1 hour
            print(f"⚡ Using cached image URL for {image_path}")
            return True, app_cache["images"][cache_key]

    if drive_service is None:
        print(f"❌ No drive service for image: {image_path}")
        return False, None

    try:
        filename = os.path.basename(image_path)  # e.g. dt-1.png
        subject = os.path.dirname(image_path).lower()  # e.g. math

        # --- 🔍 Find subject folder with SSL-safe retry ---
        folder_id = None
        subjects_file_id = os.environ.get("SUBJECTS_FILE_ID")
        if subjects_file_id:
            # Try multiple times with different approaches for SSL issues
            for attempt in range(3):
                try:
                    print(f"📂 Loading subjects.csv (attempt {attempt + 1})")
                    subjects_df = load_csv_from_drive(drive_service, subjects_file_id)
                    if not subjects_df.empty:
                        subjects_df["subject_name"] = subjects_df["subject_name"].astype(str).str.strip().str.lower()
                        match = subjects_df[subjects_df["subject_name"] == subject.strip().lower()]
                        if not match.empty:
                            folder_id = str(match.iloc[0]["subject_folder_id"])
                            print(f"📂 Found folder for subject '{subject}': {folder_id}")
                            break
                        else:
                            print(f"⚠️ No match for subject '{subject}' in subjects.csv")
                            break
                    else:
                        print(f"⚠️ Empty subjects.csv on attempt {attempt + 1}")
                except Exception as e:
                    error_msg = str(e).lower()
                    if 'ssl' in error_msg or 'timeout' in error_msg:
                        print(f"🔄 SSL/timeout error on attempt {attempt + 1}, retrying...")
                        time.sleep(1 * (attempt + 1))  # Progressive delay
                        continue
                    else:
                        print(f"❌ Non-SSL error reading subjects.csv: {e}")
                        break

        # Fallback to IMAGES_FOLDER_ID if subject folder not found
        if not folder_id and os.environ.get("IMAGES_FOLDER_ID"):
            folder_id = os.environ.get("IMAGES_FOLDER_ID")
            print(f"📂 Fallback to IMAGES folder for subject {subject}: {folder_id}")

        if not folder_id:
            print(f"❌ No folder ID found for subject: {subject}")
            return False, None

        # --- 🔍 Find file inside resolved folder with SSL-safe retry ---
        image_file_id = None
        for attempt in range(3):
            try:
                print(f"🔍 Finding image file (attempt {attempt + 1}): {filename}")
                image_file_id = find_file_by_name(drive_service, filename, folder_id)
                if image_file_id:
                    break
            except Exception as e:
                error_msg = str(e).lower()
                if 'ssl' in error_msg or 'timeout' in error_msg:
                    print(f"🔄 SSL/timeout error finding file, attempt {attempt + 1}")
                    time.sleep(1 * (attempt + 1))
                    continue
                else:
                    print(f"❌ Non-SSL error finding file: {e}")
                    break

        if image_file_id:
            # Get public URL with retry
            image_url = None
            for attempt in range(3):
                try:
                    print(f"🔗 Getting public URL (attempt {attempt + 1})")
                    image_url = get_public_url(drive_service, image_file_id)
                    if image_url:
                        break
                except Exception as e:
                    error_msg = str(e).lower()
                    if 'ssl' in error_msg or 'timeout' in error_msg:
                        print(f"🔄 SSL/timeout error getting URL, attempt {attempt + 1}")
                        time.sleep(1 * (attempt + 1))
                        continue
                    else:
                        print(f"❌ Non-SSL error getting URL: {e}")
                        break
            
            if image_url:
                app_cache["images"][cache_key] = image_url
                app_cache["timestamps"][cache_key] = time.time()
                print(f"✅ Cached image URL: {image_path} -> {image_url}")
                return True, image_url

        print(f"❌ Image file not found: {filename} in folder {folder_id}")
        return False, None

    except Exception as e:
        print(f"❌ Error processing image {image_path}: {e}")
        return False, None

@debug_logging("preload_exam_data_fixed")
def preload_exam_data_fixed(exam_id):
    """
    FIXED: Exam data preloading with proper error handling and validation
    """
    start_time = time.time()
    print(f"Preloading exam data for exam_id: {exam_id}")

    try:
        # CRITICAL: Load questions first with explicit validation
        questions_df = None
        for attempt in range(3):  # Retry loading questions
            try:
                print(f"Loading questions.csv (attempt {attempt + 1})")
                questions_df = load_csv_with_cache('questions.csv', force_reload=(attempt > 0))
                
                # Validate questions DataFrame
                if questions_df is None:
                    print(f"questions.csv returned None on attempt {attempt + 1}")
                    continue
                elif not hasattr(questions_df, 'empty'):
                    print(f"Invalid questions DataFrame type: {type(questions_df)}")
                    continue
                elif questions_df.empty:
                    print(f"questions.csv is empty on attempt {attempt + 1}")
                    if attempt == 2:  # Last attempt
                        return False, "Questions database is empty"
                    continue
                else:
                    print(f"Successfully loaded {len(questions_df)} questions")
                    break
                    
            except Exception as e:
                print(f"Error loading questions.csv (attempt {attempt + 1}): {e}")
                if attempt == 2:  # Last attempt
                    return False, f"Failed to load questions: {str(e)}"
                time.sleep(0.5)  # Brief delay before retry

        if questions_df is None or questions_df.empty:
            return False, "Questions data is unavailable or empty"

        # Load exams data with validation
        exams_df = None
        try:
            exams_df = load_csv_with_cache('exams.csv')
            if exams_df is None or exams_df.empty:
                return False, "Exams data is unavailable"
        except Exception as e:
            print(f"Error loading exams.csv: {e}")
            return False, f"Failed to load exam metadata: {str(e)}"

        # Filter questions for this exam
        exam_id_str = str(exam_id)
        try:
            # Ensure exam_id column exists
            if 'exam_id' not in questions_df.columns:
                return False, "Questions file missing exam_id column"
                
            exam_questions = questions_df[questions_df['exam_id'].astype(str) == exam_id_str]
            print(f"Found {len(exam_questions)} questions for exam {exam_id}")
        except Exception as e:
            print(f"Error filtering questions: {e}")
            return False, f"Error filtering questions for exam {exam_id}"

        if exam_questions.empty:
            # Debug: Show available exam IDs
            try:
                available_ids = sorted(questions_df['exam_id'].unique().tolist())
                print(f"Available exam_ids in questions.csv: {available_ids}")
            except:
                pass
            return False, f"No questions found for exam ID {exam_id}"

        # Get exam info with validation
        try:
            if 'id' not in exams_df.columns:
                return False, "Exams file missing id column"
                
            exam_info = exams_df[exams_df['id'].astype(str) == exam_id_str]
            if exam_info.empty:
                return False, f"Exam metadata not found for ID {exam_id}"
        except Exception as e:
            print(f"Error getting exam info: {e}")
            return False, f"Error accessing exam metadata: {str(e)}"

        # Process questions with images
        processed_questions = []
        image_urls = {}
        failed_images = []

        for _, question in exam_questions.iterrows():
            try:
                question_dict = question.to_dict()
                
                # Validate required fields
                if 'id' not in question_dict or not question_dict['id']:
                    print(f"Skipping question with missing ID")
                    continue

                # Process image with timeout protection
                try:
                    image_path = question_dict.get('image_path')
                    if image_path and str(image_path).strip() not in ['', 'nan', 'NaN', 'null', 'None']:
                        print(f"Processing image for Q{question_dict.get('id')}: {image_path}")
                        has_image, image_url = process_question_image_fixed_ssl_safe(question_dict)
                        question_dict['has_image'] = bool(has_image)
                        question_dict['image_url'] = image_url

                        if has_image and image_url:
                            image_urls[str(question_dict.get('id', ''))] = image_url
                        else:
                            failed_images.append(str(image_path))
                    else:
                        question_dict['has_image'] = False
                        question_dict['image_url'] = None
                except Exception as e:
                    print(f"Non-critical image error for Q{question_dict.get('id')}: {e}")
                    question_dict['has_image'] = False
                    question_dict['image_url'] = None

                # Parse correct answers
                try:
                    question_dict['parsed_correct_answer'] = parse_correct_answers(
                        question_dict.get('correct_answer'),
                        question_dict.get('question_type', 'MCQ')
                    )
                except Exception as e:
                    print(f"Error parsing correct answer for Q{question_dict.get('id')}: {e}")
                    question_dict['parsed_correct_answer'] = None

                processed_questions.append(question_dict)

            except Exception as e:
                print(f"Error processing question: {e}")
                continue

        if not processed_questions:
            return False, "No questions could be processed successfully"

        # Store in session with validation
        try:
            cache_key = f'exam_data_{exam_id}'
            session_data = {
                'exam_info': exam_info.iloc[0].to_dict(),
                'questions': processed_questions,
                'image_urls': image_urls,
                'failed_images': failed_images,
                'total_questions': len(processed_questions),
                'loaded_at': datetime.now().isoformat(),
                'exam_id': exam_id
            }
            
            # Validate session data before storing
            if not session_data['exam_info']:
                return False, "Exam info validation failed"
            if not session_data['questions']:
                return False, "Questions validation failed"
                
            try:
                # Limit session data size to prevent crashes
                if len(processed_questions) > 50:
                    # Store only essential data for large exams
                    session_data['questions'] = processed_questions[:50]  # Limit to 50 questions
                    print(f"Limited session storage to 50 questions (total: {len(processed_questions)})")
                
                session[cache_key] = session_data
            except Exception as e:
                print(f"Session storage error: {e}")
                # Try storing minimal data
                try:
                    minimal_data = {
                        'exam_info': exam_info.iloc[0].to_dict(),
                        'questions': processed_questions,
                        'total_questions': len(processed_questions),
                        'exam_id': exam_id
                    }
                    session[cache_key] = minimal_data
                except:
                    return False, "Failed to cache exam data"
            session.permanent = True
            
            print(f"Successfully stored exam data in session for exam {exam_id}")

        except Exception as e:
            print(f"Error storing session data: {e}")
            return False, f"Error caching exam data: {str(e)}"

        load_time = time.time() - start_time
        print(f"Successfully preloaded exam data in {load_time:.2f}s: {len(processed_questions)} questions")

        return True, f"Successfully loaded {len(processed_questions)} questions"

    except Exception as e:
        print(f"Critical error in preload_exam_data_fixed: {e}")
        import traceback
        traceback.print_exc()
        return False, f"Critical system error: {str(e)}"


def safe_csv_load_with_recovery(filename, max_retries=2):
    """
    Ultra-safe CSV loader with multiple fallback strategies
    """
    operation_id = generate_operation_id()
    
    for attempt in range(max_retries):
        try:
            print(f"[{operation_id}] Safe load attempt {attempt + 1} for {filename}")
            
            # Try main loader first
            try:
                df = safe_csv_load(filename, operation_id)
                if df is not None:
                    return df
            except Exception as e:
                print(f"[{operation_id}] safe_csv_load failed: {e}")
            
            # Try cache loader
            try:
                df = load_csv_with_cache(filename, force_reload=(attempt > 0))
                if df is not None:
                    return df
            except Exception as e:
                print(f"[{operation_id}] load_csv_with_cache failed: {e}")
            
            # Try direct file read
            try:
                local_path = os.path.join(os.getcwd(), filename)
                if os.path.exists(local_path):
                    df = pd.read_csv(local_path, dtype=str)
                    if df is not None:
                        return df
            except Exception as e:
                print(f"[{operation_id}] Local file read failed: {e}")
            
            # Brief delay before retry
            if attempt < max_retries - 1:
                time.sleep(0.5 * (attempt + 1))
                
        except Exception as e:
            print(f"[{operation_id}] Critical error in attempt {attempt + 1}: {e}")
    
    print(f"[{operation_id}] All attempts failed for {filename}, returning empty DataFrame")
    return pd.DataFrame()    


def get_cached_exam_data(exam_id):
    """Get cached exam data with comprehensive validation"""
    cache_key = f'exam_data_{exam_id}'
    cached_data = session.get(cache_key)

    if not cached_data:
        print(f"No cached data found for exam {exam_id}")
        return None

    # Validate cached data structure
    required_keys = ['exam_info', 'questions', 'total_questions', 'exam_id']
    missing_keys = [key for key in required_keys if key not in cached_data]
    
    if missing_keys:
        print(f"Invalid cached data structure for exam {exam_id}, missing keys: {missing_keys}")
        session.pop(cache_key, None)
        return None
        
    # Validate exam_id matches
    if cached_data.get('exam_id') != exam_id:
        print(f"Cached exam_id mismatch: expected {exam_id}, got {cached_data.get('exam_id')}")
        session.pop(cache_key, None)
        return None
        
    # Validate questions list
    questions = cached_data.get('questions', [])
    if not isinstance(questions, list) or len(questions) == 0:
        print(f"Invalid or empty questions list for exam {exam_id}")
        session.pop(cache_key, None)
        return None

    print(f"Found valid cached data for exam {exam_id}: {len(questions)} questions")
    return cached_data


def check_answer(given_answer, correct_answer, question_type, tolerance=0.1):
    """Enhanced answer checking with better validation"""
    if question_type == 'MCQ':
        if given_answer is None or correct_answer is None:
            return False
        return str(given_answer).strip().upper() == str(correct_answer).strip().upper()

    elif question_type == 'MSQ':
        if not given_answer or not correct_answer:
            return False

        # Convert to lists if needed
        if isinstance(given_answer, str):
            given_list = [x.strip().upper() for x in given_answer.split(',') if x.strip()]
        else:
            given_list = [str(x).strip().upper() for x in given_answer if x]

        if isinstance(correct_answer, str):
            correct_list = [x.strip().upper() for x in correct_answer.split(',') if x.strip()]
        else:
            correct_list = [str(x).strip().upper() for x in correct_answer if x]

        return set(given_list) == set(correct_list)

    elif question_type == 'NUMERIC':
        if given_answer is None or correct_answer is None:
            return False

        try:
            given_val = float(str(given_answer).strip())
            correct_val = float(str(correct_answer).strip())
            return abs(given_val - correct_val) <= tolerance
        except (ValueError, TypeError):
            return False

    return False


def calculate_question_score(is_correct, question_type, positive_marks, negative_marks):
    def safe_float(val, default=0.0):
        try:
            return float(val)
        except:
            return default

    pos = safe_float(positive_marks, 1.0)
    neg = safe_float(negative_marks, 0.0)

    if is_correct:
        return pos
    else:
        return -neg if neg else 0.0



def save_csv_to_drive_batch(df, csv_type):
    """Batch save CSV to Google Drive - FIXED"""
    global drive_service

    if drive_service is None:
        print("No Google Drive service for batch save")
        return False

    file_id = DRIVE_FILE_IDS.get(csv_type)
    if not file_id:
        print(f"No file ID found for {csv_type}")
        return False

    try:
        success = save_csv_to_drive(drive_service, df, file_id)
        if success:
            # Clear cache for this CSV type
            cache_key = f'csv_{csv_type}.csv'
            app_cache['data'].pop(cache_key, None)
            app_cache['timestamps'].pop(cache_key, None)
            print(f"Successfully saved and cleared cache for {csv_type}")
        return success
    except Exception as e:
        print(f"Error in batch save for {csv_type}: {e}")
        return False


def batch_save_responses(response_records):
    """Batch save responses to Google Drive - FIXED"""
    try:
        # Load existing responses
        responses_df = load_csv_with_cache('responses.csv')

        # Create DataFrame from new records
        new_responses_df = pd.DataFrame(response_records)

        # Combine with existing data
        if not responses_df.empty:
            combined_df = pd.concat([responses_df, new_responses_df], ignore_index=True)
        else:
            combined_df = new_responses_df

        return save_csv_to_drive_batch(combined_df, 'responses')
    except Exception as e:
        print(f"Error batch saving responses: {e}")
        return False

# -----------------------
# Safe Drive CSV wrapper
# -----------------------
import traceback

def safe_drive_csv_load(drive_service, file_id, friendly_name='csv', max_retries=3):
    """
    Wrap load_csv_from_drive() with defensive checks to avoid rare cases
    where the Drive client returns a non-dict 'meta' or unexpected types.
    Falls back to local file read if present. Returns a pd.DataFrame (may be empty).
    """
    try:
        if not drive_service or not file_id:
            print(f"safe_drive_csv_load: no drive service or file_id for {friendly_name}")
            return pd.DataFrame()

        # call library loader but catch odd return types
        df = load_csv_from_drive(drive_service, file_id, max_retries=max_retries)
        if df is None:
            # defender - ensure we always return a DataFrame
            return pd.DataFrame()

        # Some earlier bugs produced string returns in the stack; detect and handle
        if isinstance(df, str):
            print(f"safe_drive_csv_load: Unexpected string returned while loading {friendly_name}; contents head: {df[:200]}")
            return pd.DataFrame()

        # ensure df is DataFrame
        if not hasattr(df, "empty"):
            print(f"safe_drive_csv_load: Unexpected type returned for {friendly_name}: {type(df)}")
            return pd.DataFrame()

        return df.copy()
    except Exception as e:
        print(f"safe_drive_csv_load: drive load failed for {friendly_name}: {e}")
        traceback.print_exc()
        # fallback to reading local file if present
        try:
            local_path = os.path.join(os.getcwd(), friendly_name)
            # friendly_name often passed like 'exam_attempts.csv' or similar - accept both
            if os.path.exists(local_path):
                return pd.read_csv(local_path, dtype=str)
        except Exception as e2:
            print(f"safe_drive_csv_load: local fallback also failed: {e2}")

        return pd.DataFrame()



# -------------------------
# Routes - COMPLETELY FIXED VERSION
# -------------------------


print("🔧 Module loading - checking execution context...")
print(f"📍 __name__ = {__name__}")
print(f"🌐 RENDER environment: {os.environ.get('RENDER', 'Not set')}")

def force_drive_initialization():
    """Force Google Drive initialization for all execution contexts"""
    global drive_service
    
    print("🚀 Force initializing Google Drive service...")
    
    # Debug environment variables first
    json_env = os.environ.get('GOOGLE_SERVICE_ACCOUNT_JSON')
    if json_env:
        print(f"✅ GOOGLE_SERVICE_ACCOUNT_JSON found: {len(json_env)} characters")
        
        # Test JSON parsing
        try:
            test_json = json.loads(json_env)
            print(f"✅ JSON is valid. Client email: {test_json.get('client_email', 'Not found')}")
        except json.JSONDecodeError as e:
            print(f"❌ JSON parsing failed: {e}")
            print(f"📄 First 100 chars: {json_env[:100]}")
            return False
    else:
        print("❌ GOOGLE_SERVICE_ACCOUNT_JSON not found in environment")
        print("📋 Available environment variables with 'GOOGLE' or 'SERVICE':")
        for key in os.environ.keys():
            if 'GOOGLE' in key.upper() or 'SERVICE' in key.upper():
                print(f"   - {key}")
        return False
    
    # Initialize the service
    try:
        success = init_drive_service()
        if success:
            print("✅ Force initialization successful!")
            return True
        else:
            print("❌ Force initialization failed")
            return False
    except Exception as e:
        print(f"❌ Exception during force initialization: {e}")
        import traceback
        traceback.print_exc()
        return False

# CALL IT IMMEDIATELY when module loads
print("🔄 Attempting force initialization...")
initialization_success = force_drive_initialization()

if initialization_success:
    print("🎉 Google Drive service ready!")
else:
    print("⚠️ Google Drive service failed to initialize")



def get_active_attempt(user_id, exam_id):
    """
    CRASH-SAFE active attempt retrieval
    """
    try:
        # Use the new safe loader
        attempts_df = safe_csv_load_with_recovery('exam_attempts.csv')
        
        if attempts_df is None or attempts_df.empty:
            return None

        # SAFE: Normalize data
        try:
            attempts_df = attempts_df.fillna('')
            attempts_df['student_id'] = attempts_df['student_id'].astype(str)
            attempts_df['exam_id'] = attempts_df['exam_id'].astype(str)
            attempts_df['status'] = attempts_df['status'].astype(str)
        except Exception as e:
            print(f"Error normalizing attempts data: {e}")
            return None

        # SAFE: Filter and find active attempt
        try:
            mask = (
                (attempts_df['student_id'] == str(user_id)) &
                (attempts_df['exam_id'] == str(exam_id)) &
                (attempts_df['status'].str.lower() == 'in_progress')
            )
            
            candidate = attempts_df[mask]
            if candidate.empty:
                return None
            
            # Get most recent
            candidate_sorted = candidate.sort_values('start_time', ascending=False)
            return candidate_sorted.iloc[0].to_dict()
            
        except Exception as e:
            print(f"Error filtering active attempts: {e}")
            return None
            
    except Exception as e:
        print(f"Critical error in get_active_attempt: {e}")
        return None



def error_boundary(func):
    """
    Decorator to wrap functions with error boundaries
    """
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            func_name = getattr(func, '__name__', 'unknown')
            print(f"ERROR BOUNDARY caught exception in {func_name}: {e}")
            import traceback
            traceback.print_exc()
            
            # Return appropriate default based on expected return type
            if 'json' in func_name.lower() or 'api' in func_name.lower():
                return jsonify({"success": False, "message": "System error occurred"}), 500
            else:
                flash("A system error occurred. Please try again or contact support.", "error")
                return redirect(url_for('dashboard'))
    
    return wrapper



def persist_attempts_df(attempts_df):
    """
    CRASH-SAFE attempts persistence with multiple fallback strategies
    """
    operation_id = generate_operation_id()
    
    # Input validation
    if attempts_df is None:
        return False, "attempts_df is None"
    
    try:
        attempts_df = attempts_df.copy()
        
        # Ensure required columns
        required_cols = ['id', 'student_id', 'exam_id', 'attempt_number', 'status', 'start_time', 'end_time']
        for col in required_cols:
            if col not in attempts_df.columns:
                attempts_df[col] = ''
        
        # Strategy 1: Try Google Drive
        file_id = DRIVE_FILE_IDS.get('exam_attempts')
        if drive_service and file_id:
            try:
                success = save_csv_to_drive(drive_service, attempts_df, file_id)
                if success:
                    # Clear caches on success
                    try:
                        app_cache['data'].pop('csv_exam_attempts.csv', None)
                        app_cache['timestamps'].pop('csv_exam_attempts.csv', None)
                    except Exception:
                        pass
                    
                    try:
                        from google_drive_service import clear_csv_cache
                        clear_csv_cache(file_id)
                    except Exception:
                        pass
                    
                    print(f"[{operation_id}] Successfully saved to Google Drive")
                    return True, "saved_to_drive"
                else:
                    print(f"[{operation_id}] Google Drive save returned False")
            except Exception as e:
                print(f"[{operation_id}] Google Drive save failed: {e}")
        
        # Strategy 2: Local file fallback
        try:
            local_path = os.path.join(os.getcwd(), 'exam_attempts.csv')
            attempts_df.to_csv(local_path, index=False)
            
            # Clear app cache
            try:
                app_cache['data'].pop('csv_exam_attempts.csv', None)
                app_cache['timestamps'].pop('csv_exam_attempts.csv', None)
            except Exception:
                pass
            
            print(f"[{operation_id}] Successfully saved to local file")
            return True, f"saved_to_local:{local_path}"
            
        except Exception as e:
            print(f"[{operation_id}] Local file save failed: {e}")
        
        # Strategy 3: Emergency in-memory backup (last resort)
        try:
            backup_key = f'emergency_attempts_backup_{int(time.time())}'
            app_cache['data'][backup_key] = attempts_df.copy()
            app_cache['timestamps'][backup_key] = time.time()
            print(f"[{operation_id}] Created emergency in-memory backup: {backup_key}")
            return True, f"emergency_backup:{backup_key}"
        except Exception as e:
            print(f"[{operation_id}] Emergency backup failed: {e}")
        
        return False, "all_strategies_failed"
        
    except Exception as e:
        print(f"[{operation_id}] Critical error in persist_attempts_df: {e}")
        import traceback
        traceback.print_exc()
        return False, f"critical_error:{str(e)}"





def ensure_drive_csv_exists(csv_type, filename):
    """
    Ensure the DRIVE_FILE_IDS[csv_type] points to a real downloadable file.
    If missing or points to a folder, try to create a new CSV file in Drive and
    update DRIVE_FILE_IDS[csv_type] in memory (won't persist env var).
    Returns (file_id, reason)
    """
    global drive_service, DRIVE_FILE_IDS
    file_id = DRIVE_FILE_IDS.get(csv_type)
    # Quick sanity: if no drive service, bail
    if not drive_service:
        return None, "no_drive_service"

    def is_folder(fid):
        try:
            meta = drive_service.files().get(fileId=fid, fields="id,name,mimeType").execute()
            mime = meta.get("mimeType", "")
            return 'folder' in mime
        except Exception as e:
            return False

    try:
        if file_id:
            # check if it's a folder or otherwise not downloadable
            try:
                meta = drive_service.files().get(fileId=file_id, fields="id,name,mimeType,size").execute()
                mime = meta.get("mimeType","")
                if 'folder' in mime or meta.get("size") in [None, "0"]:
                    # treat as invalid
                    print(f"ensure_drive_csv_exists: configured file id {file_id} for {csv_type} appears to be a folder or empty ({mime}).")
                    file_id = None
            except Exception as e:
                print(f"ensure_drive_csv_exists: error getting metadata for {file_id}: {e}")
                file_id = None

        if not file_id:
            # Create a new empty CSV file in Drive under root or configured folder
            # Use create_file_if_not_exists if available; else try a simple create
            upload_name = filename
            try:
                # create empty local tempfile and upload it via save_csv_to_drive helper pattern
                import pandas as pd
                tmp_df = pd.DataFrame(columns=['id','student_id','exam_id','attempt_number','status','start_time','end_time'])
                # Use a helper in google_drive_service if exists to create files; else use save_csv_to_drive 
                # save_csv_to_drive(service, df, file_id) expects a file id - but we need create new file
                # Try to create using files().create
                from googleapiclient.http import MediaIoBaseUpload
                from io import BytesIO
                csv_bytes = tmp_df.to_csv(index=False).encode('utf-8')
                fh = BytesIO(csv_bytes)
                media = MediaIoBaseUpload(fh, mimetype='text/csv', resumable=False)
                file_metadata = {'name': upload_name}
                created = drive_service.files().create(body=file_metadata, media_body=media, fields='id,name').execute()
                new_id = created.get('id')
                print(f"ensure_drive_csv_exists: created new csv for {csv_type} id={new_id}")
                DRIVE_FILE_IDS[csv_type] = new_id
                return new_id, "created_new"
            except Exception as e:
                print(f"ensure_drive_csv_exists: failed to create drive csv for {csv_type}: {e}")
                return None, f"create_failed:{e}"

        return file_id, "ok"
    except Exception as e:
        print(f"ensure_drive_csv_exists unexpected error: {e}")
        return None, f"error:{e}"




def update_exam_attempt_status(user_id, exam_id, status):
    """
    CRASH-SAFE helper to update exam attempt status
    """
    try:
        attempts_df = safe_csv_load_with_recovery('exam_attempts.csv')
        
        if attempts_df is None or attempts_df.empty:
            print("No attempts data to update")
            return False, "no_data"

        # Find the in_progress attempt
        mask = (
            (attempts_df['student_id'].astype(str) == str(user_id)) &
            (attempts_df['exam_id'].astype(str) == str(exam_id)) &
            (attempts_df['status'].astype(str).str.lower() == 'in_progress')
        )

        if not mask.any():
            print("No in_progress attempt found to update")
            return False, "not_found"

        # Update the most recent one
        idx_list = attempts_df[mask].index.tolist()
        if idx_list:
            idx = idx_list[-1]
            attempts_df.at[idx, 'status'] = status
            attempts_df.at[idx, 'end_time'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            # Persist the changes
            ok, info = persist_attempts_df(attempts_df)
            return ok, info
        
        return False, "update_failed"
        
    except Exception as e:
        print(f"Error updating exam attempt status: {e}")
        return False, str(e)
    

# Helper function to validate password strength (optional)
def validate_password_strength(password):
    """
    Validate password strength and return feedback
    Returns: (is_valid, feedback_message)
    """
    if len(password) < 6:
        return False, "Password must be at least 6 characters long"
    
    has_lower = any(c.islower() for c in password)
    has_upper = any(c.isupper() for c in password)
    has_digit = any(c.isdigit() for c in password)
    
    strength_score = sum([has_lower, has_upper, has_digit, len(password) >= 8])
    
    if strength_score < 2:
        return False, "Password should contain a mix of letters, numbers, and cases"
    
    return True, "Password strength is acceptable"


# Enhanced user registration function that handles password validation
def safe_user_register_enhanced(email, full_name, custom_password=None):
    """Enhanced user registration with optional custom password"""
    operation_id = generate_operation_id()
    
    with get_file_lock('users'):
        print(f"[{operation_id}] Enhanced user registration: {email}")
        
        # Load current users
        users_df = safe_csv_load('users.csv', operation_id)
        
        # Check if email exists
        if not users_df.empty and email.lower() in users_df['email'].str.lower().values:
            existing_user = users_df[users_df['email'].str.lower() == email.lower()].iloc[0]
            return False, "exists", {
                'username': existing_user['username'],
                'password': existing_user['password'],
                'full_name': existing_user['full_name']
            }
        
        # Create new user
        existing_usernames = users_df['username'].tolist() if not users_df.empty else []
        username = generate_username(full_name, existing_usernames)
        password = custom_password if custom_password else generate_password()
        
        # Validate password if custom
        if custom_password:
            is_valid, message = validate_password_strength(custom_password)
            if not is_valid:
                return False, "invalid_password", {'message': message}
        
        next_id = 1
        if not users_df.empty and 'id' in users_df.columns:
            next_id = int(users_df['id'].fillna(0).astype(int).max()) + 1
        
        new_user = {
            'id': next_id,
            'full_name': full_name,
            'username': username,
            'email': email.lower(),
            'password': password,
            'created_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'updated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'role': 'user'
        }
        
        # Prepare new dataframe
        if users_df.empty:
            new_df = pd.DataFrame([new_user])
        else:
            new_df = pd.concat([users_df, pd.DataFrame([new_user])], ignore_index=True)
        
        # Save with retry mechanism
        if safe_csv_save_with_retry(new_df, 'users', operation_id):
            return True, "success", {
                'username': username,
                'password': password,
                'full_name': full_name
            }
        else:
            return False, "save_failed", None


# ENHANCED CSV save function with immediate verification
def enhanced_csv_save_with_verification(df, csv_type, operation_id):
    """Save CSV and immediately verify the save worked"""
    
    # Save using existing function
    success = safe_csv_save_with_retry(df, csv_type, operation_id)
    
    if success:
        # Clear cache immediately
        clear_user_cache()
        
        # Wait a moment for Drive to process
        time.sleep(1)
        
        # Verify by loading fresh data
        try:
            verification_df = load_csv_from_drive_direct(f'{csv_type}.csv')
            if verification_df is not None and len(verification_df) == len(df):
                print(f"[{operation_id}] Save verification successful for {csv_type}")
                return True
            else:
                print(f"[{operation_id}] Save verification failed for {csv_type}")
                return False
        except Exception as e:
            print(f"[{operation_id}] Save verification error: {e}")
            return success  # Return original result if verification fails
    
    return success   



# Add this function to your main.py file, around line 200 after other helper functions

def initialize_requests_raised_csv():
    """Initialize requests_raised.csv if it doesn't exist"""
    try:
        # Check if file exists and has data
        existing_df = load_csv_with_cache('requests_raised.csv')
        if existing_df is not None and not existing_df.empty:
            print("✅ requests_raised.csv already exists with data")
            return True
            
        # Create new file with proper headers
        headers_df = pd.DataFrame(columns=[
            'request_id', 'username', 'email', 'current_access',
            'requested_access', 'request_date', 'request_status', 
            'reason', 'processed_by', 'processed_date'
        ])
        
        # Save to Drive
        success = safe_csv_save_with_retry(headers_df, 'requests_raised')
        
        if success:
            print("✅ Created requests_raised.csv with headers")
            return True
        else:
            print("❌ Failed to create requests_raised.csv")
            return False
            
    except Exception as e:
        print(f"Error initializing requests_raised.csv: {e}")
        return False

# Update the ensure_required_files function to include the new CSV
def ensure_required_files():
    """Ensure all required CSV files exist in Google Drive"""
    global drive_service

    if not drive_service:
        print("❌ No Google Drive service for file verification")
        return

    required_files = {
        'users.csv': DRIVE_FILE_IDS['users'],
        'exams.csv': DRIVE_FILE_IDS['exams'],
        'questions.csv': DRIVE_FILE_IDS['questions'],
        'results.csv': DRIVE_FILE_IDS['results'],
        'responses.csv': DRIVE_FILE_IDS['responses'],
        'exam_attempts.csv': DRIVE_FILE_IDS.get('exam_attempts'),
        'requests_raised.csv': DRIVE_FILE_IDS.get('requests_raised')  # Add this line
    }

    for filename, file_id in required_files.items():
        if not file_id or file_id.startswith('YOUR_'):
            print(f"⚠️ {filename}: File ID not configured properly")
            continue
            
        try:
            # Try to get file metadata to check if it exists
            meta = drive_service.files().get(fileId=file_id, fields="id,name,size").execute()
            print(f"✅ Verified {filename}: {meta.get('name')} ({meta.get('size', '0')} bytes)")
        except Exception as e:
            print(f"❌ Error verifying {filename} (ID: {file_id}): {e}")

# Update the force_drive_initialization function to include the new CSV
def force_drive_initialization():
    """Force Google Drive initialization for all execution contexts"""
    global drive_service
    
    print("🚀 Force initializing Google Drive service...")
    
    # Debug environment variables first
    json_env = os.environ.get('GOOGLE_SERVICE_ACCOUNT_JSON')
    if json_env:
        print(f"✅ GOOGLE_SERVICE_ACCOUNT_JSON found: {len(json_env)} characters")
        
        # Test JSON parsing
        try:
            test_json = json.loads(json_env)
            print(f"✅ JSON is valid. Client email: {test_json.get('client_email', 'Not found')}")
        except json.JSONDecodeError as e:
            print(f"❌ JSON parsing failed: {e}")
            print(f"📄 First 100 chars: {json_env[:100]}")
            return False
    else:
        print("❌ GOOGLE_SERVICE_ACCOUNT_JSON not found in environment")
        return False
    
    # Initialize the service
    try:
        success = init_drive_service()
        if success:
            print("✅ Force initialization successful!")
            
            # Initialize the new CSV file
            initialize_requests_raised_csv()
            
            return True
        else:
            print("❌ Force initialization failed")
            return False
    except Exception as e:
        print(f"❌ Exception during force initialization: {e}")
        import traceback
        traceback.print_exc()
        return False


# -------------------------
# Routes - Add explicit initialization before first route
# -------------------------


# Footer page routes
@app.route('/privacy-policy')
def privacy_policy():
    return render_template('privacy_policy.html')

@app.route('/terms-of-service') 
def terms_of_service():
    return render_template('terms_of_service.html')

@app.route('/support')
def support():
    return render_template('support.html')

@app.route('/contact')
def contact():
    return render_template('contact.html')

@app.route('/about')
def about():
    return render_template('about.html')



# 6. ADD DEBUG ROUTE HERE (BEFORE MAIN ROUTES):
@app.route('/debug/env-check')
def debug_env_check():
    """Debug endpoint to check environment variables"""
    
    env_status = {}
    
    # Check all required environment variables
    required_vars = [
        'SECRET_KEY',
        'GOOGLE_SERVICE_ACCOUNT_JSON',
        'USERS_FILE_ID',
        'EXAMS_FILE_ID', 
        'QUESTIONS_FILE_ID',
        'RESULTS_FILE_ID',
        'RESPONSES_FILE_ID',
        'ROOT_FOLDER_ID',
        'IMAGES_FOLDER_ID'
    ]
    
    for var in required_vars:
        value = os.environ.get(var)
        if value:
            if var == 'GOOGLE_SERVICE_ACCOUNT_JSON':
                # Check JSON validity without exposing content
                try:
                    json_data = json.loads(value)
                    env_status[var] = {
                        'status': 'Present and Valid JSON',
                        'length': len(value),
                        'has_private_key': 'private_key' in json_data,
                        'has_client_email': 'client_email' in json_data,
                        'client_email': json_data.get('client_email', 'Not found')[:50] + '...'
                    }
                except json.JSONDecodeError as e:
                    env_status[var] = {
                        'status': 'Present but INVALID JSON',
                        'error': str(e),
                        'length': len(value),
                        'first_100_chars': value[:100]
                    }
            elif 'SECRET' in var:
                env_status[var] = {'status': 'Present', 'length': len(value)}
            else:
                env_status[var] = {'status': 'Present', 'value': value}
        else:
            env_status[var] = {'status': 'MISSING'}
    
    # Check if we're on Render
    render_detected = os.environ.get('RENDER') is not None
    
    # Try to initialize Google Drive service
    drive_init_status = "Not attempted"
    try:
        test_service = create_drive_service()
        if test_service:
            drive_init_status = "SUCCESS"
            try:
                about = test_service.about().get(fields="user").execute()
                drive_init_status += f" - Connected as: {about.get('user', {}).get('emailAddress', 'Unknown')}"
            except:
                drive_init_status += " - Service created but test failed"
        else:
            drive_init_status = "FAILED - Service is None"
    except Exception as e:
        drive_init_status = f"FAILED - Exception: {str(e)}"
    
    return jsonify({
        'platform': 'Render' if render_detected else 'Local/Other',
        'environment_variables': env_status,
        'google_drive_init': drive_init_status,
        'python_version': os.sys.version,
        'working_directory': os.getcwd(),
        'file_ids_configured': DRIVE_FILE_IDS,
        'folder_ids_configured': DRIVE_FOLDER_IDS,
        'drive_service_status': 'Initialized' if drive_service else 'Not Initialized'
    })



@app.route('/')
def home():
    # Clear any conflicting session data when going to home
    admin_id = session.get('admin_id')
    user_id = session.get('user_id')
    
    # If both admin and user sessions exist, it's invalid state
    if admin_id and user_id and str(admin_id) == str(user_id):
        # Keep admin session, clear user session parts
        session.pop('admin_id', None)
        session.pop('admin_name', None)
    
    return render_template('index.html')

@app.route("/login", methods=["GET", "POST"])
def login():
    # If user is already logged in as admin, redirect to admin dashboard
    if session.get('admin_id') and session.get('user_id'):
        flash("You are already logged in as Admin. Please logout first to access User portal.", "warning")
        return redirect(url_for("admin.dashboard"))
    
    if request.method == "POST":
        try:
            # FIXED: Use 'username' field name that matches your form
            identifier = request.form["username"].strip().lower()  # Changed from 'identifier' to 'username'
            password = request.form["password"].strip()

            if not identifier or not password:
                flash("Both username/email and password are required!", "error")
                return redirect(url_for("login"))

            # Load users data
            users_df = load_csv_with_cache("users.csv")
            
            if users_df is None or users_df.empty:
                flash("User database unavailable!", "error")
                return redirect(url_for("login"))

            # Normalize data for comparison
            users_df["username_lower"] = users_df["username"].astype(str).str.strip().str.lower()
            users_df["email_lower"] = users_df["email"].astype(str).str.strip().str.lower()
            users_df["role_lower"] = users_df["role"].astype(str).str.strip().str.lower()

            # Find user by username or email
            user_row = users_df[
                (users_df["username_lower"] == identifier) |
                (users_df["email_lower"] == identifier)
            ]

            if user_row.empty:
                flash("Invalid username/email or password!", "error")
                return redirect(url_for("login"))

            user = user_row.iloc[0]

            # Verify password
            if str(user["password"]) != password:
                flash("Invalid username/email or password!", "error")
                return redirect(url_for("login"))

            role = str(user.get("role", "")).lower()

            # ENHANCED: Validate role access for user portal
            if "user" not in role:
                flash("You don't have User portal access. Contact admin if you need access.", "error")
                return redirect(url_for("login"))

            # Invalidate any existing sessions for this user
            try:
                invalidate_session(int(user["id"]))
            except Exception as e:
                print(f"[login] Error invalidating session: {e}")

            # Create and save new session token
            # Create and save new session token (local)
            token = generate_session_token()
            save_session_record({
                "user_id": int(user["id"]),
                "token": token,
                "device_info": request.headers.get("User-Agent", "unknown"),
                "is_exam_active": False
            })

            # ENHANCED: Only set user session data, NO admin session data
            session.clear()  # Clear any existing session data
            session['user_id'] = int(user["id"])
            session['token'] = token
            session['username'] = user.get("username")
            session['full_name'] = user.get("full_name", user.get("username"))
         
            session.permanent = True

            flash("Login successful!", "success")
            return redirect(url_for("dashboard"))

        except KeyError as e:
            print(f"[login] Missing form field: {e}")
            flash("Login form error. Please try again.", "error")
            return redirect(url_for("login"))
        except Exception as e:
            print(f"[login] Unexpected error: {e}")
            flash("A system error occurred. Please try again.", "error")
            return redirect(url_for("login"))

    return render_template("login.html")





@app.route('/reset-password')
def reset_password_page():
    """Password reset page route"""
    return render_template('password_reset.html')


@app.route('/api/verify-user', methods=['POST'])
def api_verify_user():
    """API endpoint to verify if user exists"""
    try:
        data = request.get_json()
        if not data or not data.get('username'):
            return jsonify({
                'success': False,
                'message': 'Username or email is required'
            }), 400

        username_or_email = data['username'].strip().lower()
        
        # Load users data with force reload to get latest data
        users_df = load_csv_with_cache('users.csv', force_reload=True)
        if users_df.empty:
            return jsonify({
                'success': False,
                'message': 'User database is unavailable'
            }), 500

        # Search for user by username or email
        users_df['username_lower'] = users_df['username'].astype(str).str.strip().str.lower()
        users_df['email_lower'] = users_df['email'].astype(str).str.strip().str.lower()
        
        user_row = users_df[
            (users_df['username_lower'] == username_or_email) |
            (users_df['email_lower'] == username_or_email)
        ]
        
        if user_row.empty:
            return jsonify({
                'success': False,
                'message': 'User does not exist'
            }), 404
        
        user = user_row.iloc[0]
        
        # Return user info (without sensitive data)
        return jsonify({
            'success': True,
            'user': {
                'id': int(user['id']),
                'username': user['username'],
                'email': user['email'],
                'full_name': user['full_name']
            }
        })
        
    except Exception as e:
        print(f"Error verifying user: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({
            'success': False,
            'message': 'System error occurred'
        }), 500


@app.route('/api/reset-password', methods=['POST'])
def api_reset_password():
    """FIXED API endpoint to reset user password with immediate cache refresh"""
    operation_id = generate_operation_id()
    print(f"[{operation_id}] Starting password reset request")
    
    try:
        data = request.get_json()
        
        # Validate required fields
        required_fields = ['username', 'old_password', 'new_password', 'confirm_password']
        for field in required_fields:
            if not data or not data.get(field):
                return jsonify({
                    'success': False,
                    'message': f'{field.replace("_", " ").title()} is required',
                    'field_errors': {field: f'{field.replace("_", " ").title()} is required'}
                }), 400
        
        username = data['username'].strip()
        old_password = data['old_password'].strip()
        new_password = data['new_password'].strip()
        confirm_password = data['confirm_password'].strip()
        
        print(f"[{operation_id}] Password reset for user: {username}")
        
        # Validation checks
        field_errors = {}
        
        # Check password length
        if len(new_password) < 6:
            field_errors['newPassword'] = 'New password must be at least 6 characters long'
        
        # Check password match
        if new_password != confirm_password:
            field_errors['confirmPassword'] = 'New password and confirm password do not match'
        
        # Check if new password is same as old
        if new_password == old_password:
            field_errors['newPassword'] = 'New password must be different from current password'
        
        if field_errors:
            print(f"[{operation_id}] Validation failed: {field_errors}")
            return jsonify({
                'success': False,
                'message': 'Validation failed',
                'field_errors': field_errors
            }), 400
        
        # CRITICAL: Use proper file locking and force cache refresh
        file_lock = get_file_lock('users')
        with file_lock:
            print(f"[{operation_id}] Acquired file lock for users.csv")
            
            # Force clear all caches first
            clear_user_cache()
            
            # Load fresh users data directly from Drive
            users_df = None
            try:
                # Try direct Drive load first
                if drive_service and DRIVE_FILE_IDS.get('users'):
                    print(f"[{operation_id}] Loading users.csv directly from Google Drive")
                    users_df = safe_drive_csv_load(
                        drive_service, 
                        DRIVE_FILE_IDS['users'], 
                        friendly_name='users.csv'
                    )
                
                if users_df is None or users_df.empty:
                    print(f"[{operation_id}] Drive load failed, trying cache reload")
                    users_df = load_csv_with_cache('users.csv', force_reload=True)
                
                if users_df is None or users_df.empty:
                    print(f"[{operation_id}] All load methods failed")
                    return jsonify({
                        'success': False,
                        'message': 'User database is unavailable',
                        'field_errors': {'oldPassword': 'Database access error'}
                    }), 500
                    
            except Exception as e:
                print(f"[{operation_id}] Error loading users data: {e}")
                return jsonify({
                    'success': False,
                    'message': 'Failed to load user database',
                    'field_errors': {'oldPassword': 'Database load error'}
                }), 500
            
            print(f"[{operation_id}] Loaded {len(users_df)} users from database")
            
            # Find user with case-insensitive search
            try:
                users_df['username_lower'] = users_df['username'].astype(str).str.strip().str.lower()
                user_mask = users_df['username_lower'] == username.lower()
                
                if not user_mask.any():
                    print(f"[{operation_id}] User not found: {username}")
                    return jsonify({
                        'success': False,
                        'message': 'User not found',
                        'field_errors': {'oldPassword': 'User not found'}
                    }), 404
                
                user_row = users_df[user_mask].iloc[0]
                user_index = users_df[user_mask].index[0]
                
                print(f"[{operation_id}] Found user at index {user_index}: {user_row['username']}")
                
            except Exception as e:
                print(f"[{operation_id}] Error finding user: {e}")
                return jsonify({
                    'success': False,
                    'message': 'Error locating user',
                    'field_errors': {'oldPassword': 'User lookup error'}
                }), 500
            
            # Verify old password
            current_password = str(user_row['password']).strip()
            if current_password != old_password:
                print(f"[{operation_id}] Password verification failed for user {username}")
                return jsonify({
                    'success': False,
                    'message': 'Incorrect current password',
                    'field_errors': {'oldPassword': 'Incorrect current password'}
                }), 400
            
            # Update password in DataFrame
            try:
                users_df.at[user_index, 'password'] = new_password
                users_df.at[user_index, 'updated_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                
                print(f"[{operation_id}] Updated password for user {username}")
                
            except Exception as e:
                print(f"[{operation_id}] Error updating DataFrame: {e}")
                return jsonify({
                    'success': False,
                    'message': 'Error updating user data',
                    'field_errors': {'oldPassword': 'Data update error'}
                }), 500
            
            # Save to Google Drive with multiple retry attempts
            save_success = False
            save_error = None
            
            for attempt in range(5):  # 5 retry attempts
                try:
                    print(f"[{operation_id}] Save attempt {attempt + 1}")
                    
                    if drive_service and DRIVE_FILE_IDS.get('users'):
                        success = save_csv_to_drive(drive_service, users_df, DRIVE_FILE_IDS['users'])
                        if success:
                            save_success = True
                            print(f"[{operation_id}] Successfully saved to Google Drive on attempt {attempt + 1}")
                            break
                        else:
                            print(f"[{operation_id}] Drive save returned False on attempt {attempt + 1}")
                    else:
                        print(f"[{operation_id}] No drive service or file ID available")
                        
                except Exception as e:
                    save_error = str(e)
                    print(f"[{operation_id}] Save attempt {attempt + 1} failed: {e}")
                    time.sleep(0.5 * (attempt + 1))  # Progressive delay
                    continue
            
            # Fallback to local save if Drive fails
            if not save_success:
                try:
                    local_path = os.path.join(os.getcwd(), 'users.csv')
                    users_df.to_csv(local_path, index=False)
                    save_success = True
                    print(f"[{operation_id}] Saved to local file as fallback")
                except Exception as e:
                    save_error = str(e)
                    print(f"[{operation_id}] Local save also failed: {e}")
            
            if not save_success:
                print(f"[{operation_id}] All save attempts failed: {save_error}")
                return jsonify({
                    'success': False,
                    'message': 'Failed to save password update. Please try again.',
                    'field_errors': {'oldPassword': f'Save failed: {save_error}'}
                }), 500
            
            # CRITICAL: Force immediate cache refresh across the application
            try:
                # Clear all related caches
                clear_user_cache()
                
                # Force reload the updated data to verify it worked
                verification_df = load_csv_with_cache('users.csv', force_reload=True)
                if verification_df is not None and not verification_df.empty:
                    # Verify the password was actually updated
                    verification_user = verification_df[
                        verification_df['username'].astype(str).str.strip().str.lower() == username.lower()
                    ]
                    if not verification_user.empty:
                        updated_password = str(verification_user.iloc[0]['password']).strip()
                        if updated_password == new_password:
                            print(f"[{operation_id}] Password update verified successfully")
                        else:
                            print(f"[{operation_id}] WARNING: Password verification failed")
                
                print(f"[{operation_id}] Cache cleared and reloaded successfully")
                
            except Exception as e:
                print(f"[{operation_id}] Warning: Cache refresh failed: {e}")
                # Don't fail the request if cache refresh fails
            
            print(f"[{operation_id}] Password reset completed successfully")
            return jsonify({
                'success': True,
                'message': 'Password updated successfully. Please login with your new password.'
            })
    
    except Exception as e:
        print(f"[{operation_id}] Critical error in password reset: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({
            'success': False,
            'message': 'System error occurred. Please try again.',
            'field_errors': {'oldPassword': f'System error: {str(e)}'}
        }), 500







def generate_username(full_name, existing_usernames):
    """Generate a unique username based on full name"""
    name_parts = full_name.lower().replace(' ', '').replace('.', '')
    base_username = name_parts[:8]

    username = base_username
    counter = 1
    while username in existing_usernames:
        username = f"{base_username}{counter}"
        counter += 1

    return username

# 4. REPLACE your generate_password function (around line 10-15):

def generate_password(length=8):
    """Generate a random password"""
    characters = string.ascii_letters + string.digits
    return ''.join(secrets.choice(characters) for _ in range(length))


def is_valid_email(email):
    """Simple email validation"""
    return '@' in email and '.' in email.split('@')[1] and len(email) > 5


def verify_email_exists(email):
    """Simple email verification"""
    if not is_valid_email(email):
        return False, "Invalid email format"

    # Just check if it has @ and domain
    domain = email.split('@')[1].lower()
    if len(domain) > 3 and '.' in domain:
        return True, "Valid email format"
    else:
        return False, "Invalid email domain"




@app.route('/create_account', methods=['GET', 'POST'])
def create_account():
    """Enhanced user registration with concurrent safety and retry"""
    if request.method == 'POST':
        try:
            email = request.form['email'].strip().lower()
            full_name = request.form.get('full_name', '').strip()

            if not email:
                flash('Please enter your email address.', 'error')
                return render_template('create_account.html')

            if not full_name:
                flash('Please enter your full name.', 'error')
                return render_template('create_account.html', email=email)

            is_valid, error_message = verify_email_exists(email)
            if not is_valid:
                flash(f'Invalid email: {error_message}', 'error')
                return render_template('create_account.html', email=email, full_name=full_name)

            # Use safe registration with retry
            success, status, credentials = safe_user_register(email, full_name)
            
            if success or status == "exists":
                # Send credentials email
                email_sent, email_message = send_credentials_email(
                    email, credentials['full_name'], credentials['username'], credentials['password']
                )

                if email_sent:
                    if success:
                        # New account was created
                        flash('Account created successfully! Your credentials have been sent to your email. Please check your spam folder if you don\'t see it in your inbox.', 'success')
                    else:
                        # Account already existed
                        flash('Account already exists! Your credentials have been sent to your email. Please check your spam folder if you don\'t see it in your inbox.', 'success')
                else:
                    msg = 'Account created!' if success else 'Account exists!'
                    flash(f'{msg} Here are your credentials:', 'success')
                
                # Store information in session (secure way)
                session['reg_success_type'] = "created" if success else "exists"
                session['reg_email'] = email
                session['reg_username'] = credentials['username']
                session['reg_password'] = credentials['password']
                session['reg_fullname'] = credentials['full_name']
                
                # Redirect to success page with clean URL
                return redirect(url_for('registration_success'))
            else:
                flash(f'Registration failed: {status}. Please try again.', 'error')
                return render_template('create_account.html', email=email, full_name=full_name)

        except Exception as e:
            print(f"Registration error: {e}")
            flash('System error occurred. Please try again.', 'error')
            return render_template('create_account.html')
    
    # GET request
    return render_template('create_account.html')

@app.route('/registration-success')
def registration_success():
    """Show registration success page"""
    # Get data from session
    success_type = session.get('reg_success_type')
    email = session.get('reg_email')
    username = session.get('reg_username')
    password = session.get('reg_password')
    full_name = session.get('reg_fullname')
    
    # Verify we have the necessary data
    if not all([success_type, email, username, password]):
        flash('Session expired or invalid access.', 'error')
        return redirect(url_for('create_account'))
    
    # Create credentials dictionary
    credentials = {
        'username': username,
        'password': password,
        'full_name': full_name
    }
    
    # Clear session data after use
    session.pop('reg_success_type', None)
    session.pop('reg_email', None)
    session.pop('reg_username', None)
    session.pop('reg_password', None)
    session.pop('reg_fullname', None)
    
    # Render the template with the success data
    return render_template('create_account.html', 
                           success=success_type, 
                           email=email, 
                           credentials=credentials)




@app.route('/dashboard')
@require_user_role
def dashboard():
    """User dashboard route"""
    try:
        user_id = session.get('user_id')
        print(f"[DASHBOARD] User ID: {user_id}")
        
        # Your existing dashboard code here
        exams_df = load_csv_with_cache('exams.csv')
        results_df = load_csv_with_cache('results.csv')

        upcoming_exams, ongoing_exams, completed_exams = [], [], []

        if not exams_df.empty:
            if 'status' not in exams_df.columns:
                exams_df['status'] = 'upcoming'

            upcoming_exams = exams_df[exams_df['status'] == 'upcoming'].to_dict('records')
            ongoing_exams = exams_df[exams_df['status'] == 'ongoing'].to_dict('records')
            completed_exams = exams_df[exams_df['status'] == 'completed'].to_dict('records')

            # Process results for completed exams
            if not results_df.empty:
                for exam in completed_exams:
                    exam_id = int(exam.get('id', 0))
                    r = results_df[
                        (results_df['student_id'].astype(str) == str(session['user_id'])) &
                        (results_df['exam_id'].astype(str) == str(exam_id))
                        ]
                    if not r.empty:
                        score = r.iloc[0].get('score', 0)
                        max_score = r.iloc[0].get('max_score', 0)
                        grade = r.iloc[0].get('grade', 'N/A')
                        exam['result'] = f"{score}/{max_score} ({grade})" if pd.notna(score) and pd.notna(
                            max_score) else 'Recorded'
                    else:
                        exam['result'] = 'Pending'
            else:
                for exam in completed_exams:
                    exam['result'] = 'Pending'

        return render_template('dashboard.html',
                               upcoming_exams=upcoming_exams,
                               ongoing_exams=ongoing_exams,
                               completed_exams=completed_exams)
        
    except Exception as e:
        print(f"[DASHBOARD] Error: {e}")
        flash("Error loading dashboard. Please try again.", "error")
        return redirect(url_for('login'))



@app.route("/results_history")
@require_user_role
def results_history():
    if "user_id" not in session:
        flash("Please login to view your results history.", "danger")
        return redirect(url_for("login"))

    try:
        # Use cache-loading helper (consistent behaviour across app)
        results_df = load_csv_with_cache('results.csv')
        exams_df = load_csv_with_cache('exams.csv')

        # Defensive: if either DataFrame is None or empty, render page with empty results list
        if results_df is None or (hasattr(results_df, "empty") and results_df.empty):
            # render an empty results page with informative flash
            flash("No results found for your account yet.", "info")
            return render_template("results_history.html", results=[])

        if exams_df is None or (hasattr(exams_df, "empty") and exams_df.empty):
            # We can still show results but won't have exam names; show message and render empty
            flash("Exam metadata missing. Contact admin.", "warning")
            return render_template("results_history.html", results=[])

        student_id = str(session["user_id"])

        # safe column checks
        if "student_id" not in results_df.columns or "exam_id" not in results_df.columns:
            flash("Results file is missing required columns. Contact admin.", "error")
            return render_template("results_history.html", results=[])

        # filter results for this user
        student_results = results_df[results_df["student_id"].astype(str) == student_id]
        if student_results.empty:
            flash("No results found for your account yet.", "info")
            return render_template("results_history.html", results=[])

        # merge with exams to get exam names (safe merge - fill missing names)
        merged = student_results.merge(
            exams_df.rename(columns={"id": "exam_id", "name": "exam_name"}),
            left_on="exam_id", right_on="exam_id", how="left", suffixes=("_result", "_exam")
        )

        results = []
        for _, row in merged.iterrows():
            # safe extraction using .get / fallback defaults
            completed_at = row.get("completed_at") or row.get("completed_at_result") or ""
            exam_name = row.get("exam_name") or row.get("name") or f"Exam {row.get('exam_id')}"
            # other numeric fields may be missing; coerce to sensible defaults
            score = row.get("score") if row.get("score") is not None else 0
            max_score = row.get("max_score") if row.get("max_score") is not None else row.get("total_questions", 0)
            percentage = float(row.get("percentage") or 0.0)
            results.append({
                "id": int(row.get("id_result") or row.get("id") or 0),
                "exam_id": int(row.get("exam_id") or 0),
                "exam_name": exam_name,
                "subject": row.get("name") or exam_name,
                "completed_at": completed_at,
                "score": score,
                "max_score": max_score,
                "percentage": round(percentage, 2),
                "grade": row.get("grade") or "N/A",
                "time_taken_minutes": row.get("time_taken_minutes") or 0,
                "correct_answers": int(row.get("correct_answers") or 0),
                "incorrect_answers": int(row.get("incorrect_answers") or 0),
                "unanswered_questions": int(row.get("unanswered_questions") or 0),
            })

        # Sort by completed_at (safe parsing)
        def _parse_date_safe(s):
            try:
                return datetime.strptime(str(s), "%Y-%m-%d %H:%M:%S")
            except Exception:
                return datetime.min

        results.sort(key=lambda r: _parse_date_safe(r.get("completed_at", "")), reverse=True)

        return render_template("results_history.html", results=results)

    except Exception as e:
        print("Error in results_history:", str(e))
        import traceback
        traceback.print_exc()
        flash("Could not load results history.", "danger")
        return render_template("results_history.html", results=[])




@app.route('/exam-instructions/<int:exam_id>')
@require_user_role
def exam_instructions(exam_id):
    exams_df = load_csv_with_cache('exams.csv')
    if exams_df.empty:
        flash('No exams available.', 'error')
        return redirect(url_for('dashboard'))

    exam = exams_df[exams_df['id'].astype(str) == str(exam_id)]
    if exam.empty:
        flash('Exam not found!', 'error')
        return redirect(url_for('dashboard'))

    exam_data = exam.iloc[0].to_dict()

    # defaults
    if 'positive_marks' not in exam_data or pd.isna(exam_data.get('positive_marks')):
        exam_data['positive_marks'] = 1
    if 'negative_marks' not in exam_data or pd.isna(exam_data.get('negative_marks')):
        exam_data['negative_marks'] = 0

    user_id = session.get('user_id')
    active_attempt = get_active_attempt(user_id, exam_id)

    # compute attempts left using exam_attempts.csv (safe load)
    attempts_df = load_csv_with_cache('exam_attempts.csv')
    if attempts_df is None or attempts_df.empty:
        attempts_df = pd.DataFrame(columns=['id','student_id','exam_id','attempt_number','status','start_time','end_time'])

    # normalize
    attempts_df = attempts_df.fillna('')
    user_exam_mask = (attempts_df['student_id'].astype(str) == str(user_id)) & (attempts_df['exam_id'].astype(str) == str(exam_id))
    completed_count = 0
    if not attempts_df.empty and user_exam_mask.any():
        completed_count = int(attempts_df.loc[user_exam_mask & (attempts_df['status'].astype(str).str.lower()=='completed')].shape[0])

    try:
        max_attempts = int(exam_data.get('max_attempts') or 0)
    except Exception:
        max_attempts = 0  # 0 = unlimited

    attempts_left = None
    attempts_exhausted = False
    can_start = True

    if max_attempts > 0:
        attempts_left = max_attempts - completed_count
        if attempts_left <= 0:
            attempts_exhausted = True
            attempts_left = 0
            can_start = False
    else:
        # max_attempts = 0 means unlimited
        attempts_left = None  # Will show as unlimited
        can_start = True

    # Override can_start if there's already an active attempt
    if active_attempt:
        can_start = False  # Should show resume instead

    return render_template(
        'exam_instructions.html',
        exam=exam_data,
        active_attempt=active_attempt,
        attempts_left=attempts_left,
        max_attempts=max_attempts,
        attempts_exhausted=attempts_exhausted,
        can_start=can_start  # Add this new variable
    )



# Replace the start_exam route in your main.py

@app.route('/start-exam/<int:exam_id>', methods=['POST'])
@require_user_role
def start_exam(exam_id):
    """
    ENHANCED: Start exam route with pre-validation and clearer error messages
    """
    user_id = session.get('user_id')
    if not user_id:
        return jsonify({"success": False, "message": "Authentication error."}), 403

    try:
        # CRITICAL: Pre-validate exam data availability before creating attempt
        print(f"Pre-validating exam data for exam_id: {exam_id}")
        
        # Check if exam data can be loaded
        cached_data = get_cached_exam_data(exam_id)
        if not cached_data:
            print("No cached data, attempting preload validation...")
            success, message = preload_exam_data_fixed(exam_id)
            if not success:
                return jsonify({
                    "success": False, 
                    "message": f"Cannot start exam: {message}",
                    "error_type": "data_validation_failed"
                }), 400
            cached_data = get_cached_exam_data(exam_id)
            
        if not cached_data or not cached_data.get('questions'):
            return jsonify({
                "success": False,
                "message": "Exam data validation failed. Please contact administrator.",
                "error_type": "data_unavailable"
            }), 400

        # Load exam metadata
        try:
            exams_df = load_csv_with_cache('exams.csv')
            if exams_df is None or exams_df.empty:
                return jsonify({
                    "success": False,
                    "message": "Exam configuration database unavailable.",
                    "error_type": "config_unavailable"
                }), 500
        except Exception as e:
            print(f"Error loading exams.csv: {e}")
            return jsonify({
                "success": False,
                "message": "Error accessing exam configuration.",
                "error_type": "config_error"
            }), 500

        try:
            exam_row = exams_df[exams_df['id'].astype(str) == str(exam_id)]
            if exam_row.empty:
                return jsonify({
                    "success": False,
                    "message": f"Exam configuration not found (ID: {exam_id}).",
                    "error_type": "exam_not_found"
                }), 404
            exam_data = exam_row.iloc[0].to_dict()
        except Exception as e:
            print(f"Error processing exam data: {e}")
            return jsonify({
                "success": False,
                "message": "Error processing exam configuration.",
                "error_type": "config_processing_error"
            }), 500

        # Get max_attempts
        try:
            max_attempts = int(exam_data.get('max_attempts') or 0)
        except (ValueError, TypeError):
            max_attempts = 0

        # Load and validate exam_attempts data
        attempts_df = None
        try:
            attempts_df = safe_csv_load_with_recovery('exam_attempts.csv')
            if attempts_df is None:
                attempts_df = pd.DataFrame(columns=[
                    'id', 'student_id', 'exam_id', 'attempt_number', 'status', 'start_time', 'end_time'
                ])
            
            # Ensure required columns
            required_cols = ['id', 'student_id', 'exam_id', 'attempt_number', 'status', 'start_time', 'end_time']
            for col in required_cols:
                if col not in attempts_df.columns:
                    attempts_df[col] = ''

        except Exception as e:
            print(f"Error loading attempts data: {e}")
            return jsonify({
                "success": False,
                "message": "Error accessing attempt records.",
                "error_type": "attempts_data_error"
            }), 500

        # Count completed attempts
        completed_attempts = 0
        try:
            if not attempts_df.empty:
                attempts_df['student_id'] = attempts_df['student_id'].astype(str)
                attempts_df['exam_id'] = attempts_df['exam_id'].astype(str)
                attempts_df['status'] = attempts_df['status'].astype(str).fillna('')

                completed_mask = (
                    (attempts_df['student_id'] == str(user_id)) &
                    (attempts_df['exam_id'] == str(exam_id)) &
                    (attempts_df['status'].str.lower() == 'completed')
                )
                completed_attempts = int(completed_mask.sum())
        except Exception as e:
            print(f"Error counting attempts: {e}")
            completed_attempts = 0

        # Check attempt limits
        if max_attempts > 0 and completed_attempts >= max_attempts:
            return jsonify({
                "success": False,
                "message": f"Maximum attempts ({max_attempts}) reached for this exam.",
                "error_type": "max_attempts_reached"
            }), 403

        # Check for existing in-progress attempt
        try:
            if not attempts_df.empty:
                inprog_mask = (
                    (attempts_df['student_id'] == str(user_id)) &
                    (attempts_df['exam_id'] == str(exam_id)) &
                    (attempts_df['status'].str.lower() == 'in_progress')
                )
                if inprog_mask.any():
                    # Resume existing attempt
                    inprog_row = attempts_df[inprog_mask].sort_values('start_time', ascending=False).iloc[0]
                    start_time = inprog_row.get('start_time')
                    if start_time and 'exam_start_time' not in session:
                        session['exam_start_time'] = str(start_time)
                        session.permanent = True
                    try:
                        session['latest_attempt_id'] = int(inprog_row.get('id', 0))
                    except (ValueError, TypeError):
                        pass
                    
                    print(f"Resuming existing attempt {inprog_row.get('id')}")
                    return jsonify({
                        "success": True, 
                        "redirect_url": url_for('exam_page', exam_id=exam_id), 
                        "resumed": True,
                        "message": "Resuming existing attempt"
                    })
        except Exception as e:
            print(f"Error checking in-progress attempts: {e}")

        # Create new attempt
        try:
            # Generate next ID
            next_id = 1
            try:
                if not attempts_df.empty and 'id' in attempts_df.columns:
                    numeric_ids = pd.to_numeric(attempts_df['id'], errors='coerce')
                    valid_ids = numeric_ids.dropna()
                    if not valid_ids.empty:
                        next_id = int(valid_ids.max()) + 1
            except Exception as e:
                print(f"Error generating next ID: {e}")
                next_id = len(attempts_df) + 1

            # Generate attempt number
            attempt_number = 1
            try:
                if not attempts_df.empty:
                    user_exam_mask = (
                        (attempts_df['student_id'] == str(user_id)) &
                        (attempts_df['exam_id'] == str(exam_id))
                    )
                    if user_exam_mask.any():
                        attempt_nums = pd.to_numeric(attempts_df.loc[user_exam_mask, 'attempt_number'], errors='coerce')
                        valid_nums = attempt_nums.dropna()
                        if not valid_nums.empty:
                            attempt_number = int(valid_nums.max()) + 1
            except Exception as e:
                print(f"Error generating attempt number: {e}")

            start_iso = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            new_attempt = {
                "id": next_id,
                "student_id": str(user_id),
                "exam_id": str(exam_id),
                "attempt_number": attempt_number,
                "status": "in_progress",
                "start_time": start_iso,
                "end_time": ""
            }

            # Create new attempts DataFrame
            try:
                if attempts_df.empty:
                    new_attempts_df = pd.DataFrame([new_attempt])
                else:
                    new_attempts_df = pd.concat([attempts_df, pd.DataFrame([new_attempt])], ignore_index=True)
            except Exception as e:
                print(f"Error creating new attempts DataFrame: {e}")
                new_attempts_df = pd.DataFrame([new_attempt])

            # Persist with multiple fallbacks
            persisted = False
            error_details = []
            
            try:
                ok, info = persist_attempts_df(new_attempts_df)
                persisted = bool(ok)
                if not persisted:
                    error_details.append(f"persist_attempts_df: {info}")
            except Exception as e:
                error_details.append(f"persist_attempts_df exception: {str(e)}")

            # Fallback to local file
            if not persisted:
                try:
                    new_attempts_df.to_csv('exam_attempts.csv', index=False)
                    persisted = True
                    print("Saved to local exam_attempts.csv as fallback")
                except Exception as e:
                    error_details.append(f"local file save: {str(e)}")

            if not persisted:
                return jsonify({
                    "success": False, 
                    "message": "Unable to save attempt data. Please try again.",
                    "error_type": "attempt_save_failed",
                    "details": error_details
                }), 500

            # Set session data
            try:
                session['latest_attempt_id'] = int(next_id)
                session['exam_start_time'] = start_iso
                session.permanent = True
            except Exception as e:
                print(f"Error setting session data: {e}")

            # Mark exam active
            try:
                set_exam_active(user_id, session.get('token'), exam_id=exam_id, result_id=next_id, is_active=True)
            except Exception as e:
                print(f"Error setting exam active: {e}")

            print(f"Successfully created new attempt {next_id} for user {user_id}, exam {exam_id}")
            
            return jsonify({
                "success": True, 
                "redirect_url": url_for('exam_page', exam_id=exam_id), 
                "resumed": False,
                "message": "Exam started successfully",
                "attempt_id": next_id
            })

        except Exception as e:
            print(f"Error creating new attempt: {e}")
            import traceback
            traceback.print_exc()
            return jsonify({
                "success": False, 
                "message": f"Error creating exam attempt: {str(e)}",
                "error_type": "attempt_creation_failed"
            }), 500

    except Exception as e:
        print(f"Critical error in start_exam: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({
            "success": False, 
            "message": "System error occurred. Please try again or contact support.",
            "error_type": "critical_system_error"
        }), 500



@app.route('/api/exam-attempts-status/<int:exam_id>')
@require_user_role
def api_exam_attempts_status(exam_id):
    """
    CRASH-SAFE API endpoint for exam attempts status
    """
    user_id = session.get('user_id')
    if not user_id:
        return jsonify({'error': 'not_authenticated'}), 401

    try:
        # SAFE: Load exam info
        try:
            exams_df = load_csv_with_cache('exams.csv')
            if exams_df is None or exams_df.empty:
                return jsonify({'error': 'exam_data_unavailable'}), 500
            
            exam_row = exams_df[exams_df['id'].astype(str) == str(exam_id)]
            if exam_row.empty:
                return jsonify({'error': 'exam_not_found'}), 404
            
            exam_info = exam_row.iloc[0].to_dict()
            max_attempts = int(exam_info.get('max_attempts', 0) or 0)
            
        except Exception as e:
            print(f"Error loading exam info: {e}")
            return jsonify({'error': 'exam_info_error', 'message': str(e)}), 500

        # SAFE: Load attempts with reduced retries
        completed_attempts = 0
        active_exists = False
        
        try:
            attempts_df = safe_csv_load_with_recovery('exam_attempts.csv', max_retries=1)
            
            if attempts_df is not None and hasattr(attempts_df, 'empty'):
                if attempts_df.empty and len(attempts_df.columns) > 0:
                    # Header-only file
                    print("Header-only exam_attempts file - no attempts yet")
                    completed_attempts = 0
                    active_exists = False
                elif not attempts_df.empty:
                    # Has data rows
                    try:
                        attempts_df = attempts_df.fillna('')
                        attempts_df['student_id'] = attempts_df['student_id'].astype(str)
                        attempts_df['exam_id'] = attempts_df['exam_id'].astype(str)
                        attempts_df['status'] = attempts_df['status'].astype(str)

                        completed_mask = (
                            (attempts_df['student_id'] == str(user_id)) &
                            (attempts_df['exam_id'] == str(exam_id)) &
                            (attempts_df['status'].str.lower() == 'completed')
                        )
                        completed_attempts = int(completed_mask.sum())

                        inprog_mask = (
                            (attempts_df['student_id'] == str(user_id)) &
                            (attempts_df['exam_id'] == str(exam_id)) &
                            (attempts_df['status'].str.lower() == 'in_progress')
                        )
                        active_exists = bool(inprog_mask.any())
                        
                    except Exception as e:
                        print(f"Error processing attempts data: {e}")
                        completed_attempts = 0
                        active_exists = False
                else:
                    # Completely empty
                    completed_attempts = 0
                    active_exists = False
            else:
                completed_attempts = 0
                active_exists = False
                
        except Exception as e:
            print(f"Error loading attempts: {e}")
            completed_attempts = 0
            active_exists = False

        # SAFE: Calculate attempts left
        attempts_left = None
        if max_attempts <= 0:
            attempts_left = -1  # unlimited
        else:
            attempts_left = max(0, max_attempts - completed_attempts)

        return jsonify({
            'attempts_left': attempts_left,
            'max_attempts': max_attempts,
            'completed_attempts': completed_attempts,
            'active_attempt_exists': bool(active_exists)
        })

    except Exception as e:
        print(f"Critical error in api_exam_attempts_status: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({
            'error': 'server_error', 
            'message': 'System error occurred'
        }), 500




@app.route('/exam/mark-abandoned/<int:exam_id>', methods=['POST'])
@require_user_role
def mark_exam_abandoned(exam_id):

    user_id = session.get('user_id')
    if not user_id:
        return jsonify({"success": False, "message": "Not authenticated"}), 401

    lock = get_file_lock('exam_attempts')
    with lock:
        try:
            file_id = DRIVE_FILE_IDS.get('exam_attempts')
            attempts_df = pd.DataFrame()
            if file_id and drive_service:
                try:
                    attempts_df = safe_drive_csv_load(drive_service, file_id, friendly_name='exam_attempts.csv')
                except Exception:
                    attempts_df = pd.DataFrame()
            if attempts_df is None or attempts_df.empty:
                local_path = os.path.join(os.getcwd(), "exam_attempts.csv")
                if os.path.exists(local_path):
                    attempts_df = pd.read_csv(local_path, dtype=str)
                else:
                    return jsonify({"success": False, "message": "No attempts file"}), 400

            # Find the latest in_progress row
            mask = (
                (attempts_df['student_id'].astype(str) == str(user_id)) &
                (attempts_df['exam_id'].astype(str) == str(exam_id)) &
                (attempts_df['status'].astype(str) == 'in_progress')
            )
            if not mask.any():
                return jsonify({"success": False, "message": "No in-progress attempt found"}), 404

            idxs = attempts_df[mask].index.tolist()
            latest_idx = idxs[-1]
            attempts_df.at[latest_idx, 'status'] = 'abandoned'
            attempts_df.at[latest_idx, 'end_time'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            ok, info = persist_attempts_df(attempts_df)
            if ok:
                return jsonify({"success": True, "message": "Marked as abandoned"})
            else:
                return jsonify({"success": False, "message": f"Save failed: {info}"}), 500
        except Exception as e:
            print(f"mark_exam_abandoned error: {e}")
            return jsonify({"success": False, "message": "Server error"}), 500




@app.route('/preload-exam/<int:exam_id>')
@require_user_role
def preload_exam_route(exam_id):
    """API endpoint to preload exam data - ENHANCED with better error handling"""
    try:
        # Check if already cached and valid
        cached_data = get_cached_exam_data(exam_id)
        if cached_data and cached_data.get('exam_id') == exam_id:
            return jsonify({
                'success': True,
                'message': f"Using cached data with {cached_data['total_questions']} questions",
                'exam_id': exam_id,
                'cached': True,
                'question_count': cached_data['total_questions']
            })

        # Attempt preload with detailed error reporting
        success, message = preload_exam_data_fixed(exam_id)
        
        status_code = 200 if success else 400
        response_data = {
            'success': success,
            'message': message,
            'exam_id': exam_id,
            'cached': False
        }
        
        # Add diagnostic info for failures
        if not success:
            # Check if questions file exists
            try:
                questions_df = load_csv_with_cache('questions.csv')
                if questions_df is not None and not questions_df.empty:
                    available_exams = sorted(questions_df['exam_id'].unique().tolist()) if 'exam_id' in questions_df.columns else []
                    response_data['available_exam_ids'] = available_exams
                    response_data['total_questions_in_db'] = len(questions_df)
                else:
                    response_data['diagnostic'] = 'Questions database is empty or inaccessible'
            except Exception as e:
                response_data['diagnostic'] = f'Error checking questions database: {str(e)}'
        
        return jsonify(response_data), status_code
        
    except Exception as e:
        print(f"Error in preload route: {e}")
        import traceback
        traceback.print_exc()
        
        return jsonify({
            'success': False,
            'message': f"Server error during preload: {str(e)}",
            'exam_id': exam_id,
            'error_type': 'server_error'
        }), 500


from markupsafe import Markup, escape
from datetime import datetime
from flask import render_template, request, session, flash, redirect, url_for

def sanitize_for_display(s):
    """
    Escape HTML-special characters but preserve safe <br>.
    Convert newlines to actual <br> tags so they render correctly.
    """
    from markupsafe import Markup, escape
    if s is None:
        return Markup("")
    s = str(s)

    # Normalize CRLF -> LF
    s = s.replace("\r\n", "\n").replace("\r", "\n")

    # Escape HTML to prevent injection
    escaped = escape(s)

    # ✅ Fix: Convert newlines to real <br> tags
    with_breaks = escaped.replace("\n", Markup("<br>"))

    return Markup(with_breaks)



# Replace the exam_page route in your main.py

@app.route('/exam/<int:exam_id>')
@require_user_role
def exam_page(exam_id):
    """
    FIXED: Exam page with improved error handling and consistent data loading
    """
    user_id = session.get('user_id')
    
    try:
        print(f"Loading exam page for exam_id: {exam_id}")

        # Validate request context
        if not request or not hasattr(request, 'endpoint'):
            print("Invalid request context detected")
            flash("Invalid request. Please try again.", "error")
            return redirect(url_for('dashboard'))

        # CRITICAL: Ensure exam data is loaded with comprehensive error handling
        cached_data = None
        max_preload_attempts = 3
        
        for attempt in range(max_preload_attempts):
            try:
                print(f"Checking cached data (attempt {attempt + 1})")
                cached_data = get_cached_exam_data(exam_id)
                
                if cached_data:
                    print(f"Found valid cached data with {len(cached_data.get('questions', []))} questions")
                    break
                    
                print(f"No cached data found, attempting preload (attempt {attempt + 1})")
                success, message = preload_exam_data_fixed(exam_id)
                
                if success:
                    cached_data = get_cached_exam_data(exam_id)
                    if cached_data:
                        print(f"Preload successful: {len(cached_data.get('questions', []))} questions loaded")
                        break
                    else:
                        print(f"Preload reported success but no cached data found (attempt {attempt + 1})")
                else:
                    print(f"Preload failed (attempt {attempt + 1}): {message}")
                    if attempt == max_preload_attempts - 1:  # Last attempt
                        # Provide specific error messages based on failure type
                        if "Questions data is unavailable" in message:
                            flash("Questions database is currently unavailable. Please contact administrator.", "error")
                        elif "No questions found for exam ID" in message:
                            flash(f"No questions are configured for this exam (ID: {exam_id}). Please contact administrator.", "error")
                        elif "Exam metadata not found" in message:
                            flash(f"Exam configuration not found (ID: {exam_id}). Please contact administrator.", "error")
                        else:
                            flash(f"Unable to load exam data: {message}. Please try again or contact support.", "error")
                        return redirect(url_for('dashboard'))
                        
            except Exception as e:
                print(f"Error during preload attempt {attempt + 1}: {e}")
                if attempt == max_preload_attempts - 1:  # Last attempt
                    flash(f"System error loading exam data: {str(e)}. Please contact support.", "error")
                    return redirect(url_for('dashboard'))
                time.sleep(0.5)  # Brief delay before retry

        if not cached_data:
            flash("Unable to load exam data after multiple attempts. Please try again or contact support.", "error")
            return redirect(url_for('dashboard'))

        # Validate cached data structure
        try:
            exam_data = cached_data.get('exam_info') or {}
            questions = cached_data.get('questions') or []
            
            if not exam_data:
                flash("Exam configuration is invalid. Please contact administrator.", "error")
                return redirect(url_for('dashboard'))
                
            if not questions:
                flash("No questions available for this exam. Please contact administrator.", "error")
                return redirect(url_for('dashboard'))
                
            print(f"Validated exam data: {len(questions)} questions available")
            
        except Exception as e:
            print(f"Error validating cached data: {e}")
            flash("Invalid exam data structure. Please contact support.", "error")
            return redirect(url_for('dashboard'))

        # Initialize session containers
        try:
            if 'exam_answers' not in session:
                session['exam_answers'] = {}
            if 'marked_for_review' not in session:
                session['marked_for_review'] = []
        except Exception as e:
            print(f"Error initializing session data: {e}")
            session['exam_answers'] = {}
            session['marked_for_review'] = []

        # Get active attempt with error handling
        active_attempt = None
        try:
            active_attempt = get_active_attempt(user_id, exam_id)
            if active_attempt:
                print(f"Found active attempt: {active_attempt.get('id')} - {active_attempt.get('status')}")
        except Exception as e:
            print(f"Error getting active attempt: {e}")

        # Calculate attempts and limits
        completed_attempts = 0
        max_attempts = 0
        attempts_left = None
        attempts_exhausted = False

        try:
            max_attempts = int(exam_data.get('max_attempts') or 0)
        except (ValueError, TypeError):
            max_attempts = 0

        try:
            attempts_df = safe_csv_load_with_recovery('exam_attempts.csv')
            if attempts_df is not None and not attempts_df.empty:
                attempts_df['student_id'] = attempts_df['student_id'].astype(str)
                attempts_df['exam_id'] = attempts_df['exam_id'].astype(str)
                attempts_df['status'] = attempts_df.get('status', '').astype(str).fillna('')

                completed_mask = (
                    (attempts_df['student_id'] == str(user_id)) &
                    (attempts_df['exam_id'] == str(exam_id)) &
                    (attempts_df['status'].str.lower() == 'completed')
                )
                completed_attempts = int(completed_mask.sum())
        except Exception as e:
            print(f"Error calculating attempts: {e}")
            completed_attempts = 0

        if max_attempts > 0:
            attempts_left = max_attempts - completed_attempts
            if attempts_left <= 0:
                attempts_left = 0
                attempts_exhausted = True

        # Calculate remaining time with improved logic
        try:
            duration_secs = int(float(exam_data.get('duration_minutes') or exam_data.get('duration') or 0) * 60)
        except (ValueError, TypeError):
            duration_secs = 3600  # Default 1 hour

        remaining_seconds = duration_secs

        # Time calculation from active attempt
        if active_attempt and active_attempt.get('start_time'):
            try:
                start_dt = pd.to_datetime(active_attempt.get('start_time'))
                if start_dt.tzinfo is None:
                    start_dt = start_dt.tz_localize("UTC")
                now = pd.Timestamp.now(tz="UTC")
                elapsed = (now - start_dt).total_seconds()
                remaining_seconds = max(0, duration_secs - int(elapsed))
                session['exam_start_time'] = start_dt.strftime('%Y-%m-%d %H:%M:%S')
                print(f"Calculated remaining time: {remaining_seconds} seconds")
            except Exception as e:
                print(f"Error calculating remaining time: {e}")
                remaining_seconds = duration_secs

        # Handle fresh start vs resume scenarios
        is_fresh_start = False
        if not (active_attempt and str(active_attempt.get('status', '')).lower() == 'in_progress'):
            remaining_seconds = duration_secs
            is_fresh_start = True
            print(f"Fresh start detected: Using full duration of {duration_secs} seconds")
            
        # Auto-submit expired attempts
        elif active_attempt and remaining_seconds <= 0:
            print(f"Exam time expired. Auto-submitting attempt {active_attempt.get('id')}")
            try:
                update_exam_attempt_status(user_id, exam_id, 'completed')
                session.pop('exam_answers', None)
                session.pop('marked_for_review', None)
                session.pop('exam_start_time', None)
                flash("Your previous exam attempt expired due to time limit. Please start a new attempt.", "warning")
                return redirect(url_for('exam_instructions', exam_id=exam_id))
            except Exception as e:
                print(f"Error auto-submitting expired attempt: {e}")
                flash("Previous attempt expired. Please start a new attempt.", "warning")
                return redirect(url_for('exam_instructions', exam_id=exam_id))
        
        # Determine button states
        show_resume_button = (
            active_attempt and 
            str(active_attempt.get('status', '')).lower() == 'in_progress' and 
            remaining_seconds > 0
        )
        show_start_button = (
            not attempts_exhausted and 
            not show_resume_button
        )

        # Build question display
        try:
            q_index = int(request.args.get('q', 0) or 0)
            q_index = max(0, min(q_index, len(questions) - 1))

            current_question = dict(questions[q_index]) if q_index < len(questions) else {}
            
            # Sanitize display text
            current_question['question_text'] = sanitize_for_display(current_question.get('question_text', ''))
            for opt in ['option_a', 'option_b', 'option_c', 'option_d']:
                current_question[opt] = sanitize_for_display(current_question.get(opt, ''))

            selected_answer = session.get('exam_answers', {}).get(str(current_question.get('id')))

            # Build question palette
            palette = {}
            for i, q in enumerate(questions):
                qid = str(q.get('id', ''))
                if qid in session.get('marked_for_review', []):
                    palette[i] = 'review'
                elif qid in session.get('exam_answers', {}):
                    palette[i] = 'answered'
                else:
                    palette[i] = 'not-visited'
            
            if palette.get(q_index) == 'not-visited':
                palette[q_index] = 'visited'

        except Exception as e:
            print(f"Error building question display: {e}")
            flash("Error loading question data. Please refresh or contact support.", "error")
            return redirect(url_for('dashboard'))

        # Mark exam active
        try:
            set_exam_active(user_id, session.get('token'), exam_id=exam_id, 
                          result_id=session.get('latest_attempt_id'), is_active=True)
        except Exception as e:
            print(f"Error marking exam active: {e}")

        # Render template
        return render_template(
            'exam_page.html',
            exam=exam_data,
            question=current_question,
            current_index=q_index,
            selected_answer=selected_answer,
            total_questions=len(questions),
            palette=palette,
            questions=questions,
            remaining_seconds=int(remaining_seconds),
            active_attempt=active_attempt,
            attempts_left=(attempts_left if attempts_left is not None else -1),
            attempts_exhausted=attempts_exhausted,
            show_start_button=show_start_button,
            show_resume_button=show_resume_button,
            is_fresh_start=is_fresh_start
        )

    except MemoryError as e:
        print(f"MEMORY ERROR in exam_page: {e}")
        flash("System memory issue. Please contact administrator.", "error")
        return redirect(url_for('dashboard'))
    except KeyboardInterrupt:
        print("KeyboardInterrupt received - handling gracefully")
        raise
    except SystemExit:
        print("SystemExit received - handling gracefully")
        raise
    except Exception as e:
        print(f"CRITICAL ERROR in exam_page: {e}")
        import traceback
        traceback.print_exc()
        
        # Log error details
        try:
            with open('exam_page_errors.log', 'a') as f:
                f.write(f"{datetime.now()}: {str(e)}\n{traceback.format_exc()}\n\n")
        except:
            pass
            
        flash("An error occurred loading the exam page. Please try again or contact support.", "error")
        return redirect(url_for('dashboard'))



@app.route('/exam/<int:exam_id>/navigate', methods=['POST'])
@require_user_role
def navigate_exam(exam_id):
    """FIXED navigation with better error handling"""
    try:
        action = request.form.get('action')
        current_index = int(request.form.get('current_index', 0))
        question_id = request.form.get('question_id')

        print(f"Navigation: action={action}, current_index={current_index}, question_id={question_id}")

        # Initialize session data if not exists
        if 'exam_answers' not in session:
            session['exam_answers'] = {}
        if 'marked_for_review' not in session:
            session['marked_for_review'] = []

        # Get question info from cached data
        cached_data = get_cached_exam_data(exam_id)
        if not cached_data:
            flash("Exam session expired. Please restart the exam.", "error")
            return redirect(url_for('dashboard'))

        question_info = None
        for q in cached_data['questions']:
            if str(q['id']) == str(question_id):
                question_info = q
                break

        if question_info:
            question_type = question_info.get('question_type', 'MCQ')

            # Handle clear action first
            if action == 'clear':
                if question_id and question_id in session['exam_answers']:
                    del session['exam_answers'][question_id]
                if question_id and question_id in session['marked_for_review']:
                    session['marked_for_review'].remove(question_id)
                session.modified = True
                return redirect(url_for('exam_page', exam_id=exam_id, q=current_index))

            # Get answer based on question type
            if question_type == 'MCQ':
                answer = request.form.get('answer')
            elif question_type == 'MSQ':
                answer = request.form.getlist('answer')
            elif question_type == 'NUMERIC':
                answer = request.form.get('numeric_answer')
            else:
                answer = request.form.get('answer')

            # Save answer if provided
            if answer and question_id:
                if question_type == 'MSQ' and isinstance(answer, list) and len(answer) > 0:
                    session['exam_answers'][question_id] = answer
                elif question_type == 'NUMERIC' and str(answer).strip():
                    session['exam_answers'][question_id] = str(answer).strip()
                elif question_type == 'MCQ' and str(answer).strip():
                    session['exam_answers'][question_id] = str(answer).strip()

                # Remove from review if answered
                if question_id in session['marked_for_review']:
                    session['marked_for_review'].remove(question_id)

                session.modified = True
                print(f"Saved answer for question {question_id}: {answer}")

        # Handle navigation
        if action == 'prev':
            new_index = max(0, current_index - 1)
        elif action == 'next':
            new_index = min(len(cached_data['questions']) - 1, current_index + 1)
        elif action == 'review':
            if question_id and question_id not in session['marked_for_review']:
                session['marked_for_review'].append(question_id)
                session.modified = True
            new_index = min(len(cached_data['questions']) - 1, current_index + 1)
        elif action == 'submit':
            return redirect(url_for('submit_exam', exam_id=exam_id))
        else:
            new_index = current_index

        return redirect(url_for('exam_page', exam_id=exam_id, q=new_index))

    except Exception as e:
        print(f"Error in navigation: {e}")
        flash("Navigation error. Please try again.", "error")
        return redirect(url_for('exam_page', exam_id=exam_id, q=0))


@app.route('/exam/<int:exam_id>/clear-answer', methods=['POST'])
@require_user_role
def clear_answer(exam_id):
    """AJAX endpoint for clearing question answers - FIXED"""
    try:
        question_id = request.json.get('question_id')

        if 'exam_answers' not in session:
            session['exam_answers'] = {}
        if 'marked_for_review' not in session:
            session['marked_for_review'] = []

        if question_id and question_id in session['exam_answers']:
            del session['exam_answers'][question_id]

        if question_id and question_id in session['marked_for_review']:
            session['marked_for_review'].remove(question_id)

        session.modified = True

        return jsonify({
            'success': True,
            'message': 'Selection cleared successfully'
        })
    except Exception as e:
        print(f"Error clearing answer: {e}")
        return jsonify({
            'success': False,
            'message': 'Error clearing selection'
        }), 500


@app.route('/submit-exam/<int:exam_id>', methods=['GET', 'POST'])
@require_user_role
def submit_exam(exam_id):
    """
    CRASH-SAFE exam submission with comprehensive error handling
    """
    if request.method == 'GET':
        return render_template('submit_confirm.html', exam_id=exam_id)

    try:
        # SAFE: Get cached exam data
        cached_data = None
        try:
            cached_data = get_cached_exam_data(exam_id)
            if not cached_data:
                # Try to reload
                success, message = preload_exam_data_fixed(exam_id)
                if success:
                    cached_data = get_cached_exam_data(exam_id)
        except Exception as e:
            print(f"Error getting cached exam data: {e}")

        if not cached_data:
            flash("Exam session expired. Please contact administrator.", "error")
            return redirect(url_for('dashboard'))

        # SAFE: Extract exam data
        try:
            exam_data = cached_data['exam_info']
            questions = cached_data['questions']
        except (KeyError, TypeError) as e:
            print(f"Error extracting exam data: {e}")
            flash("Invalid exam data. Please contact support.", "error")
            return redirect(url_for('dashboard'))

        # SAFE: Get default marks
        try:
            default_positive_marks = float(exam_data.get('positive_marks', 1) or 1)
            default_negative_marks = float(exam_data.get('negative_marks', 0) or 0)
        except (ValueError, TypeError):
            default_positive_marks = 1.0
            default_negative_marks = 0.0

        total_questions = len(questions)
        if total_questions == 0:
            flash("No questions found for this exam.", "error")
            return redirect(url_for('dashboard'))

        # SAFE: Initialize counters
        total_score = 0.0
        max_possible_score = 0.0
        correct_answers = 0
        incorrect_answers = 0
        unanswered_questions = 0

        # SAFE: Load existing data
        try:
            results_df = safe_csv_load_with_recovery('results.csv')
            if results_df is None:
                results_df = pd.DataFrame()
        except Exception as e:
            print(f"Error loading results: {e}")
            results_df = pd.DataFrame()

        try:
            responses_df = safe_csv_load_with_recovery('responses.csv')
            if responses_df is None:
                responses_df = pd.DataFrame()
        except Exception as e:
            print(f"Error loading responses: {e}")
            responses_df = pd.DataFrame()

        # SAFE: Generate next IDs
        try:
            next_result_id = 1
            if not results_df.empty and 'id' in results_df.columns:
                numeric_ids = pd.to_numeric(results_df['id'], errors='coerce').dropna()
                if not numeric_ids.empty:
                    next_result_id = int(numeric_ids.max()) + 1
        except Exception as e:
            print(f"Error generating result ID: {e}")
            next_result_id = int(time.time())  # Fallback to timestamp

        try:
            next_response_id = 1
            if not responses_df.empty and 'id' in responses_df.columns:
                numeric_ids = pd.to_numeric(responses_df['id'], errors='coerce').dropna()
                if not numeric_ids.empty:
                    next_response_id = int(numeric_ids.max()) + 1
        except Exception as e:
            print(f"Error generating response ID: {e}")
            next_response_id = int(time.time())  # Fallback to timestamp

        response_records = []

        # SAFE: Process each question
        for question in questions:
            try:
                qid = str(question.get('id', ''))
                if not qid:
                    continue

                question_type = question.get('question_type', 'MCQ')

                # SAFE: Get marks for this question
                try:
                    q_positive_marks = float(question.get('positive_marks', default_positive_marks) or default_positive_marks)
                    q_negative_marks = float(question.get('negative_marks', default_negative_marks) or default_negative_marks)
                except (ValueError, TypeError):
                    q_positive_marks = default_positive_marks
                    q_negative_marks = default_negative_marks

                max_possible_score += q_positive_marks

                # SAFE: Get correct answer
                correct_answer = question.get('parsed_correct_answer')

                # SAFE: Get given answer
                given_answer = session.get('exam_answers', {}).get(qid, None)

                # SAFE: Check if attempted
                is_attempted = False
                try:
                    if given_answer is not None:
                        if question_type == 'MSQ':
                            is_attempted = isinstance(given_answer, list) and len(given_answer) > 0
                        elif question_type == 'NUMERIC':
                            is_attempted = str(given_answer).strip() != ''
                        else:
                            is_attempted = str(given_answer).strip() not in ['', 'None', 'null']
                except Exception:
                    is_attempted = False

                # SAFE: Check correctness and calculate score
                is_correct = False
                question_score = 0.0

                try:
                    if is_attempted:
                        tolerance = question.get('tolerance', 0.1) if question_type == 'NUMERIC' else 0.1
                        is_correct = check_answer(given_answer, correct_answer, question_type, tolerance)
                        question_score = calculate_question_score(is_correct, question_type, q_positive_marks, q_negative_marks)
                        
                        if is_correct:
                            correct_answers += 1
                        else:
                            incorrect_answers += 1
                    else:
                        unanswered_questions += 1
                except Exception as e:
                    print(f"Error checking answer for question {qid}: {e}")
                    is_correct = False
                    question_score = 0.0
                    if is_attempted:
                        incorrect_answers += 1
                    else:
                        unanswered_questions += 1

                total_score += question_score

                # SAFE: Prepare answer strings
                try:
                    if question_type == 'MSQ' and isinstance(given_answer, list):
                        given_answer_str = json.dumps(given_answer)
                    elif given_answer is not None:
                        given_answer_str = str(given_answer)
                    else:
                        given_answer_str = ""

                    if question_type == 'MSQ' and isinstance(correct_answer, list):
                        correct_answer_str = json.dumps(correct_answer)
                    elif correct_answer is not None:
                        correct_answer_str = str(correct_answer)
                    else:
                        correct_answer_str = ""
                except Exception as e:
                    print(f"Error preparing answer strings for question {qid}: {e}")
                    given_answer_str = ""
                    correct_answer_str = ""

                # SAFE: Create response record
                try:
                    response_record = {
                        'id': int(next_response_id),
                        'result_id': int(next_result_id),
                        'exam_id': int(exam_id),
                        'question_id': int(question.get('id', 0)),
                        'given_answer': given_answer_str,
                        'correct_answer': correct_answer_str,
                        'is_correct': bool(is_correct),
                        'marks_obtained': float(question_score),
                        'question_type': str(question_type),
                        'is_attempted': bool(is_attempted)
                    }
                    response_records.append(response_record)
                    next_response_id += 1
                except Exception as e:
                    print(f"Error creating response record for question {qid}: {e}")

            except Exception as e:
                print(f"Error processing question {question.get('id', 'unknown')}: {e}")
                continue

        # SAFE: Calculate final metrics
        try:
            percentage = (total_score / max_possible_score) * 100 if max_possible_score > 0 else 0.0
            
            # Determine grade
            if percentage >= 90:
                grade = 'A+'
            elif percentage >= 80:
                grade = 'A'
            elif percentage >= 70:
                grade = 'B'
            elif percentage >= 60:
                grade = 'C'
            else:
                grade = 'F'
        except Exception as e:
            print(f"Error calculating final metrics: {e}")
            percentage = 0.0
            grade = 'F'

        # SAFE: Calculate time taken
        time_taken_minutes = None
        try:
            start_time_str = session.get('exam_start_time')
            if start_time_str:
                start_time = datetime.strptime(start_time_str, '%Y-%m-%d %H:%M:%S')
                time_taken_seconds = (datetime.now() - start_time).total_seconds()
                time_taken_minutes = round(time_taken_seconds / 60, 2)
        except Exception as e:
            print(f"Error calculating time taken: {e}")

        # SAFE: Create result record
        try:
            new_result = {
                'id': int(next_result_id),
                'student_id': int(session['user_id']),
                'exam_id': int(exam_id),
                'score': float(total_score),
                'total_questions': int(total_questions),
                'correct_answers': int(correct_answers),
                'incorrect_answers': int(incorrect_answers),
                'unanswered_questions': int(unanswered_questions),
                'max_score': float(max_possible_score),
                'percentage': float(round(percentage, 2)),
                'grade': str(grade),
                'time_taken_minutes': float(time_taken_minutes) if time_taken_minutes is not None else None,
                'completed_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
        except Exception as e:
            print(f"Error creating result record: {e}")
            flash("Error preparing result data. Please try again.", "error")
            return redirect(url_for('exam_page', exam_id=exam_id))

        # SAFE: Save results atomically
        try:
            save_success, save_message = safe_dual_file_save(results_df, responses_df, new_result, response_records)
            
            if not save_success:
                print(f"Failed to save result: {save_message}")
                flash(f'Error saving exam results: {save_message}. Please try again.', 'error')
                return redirect(url_for('exam_page', exam_id=exam_id))
        except Exception as e:
            print(f"Error in atomic save: {e}")
            flash("Critical error saving results. Please contact support immediately.", "error")
            return redirect(url_for('exam_page', exam_id=exam_id))

        # SAFE: Update session
        try:
            session['latest_attempt_id'] = int(next_result_id)
            set_exam_active(session.get('user_id'), session.get('token'), is_active=False)
        except Exception as e:
            print(f"Error updating session: {e}")

        # SAFE: Clear session data
        try:
            session.pop('exam_answers', None)
            session.pop('marked_for_review', None)
            session.pop('exam_start_time', None)
            cache_key = f'exam_data_{exam_id}'
            session.pop(cache_key, None)
        except Exception as e:
            print(f"Error clearing session: {e}")

        # SAFE: Update exam_attempts
        try:
            update_exam_attempt_status(session.get('user_id'), exam_id, 'completed')
        except Exception as e:
            print(f"Non-critical error updating exam attempts: {e}")

        flash('Exam submitted successfully!', 'success')
        return redirect(url_for('result', exam_id=exam_id))

    except Exception as e:
        print(f"Critical error in submit_exam: {e}")
        import traceback
        traceback.print_exc()
        flash('Critical error during submission. Please contact support.', 'error')
        try:
            return redirect(url_for('dashboard'))
        except:
            return render_template('error.html', error_code=500, 
                                 error_message="Critical system error"), 500



@app.route('/result/<int:exam_id>', defaults={'result_id': None})
@app.route('/result/<int:exam_id>/<int:result_id>')
@require_user_role
def result(exam_id, result_id):
    """Result page with support for history view"""
    from_history = request.args.get("from_history", "0") == "1"
    try:
        results_df = load_csv_with_cache('results.csv')
        exams_df = load_csv_with_cache('exams.csv')

        if results_df.empty or exams_df.empty:
            flash('Result not found!', 'error')
            return redirect(url_for('dashboard'))

        user_id = int(session['user_id'])

        if result_id:  # Specific attempt from history
            r = results_df[
                (results_df['id'].astype('Int64') == int(result_id)) &
                (results_df['student_id'].astype('Int64') == user_id) &
                (results_df['exam_id'].astype('Int64') == int(exam_id))
            ]
        else:  # Latest attempt
            attempt_id = session.get('latest_attempt_id')
            if attempt_id:
                r = results_df[
                    (results_df['id'].astype('Int64') == int(attempt_id)) &
                    (results_df['student_id'].astype('Int64') == user_id) &
                    (results_df['exam_id'].astype('Int64') == int(exam_id))
                ]
            else:
                r = results_df[
                    (results_df['student_id'].astype('Int64') == user_id) &
                    (results_df['exam_id'].astype('Int64') == int(exam_id))
                ].sort_values('id', ascending=False).head(1)

        exam = exams_df[exams_df['id'].astype('Int64') == int(exam_id)]

        if r.empty or exam.empty:
            flash('Result not found!', 'error')
            return redirect(url_for('dashboard'))

        result_data = r.iloc[0].to_dict()
        exam_data = exam.iloc[0].to_dict()

        return render_template('result.html', result=result_data, exam=exam_data, from_history=from_history)

    except Exception as e:
        print("Error loading result:", e)
        flash("Error loading result page.", "error")
        return redirect(url_for('dashboard'))



@app.route('/response/<int:exam_id>', defaults={'result_id': None})
@app.route('/response/<int:exam_id>/<int:result_id>')
@require_user_role
def response_page(exam_id, result_id):
    """Response analysis page with support for history view (robust against missing CSVs)"""
    from_history = request.args.get("from_history", "0") == "1"
    try:
        results_df = load_csv_with_cache('results.csv')
        responses_df = load_csv_with_cache('responses.csv')
        exams_df = load_csv_with_cache('exams.csv')

        # Defensive checks
        if results_df is None or (hasattr(results_df, "empty") and results_df.empty):
            flash('No results available.', 'info')
            return redirect(url_for('dashboard'))
        if responses_df is None or (hasattr(responses_df, "empty") and responses_df.empty):
            flash('No responses available.', 'info')
            return redirect(url_for('dashboard'))
        if exams_df is None or (hasattr(exams_df, "empty") and exams_df.empty):
            flash('Exam metadata missing. Contact admin.', 'warning')
            return redirect(url_for('dashboard'))

        user_id = int(session['user_id'])

        # If specific attempt (from history)
        if result_id:
            user_results = results_df[
                (results_df['id'].astype('Int64') == int(result_id)) &
                (results_df['student_id'].astype('Int64') == user_id) &
                (results_df['exam_id'].astype('Int64') == int(exam_id))
            ]
        else:
            # Otherwise latest attempt
            user_results = results_df[
                (results_df['student_id'].astype('Int64') == user_id) &
                (results_df['exam_id'].astype('Int64') == int(exam_id))
            ].sort_values('id', ascending=False).head(1)

        if user_results.empty:
            flash('Response not found!', 'error')
            return redirect(url_for('dashboard'))

        result_record = user_results.iloc[0]
        result_id = int(result_record['id'])
        result_data = result_record.to_dict()

        # Get exam data
        exam_record = exams_df[exams_df['id'].astype('Int64') == int(exam_id)]
        if exam_record.empty:
            flash('Exam not found!', 'error')
            return redirect(url_for('dashboard'))
        exam_data = exam_record.iloc[0].to_dict()

        # Get responses for this result
        if 'exam_id' in responses_df.columns:
            user_responses = responses_df[
                (responses_df['result_id'].astype('Int64') == result_id) &
                (responses_df['exam_id'].astype('Int64') == int(exam_id))
            ].sort_values('question_id')
        else:
            user_responses = responses_df[
                responses_df['result_id'].astype('Int64') == result_id
            ].sort_values('question_id')

        if user_responses.empty:
            flash('No detailed responses saved for this result.', 'info')
            return redirect(url_for('dashboard'))

        # Collect attempted question IDs
        question_ids = set(user_responses['question_id'].astype(int).tolist())

        # Try cache first for exam questions
        cached_data = get_cached_exam_data(exam_id)
        questions_dict = {}
        if cached_data:
            for q in cached_data['questions']:
                if int(q['id']) in question_ids:
                    questions_dict[int(q['id'])] = q
        else:
            questions_df = load_csv_with_cache('questions.csv')
            if questions_df is None or (hasattr(questions_df, "empty") and questions_df.empty):
                flash('Questions metadata missing. Contact admin.', 'error')
                return redirect(url_for('dashboard'))
            filtered_questions = questions_df[questions_df['id'].astype(int).isin(question_ids)]
            for _, q in filtered_questions.iterrows():
                q_dict = q.to_dict()
                has_image, image_url = process_question_image_fixed_ssl_safe(q_dict)
                q_dict['has_image'] = has_image
                q_dict['image_url'] = image_url
                questions_dict[int(q['id'])] = q_dict

        # Build question response objects
        from markupsafe import Markup
        question_responses = []
        for _, response in user_responses.iterrows():
            qid = int(response['question_id'])
            qdata = questions_dict.get(qid, {})

            if not qdata:
                continue

            # 🔹 Sanitize question + options (same as exam_page)
            qdata['question_text'] = sanitize_for_display(qdata.get('question_text', ''))
            qdata['option_a'] = sanitize_for_display(qdata.get('option_a', ''))
            qdata['option_b'] = sanitize_for_display(qdata.get('option_b', ''))
            qdata['option_c'] = sanitize_for_display(qdata.get('option_c', ''))
            qdata['option_d'] = sanitize_for_display(qdata.get('option_d', ''))

            given_answer_str = str(response.get('given_answer') or '')
            correct_answer_str = str(response.get('correct_answer') or '')
            qtype = response.get('question_type') or qdata.get('question_type', 'MCQ')

            # Parse answers
            try:
                if qtype == 'MSQ' and given_answer_str.strip():
                    if given_answer_str.startswith('[') and given_answer_str.endswith(']'):
                        given_answer = json.loads(given_answer_str)
                    else:
                        given_answer = [ans.strip() for ans in given_answer_str.split(',') if ans.strip()]
                else:
                    given_answer = given_answer_str if given_answer_str not in ['None', '', None] else None
            except Exception:
                given_answer = given_answer_str if given_answer_str not in ['None', '', None] else None

            try:
                if qtype == 'MSQ' and correct_answer_str.strip():
                    if correct_answer_str.startswith('[') and correct_answer_str.endswith(']'):
                        correct_answer = json.loads(correct_answer_str)
                    else:
                        correct_answer = [ans.strip() for ans in correct_answer_str.split(',') if ans.strip()]
                else:
                    correct_answer = correct_answer_str if correct_answer_str not in ['None', '', None] else None
            except Exception:
                correct_answer = correct_answer_str if correct_answer_str not in ['None', '', None] else None

            is_attempted = response.get('is_attempted', True)
            if pd.isna(is_attempted):
                if given_answer_str in [None, "", "null", "None"]:
                    is_attempted = False
                elif qtype == 'MSQ' and (not given_answer or len(given_answer) == 0):
                    is_attempted = False
                else:
                    is_attempted = bool(given_answer_str and str(given_answer_str).strip())

            response_data = {
                'question': qdata,
                'given_answer': given_answer,
                'correct_answer': correct_answer,
                'is_correct': bool(response.get('is_correct', False)),
                'is_attempted': is_attempted,
                'marks_obtained': float(response.get('marks_obtained') or 0),
                'question_type': qtype
            }
            question_responses.append(response_data)

        return render_template(
            'response.html',
            exam=exam_data,
            result=result_data,
            responses=question_responses,
            from_history=from_history
        )

    except Exception as e:
        print(f"Error in response page: {e}")
        import traceback
        traceback.print_exc()
        flash('Error loading response analysis.', 'error')
        return redirect(url_for('dashboard'))


@app.route('/response-pdf/<int:exam_id>')
@require_user_role
def response_pdf(exam_id):
    """Complete PDF using ReportLab - handles all Unicode"""
    try:
        from reportlab.lib.pagesizes import letter
        from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle
        from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
        from reportlab.lib.units import inch
        from reportlab.lib import colors
        from reportlab.lib.enums import TA_CENTER
        from io import BytesIO
        
        user_id = session.get('user_id')
        username = session.get('username', 'Student')
        full_name = session.get('full_name', username)
        
        # Get all your data (same as before)
        exams_df = load_csv_with_cache('exams.csv')
        exam_info = exams_df[exams_df['id'] == exam_id]
        if exam_info.empty:
            flash('Exam not found.', 'error')
            return redirect(url_for('dashboard'))
        
        exam = exam_info.iloc[0]
        
        results_df = load_csv_with_cache('results.csv')
        user_result = results_df[
            (results_df['student_id'] == user_id) & 
            (results_df['exam_id'] == exam_id)
        ].tail(1)
        
        if user_result.empty:
            flash('No results found.', 'error')
            return redirect(url_for('dashboard'))
        
        result = user_result.iloc[0]
        result_id = result['id']
        
        responses_df = load_csv_with_cache('responses.csv')
        user_responses = responses_df[
            responses_df['result_id'] == result_id
        ].sort_values('question_id')
        
        questions_df = load_csv_with_cache('questions.csv')
        
        # Create PDF
        buffer = BytesIO()
        doc = SimpleDocTemplate(buffer, pagesize=letter, rightMargin=50, leftMargin=50, topMargin=50, bottomMargin=50)
        
        # Styles
        styles = getSampleStyleSheet()
        title_style = ParagraphStyle('CustomTitle', parent=styles['Title'], fontSize=18, textColor=colors.HexColor('#2c3e50'), spaceAfter=20, alignment=TA_CENTER)
        heading_style = ParagraphStyle('CustomHeading', parent=styles['Heading2'], fontSize=14, textColor=colors.HexColor('#2c3e50'), spaceAfter=10)
        
        story = []
        
        # Title
        story.append(Paragraph("Exam Response Analysis", title_style))
        
        # Header info
        header_data = [
            ['Exam:', str(exam['name'])],
            ['Student:', str(full_name)],
            ['Score:', f"{result['score']}/{result['max_score']} ({result['percentage']:.1f}%)"],
            ['Grade:', str(result['grade'])]
        ]
        
        header_table = Table(header_data, colWidths=[1.5*inch, 4*inch])
        header_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (0, -1), colors.lightgrey),
            ('FONTNAME', (0, 0), (-1, -1), 'Helvetica'),
            ('FONTSIZE', (0, 0), (-1, -1), 12),
            ('PADDING', (0, 0), (-1, -1), 8),
            ('GRID', (0, 0), (-1, -1), 1, colors.black)
        ]))
        
        story.append(header_table)
        story.append(Spacer(1, 20))
        
        # Questions and responses
        for idx, response in user_responses.iterrows():
            question_id = response['question_id']
            question_row = questions_df[questions_df['id'] == question_id]
            
            if question_row.empty:
                continue
                
            question = question_row.iloc[0]
            
            # Question header
            story.append(Paragraph(f"Question {question_id}", heading_style))
            
            # Question text - ReportLab handles Unicode automatically
            question_text = str(question.get('question_text', ''))
            story.append(Paragraph(f"<b>Question:</b> {question_text}", styles['Normal']))
            story.append(Spacer(1, 10))
            
            # Options for MCQ/MSQ
            question_type = question.get('question_type', '')
            if question_type in ['MCQ', 'MSQ']:
                story.append(Paragraph("<b>Options:</b>", styles['Normal']))
                
                options = [
                    ('A', question.get('option_a', '')),
                    ('B', question.get('option_b', '')),
                    ('C', question.get('option_c', '')),
                    ('D', question.get('option_d', ''))
                ]
                
                for label, option_text in options:
                    if option_text and str(option_text).strip() and str(option_text) != 'nan':
                        story.append(Paragraph(f"<b>{label}.</b> {option_text}", styles['Normal']))
                
                story.append(Spacer(1, 10))
            
            # Answers
            given_answer = str(response.get('given_answer', 'Not Answered'))
            if given_answer in ['nan', 'None', '']:
                given_answer = 'Not Answered'
                
            correct_answer = str(response.get('correct_answer', 'N/A'))
            if correct_answer in ['nan', 'None', '']:
                correct_answer = 'N/A'
            
            marks = response.get('marks_obtained', 0)
            is_correct = response.get('is_correct', False)
            
            answer_data = [
                ['Your Answer:', given_answer],
                ['Correct Answer:', correct_answer],
                ['Marks Obtained:', str(marks)],
                ['Status:', 'Correct' if is_correct else 'Incorrect' if given_answer != 'Not Answered' else 'Not Attempted']
            ]
            
            answer_table = Table(answer_data, colWidths=[1.5*inch, 4*inch])
            answer_table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (0, -1), colors.lightblue),
                ('FONTNAME', (0, 0), (-1, -1), 'Helvetica'),
                ('FONTSIZE', (0, 0), (-1, -1), 10),
                ('PADDING', (0, 0), (-1, -1), 6),
                ('GRID', (0, 0), (-1, -1), 1, colors.black)
            ]))
            
            story.append(answer_table)
            story.append(Spacer(1, 20))
        
        # Summary
        story.append(Paragraph("Performance Summary", heading_style))
        
        summary_data = [
            ['Total Questions:', str(result['total_questions'])],
            ['Correct Answers:', str(result['correct_answers'])],
            ['Incorrect Answers:', str(result['incorrect_answers'])],
            ['Unanswered:', str(result['unanswered_questions'])],
            ['Final Score:', f"{result['score']}/{result['max_score']}"],
            ['Percentage:', f"{result['percentage']:.1f}%"]
        ]
        
        summary_table = Table(summary_data, colWidths=[2*inch, 2*inch])
        summary_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, -1), colors.lightgreen),
            ('FONTNAME', (0, 0), (-1, -1), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, -1), 12),
            ('PADDING', (0, 0), (-1, -1), 8),
            ('GRID', (0, 0), (-1, -1), 1, colors.black)
        ]))
        
        story.append(summary_table)
        
        # Build PDF
        doc.build(story)
        
        pdf_bytes = buffer.getvalue()
        buffer.close()
        
        return Response(
            pdf_bytes,
            mimetype='application/pdf',
            headers={'Content-Disposition': f'attachment; filename=exam_{exam_id}_response_{username}.pdf'}
        )
        
    except Exception as e:
        print(f"PDF generation error: {e}")
        import traceback
        traceback.print_exc()
        flash('Error generating PDF.', 'error')
        return redirect(url_for('response_page', exam_id=exam_id))




@app.route('/response-txt/<int:exam_id>')
@require_user_role
def response_txt(exam_id):
    """Text file export as fallback"""
    try:
        user_id = session.get('user_id')
        username = session.get('username', 'Student')
        
        # Create simple text content
        content = f"""Exam Response Summary
Exam ID: {exam_id}
Student: {username}
Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

---

For detailed analysis including questions and answers,
please view the online response page in your browser.

This is a basic completion record for your exam attempt.
"""
        
        return Response(
            content,
            mimetype='text/plain',
            headers={
                'Content-Disposition': f'attachment; filename="exam_{exam_id}_summary.txt"'
            }
        )
        
    except Exception as e:
        print(f"Text export error: {e}")
        flash('Export failed. Please contact support.', 'error')
        return redirect(url_for('response_page', exam_id=exam_id))


@app.route('/logout')
def logout():
    """Enhanced user logout"""
    uid = session.get("user_id")
    tok = session.get("token")
    
    # Invalidate server-side session
    if uid and tok:
        try:
            set_exam_active(uid, tok, is_active=False)
            invalidate_session(uid, token=tok)
        except Exception as e:
            print(f"[user_logout] Error invalidating session: {e}")
    
    # Completely clear Flask session
    session.clear()
    
    flash("Logout successful.", "success")
    return redirect(url_for("home"))



# -------------------------
# CRITICAL: Add service check endpoint for debugging
# -------------------------
@app.route('/debug/service-status')
def debug_service_status():
    """Debug endpoint to check service status"""
    global drive_service
    
    status = {
        'drive_service_initialized': drive_service is not None,
        'environment_variables': {},
        'file_ids': DRIVE_FILE_IDS.copy(),
        'folder_ids': DRIVE_FOLDER_IDS.copy()
    }
    
    # Check environment variables (don't expose full values)
    for var in ['SECRET_KEY', 'GOOGLE_SERVICE_ACCOUNT_JSON', 'USERS_FILE_ID']:
        value = os.environ.get(var)
        if value:
            if var == 'GOOGLE_SERVICE_ACCOUNT_JSON':
                status['environment_variables'][var] = f"Present ({len(value)} chars)"
            else:
                status['environment_variables'][var] = "Present"
        else:
            status['environment_variables'][var] = "MISSING"
    
    # Test drive service if available
    if drive_service:
        try:
            about = drive_service.about().get(fields="user").execute()
            status['drive_test'] = f"Connected as: {about.get('user', {}).get('emailAddress', 'Unknown')}"
        except Exception as e:
            status['drive_test'] = f"Error: {str(e)}"
    else:
        status['drive_test'] = "Service not initialized"
    
    return jsonify(status)






# -------------------------
# Error Handlers
# -------------------------
@app.errorhandler(404)
def not_found_error(error):
    try:
        return render_template('error.html', error_code=404, error_message="Page not found"), 404
    except:
        return "404 - Page not found", 404


@app.errorhandler(500)
def internal_error(error):
    try:
        return render_template('error.html', error_code=500, error_message="Internal server error"), 500
    except:
        return "500 - Internal server error", 500




@app.errorhandler(Exception)
def handle_global_error(e):
    """Enhanced global error handler with debugging"""
    print(f"GLOBAL ERROR HANDLER caught: {e}")
    print(f"Request path: {request.path}")
    print(f"Request method: {request.method}")
    print(f"Form data keys: {list(request.form.keys()) if request.form else 'No form data'}")
    
    import traceback
    traceback.print_exc()
    
    # Log the error details
    error_info = {
        'error': str(e),
        'type': type(e).__name__,
        'route': request.endpoint,
        'method': request.method,
        'url': request.url,
        'user_id': session.get('user_id', 'anonymous'),
        'timestamp': datetime.now().isoformat()
    }
    
    try:
        # Log to file if possible
        with open('error_log.txt', 'a') as f:
            f.write(f"{datetime.now()}: {error_info}\n")
    except:
        pass
    
    # Don't flash errors for AJAX requests
    if request.is_json or '/api/' in request.path:
        return {"error": "Server error occurred"}, 500
    
    flash("A system error occurred. Please try again or contact support.", "error")
    
    # Redirect based on context
    if '/admin/' in request.path:
        return redirect(url_for('admin.admin_login'))
    else:
        return redirect(url_for('login'))
















@app.route('/request-admin-access')
def request_admin_access_page():
    """Request admin access page for users"""
    return render_template('request_admin_access.html')

@app.route('/api/validate-user-for-request', methods=['POST'])
def api_validate_user_for_request():
    try:
        data = request.get_json()
        if not data or not data.get('username') or not data.get('email'):
            return jsonify({'success': False, 'message': 'Username and email are required'}), 400

        username = data['username'].strip()
        email = data['email'].strip().lower()

        users_df = load_csv_with_cache('users.csv', force_reload=True)
        if users_df.empty:
            return jsonify({'success': False, 'message': 'User database is unavailable'}), 500

        users_df['username_lower'] = users_df['username'].astype(str).str.strip().str.lower()
        users_df['email_lower'] = users_df['email'].astype(str).str.strip().str.lower()

        user_row = users_df[
            (users_df['username_lower'] == username.lower()) &
            (users_df['email_lower'] == email.lower())
        ]

        if user_row.empty:
            return jsonify({'success': False, 'message': 'User does not exist with provided username and email combination'}), 404

        user = user_row.iloc[0]
        current_access = str(user.get('role', 'user')).strip().lower()
        
        
        init_requests_raised_if_needed()
        requests_df = load_csv_with_cache('requests_raised.csv', force_reload=True)
        if requests_df is None:
            requests_df = pd.DataFrame(columns=[
                'request_id', 'username', 'email', 'current_access',
                'requested_access', 'request_date', 'request_status', 'reason'
            ])

        user_requests = []
        if not requests_df.empty:
            user_requests_df = requests_df[
                (requests_df['username'].astype(str).str.strip().str.lower() == username.lower()) &
                (requests_df['email'].astype(str).str.strip().str.lower() == email.lower())
            ]

            for _, req in user_requests_df.iterrows():
                reason_val = req.get('reason', None)
                try:
                    if pd.isna(reason_val):
                        reason_safe = None
                    else:
                        reason_safe = reason_val if reason_val != '' else None
                except Exception:
                    try:
                        if isinstance(reason_val, float) and math.isnan(reason_val):
                            reason_safe = None
                        else:
                            reason_safe = reason_val if reason_val != '' else None
                    except Exception:
                        reason_safe = None

                try:
                    req_id = int(req['request_id'])
                except Exception:
                    try:
                        req_id = int(pd.to_numeric(req['request_id'], errors='coerce'))
                    except Exception:
                        req_id = None

                user_requests.append({
                    'request_id': req_id,
                    'requested_access': req.get('requested_access', ''),
                    'request_date': str(req.get('request_date', '')),
                    'status': req.get('request_status', ''),
                    'reason': reason_safe
                })

        available_requests = []
        if current_access == 'user':
            available_requests = ['admin', 'user,admin']
        elif current_access == 'admin':
            available_requests = ['user', 'user,admin']
        elif current_access in ['user,admin', 'admin,user']:
            available_requests = []
        else:
            available_requests = ['admin', 'user,admin']

        has_pending = any((str(req.get('status', '')).lower() == 'pending') for req in user_requests)

        response_payload = {
            'success': True,
            'user': {
                'username': str(user['username']),
                'email': str(user['email']),
                'current_access': current_access,
                'full_name': str(user.get('full_name', user['username']))
            },
            'requests': user_requests,
            'available_requests': available_requests,
            'has_pending_request': has_pending,
            'can_request': len(available_requests) > 0 and not has_pending
        }

        return jsonify(response_payload)
    except Exception as e:
        print(f"Error validating user for request: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({'success': False, 'message': 'System error occurred'}), 500




@app.route('/api/submit-access-request', methods=['POST'])
def api_submit_access_request():
    try:
        data = request.get_json()
        required_fields = ['username', 'email', 'current_access', 'requested_access']

        for field in required_fields:
            if not data or not data.get(field):
                return jsonify({'success': False, 'message': f'{field.replace("_", " ").title()} is required'}), 400

        username = data['username'].strip()
        email = data['email'].strip().lower()
        current_access = data['current_access'].strip().lower()
        requested_access = data['requested_access'].strip().lower()

        users_df = load_csv_with_cache('users.csv')
        if users_df.empty:
            return jsonify({'success': False, 'message': 'User database unavailable'}), 500

        users_df['username_lower'] = users_df['username'].astype(str).str.strip().str.lower()
        users_df['email_lower'] = users_df['email'].astype(str).str.strip().str.lower()

        user_exists = not users_df[
            (users_df['username_lower'] == username.lower()) &
            (users_df['email_lower'] == email.lower())
        ].empty

        if not user_exists:
            return jsonify({'success': False, 'message': 'User validation failed'}), 400

        try:
            init_requests_raised_if_needed()
            requests_df = load_csv_with_cache('requests_raised.csv')
            if requests_df is None or requests_df.empty:
                requests_df = pd.DataFrame(columns=[
                    'request_id', 'username', 'email', 'current_access',
                    'requested_access', 'request_date', 'request_status', 'reason'
                ])
        except Exception as e:
            print(f"Error loading requests_raised.csv: {e}")
            requests_df = pd.DataFrame(columns=[
                'request_id', 'username', 'email', 'current_access',
                'requested_access', 'request_date', 'request_status', 'reason'
            ])

        if not requests_df.empty:
            pending_requests = requests_df[
                (requests_df['username'].astype(str).str.strip().str.lower() == username.lower()) &
                (requests_df['email'].astype(str).str.strip().str.lower() == email.lower()) &
                (requests_df['request_status'].astype(str).str.lower() == 'pending')
            ]

            if not pending_requests.empty:
                return jsonify({'success': False, 'message': 'You already have a pending request. Please wait for admin approval.'}), 400

        try:
            if requests_df.empty or 'request_id' not in requests_df.columns:
                next_id = 1
            else:
                numeric_ids = pd.to_numeric(requests_df['request_id'], errors='coerce')
                valid_ids = numeric_ids.dropna()
                next_id = int(valid_ids.max()) + 1 if not valid_ids.empty else 1
        except Exception:
            next_id = 1

        new_request = {
            'request_id': next_id,
            'username': username,
            'email': email,
            'current_access': current_access,
            'requested_access': requested_access,
            'request_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'request_status': 'pending',
            'reason': ''
        }

        new_df = pd.concat([requests_df, pd.DataFrame([new_request])], ignore_index=True)
        success = safe_csv_save_with_retry(new_df, 'requests_raised')

        if not success:
            return jsonify({'success': False, 'message': 'Failed to save request. Please try again.'}), 500

        requests_df = load_csv_with_cache('requests_raised.csv', force_reload=True)
        if requests_df is None:
            requests_df = pd.DataFrame(columns=[
                'request_id', 'username', 'email', 'current_access',
                'requested_access', 'request_date', 'request_status', 'reason'
            ])

        user_requests = []
        if not requests_df.empty:
            user_requests_df = requests_df[
                (requests_df['username'].astype(str).str.strip().str.lower() == username.lower()) &
                (requests_df['email'].astype(str).str.strip().str.lower() == email.lower())
            ]

            for _, req in user_requests_df.iterrows():
                reason_val = req.get('reason', None)
                try:
                    if pd.isna(reason_val):
                        reason_safe = None
                    else:
                        reason_safe = reason_val if reason_val != '' else None
                except Exception:
                    try:
                        if isinstance(reason_val, float) and math.isnan(reason_val):
                            reason_safe = None
                        else:
                            reason_safe = reason_val if reason_val != '' else None
                    except Exception:
                        reason_safe = None

                try:
                    req_id = int(req['request_id'])
                except Exception:
                    try:
                        req_id = int(pd.to_numeric(req['request_id'], errors='coerce'))
                    except Exception:
                        req_id = None

                user_requests.append({
                    'request_id': req_id,
                    'requested_access': req.get('requested_access', ''),
                    'request_date': str(req.get('request_date', '')),
                    'status': req.get('request_status', ''),
                    'reason': reason_safe
                })

        current_access = current_access
        available_requests = []
        if current_access == 'user':
            available_requests = ['admin', 'user,admin']
        elif current_access == 'admin':
            available_requests = ['user', 'user,admin']
        elif current_access in ['user,admin', 'admin,user']:
            available_requests = []
        else:
            available_requests = ['admin', 'user,admin']

        has_pending = any((str(req.get('status', '')).lower() == 'pending') for req in user_requests)

        response_payload = {
            'success': True,
            'message': 'Access request submitted successfully. Please wait for admin approval.',
            'request_id': next_id,
            'user': {
                'username': username,
                'email': email,
                'current_access': current_access,
                'full_name': username
            },
            'requests': user_requests,
            'available_requests': available_requests,
            'has_pending_request': has_pending,
            'can_request': len(available_requests) > 0 and not has_pending
        }

        return jsonify(response_payload)
    except Exception as e:
        print(f"Error submitting access request: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({'success': False, 'message': 'System error occurred'}), 500


# Helper function to initialize requests_raised.csv if it doesn't exist
def ensure_requests_raised_csv_safe():
    """Safe version that doesn't use Flask session functions"""
    try:
        # Only check if file exists, don't try to load it
        print("Checking requests_raised.csv file...")
        return True  # Just return success for now
    except Exception as e:
        print(f"Error checking requests_raised.csv: {e}")
        return False

def init_requests_raised_if_needed():
    """Initialize requests_raised.csv when actually needed (within request context)"""
    try:
        requests_df = load_csv_with_cache('requests_raised.csv')
        if requests_df is None or requests_df.empty:
            headers_df = pd.DataFrame(columns=[
                'request_id', 'username', 'email', 'current_access',
                'requested_access', 'request_date', 'request_status', 'reason'
            ])
            success = safe_csv_save_with_retry(headers_df, 'requests_raised')
            if success:
                print("✅ Created requests_raised.csv with headers")
            return success
        return True
    except Exception as e:
        print(f"Error initializing requests_raised.csv: {e}")
        return False


def cleanup_app_cache():
    """Periodic cache cleanup"""
    try:
        current_time = time.time()
        cache_data = app_cache.get('data', {})
        cache_timestamps = app_cache.get('timestamps', {})
        
        # Remove items older than 10 minutes
        for key in list(cache_data.keys()):
            if current_time - cache_timestamps.get(key, 0) > 600:
                cache_data.pop(key, None)
                cache_timestamps.pop(key, None)
        
        # Limit total cache items
        if len(cache_data) > 50:
            # Keep only the 30 most recent items
            sorted_items = sorted(cache_timestamps.items(), key=lambda x: x[1], reverse=True)
            keep_keys = [key for key, _ in sorted_items[:30]]
            
            app_cache['data'] = {k: v for k, v in cache_data.items() if k in keep_keys}
            app_cache['timestamps'] = {k: v for k, v in cache_timestamps.items() if k in keep_keys}
    
    except Exception as e:
        print(f"Cache cleanup error: {e}")

# Run cleanup every 5 minutes
import threading
def periodic_cleanup():
    cleanup_app_cache()
    threading.Timer(300, periodic_cleanup).start()

periodic_cleanup()

@app.route('/_ping', methods=['POST'])
def ping():
    """Keep session alive"""
    if 'user_id' in session:
        return '', 204  # No content, session is alive
    return jsonify({'reason': 'no_session'}), 401

# -------------------------
# Run App - CRITICAL INITIALIZATION
# -------------------------
if __name__ == '__main__':
    print("🚀 Starting FIXED Exam Portal...")
    
    # CRITICAL: Force initialization during startup
    print("🔧 Forcing Google Drive service initialization...")
    if init_drive_service():
        print("✅ Google Drive integration: ACTIVE")
    else:
        print("❌ Google Drive integration: INACTIVE")
        print("⚠️ App will run in limited mode")

    app.run(debug=True if not IS_PRODUCTION else False)
else:
    # CRITICAL: This runs when deployed with Gunicorn
    print("🌐 Gunicorn detected - initializing services for production...")
    
    # Force immediate initialization
    if init_drive_service():
        print("✅ Production Google Drive integration: ACTIVE")
    else:
        print("❌ Production Google Drive integration: FAILED")

        print("📋 Check environment variables and credentials")
