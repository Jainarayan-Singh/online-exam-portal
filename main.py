# main.py - FIXED VERSION with explicit Google Drive initialization
from flask import Flask, render_template, request, redirect, url_for, session, flash, jsonify
import pandas as pd
import os
from datetime import datetime
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

DRIVE_FILE_IDS = {
    'users': USERS_FILE_ID,
    'exams': EXAMS_FILE_ID,
    'questions': QUESTIONS_FILE_ID,
    'results': RESULTS_FILE_ID,
    'responses': RESPONSES_FILE_ID
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

def clear_user_cache():
    """Clear both global app cache and per-user session cache"""
    global app_cache
    from flask import session

    # Clear global in-memory cache
    app_cache['data'].clear()
    app_cache['timestamps'].clear()
    app_cache['images'].clear()
    app_cache['force_refresh'] = True
    print("🗑️ Cleared global app_cache")

    # Clear Flask session cache keys (exam + csv data)
    keys_to_clear = [k for k in list(session.keys()) if k.startswith("csv_") or k.startswith("exam_data_")]
    for k in keys_to_clear:
        session.pop(k, None)
    print(f"🗑️ Cleared {len(keys_to_clear)} cached session keys")


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


def load_csv_with_cache(filename, force_reload=False):
    """Load CSV with smart caching - supports global + per-user force refresh"""
    global app_cache

    cache_key = f'csv_{filename}'
    cache_duration = 300  # 5 minutes

    # 🔥 Force refresh logic (global or session)
    if app_cache.get('force_refresh', False) or session.get('force_refresh', False):
        print(f"♻️ Force refresh enabled for {filename}, skipping cache")
        force_reload = True
        app_cache['force_refresh'] = False   # reset global
        session.pop('force_refresh', None)   # reset per-user

    # Normal cache check
    if not force_reload and cache_key in app_cache['data']:
        cached_time = app_cache['timestamps'].get(cache_key, 0)
        if time.time() - cached_time < cache_duration:
            print(f"📋 Using cached data for {filename}")
            return app_cache['data'][cache_key].copy()

    # Load fresh from Google Drive
    print(f"📥 Loading fresh data for {filename}...")
    df = load_csv_from_drive_direct(filename)

    if not df.empty:
        app_cache['data'][cache_key] = df.copy()
        app_cache['timestamps'][cache_key] = time.time()
        print(f"💾 Cached {len(df)} records for {filename}")

    return df



def load_csv_from_drive_direct(filename):
    """Direct CSV loading from Google Drive with better error handling"""
    global drive_service

    if drive_service is None:
        print("No Google Drive service available")
        return pd.DataFrame()

    file_id_key = filename.replace('.csv', '')
    file_id = DRIVE_FILE_IDS.get(file_id_key)

    if file_id and not file_id.startswith('YOUR_'):
        try:
            df = load_csv_from_drive(drive_service, file_id)
            if not df.empty:
                # Clean column names
                df.columns = df.columns.str.strip()
                print(f"Successfully loaded {len(df)} rows from {filename}")
                return df
            else:
                print(f"Empty DataFrame for {filename}")
        except Exception as e:
            print(f"Error loading {filename} from Drive: {e}")

    return pd.DataFrame()


def process_question_image_fixed(question):
    """Process image path using subjects.csv and return public URL"""
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

        # --- 🔎 Find subject folder dynamically from subjects.csv ---
        folder_id = None
        subjects_file_id = os.environ.get("SUBJECTS_FILE_ID")
        if subjects_file_id:
            try:
                subjects_df = load_csv_from_drive(drive_service, subjects_file_id)
                if not subjects_df.empty:
                    subjects_df["subject_name"] = subjects_df["subject_name"].astype(str).str.strip().str.lower()
                    match = subjects_df[subjects_df["subject_name"] == subject.strip().lower()]
                    if not match.empty:
                        folder_id = str(match.iloc[0]["subject_folder_id"])
                        print(f"📂 Found folder for subject '{subject}': {folder_id}")
                    else:
                        print(f"⚠️ No match for subject '{subject}' in subjects.csv")
            except Exception as e:
                print(f"⚠️ Error reading subjects.csv: {e}")

        # Fallback to IMAGES_FOLDER_ID if subject folder not found
        if not folder_id and os.environ.get("IMAGES_FOLDER_ID"):
            folder_id = os.environ.get("IMAGES_FOLDER_ID")
            print(f"📂 Fallback to IMAGES folder for subject {subject}: {folder_id}")

        if not folder_id:
            print(f"❌ No folder ID found for subject: {subject}")
            return False, None

        # --- 🔎 Find file inside resolved folder ---
        image_file_id = find_file_by_name(drive_service, filename, folder_id)
        if image_file_id:
            image_url = get_public_url(drive_service, image_file_id)
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


def preload_exam_data_fixed(exam_id):
    """COMPLETELY FIXED exam data preloading with comprehensive error handling"""
    start_time = time.time()
    print(f"Preloading exam data for exam_id: {exam_id}")

    try:
        # Load all required data
        questions_df = load_csv_with_cache('questions.csv')
        exams_df = load_csv_with_cache('exams.csv')

        if questions_df.empty:
            print("Questions DataFrame is empty")
            return False, "Failed to load questions data"

        if exams_df.empty:
            print("Exams DataFrame is empty")
            return False, "Failed to load exam data"

        # Convert exam_id to ensure consistency
        exam_id_str = str(exam_id)

        # Filter questions for this exam
        print(f"Filtering questions for exam_id: {exam_id_str}")
        exam_questions = questions_df[questions_df['exam_id'].astype(str) == exam_id_str]

        if exam_questions.empty:
            print(f"No questions found for exam_id: {exam_id_str}")
            print(f"Available exam_ids in questions: {questions_df['exam_id'].unique().tolist()}")
            return False, f"No questions found for exam ID {exam_id}"

        # Get exam info
        exam_info = exams_df[exams_df['id'].astype(str) == exam_id_str]
        if exam_info.empty:
            print(f"Exam not found with id: {exam_id_str}")
            return False, "Exam not found"

        # Process and cache questions with images
        processed_questions = []
        image_urls = {}
        failed_images = []

        for _, question in exam_questions.iterrows():
            question_dict = question.to_dict()

            # Process image if exists
            image_path = question_dict.get('image_path')
            if image_path and str(image_path).strip() not in ['', 'nan', 'NaN', 'null', 'None']:
                has_image, image_url = process_question_image_fixed(question_dict)
                question_dict['has_image'] = has_image
                question_dict['image_url'] = image_url

                if has_image and image_url:
                    image_urls[str(question_dict['id'])] = image_url
                    print(f"Image processed for Q{question_dict['id']}: {image_path}")
                else:
                    failed_images.append(image_path)
                    print(f"Failed to load image for Q{question_dict['id']}: {image_path}")
            else:
                question_dict['has_image'] = False
                question_dict['image_url'] = None

            # Parse correct answers
            question_dict['parsed_correct_answer'] = parse_correct_answers(
                question_dict.get('correct_answer'),
                question_dict.get('question_type', 'MCQ')
            )

            processed_questions.append(question_dict)

        # Store in session with more comprehensive data
        cache_key = f'exam_data_{exam_id}'
        session[cache_key] = {
            'exam_info': exam_info.iloc[0].to_dict(),
            'questions': processed_questions,
            'image_urls': image_urls,
            'failed_images': failed_images,
            'total_questions': len(processed_questions),
            'loaded_at': datetime.now().isoformat(),
            'exam_id': exam_id
        }
        session.permanent = True

        load_time = time.time() - start_time
        print(
            f"Preloaded exam data in {load_time:.2f}s: {len(processed_questions)} questions, {len(image_urls)} images loaded, {len(failed_images)} images failed")

        return True, f"Successfully loaded {len(processed_questions)} questions"

    except Exception as e:
        print(f"Critical error preloading exam data: {e}")
        import traceback
        traceback.print_exc()
        return False, f"Critical error: {str(e)}"


def get_cached_exam_data(exam_id):
    """Get cached exam data with validation"""
    cache_key = f'exam_data_{exam_id}'
    cached_data = session.get(cache_key)

    if cached_data:
        # Validate cached data structure
        required_keys = ['exam_info', 'questions', 'total_questions']
        if all(key in cached_data for key in required_keys):
            return cached_data
        else:
            print(f"Invalid cached data structure for exam {exam_id}")
            session.pop(cache_key, None)

    return None


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
    """Calculate score for a question"""
    if is_correct:
        return float(positive_marks)
    else:
        return -float(negative_marks) if negative_marks else 0.0


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
    """Landing page route"""
    return render_template('index.html')

@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        identifier = request.form["username"].strip().lower()
        password = request.form["password"].strip()

        users_df = load_csv_with_cache("users.csv")

        if users_df.empty:
            flash("No users available!", "error")
            return redirect(url_for("login"))

        # Normalize case for matching
        users_df["username_lower"] = users_df["username"].astype(str).str.strip().str.lower()
        users_df["email_lower"] = users_df["email"].astype(str).str.strip().str.lower()
        users_df["role_lower"] = users_df["role"].astype(str).str.strip().str.lower()

        # Find matching row (username OR email)
        user_row = users_df[
            (users_df["username_lower"] == identifier) |
            (users_df["email_lower"] == identifier)
        ]

        if user_row.empty:
            flash("Invalid username/email or password!", "error")
            return redirect(url_for("login"))

        user = user_row.iloc[0]

        # Password check
        if str(user["password"]) != password:
            flash("Invalid username/email or password!", "error")
            return redirect(url_for("login"))

        role = str(user["role_lower"])

        # --- LOGIN HANDLING ---
        if "admin" in role and "user" in role:
            # Both roles (user+admin) → check if coming from ?role=admin
            if request.args.get("role") == "admin":
                session["admin_id"] = int(user["id"])
                session["admin_name"] = user["username"]
                flash("Admin login successful!", "success")
                return redirect(url_for("dashboard"))
            else:
                session["user_id"] = int(user["id"])
                session["username"] = user["username"]
                session["full_name"] = user.get("full_name", user["username"])
                flash("Login successful!", "success")
                return redirect(url_for("dashboard"))

        elif "admin" in role:
            session["admin_id"] = int(user["id"])
            session["admin_name"] = user["username"]
            flash("Admin login successful!", "success")
            return redirect(url_for("admin.dashboard"))

        elif "user" in role:
            session["user_id"] = int(user["id"])
            session["username"] = user["username"]
            session["full_name"] = user.get("full_name", user["username"])
            flash("Login successful!", "success")
            return redirect(url_for("dashboard"))

        else:
            flash("Invalid role for this user!", "error")
            return redirect(url_for("login"))

    return render_template("login.html")




# 1. REPLACE your EMAIL_CONFIG with this (line 1-8 replace karo):

EMAIL_CONFIG = {
    'SMTP_SERVER': 'smtp.gmail.com',
    'SMTP_PORT': 587,
    'EMAIL_ADDRESS': os.environ.get('EMAIL_ADDRESS'),
    'EMAIL_PASSWORD': os.environ.get('EMAIL_PASSWORD'),
    'FROM_NAME': 'ExamPortal System'
}

# 2. REPLACE your send_credentials_email function completely (around line 50-90):

def send_credentials_email(email, full_name, username, password):
    """Send welcome email with credentials"""
    try:
        msg = MIMEMultipart('alternative')
        msg['Subject'] = 'Welcome to ExamPortal - Your Account Credentials'
        msg['From'] = f"{EMAIL_CONFIG['FROM_NAME']} <{EMAIL_CONFIG['EMAIL_ADDRESS']}>"
        msg['To'] = email

        html_content = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <style>
                body {{
                    font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
                    line-height: 1.6;
                    color: #333;
                    max-width: 600px;
                    margin: 0 auto;
                    padding: 20px;
                }}
                .header {{
                    background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                    color: white;
                    padding: 30px;
                    text-align: center;
                    border-radius: 10px 10px 0 0;
                }}
                .content {{
                    background: #f8f9fa;
                    padding: 30px;
                    border-radius: 0 0 10px 10px;
                    border: 1px solid #e9ecef;
                }}
                .credentials-box {{
                    background: white;
                    padding: 20px;
                    border-radius: 8px;
                    border-left: 4px solid #28a745;
                    margin: 20px 0;
                    box-shadow: 0 2px 4px rgba(0,0,0,0.1);
                }}
                .credential-item {{
                    margin: 10px 0;
                    padding: 8px;
                    background: #f8f9fa;
                    border-radius: 4px;
                }}
                .credential-label {{
                    font-weight: bold;
                    color: #495057;
                }}
                .credential-value {{
                    font-family: 'Courier New', monospace;
                    color: #007bff;
                    font-size: 16px;
                }}
            </style>
        </head>
        <body>
            <div class="header">
                <h1>🎓 Welcome to ExamPortal!</h1>
                <p>Your account has been successfully created</p>
            </div>

            <div class="content">
                <p>Dear <strong>{full_name}</strong>,</p>

                <p>Welcome to ExamPortal! Your account has been created successfully.</p>

                <div class="credentials-box">
                    <h3>🔐 Your Login Credentials</h3>
                    <div class="credential-item">
                        <span class="credential-label">Username:</span>
                        <div class="credential-value">{username}</div>
                    </div>
                    <div class="credential-item">
                        <span class="credential-label">Password:</span>
                        <div class="credential-value">{password}</div>
                    </div>
                    <div class="credential-item">
                        <span class="credential-label">Email:</span>
                        <div class="credential-value">{email}</div>
                    </div>
                </div>

                <p>Please keep these credentials secure and change your password after first login.</p>

                <div style="text-align: center; margin-top: 30px; color: #6c757d; font-size: 14px;">
                    <p><strong>ExamPortal Team</strong></p>
                    <p>Generated on {datetime.now().strftime('%B %d, %Y at %I:%M %p')}</p>
                </div>
            </div>
        </body>
        </html>
        """

        text_content = f"""
        Welcome to ExamPortal!

        Dear {full_name},

        Your account has been created successfully. Here are your login credentials:

        Username: {username}
        Password: {password}
        Email: {email}

        Please keep these credentials secure don't share with anyone.

        Best regards,
        ExamPortal Team
        """

        text_part = MIMEText(text_content, 'plain')
        html_part = MIMEText(html_content, 'html')

        msg.attach(text_part)
        msg.attach(html_part)

        with smtplib.SMTP(EMAIL_CONFIG['SMTP_SERVER'], EMAIL_CONFIG['SMTP_PORT']) as server:
            server.starttls()
            server.login(EMAIL_CONFIG['EMAIL_ADDRESS'], EMAIL_CONFIG['EMAIL_PASSWORD'])
            server.send_message(msg)

        return True, "Email sent successfully"

    except Exception as e:
        print(f"Error sending email: {e}")
        return False, f"Failed to send email: {str(e)}"

# 3. REPLACE your generate_username function (around line 15-25):

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


# 6. Your forgot_password route looks good, but make sure it's exactly like this:
# (Replace the existing forgot_password route completely)

@app.route('/forgot_password', methods=['GET', 'POST'])
def forgot_password():
    """Enhanced user registration with concurrent safety and retry"""
    if request.method == 'POST':
        try:
            email = request.form['email'].strip().lower()
            full_name = request.form.get('full_name', '').strip()

            if not email:
                flash('Please enter your email address.', 'error')
                return render_template('forgot_password.html')

            if not full_name:
                flash('Please enter your full name.', 'error')
                return render_template('forgot_password.html', email=email)

            is_valid, error_message = verify_email_exists(email)
            if not is_valid:
                flash(f'Invalid email: {error_message}', 'error')
                return render_template('forgot_password.html', email=email, full_name=full_name)

            # Use safe registration with retry
            success, status, credentials = safe_user_register(email, full_name)
            
            if success or status == "exists":
                # Send credentials email
                email_sent, email_message = send_credentials_email(
                    email, credentials['full_name'], credentials['username'], credentials['password']
                )

                if email_sent:
                    msg = 'Account created successfully!' if success else 'Account already exists!'
                    flash(f'{msg} Your credentials have been sent to {email}', 'success')
                else:
                    msg = 'Account created!' if success else 'Account exists!'
                    flash(f'{msg} Here are your credentials:', 'success')
                    
                return render_template('forgot_password.html', success=True, email=email, credentials=credentials)
            else:
                flash(f'Registration failed: {status}. Please try again.', 'error')

        except Exception as e:
            print(f"Registration error: {e}")
            flash('System error occurred. Please try again.', 'error')

    return render_template('forgot_password.html')




@app.route('/dashboard')
@login_required
def dashboard():
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


from datetime import datetime

@app.route("/results_history", endpoint="results_history")
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
@login_required
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

    # Add default marking scheme if not present
    if 'positive_marks' not in exam_data or pd.isna(exam_data.get('positive_marks')):
        exam_data['positive_marks'] = 1
    if 'negative_marks' not in exam_data or pd.isna(exam_data.get('negative_marks')):
        exam_data['negative_marks'] = 0

    return render_template('exam_instructions.html', exam=exam_data)


@app.route('/preload-exam/<int:exam_id>')
@login_required
def preload_exam_route(exam_id):
    """API endpoint to preload exam data - FIXED"""
    try:
        # Check if already cached and valid
        cached_data = get_cached_exam_data(exam_id)
        if cached_data and cached_data.get('exam_id') == exam_id:
            return jsonify({
                'success': True,
                'message': f"Using cached data with {cached_data['total_questions']} questions",
                'exam_id': exam_id,
                'cached': True
            })

        success, message = preload_exam_data_fixed(exam_id)
        return jsonify({
            'success': success,
            'message': message,
            'exam_id': exam_id,
            'cached': False
        })
    except Exception as e:
        print(f"Error in preload route: {e}")
        return jsonify({
            'success': False,
            'message': f"Error preloading exam: {str(e)}",
            'exam_id': exam_id
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



@app.route('/exam/<int:exam_id>')
@login_required
def exam_page(exam_id):
    """COMPLETELY FIXED exam page with comprehensive error handling and sanitization"""
    print(f"Loading exam page for exam_id: {exam_id}")

    try:
        # Navigation / new attempt flags
        is_new_attempt = 'new_attempt' in request.args

        if is_new_attempt:
            session.pop('exam_answers', None)
            session.pop('marked_for_review', None)
            session.pop('exam_start_time', None)
            # optional: clear any exam cache key in session
            cache_key = f'exam_data_{exam_id}'
            session.pop(cache_key, None)
            print("Cleared session data for new attempt")

        # Attempt to get cached exam data (function must exist in your code)
        cached_data = get_cached_exam_data(exam_id)
        if not cached_data:
            print("No cached data found, preloading...")
            success, message = preload_exam_data_fixed(exam_id)
            if not success:
                print(f"Preload failed: {message}")
                flash(f"Error loading exam: {message}", "error")
                return redirect(url_for('dashboard'))
            cached_data = get_cached_exam_data(exam_id)

        if not cached_data:
            print("Failed to get cached data after preload")
            flash("Failed to load exam data! Please try again.", "error")
            return redirect(url_for('dashboard'))

        exam_data = cached_data.get('exam_info')
        questions = cached_data.get('questions') or []

        if not questions:
            print("No questions in cached data")
            flash("No questions found for this exam!", "error")
            return redirect(url_for('dashboard'))

        # Validate and clamp q index
        q_index = int(request.args.get('q', 0) or 0)
        if q_index < 0:
            q_index = 0
        if q_index >= len(questions):
            q_index = len(questions) - 1

        current_question = questions[q_index].copy() if isinstance(questions[q_index], dict) else dict(questions[q_index])

        # Initialize session structures
        session.setdefault('exam_answers', {})
        session.setdefault('marked_for_review', [])
        session.setdefault('exam_start_time', datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

        # sanitize question text + options so template can use |safe
        # only modify the display fields, do not change stored canonical fields
        current_question['question_text'] = sanitize_for_display(current_question.get('question_text', ''))
        current_question['option_a'] = sanitize_for_display(current_question.get('option_a', ''))
        current_question['option_b'] = sanitize_for_display(current_question.get('option_b', ''))
        current_question['option_c'] = sanitize_for_display(current_question.get('option_c', ''))
        current_question['option_d'] = sanitize_for_display(current_question.get('option_d', ''))

        # get selected answer for this question (stored as plain text in session)
        selected_answer = session['exam_answers'].get(str(current_question.get('id')), None)

        # Build palette statuses
        palette = {}
        for i, q in enumerate(questions):
            qid = str(q.get('id'))
            if qid in session['marked_for_review']:
                palette[i] = 'review'
            elif qid in session['exam_answers']:
                palette[i] = 'answered'
            else:
                palette[i] = 'not-visited'

        # mark this question visited if it was not visited
        if palette.get(q_index) == 'not-visited':
            palette[q_index] = 'visited'

        print(f"Successfully loaded exam page: Q{q_index + 1}/{len(questions)}")

        return render_template(
            'exam_page.html',
            exam=exam_data,
            question=current_question,
            current_index=q_index,
            selected_answer=selected_answer,
            total_questions=len(questions),
            palette=palette,
            questions=questions
        )

    except Exception as e:
        print(f"Critical error in exam_page: {e}")
        import traceback
        traceback.print_exc()
        flash(f"Critical error loading exam page: {str(e)}", "error")
        return redirect(url_for('dashboard'))



@app.route('/exam/<int:exam_id>/navigate', methods=['POST'])
@login_required
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
@login_required
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
@login_required
def submit_exam(exam_id):
    """Enhanced exam submission with concurrent safety and retry mechanism"""
    if request.method == 'GET':
        return render_template('submit_confirm.html', exam_id=exam_id)

    try:
        # Get cached exam data
        cached_data = get_cached_exam_data(exam_id)
        if not cached_data:
            print("No cached exam data for submission")
            flash("Exam session expired. Please contact administrator.", "error")
            return redirect(url_for('dashboard'))

        exam_data = cached_data['exam_info']
        questions = cached_data['questions']

        # Get marking scheme
        default_positive_marks = exam_data.get('positive_marks', 1) or 1
        default_negative_marks = exam_data.get('negative_marks', 0) or 0

        total_questions = len(questions)
        total_score = 0
        max_possible_score = 0

        # Track answer statistics
        correct_answers = 0
        incorrect_answers = 0
        unanswered_questions = 0

        # Load current data using safe functions
        results_df = safe_csv_load('results.csv')
        responses_df = safe_csv_load('responses.csv')

        # Generate next result ID
        next_result_id = 1
        if not results_df.empty and 'id' in results_df.columns and results_df['id'].notna().any():
            try:
                next_result_id = int(results_df['id'].fillna(0).astype(int).max()) + 1
            except Exception:
                next_result_id = len(results_df) + 1

        # Process each question and store responses
        response_records = []
        next_response_id = 1
        if not responses_df.empty and 'id' in responses_df.columns and responses_df['id'].notna().any():
            try:
                next_response_id = int(responses_df['id'].fillna(0).astype(int).max()) + 1
            except Exception:
                next_response_id = len(responses_df) + 1

        print(f"Processing {total_questions} questions for submission...")

        if total_questions > 0:
            for question in questions:
                qid = str(question['id'])
                question_type = question.get('question_type', 'MCQ')

                # Get marking scheme for this question
                q_positive_marks = question.get('positive_marks', default_positive_marks) or default_positive_marks
                q_negative_marks = question.get('negative_marks', default_negative_marks) or default_negative_marks

                max_possible_score += q_positive_marks

                # Get correct answer
                correct_answer = question.get('parsed_correct_answer')

                # Get given answer from session
                given_answer = session.get('exam_answers', {}).get(qid, None)

                # Check if question was attempted
                is_attempted = False
                if given_answer is not None:
                    if question_type == 'MSQ':
                        is_attempted = isinstance(given_answer, list) and len(given_answer) > 0
                    elif question_type == 'NUMERIC':
                        is_attempted = str(given_answer).strip() != ''
                    else:  # MCQ
                        is_attempted = str(given_answer).strip() not in ['', 'None', 'null']

                # Default values
                is_correct = False
                question_score = 0

                # Only evaluate if the student actually attempted
                if is_attempted:
                    tolerance = question.get('tolerance', 0.1) if question_type == 'NUMERIC' else None
                    is_correct = check_answer(given_answer, correct_answer, question_type, tolerance or 0.1)
                    question_score = calculate_question_score(is_correct, question_type, q_positive_marks, q_negative_marks)

                    if is_correct:
                        correct_answers += 1
                    else:
                        incorrect_answers += 1
                else:
                    unanswered_questions += 1

                total_score += question_score

                # Prepare answers for storage
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

                # Store response record
                response_record = {
                    'id': int(next_response_id),
                    'result_id': int(next_result_id),
                    'exam_id': int(exam_id),
                    'question_id': int(question['id']),
                    'given_answer': given_answer_str,
                    'correct_answer': correct_answer_str,
                    'is_correct': bool(is_correct),
                    'marks_obtained': float(question_score),
                    'question_type': str(question_type),
                    'is_attempted': bool(is_attempted)
                }
                response_records.append(response_record)
                next_response_id += 1

            percentage = (total_score / max_possible_score) * 100 if max_possible_score > 0 else 0.0
        else:
            percentage = 0.0

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

        # Calculate time taken
        start_time_str = session.get('exam_start_time')
        if start_time_str:
            try:
                start_time = datetime.strptime(start_time_str, '%Y-%m-%d %H:%M:%S')
                time_taken_seconds = (datetime.now() - start_time).total_seconds()
                time_taken_minutes = round(time_taken_seconds / 60, 2)
            except Exception:
                time_taken_minutes = None
        else:
            time_taken_minutes = None

        # Create new result record
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
            'time_taken_minutes': float(time_taken_minutes) if time_taken_minutes else None,
            'completed_at': str(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
        }

        print(f"Attempting to save results: Score {total_score}/{max_possible_score} ({percentage:.1f}%)")

        # Use atomic dual file save with retry mechanism
        save_success, save_message = safe_dual_file_save(results_df, responses_df, new_result, response_records)

        if save_success:
            session['latest_attempt_id'] = next_result_id
            print(f"Successfully saved exam results and {len(response_records)} responses")

            # Clear exam session data
            session.pop('exam_answers', None)
            session.pop('marked_for_review', None)
            session.pop('exam_start_time', None)

            # Clear exam cache
            cache_key = f'exam_data_{exam_id}'
            session.pop(cache_key, None)

            flash('Exam submitted successfully!', 'success')
            return redirect(url_for('result', exam_id=exam_id))
        else:
            print(f"Failed to save exam submission: {save_message}")
            flash(f'Error saving exam results: {save_message}. Please try again.', 'error')
            return redirect(url_for('exam_page', exam_id=exam_id))

    except Exception as e:
        print(f"Critical error in submit_exam: {e}")
        import traceback
        traceback.print_exc()
        flash(f'Critical error during submission: {str(e)}', 'error')
        return redirect(url_for('dashboard'))


@app.route('/result/<int:exam_id>', defaults={'result_id': None})
@app.route('/result/<int:exam_id>/<int:result_id>')
@login_required
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
@login_required
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
                has_image, image_url = process_question_image_fixed(q_dict)
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
@login_required
def response_pdf(exam_id):
    """Generate PDF for exam response analysis with optimized images"""
    try:
        # Get response data as in response_page
        results_df = load_csv_with_cache('results.csv')
        responses_df = load_csv_with_cache('responses.csv')
        exams_df = load_csv_with_cache('exams.csv')

        user_id = int(session['user_id'])
        user_results = results_df[
            (results_df['student_id'].astype('Int64') == user_id) &
            (results_df['exam_id'].astype('Int64') == int(exam_id))
        ].sort_values('id', ascending=False)

        if user_results.empty:
            flash('Response not found!', 'error')
            return redirect(url_for('dashboard'))

        result_record = user_results.head(1)
        result_id = int(result_record.iloc[0]['id'])
        result_data = result_record.iloc[0].to_dict()

        exam_record = exams_df[exams_df['id'].astype('Int64') == int(exam_id)]
        if exam_record.empty:
            flash('Exam not found!', 'error')
            return redirect(url_for('dashboard'))

        exam_data = exam_record.iloc[0].to_dict()

        if 'exam_id' in responses_df.columns:
            user_responses = responses_df[
                (responses_df['result_id'].astype('Int64') == result_id) &
                (responses_df['exam_id'].astype('Int64') == int(exam_id))
            ].sort_values('question_id')
        else:
            user_responses = responses_df[
                responses_df['result_id'].astype('Int64') == result_id
            ].sort_values('question_id')

        question_ids = set(user_responses['question_id'].astype(int).tolist())
        cached_data = get_cached_exam_data(exam_id)
        questions_dict = {}

        if cached_data:
            for q in cached_data['questions']:
                if int(q['id']) in question_ids:
                    questions_dict[int(q['id'])] = q
        else:
            questions_df = load_csv_with_cache('questions.csv')
            filtered_questions = questions_df[questions_df['id'].astype(int).isin(question_ids)]
            for _, q in filtered_questions.iterrows():
                q_dict = q.to_dict()
                has_image, image_url = process_question_image_fixed(q_dict)
                q_dict['has_image'] = has_image
                q_dict['image_url'] = image_url
                questions_dict[int(q['id'])] = q_dict

        # Prepare PDF
        pdf = FPDF()
        pdf.set_auto_page_break(auto=True, margin=15)
        pdf.add_page()
        pdf.set_font("Arial", size=12)

        pdf.cell(0, 10, f"Exam Response Analysis: {exam_data.get('title', 'Exam')}", ln=True)
        pdf.cell(0, 10, f"Student: {session.get('full_name', '')}", ln=True)
        pdf.cell(0, 10, f"Score: {result_data.get('score', '')} / {result_data.get('max_score', '')}", ln=True)
        pdf.ln(10)

        for _, response in user_responses.iterrows():
            question_id = int(response['question_id'])
            question_data = questions_dict.get(question_id, {})

            if not question_data:
                continue

            pdf.set_font("Arial", style='B', size=12)
            pdf.cell(0, 10, f"Q{question_id}: {question_data.get('question_text', '')}", ln=True)
            pdf.set_font("Arial", size=11)
            pdf.cell(0, 8, f"Your Answer: {response.get('given_answer', '')}", ln=True)
            pdf.cell(0, 8, f"Correct Answer: {response.get('correct_answer', '')}", ln=True)
            pdf.cell(0, 8, f"Marks Obtained: {response.get('marks_obtained', '')}", ln=True)

            # Embed image if available
            image_url = question_data.get('image_url')
            if question_data.get('has_image') and image_url:
                try:
                    img_response = requests.get(image_url, timeout=10)
                    img_response.raise_for_status()
                    content_type = img_response.headers.get('Content-Type', '')
                    if not content_type.startswith('image/'):
                        print(f"Image error Q{question_id}: URL did not return image, got {content_type}")
                        continue
                    img = Image.open(BytesIO(img_response.content))
                    print(f"Image format: {img.format}, size: {img.size}, mode: {img.mode}")
                    print(f"Downloaded image size: {len(img_response.content) / 1024:.2f} KB")
                    # Always resize to max width 400
                    max_width = 400
                    if img.size[0] > max_width:
                        w_percent = (max_width / float(img.size[0]))
                        h_size = int((float(img.size[1]) * float(w_percent)))
                        img = img.resize((max_width, h_size), Image.LANCZOS)
                    # Always convert to RGB (JPEG does not support transparency)
                    img = img.convert('RGB')
                    with tempfile.NamedTemporaryFile(delete=False, suffix='.jpg') as tmp_img:
                        img.save(tmp_img.name, format='JPEG', quality=40, optimize=True)
                        pdf.image(tmp_img.name, w=100)
                    os.remove(tmp_img.name)
                except Exception as e:
                    print(f"Image error Q{question_id}: {e}")
            pdf.ln(10)

        # Output PDF to browser
        pdf_output = BytesIO()
        pdf.output(pdf_output)
        pdf_output.seek(0)
        return (
            pdf_output.read(),
            200,
            {
                'Content-Type': 'application/pdf',
                'Content-Disposition': f'attachment; filename=exam_response_{exam_id}.pdf'
            }
        )

    except Exception as e:
        print(f"PDF generation error: {e}")
        flash('Error generating PDF.', 'error')
        return redirect(url_for('response_page', exam_id=exam_id))


@app.route('/response-pdf-alt/<int:exam_id>')
@login_required
def response_pdf_alt(exam_id):
    """Alternative PDF generation using reportlab for better image support"""
    try:
        # Get response data as in response_page
        results_df = load_csv_with_cache('results.csv')
        responses_df = load_csv_with_cache('responses.csv')
        exams_df = load_csv_with_cache('exams.csv')

        user_id = int(session['user_id'])
        user_results = results_df[
            (results_df['student_id'].astype('Int64') == user_id) &
            (results_df['exam_id'].astype('Int64') == int(exam_id))
        ].sort_values('id', ascending=False)

        if user_results.empty:
            flash('Response not found!', 'error')
            return redirect(url_for('dashboard'))

        result_record = user_results.head(1)
        result_id = int(result_record.iloc[0]['id'])
        result_data = result_record.iloc[0].to_dict()

        exam_record = exams_df[exams_df['id'].astype('Int64') == int(exam_id)]
        if exam_record.empty:
            flash('Exam not found!', 'error')
            return redirect(url_for('dashboard'))

        exam_data = exam_record.iloc[0].to_dict()

        if 'exam_id' in responses_df.columns:
            user_responses = responses_df[
                (responses_df['result_id'].astype('Int64') == result_id) &
                (responses_df['exam_id'].astype('Int64') == int(exam_id))
            ].sort_values('question_id')
        else:
            user_responses = responses_df[
                responses_df['result_id'].astype('Int64') == result_id
            ].sort_values('question_id')

        question_ids = set(user_responses['question_id'].astype(int).tolist())
        cached_data = get_cached_exam_data(exam_id)
        questions_dict = {}

        if cached_data:
            for q in cached_data['questions']:
                if int(q['id']) in question_ids:
                    questions_dict[int(q['id'])] = q
        else:
            questions_df = load_csv_with_cache('questions.csv')
            filtered_questions = questions_df[questions_df['id'].astype(int).isin(question_ids)]
            for _, q in filtered_questions.iterrows():
                q_dict = q.to_dict()
                has_image, image_url = process_question_image_fixed(q_dict)
                q_dict['has_image'] = has_image
                q_dict['image_url'] = image_url
                questions_dict[int(q['id'])] = q_dict

        # Prepare PDF
        pdf_buffer = BytesIO()
        c = canvas.Canvas(pdf_buffer, pagesize=letter)
        width, height = letter
        y = height - 50

        c.setFont("Helvetica-Bold", 14)
        c.drawString(50, y, f"Exam Response Analysis: {exam_data.get('title', 'Exam')}")
        y -= 30
        c.setFont("Helvetica", 12)
        c.drawString(50, y, f"Student: {session.get('full_name', '')}")
        y -= 20
        c.drawString(50, y, f"Score: {result_data.get('score', '')} / {result_data.get('max_score', '')}")
        y -= 30

        for _, response in user_responses.iterrows():
            question_id = int(response['question_id'])
            question_data = questions_dict.get(question_id, {})
            if not question_data:
                continue

            c.setFont("Helvetica-Bold", 12)
            c.drawString(50, y, f"Q{question_id}: {question_data.get('question_text', '')}")
            y -= 20
            c.setFont("Helvetica", 11)
            c.drawString(50, y, f"Your Answer: {response.get('given_answer', '')}")
            y -= 15
            c.drawString(50, y, f"Correct Answer: {response.get('correct_answer', '')}")
            y -= 15
            c.drawString(50, y, f"Marks Obtained: {response.get('marks_obtained', '')}")
            y -= 20

            image_url = question_data.get('image_url')
            if question_data.get('has_image') and image_url:
                try:
                    img_response = requests.get(image_url, timeout=10)
                    img_response.raise_for_status()
                    img = Image.open(BytesIO(img_response.content))
                    img = img.convert('RGB')
                    max_width = 300
                    if img.size[0] > max_width:
                        w_percent = (max_width / float(img.size[0]))
                        h_size = int((float(img.size[1]) * float(w_percent)))
                        img = img.resize((max_width, h_size), Image.LANCZOS)
                    img_byte_arr = BytesIO()
                    img.save(img_byte_arr, format='JPEG', quality=40)
                    img_byte_arr.seek(0)
                    c.drawImage(ImageReader(img_byte_arr), 50, y-120, width=150, preserveAspectRatio=True, mask='auto')
                    y -= 130
                except Exception as e:
                    print(f"Image error Q{question_id}: {e}")
            else:
                y -= 10

            if y < 100:
                c.showPage()
                y = height - 50

        c.save()
        pdf_buffer.seek(0)
        return (
            pdf_buffer.read(),
            200,
            {
                'Content-Type': 'application/pdf',
                'Content-Disposition': f'attachment; filename=exam_response_{exam_id}_alt.pdf'
            }
        )
    except Exception as e:
        print(f"PDF generation error (reportlab): {e}")
        flash('Error generating PDF.', 'error')
        return redirect(url_for('response_page', exam_id=exam_id))


@app.route('/logout')
def logout():
    user_keys = [
        "user_id",
        "full_name",
        "email",
        "token",
        "user_role",
        "student_data",
    ]

    for k in user_keys:
        session.pop(k, None)

    flash('You have been logged out successfully.', 'info')
    return redirect(url_for('login'))


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
    return render_template('error.html', error_code=404, error_message="Page not found"), 404


@app.errorhandler(500)
def internal_error(error):
    return render_template('error.html', error_code=500, error_message="Internal server error"), 500



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