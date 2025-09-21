# drive_utils.py
import os, time
from threading import RLock
from google_drive_service import create_drive_service, save_csv_to_drive

CSV_ENV_MAP = {
    'users': 'USERS_FILE_ID',
    'exams': 'EXAMS_FILE_ID',
    'questions': 'QUESTIONS_FILE_ID',
    'results': 'RESULTS_FILE_ID',
    'responses': 'RESPONSES_FILE_ID',
    'exam_attempts': 'EXAM_ATTEMPTS_FILE_ID',
    'requests_raised': 'REQUESTS_RAISED_FILE_ID',
    'sessions': 'SESSIONS_FILE_ID'
}

_lock = RLock()
_drive_service = None

def _get_drive_service():
    global _drive_service
    if _drive_service is None:
        _drive_service = create_drive_service()
    return _drive_service

def safe_csv_save_with_retry(df, csv_type, max_retries=5):
    file_env = CSV_ENV_MAP.get(csv_type)
    file_id = os.environ.get(file_env) if file_env else None
    if not file_id:
        print(f"[drive_utils] No file id configured for '{csv_type}' (env {file_env})")
        return False
    service = _get_drive_service()
    for attempt in range(max_retries):
        try:
            ok = save_csv_to_drive(service, df, file_id)
            if ok:
                return True
        except Exception as e:
            print(f"[drive_utils] attempt {attempt+1} error saving {csv_type}: {e}")
        if attempt < max_retries - 1:
            time.sleep((2 ** attempt) * 0.5)
    print(f"[drive_utils] FAILED to save {csv_type} after {max_retries} attempts")
    return False

def safe_dual_file_save(results_df, responses_df, results_type='results', responses_type='responses', max_retries=5):
    with _lock:
        ok1 = safe_csv_save_with_retry(results_df, results_type, max_retries)
        if not ok1:
            return False, "Failed to save results"
        ok2 = safe_csv_save_with_retry(responses_df, responses_type, max_retries)
        if not ok2:
            return False, "Failed to save responses"
        return True, "Both saved"
