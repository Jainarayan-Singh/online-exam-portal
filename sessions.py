# sessions.py - Smart cleanup with automatic expired token removal

import os
import json
import threading
from datetime import datetime, timedelta
from functools import wraps
from flask import session, redirect, url_for, flash

_lock = threading.RLock()
SESSIONS_FILE = os.path.join(os.getcwd(), 'sessions.json')

def _is_token_expired(last_seen_str, hours=3):
    """Check if token is expired based on last seen time"""
    try:
        last_seen = datetime.fromisoformat(last_seen_str)
        return datetime.now() - last_seen > timedelta(hours=hours)
    except:
        return True  # If can't parse date, consider expired

def _load_active_sessions():
    """Load only active (non-expired) sessions"""
    try:
        if os.path.exists(SESSIONS_FILE):
            with open(SESSIONS_FILE, 'r') as f:
                all_sessions = json.load(f)
            
            # Filter out expired tokens automatically
            active_sessions = {}
            for token, data in all_sessions.items():
                if not _is_token_expired(data.get('last_seen', '2020-01-01T00:00:00')):
                    active_sessions[token] = data
            
            # If we removed expired sessions, save the cleaned file
            if len(active_sessions) != len(all_sessions):
                _save_sessions(active_sessions)
                print(f"Cleaned {len(all_sessions) - len(active_sessions)} expired sessions")
            
            return active_sessions
    except Exception as e:
        print(f"Error loading sessions: {e}")
    return {}

def _save_sessions(sessions_data):
    """Save sessions to local JSON file"""
    try:
        with open(SESSIONS_FILE, 'w') as f:
            json.dump(sessions_data, f, indent=2)
    except Exception as e:
        print(f"Error saving sessions: {e}")

def generate_session_token():
    import secrets
    return secrets.token_urlsafe(32)

def save_session_record(session_data):
    """Save session record locally with automatic cleanup"""
    with _lock:
        sessions = _load_active_sessions()  # This automatically removes expired ones
        token = session_data.get('token')
        if token:
            sessions[token] = {
                'user_id': session_data.get('user_id'),
                'device_info': session_data.get('device_info', 'unknown'),
                'last_seen': datetime.now().isoformat(),
                'is_exam_active': session_data.get('is_exam_active', False),
                'active': True
            }
            _save_sessions(sessions)
            return True
    return False

def get_session_by_token(token):
    """Get session by token (automatically filters expired)"""
    sessions = _load_active_sessions()
    return sessions.get(token)

def invalidate_session(user_id, token=None):
    """Invalidate sessions for a user"""
    with _lock:
        sessions = _load_active_sessions()
        user_id = str(user_id)
        
        if token:
            # Remove specific token
            sessions.pop(token, None)
        else:
            # Remove all sessions for user
            sessions_to_remove = [
                tok for tok, data in sessions.items() 
                if str(data.get('user_id')) == user_id
            ]
            for tok in sessions_to_remove:
                sessions.pop(tok, None)
        
        _save_sessions(sessions)
        return True

def update_last_seen(user_id, token):
    """Update last seen timestamp"""
    with _lock:
        sessions = _load_active_sessions()
        if token in sessions and str(sessions[token].get('user_id')) == str(user_id):
            sessions[token]['last_seen'] = datetime.now().isoformat()
            _save_sessions(sessions)
            return True
    return False

def set_exam_active(user_id, token, exam_id=None, result_id=None, is_active=True):
    """Set exam active status"""
    with _lock:
        sessions = _load_active_sessions()
        if token in sessions and str(sessions[token].get('user_id')) == str(user_id):
            sessions[token]['is_exam_active'] = is_active
            if exam_id is not None:
                sessions[token]['exam_id'] = exam_id
            if result_id is not None:
                sessions[token]['result_id'] = result_id
            _save_sessions(sessions)
            return True
    return False

def get_user_role(user_id):
    """Get user role from users.csv cache"""
    try:
        from main import load_csv_with_cache
        users_df = load_csv_with_cache('users.csv')
        if users_df is not None and not users_df.empty:
            user_row = users_df[users_df['id'].astype(str) == str(user_id)]
            if not user_row.empty:
                role = user_row.iloc[0].get('role', '')
                return str(role).lower().strip()
    except Exception as e:
        print(f"Error getting user role: {e}")
    return None

# Ultra-lightweight decorators
def require_valid_session(f):
    """Basic session validation"""
    @wraps(f)
    def wrapped(*args, **kwargs):
        uid = session.get("user_id")
        tok = session.get("token")
        
        if not uid or not tok:
            return redirect(url_for("login"))
        
        # Quick check - expired tokens are auto-removed by _load_active_sessions
        if not get_session_by_token(tok):
            session.clear()
            flash("Session expired. Please login again.", "warning")
            return redirect(url_for("login"))
        
        return f(*args, **kwargs)
    return wrapped

def require_user_role(f):
    """User role validation"""
    @wraps(f)
    def wrapped(*args, **kwargs):
        uid = session.get("user_id")
        tok = session.get("token")
        
        if not uid or not tok:
            flash("Please login to access this page.", "warning")
            return redirect(url_for("login"))
        
        # Expired tokens are automatically filtered out
        if not get_session_by_token(tok):
            session.clear()
            flash("Session expired. Please login again.", "warning")
            return redirect(url_for("login"))
        
        # Check admin conflict
        admin_id = session.get("admin_id")
        if admin_id:
            flash("You are logged in as Admin. Please logout to access User portal.", "warning")
            return redirect(url_for("admin.dashboard"))
        
        return f(*args, **kwargs)
    return wrapped

def require_admin_role(f):
    """Admin role validation"""
    @wraps(f)
    def wrapped(*args, **kwargs):
        uid = session.get("user_id")
        tok = session.get("token")
        admin_id = session.get("admin_id")
        
        if not uid or not tok or not admin_id:
            flash("Admin login required.", "warning")
            return redirect(url_for("admin.admin_login"))
        
        return f(*args, **kwargs)
    return wrapped

# Optional: Force cleanup every hour (lightweight since we auto-clean on load)
import threading
def periodic_maintenance():
    """Light maintenance - just touch the file to trigger auto-cleanup"""
    try:
        _load_active_sessions()  # This will auto-clean expired tokens
    except:
        pass
    threading.Timer(3600, periodic_maintenance).start()  # 1 hour

periodic_maintenance()



def _ensure_sessions_file():
    """Ensure sessions.json file exists"""
    if not os.path.exists(SESSIONS_FILE):
        try:
            with open(SESSIONS_FILE, 'w') as f:
                json.dump({}, f)
            print(f"Created sessions file: {SESSIONS_FILE}")
        except Exception as e:
            print(f"Error creating sessions file: {e}")

# Call this when the module loads
_ensure_sessions_file()