import os
import mimetypes
import pandas as pd
from functools import wraps
from datetime import datetime
from flask import Blueprint, render_template, request, redirect, url_for, flash, session, jsonify
from werkzeug.utils import secure_filename
from googleapiclient.http import MediaFileUpload
from googleapiclient.errors import HttpError
import json
import pandas as pd
from flask import request, jsonify, render_template
import os, json
from flask import session, redirect, url_for, request
from markupsafe import escape
import html
from latex_editor import latex_bp
from markupsafe import Markup
from drive_utils import safe_csv_save_with_retry
from sessions import require_valid_session, generate_session_token, save_session_record, invalidate_session, get_session_by_token
from datetime import datetime
from flask import abort, send_file
import io



from google_drive_service import (
    create_drive_service,         
    create_subject_folder,
    load_csv_from_drive,
    save_csv_to_drive,
    clear_cache,
    find_file_by_name,
    get_drive_service_for_upload  # USER OAUTH (token.json) — for image uploads & folder ops
)
from sessions import require_valid_session

# ========== Blueprint ==========
admin_bp = Blueprint("admin", __name__, url_prefix="/admin", template_folder="templates")
admin_bp.register_blueprint(latex_bp)
# ========== Config ==========
USERS_FILE_ID     = os.environ.get("USERS_FILE_ID")
EXAMS_FILE_ID     = os.environ.get("EXAMS_FILE_ID")
QUESTIONS_FILE_ID = os.environ.get("QUESTIONS_FILE_ID")
SUBJECTS_FILE_ID  = os.environ.get("SUBJECTS_FILE_ID")
REQUESTS_RAISED_FILE_ID = os.environ.get("REQUESTS_RAISED_FILE_ID")
RESULTS_FILE_ID  = os.environ.get("RESULTS_FILE_ID")
RESPONSES_FILE_ID  = os.environ.get("RESPONSES_FILE_ID")

UPLOAD_TMP_DIR = os.path.join(os.path.dirname(__file__), "uploads_tmp")
os.makedirs(UPLOAD_TMP_DIR, exist_ok=True)

ALLOWED_IMAGE_EXTS = {".png", ".jpg", ".jpeg", ".gif", ".webp", ".bmp"}
MAX_FILE_SIZE_MB = 15

# Allowed HTML tags for question text (very limited)
BLEACH_ALLOWED_TAGS = ["br", "b", "i", "u", "sup", "sub", "strong", "em"]
BLEACH_ALLOWED_ATTRIBUTES = {}  # no attributes allowed

EXAM_ATTEMPTS_FILE_ID = os.environ.get("EXAM_ATTEMPTS_FILE_ID")

# ========== Helpers ==========
def admin_required(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        if "admin_id" not in session:
            flash("Admin login required.", "warning")
            return redirect(url_for("admin.admin_login"))
        return f(*args, **kwargs)
    return wrapper

def _get_subject_folders(service):
    """Return [{'id', 'name', 'folder_id'}] from subjects.csv for dropdown."""
    out = []
    try:
        if not SUBJECTS_FILE_ID:
            return out
        df = load_csv_from_drive(service, SUBJECTS_FILE_ID)  # caching ok
        if df is None or df.empty:
            return out
        norm = {c.lower(): c for c in df.columns}
        name_col = norm.get("subject_name") or norm.get("name")
        folder_col = norm.get("subject_folder_id") or norm.get("folder_id")
        id_col = norm.get("id")
        if not (name_col and folder_col):
            return out
        for _, r in df.iterrows():
            fid = str(r.get(folder_col, "")).strip()
            if fid:
                out.append({
                    "id": int(r.get(id_col, 0)) if (id_col and id_col in df.columns) else None,
                    "name": str(r.get(name_col, "")).strip(),
                    "folder_id": fid,
                })
    except Exception as e:
        print(f"⚠️ _get_subject_folders error: {e}")
    out.sort(key=lambda x: x["name"].lower())
    return out


def sanitize_for_display(text):
    if not text:
        return ""
    # HTML escape sab kuch
    safe = html.escape(str(text))
    # But allow <br> and mathjax ($$...$$)
    safe = safe.replace("&lt;br&gt;", "<br>")
    safe = safe.replace("&lt;br/&gt;", "<br>")
    safe = safe.replace("&dollar;&dollar;", "$$")
    return safe


def sanitize_html(s):
    """
    Lightweight sanitizer used by admin listing.
    - Normalizes newlines (CRLF -> LF)
    - Escapes HTML special chars to prevent injection
    - Returns a plain string (safe for template rendering without |safe)
    """
    if s is None:
        return ""
    s = str(s)
    # normalize newlines
    s = s.replace("\r\n", "\n").replace("\r", "\n")
    # escape HTML special chars
    return str(escape(s))


@admin_bp.route("/login", methods=["GET", "POST"])
def admin_login():
    if request.method == "POST":
        identifier = request.form.get("username", "").strip().lower()
        password = request.form.get("password", "").strip()
        from main import load_csv_with_cache
        users_df = load_csv_with_cache("users.csv")
        if users_df.empty:
            flash("No users available!", "error")
            return redirect(url_for("admin.admin_login"))

        users_df["username_lower"] = users_df["username"].astype(str).str.strip().str.lower()
        users_df["email_lower"] = users_df["email"].astype(str).str.strip().str.lower()
        users_df["role_lower"] = users_df["role"].astype(str).str.strip().str.lower()

        user_row = users_df[
            (users_df["username_lower"] == identifier) |
            (users_df["email_lower"] == identifier)
        ]
        if user_row.empty:
            flash("Invalid username/email or password!", "error")
            return redirect(url_for("admin.admin_login"))

        user = user_row.iloc[0]
        if str(user.get("password", "")) != password:
            flash("Invalid username/email or password!", "error")
            return redirect(url_for("admin.admin_login"))

        role = str(user.get("role", "")).lower()
        if "admin" not in role:
            flash("You do not have admin access.", "error")
            return redirect(url_for("admin.admin_login"))

        # enforce single active session for this user (invalidate previous tokens)
        try:
            invalidate_session(int(user["id"]))
        except Exception as e:
            print("[admin_login] invalidate_session error:", e)

        # create new token and save server-side record
        # create new token and save locally (fast)
        token = generate_session_token()
        save_session_record({
            "user_id": int(user["id"]),
            "token": token,
            "device_info": request.headers.get("User-Agent", "unknown"),
            "is_exam_active": False
        })

        # set flask session for admin
        session["admin_id"] = int(user["id"])
        session["admin_name"] = user.get("username")
        session["user_id"] = int(user["id"])
        session["username"] = user.get("username")
        session["full_name"] = user.get("full_name", user.get("username"))
        session["token"] = token
        session.permanent = True
        print("[admin_login] flask session snapshot:", {k: session.get(k) for k in ['user_id','token','admin_id','admin_name']})

        flash("Admin login successful!", "success")
        return redirect(url_for("admin.dashboard"))

    return render_template("admin/admin_login.html")

def _parse_max_attempts(raw):
    if raw is None:
        return None
    s = str(raw).strip()
    if s == "":
        return None
    if not s.isdigit():
        raise ValueError("max_attempts must be a non-negative integer")
    val = int(s)
    if val < 0:
        raise ValueError("max_attempts must be non-negative")
    return val

@admin_bp.route("/logout")
def logout():
    """Enhanced admin logout - completely clear session and invalidate tokens"""
    uid = session.get("user_id")
    tok = session.get("token")
    
    # Invalidate server-side session
    if uid and tok:
        try:
            from sessions import invalidate_session, set_exam_active
            set_exam_active(uid, tok, is_active=False)
            invalidate_session(uid, token=tok)
        except Exception as e:
            print(f"[admin_logout] Error invalidating session: {e}")
    
    # Completely clear Flask session
    session.clear()
    
    flash("Admin logout successful.", "success")
    return redirect(url_for("home"))

# ========== Dashboard ==========
@admin_bp.route("/dashboard")
@admin_required
def dashboard():
    sa = create_drive_service()

    exams_df = load_csv_from_drive(sa, EXAMS_FILE_ID)
    users_df = load_csv_from_drive(sa, USERS_FILE_ID)

    total_exams = 0 if exams_df is None or exams_df.empty else len(exams_df)
    total_users = 0 if users_df is None or users_df.empty else len(users_df)

    admins_count = 0
    if users_df is not None and not users_df.empty and "role" in users_df.columns:
        admins_count = (
            users_df["role"]
            .astype(str)
            .str.strip()
            .str.lower()
            .str.contains("admin")
            .sum()
        )

    stats = {
        "total_exams": total_exams,
        "total_users": total_users,
        "total_admins": admins_count,
    }
    return render_template("admin/dashboard.html", stats=stats)

# ========== Subjects ==========
@admin_bp.route("/subjects", methods=["GET", "POST"])
@admin_required
def subjects():
    sa = create_drive_service()
    subjects_df = load_csv_from_drive(sa, SUBJECTS_FILE_ID)

    if request.method == "POST":
        subject_name = request.form["subject_name"].strip()
        if not subject_name:
            flash("Subject name required.", "danger")
            return redirect(url_for("admin.subjects"))

        if (not subjects_df.empty and
            subjects_df["subject_name"].astype(str).str.lower().eq(subject_name.lower()).any()):
            flash("Subject already exists.", "warning")
            return redirect(url_for("admin.subjects"))

        try:
            drive_owner = get_drive_service_for_upload()
        except Exception as e:
            flash(f"Cannot create folder: {e}", "danger")
            return redirect(url_for("admin.subjects"))

        folder_id, created_at = create_subject_folder(drive_owner, subject_name)

        new_id = 1 if subjects_df.empty else int(subjects_df["id"].max()) + 1
        new_row = pd.DataFrame([{
            "id": new_id,
            "subject_name": subject_name,
            "subject_folder_id": folder_id,
            "subject_folder_created_at": created_at
        }])
        updated_df = pd.concat([subjects_df, new_row], ignore_index=True)
        safe_csv_save_with_retry(updated_df, 'subjects')
        clear_cache()
        flash(f"Subject '{subject_name}' created successfully.", "success")
        return redirect(url_for("admin.subjects"))

    return render_template("admin/subjects.html", subjects=subjects_df.to_dict(orient="records"))

@admin_bp.route("/subjects/edit/<int:subject_id>", methods=["POST"])
@admin_required
def edit_subject(subject_id):
    sa = create_drive_service()
    subjects_df = load_csv_from_drive(sa, SUBJECTS_FILE_ID)
    if subjects_df.empty or subject_id not in subjects_df["id"].values:
        flash("Subject not found.", "danger")
        return redirect(url_for("admin.subjects"))

    new_name = request.form.get("subject_name", "").strip()
    if not new_name:
        flash("Subject name required.", "danger")
        return redirect(url_for("admin.subjects"))

    row = subjects_df[subjects_df["id"] == subject_id].iloc[0]
    folder_id = row["subject_folder_id"]

    try:
        drive_owner = get_drive_service_for_upload()
        drive_owner.files().update(fileId=folder_id, body={"name": new_name}).execute()
    except Exception as e:
        print(f"⚠️ rename folder failed: {e}")
        flash("Drive folder rename failed; CSV updated.", "warning")

    subjects_df.loc[subjects_df["id"] == subject_id, "subject_name"] = new_name
    safe_csv_save_with_retry(subjects_df, 'subjects')
    clear_cache()
    flash("Subject updated successfully.", "success")
    return redirect(url_for("admin.subjects"))

@admin_bp.route("/subjects/delete/<int:subject_id>")
@admin_required
def delete_subject(subject_id):
    service = create_drive_service()
    subjects_df = load_csv_from_drive(service, SUBJECTS_FILE_ID)
    if subjects_df is None or subjects_df.empty:
        flash("No subjects found.", "warning")
        return redirect(url_for("admin.subjects"))

    if "id" not in subjects_df.columns:
        flash("Subjects file is missing 'id' column.", "danger")
        return redirect(url_for("admin.subjects"))
    working_df = subjects_df.copy()
    working_df["id"] = pd.to_numeric(working_df["id"], errors="coerce").astype("Int64")

    hit = working_df[working_df["id"] == int(subject_id)]
    if hit.empty:
        flash("Subject not found.", "danger")
        return redirect(url_for("admin.subjects"))

    folder_id_col = "subject_folder_id" if "subject_folder_id" in working_df.columns else "folder_id"
    folder_id = str(hit.iloc[0].get(folder_id_col, "")).strip()

    if folder_id:
        try:
            drive_owner = get_drive_service_for_upload()
            try:
                drive_owner.files().delete(fileId=folder_id, supportsAllDrives=True).execute()
                print(f"✅ Deleted folder {folder_id} using owner OAuth client.")
            except Exception as e_del:
                print(f"⚠ Owner delete failed for {folder_id}: {e_del} — trying to trash it instead.")
                try:
                    drive_owner.files().update(fileId=folder_id, body={"trashed": True}, supportsAllDrives=True).execute()
                    print(f"♻ Trashed folder {folder_id} using owner OAuth client.")
                except Exception as e_trash:
                    print(f"❌ Failed to trash folder {folder_id} with owner client: {e_trash}")
        except Exception as e_owner:
            print(f"⚠ get_drive_service_for_upload() failed: {e_owner}. Trying service-account client as fallback.")
            try:
                service.files().delete(fileId=folder_id, supportsAllDrives=True).execute()
                print(f"✅ Deleted folder {folder_id} using service-account client (fallback).")
            except Exception as e_sa:
                print(f"❌ Fallback SA delete also failed for {folder_id}: {e_sa}")

    new_df = working_df[working_df["id"] != int(subject_id)].copy()
    ok = safe_csv_save_with_retry(new_df, 'subjects')
    if ok:
        clear_cache()
        flash("Subject deleted (Drive folder removed if permitted).", "info")
    else:
        flash("Failed to update subjects.csv after delete.", "danger")

    return redirect(url_for("admin.subjects"))

# ========== Exams ==========
@admin_bp.route("/exams", methods=["GET", "POST"])
@admin_required
def exams():
    service = create_drive_service()
    exams_df = load_csv_from_drive(service, EXAMS_FILE_ID)
    if exams_df is None:
        exams_df = pd.DataFrame()
    if "max_attempts" not in exams_df.columns:
        exams_df["max_attempts"] = ""
    if request.method == "POST":
        form = request.form
        try:
            new_id = int(exams_df["id"].max()) + 1 if not exams_df.empty else 1
        except Exception:
            new_id = 1
        try:
            parsed_max = _parse_max_attempts(form.get("max_attempts", ""))
        except ValueError as e:
            flash(str(e), "danger")
            return redirect(url_for("admin.exams"))
        row = {
            "id": new_id,
            "name": form.get("name", "").strip(),
            "date": form.get("date", "").strip(),
            "start_time": form.get("start_time", "").strip(),
            "duration": int(form.get("duration") or 0),
            "total_questions": int(form.get("total_questions") or 0),
            "status": form.get("status", "").strip(),
            "instructions": form.get("instructions", "").strip(),
            "positive_marks": form.get("positive_marks", "").strip(),
            "negative_marks": form.get("negative_marks", "").strip(),
            "max_attempts": "" if parsed_max is None else str(parsed_max)
        }
        new_df = pd.concat([exams_df, pd.DataFrame([row])], ignore_index=True)
        ok = safe_csv_save_with_retry(new_df, 'exams')
        if ok:
            clear_cache()
            flash("Exam created successfully.", "success")
            return redirect(url_for("admin.exams"))
        else:
            flash("Failed to save exam.", "danger")
            return redirect(url_for("admin.exams"))
    return render_template("admin/exams.html", exams=exams_df.to_dict(orient="records"))


@admin_bp.route("/exams/edit/<int:exam_id>", methods=["GET", "POST"])
@admin_required
def edit_exam(exam_id):
    service = create_drive_service()
    exams_df = load_csv_from_drive(service, EXAMS_FILE_ID)
    if exams_df is None:
        exams_df = pd.DataFrame()
    if "max_attempts" not in exams_df.columns:
        exams_df["max_attempts"] = ""
    exam = exams_df[exams_df["id"] == exam_id]
    if exam.empty:
        flash("Exam not found.", "danger")
        return redirect(url_for("admin.exams"))
    if request.method == "POST":
        form = request.form
        try:
            duration_val = int(form.get("duration") or 0)
            total_q_val = int(form.get("total_questions") or 0)
        except Exception:
            flash("Duration and Total Questions must be integers.", "danger")
            return redirect(url_for("admin.edit_exam", exam_id=exam_id))
        try:
            parsed_max = _parse_max_attempts(form.get("max_attempts", ""))
        except ValueError as e:
            flash(str(e), "danger")
            return redirect(url_for("admin.edit_exam", exam_id=exam_id))
        exams_df.loc[exams_df["id"] == exam_id, [
            "name", "date", "start_time", "duration",
            "total_questions", "status",
            "instructions", "positive_marks", "negative_marks", "max_attempts"
        ]] = [
            form.get("name", "").strip(),
            form.get("date", "").strip(),
            form.get("start_time", "").strip(),
            duration_val,
            total_q_val,
            form.get("status", "").strip(),
            form.get("instructions", "").strip(),
            form.get("positive_marks", "").strip(),
            form.get("negative_marks", "").strip(),
            "" if parsed_max is None else str(parsed_max)
        ]
        ok = safe_csv_save_with_retry(exams_df, 'exams')
        if ok:
            clear_cache()
            flash("Exam updated successfully.", "success")
            return redirect(url_for("admin.exams"))
        else:
            flash("Failed to save exam changes.", "danger")
            return redirect(url_for("admin.edit_exam", exam_id=exam_id))
    return render_template("admin/edit_exam.html", exam=exam.iloc[0].to_dict())

@admin_bp.route("/exams/delete/<int:exam_id>", methods=["GET"])
@admin_required
def delete_exam(exam_id):
    service = create_drive_service()
    exams_df = load_csv_from_drive(service, EXAMS_FILE_ID)
    if exams_df is None or exams_df.empty:
        flash("Exam not found.", "danger")
        return redirect(url_for("admin.exams"))
    try:
        ids = exams_df["id"].astype(int)
    except Exception:
        ids = exams_df["id"].apply(lambda x: int(str(x).strip()) if str(x).strip().isdigit() else None)
    if int(exam_id) not in ids.tolist():
        flash("Exam not found.", "danger")
        return redirect(url_for("admin.exams"))
    exams_df = exams_df[ids != int(exam_id)].reset_index(drop=True)
    ok = safe_csv_save_with_retry(exams_df, "exams")
    if ok:
        clear_cache()
        flash("Exam deleted successfully.", "success")
    else:
        flash("Failed to delete exam.", "danger")
    return redirect(url_for("admin.exams"))

# ========== Questions helpers & CRUD ==========
QUESTIONS_COLUMNS = [
    "id", "exam_id", "question_text", "option_a", "option_b", "option_c", "option_d",
    "correct_answer", "question_type", "image_path", "positive_marks", "negative_marks", "tolerance"
]

def _ensure_questions_df(df):
    """Return a DataFrame guaranteed to have QUESTIONS_COLUMNS in order and safe dtypes."""
    if df is None or df.empty:
        df = pd.DataFrame(columns=QUESTIONS_COLUMNS)

    for c in QUESTIONS_COLUMNS:
        if c not in df.columns:
            df[c] = ""

    for col in ("positive_marks", "negative_marks", "tolerance"):
        if col in df.columns:
            df[col] = df[col].fillna("").astype(str)

    return df[QUESTIONS_COLUMNS].copy()

@admin_bp.route("/questions", methods=["GET"])
@admin_required
def questions_index():
    sa = create_drive_service()
    exams_df = load_csv_from_drive(sa, EXAMS_FILE_ID)
    exams = []
    if not exams_df.empty:
        for _, r in exams_df.iterrows():
            exams.append({
                "id": int(r.get("id")) if "id" in exams_df.columns and str(r.get("id")).strip() else None,
                "name": r.get("name") if "name" in exams_df.columns else f"Exam {r.get('id')}"
            })

    selected_exam_id = request.args.get("exam_id", type=int)
    if not selected_exam_id and exams:
        selected_exam_id = exams[0]["id"]

    questions_df = load_csv_from_drive(sa, QUESTIONS_FILE_ID)
    questions_df = _ensure_questions_df(questions_df)

    if selected_exam_id:
        filtered = questions_df[questions_df["exam_id"].astype(str) == str(selected_exam_id)]
    else:
        filtered = questions_df.copy()

    questions = []
    for _, r in filtered.iterrows():
        # sanitize server-side and keep markup safe for templates
        qtext = sanitize_html(r.get("question_text", ""))
        questions.append({
            "id": int(r["id"]) if str(r["id"]).strip() else None,
            "exam_id": int(r["exam_id"]) if str(r["exam_id"]).strip() else None,
            "question_text": qtext,
            "option_a": sanitize_html(r.get("option_a", "")),
            "option_b": sanitize_html(r.get("option_b", "")),
            "option_c": sanitize_html(r.get("option_c", "")),
            "option_d": sanitize_html(r.get("option_d", "")),
            "correct_answer": r.get("correct_answer", ""),
            "question_type": r.get("question_type", ""),
            "image_path": r.get("image_path", ""),
            "positive_marks": r.get("positive_marks", ""),
            "negative_marks": r.get("negative_marks", ""),
            "tolerance": r.get("tolerance", "")
        })

    return render_template("admin/questions.html",
                           exams=exams,
                           selected_exam_id=selected_exam_id,
                           questions=questions)

@admin_bp.route("/questions/add", methods=["GET", "POST"])
@admin_required
def add_question():
    sa = create_drive_service()
    exams_df = load_csv_from_drive(sa, EXAMS_FILE_ID)
    exams = []
    if not exams_df.empty:
        for _, r in exams_df.iterrows():
            exams.append({"id": int(r.get("id")) if "id" in exams_df.columns and str(r.get("id")).strip() else None,
                          "name": r.get("name") if "name" in exams_df.columns else f"Exam {r.get('id')}"})

    if request.method == "POST":
        qdf = load_csv_from_drive(sa, QUESTIONS_FILE_ID)
        qdf = _ensure_questions_df(qdf)

        try:
            next_id = int(qdf["id"].max()) + 1 if not qdf.empty and qdf["id"].astype(str).str.strip().any() else 1
        except Exception:
            next_id = 1

        data = request.form.to_dict()
        new_row = {
            "id": next_id,
            "exam_id": int(data.get("exam_id") or 0),
            "question_text": data.get("question_text", "").strip(),
            "option_a": data.get("option_a", "").strip(),
            "option_b": data.get("option_b", "").strip(),
            "option_c": data.get("option_c", "").strip(),
            "option_d": data.get("option_d", "").strip(),
            "correct_answer": data.get("correct_answer", "").strip(),
            "question_type": data.get("question_type", "").strip(),
            "image_path": data.get("image_path", "").strip(),
            "positive_marks": data.get("positive_marks", "").strip() or "4",
            "negative_marks": data.get("negative_marks", "").strip() or "1",
            "tolerance": data.get("tolerance", "").strip() or ""
        }

        new_df = pd.concat([qdf, pd.DataFrame([new_row])], ignore_index=True)
        ok = safe_csv_save_with_retry(new_df, 'questions')
        if ok:
            clear_cache()
            flash("Question added successfully.", "success")
            return redirect(url_for("admin.questions_index", exam_id=new_row["exam_id"]))
        else:
            flash("Failed to save question.", "danger")
            return redirect(url_for("admin.add_question"))

    return render_template("admin/add_question.html", exams=exams, question=None, form_mode="add")

@admin_bp.route("/questions/edit/<int:question_id>", methods=["GET", "POST"])
@admin_required
def edit_question(question_id):
    sa = create_drive_service()
    exams_df = load_csv_from_drive(sa, EXAMS_FILE_ID)
    exams = []
    if not exams_df.empty:
        for _, r in exams_df.iterrows():
            exams.append({"id": int(r.get("id")) if "id" in exams_df.columns and str(r.get("id")).strip() else None,
                          "name": r.get("name") if "name" in exams_df.columns else f"Exam {r.get('id')}"})

    qdf = load_csv_from_drive(sa, QUESTIONS_FILE_ID)
    qdf = _ensure_questions_df(qdf)

    hit = qdf[qdf["id"].astype(str) == str(question_id)]
    if hit.empty:
        flash("Question not found.", "danger")
        return redirect(url_for("admin.questions_index"))

    if request.method == "POST":
        data = request.form.to_dict()
        idx = hit.index[0]
        qdf.at[idx, "exam_id"] = int(data.get("exam_id") or qdf.at[idx, "exam_id"])
        qdf.at[idx, "question_text"] = data.get("question_text", "").strip()
        qdf.at[idx, "option_a"] = data.get("option_a", "").strip()
        qdf.at[idx, "option_b"] = data.get("option_b", "").strip()
        qdf.at[idx, "option_c"] = data.get("option_c", "").strip()
        qdf.at[idx, "option_d"] = data.get("option_d", "").strip()
        qdf.at[idx, "correct_answer"] = data.get("correct_answer", "").strip()
        qdf.at[idx, "question_type"] = data.get("question_type", "").strip()
        qdf.at[idx, "image_path"] = data.get("image_path", "").strip()
        qdf.at[idx, "positive_marks"] = data.get("positive_marks", "").strip() or "4"
        qdf.at[idx, "negative_marks"] = data.get("negative_marks", "").strip() or "1"
        qdf.at[idx, "tolerance"] = data.get("tolerance", "").strip() or ""

        ok = save_csv_to_drive(sa, qdf, QUESTIONS_FILE_ID)
        if ok:
            clear_cache()
            flash("Question updated.", "success")
            return redirect(url_for("admin.questions_index", exam_id=qdf.at[idx, "exam_id"]))
        else:
            flash("Failed to save changes.", "danger")
            return redirect(url_for("admin.edit_question", question_id=question_id))

    qrow = hit.iloc[0].to_dict()
    # Provide sanitized markup to the edit form (it will be shown inside textarea - we send raw string)
    return render_template("admin/edit_question.html", exams=exams, question=qrow, form_mode="edit")

@admin_bp.route("/questions/delete/<int:question_id>", methods=["POST"])
@admin_required
def delete_question(question_id):
    sa = create_drive_service()
    qdf = load_csv_from_drive(sa, QUESTIONS_FILE_ID)
    qdf = _ensure_questions_df(qdf)
    new_df = qdf[qdf["id"].astype(str) != str(question_id)].copy()
    ok = safe_csv_save_with_retry(new_df, 'questions')
    if ok:
        clear_cache()
        flash("Question deleted.", "info")
    else:
        flash("Failed to delete question.", "danger")
    return redirect(url_for("admin.questions_index"))

@admin_bp.route("/questions/delete-multiple", methods=["POST"])
@admin_required
def delete_multiple_questions():
    try:
        payload = request.get_json(force=True)
        if not payload or "ids" not in payload:
            return jsonify({"success": False, "message": "Invalid payload"}), 400

        ids = payload.get("ids") or []
        if not isinstance(ids, list) or not ids:
            return jsonify({"success": False, "message": "No IDs provided"}), 400

        ids_str = set([str(int(i)) for i in ids if str(i).strip()])

        sa = create_drive_service()
        qdf = load_csv_from_drive(sa, QUESTIONS_FILE_ID)
        qdf = _ensure_questions_df(qdf)

        before_count = len(qdf)
        new_df = qdf[~qdf["id"].astype(str).isin(ids_str)].copy()
        after_count = len(new_df)
        deleted_count = before_count - after_count

        ok = safe_csv_save_with_retry(new_df, 'questions')
        if not ok:
            return jsonify({"success": False, "message": "Failed to save updated questions CSV"}), 500

        clear_cache()
        return jsonify({"success": True, "deleted": deleted_count})

    except Exception as e:
        print(f"❌ delete_multiple_questions error: {e}")
        return jsonify({"success": False, "message": str(e)}), 500

@admin_bp.route("/questions/bulk-update", methods=["POST"])
@admin_required
def questions_bulk_update():
    try:
        payload = request.get_json(force=True)
        if not payload:
            return jsonify({"success": False, "message": "Empty payload"}), 400

        exam_id = payload.get("exam_id")
        qtype = str(payload.get("question_type") or "").strip()
        pos = payload.get("positive_marks")
        neg = payload.get("negative_marks")
        tol = payload.get("tolerance")

        if not exam_id:
            return jsonify({"success": False, "message": "exam_id required"}), 400
        if not qtype:
            return jsonify({"success": False, "message": "question_type required"}), 400

        pos_str = None if pos is None else str(pos).strip()
        neg_str = None if neg is None else str(neg).strip()
        tol_str = None if tol is None else str(tol)

        sa = create_drive_service()
        qdf = load_csv_from_drive(sa, QUESTIONS_FILE_ID)
        qdf = _ensure_questions_df(qdf)

        mask_exam = qdf["exam_id"].astype(str) == str(exam_id)
        mask_type = qdf["question_type"].astype(str).str.strip().str.upper() == qtype.upper()
        mask = mask_exam & mask_type

        if not mask.any():
            return jsonify({"success": True, "updated": 0, "message": "No matching questions found"}), 200

        idxs = qdf[mask].index.tolist()
        for idx in idxs:
            if pos_str is not None and pos_str != "":
                qdf.at[idx, "positive_marks"] = pos_str
            if neg_str is not None and neg_str != "":
                qdf.at[idx, "negative_marks"] = neg_str
            if tol is not None:
                qdf.at[idx, "tolerance"] = tol_str

        ok = save_csv_to_drive(sa, qdf, QUESTIONS_FILE_ID)
        if not ok:
            return jsonify({"success": False, "message": "Failed to save CSV"}), 500

        clear_cache()
        return jsonify({"success": True, "updated": len(idxs)}), 200

    except Exception as e:
        print(f"❌ questions_bulk_update error: {e}")
        return jsonify({"success": False, "message": str(e)}), 500

# ========== Upload Images ==========
from googleapiclient.http import MediaIoBaseUpload  # add near other imports
import io

@admin_bp.route("/upload-images", methods=["GET", "POST"])
@admin_required
def upload_images_page():
    if request.method == "POST":
        try:
            folder_id = request.form.get("subject_folder_id", "").strip()
            files = request.files.getlist("images")

            if not folder_id:
                return jsonify({"success": False, "message": "No folder selected."}), 400
            if not files:
                return jsonify({"success": False, "message": "No files received."}), 400

            try:
                drive_upload = get_drive_service_for_upload()
            except Exception as e:
                return jsonify({"success": False, "message": str(e)}), 500

            uploaded = 0
            failed = []

            for f in files:
                if not f or not f.filename:
                    continue
                safe_name = secure_filename(f.filename)
                ext = os.path.splitext(safe_name)[1].lower()
                if ext not in ALLOWED_IMAGE_EXTS:
                    failed.append({"filename": safe_name, "error": f"Not allowed type ({ext})"})
                    continue

                f.seek(0, os.SEEK_END)
                size_mb = f.tell() / (1024 * 1024)
                f.seek(0)
                if size_mb > MAX_FILE_SIZE_MB:
                    failed.append({"filename": safe_name, "error": f"Exceeds {MAX_FILE_SIZE_MB} MB"})
                    continue

                temp_path = os.path.join(UPLOAD_TMP_DIR, safe_name)
                f.save(temp_path)

                fh = None
                try:
                    existing_id = find_file_by_name(drive_upload, safe_name, folder_id)
                    mime, _ = mimetypes.guess_type(safe_name)
                    fh = open(temp_path, "rb")
                    media = MediaIoBaseUpload(fh, mimetype=mime or "application/octet-stream", resumable=True)

                    if existing_id:
                        drive_upload.files().update(fileId=existing_id, media_body=media).execute()
                    else:
                        drive_upload.files().create(
                            body={"name": safe_name, "parents": [folder_id]},
                            media_body=media,
                            fields="id"
                        ).execute()
                    uploaded += 1
                except HttpError as e:
                    failed.append({"filename": safe_name, "error": str(e)})
                except Exception as e:
                    failed.append({"filename": safe_name, "error": str(e)})
                finally:
                    try:
                        if fh and not fh.closed:
                            fh.close()
                    except Exception as _close_err:
                        print(f"⚠ Could not close temp file handle for {temp_path}: {_close_err}")
                    try:
                        if os.path.exists(temp_path):
                            os.remove(temp_path)
                    except Exception as rm_err:
                        print(f"⚠ Could not remove temp file {temp_path}: {rm_err}")

            return jsonify({"success": True, "uploaded": uploaded, "failed": failed}), 200

        except Exception as e:
            return jsonify({"success": False, "message": f"Unexpected error: {str(e)}"}), 500

    sa = create_drive_service()
    subjects = _get_subject_folders(sa)
    load_error = None if subjects else "No subjects found (or subjects.csv missing)."
    return render_template(
        "admin/upload_images.html",
        subjects=subjects,
        load_error=load_error
    )

@admin_bp.route("/questions/batch-add", methods=["POST"])
@admin_required
def questions_batch_add():
    try:
        payload = request.get_json(force=True)
        if not payload or "questions" not in payload or "exam_id" not in payload:
            return jsonify({"success": False, "message": "Invalid payload"}), 400

        exam_id = int(payload.get("exam_id"))
        items = payload.get("questions", [])
        if not items:
            return jsonify({"success": False, "message": "No questions provided"}), 400

        sa = create_drive_service()
        qdf = load_csv_from_drive(sa, QUESTIONS_FILE_ID)
        qdf = _ensure_questions_df(qdf)

        try:
            next_id = int(qdf["id"].max()) + 1 if not qdf.empty and qdf["id"].astype(str).str.strip().any() else 1
        except Exception:
            next_id = 1

        new_rows = []
        added_count = 0
        for it in items:
            qt = (it.get("question_text") or "").strip()
            if not qt:
                continue
            row = {
                "id": next_id,
                "exam_id": exam_id,
                "question_text": qt,
                "option_a": (it.get("option_a") or "").strip(),
                "option_b": (it.get("option_b") or "").strip(),
                "option_c": (it.get("option_c") or "").strip(),
                "option_d": (it.get("option_d") or "").strip(),
                "correct_answer": (it.get("correct_answer") or "").strip(),
                "question_type": (it.get("question_type") or "MCQ").strip(),
                "image_path": (it.get("image_path") or "").strip(),
                "positive_marks": str(it.get("positive_marks") or "4"),
                "negative_marks": str(it.get("negative_marks") or "1"),
                "tolerance": str(it.get("tolerance") or "")
            }
            new_rows.append(row)
            next_id += 1
            added_count += 1

        if not new_rows:
            return jsonify({"success": False, "message": "No valid rows to add"}), 400

        appended = pd.concat([qdf, pd.DataFrame(new_rows)], ignore_index=True)
        ok = safe_csv_save_with_retry(appended, 'questions')
        if not ok:
            return jsonify({"success": False, "message": "Failed to save to Drive"}), 500

        clear_cache()
        return jsonify({"success": True, "added": added_count})

    except Exception as e:
        print(f"❌ questions_batch_add error: {e}")
        return jsonify({"success": False, "message": str(e)}), 500

# ========== Publish ==========
@admin_bp.route("/publish", methods=["GET", "POST"])
@admin_required
def publish():
    if request.method == "POST":
        clear_cache()
        try:
            from main import clear_user_cache
            clear_user_cache()
            session["force_refresh"] = True
        except Exception as e:
            print(f"⚠️ Failed to clear user cache: {e}")
        flash("✅ All caches cleared. Fresh data will load now!", "success")
        return redirect(url_for("admin.dashboard"))
    return render_template("admin/publish.html")

# --- START: Web OAuth routes for admin (paste into admin.py) ---


# Make sure your Flask app sets a secret key (main.py already may do this).
# These routes are under admin_bp (url_prefix="/admin"), so redirect URIs must include /admin/oauth2callback

@admin_bp.route("/authorize", methods=["GET"])
@admin_required
def admin_oauth_authorize():
    """
    Start web-OAuth flow (one-time). User (admin) must visit this and approve Google Drive scopes.
    Requires GOOGLE_OAUTH_CLIENT_JSON env (client_secret.json content or path).
    """
    from google_auth_oauthlib.flow import Flow

    raw = os.getenv("GOOGLE_OAUTH_CLIENT_JSON")
    if not raw:
        return "Missing GOOGLE_OAUTH_CLIENT_JSON env. Paste your client_secret_web.json here.", 500

    # Accept either raw JSON text or a file path
    try:
        cfg = json.loads(raw) if raw.strip().startswith("{") else json.load(open(raw, "r", encoding="utf-8"))
    except Exception as e:
        return f"Failed to load client JSON: {e}", 500

    # prefer 'web' key if present
    client_cfg = {"web": cfg.get("web")} if "web" in cfg else {"installed": cfg.get("installed", cfg)}
    scopes = ["https://www.googleapis.com/auth/drive", "https://www.googleapis.com/auth/drive.file", "https://www.googleapis.com/auth/drive.readonly"]

    flow = Flow.from_client_config(client_cfg, scopes=scopes)
    # redirect URI must match EXACTLY what's in Google Cloud Console (see instructions)
    flow.redirect_uri = url_for("admin.admin_oauth_callback", _external=True)

    auth_url, state = flow.authorization_url(access_type="offline", include_granted_scopes="true", prompt="consent")
    session["oauth_state"] = state
    return redirect(auth_url)

@admin_bp.route("/oauth2callback", methods=["GET"])
@admin_required
def admin_oauth_callback():
    """
    OAuth callback for admin authorize. Exchanges code -> token and attempts to save token.json.
    If server can't write file, it will return the token JSON so you can paste it into Render env.
    """
    from google_auth_oauthlib.flow import Flow
    from google.oauth2.credentials import Credentials as UserCredentials
    from googleapiclient.discovery import build
    import datetime

    raw = os.getenv("GOOGLE_OAUTH_CLIENT_JSON")
    if not raw:
        return "Missing GOOGLE_OAUTH_CLIENT_JSON env. Cannot complete auth.", 500

    try:
        cfg = json.loads(raw) if raw.strip().startswith("{") else json.load(open(raw, "r", encoding="utf-8"))
    except Exception as e:
        return f"Failed to load client JSON: {e}", 500

    client_cfg = {"web": cfg.get("web")} if "web" in cfg else {"installed": cfg.get("installed", cfg)}
    scopes = ["https://www.googleapis.com/auth/drive", "https://www.googleapis.com/auth/drive.file", "https://www.googleapis.com/auth/drive.readonly"]

    state = session.get("oauth_state")
    flow = Flow.from_client_config(client_cfg, scopes=scopes, state=state)
    flow.redirect_uri = url_for("admin.admin_oauth_callback", _external=True)

    # Exchange code
    flow.fetch_token(authorization_response=request.url)
    creds = flow.credentials

    token_obj = {
        "token": creds.token,
        "refresh_token": creds.refresh_token,
        "token_uri": creds.token_uri,
        "client_id": creds.client_id,
        "client_secret": creds.client_secret,
        "scopes": list(creds.scopes or scopes),
        "expiry": creds.expiry.isoformat() if getattr(creds, "expiry", None) else None
    }

    # Try to save to disk (token.json path) — fallback is to display JSON for manual copy
    token_path = os.getenv("GOOGLE_SERVICE_TOKEN_JSON", "token.json")
    try:
        with open(token_path, "w", encoding="utf-8") as f:
            json.dump(token_obj, f)
        # Try to read Drive user email to confirm
        try:
            creds_obj = UserCredentials.from_authorized_user_info(token_obj, scopes=scopes)
            svc = build("drive", "v3", credentials=creds_obj, cache_discovery=False)
            about = svc.about().get(fields="user").execute()
            email = about.get("user", {}).get("emailAddress", "unknown")
            return f"Success — token saved to <code>{token_path}</code>. Authorized as: {email}"
        except Exception:
            return f"Success — token saved to <code>{token_path}</code>. Authorization complete."
    except Exception as e:
        # If cannot write, return token JSON so user can copy-paste into Render env
        pretty = json.dumps(token_obj, indent=2)
        return (
            "Could not write token.json on server. Copy the JSON below and set it as the value of the "
            "<code>GOOGLE_SERVICE_TOKEN_JSON</code> environment variable in Render (paste full JSON):"
            + "<pre>" + pretty + "</pre>"
        )

# --- END: Web OAuth routes for admin ---

@admin_bp.route("/attempts")
@admin_required
def attempts():
    sa = create_drive_service()
    users_df = load_csv_from_drive(sa, USERS_FILE_ID)
    exams_df = load_csv_from_drive(sa, EXAMS_FILE_ID)
    attempts_df = load_csv_from_drive(sa, EXAM_ATTEMPTS_FILE_ID)

    if users_df is None: users_df = pd.DataFrame()
    if exams_df is None: exams_df = pd.DataFrame()
    if attempts_df is None: attempts_df = pd.DataFrame()

    rows = []
    for _, u in users_df.iterrows():
        for _, e in exams_df.iterrows():
            student_id, exam_id = str(u["id"]), str(e["id"])
            user_attempts = attempts_df[(attempts_df["student_id"].astype(str)==student_id) &
                                        (attempts_df["exam_id"].astype(str)==exam_id)]
            used = len(user_attempts)
            
            # More robust max_attempts handling
            max_att_raw = e.get("max_attempts", "")
            
            # Convert to string and strip
            if pd.isna(max_att_raw):
                max_att = ""
            else:
                max_att = str(max_att_raw).strip()
            
            # Calculate remaining
            if max_att == "" or max_att == "0" or max_att.lower() == "nan":
                remaining = "∞"
                display_max = "∞"
            else:
                try:
                    max_attempts_int = int(float(max_att))  # Handle case where it's stored as float string
                    remaining = max(max_attempts_int - used, 0)
                    display_max = str(max_attempts_int)
                except (ValueError, TypeError):
                    remaining = "?"
                    display_max = max_att
            
            rows.append({
                "student_id": student_id,
                "username": u.get("username"),
                "exam_id": exam_id,
                "exam_name": e.get("name"),
                "max_attempts": display_max,
                "attempts_used": used,
                "remaining": remaining
            })
    
    return render_template("admin/attempts.html", rows=rows)


@admin_bp.route("/attempts/modify", methods=["POST"])
@admin_required
def attempts_modify():
    sa = create_drive_service()
    payload = request.get_json(force=True)
    student_id = str(payload.get("student_id"))
    exam_id = str(payload.get("exam_id"))
    action = payload.get("action")
    amount = int(payload.get("amount") or 0)

    attempts_df = load_csv_from_drive(sa, EXAM_ATTEMPTS_FILE_ID)
    if attempts_df is None: 
        attempts_df = pd.DataFrame(columns=["id","student_id","exam_id","attempt_number","status","start_time","end_time"])

    mask = (attempts_df["student_id"].astype(str)==student_id) & (attempts_df["exam_id"].astype(str)==exam_id)
    current = attempts_df[mask]
    used = len(current)

    if action == "reset":
        attempts_df = attempts_df[~mask]
    elif action == "decrease":
        drop_ids = current.tail(amount)["id"].tolist()
        attempts_df = attempts_df[~attempts_df["id"].isin(drop_ids)]
    elif action == "increase":
        start_id = (attempts_df["id"].astype(int).max() + 1) if not attempts_df.empty else 1
        for i in range(amount):
            attempts_df = pd.concat([attempts_df, pd.DataFrame([{
                "id": start_id+i,
                "student_id": student_id,
                "exam_id": exam_id,
                "attempt_number": used+i+1,
                "status": "manual_add",
                "start_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "end_time": ""
            }])], ignore_index=True)

    # Use save_csv_to_drive directly instead of safe_csv_save_with_retry
    ok = save_csv_to_drive(sa, attempts_df, EXAM_ATTEMPTS_FILE_ID)
    if ok:
        clear_cache()
        return jsonify({"success": True})
    return jsonify({"success": False}), 500




@admin_bp.route("/requests")
@admin_required
def requests_dashboard():
    """Requests dashboard with new and history tabs"""
    return render_template("admin/requests.html")

@admin_bp.route("/requests/new")
@admin_required
def new_requests():
    """View new (pending) access requests"""
    try:
        service = create_drive_service()
        
        # Load requests data
        requests_df = load_csv_from_drive(service, REQUESTS_RAISED_FILE_ID)
        if requests_df is None:
            requests_df = pd.DataFrame(columns=[
                'request_id', 'username', 'email', 'current_access',
                'requested_access', 'request_date', 'request_status', 'reason'
            ])
        
        # Filter pending requests
        if not requests_df.empty:
            pending_requests = requests_df[
                requests_df['request_status'].astype(str).str.lower() == 'pending'
            ].sort_values('request_date', ascending=False)
        else:
            pending_requests = pd.DataFrame()
        
        # Convert to list of dictionaries for template
        requests_list = []
        for _, row in pending_requests.iterrows():
            requests_list.append({
                'request_id': int(row['request_id']),
                'username': row['username'],
                'email': row['email'],
                'current_access': row['current_access'],
                'requested_access': row['requested_access'],
                'request_date': row['request_date'],
                'status': row['request_status']
            })
        
        return render_template("admin/new_requests.html", requests=requests_list)
        
    except Exception as e:
        print(f"Error loading new requests: {e}")
        flash("Error loading requests data.", "error")
        return render_template("admin/new_requests.html", requests=[])

@admin_bp.route("/requests/history")
@admin_required
def requests_history():
    """View completed/denied requests history"""
    try:
        service = create_drive_service()
        
        # Load requests data
        requests_df = load_csv_from_drive(service, REQUESTS_RAISED_FILE_ID)
        if requests_df is None:
            requests_df = pd.DataFrame()
        
        # Filter completed/denied requests
        history_requests = []
        if not requests_df.empty:
            history_df = requests_df[
                requests_df['request_status'].astype(str).str.lower().isin(['completed', 'denied'])
            ].sort_values('request_date', ascending=False)
            
            for _, row in history_df.iterrows():
                history_requests.append({
                    'request_id': int(row['request_id']),
                    'username': row['username'],
                    'email': row['email'],
                    'current_access': row['current_access'],
                    'requested_access': row['requested_access'],
                    'request_date': row['request_date'],
                    'status': row['request_status'],
                    'reason': row.get('reason', ''),
                    'processed_by': row.get('processed_by', 'Admin'),
                    'processed_date': row.get('processed_date', '')
                })
        
        return render_template("admin/requests_history.html", requests=history_requests)
        
    except Exception as e:
        print(f"Error loading requests history: {e}")
        flash("Error loading requests history.", "error")
        return render_template("admin/requests_history.html", requests=[])

@admin_bp.route("/requests/approve/<int:request_id>", methods=["POST"])
@admin_required
def approve_request(request_id):
    """Approve an access request"""
    try:
        data = request.get_json()
        approved_access = data.get('approved_access')
        
        if not approved_access:
            return jsonify({
                'success': False,
                'message': 'Please select an access level to approve'
            }), 400
        
        service = create_drive_service()
        
        # Load requests data
        requests_df = load_csv_from_drive(service, REQUESTS_RAISED_FILE_ID)
        if requests_df is None or requests_df.empty:
            return jsonify({
                'success': False,
                'message': 'No requests found'
            }), 404
        
        # Find the specific request
        request_row = requests_df[
            (requests_df['request_id'].astype(int) == request_id) &
            (requests_df['request_status'].astype(str).str.lower() == 'pending')
        ]
        
        if request_row.empty:
            return jsonify({
                'success': False,
                'message': 'Request not found or already processed'
            }), 404
        
        request_data = request_row.iloc[0]
        username = request_data['username']
        email = request_data['email']
        
        # Load users data and update access
        users_df = load_csv_from_drive(service, USERS_FILE_ID)
        if users_df is None or users_df.empty:
            return jsonify({
                'success': False,
                'message': 'Users database unavailable'
            }), 500
        
        # Find and update user
        users_df['username_lower'] = users_df['username'].astype(str).str.strip().str.lower()
        users_df['email_lower'] = users_df['email'].astype(str).str.strip().str.lower()
        
        user_mask = (
            (users_df['username_lower'] == username.lower()) &
            (users_df['email_lower'] == email.lower())
        )
        
        if not user_mask.any():
            return jsonify({
                'success': False,
                'message': 'User not found in database'
            }), 404
        
        # Update user access
        users_df.loc[user_mask, 'role'] = approved_access
        users_df.loc[user_mask, 'updated_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        # Update request status
        request_mask = requests_df['request_id'].astype(int) == request_id
        requests_df.loc[request_mask, 'request_status'] = 'completed'
        requests_df.loc[request_mask, 'reason'] = f'Approved: {approved_access}'
        requests_df.loc[request_mask, 'processed_by'] = session.get('admin_name', 'Admin')
        requests_df.loc[request_mask, 'processed_date'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        # Save both files
        users_success = safe_csv_save_with_retry(users_df, 'users')
        requests_success = safe_csv_save_with_retry(requests_df, 'requests_raised')
        
        if users_success and requests_success:
            clear_cache()
            return jsonify({
                'success': True,
                'message': f'Request approved successfully. User {username} now has {approved_access} access.'
            })
        else:
            return jsonify({
                'success': False,
                'message': 'Error saving approval. Please try again.'
            }), 500
        
    except Exception as e:
        print(f"Error approving request: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({
            'success': False,
            'message': 'System error occurred'
        }), 500

@admin_bp.route("/requests/deny/<int:request_id>", methods=["POST"])
@admin_required
def deny_request(request_id):
    """Deny an access request with reason"""
    try:
        data = request.get_json()
        denial_reason = data.get('reason', '').strip()
        
        if not denial_reason:
            return jsonify({
                'success': False,
                'message': 'Please provide a reason for denial'
            }), 400
        
        service = create_drive_service()
        
        # Load requests data
        requests_df = load_csv_from_drive(service, REQUESTS_RAISED_FILE_ID)
        if requests_df is None or requests_df.empty:
            return jsonify({
                'success': False,
                'message': 'No requests found'
            }), 404
        
        # Find the specific request
        request_row = requests_df[
            (requests_df['request_id'].astype(int) == request_id) &
            (requests_df['request_status'].astype(str).str.lower() == 'pending')
        ]
        
        if request_row.empty:
            return jsonify({
                'success': False,
                'message': 'Request not found or already processed'
            }), 404
        
        # Update request status
        request_mask = requests_df['request_id'].astype(int) == request_id
        requests_df.loc[request_mask, 'request_status'] = 'denied'
        requests_df.loc[request_mask, 'reason'] = denial_reason
        requests_df.loc[request_mask, 'processed_by'] = session.get('admin_name', 'Admin')
        requests_df.loc[request_mask, 'processed_date'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        # Save requests file
        success = safe_csv_save_with_retry(requests_df, 'requests_raised')
        
        if success:
            clear_cache()
            return jsonify({
                'success': True,
                'message': f'Request denied successfully.'
            })
        else:
            return jsonify({
                'success': False,
                'message': 'Error saving denial. Please try again.'
            }), 500
        
    except Exception as e:
        print(f"Error denying request: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({
            'success': False,
            'message': 'System error occurred'
        }), 500

@admin_bp.route("/api/requests/stats")
@admin_required
def api_requests_stats():
    """API endpoint for request statistics"""
    try:
        service = create_drive_service()
        
        # Load requests data
        requests_df = load_csv_from_drive(service, REQUESTS_RAISED_FILE_ID)
        if requests_df is None or requests_df.empty:
            return jsonify({
                'pending': 0,
                'completed': 0,
                'denied': 0,
                'total': 0
            })
        
        # Count by status
        status_counts = requests_df['request_status'].astype(str).str.lower().value_counts()
        
        return jsonify({
            'pending': int(status_counts.get('pending', 0)),
            'completed': int(status_counts.get('completed', 0)),
            'denied': int(status_counts.get('denied', 0)),
            'total': len(requests_df)
        })
        
    except Exception as e:
        print(f"Error getting request stats: {e}")
        return jsonify({
            'pending': 0,
            'completed': 0,
            'denied': 0,
            'total': 0
        })




# Add these routes to your admin.py file

@admin_bp.route("/users/manage")
@admin_required
def users_manage():
    """View users management page"""
    try:
        service = create_drive_service()
        
        # Load users data
        users_df = load_csv_from_drive(service, USERS_FILE_ID)
        if users_df is None:
            users_df = pd.DataFrame(columns=[
                'id', 'username', 'email', 'full_name', 'role', 'created_at', 'updated_at'
            ])
        
        # Prepare users data (exclude sensitive information)
        users_list = []
        if not users_df.empty:
            for _, row in users_df.iterrows():
                users_list.append({
                    'id': int(row['id']),
                    'username': row.get('username', ''),
                    'email': row.get('email', ''),
                    'full_name': row.get('full_name', ''),
                    'role': row.get('role', 'user'),
                    'created_at': row.get('created_at', ''),
                    'updated_at': row.get('updated_at', '')
                })
        
        # Sort by username
        users_list.sort(key=lambda x: x['username'].lower())
        
        return render_template("admin/users_manage.html", users=users_list)
        
    except Exception as e:
        print(f"Error loading users management: {e}")
        flash("Error loading users data.", "error")
        return render_template("admin/users_manage.html", users=[])

@admin_bp.route("/users/update-role", methods=["POST"])
@admin_required
def update_user_role():
    """Update user role"""
    try:
        data = request.get_json()
        user_id = data.get('user_id')
        new_role = data.get('new_role', '').strip()
        
        if not user_id or not new_role:
            return jsonify({
                'success': False,
                'message': 'User ID and role are required'
            }), 400
        
        # Validate role
        valid_roles = ['user', 'admin', 'user,admin']
        if new_role not in valid_roles:
            return jsonify({
                'success': False,
                'message': 'Invalid role selected'
            }), 400
        
        service = create_drive_service()
        
        # Load users data
        users_df = load_csv_from_drive(service, USERS_FILE_ID)
        if users_df is None or users_df.empty:
            return jsonify({
                'success': False,
                'message': 'Users database unavailable'
            }), 500
        
        # Find user
        user_mask = users_df['id'].astype(str) == str(user_id)
        if not user_mask.any():
            return jsonify({
                'success': False,
                'message': 'User not found'
            }), 404
        
        # Get current user info
        user_row = users_df[user_mask].iloc[0]
        username = user_row['username']
        current_role = user_row.get('role', 'user')
        
        # Check if role actually changed
        if current_role == new_role:
            return jsonify({
                'success': True,
                'message': f'User {username} already has {new_role} role',
                'no_change': True
            })
        
        # Update user role
        users_df.loc[user_mask, 'role'] = new_role
        users_df.loc[user_mask, 'updated_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        # Save to CSV
        success = safe_csv_save_with_retry(users_df, 'users')
        
        if success:
            clear_cache()
            return jsonify({
                'success': True,
                'message': f'Successfully updated {username} role from {current_role} to {new_role}',
                'user_id': user_id,
                'new_role': new_role,
                'username': username
            })
        else:
            return jsonify({
                'success': False,
                'message': 'Error saving role update. Please try again.'
            }), 500
        
    except Exception as e:
        print(f"Error updating user role: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({
            'success': False,
            'message': 'System error occurred'
        }), 500

@admin_bp.route("/users/bulk-update-roles", methods=["POST"])
@admin_required
def bulk_update_user_roles():
    """Bulk update multiple user roles"""
    try:
        data = request.get_json()
        updates = data.get('updates', [])
        
        if not updates:
            return jsonify({
                'success': False,
                'message': 'No updates provided'
            }), 400
        
        service = create_drive_service()
        users_df = load_csv_from_drive(service, USERS_FILE_ID)
        
        if users_df is None or users_df.empty:
            return jsonify({
                'success': False,
                'message': 'Users database unavailable'
            }), 500
        
        valid_roles = ['user', 'admin', 'user,admin']
        updated_count = 0
        errors = []
        
        for update in updates:
            user_id = update.get('user_id')
            new_role = update.get('new_role', '').strip()
            
            if not user_id or not new_role:
                errors.append(f'Invalid update data for user {user_id}')
                continue
                
            if new_role not in valid_roles:
                errors.append(f'Invalid role {new_role} for user {user_id}')
                continue
            
            user_mask = users_df['id'].astype(str) == str(user_id)
            if not user_mask.any():
                errors.append(f'User {user_id} not found')
                continue
            
            # Update role
            users_df.loc[user_mask, 'role'] = new_role
            users_df.loc[user_mask, 'updated_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            updated_count += 1
        
        if updated_count > 0:
            success = safe_csv_save_with_retry(users_df, 'users')
            if success:
                clear_cache()
                message = f'Successfully updated {updated_count} user roles'
                if errors:
                    message += f' ({len(errors)} errors occurred)'
                
                return jsonify({
                    'success': True,
                    'message': message,
                    'updated_count': updated_count,
                    'errors': errors
                })
            else:
                return jsonify({
                    'success': False,
                    'message': 'Error saving bulk updates'
                }), 500
        else:
            return jsonify({
                'success': False,
                'message': 'No valid updates to apply',
                'errors': errors
            }), 400
        
    except Exception as e:
        print(f"Error in bulk update: {e}")
        return jsonify({
            'success': False,
            'message': 'System error occurred'
        }), 500

@admin_bp.route("/api/users/stats")
@admin_required
def api_users_stats():
    """API endpoint for user statistics"""
    try:
        service = create_drive_service()
        users_df = load_csv_from_drive(service, USERS_FILE_ID)
        
        if users_df is None or users_df.empty:
            return jsonify({
                'total_users': 0,
                'user_role': 0,
                'admin_role': 0,
                'both_roles': 0
            })
        
        # Count by role
        role_counts = {'user': 0, 'admin': 0, 'both': 0}
        
        for _, row in users_df.iterrows():
            role = str(row.get('role', 'user')).lower().strip()
            if ',' in role or 'user' in role and 'admin' in role:
                role_counts['both'] += 1
            elif 'admin' in role:
                role_counts['admin'] += 1
            else:
                role_counts['user'] += 1
        
        return jsonify({
            'total_users': len(users_df),
            'user_role': role_counts['user'],
            'admin_role': role_counts['admin'],
            'both_roles': role_counts['both']
        })
        
    except Exception as e:
        print(f"Error getting user stats: {e}")
        return jsonify({
            'total_users': 0,
            'user_role': 0,
            'admin_role': 0,
            'both_roles': 0
        })        
        
        


# Add these CORRECTED routes to admin.py (replace the existing analytics routes)

@admin_bp.route("/users-analytics")
@admin_required
def users_analytics():
    """Main users analytics dashboard"""
    return render_template("admin/users_analytics.html")

@admin_bp.route("/api/users-analytics/stats")
@admin_required
def api_users_analytics_stats():
    """API endpoint for users analytics overview stats"""
    try:
        service = create_drive_service()
        
        # Load all required data
        users_df = load_csv_from_drive(service, USERS_FILE_ID)
        exams_df = load_csv_from_drive(service, EXAMS_FILE_ID)
        results_df = load_csv_from_drive(service, RESULTS_FILE_ID)
        responses_df = load_csv_from_drive(service, RESPONSES_FILE_ID)
        
        stats = {
            'total_users': len(users_df) if users_df is not None and not users_df.empty else 0,
            'total_exams': len(exams_df) if exams_df is not None and not exams_df.empty else 0,
            'total_results': len(results_df) if results_df is not None and not results_df.empty else 0,
            'total_responses': len(responses_df) if responses_df is not None and not responses_df.empty else 0
        }
        
        return jsonify(stats)
        
    except Exception as e:
        print(f"Error getting analytics stats: {e}")
        return jsonify({
            'total_users': 0,
            'total_exams': 0,
            'total_results': 0,
            'total_responses': 0
        })

@admin_bp.route("/users-analytics/results")
@admin_required
def users_analytics_results():
    """Results tab content for users analytics"""
    try:
        service = create_drive_service()
        
        # Get filter parameters
        user_filter = request.args.get('user', '')
        exam_filter = request.args.get('exam', '')
        date_from = request.args.get('dateFrom', '')
        date_to = request.args.get('dateTo', '')
        page = int(request.args.get('page', 1))
        per_page = 20
        
        # Load data
        results_df = load_csv_from_drive(service, RESULTS_FILE_ID)
        users_df = load_csv_from_drive(service, USERS_FILE_ID)
        exams_df = load_csv_from_drive(service, EXAMS_FILE_ID)
        
        results_list = []
        pagination = None
        
        if results_df is not None and not results_df.empty:
            # Merge with users and exams data
            if users_df is not None and not users_df.empty:
                results_df = results_df.merge(
                    users_df[['id', 'username', 'full_name']], 
                    left_on='student_id', 
                    right_on='id', 
                    how='left',
                    suffixes=('', '_user')
                )
            
            if exams_df is not None and not exams_df.empty:
                results_df = results_df.merge(
                    exams_df[['id', 'name']], 
                    left_on='exam_id', 
                    right_on='id', 
                    how='left',
                    suffixes=('', '_exam')
                )
            
            # Apply filters
            filtered_df = results_df.copy()
            
            if user_filter:
                filtered_df = filtered_df[filtered_df['student_id'].astype(str) == str(user_filter)]
            
            if exam_filter:
                filtered_df = filtered_df[filtered_df['exam_id'].astype(str) == str(exam_filter)]
            
            # Use 'completed_at' for date filtering
            if date_from:
                try:
                    filtered_df = filtered_df[pd.to_datetime(filtered_df['completed_at']).dt.date >= pd.to_datetime(date_from).date()]
                except:
                    pass
            
            if date_to:
                try:
                    filtered_df = filtered_df[pd.to_datetime(filtered_df['completed_at']).dt.date <= pd.to_datetime(date_to).date()]
                except:
                    pass
            
            # Sort by date
            filtered_df = filtered_df.sort_values('completed_at', ascending=False)
            
            # Pagination
            total_results = len(filtered_df)
            start_idx = (page - 1) * per_page
            end_idx = start_idx + per_page
            paginated_df = filtered_df.iloc[start_idx:end_idx]
            
            # Convert to list
            for _, row in paginated_df.iterrows():
                results_list.append({
                    'id': int(row.get('id', 0)),
                    'username': row.get('username', 'Unknown'),
                    'full_name': row.get('full_name', 'Unknown'),
                    'exam_id': int(row.get('exam_id', 0)),
                    'exam_name': row.get('name', 'Unknown Exam'),
                    'subject_name': 'N/A',
                    'score': row.get('score', 0),
                    'max_score': row.get('max_score', 0),
                    'percentage': float(row.get('percentage', 0)),
                    'grade': row.get('grade', 'N/A'),
                    'duration': f"{row.get('time_taken_minutes', 0):.1f} min" if row.get('time_taken_minutes') else 'N/A',
                    'created_at': row.get('completed_at', 'N/A')
                })
            
            # Create pagination object
            pagination = {
                'page': page,
                'per_page': per_page,
                'total': total_results,
                'start': start_idx + 1 if results_list else 0,
                'end': min(end_idx, total_results),
                'has_prev': page > 1,
                'has_next': end_idx < total_results,
                'prev_num': page - 1 if page > 1 else None,
                'next_num': page + 1 if end_idx < total_results else None
            }
            
            # Add iter_pages method simulation
            def iter_pages():
                total_pages = (total_results + per_page - 1) // per_page
                for p in range(max(1, page - 2), min(total_pages + 1, page + 3)):
                    yield p
            pagination['iter_pages'] = iter_pages
        
        # Get users and exams for filters
        users_list = []
        if users_df is not None and not users_df.empty:
            for _, user in users_df.iterrows():
                users_list.append({
                    'id': int(user.get('id', 0)),
                    'username': user.get('username', ''),
                    'full_name': user.get('full_name', '')
                })
        
        exams_list = []
        if exams_df is not None and not exams_df.empty:
            for _, exam in exams_df.iterrows():
                exams_list.append({
                    'id': int(exam.get('id', 0)),
                    'name': exam.get('name', '')
                })
        
        return render_template("admin/users_analytics_results.html", 
                             results=results_list, 
                             users=users_list, 
                             exams=exams_list,
                             pagination=pagination)
        
    except Exception as e:
        print(f"Error loading results analytics: {e}")
        import traceback
        traceback.print_exc()
        return render_template("admin/users_analytics_results.html", 
                             results=[], 
                             users=[], 
                             exams=[],
                             pagination=None)









@admin_bp.route("/users-analytics/view-result/<int:result_id>/<int:exam_id>")
@admin_required
def users_analytics_view_result(result_id, exam_id):
    """
    Return an HTML view (popup) for a specific result.
    Normalizes fields from:
      results.csv -> id, student_id, exam_id, score, total_questions, correct_answers,
                    incorrect_answers, unanswered_questions, max_score, percentage, grade,
                    time_taken_minutes, completed_at
      exams.csv   -> id, name, date, start_time, duration, total_questions, status,
                    instructions, positive_marks, negative_marks, max_attempts
    Converts NaN/NA to safe defaults, coerces numeric types, and derives attempted_questions.
    """
    try:
        service = create_drive_service()

        results_df = load_csv_from_drive(service, RESULTS_FILE_ID)
        users_df = load_csv_from_drive(service, USERS_FILE_ID)
        exams_df = load_csv_from_drive(service, EXAMS_FILE_ID)
        responses_df = load_csv_from_drive(service, RESPONSES_FILE_ID)
        questions_df = load_csv_from_drive(service, QUESTIONS_FILE_ID)

        # helpers
        def is_missing(v):
            # treat None, NaN, empty string and "NA"/"N/A" as missing
            if v is None:
                return True
            try:
                if isinstance(v, float) and pd.isna(v):
                    return True
            except Exception:
                pass
            s = str(v).strip()
            if s == '' or s.lower() in ('na', 'n/a', 'none', 'nan'):
                return True
            return False

        def s(v, default=''):
            return default if is_missing(v) else str(v)

        def to_int(v, default=0):
            if is_missing(v): return default
            try:
                return int(float(str(v).strip()))
            except Exception:
                return default

        def to_float(v, default=0.0):
            if is_missing(v): return default
            try:
                return float(str(v).strip())
            except Exception:
                return default

        # find result row
        if results_df is None or results_df.empty:
            abort(404)

        rrow = results_df[results_df['id'].astype(str) == str(result_id)]
        if rrow.empty:
            abort(404)
        r = rrow.iloc[0].to_dict()

        # normalized result dict expected by template
        result = {}

        # map / normalize fields from results.csv
        result['id'] = s(r.get('id', result_id), str(result_id))
        result['student_id'] = s(r.get('student_id', ''))
        # score and max_score
        result['score'] = to_float(r.get('score', 0.0))
        # some CSV uses max_score, others use 'max_score' or 'max_score' already; fallback to total_questions*positive_marks if missing
        result['max_score'] = to_float(r.get('max_score', r.get('max_marks', None)), default=0.0)
        # total_questions & counts
        result['total_questions'] = to_int(r.get('total_questions', r.get('total_qs', None)), default=to_int(r.get('total_questions', 0)))
        result['correct_answers'] = to_int(r.get('correct_answers', r.get('correct', 0)))
        result['incorrect_answers'] = to_int(r.get('incorrect_answers', r.get('incorrect', 0)))
        result['unanswered_questions'] = to_int(r.get('unanswered_questions', r.get('unanswered', 0)))

        # Ensure attempted_questions derived
        if result['total_questions'] and (result['unanswered_questions'] is not None):
            result['attempted_questions'] = max(0, result['total_questions'] - result['unanswered_questions'])
        else:
            result['attempted_questions'] = result['correct_answers'] + result['incorrect_answers']

        # percentage and grade
        result['percentage'] = to_float(r.get('percentage', r.get('percent', 0.0)))
        result['grade'] = s(r.get('grade', r.get('grade_label', '')))

        # time and timestamp fields
        result['time_taken_minutes'] = to_float(r.get('time_taken_minutes', r.get('duration_minutes', 0.0)))
        result['completed_at'] = s(r.get('completed_at', r.get('completed_on', '')))

        # fallback: if max_score still zero, try to compute from exam positive_marks * total_questions
        if result['max_score'] in (0, 0.0):
            try:
                # find exam row to compute max_score if possible
                if exams_df is not None and not exams_df.empty:
                    erow = exams_df[exams_df['id'].astype(str) == str(exam_id)]
                    if not erow.empty:
                        e0 = erow.iloc[0].to_dict()
                        positive_marks = to_float(e0.get('positive_marks', e0.get('pos_marks', 0.0)))
                        tq = result.get('total_questions', 0)
                        if positive_marks and tq:
                            result['max_score'] = positive_marks * tq
            except Exception:
                pass

        # find user
        user = {}
        if users_df is not None and not users_df.empty:
            urow = users_df[users_df['id'].astype(str) == str(result.get('student_id', ''))]
            if not urow.empty:
                user = urow.iloc[0].to_dict()
            else:
                user = {'id': result.get('student_id', ''), 'username': 'Unknown', 'full_name': 'Unknown', 'email': ''}
        else:
            user = {'id': result.get('student_id', ''), 'username': 'Unknown', 'full_name': 'Unknown', 'email': ''}

        # find exam and normalize important fields
        exam = {}
        if exams_df is not None and not exams_df.empty:
            erow = exams_df[exams_df['id'].astype(str) == str(exam_id)]
            if not erow.empty:
                e = erow.iloc[0].to_dict()
                exam['id'] = s(e.get('id', exam_id), str(exam_id))
                exam['name'] = s(e.get('name', 'Unknown Exam'))
                exam['date'] = s(e.get('date', ''))
                exam['start_time'] = s(e.get('start_time', ''))
                exam['duration'] = s(e.get('duration', ''))
                exam['total_questions'] = to_int(e.get('total_questions', result.get('total_questions', 0)))
                exam['status'] = s(e.get('status', ''))
                exam['instructions'] = s(e.get('instructions', ''))
                exam['positive_marks'] = to_float(e.get('positive_marks', e.get('positive_mark', 0.0)))
                exam['negative_marks'] = to_float(e.get('negative_marks', e.get('negative_mark', 0.0)))
                exam['max_attempts'] = to_int(e.get('max_attempts', e.get('max_attempts', 1)), default=1)
                # if result max_score still missing, compute from exam
                if (not result.get('max_score')) and exam['positive_marks'] and exam['total_questions']:
                    result['max_score'] = exam['positive_marks'] * exam['total_questions']
            else:
                exam = {'id': exam_id, 'name': 'Unknown Exam', 'description': ''}
        else:
            exam = {'id': exam_id, 'name': 'Unknown Exam', 'description': ''}

        # gather basic responses for this result (optional)
        responses = []
        if responses_df is not None and not responses_df.empty:
            rows = responses_df[responses_df['result_id'].astype(str) == str(result_id)]
            for _, rr in rows.iterrows():
                # keep raw dict here; responses normalization used in view-responses route
                responses.append({k: ("" if (isinstance(v, float) and pd.isna(v)) else v) for k,v in rr.to_dict().items()})

        # final safety: ensure types and defaults for template
        # ensure numeric formats
        result['score'] = to_float(result.get('score', 0.0))
        result['max_score'] = to_float(result.get('max_score', 0.0))
        result['percentage'] = to_float(result.get('percentage', 0.0))
        result['grade'] = s(result.get('grade', ''))

        # attempted questions fallback sanity
        if result.get('attempted_questions') is None:
            result['attempted_questions'] = result.get('correct_answers', 0) + result.get('incorrect_answers', 0)

        # pass everything to template
        return render_template("admin/view_result_popup.html",
                               result=result,
                               user=user,
                               exam=exam,
                               responses=responses)

    except Exception as e:
        print(f"Error in view-result route: {e}")
        import traceback; traceback.print_exc()
        abort(500)



@admin_bp.route("/users-analytics/view-responses/<int:result_id>/<int:exam_id>")
@admin_required
def users_analytics_view_responses(result_id, exam_id):
    """
    View responses for a result — tailored to responses.csv with columns:
    id,result_id,exam_id,question_id,given_answer,correct_answer,is_correct,marks_obtained,question_type,is_attempted
    Normalizes rows into dicts with keys used by template:
      question_id, question_text, user_answer, correct_answer, status, explanation, marks_obtained
    """
    try:
        service = create_drive_service()

        results_df = load_csv_from_drive(service, RESULTS_FILE_ID)
        users_df = load_csv_from_drive(service, USERS_FILE_ID)
        exams_df = load_csv_from_drive(service, EXAMS_FILE_ID)
        responses_df = load_csv_from_drive(service, RESPONSES_FILE_ID)
        questions_df = load_csv_from_drive(service, QUESTIONS_FILE_ID)

        if results_df is None or results_df.empty:
            abort(404)

        # find result
        rrow = results_df[results_df['id'].astype(str) == str(result_id)]
        if rrow.empty:
            abort(404)
        r = rrow.iloc[0].to_dict()
        result = {k: (("" if pd.isna(v) else v)) for k, v in r.items()}

        # find user
        if users_df is not None and not users_df.empty:
            urow = users_df[users_df['id'].astype(str) == str(result.get('student_id', ''))]
            if not urow.empty:
                user = urow.iloc[0].to_dict()
            else:
                user = {'id': result.get('student_id', ''), 'username': 'Unknown', 'full_name': 'Unknown', 'email': ''}
        else:
            user = {'id': result.get('student_id', ''), 'username': 'Unknown', 'full_name': 'Unknown', 'email': ''}

        # find exam
        if exams_df is not None and not exams_df.empty:
            erow = exams_df[exams_df['id'].astype(str) == str(exam_id)]
            if not erow.empty:
                exam = erow.iloc[0].to_dict()
            else:
                exam = {'id': exam_id, 'name': 'Unknown Exam', 'description': ''}
        else:
            exam = {'id': exam_id, 'name': 'Unknown Exam', 'description': ''}

        # helper: safe stringify
        def s(val):
            if val is None: return ''
            if isinstance(val, float) and pd.isna(val): return ''
            return str(val)

        # build responses list
        responses = []
        if responses_df is not None and not responses_df.empty:
            rows = responses_df[responses_df['result_id'].astype(str) == str(result_id)]
            for _, rr in rows.iterrows():
                rd = rr.to_dict()

                # map known columns
                qid = s(rd.get('question_id', '')).strip()
                user_answer = s(rd.get('given_answer', '')).strip()
                correct_answer = s(rd.get('correct_answer', '')).strip()
                marks_obtained = s(rd.get('marks_obtained', '')).strip()
                # is_attempted may be '1','0',True/False, etc.
                is_attempted_raw = rd.get('is_attempted', None)
                is_correct_raw = rd.get('is_correct', None)

                # normalize booleans
                def truthy(x):
                    if x is None: return False
                    if isinstance(x, bool): return x
                    try:
                        xs = str(x).strip().lower()
                    except Exception:
                        return False
                    return xs in ('1', 'true', 'yes', 'y', 't')

                is_attempted = truthy(is_attempted_raw)
                is_correct = truthy(is_correct_raw)

                # compute status
                if not is_attempted:
                    status = 'unanswered'
                else:
                    status = 'correct' if is_correct else 'incorrect'

                # question text: try response row first, else questions_df lookup
                qtext = ''
                if 'question_text' in rd and s(rd.get('question_text')).strip():
                    qtext = s(rd.get('question_text'))
                else:
                    if qid and questions_df is not None and not questions_df.empty:
                        qrow = questions_df[questions_df['id'].astype(str) == qid]
                        if not qrow.empty:
                            # prefer 'question_text' or 'text' or 'question'
                            qtext = s(qrow.iloc[0].get('question_text') or qrow.iloc[0].get('text') or qrow.iloc[0].get('question') or '')
                        else:
                            qtext = ''
                    else:
                        qtext = ''

                # explanation/hint if present in CSV
                explanation = s(rd.get('explanation', '') or rd.get('hint', '') or rd.get('solution', '')).strip()

                norm = {
                    'question_id': qid,
                    'question_text': qtext,
                    'user_answer': user_answer,
                    'correct_answer': correct_answer,
                    'status': status,
                    'explanation': explanation,
                    'marks_obtained': marks_obtained
                }
                responses.append(norm)

        return render_template("admin/view_responses_popup.html",
                               result=result,
                               user=user,
                               exam=exam,
                               responses=responses)

    except Exception as e:
        print(f"Error in view-responses route: {e}")
        import traceback; traceback.print_exc()
        abort(500)



@admin_bp.route("/users-analytics/download-result/<int:result_id>")
@admin_required
def users_analytics_download_result(result_id):
    """
    Generate a detailed PDF for a result: includes exam name, student, score, %
    and a paginated list of responses with status, student answer, correct answer, explanation.
    """
    try:
        service = create_drive_service()
        results_df = load_csv_from_drive(service, RESULTS_FILE_ID)
        users_df = load_csv_from_drive(service, USERS_FILE_ID)
        exams_df = load_csv_from_drive(service, EXAMS_FILE_ID)
        responses_df = load_csv_from_drive(service, RESPONSES_FILE_ID)
        questions_df = load_csv_from_drive(service, QUESTIONS_FILE_ID)

        if results_df is None or results_df.empty:
            abort(404)
        rrow = results_df[results_df['id'].astype(str) == str(result_id)]
        if rrow.empty:
            abort(404)
        r = rrow.iloc[0].to_dict()
        result = {k: (("" if pd.isna(v) else v)) for k, v in r.items()}

        # user
        user = {'username': 'Unknown', 'full_name': 'Unknown', 'email': ''}
        if users_df is not None and not users_df.empty:
            urows = users_df[users_df['id'].astype(str) == str(result.get('student_id', ''))]
            if not urows.empty:
                user = urows.iloc[0].to_dict()

        # exam
        exam = {'name': 'Unknown Exam', 'description': ''}
        if exams_df is not None and not exams_df.empty:
            erows = exams_df[exams_df['id'].astype(str) == str(result.get('exam_id', ''))]
            if not erows.empty:
                exam = erows.iloc[0].to_dict()

        # responses list
        resp_list = []
        if responses_df is not None and not responses_df.empty:
            rows = responses_df[responses_df['result_id'].astype(str) == str(result_id)]
            for _, rr in rows.iterrows():
                rd = rr.to_dict()
                qid = rd.get('question_id')
                if qid and questions_df is not None and not questions_df.empty:
                    qrow = questions_df[questions_df['id'].astype(str) == str(qid)]
                    if not qrow.empty:
                        rd['question_text'] = qrow.iloc[0].get('question_text', rd.get('question_text', ''))
                resp_list.append(rd)

        # Build PDF with ReportLab
        from reportlab.lib.pagesizes import letter
        from reportlab.lib.units import inch
        from reportlab.pdfgen import canvas
        from reportlab.lib.styles import getSampleStyleSheet
        from reportlab.platypus import Paragraph
        from reportlab.lib import utils

        buf = io.BytesIO()
        c = canvas.Canvas(buf, pagesize=letter)
        width, height = letter
        margin = 0.6 * inch
        x = margin
        y = height - margin

        def draw_header():
            nonlocal y
            c.setFont("Helvetica-Bold", 16)
            c.drawString(x, y, f"Result Report - {exam.get('name', 'Exam')}")
            c.setFont("Helvetica", 10)
            c.drawString(x, y - 18, f"Student: {user.get('username', '')} ({user.get('full_name', '')})")
            c.drawString(x + 350, y - 18, f"Result ID: {result_id}")
            c.drawString(x, y - 34, f"Score: {result.get('score', '')} / {result.get('max_score', '')}")
            c.drawString(x + 200, y - 34, f"Percentage: {result.get('percentage', '')}%")
            c.drawString(x + 360, y - 34, f"Grade: {result.get('grade', '')}")
            y -= 54
            c.line(x, y, width - margin, y)
            y -= 12

        def draw_wrapped(text, fontsize=10, leading=12, indent=0):
            nonlocal y
            styles = getSampleStyleSheet()
            style = styles["Normal"]
            style.fontName = "Helvetica"
            style.fontSize = fontsize
            style.leading = leading
            # Use Paragraph to wrap text to PDF width
            avail_width = width - 2 * margin - indent
            p = Paragraph(text.replace("\n", "<br/>"), style)
            w, h = p.wrap(avail_width, y)
            if y - h < margin:
                c.showPage()
                reset_position()
            p.drawOn(c, x + indent, y - h)
            y -= (h + 6)

        def reset_position():
            nonlocal y
            y = height - margin
            draw_header()

        # Create header
        draw_header()

        # Add optional exam description
        if exam.get('description'):
            draw_wrapped(f"<b>Exam Description:</b> {exam.get('description')}", fontsize=9)

        # Add a short spacer
        y -= 6

        # Responses listing
        if resp_list:
            idx = 1
            for resp in resp_list:
                status = resp.get('status', 'unanswered')
                # Break page when needed:
                if y < margin + 80:
                    c.showPage()
                    reset_position()
                c.setFont("Helvetica-Bold", 11)
                c.drawString(x, y, f"Q{idx}. [{status.upper()}]")
                y -= 14
                qtext = resp.get('question_text', '')
                draw_wrapped(f"<b>Question:</b> {qtext}", fontsize=9)
                ua = resp.get('user_answer') or '(Not answered)'
                ca = resp.get('correct_answer') or '(N/A)'
                draw_wrapped(f"<b>Student Answer:</b> {ua}", fontsize=9, indent=12)
                draw_wrapped(f"<b>Correct Answer:</b> {ca}", fontsize=9, indent=12)
                if resp.get('explanation'):
                    draw_wrapped(f"<b>Explanation:</b> {resp.get('explanation')}", fontsize=9, indent=12)
                y -= 6
                idx += 1
        else:
            draw_wrapped("No individual responses available for this result.", fontsize=10)

        # Footer with generation note
        if y < margin + 40:
            c.showPage()
            reset_position()
        c.setFont("Helvetica-Oblique", 8)
        c.drawString(x, margin - 10, f"Generated by Admin Portal")

        c.save()
        buf.seek(0)
        return send_file(buf, as_attachment=True, download_name=f"result_{result_id}.pdf", mimetype='application/pdf')

    except Exception as e:
        print(f"Error generating PDF for result {result_id}: {e}")
        import traceback; traceback.print_exc()
        abort(500)
# --- end routes ---






def _parse_datetime_series(df, candidates):
    for c in candidates:
        if c in df.columns:
            try:
                return pd.to_datetime(df[c], errors='coerce')
            except Exception:
                return pd.to_datetime(df[c].astype(str), errors='coerce')
    return pd.Series([pd.NaT] * len(df), index=df.index)

@admin_bp.route("/users-analytics/analytics")
@admin_required
def users_analytics_analytics():
    """
    Render analytics page. Provide list of exams (exams.csv) for the filter.
    """
    try:
        service = create_drive_service()
        exams_df = load_csv_from_drive(service, EXAMS_FILE_ID)

        exams_list = []
        if exams_df is not None and not exams_df.empty:
            # prefer columns: id, name
            for _, e in exams_df.iterrows():
                exid = e.get('id', '')
                name = e.get('name') or e.get('title') or f"Exam {exid}"
                try:
                    exid_val = int(exid)
                except Exception:
                    exid_val = str(exid)
                exams_list.append({'id': exid_val, 'name': name})

        return render_template("admin/users_analytics_analytics.html", exams=exams_list)

    except Exception as exc:
        print("Error rendering analytics page")
        return render_template("admin/users_analytics_analytics.html", exams=[])


@admin_bp.route("/api/users-analytics/data")
@admin_required
def users_analytics_data_api():
    """
    JSON API for analytics.
    Query params:
      - timePeriod
      - exam  (exam id, optional)  <-- changed from subject
      - startDate, endDate (for custom)
    """
    try:
        service = create_drive_service()
        results_df = load_csv_from_drive(service, RESULTS_FILE_ID)
        users_df = load_csv_from_drive(service, USERS_FILE_ID)
        exams_df = load_csv_from_drive(service, EXAMS_FILE_ID)

        if results_df is None or results_df.empty:
            empty_payload = {
                "summary": {"avgScore": 0.0, "totalAttempts": 0, "passRate": 0.0, "activeUsers": 0,
                            "scoreChange":0.0,"attemptsChange":0.0,"passRateChange":0.0,"usersChange":0.0},
                "charts": {
                    "scoreDistribution":[0,0,0,0],
                    "performanceTrends":{"labels":[],"data":[]},
                    "examPerformance":{"labels":[],"data":[]},
                    "userActivity":{"labels":[],"data":[]}
                },
                "tables":{"topPerformers":[], "examStats":[], "recentActivity":[]}
            }
            return jsonify(empty_payload)

        # params
        time_period = (request.args.get('timePeriod') or 'all').lower()
        exam_filter = (request.args.get('exam') or '').strip()
        start_date = request.args.get('startDate') or ''
        end_date = request.args.get('endDate') or ''

        # attach parsed timestamp (results.csv uses completed_at)
        completed = _parse_datetime_series(results_df, ['completed_at', 'created_at', 'timestamp', 'submitted_at'])
        rd = results_df.copy()
        rd['_completed'] = completed

        # build exam map: id -> name (from exams_df)
        exam_name_map = {}
        if exams_df is not None and not exams_df.empty:
            for _, e in exams_df.iterrows():
                eid = str(e.get('id',''))
                en = e.get('name') or e.get('title') or f"Exam {eid}"
                exam_name_map[eid] = en

        # time window
        now = pd.Timestamp.now()
        start, end = None, None
        if time_period == 'today':
            start = now.normalize(); end = start + pd.Timedelta(days=1)
        elif time_period == 'week':
            start = (now - pd.Timedelta(days=now.weekday())).normalize(); end = start + pd.Timedelta(days=7)
        elif time_period == 'month':
            start = now.replace(day=1).normalize(); end = (start + pd.offsets.MonthBegin(1))
        elif time_period == 'quarter':
            q = (now.month - 1) // 3
            start = (now.replace(month=q*3+1, day=1)).normalize(); end = (start + pd.offsets.DateOffset(months=3))
        elif time_period == 'year':
            start = now.replace(month=1, day=1).normalize(); end = (start + pd.offsets.DateOffset(years=1))
        elif time_period == 'custom' and start_date and end_date:
            try:
                start = pd.to_datetime(start_date).normalize(); end = pd.to_datetime(end_date).normalize() + pd.Timedelta(days=1)
            except Exception:
                start, end = None, None

        mask = pd.Series([True] * len(rd), index=rd.index)
        if start is not None and end is not None:
            mask = mask & rd['_completed'].between(start, end, inclusive='left')

        # exam filter (exact match to results.exam_id)
        if exam_filter:
            # accept numeric or string exam ids
            mask = mask & (rd['exam_id'].astype(str) == str(exam_filter))

        filtered = rd[mask].copy()

        # compute row percentage (results.csv has 'percentage' column; fallback compute from score/max_score)
        def _extract_percentage(row):
            if 'percentage' in row and pd.notna(row['percentage']):
                try:
                    return float(row['percentage'])
                except Exception:
                    pass
            try:
                score = row.get('score')
                max_score = row.get('max_score') or row.get('max')
                if pd.notna(score) and pd.notna(max_score) and float(max_score) != 0:
                    return float(score) / float(max_score) * 100.0
            except Exception:
                pass
            return 0.0

        if not filtered.empty:
            filtered['_pct'] = filtered.apply(_extract_percentage, axis=1)
        else:
            filtered['_pct'] = pd.Series(dtype=float)

        total_attempts = int(len(filtered))
        avg_score = float(filtered['_pct'].mean()) if total_attempts > 0 else 0.0
        pass_threshold = 40.0
        pass_rate = float((filtered['_pct'] >= pass_threshold).sum() / total_attempts * 100.0) if total_attempts > 0 else 0.0
        active_users = int(filtered['student_id'].astype(str).nunique()) if 'student_id' in filtered.columns else 0

        summary = {"avgScore": round(avg_score,2), "totalAttempts": total_attempts, "passRate": round(pass_rate,2),
                   "activeUsers": active_users, "scoreChange":0.0,"attemptsChange":0.0,"passRateChange":0.0,"usersChange":0.0}

        # CHARTS
        buckets = {
            'excellent': int(((filtered['_pct'] >= 90)).sum()) if not filtered.empty else 0,
            'good': int((((filtered['_pct'] >= 75) & (filtered['_pct'] < 90))).sum()) if not filtered.empty else 0,
            'average': int((((filtered['_pct'] >= 60) & (filtered['_pct'] < 75))).sum()) if not filtered.empty else 0,
            'poor': int(((filtered['_pct'] < 60)).sum()) if not filtered.empty else 0
        }
        scoreDistribution = [buckets['excellent'], buckets['good'], buckets['average'], buckets['poor']]

        # performance trends grouped by day
        if not filtered.empty and filtered['_completed'].notna().any():
            tmp = filtered.dropna(subset=['_completed']).copy()
            tmp['_day'] = tmp['_completed'].dt.strftime('%Y-%m-%d')
            trend = tmp.groupby('_day')['_pct'].mean().reset_index().sort_values('_day')
            perf_labels = trend['_day'].tolist(); perf_data = [round(float(x),2) for x in trend['_pct'].tolist()]
        else:
            perf_labels = []; perf_data = []

        # exam performance (group by exam_id -> exam name)
        exam_perf_map = {}
        if not filtered.empty:
            for _, r in filtered.iterrows():
                exid = str(r.get('exam_id','') or r.get('exam',''))
                name = exam_name_map.get(exid, f"Exam {exid}")
                exam_perf_map.setdefault(name, []).append(r['_pct'])
            exam_labels = list(exam_perf_map.keys())
            exam_data = [round(float(pd.Series(vals).mean()),2) for vals in exam_perf_map.values()]
        else:
            exam_labels = []; exam_data = []

        # user activity per day
        if not filtered.empty and filtered['_completed'].notna().any():
            activity = filtered.dropna(subset=['_completed']).copy()
            activity['_day'] = activity['_completed'].dt.strftime('%Y-%m-%d')
            act = activity.groupby('_day').size().reset_index(name='count').sort_values('_day')
            act_labels = act['_day'].tolist(); act_data = [int(x) for x in act['count'].tolist()]
        else:
            act_labels = []; act_data = []

        charts_payload = {
            "scoreDistribution": scoreDistribution,
            "performanceTrends": {"labels": perf_labels, "data": perf_data},
            "examPerformance": {"labels": exam_labels, "data": exam_data},
            "userActivity": {"labels": act_labels, "data": act_data}
        }

        # TABLES
        top_performers = []
        if not filtered.empty and 'student_id' in filtered.columns:
            gp = filtered.groupby(filtered['student_id'].astype(str)).agg({'_pct':'mean','id':'count'}).rename(columns={'_pct':'avgPct','id':'attempts'}).reset_index()
            gp = gp.sort_values('avgPct', ascending=False).head(10)
            for _, row in gp.iterrows():
                sid = str(row['student_id']); attempts = int(row['attempts']); avgScoreVal = round(float(row['avgPct']),2)
                username = sid; full_name = ''
                if users_df is not None and not users_df.empty:
                    urow = users_df[users_df['id'].astype(str) == sid]
                    if not urow.empty:
                        username = str(urow.iloc[0].get('username') or urow.iloc[0].get('email') or sid)
                        full_name = str(urow.iloc[0].get('full_name') or '')
                top_performers.append({"student_id": sid, "username": username, "full_name": full_name, "avgScore": avgScoreVal, "attempts": attempts})

        exam_stats = []
        if not filtered.empty:
            exid_col = 'exam_id' if 'exam_id' in filtered.columns else 'exam'
            filtered['_exam_id_str'] = filtered[exid_col].astype(str) if exid_col in filtered.columns else filtered.get('exam_id', pd.Series(dtype=str)).astype(str)
            eg = filtered.groupby('_exam_id_str').agg({'_pct':'mean','id':'count'}).rename(columns={'_pct':'avgPct','id':'attempts'}).reset_index()
            for _, row in eg.iterrows():
                exid = str(row['_exam_id_str']); attempts = int(row['attempts']); avgScoreVal = round(float(row['avgPct']),2)
                exam_name = exam_name_map.get(exid, exid)
                ex_rows = filtered[filtered['_exam_id_str'] == exid]
                pass_rate_ex = float((ex_rows['_pct'] >= pass_threshold).sum() / len(ex_rows) * 100.0) if len(ex_rows) > 0 else 0.0
                exam_stats.append({"id": exid, "name": exam_name, "subject": "", "attempts": attempts, "avgScore": avgScoreVal, "passRate": round(pass_rate_ex,2)})

        recent_activity = []
        if not filtered.empty:
            tmp = filtered.copy().sort_values('_completed', ascending=False).head(10)
            for _, r in tmp.iterrows():
                sid = str(r.get('student_id','')); username = sid; full_name = ''
                if users_df is not None and not users_df.empty:
                    urow = users_df[users_df['id'].astype(str) == sid]
                    if not urow.empty:
                        username = str(urow.iloc[0].get('username') or urow.iloc[0].get('email') or sid)
                        full_name = str(urow.iloc[0].get('full_name') or '')
                exid = str(r.get('exam_id','') or r.get('exam',''))
                exam_name = exam_name_map.get(exid, exid)
                pct = float(r.get('_pct') or 0.0)
                recent_activity.append({"created_at": str(r.get('_completed')) if pd.notna(r.get('_completed')) else '', "username": username, "full_name": full_name, "exam_name": exam_name, "subject_name": "", "score": r.get('score') if 'score' in r.index else None, "max_score": r.get('max_score') if 'max_score' in r.index else None, "percentage": round(pct,2)})

        payload = {"summary": summary, "charts": charts_payload, "tables": {"topPerformers": top_performers, "examStats": exam_stats, "recentActivity": recent_activity}}
        return jsonify(payload)

    except Exception as exc:
        print("Analytics error")
        return jsonify({"error":"Failed to compute analytics","message":str(exc)}), 500        