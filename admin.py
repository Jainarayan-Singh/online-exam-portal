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

from google_drive_service import (
    create_drive_service,         # SA (read/write CSV)
    create_subject_folder,
    load_csv_from_drive,
    save_csv_to_drive,
    clear_cache,
    find_file_by_name,
    get_drive_service_for_upload  # USER OAUTH (token.json) — for image uploads & folder ops
)

# ==========
# Blueprint
# ==========
admin_bp = Blueprint("admin", __name__, url_prefix="/admin", template_folder="templates")

# ==========
# Config
# ==========
USERS_FILE_ID     = os.environ.get("USERS_FILE_ID")
EXAMS_FILE_ID     = os.environ.get("EXAMS_FILE_ID")
QUESTIONS_FILE_ID = os.environ.get("QUESTIONS_FILE_ID")
SUBJECTS_FILE_ID  = os.environ.get("SUBJECTS_FILE_ID")

UPLOAD_TMP_DIR = os.path.join(os.path.dirname(__file__), "uploads_tmp")
os.makedirs(UPLOAD_TMP_DIR, exist_ok=True)

ALLOWED_IMAGE_EXTS = {".png", ".jpg", ".jpeg", ".gif", ".webp", ".bmp"}
MAX_FILE_SIZE_MB = 15

# ==========
# Helpers
# ==========
def admin_required(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        if "admin_id" not in session:
            flash("Admin login required.", "warning")
            return redirect(url_for("admin.login"))
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

# ==========
# Auth
# ==========
@admin_bp.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        username_or_email = request.form["username"].strip().lower()
        password = request.form["password"]

        service = create_drive_service()
        users_df = load_csv_from_drive(service, USERS_FILE_ID)
        if users_df.empty:
            flash("No users found.", "danger")
            return redirect(url_for("admin.login"))

        user = users_df[
            ((users_df["email"].astype(str).str.lower() == username_or_email) |
             (users_df["username"].astype(str).str.lower() == username_or_email)) &
            (users_df["password"].astype(str) == str(password))
        ]
        if not user.empty and "admin" in str(user.iloc[0].get("role", "")).lower():
            session["admin_id"] = int(user.iloc[0]["id"])
            session["admin_name"] = user.iloc[0].get("full_name") or user.iloc[0].get("username")
            session["role"] = user.iloc[0].get("role")
            flash("Welcome Admin!", "success")
            return redirect(url_for("admin.dashboard"))

        flash("Invalid credentials or not an admin.", "danger")
    return render_template("admin/admin_login.html")

@admin_bp.route("/logout")
def logout():
    session.pop("admin_id", None)
    session.pop("admin_name", None)
    session.pop("role", None)
    flash("Logged out successfully.", "info")
    return redirect(url_for("admin.login"))

# ==========
# Dashboard
# ==========
# ========== 
# Dashboard
# ==========
@admin_bp.route("/dashboard")
@admin_required
def dashboard():
    sa = create_drive_service()

    exams_df = load_csv_from_drive(sa, EXAMS_FILE_ID)
    users_df = load_csv_from_drive(sa, USERS_FILE_ID)

    # Safe lengths
    total_exams = 0 if exams_df is None or exams_df.empty else len(exams_df)
    total_users = 0 if users_df is None or users_df.empty else len(users_df)

    # IMPORTANT: exact match 'admin' (ignores strings like "not admin")
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


# ==========
# Subjects
# ==========
@admin_bp.route("/subjects", methods=["GET", "POST"])
@admin_required
def subjects():
    # SA for CSV work
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

        # IMPORTANT: create the Drive folder with the USER (owner) client
        try:
            drive_owner = get_drive_service_for_upload()
        except Exception as e:
            flash(f"Cannot create folder: {e}", "danger")
            return redirect(url_for("admin.subjects"))

        # Create subject folder under IMAGES_FOLDER_ID/ROOT_FOLDER_ID
        folder_id, created_at = create_subject_folder(drive_owner, subject_name)

        new_id = 1 if subjects_df.empty else int(subjects_df["id"].max()) + 1
        new_row = pd.DataFrame([{
            "id": new_id,
            "subject_name": subject_name,
            "subject_folder_id": folder_id,
            "subject_folder_created_at": created_at
        }])
        updated_df = pd.concat([subjects_df, new_row], ignore_index=True)
        save_csv_to_drive(sa, updated_df, SUBJECTS_FILE_ID)
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

    # Rename with USER client (owner)
    try:
        drive_owner = get_drive_service_for_upload()
        drive_owner.files().update(fileId=folder_id, body={"name": new_name}).execute()
    except Exception as e:
        print(f"⚠️ rename folder failed: {e}")
        flash("Drive folder rename failed; CSV updated.", "warning")

    subjects_df.loc[subjects_df["id"] == subject_id, "subject_name"] = new_name
    save_csv_to_drive(sa, subjects_df, SUBJECTS_FILE_ID)
    clear_cache()
    flash("Subject updated successfully.", "success")
    return redirect(url_for("admin.subjects"))

@admin_bp.route("/subjects/delete/<int:subject_id>")
@admin_required
def delete_subject(subject_id):
    service = create_drive_service()

    # Always load fresh, then coerce id column to integer for safe matching
    subjects_df = load_csv_from_drive(service, SUBJECTS_FILE_ID)
    if subjects_df is None or subjects_df.empty:
        flash("No subjects found.", "warning")
        return redirect(url_for("admin.subjects"))

    # Normalize id dtype → Int64 (nullable int)
    if "id" not in subjects_df.columns:
        flash("Subjects file is missing 'id' column.", "danger")
        return redirect(url_for("admin.subjects"))
    working_df = subjects_df.copy()
    working_df["id"] = pd.to_numeric(working_df["id"], errors="coerce").astype("Int64")

    # Locate the row to delete
    hit = working_df[working_df["id"] == int(subject_id)]
    if hit.empty:
        flash("Subject not found.", "danger")
        return redirect(url_for("admin.subjects"))

    # Determine folder id column name
    folder_id_col = "subject_folder_id" if "subject_folder_id" in working_df.columns else "folder_id"
    folder_id = str(hit.iloc[0].get(folder_id_col, "")).strip()

    if folder_id:
        # First: try using the USER OAUTH (owner) client - this usually has permission
        try:
            drive_owner = get_drive_service_for_upload()
            try:
                # Try delete (works if we have permission)
                drive_owner.files().delete(fileId=folder_id, supportsAllDrives=True).execute()
                print(f"✅ Deleted folder {folder_id} using owner OAuth client.")
            except Exception as e_del:
                # If delete fails, try moving to trash (safer fallback)
                print(f"⚠ Owner delete failed for {folder_id}: {e_del} — trying to trash it instead.")
                try:
                    drive_owner.files().update(fileId=folder_id, body={"trashed": True}, supportsAllDrives=True).execute()
                    print(f"♻ Trashed folder {folder_id} using owner OAuth client.")
                except Exception as e_trash:
                    print(f"❌ Failed to trash folder {folder_id} with owner client: {e_trash}")
        except Exception as e_owner:
            # Could not create owner client (token missing/invalid) — try SA as fallback
            print(f"⚠ get_drive_service_for_upload() failed: {e_owner}. Trying service-account client as fallback.")
            try:
                service.files().delete(fileId=folder_id, supportsAllDrives=True).execute()
                print(f"✅ Deleted folder {folder_id} using service-account client (fallback).")
            except Exception as e_sa:
                print(f"❌ Fallback SA delete also failed for {folder_id}: {e_sa}")
                # Do not stop — we'll still remove CSV row below so UI remains consistent

    # Now drop the row from the CSV using normalized ids
    new_df = working_df[working_df["id"] != int(subject_id)].copy()

    # Save back to Drive and clear caches
    ok = save_csv_to_drive(service, new_df, SUBJECTS_FILE_ID)
    if ok:
        clear_cache()
        flash("Subject deleted (Drive folder removed if permitted).", "info")
    else:
        flash("Failed to update subjects.csv after delete.", "danger")

    return redirect(url_for("admin.subjects"))


# ==========
# Exams
# ==========
@admin_bp.route("/exams", methods=["GET", "POST"])
@admin_required
def exams():
    service = create_drive_service()
    exams_df = load_csv_from_drive(service, EXAMS_FILE_ID)
    if request.method == "POST":
        form = request.form
        new_id = exams_df["id"].max() + 1 if not exams_df.empty else 1
        exams_df.loc[len(exams_df)] = [
            new_id,
            form["name"],
            form["date"],
            form["start_time"],
            int(form["duration"]),
            int(form["total_questions"]),
            form["status"],
            form["instructions"],
            form["positive_marks"],
            form["negative_marks"]
        ]
        save_csv_to_drive(service, exams_df, EXAMS_FILE_ID)
        flash("Exam created successfully.", "success")
        return redirect(url_for("admin.exams"))
    return render_template("admin/exams.html", exams=exams_df.to_dict(orient="records"))

@admin_bp.route("/exams/edit/<int:exam_id>", methods=["GET", "POST"])
@admin_required
def edit_exam(exam_id):
    service = create_drive_service()
    exams_df = load_csv_from_drive(service, EXAMS_FILE_ID)
    exam = exams_df[exams_df["id"] == exam_id]
    if exam.empty:
        flash("Exam not found.", "danger")
        return redirect(url_for("admin.exams"))

    if request.method == "POST":
        form = request.form
        exams_df.loc[exams_df["id"] == exam_id, [
            "name", "date", "start_time", "duration",
            "total_questions", "status",
            "instructions", "positive_marks", "negative_marks"
        ]] = [
            form["name"],
            form["date"],
            form["start_time"],
            int(form["duration"]),
            int(form["total_questions"]),
            form["status"],
            form["instructions"],
            form["positive_marks"],
            form["negative_marks"]
        ]
        save_csv_to_drive(service, exams_df, EXAMS_FILE_ID)
        flash("Exam updated successfully.", "success")
        return redirect(url_for("admin.exams"))
    return render_template("admin/edit_exam.html", exam=exam.iloc[0].to_dict())

@admin_bp.route("/exams/delete/<int:exam_id>")
@admin_required
def delete_exam(exam_id):
    service = create_drive_service()
    exams_df = load_csv_from_drive(service, EXAMS_FILE_ID)
    exams_df = exams_df[exams_df["id"] != exam_id]
    save_csv_to_drive(service, exams_df, EXAMS_FILE_ID)
    flash("Exam deleted.", "info")
    return redirect(url_for("admin.exams"))

# ==========
# Questions (unchanged for now)
# ==========
@admin_bp.route("/questions/<int:exam_id>", methods=["GET", "POST"])
@admin_required
def questions(exam_id):
    service = create_drive_service()
    questions_df = load_csv_from_drive(service, QUESTIONS_FILE_ID)
    exam_questions = questions_df[questions_df["exam_id"] == exam_id]
    if request.method == "POST":
        form = request.form
        new_id = questions_df["id"].max() + 1 if not questions_df.empty else 1
        questions_df.loc[len(questions_df)] = [
            new_id,
            exam_id,
            form["question_text"],
            form.get("option_a", ""),
            form.get("option_b", ""),
            form.get("option_c", ""),
            form.get("option_d", ""),
            form.get("correct_answer", ""),
            form["question_type"],
            form.get("image_path", ""),
            form.get("positive_marks", "4"),
            form.get("negative_marks", "1"),
            form.get("tolerance", "")
        ]
        save_csv_to_drive(service, questions_df, QUESTIONS_FILE_ID)
        flash("Question added successfully.", "success")
        return redirect(url_for("admin.questions", exam_id=exam_id))
    return render_template("admin/questions.html",
                           questions=exam_questions.to_dict(orient="records"),
                           exam_id=exam_id)



# -----------------------
# New: delete multiple questions
# -----------------------
@admin_bp.route("/questions/delete-multiple", methods=["POST"])
@admin_required
def delete_multiple_questions():
    """
    Accepts JSON: { "ids": [1,2,3] }
    Returns JSON: { success: True, deleted: N }
    """
    try:
        payload = request.get_json(force=True)
        if not payload or "ids" not in payload:
            return jsonify({"success": False, "message": "Invalid payload"}), 400

        ids = payload.get("ids") or []
        if not isinstance(ids, list) or not ids:
            return jsonify({"success": False, "message": "No IDs provided"}), 400

        # Normalise to strings for safe comparison
        ids_str = set([str(int(i)) for i in ids if str(i).strip()])

        sa = create_drive_service()
        qdf = load_csv_from_drive(sa, QUESTIONS_FILE_ID)
        qdf = _ensure_questions_df(qdf)

        before_count = len(qdf)
        # filter out rows whose id in ids_str
        new_df = qdf[~qdf["id"].astype(str).isin(ids_str)].copy()
        after_count = len(new_df)
        deleted_count = before_count - after_count

        ok = save_csv_to_drive(sa, new_df, QUESTIONS_FILE_ID)
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
    """
    Accepts JSON:
    {
      "exam_id": 1,
      "question_type": "MCQ",
      "positive_marks": "4",     # optional (string)
      "negative_marks": "1",     # optional (string)
      "tolerance": "0.5"         # optional (string). If present it will be set (can be empty string to clear)
    }
    Updates matching rows and returns {"success": True, "updated": N}
    """
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

        # Normalise inputs
        pos_str = None if pos is None else str(pos).strip()
        neg_str = None if neg is None else str(neg).strip()
        tol_str = None if tol is None else str(tol)

        sa = create_drive_service()
        qdf = load_csv_from_drive(sa, QUESTIONS_FILE_ID)
        qdf = _ensure_questions_df(qdf)

        # create mask for exam_id and question_type (case-insensitive)
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
            # If tolerance key is present in payload, set it (even empty string to clear)
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





# ==========
# Upload Images (GET -> page, POST -> JSON upload)
# ==========
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

            # USER OAUTH client for uploads (token.json)
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

                # size check
                f.seek(0, os.SEEK_END)
                size_mb = f.tell() / (1024 * 1024)
                f.seek(0)
                if size_mb > MAX_FILE_SIZE_MB:
                    failed.append({"filename": safe_name, "error": f"Exceeds {MAX_FILE_SIZE_MB} MB"})
                    continue

                # save temp (use unique name to avoid collisions)
                temp_path = os.path.join(UPLOAD_TMP_DIR, safe_name)
                f.save(temp_path)

                # We'll open the file and pass the file-object to MediaIoBaseUpload,
                # and make sure to close the file-object in finally so Windows lock is released.
                fh = None
                try:
                    existing_id = find_file_by_name(drive_upload, safe_name, folder_id)
                    mime, _ = mimetypes.guess_type(safe_name)
                    # open file in binary mode
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
                    # ensure file handle is closed before we try to remove the temp file
                    try:
                        if fh and not fh.closed:
                            fh.close()
                    except Exception as _close_err:
                        print(f"⚠ Could not close temp file handle for {temp_path}: {_close_err}")

                    # now try to remove the temp file (best-effort)
                    try:
                        if os.path.exists(temp_path):
                            os.remove(temp_path)
                    except Exception as rm_err:
                        # Windows may transiently keep file locked by antivirus or indexing — log it
                        print(f"⚠ Could not remove temp file {temp_path}: {rm_err}")

            return jsonify({"success": True, "uploaded": uploaded, "failed": failed}), 200

        except Exception as e:
            return jsonify({"success": False, "message": f"Unexpected error: {str(e)}"}), 500

    # GET -> render page (uses SA to read CSV)
    sa = create_drive_service()
    subjects = _get_subject_folders(sa)
    load_error = None if subjects else "No subjects found (or subjects.csv missing)."
    return render_template(
        "admin/upload_images.html",
        subjects=subjects,
        load_error=load_error
    )


# ---------- QUESTIONS CRUD & Batch Add (paste into admin.py) ----------


# Columns canonical order
QUESTIONS_COLUMNS = [
    "id", "exam_id", "question_text", "option_a", "option_b", "option_c", "option_d",
    "correct_answer", "question_type", "image_path", "positive_marks", "negative_marks", "tolerance"
]

def _ensure_questions_df(df):
    """Return a DataFrame guaranteed to have QUESTIONS_COLUMNS in order."""
    if df is None or df.empty:
        return pd.DataFrame(columns=QUESTIONS_COLUMNS)
    # ensure all columns present
    for c in QUESTIONS_COLUMNS:
        if c not in df.columns:
            df[c] = ""
    # reorder to canonical order
    return df[QUESTIONS_COLUMNS].copy()

@admin_bp.route("/questions", methods=["GET"])
@admin_required
def questions_index():
    """
    List questions for a selected exam. Query param: ?exam_id=#
    Endpoint name: admin.questions_index
    Renders: templates/admin/questions.html
    """
    sa = create_drive_service()
    exams_df = load_csv_from_drive(sa, EXAMS_FILE_ID)
    exams = []
    if not exams_df.empty:
        for _, r in exams_df.iterrows():
            exams.append({
                "id": int(r.get("id")) if "id" in exams_df.columns and str(r.get("id")).strip() else None,
                "name": r.get("name") if "name" in exams_df.columns else f"Exam {r.get('id')}"
            })

    # choose selected exam from query or default to first exam id
    selected_exam_id = request.args.get("exam_id", type=int)
    if not selected_exam_id and exams:
        selected_exam_id = exams[0]["id"]

    # load questions
    questions_df = load_csv_from_drive(sa, QUESTIONS_FILE_ID)
    questions_df = _ensure_questions_df(questions_df)

    # filter by exam
    if selected_exam_id:
        filtered = questions_df[questions_df["exam_id"].astype(str) == str(selected_exam_id)]
    else:
        filtered = questions_df.copy()

    # prepare list for template
    questions = []
    for _, r in filtered.iterrows():
        questions.append({
            "id": int(r["id"]) if str(r["id"]).strip() else None,
            "exam_id": int(r["exam_id"]) if str(r["exam_id"]).strip() else None,
            "question_text": r.get("question_text", ""),
            "option_a": r.get("option_a", ""),
            "option_b": r.get("option_b", ""),
            "option_c": r.get("option_c", ""),
            "option_d": r.get("option_d", ""),
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
    """
    Add single question (form).
    Endpoint name: admin.add_question
    Renders: templates/admin/add_question.html
    """
    sa = create_drive_service()
    exams_df = load_csv_from_drive(sa, EXAMS_FILE_ID)
    exams = []
    if not exams_df.empty:
        for _, r in exams_df.iterrows():
            exams.append({"id": int(r.get("id")) if "id" in exams_df.columns and str(r.get("id")).strip() else None,
                          "name": r.get("name") if "name" in exams_df.columns else f"Exam {r.get('id')}"})

    if request.method == "POST":
        # load existing questions
        qdf = load_csv_from_drive(sa, QUESTIONS_FILE_ID)
        qdf = _ensure_questions_df(qdf)

        # determine next id
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
        ok = save_csv_to_drive(sa, new_df, QUESTIONS_FILE_ID)
        if ok:
            clear_cache()
            flash("Question added successfully.", "success")
            return redirect(url_for("admin.questions_index", exam_id=new_row["exam_id"]))
        else:
            flash("Failed to save question.", "danger")
            return redirect(url_for("admin.add_question"))

    # GET -> render form
    return render_template("admin/add_question.html", exams=exams, question=None, form_mode="add")

@admin_bp.route("/questions/edit/<int:question_id>", methods=["GET", "POST"])
@admin_required
def edit_question(question_id):
    """
    Edit single question
    Endpoint name: admin.edit_question
    Renders: templates/admin/edit_question.html
    """
    sa = create_drive_service()
    exams_df = load_csv_from_drive(sa, EXAMS_FILE_ID)
    exams = []
    if not exams_df.empty:
        for _, r in exams_df.iterrows():
            exams.append({"id": int(r.get("id")) if "id" in exams_df.columns and str(r.get("id")).strip() else None,
                          "name": r.get("name") if "name" in exams_df.columns else f"Exam {r.get('id')}"})

    qdf = load_csv_from_drive(sa, QUESTIONS_FILE_ID)
    qdf = _ensure_questions_df(qdf)

    # find question row
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

    # GET -> provide question dict to template
    qrow = hit.iloc[0].to_dict()
    return render_template("admin/edit_question.html", exams=exams, question=qrow, form_mode="edit")

@admin_bp.route("/questions/delete/<int:question_id>", methods=["POST"])
@admin_required
def delete_question(question_id):
    """
    Delete question (POST).
    Endpoint name: admin.delete_question
    """
    sa = create_drive_service()
    qdf = load_csv_from_drive(sa, QUESTIONS_FILE_ID)
    qdf = _ensure_questions_df(qdf)

    # drop rows where id matches
    new_df = qdf[qdf["id"].astype(str) != str(question_id)].copy()

    ok = save_csv_to_drive(sa, new_df, QUESTIONS_FILE_ID)
    if ok:
        clear_cache()
        flash("Question deleted.", "info")
    else:
        flash("Failed to delete question.", "danger")
    return redirect(url_for("admin.questions_index"))

@admin_bp.route("/questions/batch-add", methods=["POST"])
@admin_required
def questions_batch_add():
    """
    Accepts JSON:
    { "exam_id": 1, "questions": [ {question_text, option_a, ...}, ... ] }
    Returns JSON: { success: True, added: N }
    Endpoint name: admin.questions_batch_add
    """
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

        # compute next id
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
        ok = save_csv_to_drive(sa, appended, QUESTIONS_FILE_ID)
        if not ok:
            return jsonify({"success": False, "message": "Failed to save to Drive"}), 500

        clear_cache()
        return jsonify({"success": True, "added": added_count})

    except Exception as e:
        print(f"❌ questions_batch_add error: {e}")
        return jsonify({"success": False, "message": str(e)}), 500

# ------------------------------------------------------------------------
# End of Questions routes
# ------------------------------------------------------------------------


# ==========
# Publish
# ==========
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
