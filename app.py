import os
import re
import uuid
import io
import json
import html as html_lib
import secrets
import sqlite3
import urllib.request
from datetime import datetime
from pathlib import Path
from typing import Iterable, List, Set, Optional
from flask import (
    Flask, render_template, request, redirect, url_for, jsonify, send_file,
    session, abort
)
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import literal, event, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import IntegrityError
from PIL import Image

app = Flask(__name__)

APP_VERSION = "v1.0.2"
UPDATE_REPO = os.environ.get("UPDATE_REPO", "KEV0143/Education-Rating-System")
UPDATE_CHECK_TIMEOUT = float(os.environ.get("UPDATE_CHECK_TIMEOUT", "3.0"))
UPDATE_USER_AGENT = "RatingSystemUpdateCheck"

UPDATE_INFO = {
    "available": False,
    "url": None,
    "release_url": None,
    "checked_at": None,
    "remote_version": None,
    "notes": None,
}


BASE_DIR = Path(__file__).resolve().parent
DB_DIR = BASE_DIR / "db"
DB_DIR.mkdir(exist_ok=True)
DB_PATH = DB_DIR / "RatingSystemKev.db"
DB_URI = f"sqlite:///{DB_PATH.as_posix()}"

app.config["SQLALCHEMY_DATABASE_URI"] = DB_URI
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False

app.config["UPLOAD_FOLDER"] = str(BASE_DIR / "static" / "uploads")
app.config["SECRET_KEY"] = os.environ.get("SECRET_KEY", secrets.token_hex(32))
app.config["MAX_CONTENT_LENGTH"] = 10 * 1024 * 1024

os.makedirs(app.config["UPLOAD_FOLDER"], exist_ok=True)

db = SQLAlchemy(app)


def _normalize_version(value: str) -> Optional[tuple]:
    if not value:
        return None
    nums = [int(x) for x in re.findall(r"\d+", value)]
    if not nums:
        return None
    while len(nums) < 3:
        nums.append(0)
    return tuple(nums[:3])


def _is_newer_version(remote: str, local: str) -> bool:
    r = _normalize_version(remote)
    l = _normalize_version(local)
    if not r or not l:
        return False
    return r > l


def _fetch_text(url: str, timeout: float) -> Optional[str]:
    req = urllib.request.Request(url, headers={"User-Agent": UPDATE_USER_AGENT})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.read().decode("utf-8", errors="replace")


def _fetch_json(url: str, timeout: float) -> Optional[dict]:
    headers = {
        "Accept": "application/vnd.github+json",
        "User-Agent": UPDATE_USER_AGENT,
    }
    token = os.environ.get("GITHUB_TOKEN")
    if token:
        headers["Authorization"] = f"Bearer {token}"

    req = urllib.request.Request(url, headers=headers)
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.load(resp)


def _html_to_text(raw_html: str) -> str:
    if not raw_html:
        return ""
    text = re.sub(r"(?i)<br\s*/?>", "\n", raw_html)
    text = re.sub(r"(?i)</p>", "\n\n", text)
    text = re.sub(r"(?i)</li>", "\n", text)
    text = re.sub(r"(?i)<li\b[^>]*>", "- ", text)
    text = re.sub(r"<[^>]+>", "", text)
    text = html_lib.unescape(text)
    return text.strip()


def _get_release_notes_from_html(tag: str) -> Optional[str]:
    try:
        url = f"https://github.com/{UPDATE_REPO}/releases/tag/{tag}"
        html = _fetch_text(url, UPDATE_CHECK_TIMEOUT)
    except Exception:
        return None

    match = re.search(r"<div class=\"markdown-body[^\"]*\"[^>]*>(.*?)</div>", html, re.S)
    if not match:
        return None

    notes = _html_to_text(match.group(1))
    return notes or None


def _select_release_download_url(release: dict) -> Optional[str]:
    assets = release.get("assets") or []
    preferred_exts = (".zip", ".exe", ".msi", ".dmg", ".deb", ".rpm")

    for ext in preferred_exts:
        for asset in assets:
            name = str(asset.get("name") or "").lower()
            if name.endswith(ext):
                url = asset.get("browser_download_url")
                if url:
                    return url

    for asset in assets:
        url = asset.get("browser_download_url")
        if url:
            return url

    return release.get("zipball_url") or release.get("html_url")


def _get_latest_release_fallback():
    url = f"https://github.com/{UPDATE_REPO}/releases/latest"
    req = urllib.request.Request(url, headers={"User-Agent": UPDATE_USER_AGENT})
    with urllib.request.urlopen(req, timeout=UPDATE_CHECK_TIMEOUT) as resp:
        final_url = resp.geturl()

    match = re.search(r"/releases/tag/([^/]+)", final_url)
    if not match:
        return None, None, None, None

    tag = match.group(1).strip()
    if not tag:
        return None, None, None, None
    zip_url = f"https://github.com/{UPDATE_REPO}/archive/refs/tags/{tag}.zip"
    release_url = final_url
    notes = _get_release_notes_from_html(tag)
    return tag, zip_url, release_url, notes


def _get_latest_release():
    try:
        release = _fetch_json(
            f"https://api.github.com/repos/{UPDATE_REPO}/releases/latest",
            UPDATE_CHECK_TIMEOUT,
        )
        if isinstance(release, dict):
            tag = (release.get("tag_name") or "").strip()
            if tag:
                download_url = _select_release_download_url(release)
                release_url = release.get("html_url")
                notes = (release.get("body") or "").strip() or None
                if download_url:
                    return tag, download_url, release_url, notes
    except Exception:
        pass

    return _get_latest_release_fallback()


def check_for_updates() -> None:
    global UPDATE_INFO
    UPDATE_INFO = {
        "available": False,
        "url": None,
        "release_url": None,
        "checked_at": datetime.utcnow(),
        "remote_version": None,
        "notes": None,
    }
    try:
        remote_version, download_url, release_url, notes = _get_latest_release()
        UPDATE_INFO["remote_version"] = remote_version
        UPDATE_INFO["release_url"] = release_url
        UPDATE_INFO["notes"] = notes
        UPDATE_INFO["checked_at"] = datetime.utcnow()
        if remote_version and download_url and _is_newer_version(remote_version, APP_VERSION):
            UPDATE_INFO["available"] = True
            UPDATE_INFO["url"] = download_url
    except Exception:
        UPDATE_INFO["checked_at"] = datetime.utcnow()

@event.listens_for(Engine, "connect")
def _set_sqlite_pragma(dbapi_connection, connection_record):
    try:
        if isinstance(dbapi_connection, sqlite3.Connection):
            cur = dbapi_connection.cursor()
            cur.execute("PRAGMA foreign_keys=ON;")
            cur.close()
    except Exception:
        pass


class Group(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(50), nullable=False, unique=True)

    students = db.relationship(
        "Student",
        backref="group",
        cascade="all, delete-orphan",
        passive_deletes=True
    )

    def to_dict(self):
        return {"id": self.id, "name": self.name}


class Student(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    fio = db.Column(db.String(150), nullable=False)
    group_id = db.Column(
        db.Integer,
        db.ForeignKey("group.id", ondelete="CASCADE"),
        nullable=False,
        index=True
    )

    def to_dict(self):
        return {"id": self.id, "fio": self.fio, "group_id": self.group_id}


class AppSetting(db.Model):
    key = db.Column(db.String(64), primary_key=True)
    value = db.Column(db.String(255), nullable=True)
    updated_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow)


class Course(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    title = db.Column(db.String(100), nullable=False)
    image_filename = db.Column(db.String(200), nullable=False, default="default.jpg")
    year = db.Column(db.String(20), nullable=False)
    semester = db.Column(db.Integer, nullable=False)
    group_ids = db.Column(db.String(200), nullable=False, default="")
    archived = db.Column(db.Boolean, nullable=False, default=False)

    def get_group_names(self):
        ids = parse_group_ids(self.group_ids)
        if not ids:
            return []
        groups = Group.query.filter(Group.id.in_(ids)).order_by(Group.name).all()
        return [g.name for g in groups]


@app.before_request
def _csrf_protect():
    if "csrf_token" not in session:
        session["csrf_token"] = secrets.token_urlsafe(32)

    if request.method in ("POST", "PUT", "PATCH", "DELETE"):
        token = request.headers.get("X-CSRFToken") or request.form.get("csrf_token")

        if token is None and request.is_json:
            payload = request.get_json(silent=True) or {}
            token = payload.get("csrf_token")

        if not token or token != session.get("csrf_token"):
            abort(400, description="CSRF token missing or invalid")


@app.context_processor
def _inject_csrf():
    return {
        "csrf_token": session.get("csrf_token", ""),
        "app_version": APP_VERSION,
        "update_remote_version": UPDATE_INFO.get("remote_version"),
        "update_available": UPDATE_INFO.get("available", False),
        "update_url": UPDATE_INFO.get("url"),
        "update_release_url": UPDATE_INFO.get("release_url"),
        "update_notes": UPDATE_INFO.get("notes"),
    }


@app.errorhandler(400)
def bad_request(e):
    if request.path.startswith("/api/"):
        return jsonify({"success": False, "error": str(e)}), 400
    return f"400 Bad Request: {e}", 400


def get_or_404(model, ident: int):
    obj = db.session.get(model, ident)
    if obj is None:
        abort(404)
    return obj


def parse_int(value: Optional[str], default: int = 0) -> int:
    try:
        return int(str(value).strip())
    except Exception:
        return default


def clean_student_name(raw_name: str) -> str:
    return re.sub(r"^\s*\d+[\.\)]\s*", "", (raw_name or "")).strip()


def get_setting(key: str, default: Optional[str] = None) -> Optional[str]:
    s = db.session.get(AppSetting, key)
    if s is None or s.value is None:
        return default
    return s.value


def set_setting(key: str, value: str) -> AppSetting:
    s = db.session.get(AppSetting, key)
    if s is None:
        s = AppSetting(key=key, value=value)
        db.session.add(s)
    else:
        s.value = value
    db.session.commit()
    return s


def parse_group_ids(group_ids_str: str) -> List[int]:
    if not group_ids_str:
        return []
    out: List[int] = []
    for x in str(group_ids_str).split(","):
        x = x.strip()
        if x.isdigit():
            out.append(int(x))
    return out


def normalize_group_ids(values: Iterable[str]) -> str:
    ids: List[int] = []
    for v in values:
        v = str(v).strip()
        if v.isdigit():
            ids.append(int(v))

    seen: Set[int] = set()
    uniq: List[int] = []
    for i in ids:
        if i not in seen:
            seen.add(i)
            uniq.append(i)

    return ",".join(str(i) for i in uniq)


def remove_group_id_from_csv(csv_ids: str, gid: int) -> str:
    ids = [x for x in parse_group_ids(csv_ids) if x != gid]
    return ",".join(str(i) for i in ids)


def _sqlite_columns(table: str) -> Set[str]:
    try:
        rows = db.session.execute(text(f"PRAGMA table_info({table})")).fetchall()
        return {str(r[1]) for r in rows}
    except Exception:
        return set()


def ensure_schema() -> None:
    cols = _sqlite_columns("course")
    if cols and "archived" not in cols:
        db.session.execute(text("ALTER TABLE course ADD COLUMN archived INTEGER NOT NULL DEFAULT 0"))

    cols_pg = _sqlite_columns("practice_grade")
    if cols_pg:
        if "score_updated_at" not in cols_pg:
            db.session.execute(text("ALTER TABLE practice_grade ADD COLUMN score_updated_at DATETIME"))

        cols_pg2 = _sqlite_columns("practice_grade")
        if "score_updated_at" in cols_pg2:
            db.session.execute(text(
                "UPDATE practice_grade SET score_updated_at = updated_at "
                "WHERE score_updated_at IS NULL AND score IS NOT NULL"
            ))

        if "comment_updated_at" not in cols_pg2:
            db.session.execute(text("ALTER TABLE practice_grade ADD COLUMN comment_updated_at DATETIME"))

        cols_pg3 = _sqlite_columns("practice_grade")
        if "comment_updated_at" in cols_pg3:
            db.session.execute(text(
                "UPDATE practice_grade SET comment_updated_at = updated_at "
                "WHERE comment_updated_at IS NULL AND COALESCE(comment,'') <> ''"
            ))

    db.session.commit()


def save_image(file) -> Optional[str]:
    try:
        ext = file.filename.rsplit(".", 1)[1].lower() if "." in file.filename else "jpg"
        if ext not in {"jpg", "jpeg", "png", "webp"}:
            ext = "jpg"

        filename = f"{uuid.uuid4()}.{ext}"
        filepath = os.path.join(app.config["UPLOAD_FOLDER"], filename)

        img = Image.open(file)
        img.verify()
        file.stream.seek(0)

        img = Image.open(file).convert("RGB")
        if img.width > 800:
            ratio = 800 / img.width
            new_height = max(1, int(img.height * ratio))
            img = img.resize((800, new_height), Image.Resampling.LANCZOS)

        img.save(filepath, quality=85, optimize=True)
        return filename
    except Exception:
        return None


def course_query_filter_by_group(query, group_id_str: Optional[str]):
    gid = parse_int(group_id_str, default=0)
    if gid <= 0:
        return query
    pattern = f"%,{gid},%"
    return query.filter((literal(",") + Course.group_ids + literal(",")).like(pattern))


@app.route("/")
def index():
    search = (request.args.get("search") or "").strip()
    greeting_name = get_setting("greeting_name")
    f_year = (request.args.get("year") or "").strip() or None
    f_sem = (request.args.get("semester") or "").strip() or None
    f_group_id = (request.args.get("group_id") or "").strip() or None

    selected_sort = (request.args.get("sort") or "").strip() or "title_asc"

    sort_options = {
        "title_asc": ("Название (А–Я)", [Course.title.asc()]),
        "title_desc": ("Название (Я–А)", [Course.title.desc()]),
        "year_desc": ("Год (новые)", [Course.year.desc(), Course.title.asc()]),
        "year_asc": ("Год (старые)", [Course.year.asc(), Course.title.asc()]),
        "semester_desc": ("Семестр (убыв.)", [Course.semester.desc(), Course.title.asc()]),
        "semester_asc": ("Семестр (возр.)", [Course.semester.asc(), Course.title.asc()]),
    }
    sort_label, sort_order = sort_options.get(selected_sort, sort_options["title_asc"])

    years = [
        y[0] for y in db.session.query(Course.year)
        .filter(Course.archived.is_(False))
        .distinct()
        .order_by(Course.year)
        .all()
    ]

    groups = Group.query.order_by(Group.name).all()

    results = []
    added_course_ids: Set[int] = set()

    if search:
        search_lower = search.lower()
        courses = Course.query.filter(Course.archived.is_(False)).all()
        course_gid_map = {c.id: set(parse_group_ids(c.group_ids)) for c in courses}

        for s in Student.query.all():
            if search_lower in s.fio.lower():
                for c in courses:
                    if s.group_id in course_gid_map.get(c.id, set()) and c.id not in added_course_ids:
                        results.append({"course": c, "reason": f"Студент: {s.fio} ({s.group.name})"})
                        added_course_ids.add(c.id)

        for g in groups:
            if search_lower in g.name.lower():
                for c in courses:
                    if g.id in course_gid_map.get(c.id, set()) and c.id not in added_course_ids:
                        results.append({"course": c, "reason": f"Группа: {g.name}"})
                        added_course_ids.add(c.id)

        for c in courses:
            if search_lower in c.title.lower() and c.id not in added_course_ids:
                results.append({"course": c, "reason": None})
                added_course_ids.add(c.id)
    else:
        query = Course.query.filter(Course.archived.is_(False))
        if f_year:
            query = query.filter_by(year=f_year)
        if f_sem:
            query = query.filter_by(semester=parse_int(f_sem, default=0))
        if f_group_id:
            query = course_query_filter_by_group(query, f_group_id)

        for c in query.order_by(*sort_order).all():
            results.append({"course": c, "reason": None})

    archived_courses = Course.query.filter(Course.archived.is_(True)).order_by(Course.year.desc(), Course.semester.desc(), Course.title.asc()).all()

    return render_template(
        "index.html",
        results=results,
        years=years,
        groups=groups,
        search=search,
        selected_year=f_year,
        selected_sem=f_sem,
        selected_group_id=f_group_id,
        selected_sort=selected_sort,
        sort_label=sort_label,
        archived_courses=archived_courses,
        greeting_name=greeting_name,
    )


@app.get("/api/greeting")
def api_get_greeting():
    name = get_setting("greeting_name")
    return jsonify({"success": True, "name": name})


@app.post("/api/greeting")
def api_set_greeting():
    data = request.get_json(silent=True) or {}
    name = (data.get("name") or "").strip()
    if not name:
        abort(400, description="Name is required")
    if len(name) > 80:
        name = name[:80]

    set_setting("greeting_name", name)
    return jsonify({"success": True, "name": name})


@app.route("/placeholder")
def placeholder():
    img = Image.new("RGB", (250, 160), color="#f1f3f5")
    img_io = io.BytesIO()
    img.save(img_io, "JPEG")
    img_io.seek(0)
    return send_file(img_io, mimetype="image/jpeg")


@app.route("/add_course", methods=["POST"])
def add_course():
    title = (request.form.get("title") or "").strip()
    year = (request.form.get("year") or "").strip()
    semester = parse_int(request.form.get("semester"), default=1)
    group_ids = normalize_group_ids(request.form.getlist("groups"))

    if not title:
        abort(400, description="Title is required")
    if not year:
        year = "2024-2025"
    if semester not in range(1, 11):
        semester = 1

    filename = "default.jpg"
    file = request.files.get("image")
    if file and file.filename:
        res = save_image(file)
        if res:
            filename = res

    db.session.add(
        Course(
            title=title,
            year=year,
            semester=semester,
            group_ids=group_ids,
            image_filename=filename,
        )
    )
    db.session.commit()
    return redirect(url_for("index"))


@app.route("/edit_course/<int:course_id>", methods=["POST"])
def edit_course(course_id: int):
    c = get_or_404(Course, course_id)

    title = (request.form.get("title") or "").strip()
    year = (request.form.get("year") or "").strip()
    semester = parse_int(request.form.get("semester"), default=c.semester)
    group_ids = normalize_group_ids(request.form.getlist("groups"))

    if title:
        c.title = title
    if year:
        c.year = year
    if semester in range(1, 11):
        c.semester = semester

    c.group_ids = group_ids

    file = request.files.get("image")
    if file and file.filename:
        res = save_image(file)
        if res:
            c.image_filename = res

    db.session.commit()
    return redirect(url_for("index"))


@app.route("/delete_course/<int:course_id>", methods=["POST"])
def delete_course(course_id: int):
    c = get_or_404(Course, course_id)
    db.session.delete(c)
    db.session.commit()
    return redirect(url_for("index"))


@app.route("/archive_course/<int:course_id>", methods=["POST"])
def archive_course(course_id: int):
    c = get_or_404(Course, course_id)
    c.archived = True
    db.session.commit()
    return redirect(url_for("index"))


@app.route("/unarchive_course/<int:course_id>", methods=["POST"])
def unarchive_course(course_id: int):
    c = get_or_404(Course, course_id)
    c.archived = False
    db.session.commit()
    return redirect(url_for("index"))


@app.get("/api/groups")
def api_groups():
    groups = Group.query.order_by(Group.name).all()
    return jsonify({"success": True, "groups": [g.to_dict() for g in groups]})


@app.route("/api/create_group", methods=["POST"])
def api_create_group():
    data = request.get_json(silent=True) or {}
    name = (data.get("name") or "").strip()
    if not name:
        return jsonify({"success": False, "error": "Name is required"}), 400

    g = Group(name=name)
    db.session.add(g)
    try:
        db.session.commit()
        return jsonify({"success": True, "group": g.to_dict()})
    except IntegrityError:
        db.session.rollback()
        return jsonify({"success": False, "error": "Group already exists"}), 409


@app.route("/api/rename_group", methods=["POST"])
def api_rename_group():
    data = request.get_json(silent=True) or {}
    gid = parse_int(data.get("id"), default=0)
    name = (data.get("name") or "").strip()
    if gid <= 0 or not name:
        return jsonify({"success": False, "error": "Invalid id or name"}), 400

    g = db.session.get(Group, gid)
    if not g:
        return jsonify({"success": False, "error": "Group not found"}), 404

    g.name = name
    try:
        db.session.commit()
        return jsonify({"success": True, "group": g.to_dict()})
    except IntegrityError:
        db.session.rollback()
        return jsonify({"success": False, "error": "Group name must be unique"}), 409


@app.route("/api/delete_group/<int:group_id>", methods=["POST"])
def api_delete_group(group_id: int):
    g = db.session.get(Group, group_id)
    if not g:
        return jsonify({"success": False, "error": "Group not found"}), 404

    touched = 0
    courses = Course.query.all()
    for c in courses:
        if not c.group_ids:
            continue
        old = c.group_ids
        new = remove_group_id_from_csv(old, group_id)
        if new != old:
            c.group_ids = new
            touched += 1

    db.session.delete(g)
    db.session.commit()
    return jsonify({"success": True, "courses_updated": touched})


@app.route("/api/get_students/<int:group_id>")
def api_get_students(group_id: int):
    return jsonify(
        [s.to_dict() for s in Student.query.filter_by(group_id=group_id).order_by(Student.fio).all()]
    )


@app.route("/api/add_students_bulk", methods=["POST"])
def api_add_students_bulk():
    data = request.get_json(silent=True) or {}
    group_id = parse_int(data.get("group_id"), default=0)
    text = data.get("text") or ""

    if group_id <= 0:
        return jsonify({"success": False, "error": "Invalid group_id"}), 400

    g = db.session.get(Group, group_id)
    if not g:
        return jsonify({"success": False, "error": "Group not found"}), 404

    if not str(text).strip():
        return jsonify({"success": True, "added": 0})

    added = 0
    for raw in str(text).splitlines():
        fio = clean_student_name(raw)
        if not fio:
            continue

        exists = Student.query.filter_by(fio=fio, group_id=group_id).first()
        if exists:
            continue

        db.session.add(Student(fio=fio, group_id=group_id))
        added += 1

    db.session.commit()
    return jsonify({"success": True, "added": added})


@app.route("/api/delete_student/<int:student_id>", methods=["POST"])
def api_delete_student(student_id: int):
    s = get_or_404(Student, student_id)
    db.session.delete(s)
    db.session.commit()
    return jsonify({"success": True})


@app.route("/api/update_student/<int:student_id>", methods=["POST"])
def api_update_student(student_id: int):
    s = get_or_404(Student, student_id)
    data = request.get_json(silent=True) or {}

    fio = (data.get("fio") or "").strip()
    fio = re.sub(r"\s+", " ", fio)
    if not fio:
        return jsonify({"success": False, "error": "FIO is required"}), 400
    if len(fio) > 150:
        fio = fio[:150]

    # Защита от дубликатов внутри группы
    exists = Student.query.filter(
        Student.group_id == s.group_id,
        Student.fio == fio,
        Student.id != s.id
    ).first()
    if exists:
        return jsonify({"success": False, "error": "Такой студент уже есть в группе"}), 409

    s.fio = fio
    db.session.commit()
    return jsonify({"success": True, "student": s.to_dict()})


from utils.practice_models import init_practice_models
Practice, PracticeGrade = init_practice_models(db)

with app.app_context():
    db.create_all()
    ensure_schema()

from utils.course_routes import register_course_routes
register_course_routes(app, db, Course, Group, Student, Practice, PracticeGrade, parse_group_ids)

from utils.practice_routes import register_practice_routes
register_practice_routes(app, db, Course, Group, Student, Practice, PracticeGrade, parse_group_ids)

from utils.excel_export import register_excel_export_routes
register_excel_export_routes(app, db, Course, Group, Student, Practice, PracticeGrade, parse_group_ids)


if __name__ == "__main__":
    check_for_updates()
    app.run(debug=True)
