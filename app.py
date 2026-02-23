import os
import secrets
import threading
import webbrowser

from flask import Flask, redirect, url_for
from utils.course_routes import register_course_routes
from utils.excel_export import register_excel_export_routes
from utils.image_store import migrate_legacy_course_images
from utils.journal_models import init_journal_models
from utils.journal_routes import register_journal_routes
from utils.practice_models import init_practice_models
from utils.practice_routes import register_practice_routes
from utils.app_routes import register_app_routes
from utils.db import (
    Course,
    CourseImage,
    Group,
    Student,
    db,
    ensure_schema,
    get_or_404,
    get_setting,
    init_db_app,
    normalize_group_ids,
    parse_group_ids,
    prepare_sqlite_database,
    remove_group_id_from_csv,
    set_setting,
    upsert_course_image,
)
from utils.runtime_env import (
    env_flag,
    parse_int,
    pick_available_port,
    resource_dir,
    runtime_data_dir,
)
from utils.update_service import UpdateService

APP_VERSION = "v1.0.4"
APP_DIR_NAME = "EducationRatingSystem"
DEFAULT_APP_HOST = "127.0.0.1"
DEFAULT_APP_PORT = 54791

UPDATE_REPO = os.environ.get("UPDATE_REPO", "KEV0143/Education-Rating-System")
UPDATE_CHECK_TIMEOUT = float(os.environ.get("UPDATE_CHECK_TIMEOUT", "5.0"))
UPDATE_USER_AGENT = "RatingSystemUpdateCheck"

RESOURCE_DIR = resource_dir()
DATA_DIR = runtime_data_dir(APP_DIR_NAME)

app = Flask(
    __name__,
    template_folder=str(RESOURCE_DIR / "templates"),
    static_folder=str(RESOURCE_DIR / "static"),
)

_, _, DB_URI = prepare_sqlite_database(DATA_DIR, filename="RatingSystemKev.db")

app.config["SECRET_KEY"] = os.environ.get("SECRET_KEY", secrets.token_hex(32))
app.config["MAX_CONTENT_LENGTH"] = 10 * 1024 * 1024

init_db_app(app, DB_URI)
update_service = UpdateService(
    app_version=APP_VERSION,
    repo=UPDATE_REPO,
    timeout=UPDATE_CHECK_TIMEOUT,
    user_agent=UPDATE_USER_AGENT,
)

register_app_routes(
    app=app,
    db=db,
    Course=Course,
    CourseImage=CourseImage,
    Group=Group,
    Student=Student,
    get_or_404=get_or_404,
    get_setting=get_setting,
    set_setting=set_setting,
    normalize_group_ids=normalize_group_ids,
    parse_group_ids=parse_group_ids,
    remove_group_id_from_csv=remove_group_id_from_csv,
    upsert_course_image=upsert_course_image,
    parse_int=parse_int,
    update_service=update_service,
    app_version=APP_VERSION,
    data_dir=DATA_DIR,
    resource_dir=RESOURCE_DIR,
)

Practice, PracticeGrade, PracticeGroupInterval = init_practice_models(db)
JournalLesson, JournalLessonSession, JournalAttendance = init_journal_models(db)

with app.app_context():
    db.create_all()
    ensure_schema()
    migrate_legacy_course_images(db, Course, CourseImage, data_dir=DATA_DIR, resource_dir=RESOURCE_DIR)

register_course_routes(
    app,
    db,
    Course,
    Group,
    Student,
    Practice,
    PracticeGrade,
    PracticeGroupInterval,
    parse_group_ids,
)

register_practice_routes(
    app,
    db,
    Course,
    Group,
    Student,
    Practice,
    PracticeGrade,
    PracticeGroupInterval,
    parse_group_ids,
)

register_journal_routes(
    app,
    db,
    Course,
    Group,
    Student,
    JournalLesson,
    JournalLessonSession,
    JournalAttendance,
    parse_int,
)

register_excel_export_routes(app, db, Course, Group, Student, Practice, PracticeGrade, parse_group_ids)


@app.get("/favicon.ico")
def favicon():
    return redirect(url_for("static", filename="iconkev.ico"), code=302)


_BROWSER_OPENED = False


def _start_browser_once(host: str, port: int, debug: bool) -> None:
    global _BROWSER_OPENED
    if _BROWSER_OPENED:
        return

    if debug and os.environ.get("WERKZEUG_RUN_MAIN", "").lower() != "true":
        return

    url = f"http://{host}:{port}/"
    _BROWSER_OPENED = True

    def _open():
        try:
            webbrowser.open(url, new=0, autoraise=True)
        except Exception:
            pass

    threading.Timer(1.0, _open).start()


if __name__ == "__main__":
    host = (os.environ.get("APP_HOST") or DEFAULT_APP_HOST).strip() or DEFAULT_APP_HOST

    raw_port = (os.environ.get("APP_PORT") or "").strip()
    if raw_port:
        port = parse_int(raw_port, default=DEFAULT_APP_PORT)
    else:
        port = pick_available_port(host, DEFAULT_APP_PORT, max_tries=24)

    debug = env_flag("APP_DEBUG", True)
    auto_open_browser = env_flag("AUTO_OPEN_BROWSER", True)

    update_service.check_for_updates()
    if auto_open_browser:
        _start_browser_once(host, port, debug)
    app.run(host=host, port=port, debug=debug)
