from utils.core import db, prepare_sqlite_database, init_db_app, ensure_schema, Group, Student, Course, CourseImage, AppSetting, get_or_404, get_setting, set_setting, parse_group_ids, normalize_group_ids, remove_group_id_from_csv, upsert_course_image
from utils.services.runtime import parse_int, pick_available_port, resource_dir, runtime_data_dir
from utils.services.update_service import UpdateService
from utils.routes import register_main_routes, register_course_crud_routes, register_student_routes, register_course_routes
from utils.practice import init_practice_models, register_practice_routes, register_excel_export_routes
from utils.journal.models import init_journal_models
from utils.journal import register_journal_routes
