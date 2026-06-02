from datetime import date
from typing import Optional, Set
from sqlalchemy import text
from utils.core.database import db


def _sqlite_columns(table: str) -> Set[str]:
    try:
        rows = db.session.execute(text(f"PRAGMA table_info({table})")).fetchall()
        db.session.commit()
        return {str(row[1]) for row in rows}
    except Exception:
        try:
            db.session.rollback()
        except Exception:
            pass
        return set()


def _semester_key_for_date(value: date) -> Optional[str]:
    month = int(value.month)
    year = int(value.year)
    if month in (9, 10, 11, 12):
        return f"{year}-{year + 1}:1"
    if month == 1:
        return f"{year - 1}-{year}:1"
    if month in (2, 3, 4, 5, 6):
        return f"{year - 1}-{year}:2"
    return None


def _safe_execute(stmt: str, params: dict = None) -> bool:
    try:
        db.session.execute(text(stmt), params or {})
        db.session.commit()
        return True
    except Exception:
        try:
            db.session.rollback()
        except Exception:
            pass
        return False


def ensure_schema() -> None:
    cols = _sqlite_columns("course")
    if cols and "archived" not in cols:
        _safe_execute("ALTER TABLE course ADD COLUMN archived INTEGER NOT NULL DEFAULT 0")
    if cols and "is_coursework" not in cols:
        _safe_execute("ALTER TABLE course ADD COLUMN is_coursework INTEGER NOT NULL DEFAULT 0")

    cols_p = _sqlite_columns("practice")
    if cols_p:
        if "start_date" not in cols_p:
            _safe_execute("ALTER TABLE practice ADD COLUMN start_date DATE")
        cols_p2 = _sqlite_columns("practice")
        if "end_date" not in cols_p2:
            _safe_execute("ALTER TABLE practice ADD COLUMN end_date DATE")

    cols_pg = _sqlite_columns("practice_grade")
    if cols_pg:
        if "score_updated_at" not in cols_pg:
            _safe_execute("ALTER TABLE practice_grade ADD COLUMN score_updated_at DATETIME")

        cols_pg2 = _sqlite_columns("practice_grade")
        if "score_updated_at" in cols_pg2:
            _safe_execute(
                "UPDATE practice_grade SET score_updated_at = updated_at "
                "WHERE score_updated_at IS NULL AND score IS NOT NULL"
            )

        if "comment_updated_at" not in cols_pg2:
            _safe_execute("ALTER TABLE practice_grade ADD COLUMN comment_updated_at DATETIME")

        cols_pg3 = _sqlite_columns("practice_grade")
        if "comment_updated_at" in cols_pg3:
            _safe_execute(
                "UPDATE practice_grade SET comment_updated_at = updated_at "
                "WHERE comment_updated_at IS NULL AND COALESCE(comment,'') <> ''"
            )

        if "change_history" not in cols_pg3:
            _safe_execute(
                "ALTER TABLE practice_grade ADD COLUMN change_history VARCHAR(2000) NOT NULL DEFAULT '[]'"
            )

    cols_j = _sqlite_columns("journal_lesson")
    if cols_j and "semester_key" not in cols_j:
        _safe_execute("ALTER TABLE journal_lesson ADD COLUMN semester_key VARCHAR(16)")
        active_semester = _semester_key_for_date(date.today())
        if not active_semester:
            now = date.today()
            active_semester = f"{now.year}-{now.year + 1}:1"
        _safe_execute(
            "UPDATE journal_lesson SET semester_key = :semester_key WHERE COALESCE(semester_key, '') = ''",
            {"semester_key": active_semester}
        )

    cols_j2 = _sqlite_columns("journal_lesson")
    if cols_j2 and "room" not in cols_j2:
        _safe_execute("ALTER TABLE journal_lesson ADD COLUMN room VARCHAR(40) NOT NULL DEFAULT ''")

    if cols_j2 and "group_ids" not in cols_j2:
        _safe_execute("ALTER TABLE journal_lesson ADD COLUMN group_ids VARCHAR(500) NOT NULL DEFAULT ''")
        _safe_execute(
            "UPDATE journal_lesson "
            "SET group_ids = CAST(group_id AS TEXT) "
            "WHERE COALESCE(group_ids, '') = ''"
        )
    elif cols_j2 and "group_ids" in cols_j2:
        _safe_execute(
            "UPDATE journal_lesson "
            "SET group_ids = CAST(group_id AS TEXT) "
            "WHERE COALESCE(group_ids, '') = ''"
        )

    cols_js = _sqlite_columns("journal_lesson_session")
    if cols_js:
        if "qr_token" not in cols_js:
            _safe_execute("ALTER TABLE journal_lesson_session ADD COLUMN qr_token VARCHAR(96) NOT NULL DEFAULT ''")
        cols_js2 = _sqlite_columns("journal_lesson_session")
        if "qr_token_created_at" not in cols_js2:
            _safe_execute("ALTER TABLE journal_lesson_session ADD COLUMN qr_token_created_at DATETIME")

    cols_ja = _sqlite_columns("journal_attendance")
    if cols_ja:
        if "status" not in cols_ja:
            _safe_execute(
                "ALTER TABLE journal_attendance ADD COLUMN status VARCHAR(16) NOT NULL DEFAULT 'absent'"
            )
        cols_ja2 = _sqlite_columns("journal_attendance")
        if "source" not in cols_ja2:
            _safe_execute(
                "ALTER TABLE journal_attendance ADD COLUMN source VARCHAR(16) NOT NULL DEFAULT 'manual'"
            )
        cols_ja3 = _sqlite_columns("journal_attendance")
        if "source_ip" not in cols_ja3:
            _safe_execute(
                "ALTER TABLE journal_attendance ADD COLUMN source_ip VARCHAR(64) NOT NULL DEFAULT ''"
            )
        cols_ja4 = _sqlite_columns("journal_attendance")
        if "marked_at" not in cols_ja4:
            _safe_execute("ALTER TABLE journal_attendance ADD COLUMN marked_at DATETIME")
        cols_ja5 = _sqlite_columns("journal_attendance")
        if "marked_at" in cols_ja5:
            if "updated_at" in cols_ja5:
                _safe_execute(
                    "UPDATE journal_attendance "
                    "SET marked_at = COALESCE(marked_at, updated_at, CURRENT_TIMESTAMP)"
                )
            else:
                _safe_execute(
                    "UPDATE journal_attendance "
                    "SET marked_at = COALESCE(marked_at, CURRENT_TIMESTAMP)"
                )

    _safe_execute("CREATE INDEX IF NOT EXISTS idx_student_group_id ON student (group_id)")
    _safe_execute("CREATE INDEX IF NOT EXISTS idx_practice_grade_student_id ON practice_grade (student_id)")
    _safe_execute("CREATE INDEX IF NOT EXISTS idx_practice_grade_practice_id ON practice_grade (practice_id)")
    _safe_execute("CREATE INDEX IF NOT EXISTS idx_journal_lesson_course_id ON journal_lesson (course_id)")
    _safe_execute("CREATE INDEX IF NOT EXISTS idx_journal_lesson_session_lesson_id ON journal_lesson_session (lesson_id)")
    _safe_execute("CREATE INDEX IF NOT EXISTS idx_journal_attendance_session_id ON journal_attendance (session_id)")
    _safe_execute("CREATE INDEX IF NOT EXISTS idx_journal_attendance_student_id ON journal_attendance (student_id)")
