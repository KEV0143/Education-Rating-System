import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Iterable, List, Optional, Set

from flask import abort
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import event, text
from sqlalchemy.engine import Engine

from utils.image_store import process_uploaded_image
from utils.runtime_env import ensure_sqlite_file

db = SQLAlchemy()


@event.listens_for(Engine, "connect")
def _set_sqlite_pragma(dbapi_connection, _connection_record):
    try:
        if isinstance(dbapi_connection, sqlite3.Connection):
            cur = dbapi_connection.cursor()
            cur.execute("PRAGMA foreign_keys=ON;")
            cur.close()
    except Exception:
        pass


def prepare_sqlite_database(data_dir: Path, filename: str = "RatingSystemKev.db"):
    db_dir = data_dir / "db"
    db_dir.mkdir(parents=True, exist_ok=True)
    db_path = db_dir / filename
    ensure_sqlite_file(db_path)
    db_uri = f"sqlite:///{db_path.as_posix()}"
    return db_dir, db_path, db_uri


def init_db_app(app, db_uri: str) -> None:
    app.config["SQLALCHEMY_DATABASE_URI"] = db_uri
    app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
    db.init_app(app)


class Group(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(50), nullable=False, unique=True)

    students = db.relationship(
        "Student",
        backref="group",
        cascade="all, delete-orphan",
        passive_deletes=True,
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
        index=True,
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


class CourseImage(db.Model):
    __tablename__ = "course_image"

    course_id = db.Column(
        db.Integer,
        db.ForeignKey("course.id", ondelete="CASCADE"),
        primary_key=True,
    )
    image_data = db.Column(db.LargeBinary, nullable=False)
    mime_type = db.Column(db.String(64), nullable=False, default="image/jpeg")
    updated_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow)


def get_or_404(model, ident: int):
    obj = db.session.get(model, ident)
    if obj is None:
        abort(404)
    return obj


def get_setting(key: str, default: Optional[str] = None) -> Optional[str]:
    setting = db.session.get(AppSetting, key)
    if setting is None or setting.value is None:
        return default
    return setting.value


def set_setting(key: str, value: str) -> AppSetting:
    setting = db.session.get(AppSetting, key)
    if setting is None:
        setting = AppSetting(key=key, value=value)
        db.session.add(setting)
    else:
        setting.value = value
    db.session.commit()
    return setting


def parse_group_ids(group_ids_str: str) -> List[int]:
    if not group_ids_str:
        return []
    out: List[int] = []
    for value in str(group_ids_str).split(","):
        value = value.strip()
        if value.isdigit():
            out.append(int(value))
    return out


def normalize_group_ids(values: Iterable[str]) -> str:
    ids: List[int] = []
    for value in values:
        value = str(value).strip()
        if value.isdigit():
            ids.append(int(value))

    seen: Set[int] = set()
    uniq: List[int] = []
    for value in ids:
        if value not in seen:
            seen.add(value)
            uniq.append(value)

    return ",".join(str(value) for value in uniq)


def remove_group_id_from_csv(csv_ids: str, gid: int) -> str:
    ids = [value for value in parse_group_ids(csv_ids) if value != gid]
    return ",".join(str(value) for value in ids)


def _sqlite_columns(table: str) -> Set[str]:
    try:
        rows = db.session.execute(text(f"PRAGMA table_info({table})")).fetchall()
        return {str(row[1]) for row in rows}
    except Exception:
        return set()


def ensure_schema() -> None:
    cols = _sqlite_columns("course")
    if cols and "archived" not in cols:
        db.session.execute(text("ALTER TABLE course ADD COLUMN archived INTEGER NOT NULL DEFAULT 0"))

    cols_p = _sqlite_columns("practice")
    if cols_p:
        if "start_date" not in cols_p:
            db.session.execute(text("ALTER TABLE practice ADD COLUMN start_date DATE"))
        cols_p2 = _sqlite_columns("practice")
        if "end_date" not in cols_p2:
            db.session.execute(text("ALTER TABLE practice ADD COLUMN end_date DATE"))

    cols_pg = _sqlite_columns("practice_grade")
    if cols_pg:
        if "score_updated_at" not in cols_pg:
            db.session.execute(text("ALTER TABLE practice_grade ADD COLUMN score_updated_at DATETIME"))

        cols_pg2 = _sqlite_columns("practice_grade")
        if "score_updated_at" in cols_pg2:
            db.session.execute(
                text(
                    "UPDATE practice_grade SET score_updated_at = updated_at "
                    "WHERE score_updated_at IS NULL AND score IS NOT NULL"
                )
            )

        if "comment_updated_at" not in cols_pg2:
            db.session.execute(text("ALTER TABLE practice_grade ADD COLUMN comment_updated_at DATETIME"))

        cols_pg3 = _sqlite_columns("practice_grade")
        if "comment_updated_at" in cols_pg3:
            db.session.execute(
                text(
                    "UPDATE practice_grade SET comment_updated_at = updated_at "
                    "WHERE comment_updated_at IS NULL AND COALESCE(comment,'') <> ''"
                )
            )

    db.session.commit()


def upsert_course_image(course_id: int, file) -> bool:
    payload = process_uploaded_image(file)
    if not payload:
        return False

    img_bytes, mime = payload
    rec = db.session.get(CourseImage, course_id)
    if rec is None:
        rec = CourseImage(course_id=course_id, image_data=img_bytes, mime_type=mime)
        db.session.add(rec)
    else:
        rec.image_data = img_bytes
        rec.mime_type = mime
    return True
