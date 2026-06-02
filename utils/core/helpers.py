from typing import Iterable, List, Optional, Set
from flask import abort
from utils.core.database import db
from utils.core.models import AppSetting, CourseImage
from utils.services.image_store import process_uploaded_image


def get_or_404(model, ident: int):
    obj = db.session.get(model, ident)
    if obj is None:
        abort(404)
    return obj


class SystemSettings:
    @staticmethod
    def get(key: str, default: Optional[str] = None) -> Optional[str]:
        import flask
        if flask.has_request_context():
            if not hasattr(flask.g, "settings_cache"):
                flask.g.settings_cache = {}
            if key not in flask.g.settings_cache:
                setting = db.session.get(AppSetting, key)
                flask.g.settings_cache[key] = setting.value if (setting and setting.value is not None) else None
            val = flask.g.settings_cache[key]
            return val if val is not None else default

        setting = db.session.get(AppSetting, key)
        if setting is None or setting.value is None:
            return default
        return setting.value

    @staticmethod
    def set(key: str, value: str) -> AppSetting:
        import flask
        setting = db.session.get(AppSetting, key)
        if setting is None:
            setting = AppSetting(key=key, value=value)
            db.session.add(setting)
        else:
            setting.value = value
        db.session.commit()

        if flask.has_request_context():
            if not hasattr(flask.g, "settings_cache"):
                flask.g.settings_cache = {}
            flask.g.settings_cache[key] = value
        return setting


def get_setting(key: str, default: Optional[str] = None) -> Optional[str]:
    return SystemSettings.get(key, default)


def set_setting(key: str, value: str) -> AppSetting:
    return SystemSettings.set(key, value)


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
