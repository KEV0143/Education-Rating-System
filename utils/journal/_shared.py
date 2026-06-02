from flask import request, abort
from sqlalchemy import func
from utils.core.database import db
from utils.core.models import Group, Student, Course
from utils.services.runtime import parse_int
from utils.journal.helpers import (
    PAIR_SLOT_BY_NUMBER,
    ATTENDANCE_STATUSES,
    ATTENDANCE_STATUS_PRESENT,
    ATTENDANCE_STATUS_EXCUSED,
    ATTENDANCE_STATUS_ABSENT,
    ATTENDANCE_STATUS_LABELS,
    ATTENDANCE_STATUS_SHORT,
    PUBLIC_ENDPOINTS
)
from utils.journal.tunnel import is_local_request


def _unique_group_ids(values):
    out = []
    for raw in values or []:
        gid = parse_int(raw, default=0)
        if gid > 0 and gid not in out:
            out.append(int(gid))
    return out


def _parse_int_list(values):
    out = []
    for raw in values or []:
        value = parse_int(raw, default=0)
        if value > 0 and value not in out:
            out.append(int(value))
    return out


def _lesson_group_ids(lesson):
    if lesson is None:
        return []
    out = []
    raw_csv = str(getattr(lesson, "group_ids", "") or "").strip()
    if raw_csv:
        out.extend(_unique_group_ids(raw_csv.split(",")))
    primary_group_id = parse_int(getattr(lesson, "group_id", 0), default=0)
    if primary_group_id > 0 and primary_group_id not in out:
        out.insert(0, int(primary_group_id))
    return _unique_group_ids(out)


def _lesson_primary_group_id(lesson):
    group_ids = _lesson_group_ids(lesson)
    if group_ids:
        return int(group_ids[0])
    return parse_int(getattr(lesson, "group_id", 0), default=0)


def _student_count_map():
    rows = db.session.query(Student.group_id, func.count(Student.id)).group_by(Student.group_id).all()
    return {int(group_id): int(count) for group_id, count in rows}


def _summary_for_session_groups(session_row, group_ids, student_counts):
    ids = _unique_group_ids(group_ids)
    total_students = sum(int(student_counts.get(int(gid), 0)) for gid in ids)
    present_count = 0
    excused_count = 0
    if session_row and ids:
        from flask import current_app
        JournalAttendance = current_app.extensions["journal_runtime"]["JournalAttendance"]
        rows = (
            db.session.query(
                JournalAttendance.status,
                func.count(JournalAttendance.id),
            )
            .join(Student, Student.id == JournalAttendance.student_id)
            .filter(
                JournalAttendance.session_id == session_row.id,
                Student.group_id.in_(ids),
            )
            .group_by(JournalAttendance.status)
            .all()
        )
        by_status = {str(status): int(count) for status, count in rows}
        present_count = int(by_status.get(ATTENDANCE_STATUS_PRESENT, 0))
        excused_count = int(by_status.get(ATTENDANCE_STATUS_EXCUSED, 0))
    return {
        "total_students": int(total_students),
        "present_count": int(present_count),
        "excused_count": int(excused_count),
        "absent_count": max(int(total_students) - int(present_count) - int(excused_count), 0),
    }


def _lesson_payload(
    lesson,
    course_titles,
    group_names,
    student_counts,
    present_count=0,
    absent_count=0,
    excused_count=0,
    attendance_url="",
    attendance_date="",
):
    group_ids = _lesson_group_ids(lesson)
    primary_group_id = _lesson_primary_group_id(lesson)
    course_id = int(lesson.course_id)
    group_name_list = [group_names.get(int(gid), f"Группа #{gid}") for gid in group_ids]
    if not group_name_list:
        group_name_list = [group_names.get(primary_group_id, f"Группа #{primary_group_id}")]
    student_count = sum(int(student_counts.get(int(gid), 0)) for gid in group_ids)
    return {
        "id": lesson.id,
        "week_parity": lesson.week_parity,
        "day_of_week": lesson.day_of_week,
        "pair_number": lesson.pair_number,
        "semester_key": lesson.semester_key,
        "course_id": lesson.course_id,
        "course_title": course_titles.get(course_id, f"Предмет #{course_id}"),
        "group_id": int(primary_group_id),
        "group_ids": [int(gid) for gid in group_ids],
        "group_names": group_name_list,
        "group_name": ", ".join(group_name_list),
        "room": lesson.room,
        "student_count": student_count,
        "present_count": int(present_count),
        "absent_count": int(absent_count),
        "excused_count": int(excused_count),
        "attendance_url": attendance_url or "",
        "attendance_date": attendance_date or "",
    }


def _normalize_status(raw_status: str):
    value = str(raw_status or "").strip().lower()
    if value in ATTENDANCE_STATUSES:
        return value
    return None


def _request_ip() -> str:
    for header in ("X-Forwarded-For", "CF-Connecting-IP", "X-Real-IP"):
        raw = str(request.headers.get(header) or "").strip()
        if raw:
            first = raw.split(",")[0].strip()
            if first:
                return first[:64]
    return str(request.remote_addr or "")[:64]


def _pair_info(pair_number: int):
    return PAIR_SLOT_BY_NUMBER.get(int(pair_number or 0), {"number": pair_number, "label": f"{pair_number} пара", "time": ""})


def _restrict_public_routes():
    if is_local_request(request):
        return None
    endpoint = str(request.endpoint or "")
    if endpoint in PUBLIC_ENDPOINTS:
        return None
    return ("Not Found", 404)


def _groups_map(group_ids):
    ids = _unique_group_ids(group_ids)
    if not ids:
        return {}
    groups = Group.query.filter(Group.id.in_(ids)).all()
    return {int(group.id): group for group in groups}

