import json
import re
import time
from datetime import date
from flask import Response, jsonify, render_template, request, url_for, stream_with_context
from sqlalchemy import case, func, or_
from utils.core.database import db
from utils.journal.helpers import (
    WEEK_PARITY_OPTIONS,
    DAY_OPTIONS,
    PAIR_SLOTS,
    _parse_lesson_date,
    _active_semester_base
)
from utils.journal._shared import (
    _student_count_map,
    _lesson_group_ids,
    _lesson_payload,
    _summary_for_session_groups
)


def _event_key_date(lesson_date: date) -> str:
    return f"date:{lesson_date.isoformat()}"


def _event_key_lesson(lesson_id: int, lesson_date: date) -> str:
    return f"lesson:{int(lesson_id)}:{lesson_date.isoformat()}"


def register_calendar_routes(app, db, Course, Group, Student, JournalLesson, JournalLessonSession, JournalAttendance, parse_int, ctx):
    runtime = ctx['runtime']
    attendance_events = ctx['attendance_events']
    tunnel_events = ctx['tunnel_events']
    tunnel = ctx['tunnel']

    _last_cleanup_time = 0.0

    def _cleanup_outdated_lessons(active_semester_key: str) -> None:
        nonlocal _last_cleanup_time
        now = time.monotonic()
        if now - _last_cleanup_time < 60.0:
            return
        _last_cleanup_time = now
        stale_lessons = JournalLesson.query.filter(
            or_(
                JournalLesson.semester_key.is_(None),
                JournalLesson.semester_key == "",
                JournalLesson.semester_key != active_semester_key,
            )
        ).all()
        if not stale_lessons:
            return
        for stale_lesson in stale_lessons:
            db.session.delete(stale_lesson)
        db.session.commit()

    def _bump_date_event(lesson_date: date) -> None:
        attendance_events.bump(_event_key_date(lesson_date))

    def _build_lessons_for_date(lesson_date: date, active_semester_key: str):
        from utils.journal.helpers import _date_context
        lesson_ctx = _date_context(lesson_date)
        if not lesson_ctx:
            return []

        day_of_week = int(lesson_ctx["day_of_week"])
        if day_of_week == 7:
            return []
        if str(lesson_ctx.get("stage")) not in ("classes_autumn", "classes_spring"):
            return []
        if str(lesson_ctx.get("semester_key")) != str(active_semester_key):
            return []

        week_parity = str(lesson_ctx.get("week_parity") or "")
        if week_parity not in WEEK_PARITY_OPTIONS:
            return []

        lessons = (
            JournalLesson.query.filter_by(
                semester_key=active_semester_key,
                week_parity=week_parity,
                day_of_week=day_of_week,
            )
            .order_by(JournalLesson.pair_number.asc(), JournalLesson.id.asc())
            .all()
        )
        if not lessons:
            return []

        lesson_ids = [int(lesson.id) for lesson in lessons]
        course_titles = {int(course_id): title for course_id, title in db.session.query(Course.id, Course.title).all()}
        group_names = {int(group_id): name for group_id, name in db.session.query(Group.id, Group.name).all()}
        student_counts = _student_count_map()

        sessions = JournalLessonSession.query.filter(
            JournalLessonSession.lesson_id.in_(lesson_ids),
            JournalLessonSession.session_date == lesson_date,
        ).all()
        session_by_lesson = {int(session_row.lesson_id): session_row for session_row in sessions}

        payload = []
        for lesson in lessons:
            session_row = session_by_lesson.get(int(lesson.id))
            lesson_group_ids = _lesson_group_ids(lesson)
            if lesson_group_ids:
                summary_totals = _summary_for_session_groups(session_row, lesson_group_ids, student_counts)
                present_count = int(summary_totals["present_count"])
                excused_count = int(summary_totals["excused_count"])
                absent_count = int(summary_totals["absent_count"])
            else:
                present_count = 0
                excused_count = 0
                absent_count = 0

            payload.append(
                _lesson_payload(
                    lesson,
                    course_titles=course_titles,
                    group_names=group_names,
                    student_counts=student_counts,
                    present_count=present_count,
                    absent_count=absent_count,
                    excused_count=excused_count,
                    attendance_url=url_for("journal_lesson_page", lesson_id=lesson.id, date=lesson_date.isoformat()),
                    attendance_date=lesson_date.isoformat(),
                )
            )

        payload.sort(
            key=lambda lesson_payload: (
                int(lesson_payload.get("pair_number") or 0),
                str(lesson_payload.get("group_name") or ""),
            )
        )
        return payload

    def _tunnel_payload(lesson=None, lesson_date: date | None = None):
        snap = tunnel.snapshot()
        scoped = lesson is not None and lesson_date is not None
        if scoped:
            active_key = str(runtime.get("active_public_session_key") or "")
            current_key = f"{int(lesson.id)}:{lesson_date.isoformat()}"
            is_current_session = bool(active_key) and active_key == current_key
        else:
            is_current_session = True

        if not is_current_session:
            return {
                "active": False,
                "public_url": "",
                "error_message": "",
                "reconnecting": False,
                "next_refresh_epoch": None,
                "refresh_interval_seconds": snap.get("refresh_interval_seconds"),
            }

        return {
            "active": bool(snap.get("active", False)),
            "public_url": str(snap.get("public_url") or ""),
            "error_message": str(snap.get("error_message") or ""),
            "reconnecting": bool(snap.get("reconnecting", False)),
            "next_refresh_epoch": snap.get("next_refresh_epoch"),
            "refresh_interval_seconds": snap.get("refresh_interval_seconds"),
        }

    @app.get("/journal")
    def journal_page():
        active_semester = _active_semester_base()
        active_semester_key = active_semester["key"]
        _cleanup_outdated_lessons(active_semester_key)

        selected_date = _parse_lesson_date(request.args.get("date")) or date.today()

        courses = Course.query.filter(Course.archived.is_(False)).order_by(Course.title.asc()).all()
        groups = Group.query.order_by(Group.name.asc()).all()

        lessons = (
            JournalLesson.query.filter_by(semester_key=active_semester_key)
            .order_by(
                case((JournalLesson.week_parity == "I", 0), else_=1),
                JournalLesson.day_of_week.asc(),
                JournalLesson.pair_number.asc(),
                JournalLesson.id.asc(),
            )
            .all()
        )

        course_titles = {int(course_id): title for course_id, title in db.session.query(Course.id, Course.title).all()}
        group_names = {int(group.id): group.name for group in groups}
        student_counts = _student_count_map()

        default_lessons = []
        for lesson in lessons:
            lesson_group_ids = _lesson_group_ids(lesson)
            student_count = sum(int(student_counts.get(int(gid), 0)) for gid in lesson_group_ids)
            default_lessons.append(
                _lesson_payload(
                    lesson,
                    course_titles=course_titles,
                    group_names=group_names,
                    student_counts=student_counts,
                    present_count=0,
                    absent_count=student_count,
                    excused_count=0,
                    attendance_url="",
                    attendance_date="",
                )
            )

        from utils.core.helpers import get_setting
        import json

        auto_cal_users_raw = get_setting("journal_auto_cal_users", "[]")
        auto_cal_selected_raw = get_setting("journal_auto_cal_selected", "[]")
        auto_cal_use_custom = get_setting("journal_auto_cal_use_custom", "1") == "1"

        try:
            auto_cal_users = json.loads(auto_cal_users_raw)
        except Exception:
            auto_cal_users = []

        try:
            auto_cal_selected = json.loads(auto_cal_selected_raw)
        except Exception:
            auto_cal_selected = []

        return render_template("journal/journal.html",
            courses=courses,
            groups=groups,
            week_parities=WEEK_PARITY_OPTIONS,
            days=DAY_OPTIONS,
            pair_slots=PAIR_SLOTS,
            active_semester_key=active_semester_key,
            active_semester_label=active_semester["label"],
            selected_date_iso=selected_date.isoformat(),
            lessons=default_lessons,
            auto_cal_users=auto_cal_users,
            auto_cal_selected=auto_cal_selected,
            auto_cal_use_custom=auto_cal_use_custom,
        )

    @app.get("/api/journal/lessons/<int:lesson_id>/delete-scope-preview")
    def api_journal_delete_lesson_scope_preview(lesson_id: int):
        lesson = db.session.get(JournalLesson, lesson_id)
        if not lesson:
            return jsonify({"success": False, "error": "Занятие не найдено"}), 404

        scope = str(request.args.get("scope") or "").strip().lower() or "single"
        if scope == "single":
            lessons_to_delete = [lesson]
        elif scope in {"course", "name", "all"}:
            lessons_to_delete = JournalLesson.query.filter_by(
                semester_key=str(lesson.semester_key),
                course_id=int(lesson.course_id),
            ).all()
            scope = "course"
        else:
            return jsonify({"success": False, "error": "Некорректный режим удаления"}), 400

        if not lessons_to_delete:
            return jsonify({"success": False, "error": "Занятия для удаления не найдены"}), 404

        lesson_ids = [int(item.id) for item in lessons_to_delete]
        session_rows = (
            JournalLessonSession.query.filter(JournalLessonSession.lesson_id.in_(lesson_ids))
            .order_by(JournalLessonSession.session_date.asc())
            .all()
        )
        session_ids = [int(row.id) for row in session_rows]
        attendance_count = 0
        if session_ids:
            attendance_count = (
                db.session.query(func.count(JournalAttendance.id))
                .filter(JournalAttendance.session_id.in_(session_ids))
                .scalar()
                or 0
            )

        session_dates = sorted(
            {
                row.session_date.isoformat()
                for row in session_rows
                if row.session_date is not None
            }
        )

        group_ids = []
        for item in lessons_to_delete:
            for gid in _lesson_group_ids(item):
                gid_value = int(gid)
                if gid_value > 0 and gid_value not in group_ids:
                    group_ids.append(gid_value)

        group_map = {}
        if group_ids:
            group_map = {
                int(group.id): group.name
                for group in Group.query.filter(Group.id.in_(group_ids)).all()
            }
        group_names = [group_map.get(int(gid), f"Группа #{gid}") for gid in group_ids]

        course = db.session.get(Course, int(lesson.course_id))
        session_dates_preview = session_dates[:20]
        return jsonify(
            {
                "success": True,
                "scope": scope,
                "course_id": int(lesson.course_id),
                "course_title": course.title if course else f"Предмет #{int(lesson.course_id)}",
                "semester_key": str(lesson.semester_key or ""),
                "lessons_count": int(len(lesson_ids)),
                "sessions_count": int(len(session_rows)),
                "attendance_count": int(attendance_count),
                "group_names": group_names,
                "date_from": session_dates[0] if session_dates else None,
                "date_to": session_dates[-1] if session_dates else None,
                "session_dates_preview": session_dates_preview,
                "session_dates_hidden": int(max(len(session_dates) - len(session_dates_preview), 0)),
            }
        )

    @app.get("/journal/export/attendance")
    def api_journal_students_search():
        raw_query = str(request.args.get("q") or "")
        query = re.sub(r"\s+", " ", raw_query).strip()
        query_fold = query.casefold()
        if len(query) < 2:
            return jsonify({"success": True, "students": []})

        limit = parse_int(request.args.get("limit"), default=10)
        limit = min(max(limit, 1), 20)

        candidates_query = (
            db.session.query(Student.id, Student.fio, Student.group_id, Group.name)
            .join(Group, Group.id == Student.group_id)
            .order_by(Student.fio.asc())
        )

        sql_like = f"%{query}%"
        candidates = candidates_query.filter(Student.fio.ilike(sql_like)).limit(int(limit * 6)).all()
        if not candidates:
            return jsonify({"success": True, "students": []})

        items = []
        seen_ids = set()
        for student_id, fio, group_id, group_name in candidates:
            sid = int(student_id)
            if sid in seen_ids:
                continue
            safe_fio = str(fio or "").strip()
            if not safe_fio:
                continue
            if query_fold not in safe_fio.casefold():
                continue
            seen_ids.add(sid)
            gid = int(group_id)
            items.append(
                {
                    "id": sid,
                    "fio": safe_fio,
                    "group_id": gid,
                    "group_name": str(group_name or f"Группа #{gid}"),
                }
            )
            if len(items) >= limit:
                break

        return jsonify({"success": True, "students": items})

    @app.get("/api/journal/date/<lesson_date>/lessons")
    def api_journal_lessons_by_date(lesson_date: str):
        lesson_date_value = _parse_lesson_date(lesson_date)
        if lesson_date_value is None:
            return jsonify({"success": False, "error": "Некорректная дата"}), 400

        active_semester = _active_semester_base()
        active_semester_key = str(active_semester["key"])
        _cleanup_outdated_lessons(active_semester_key)

        lessons = _build_lessons_for_date(lesson_date_value, active_semester_key)
        return jsonify({"success": True, "lessons": lessons})

    @app.get("/stream/journal/date/<lesson_date>")
    def stream_journal_date(lesson_date: str):
        lesson_date_value = _parse_lesson_date(lesson_date)
        if lesson_date_value is None:
            return jsonify({"success": False, "error": "Некорректная дата"}), 400

        def _date_payload():
            active_semester = _active_semester_base()
            active_semester_key = str(active_semester["key"])
            lessons = _build_lessons_for_date(lesson_date_value, active_semester_key)
            return {"date": lesson_date_value.isoformat(), "lessons": lessons}

        event_key = _event_key_date(lesson_date_value)

        @stream_with_context
        def generate():
            version = attendance_events.get_version(event_key)
            initial_payload = _date_payload()
            yield f"event: lessons\ndata: {json.dumps(initial_payload, ensure_ascii=False)}\n\n"

            while True:
                next_version = attendance_events.wait_for_change(event_key, version, timeout=30.0)
                if next_version == version:
                    yield ": keepalive\n\n"
                    continue
                version = next_version
                payload = _date_payload()
                yield f"event: lessons\ndata: {json.dumps(payload, ensure_ascii=False)}\n\n"

        headers = {
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
            "Connection": "keep-alive",
        }
        return Response(generate(), mimetype="text/event-stream", headers=headers)

    @app.get("/stream/journal/tunnel")
    def stream_journal_tunnel():
        event_key = "tunnel"
        scoped_lesson = None
        scoped_lesson_date = _parse_lesson_date(request.args.get("date"))
        scoped_lesson_id = parse_int(request.args.get("lesson_id"), default=0)
        if scoped_lesson_id > 0 and scoped_lesson_date is not None:
            scoped_lesson = db.session.get(JournalLesson, scoped_lesson_id)

        @stream_with_context
        def generate():
            version = tunnel_events.get_version(event_key)
            initial_payload = _tunnel_payload(lesson=scoped_lesson, lesson_date=scoped_lesson_date)
            initial_payload["version"] = int(version)
            yield f"event: state\ndata: {json.dumps(initial_payload, ensure_ascii=False)}\n\n"

            while True:
                next_version = tunnel_events.wait_for_change(event_key, version, timeout=30.0)
                if next_version == version:
                    yield ": keepalive\n\n"
                    continue
                version = next_version
                payload = _tunnel_payload(lesson=scoped_lesson, lesson_date=scoped_lesson_date)
                payload["version"] = int(version)
                yield f"event: state\ndata: {json.dumps(payload, ensure_ascii=False)}\n\n"

        headers = {
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
            "Connection": "keep-alive",
        }
        return Response(generate(), mimetype="text/event-stream", headers=headers)
