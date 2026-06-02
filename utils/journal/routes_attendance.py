import json
import re
import secrets
from datetime import date, datetime, timezone
from urllib.parse import urlparse
from flask import Response, flash, jsonify, redirect, render_template, request, url_for, stream_with_context
from sqlalchemy import func
from utils.journal.helpers import (
    DAY_OPTIONS,
    ATTENDANCE_STATUSES,
    ATTENDANCE_STATUS_LABELS,
    ATTENDANCE_STATUS_ABSENT,
    ATTENDANCE_STATUS_PRESENT,
    ATTENDANCE_STATUS_EXCUSED,
    ATTENDANCE_STATUS_SHORT,
    PUBLIC_ENDPOINTS,
    _parse_lesson_date,
    _active_semester_base,
    _date_context,
    _stage_add_error,
    _build_qr_data_uri
)
from utils.journal.tunnel import is_local_request
from utils.journal._shared import (
    _lesson_group_ids,
    _groups_map,
    _student_count_map,
    _summary_for_session_groups,
    _pair_info,
    _normalize_status,
    _request_ip,
    _restrict_public_routes
)


def _event_key_date(lesson_date: date) -> str:
    return f"date:{lesson_date.isoformat()}"


def _event_key_lesson(lesson_id: int, lesson_date: date) -> str:
    return f"lesson:{int(lesson_id)}:{lesson_date.isoformat()}"


def _is_public_tunnel_host(host: str) -> bool:
    safe = str(host or "").strip().lower()
    return safe.endswith(".lhr.life") or safe.endswith(".localhost.run")


def _is_local_like_host(host: str) -> bool:
    safe_host = str(host or "").strip().strip("[]").lower()
    if not safe_host:
        return False
    if safe_host in {"127.0.0.1", "localhost", "::1"}:
        return True
    if safe_host.endswith(".local"):
        return True
    try:
        import ipaddress
        ip_value = ipaddress.ip_address(safe_host)
        return bool(ip_value.is_loopback or ip_value.is_private or ip_value.is_link_local)
    except ValueError:
        return False


def register_attendance_routes(app, db, Course, Group, Student, JournalLesson, JournalLessonSession, JournalAttendance, parse_int, ctx):
    runtime = ctx['runtime']
    attendance_events = ctx['attendance_events']
    tunnel_events = ctx['tunnel_events']
    tunnel = ctx['tunnel']

    def _public_session_key(lesson_id: int, lesson_date: date) -> str:
        return f"{int(lesson_id)}:{lesson_date.isoformat()}"

    def _get_active_public_session_key() -> str:
        return str(runtime.get("active_public_session_key") or "")

    def _set_active_public_session_key(value: str) -> None:
        runtime["active_public_session_key"] = str(value or "").strip()

    def _parse_public_session_key(value: str):
        raw = str(value or "").strip()
        if not raw or ":" not in raw:
            return None
        lesson_part, date_part = raw.split(":", 1)
        lesson_id = parse_int(lesson_part, default=0)
        lesson_date = _parse_lesson_date(date_part)
        if lesson_id <= 0 or lesson_date is None:
            return None
        return int(lesson_id), lesson_date

    def _active_public_session_exists(key: str) -> bool:
        parsed = _parse_public_session_key(key)
        if not parsed:
            return False
        lesson_id, lesson_date = parsed
        session = JournalLessonSession.query.filter_by(lesson_id=lesson_id, session_date=lesson_date).first()
        return bool(session and str(session.qr_token or "").strip())

    def _request_host_candidates(req) -> set[str]:
        hosts = set()
        raw_values = [
            str(getattr(req, "host", "") or ""),
            str(req.headers.get("X-Forwarded-Host", "") or ""),
            str(req.headers.get("X-Original-Host", "") or ""),
        ]
        for raw in raw_values:
            for part in str(raw).split(","):
                value = part.strip()
                if not value:
                    continue
                host = value.split(":")[0].strip("[]").lower()
                if host:
                    hosts.add(host)
        return hosts

    def _bump_date_event(lesson_date: date) -> None:
        attendance_events.bump(_event_key_date(lesson_date))

    def _bump_lesson_event(lesson_id: int, lesson_date: date) -> None:
        attendance_events.bump(_event_key_lesson(lesson_id, lesson_date))

    def _bump_related_events(lesson_id: int, lesson_date: date) -> None:
        _bump_date_event(lesson_date)
        _bump_lesson_event(lesson_id, lesson_date)

    def _get_or_create_session(lesson, lesson_date: date):
        session_row = JournalLessonSession.query.filter_by(lesson_id=lesson.id, session_date=lesson_date).first()
        if session_row:
            return session_row
        session_row = JournalLessonSession(lesson_id=lesson.id, session_date=lesson_date, qr_token="", qr_token_created_at=None)
        db.session.add(session_row)
        db.session.flush()
        return session_row

    def _generate_qr_token():
        return secrets.token_urlsafe(24)

    def _ensure_session_token(session_row):
        if str(session_row.qr_token or "").strip():
            return False
        session_row.qr_token = _generate_qr_token()
        session_row.qr_token_created_at = datetime.now(timezone.utc)
        return True

    def _validate_lesson_date_for_attendance(lesson, lesson_date: date, active_semester):
        if lesson is None or lesson_date is None:
            return None, "Некорректные параметры занятия"

        active_semester_key = str(active_semester["key"])
        if str(lesson.semester_key) != active_semester_key:
            return None, f"Занятие относится к другому семестру. Активный: {active_semester['label']}"

        lesson_ctx = _date_context(lesson_date)
        if not lesson_ctx:
            return None, "Дата вне учебного периода"

        day_of_week = int(lesson_ctx["day_of_week"])
        if day_of_week == 7:
            return None, "В воскресенье занятия не проводятся"

        if str(lesson_ctx.get("semester_key")) != active_semester_key:
            return None, f"Дата вне активного семестра ({active_semester['label']})"

        if str(lesson_ctx.get("stage")) not in ("classes_autumn", "classes_spring"):
            return None, _stage_add_error(lesson_ctx)

        if int(lesson.day_of_week) != day_of_week:
            return None, "Выбранная дата не совпадает с днем недели этого занятия"

        if str(lesson.week_parity) != str(lesson_ctx.get("week_parity") or ""):
            return None, "Выбранная дата не совпадает с четностью недели этого занятия"

        return lesson_ctx, None

    def _attendance_rows_for_session(session_row, students):
        from utils.journal.helpers import _to_moscow, _format_moscow
        student_ids = [int(student.id) for student in students]
        rows = (
            JournalAttendance.query.filter(
                JournalAttendance.session_id == session_row.id,
                JournalAttendance.student_id.in_(student_ids),
            )
            .all()
            if student_ids
            else []
        )
        by_student = {int(row.student_id): row for row in rows}

        out_rows = []
        present_count = 0
        excused_count = 0
        for index, student in enumerate(students, start=1):
            row = by_student.get(int(student.id))
            status = ATTENDANCE_STATUS_ABSENT
            source = ""
            source_ip = ""
            marked_at = None
            if row and row.status in ATTENDANCE_STATUSES:
                status = str(row.status)
                source = str(row.source or "")
                source_ip = str(row.source_ip or "")
                marked_at = row.marked_at

            if status == ATTENDANCE_STATUS_PRESENT:
                present_count += 1
            elif status == ATTENDANCE_STATUS_EXCUSED:
                excused_count += 1

            out_rows.append(
                {
                    "index": index,
                    "id": int(student.id),
                    "fio": student.fio,
                    "status": status,
                    "status_label": ATTENDANCE_STATUS_LABELS.get(status, status),
                    "status_short": ATTENDANCE_STATUS_SHORT.get(status, status),
                    "source": source,
                    "source_ip": source_ip,
                    "marked_at": marked_at,
                    "marked_at_display": _format_moscow(marked_at, with_seconds=True) if marked_at else "-",
                }
            )

        total_students = len(students)
        return out_rows, {
            "total_students": total_students,
            "present_count": present_count,
            "excused_count": excused_count,
            "absent_count": max(total_students - present_count - excused_count, 0),
        }

    def _recent_qr_marks(session_row, limit: int = 14):
        from utils.journal.helpers import _to_moscow, _format_moscow
        if session_row is None:
            return []
        marks = (
            db.session.query(
                JournalAttendance.student_id,
                JournalAttendance.marked_at,
                JournalAttendance.source_ip,
                JournalAttendance.source,
                JournalAttendance.status,
                Student.fio,
            )
            .join(Student, Student.id == JournalAttendance.student_id)
            .filter(
                JournalAttendance.session_id == session_row.id,
            )
            .order_by(JournalAttendance.marked_at.desc(), JournalAttendance.id.desc())
            .limit(int(limit))
            .all()
        )

        out = []
        for student_id, marked_at, source_ip, source, status, fio in marks:
            local_marked_at = _to_moscow(marked_at)
            source_value = str(source or "").strip().lower()
            if source_value == "qr":
                source_label = "QR"
            elif source_value == "manual":
                source_label = "Локально"
            else:
                source_label = "Система"
            out.append(
                {
                    "student_id": int(student_id),
                    "fio": str(fio or ""),
                    "source_ip": str(source_ip or ""),
                    "source": source_value,
                    "source_label": source_label,
                    "status": str(status or ""),
                    "status_label": ATTENDANCE_STATUS_LABELS.get(str(status or ""), str(status or "")),
                    "marked_at": local_marked_at.isoformat() if local_marked_at else "",
                    "marked_at_display": _format_moscow(marked_at, with_seconds=True) if marked_at else "-",
                }
            )
        return out

    def _build_checkin_urls(session_row, lesson=None, lesson_date: date | None = None):
        checkin_path = ""
        local_checkin_url = ""
        public_checkin_url = ""
        effective_checkin_url = ""

        if session_row and str(session_row.qr_token or "").strip():
            checkin_path = url_for("journal_checkin_page", token=session_row.qr_token)
            local_checkin_url = url_for("journal_checkin_page", token=session_row.qr_token, _external=True)
            active_key = _get_active_public_session_key()
            current_key = ""
            if lesson is not None and lesson_date is not None:
                current_key = _public_session_key(int(lesson.id), lesson_date)

            if not active_key and current_key:
                snap = tunnel.snapshot()
                if bool(snap.get("active")) and str(snap.get("public_url") or "").strip():
                    _set_active_public_session_key(current_key)
                    active_key = current_key

            if active_key and current_key and active_key == current_key:
                public_checkin_url = tunnel.build_public_url_for_path(checkin_path)
                effective_checkin_url = public_checkin_url

        return {
            "checkin_path": checkin_path,
            "local_checkin_url": local_checkin_url,
            "public_checkin_url": public_checkin_url,
            "effective_checkin_url": effective_checkin_url,
        }

    def _tunnel_payload(lesson=None, lesson_date: date | None = None):
        snap = tunnel.snapshot()
        scoped = lesson is not None and lesson_date is not None
        if scoped:
            active_key = _get_active_public_session_key()
            current_key = _public_session_key(int(lesson.id), lesson_date)
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

    def _default_attendance_rows(students):
        rows = []
        for index, student in enumerate(students, start=1):
            rows.append(
                {
                    "index": index,
                    "id": int(student.id),
                    "fio": student.fio,
                    "status": ATTENDANCE_STATUS_ABSENT,
                    "status_label": ATTENDANCE_STATUS_LABELS[ATTENDANCE_STATUS_ABSENT],
                    "status_short": ATTENDANCE_STATUS_SHORT[ATTENDANCE_STATUS_ABSENT],
                    "source": "",
                    "source_ip": "",
                    "marked_at": None,
                    "marked_at_display": "-",
                }
            )
        total = len(students)
        return rows, {
            "total_students": total,
            "present_count": 0,
            "excused_count": 0,
            "absent_count": total,
        }

    def _serialize_rows_for_api(rows):
        from utils.journal.helpers import _to_moscow, _format_moscow
        payload = []
        for row in rows:
            marked_at = row.get("marked_at")
            local_marked_at = _to_moscow(marked_at)
            marked_at_display = str(row.get("marked_at_display") or "")
            if not marked_at_display:
                marked_at_display = _format_moscow(marked_at, with_seconds=True) if marked_at else "-"
            payload.append(
                {
                    "id": int(row.get("id") or 0),
                    "index": int(row.get("index") or 0),
                    "fio": str(row.get("fio") or ""),
                    "status": str(row.get("status") or ATTENDANCE_STATUS_ABSENT),
                    "status_label": str(row.get("status_label") or ""),
                    "status_short": str(row.get("status_short") or ""),
                    "source": str(row.get("source") or ""),
                    "source_ip": str(row.get("source_ip") or ""),
                    "marked_at": local_marked_at.isoformat() if local_marked_at else "",
                    "marked_at_display": marked_at_display,
                }
            )
        return payload

    def _lesson_attendance_payload(lesson, lesson_date: date, group_id: int | None = None):
        lesson_group_ids = _lesson_group_ids(lesson)
        active_group_id = parse_int(group_id, default=0)
        if active_group_id not in lesson_group_ids:
            active_group_id = int(lesson_group_ids[0]) if lesson_group_ids else 0

        students = (
            Student.query.filter_by(group_id=active_group_id).order_by(Student.fio.asc()).all()
            if active_group_id > 0
            else []
        )
        session_row = _session_by_lesson_date(lesson, lesson_date)
        if session_row:
            rows, summary = _attendance_rows_for_session(session_row, students)
        else:
            rows, summary = _default_attendance_rows(students)

        student_counts = _student_count_map()
        overall_summary = _summary_for_session_groups(session_row, lesson_group_ids, student_counts)

        return {
            "lesson_id": int(lesson.id),
            "lesson_date": lesson_date.isoformat(),
            "session_id": int(session_row.id) if session_row else None,
            "active_group_id": int(active_group_id) if active_group_id > 0 else None,
            "group_ids": [int(gid) for gid in lesson_group_ids],
            "summary": {
                "total_students": int(summary["total_students"]),
                "present_count": int(summary["present_count"]),
                "absent_count": int(summary["absent_count"]),
                "excused_count": int(summary["excused_count"]),
            },
            "overall_summary": {
                "total_students": int(overall_summary["total_students"]),
                "present_count": int(overall_summary["present_count"]),
                "absent_count": int(overall_summary["absent_count"]),
                "excused_count": int(overall_summary["excused_count"]),
            },
            "students": _serialize_rows_for_api(rows),
            "qr_marks": _recent_qr_marks(session_row),
        }

    def _lesson_qr_payload(lesson, lesson_date: date):
        session_row = _session_by_lesson_date(lesson, lesson_date)
        created = False
        if session_row is None:
            session_row = _get_or_create_session(lesson, lesson_date)
            created = True
        if _ensure_session_token(session_row):
            created = True
        if created:
            db.session.commit()

        checkin_urls = _build_checkin_urls(session_row, lesson=lesson, lesson_date=lesson_date)
        qr_data_uri, qr_error = _build_qr_data_uri(checkin_urls["effective_checkin_url"])
        checkin_summary = _summary_for_session_groups(session_row, _lesson_group_ids(lesson), _student_count_map())
        return {
            "lesson_id": int(lesson.id),
            "lesson_date": lesson_date.isoformat(),
            "session_id": int(session_row.id),
            "local_checkin_url": checkin_urls["local_checkin_url"],
            "public_checkin_url": checkin_urls["public_checkin_url"],
            "effective_checkin_url": checkin_urls["effective_checkin_url"],
            "qr_data_uri": qr_data_uri or "",
            "qr_error": qr_error or "",
            "checkin_summary": {
                "total_students": int(checkin_summary["total_students"]),
                "present_count": int(checkin_summary["present_count"]),
                "absent_count": int(checkin_summary["absent_count"]),
                "excused_count": int(checkin_summary["excused_count"]),
            },
            "tunnel": _tunnel_payload(lesson=lesson, lesson_date=lesson_date),
        }

    def _is_ajax_request() -> bool:
        requested_with = str(request.headers.get("X-Requested-With") or "").strip().lower()
        if requested_with == "xmlhttprequest":
            return True
        accept = str(request.headers.get("Accept") or "").lower()
        return "application/json" in accept

    def _request_local_port(default: int = 5000) -> int:
        try:
            host_value = str(request.host or "")
            if ":" in host_value:
                port_raw = host_value.rsplit(":", 1)[1]
                return int(port_raw)
        except Exception:
            pass
        return int(default)

    def _session_by_lesson_date(lesson, lesson_date: date):
        return JournalLessonSession.query.filter_by(lesson_id=lesson.id, session_date=lesson_date).first()

    @app.get("/journal/lesson/<int:lesson_id>")
    def journal_lesson_page(lesson_id: int):
        lesson = db.session.get(JournalLesson, lesson_id)
        if not lesson:
            flash("Занятие не найдено", "error")
            return redirect(url_for("journal_page"))

        lesson_date = _parse_lesson_date(request.args.get("date"))
        if lesson_date is None:
            flash("Укажите корректную дату занятия", "error")
            return redirect(url_for("journal_page"))

        active_semester = _active_semester_base()
        
        from sqlalchemy import or_
        JournalLesson.query.filter(
            or_(
                JournalLesson.semester_key.is_(None),
                JournalLesson.semester_key == "",
                JournalLesson.semester_key != str(active_semester["key"]),
            )
        ).delete()
        db.session.commit()

        lesson_ctx, validation_error = _validate_lesson_date_for_attendance(lesson, lesson_date, active_semester)
        if validation_error:
            flash(validation_error, "error")
            return redirect(url_for("journal_page", date=lesson_date.isoformat()))

        course = db.session.get(Course, lesson.course_id)
        lesson_group_ids = _lesson_group_ids(lesson)
        lesson_groups_map = _groups_map(lesson_group_ids)
        ordered_group_ids = [gid for gid in lesson_group_ids if gid in lesson_groups_map]

        if course is None or not ordered_group_ids:
            flash("Связанные данные занятия не найдены", "error")
            return redirect(url_for("journal_page", date=lesson_date.isoformat()))

        selected_group_id = parse_int(request.args.get("group_id"), default=0)
        if selected_group_id not in ordered_group_ids:
            selected_group_id = int(ordered_group_ids[0])
        group = lesson_groups_map.get(int(selected_group_id))
        if group is None:
            flash("Группа занятия не найдена", "error")
            return redirect(url_for("journal_page", date=lesson_date.isoformat()))

        session_row = _get_or_create_session(lesson, lesson_date)
        changed = _ensure_session_token(session_row)
        if changed:
            db.session.commit()
        else:
            db.session.flush()

        students = Student.query.filter_by(group_id=group.id).order_by(Student.fio.asc()).all()
        student_rows, summary = _attendance_rows_for_session(session_row, students)
        qr_marks = _recent_qr_marks(session_row)

        checkin_urls = _build_checkin_urls(session_row, lesson=lesson, lesson_date=lesson_date)
        qr_data_uri, qr_error = _build_qr_data_uri(checkin_urls["effective_checkin_url"])
        tunnel_state = _tunnel_payload(lesson=lesson, lesson_date=lesson_date)

        group_student_counts = _student_count_map()
        group_name_by_id = {int(gid): lesson_groups_map[int(gid)].name for gid in ordered_group_ids if int(gid) in lesson_groups_map}
        lesson_tabs = []
        for gid in ordered_group_ids:
            lesson_tabs.append(
                {
                    "lesson_id": int(lesson.id),
                    "group_id": int(gid),
                    "group_name": group_name_by_id.get(gid, f"Группа #{gid}"),
                    "student_count": int(group_student_counts.get(gid, 0)),
                    "is_active": int(gid) == int(selected_group_id),
                }
            )

        overall_summary = _summary_for_session_groups(session_row, ordered_group_ids, group_student_counts)
        group_names_display = ", ".join(lesson_groups_map[int(gid)].name for gid in ordered_group_ids if int(gid) in lesson_groups_map)

        pair_info = _pair_info(lesson.pair_number)
        return render_template("journal/journal_lesson.html",
            lesson=lesson,
            lesson_date_iso=lesson_date.isoformat(),
            lesson_ctx=lesson_ctx,
            course=course,
            group=group,
            pair_info=pair_info,
            session_row=session_row,
            summary=summary,
            overall_summary=overall_summary,
            student_rows=student_rows,
            qr_marks=qr_marks,
            status_labels=ATTENDANCE_STATUS_LABELS,
            status_short=ATTENDANCE_STATUS_SHORT,
            local_checkin_url=checkin_urls["local_checkin_url"],
            public_checkin_url=checkin_urls["public_checkin_url"],
            effective_checkin_url=checkin_urls["effective_checkin_url"],
            qr_data_uri=qr_data_uri,
            qr_error=qr_error,
            tunnel=tunnel_state,
            active_semester_label=active_semester["label"],
            lesson_tabs=lesson_tabs,
            active_group_id=int(selected_group_id),
            group_names_display=group_names_display,
        )

    @app.get("/journal/lesson/<int:lesson_id>/qr/view")
    def journal_lesson_qr_view_page(lesson_id: int):
        lesson = db.session.get(JournalLesson, lesson_id)
        if not lesson:
            flash("Занятие не найдено", "error")
            return redirect(url_for("journal_page"))

        lesson_date = _parse_lesson_date(request.args.get("date"))
        if lesson_date is None:
            flash("Укажите корректную дату занятия", "error")
            return redirect(url_for("journal_page"))

        active_semester = _active_semester_base()
        
        from sqlalchemy import or_
        JournalLesson.query.filter(
            or_(
                JournalLesson.semester_key.is_(None),
                JournalLesson.semester_key == "",
                JournalLesson.semester_key != str(active_semester["key"]),
            )
        ).delete()
        db.session.commit()

        lesson_ctx, validation_error = _validate_lesson_date_for_attendance(lesson, lesson_date, active_semester)
        if validation_error:
            flash(validation_error, "error")
            return redirect(url_for("journal_page", date=lesson_date.isoformat()))

        course = db.session.get(Course, lesson.course_id)
        lesson_group_ids = _lesson_group_ids(lesson)
        lesson_groups_map = _groups_map(lesson_group_ids)
        ordered_group_ids = [gid for gid in lesson_group_ids if gid in lesson_groups_map]
        if course is None or not ordered_group_ids:
            flash("Связанные данные занятия не найдены", "error")
            return redirect(url_for("journal_page", date=lesson_date.isoformat()))

        active_group_id = parse_int(request.args.get("group_id"), default=0)
        if active_group_id not in ordered_group_ids:
            active_group_id = int(ordered_group_ids[0])

        qr_payload = _lesson_qr_payload(lesson, lesson_date)
        group_names_display = ", ".join(
            lesson_groups_map[int(gid)].name for gid in ordered_group_ids if int(gid) in lesson_groups_map
        )
        pair_info = _pair_info(lesson.pair_number)
        return render_template("journal/journal_qr_view.html",
            lesson=lesson,
            lesson_date_iso=lesson_date.isoformat(),
            lesson_ctx=lesson_ctx,
            course=course,
            pair_info=pair_info,
            qr=qr_payload,
            group_names_display=group_names_display,
            active_group_id=int(active_group_id),
            active_semester_label=active_semester["label"],
        )

    @app.get("/api/journal/lesson/<int:lesson_id>/attendance")
    def api_journal_lesson_attendance(lesson_id: int):
        lesson = db.session.get(JournalLesson, lesson_id)
        if not lesson:
            return jsonify({"success": False, "error": "Занятие не найдено"}), 404

        lesson_date = _parse_lesson_date(request.args.get("date"))
        if lesson_date is None:
            return jsonify({"success": False, "error": "Некорректная дата занятия"}), 400
        group_id = parse_int(request.args.get("group_id"), default=0)

        active_semester = _active_semester_base()
        _, validation_error = _validate_lesson_date_for_attendance(lesson, lesson_date, active_semester)
        if validation_error:
            return jsonify({"success": False, "error": validation_error}), 400

        return jsonify({"success": True, "attendance": _lesson_attendance_payload(lesson, lesson_date, group_id=group_id)})

    @app.get("/api/journal/lesson/<int:lesson_id>/qr")
    def api_journal_lesson_qr(lesson_id: int):
        lesson = db.session.get(JournalLesson, lesson_id)
        if not lesson:
            return jsonify({"success": False, "error": "Занятие не найдено"}), 404

        lesson_date = _parse_lesson_date(request.args.get("date"))
        if lesson_date is None:
            return jsonify({"success": False, "error": "Некорректная дата занятия"}), 400

        active_semester = _active_semester_base()
        _, validation_error = _validate_lesson_date_for_attendance(lesson, lesson_date, active_semester)
        if validation_error:
            return jsonify({"success": False, "error": validation_error}), 400

        return jsonify({"success": True, "qr": _lesson_qr_payload(lesson, lesson_date)})

    @app.get("/stream/journal/lesson/<int:lesson_id>/<lesson_date>/attendance")
    def stream_journal_lesson_attendance(lesson_id: int, lesson_date: str):
        lesson = db.session.get(JournalLesson, lesson_id)
        if not lesson:
            return jsonify({"success": False, "error": "Занятие не найдено"}), 404

        lesson_date_value = _parse_lesson_date(lesson_date)
        if lesson_date_value is None:
            return jsonify({"success": False, "error": "Некорректная дата занятия"}), 400
        group_id = parse_int(request.args.get("group_id"), default=0)

        active_semester = _active_semester_base()
        _, validation_error = _validate_lesson_date_for_attendance(lesson, lesson_date_value, active_semester)
        if validation_error:
            return jsonify({"success": False, "error": validation_error}), 400

        event_key = _event_key_lesson(lesson_id, lesson_date_value)

        @stream_with_context
        def generate():
            version = attendance_events.get_version(event_key)
            initial_payload = _lesson_attendance_payload(lesson, lesson_date_value, group_id=group_id)
            yield f"event: attendance\ndata: {json.dumps(initial_payload, ensure_ascii=False)}\n\n"

            while True:
                next_version = attendance_events.wait_for_change(event_key, version, timeout=30.0)
                if next_version == version:
                    yield ": keepalive\n\n"
                    continue
                version = next_version
                payload = _lesson_attendance_payload(lesson, lesson_date_value, group_id=group_id)
                yield f"event: attendance\ndata: {json.dumps(payload, ensure_ascii=False)}\n\n"

        headers = {
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
            "Connection": "keep-alive",
        }
        return Response(generate(), mimetype="text/event-stream", headers=headers)

    @app.post("/api/journal/qr/open")
    @app.post("/journal/qr/open")
    def journal_open_public_qr():
        if not is_local_request(request):
            return jsonify({"success": False, "error": "Открытие публичного QR доступно только локально"}), 403

        data = request.get_json(silent=True) or {}
        lesson_id = parse_int(data.get("lesson_id") or request.form.get("lesson_id"), default=0)
        lesson_date = _parse_lesson_date(data.get("date") or request.form.get("date"))

        lesson = db.session.get(JournalLesson, lesson_id) if lesson_id > 0 else None
        if not lesson or lesson_date is None:
            error_text = "Некорректные параметры занятия"
            if _is_ajax_request():
                return jsonify({"success": False, "error": error_text}), 400
            flash(error_text, "error")
            return redirect(url_for("journal_page"))

        active_semester = _active_semester_base()
        _, validation_error = _validate_lesson_date_for_attendance(lesson, lesson_date, active_semester)
        if validation_error:
            if _is_ajax_request():
                return jsonify({"success": False, "error": validation_error}), 400
            flash(validation_error, "error")
            return redirect(url_for("journal_page", date=lesson_date.isoformat()))

        intended_key = _public_session_key(lesson.id, lesson_date)
        _set_active_public_session_key(intended_key)
        local_port = _request_local_port(default=5000)
        ok, message = tunnel.open(local_port=local_port, local_host="127.0.0.1")
        snap = tunnel.snapshot()
        if not ok and bool(snap.get("active")):
            ok = True
            message = "Публичный туннель запускается. Подождите несколько секунд."
        if ok:
            _bump_lesson_event(lesson.id, lesson_date)
            tunnel_events.bump("tunnel")
        else:
            if not bool(snap.get("active")):
                _set_active_public_session_key("")

        if _is_ajax_request():
            payload = _lesson_qr_payload(lesson, lesson_date) if ok else {}
            response_payload = {"success": bool(ok), "message": message, "qr": payload}
            if not ok:
                response_payload["error"] = message
            return jsonify(response_payload)

        flash(message, "success" if ok else "error")
        return redirect(url_for("journal_lesson_page", lesson_id=lesson.id, date=lesson_date.isoformat()))

    @app.post("/api/journal/qr/close")
    @app.post("/journal/qr/close")
    def journal_close_public_qr():
        if not is_local_request(request):
            return jsonify({"success": False, "error": "Закрытие публичного QR доступно только локально"}), 403

        data = request.get_json(silent=True) or {}
        lesson_id = parse_int(data.get("lesson_id") or request.form.get("lesson_id"), default=0)
        lesson_date = _parse_lesson_date(data.get("date") or request.form.get("date"))

        ok, message = tunnel.close(manual=True)
        _set_active_public_session_key("")
        tunnel_events.bump("tunnel")

        if lesson_id > 0 and lesson_date is not None:
            _bump_lesson_event(lesson_id, lesson_date)

        if _is_ajax_request():
            response_payload = {"success": bool(ok), "message": message}
            if not ok:
                response_payload["error"] = message
            return jsonify(response_payload)

        if lesson_id > 0 and lesson_date is not None:
            flash(message, "success" if ok else "error")
            return redirect(url_for("journal_lesson_page", lesson_id=lesson_id, date=lesson_date.isoformat()))
        flash(message, "success" if ok else "error")
        return redirect(url_for("journal_page"))

    @app.post("/journal/lesson/<int:lesson_id>/attendance")
    def journal_set_attendance(lesson_id: int):
        ajax = _is_ajax_request()
        lesson = db.session.get(JournalLesson, lesson_id)
        if not lesson:
            if ajax:
                return jsonify({"success": False, "error": "Занятие не найдено"}), 404
            flash("Занятие не найдено", "error")
            return redirect(url_for("journal_page"))

        lesson_date = _parse_lesson_date(request.form.get("date"))
        if lesson_date is None:
            if ajax:
                return jsonify({"success": False, "error": "Некорректная дата занятия"}), 400
            flash("Некорректная дата занятия", "error")
            return redirect(url_for("journal_lesson_page", lesson_id=lesson_id))

        active_semester = _active_semester_base()
        _, validation_error = _validate_lesson_date_for_attendance(lesson, lesson_date, active_semester)
        if validation_error:
            if ajax:
                return jsonify({"success": False, "error": validation_error}), 400
            flash(validation_error, "error")
            return redirect(url_for("journal_page", date=lesson_date.isoformat()))

        lesson_group_ids = set(_lesson_group_ids(lesson))
        active_group_id = parse_int(request.form.get("group_id"), default=0)
        if active_group_id not in lesson_group_ids:
            active_group_id = 0

        student_id = parse_int(request.form.get("student_id"), default=0)
        student = db.session.get(Student, student_id)
        if not student or int(student.group_id) not in lesson_group_ids:
            if ajax:
                return jsonify({"success": False, "error": "Студент не найден в группе занятия"}), 404
            flash("Студент не найден в группе занятия", "error")
            redirect_kwargs = {"lesson_id": lesson_id, "date": lesson_date.isoformat()}
            if active_group_id > 0:
                redirect_kwargs["group_id"] = active_group_id
            return redirect(url_for("journal_lesson_page", **redirect_kwargs))

        status = _normalize_status(request.form.get("status"))
        if status is None:
            if ajax:
                return jsonify({"success": False, "error": "Некорректный статус посещаемости"}), 400
            flash("Некорректный статус посещаемости", "error")
            redirect_kwargs = {"lesson_id": lesson_id, "date": lesson_date.isoformat()}
            if active_group_id > 0:
                redirect_kwargs["group_id"] = active_group_id
            return redirect(url_for("journal_lesson_page", **redirect_kwargs))

        session_row = _get_or_create_session(lesson, lesson_date)

        record = JournalAttendance.query.filter_by(session_id=session_row.id, student_id=student.id).first()
        now_value = datetime.now(timezone.utc)
        source_ip = _request_ip()

        if record:
            record.status = status
            record.source = "manual"
            record.source_ip = source_ip
            record.marked_at = now_value
        else:
            db.session.add(
                JournalAttendance(
                    session_id=session_row.id,
                    student_id=student.id,
                    status=status,
                    source="manual",
                    source_ip=source_ip,
                    marked_at=now_value,
                )
            )

        db.session.commit()
        _bump_related_events(lesson_id, lesson_date)

        if ajax:
            payload = _lesson_attendance_payload(lesson, lesson_date, group_id=active_group_id)
            return jsonify({"success": True, "attendance": payload})

        flash(f"{student.fio}: {ATTENDANCE_STATUS_LABELS.get(status, status)}", "success")
        redirect_kwargs = {"lesson_id": lesson_id, "date": lesson_date.isoformat()}
        if active_group_id > 0:
            redirect_kwargs["group_id"] = active_group_id
        return redirect(url_for("journal_lesson_page", **redirect_kwargs))

    @app.post("/journal/lesson/<int:lesson_id>/qr/regenerate")
    def journal_regenerate_qr(lesson_id: int):
        lesson = db.session.get(JournalLesson, lesson_id)
        if not lesson:
            flash("Занятие не найдено", "error")
            return redirect(url_for("journal_page"))

        lesson_date = _parse_lesson_date(request.form.get("date"))
        if lesson_date is None:
            flash("Некорректная дата занятия", "error")
            return redirect(url_for("journal_lesson_page", lesson_id=lesson_id))

        active_semester = _active_semester_base()
        _, validation_error = _validate_lesson_date_for_attendance(lesson, lesson_date, active_semester)
        if validation_error:
            flash(validation_error, "error")
            return redirect(url_for("journal_page", date=lesson_date.isoformat()))

        session_row = _get_or_create_session(lesson, lesson_date)
        session_row.qr_token = _generate_qr_token()
        session_row.qr_token_created_at = datetime.now(timezone.utc)
        db.session.commit()
        _bump_lesson_event(lesson_id, lesson_date)

        if _is_ajax_request():
            return jsonify({"success": True, "qr": _lesson_qr_payload(lesson, lesson_date)})
        flash("QR-ссылка обновлена", "success")
        return redirect(url_for("journal_lesson_page", lesson_id=lesson_id, date=lesson_date.isoformat()))

    @app.route("/journal/checkin/<string:token>", methods=["GET", "POST"])
    def journal_checkin_page(token: str):
        safe_token = str(token or "").strip()
        session_row = (
            JournalLessonSession.query.filter(JournalLessonSession.qr_token == safe_token).order_by(JournalLessonSession.id.desc()).first()
            if safe_token
            else None
        )

        access_error = None
        if session_row is None:
            access_error = "Недействительная или устаревшая QR-ссылка. Попросите преподавателя обновить QR."

        lesson = db.session.get(JournalLesson, int(session_row.lesson_id)) if session_row else None
        if session_row and lesson is None:
            access_error = "Занятие для этой QR-ссылки не найдено."

        if session_row and lesson and not is_local_request(request):
            active_public_key = _get_active_public_session_key()
            current_key = _public_session_key(lesson.id, session_row.session_date)

            snap = tunnel.snapshot()
            snap_public_url = str(snap.get("public_url") or "").strip()
            snap_public_host = (urlparse(snap_public_url).hostname or "").lower() if snap_public_url else ""
            request_hosts = _request_host_candidates(request)
            tunnel_is_active = bool(snap.get("active")) and bool(snap_public_url)
            request_via_active_tunnel = tunnel_is_active and snap_public_host and snap_public_host in request_hosts
            request_via_tunnel_host = any(_is_public_tunnel_host(host) for host in request_hosts)
            can_recover_active_key = bool(
                tunnel_is_active
                and (
                    request_via_active_tunnel
                    or request_via_tunnel_host
                    or not active_public_key
                    or not _active_public_session_exists(active_public_key)
                )
            )

            if can_recover_active_key and (not active_public_key or not _active_public_session_exists(active_public_key)):
                _set_active_public_session_key(current_key)
                active_public_key = current_key

            if not active_public_key or active_public_key != current_key:
                access_error = "Эта QR-ссылка сейчас неактивна. Попросите преподавателя открыть QR для текущего занятия."

        course = db.session.get(Course, int(lesson.course_id)) if lesson else None
        lesson_group_ids = _lesson_group_ids(lesson) if lesson else []
        lesson_groups_map = _groups_map(lesson_group_ids) if lesson_group_ids else {}
        ordered_group_ids = [gid for gid in lesson_group_ids if gid in lesson_groups_map]
        allowed_group_ids = set(ordered_group_ids)
        students = (
            Student.query.filter(Student.group_id.in_(ordered_group_ids)).order_by(Student.fio.asc()).all()
            if ordered_group_ids
            else []
        )
        student_options = []
        for student in students:
            gid = int(student.group_id)
            group_obj = lesson_groups_map.get(gid)
            student_options.append(
                {
                    "id": int(student.id),
                    "fio": student.fio,
                    "group_id": gid,
                    "group_name": group_obj.name if group_obj is not None else f"Группа #{gid}",
                }
            )
        group_names_display = ", ".join(
            lesson_groups_map[int(gid)].name for gid in ordered_group_ids if int(gid) in lesson_groups_map
        )
        has_multiple_groups = len(ordered_group_ids) > 1

        selected_student_id = None
        done_message = None
        done_type = "success"

        if request.method == "POST" and not access_error and lesson and session_row and ordered_group_ids:
            student_id = parse_int(request.form.get("student_id"), default=0)
            selected_student_id = student_id
            student = db.session.get(Student, student_id)
            if not student or int(student.group_id) not in allowed_group_ids:
                done_message = "Выберите себя из списка группы."
                done_type = "error"
            else:
                attendance = JournalAttendance.query.filter_by(session_id=session_row.id, student_id=student.id).first()
                if attendance and attendance.status == ATTENDANCE_STATUS_PRESENT:
                    done_message = f"{student.fio}, вы уже отмечены."
                    done_type = "info"
                else:
                    now_value = datetime.now(timezone.utc)
                    source_ip = _request_ip()
                    if attendance:
                        attendance.status = ATTENDANCE_STATUS_PRESENT
                        attendance.source = "qr"
                        attendance.source_ip = source_ip
                        attendance.marked_at = now_value
                    else:
                        db.session.add(
                            JournalAttendance(
                                session_id=session_row.id,
                                student_id=student.id,
                                status=ATTENDANCE_STATUS_PRESENT,
                                source="qr",
                                source_ip=source_ip,
                                marked_at=now_value,
                            )
                        )
                    db.session.commit()
                    _bump_related_events(lesson.id, session_row.session_date)
                    done_message = f"{student.fio}, отметка сохранена."
                    done_type = "success"

        lesson_date_iso = session_row.session_date.isoformat() if session_row and session_row.session_date else ""
        pair_info = _pair_info(lesson.pair_number if lesson else 0)

        return render_template("journal/journal_checkin.html",
            access_error=access_error,
            token=safe_token,
            session_row=session_row,
            lesson=lesson,
            course=course,
            students=student_options,
            selected_student_id=selected_student_id,
            done_message=done_message,
            done_type=done_type,
            lesson_date_iso=lesson_date_iso,
            pair_info=pair_info,
            group_names_display=group_names_display,
            has_multiple_groups=has_multiple_groups,
        )
