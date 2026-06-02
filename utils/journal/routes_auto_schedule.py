import json
import urllib.parse
import urllib.error
import time
from datetime import date, datetime, timedelta
from flask import jsonify, request, Response, url_for
from sqlalchemy import func, or_
from utils.journal.helpers import (
    AUTO_CAL_UPSTREAM,
    AUTO_CAL_TIMEOUT_SEC,
    AUTO_CAL_SEARCH_LIMIT_MAX,
    AUTO_CAL_TEACHER_TARGET,
    VALID_PAIR_NUMBERS,
    ATTENDANCE_STATUS_PRESENT,
    ATTENDANCE_STATUS_EXCUSED,
    _parse_lesson_date,
    _parse_datetime_iso,
    _date_context,
    _stage_add_error,
    _active_semester_base,
    _pair_number_from_datetime,
    _auto_cal_proxy_get,
    _clean_course_title_for_import,
    _normalize_course_title_for_import,
    _clean_group_name_for_import,
    _normalize_group_name_for_import,
    _unique_group_ids,
    _group_stream_years_from_ids,
    _lesson_group_ids,
    _lesson_primary_group_id,
    _normalize_group_ids_csv,
    MOSCOW_TZ
)


def _auto_cal_error_response(exc: urllib.error.HTTPError, default_message: str):
    details = ""
    status_code = int(getattr(exc, "code", 502) or 502)
    try:
        details = exc.read().decode("utf-8", errors="replace").strip()
    except Exception:
        details = ""
    message = details or f"HTTP {status_code}"
    return jsonify({"success": False, "error": default_message, "details": message[:1000]}), status_code


def register_journal_auto_schedule_routes(
    app,
    db,
    Course,
    Group,
    Student,
    JournalLesson,
    JournalLessonSession,
    JournalAttendance,
    parse_int,
):
    runtime = app.extensions.setdefault("journal_runtime", {})
    attendance_events = runtime.get("attendance_events")

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

    def _student_count_map():
        rows = db.session.query(Student.group_id, func.count(Student.id)).group_by(Student.group_id).all()
        return {int(group_id): int(count) for group_id, count in rows}

    def _slot_group_conflicts(semester_key: str, week_parity: str, day_of_week: int, pair_number: int, group_ids, exclude_lesson_id: int = 0):
        requested = set(_unique_group_ids(group_ids))
        if not requested:
            return []
        lessons = JournalLesson.query.filter_by(
            semester_key=str(semester_key),
            week_parity=str(week_parity),
            day_of_week=int(day_of_week),
            pair_number=int(pair_number),
        ).all()
        conflicts = set()
        for item in lessons:
            if exclude_lesson_id and int(item.id) == int(exclude_lesson_id):
                continue
            lesson_groups = set(_lesson_group_ids(item))
            overlaps = requested.intersection(lesson_groups)
            if overlaps:
                conflicts.update(overlaps)
        return sorted(conflicts)

    def _summary_for_session_groups(session_row, group_ids, student_counts):
        ids = _unique_group_ids(group_ids)
        total_students = sum(int(student_counts.get(int(gid), 0)) for gid in ids)
        present_count = 0
        excused_count = 0
        if session_row and ids:
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

    def _bump_date_event(lesson_date: date) -> None:
        if attendance_events:
            attendance_events.bump(f"date:{lesson_date.isoformat()}")

    @app.get("/api/journal/auto-calendar/search")
    def api_journal_auto_calendar_search():
        query_text = str(request.args.get("q") or request.args.get("match") or "").strip()
        if len(query_text) < 2:
            return jsonify({"success": True, "data": []})

        requested_limit = parse_int(request.args.get("limit"), default=12)
        if requested_limit <= 0:
            requested_limit = 12
        requested_limit = min(int(requested_limit), AUTO_CAL_SEARCH_LIMIT_MAX)

        upstream_limit = min(max(requested_limit * 6, 30), 100)
        result = []
        seen_ids = set()
        next_page_token = ""
        max_pages = 6

        for _ in range(max_pages):
            query_params = {"limit": upstream_limit, "match": query_text}
            if next_page_token:
                query_params["pageToken"] = next_page_token

            query_string = urllib.parse.urlencode(query_params)
            upstream_url = f"{AUTO_CAL_UPSTREAM}/schedule/api/search?{query_string}"
            try:
                payload_bytes, _ = _auto_cal_proxy_get(upstream_url, headers={"Accept": "application/json"})
                payload = json.loads(payload_bytes.decode("utf-8", errors="replace"))
            except urllib.error.HTTPError as exc:
                return _auto_cal_error_response(exc, "Не удалось выполнить поиск преподавателей")
            except Exception as exc:
                return jsonify(
                    {
                        "success": False,
                        "error": "Не удалось выполнить поиск преподавателей",
                        "details": str(exc),
                    }
                ), 502

            payload_items = payload.get("data", [])
            if not isinstance(payload_items, list):
                payload_items = []

            for item in payload_items:
                if not isinstance(item, dict):
                    continue
                teacher_id = parse_int(item.get("id"), default=0)
                schedule_target = parse_int(item.get("scheduleTarget"), default=0)
                if teacher_id <= 0 or schedule_target != AUTO_CAL_TEACHER_TARGET:
                    continue
                if int(teacher_id) in seen_ids:
                    continue
                seen_ids.add(int(teacher_id))

                target_title = str(item.get("targetTitle") or "").strip()
                full_title = str(item.get("fullTitle") or target_title).strip()
                result.append(
                    {
                        "id": int(teacher_id),
                        "scheduleTarget": int(schedule_target),
                        "targetTitle": target_title,
                        "fullTitle": full_title,
                        "iCalLink": str(item.get("iCalLink") or "").strip(),
                        "scheduleImageLink": str(item.get("scheduleImageLink") or "").strip(),
                        "scheduleUpdateImageLink": str(item.get("scheduleUpdateImageLink") or "").strip(),
                    }
                )
                if len(result) >= requested_limit:
                    break

            if len(result) >= requested_limit:
                break

            next_page_token = str(payload.get("nextPageToken") or "").strip()
            if not next_page_token:
                break

        if len(result) > requested_limit:
            result = result[:requested_limit]
        return jsonify({"success": True, "data": result})

    @app.get("/api/journal/auto-calendar/baseinfo")
    def api_journal_auto_calendar_baseinfo():
        teacher_id = parse_int(request.args.get("id"), default=0)
        if teacher_id <= 0:
            return jsonify({"success": False, "error": "Некорректный id преподавателя"}), 400

        query_string = urllib.parse.urlencode({"id": int(teacher_id), "type": AUTO_CAL_TEACHER_TARGET})
        upstream_url = f"{AUTO_CAL_UPSTREAM}/schedule/api/baseinfo?{query_string}"
        try:
            payload_bytes, _ = _auto_cal_proxy_get(upstream_url, headers={"Accept": "application/json"})
            payload = json.loads(payload_bytes.decode("utf-8", errors="replace"))
        except urllib.error.HTTPError as exc:
            return _auto_cal_error_response(exc, "Не удалось получить данные преподавателя")
        except Exception as exc:
            return jsonify(
                {
                    "success": False,
                    "error": "Не удалось получить данные преподавателя",
                    "details": str(exc),
                }
            ), 502

        schedule_target = parse_int(payload.get("scheduleTarget"), default=0)
        payload_teacher_id = parse_int(payload.get("id"), default=0)
        if payload_teacher_id <= 0 or schedule_target != AUTO_CAL_TEACHER_TARGET:
            return jsonify({"success": False, "error": "Преподаватель не найден"}), 404

        target_title = str(payload.get("targetTitle") or "").strip()
        full_title = str(payload.get("fullTitle") or target_title).strip()
        teacher_payload = {
            "id": int(payload_teacher_id),
            "scheduleTarget": int(schedule_target),
            "targetTitle": target_title,
            "fullTitle": full_title,
            "iCalLink": str(payload.get("iCalLink") or "").strip(),
            "scheduleImageLink": str(payload.get("scheduleImageLink") or "").strip(),
            "scheduleUpdateImageLink": str(payload.get("scheduleUpdateImageLink") or "").strip(),
        }
        return jsonify({"success": True, "teacher": teacher_payload})

    @app.get("/api/journal/auto-calendar/ical")
    def api_journal_auto_calendar_ical():
        teacher_id = parse_int(request.args.get("id"), default=0)
        if teacher_id <= 0:
            return jsonify({"success": False, "error": "Некорректный id преподавателя"}), 400

        upstream_url = f"{AUTO_CAL_UPSTREAM}/schedule/api/ical/{AUTO_CAL_TEACHER_TARGET}/{int(teacher_id)}?includeMeta=true"
        try:
            payload_bytes, _ = _auto_cal_proxy_get(
                upstream_url,
                headers={
                    "Accept": "text/calendar,*/*",
                    "Client-Name": "schedule-ui",
                },
            )
        except urllib.error.HTTPError as exc:
            return _auto_cal_error_response(exc, "Не удалось получить iCal-файл преподавателя")
        except Exception as exc:
            return jsonify(
                {
                    "success": False,
                    "error": "Не удалось получить iCal-файл преподавателя",
                    "details": str(exc),
                }
            ), 502

        filename = f"mirea-teacher-{int(teacher_id)}.ics"
        return Response(
            payload_bytes,
            status=200,
            headers={"Content-Disposition": f'inline; filename="{filename}"'},
            content_type="text/calendar; charset=utf-8",
        )

    @app.get("/api/journal/auto-calendar/schedule")
    def api_journal_auto_calendar_schedule():
        teacher_id = parse_int(request.args.get("teacher_id") or request.args.get("id"), default=0)
        if teacher_id <= 0:
            return jsonify({"success": False, "error": "Некорректный id преподавателя"}), 400

        lesson_date = _parse_lesson_date(request.args.get("date"))
        if lesson_date is None:
            return jsonify({"success": False, "error": "Укажите корректную дату (YYYY-MM-DD)"}), 400

        try:
            from utils.journal.ics_parser import MOSCOW_TZ as AUTO_SCHEDULE_MOSCOW_TZ, parse_ical_lessons
        except Exception as exc:
            return jsonify(
                {
                    "success": False,
                    "error": "Модуль парсинга расписания недоступен",
                    "details": str(exc),
                }
            ), 500

        upstream_url = f"{AUTO_CAL_UPSTREAM}/schedule/api/ical/{AUTO_CAL_TEACHER_TARGET}/{int(teacher_id)}?includeMeta=true"
        try:
            payload_bytes, _ = _auto_cal_proxy_get(
                upstream_url,
                headers={
                    "Accept": "text/calendar,*/*",
                    "Client-Name": "schedule-ui",
                },
            )
            ics_text = payload_bytes.decode("utf-8", errors="replace")
            range_start = datetime(lesson_date.year, lesson_date.month, lesson_date.day, tzinfo=AUTO_SCHEDULE_MOSCOW_TZ)
            range_end = range_start + timedelta(days=1)
            lessons = parse_ical_lessons(ics_text, range_start, range_end)
        except urllib.error.HTTPError as exc:
            return _auto_cal_error_response(exc, "Не удалось получить расписание преподавателя")
        except Exception as exc:
            return jsonify(
                {
                    "success": False,
                    "error": "Не удалось обработать расписание преподавателя",
                    "details": str(exc),
                }
            ), 502

        return jsonify(
            {
                "success": True,
                "teacher_id": int(teacher_id),
                "date": lesson_date.isoformat(),
                "lessons": lessons,
            }
        )

    @app.get("/api/journal/auto-calendar/month-counts")
    def api_journal_auto_calendar_month_counts():
        teacher_id = parse_int(request.args.get("teacher_id") or request.args.get("id"), default=0)
        if teacher_id <= 0:
            return jsonify({"success": False, "error": "Некорректный id преподавателя"}), 400

        raw_month = str(request.args.get("month") or "").strip()
        if not raw_month or len(raw_month) != 7 or "-" not in raw_month:
            return jsonify({"success": False, "error": "Укажите месяц в формате YYYY-MM"}), 400

        try:
            year_text, month_text = raw_month.split("-", 1)
            year_value = int(year_text)
            month_value = int(month_text)
            month_start = date(year_value, month_value, 1)
        except Exception:
            return jsonify({"success": False, "error": "Укажите месяц в формате YYYY-MM"}), 400

        if month_value == 12:
            month_end = date(year_value + 1, 1, 1)
        else:
            month_end = date(year_value, month_value + 1, 1)

        try:
            from utils.journal.ics_parser import MOSCOW_TZ as AUTO_SCHEDULE_MOSCOW_TZ, parse_ical_lessons
        except Exception as exc:
            return jsonify(
                {
                    "success": False,
                    "error": "Модуль парсинга расписания недоступен",
                    "details": str(exc),
                }
            ), 500

        upstream_url = f"{AUTO_CAL_UPSTREAM}/schedule/api/ical/{AUTO_CAL_TEACHER_TARGET}/{int(teacher_id)}?includeMeta=true"
        try:
            payload_bytes, _ = _auto_cal_proxy_get(
                upstream_url,
                headers={
                    "Accept": "text/calendar,*/*",
                    "Client-Name": "schedule-ui",
                },
            )
            ics_text = payload_bytes.decode("utf-8", errors="replace")
            range_start = datetime(month_start.year, month_start.month, month_start.day, tzinfo=AUTO_SCHEDULE_MOSCOW_TZ)
            range_end = datetime(month_end.year, month_end.month, month_end.day, tzinfo=AUTO_SCHEDULE_MOSCOW_TZ)
            lessons = parse_ical_lessons(ics_text, range_start, range_end)
        except urllib.error.HTTPError as exc:
            return _auto_cal_error_response(exc, "Не удалось получить расписание преподавателя")
        except Exception as exc:
            return jsonify(
                {
                    "success": False,
                    "error": "Не удалось обработать расписание преподавателя",
                    "details": str(exc),
                }
            ), 502

        counts_map = {}
        pair_map = {}
        for lesson in lessons:
            start_value = _parse_datetime_iso((lesson or {}).get("start"))
            if start_value is None:
                continue
            day_key = start_value.date().isoformat()
            pair_number = _pair_number_from_datetime(start_value)
            if pair_number <= 0:
                pair_number = int(start_value.hour * 60 + start_value.minute)

            if day_key not in pair_map:
                pair_map[day_key] = set()
            pair_map[day_key].add(pair_number)

        for day_key, pair_set in pair_map.items():
            counts_map[day_key] = int(len(pair_set))

        return jsonify(
            {
                "success": True,
                "teacher_id": int(teacher_id),
                "month": raw_month,
                "counts": counts_map,
            }
        )

    @app.get("/api/journal/auto-calendar/weekly-stats")
    def api_journal_auto_calendar_weekly_stats():
        teacher_id = parse_int(request.args.get("teacher_id") or request.args.get("id"), default=0)
        if teacher_id <= 0:
            return jsonify({"success": False, "error": "Некорректный id преподавателя"}), 400

        requested_date = _parse_lesson_date(request.args.get("date"))
        if requested_date is None:
            requested_date = datetime.now(MOSCOW_TZ).date()

        week_start_date = requested_date - timedelta(days=int(requested_date.weekday()))
        week_end_date = week_start_date + timedelta(days=6)

        try:
            from utils.journal.ics_parser import MOSCOW_TZ as AUTO_SCHEDULE_MOSCOW_TZ, parse_ical_lessons
        except Exception as exc:
            return jsonify(
                {
                    "success": False,
                    "error": "Модуль парсинга расписания недоступен",
                    "details": str(exc),
                }
            ), 500

        upstream_url = f"{AUTO_CAL_UPSTREAM}/schedule/api/ical/{AUTO_CAL_TEACHER_TARGET}/{int(teacher_id)}?includeMeta=true"
        try:
            payload_bytes, _ = _auto_cal_proxy_get(
                upstream_url,
                headers={
                    "Accept": "text/calendar,*/*",
                    "Client-Name": "schedule-ui",
                },
            )
            ics_text = payload_bytes.decode("utf-8", errors="replace")
            range_start = datetime(
                week_start_date.year,
                week_start_date.month,
                week_start_date.day,
                tzinfo=AUTO_SCHEDULE_MOSCOW_TZ,
            )
            range_end = datetime(
                (week_end_date + timedelta(days=1)).year,
                (week_end_date + timedelta(days=1)).month,
                (week_end_date + timedelta(days=1)).day,
                tzinfo=AUTO_SCHEDULE_MOSCOW_TZ,
            )
            lessons = parse_ical_lessons(ics_text, range_start, range_end)
        except urllib.error.HTTPError as exc:
            return _auto_cal_error_response(exc, "Не удалось получить расписание преподавателя")
        except Exception as exc:
            return jsonify(
                {
                    "success": False,
                    "error": "Не удалось обработать расписание преподавателя",
                    "details": str(exc),
                }
            ), 502

        total_pairs = int(len(lessons))
        total_minutes = 0
        unique_days = set()
        for lesson in lessons:
            start_dt = _parse_datetime_iso((lesson or {}).get("start"))
            end_dt = _parse_datetime_iso((lesson or {}).get("end"))
            if start_dt is not None:
                unique_days.add(start_dt.date().isoformat())
            if start_dt is None or end_dt is None or end_dt <= start_dt:
                total_minutes += 95
                continue
            total_minutes += int(max((end_dt - start_dt).total_seconds() // 60, 0))

        total_hours = round(float(total_minutes) / 60.0, 1)
        unique_days = sorted(unique_days)

        return jsonify(
            {
                "success": True,
                "teacher_id": int(teacher_id),
                "week_start": week_start_date.isoformat(),
                "week_end": week_end_date.isoformat(),
                "week_pairs": total_pairs,
                "week_hours": total_hours,
                "week_minutes": int(total_minutes),
                "teaching_days": int(len(unique_days)),
            }
        )

    @app.post("/api/journal/auto-calendar/import-lesson")
    def api_journal_auto_calendar_import_lesson():
        data = request.get_json(silent=True) or {}
        lesson_payload = data.get("lesson") if isinstance(data.get("lesson"), dict) else {}

        lesson_date = _parse_lesson_date(data.get("date"))
        if lesson_date is None:
            start_date_dt = _parse_datetime_iso(lesson_payload.get("start"))
            if start_date_dt is not None:
                lesson_date = start_date_dt.date()
        if lesson_date is None:
            return jsonify({"success": False, "error": "Укажите корректную дату (YYYY-MM-DD)"}), 400

        lesson_ctx = _date_context(lesson_date)
        if not lesson_ctx:
            return jsonify({"success": False, "error": "Не удалось определить параметры даты"}), 400
        if int(lesson_ctx.get("day_of_week") or 0) == 7:
            return jsonify({"success": False, "error": "Воскресенье недоступно"}), 400
        if str(lesson_ctx.get("stage")) not in ("classes_autumn", "classes_spring"):
            return jsonify({"success": False, "error": _stage_add_error(lesson_ctx)}), 400

        active_semester = _active_semester_base()
        active_semester_key = str(active_semester["key"])
        if str(lesson_ctx.get("semester_key") or "") != active_semester_key:
            return (
                jsonify(
                    {
                        "success": False,
                        "error": f"Можно импортировать занятия только для активного семестра: {active_semester['label']}",
                    }
                ),
                400,
            )

        start_dt = _parse_datetime_iso(lesson_payload.get("start"))
        pair_number = _pair_number_from_datetime(start_dt)
        if pair_number not in VALID_PAIR_NUMBERS:
            return jsonify({"success": False, "error": "Не удалось определить номер пары"}), 400

        discipline_title = str(
            lesson_payload.get("discipline")
            or lesson_payload.get("title")
            or data.get("title")
            or ""
        ).strip()
        room_value = str(
            lesson_payload.get("location")
            or data.get("room")
            or "-"
        ).strip()
        if len(room_value) > 40:
            room_value = room_value[:40].rstrip() or "-"
        if not room_value:
            room_value = "-"

        groups_raw = lesson_payload.get("groups")
        if not isinstance(groups_raw, list):
            groups_raw = data.get("groups") if isinstance(data.get("groups"), list) else []

        _cleanup_outdated_lessons(active_semester_key)

        safe_title = _clean_course_title_for_import(discipline_title) or "Импортированная дисциплина"
        target_normalized = _normalize_course_title_for_import(safe_title)

        course = None
        existing_courses = Course.query.filter(Course.archived.is_(False)).all()
        for c in existing_courses:
            if _normalize_course_title_for_import(str(c.title)) == target_normalized:
                course = c
                break

        if not course:
            archived_courses = Course.query.filter(Course.archived.is_(True)).all()
            for c in archived_courses:
                if _normalize_course_title_for_import(str(c.title)) == target_normalized:
                    course = c
                    break

        if not course:
            year_val = "2025-2026"
            sem_val = 1
            if active_semester_key and ":" in active_semester_key:
                parts = active_semester_key.split(":")
                year_val = parts[0]
                if len(parts) > 1 and parts[1].isdigit():
                     sem_val = int(parts[1])

            course = Course(
                title=safe_title,
                year=year_val,
                semester=sem_val,
                group_ids="",
                archived=True
            )
            db.session.add(course)
            db.session.flush()

        groups = []
        for raw in groups_raw or []:
            name = _clean_group_name_for_import(raw)
            if not name:
                continue
            norm = _normalize_group_name_for_import(name)

            matched_group = None
            for g in Group.query.all():
                if _normalize_group_name_for_import(str(g.name or "")) == norm:
                    matched_group = g
                    break

            if matched_group:
                groups.append(matched_group)
            else:
                new_group = Group(name=name)
                db.session.add(new_group)
                db.session.flush()
                groups.append(new_group)

        group_ids = _unique_group_ids([group.id for group in groups])
        if not group_ids:
            return jsonify({"success": False, "error": "Не удалось определить группу для занятия"}), 400

        semester_key = str(lesson_ctx["semester_key"])
        week_parity = str(lesson_ctx["week_parity"] or "")
        day_of_week = int(lesson_ctx["day_of_week"])

        target_lesson = None
        target_group_ids = list(group_ids)
        slot_lessons = JournalLesson.query.filter_by(
            semester_key=semester_key,
            week_parity=week_parity,
            day_of_week=day_of_week,
            pair_number=pair_number,
        ).all()

        requested_group_set = set(group_ids)
        requested_stream_years, _ = _group_stream_years_from_ids(group_ids)
        same_course_candidates = []
        for candidate in slot_lessons:
            candidate_group_ids = set(_lesson_group_ids(candidate))
            if not candidate_group_ids:
                continue
            if candidate_group_ids == requested_group_set:
                target_lesson = candidate
                target_group_ids = _lesson_group_ids(candidate)
                break
            if int(candidate.course_id) == int(course.id):
                same_course_candidates.append(candidate)

        if target_lesson is None and same_course_candidates:
            for candidate in same_course_candidates:
                candidate_group_ids = _lesson_group_ids(candidate)
                if set(candidate_group_ids).intersection(requested_group_set):
                    target_lesson = candidate
                    target_group_ids = _unique_group_ids(candidate_group_ids + group_ids)
                    break

        if target_lesson is None and same_course_candidates:
            for candidate in same_course_candidates:
                candidate_group_ids = _lesson_group_ids(candidate)
                candidate_stream_years, _candidate_names = _group_stream_years_from_ids(candidate_group_ids)
                if (
                    requested_stream_years
                    and candidate_stream_years
                    and requested_stream_years.intersection(candidate_stream_years)
                ):
                    target_lesson = candidate
                    target_group_ids = _unique_group_ids(candidate_group_ids + group_ids)
                    break

        if target_lesson is None and len(same_course_candidates) == 1:
            candidate = same_course_candidates[0]
            candidate_group_ids = _lesson_group_ids(candidate)
            target_lesson = candidate
            target_group_ids = _unique_group_ids(candidate_group_ids + group_ids)

        existing_course_groups = _unique_group_ids(str(course.group_ids or "").split(","))
        merged_course_groups = _unique_group_ids(existing_course_groups + target_group_ids)
        course.group_ids = ",".join(str(gid) for gid in merged_course_groups)

        if target_lesson is None:
            conflict_group_ids = _slot_group_conflicts(
                semester_key=semester_key,
                week_parity=week_parity,
                day_of_week=day_of_week,
                pair_number=pair_number,
                group_ids=target_group_ids,
                exclude_lesson_id=0,
            )
            if conflict_group_ids:
                group_name_map = {int(group.id): str(group.name) for group in Group.query.filter(Group.id.in_(conflict_group_ids)).all()}
                conflict_names = [group_name_map.get(int(gid), f"Группа #{int(gid)}") for gid in conflict_group_ids]
                return (
                    jsonify(
                        {
                            "success": False,
                            "error": f"Для выбранной пары уже есть занятия у групп: {', '.join(conflict_names)}",
                        }
                    ),
                    409,
                )

            target_lesson = JournalLesson(
                week_parity=week_parity,
                day_of_week=day_of_week,
                pair_number=pair_number,
                semester_key=semester_key,
                course_id=int(course.id),
                group_id=int(target_group_ids[0]),
                group_ids=_normalize_group_ids_csv(target_group_ids),
                room=room_value,
            )
            db.session.add(target_lesson)
        else:
            conflict_group_ids = _slot_group_conflicts(
                semester_key=semester_key,
                week_parity=week_parity,
                day_of_week=day_of_week,
                pair_number=pair_number,
                group_ids=target_group_ids,
                exclude_lesson_id=int(target_lesson.id),
            )
            if conflict_group_ids:
                group_name_map = {int(group.id): str(group.name) for group in Group.query.filter(Group.id.in_(conflict_group_ids)).all()}
                conflict_names = [group_name_map.get(int(gid), f"Группа #{int(gid)}") for gid in conflict_group_ids]
                return (
                    jsonify(
                        {
                            "success": False,
                            "error": f"Для выбранной пары уже есть занятия у групп: {', '.join(conflict_names)}",
                        }
                    ),
                    409,
                )
            target_lesson.course_id = int(course.id)
            target_lesson.group_id = int(target_group_ids[0])
            target_lesson.group_ids = _normalize_group_ids_csv(target_group_ids)
            target_lesson.room = room_value

        db.session.commit()
        _bump_date_event(lesson_date)

        course_titles = {int(course.id): str(course.title)}
        lesson_groups = _lesson_group_ids(target_lesson)
        group_names = {
            int(group.id): str(group.name)
            for group in Group.query.filter(Group.id.in_(lesson_groups)).all()
        }
        student_counts = _student_count_map()
        summary = _summary_for_session_groups(
            JournalLessonSession.query.filter_by(
                lesson_id=target_lesson.id,
                session_date=lesson_date,
            ).first(),
            lesson_groups,
            student_counts,
        )

        payload = _lesson_payload(
            target_lesson,
            course_titles=course_titles,
            group_names=group_names,
            student_counts=student_counts,
            present_count=int(summary.get("present_count") or 0),
            absent_count=int(summary.get("absent_count") or 0),
            excused_count=int(summary.get("excused_count") or 0),
            attendance_url=url_for("journal_lesson_page", lesson_id=target_lesson.id, date=lesson_date.isoformat()),
            attendance_date=lesson_date.isoformat(),
        )

        return jsonify({"success": True, "lesson": payload})

    @app.get("/api/journal/auto-calendar/settings")
    def api_journal_auto_cal_get_settings():
        from utils.core.helpers import get_setting
        users = get_setting("journal_auto_cal_users", "[]")
        selected = get_setting("journal_auto_cal_selected", "[]")
        use_custom = get_setting("journal_auto_cal_use_custom", "1")
        return jsonify({
            "success": True,
            "users": json.loads(users),
            "selected": json.loads(selected),
            "use_custom": use_custom == "1"
        })

    @app.post("/api/journal/auto-calendar/settings")
    def api_journal_auto_cal_save_settings():
        from utils.core.helpers import set_setting
        data = request.get_json(silent=True) or {}
        users = data.get("users", [])
        selected = data.get("selected", [])
        use_custom = data.get("use_custom", True)

        set_setting("journal_auto_cal_users", json.dumps(users))
        set_setting("journal_auto_cal_selected", json.dumps(selected))
        set_setting("journal_auto_cal_use_custom", "1" if use_custom else "0")
        return jsonify({"success": True})
