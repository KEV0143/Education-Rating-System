from flask import jsonify, request, url_for
from utils.journal.helpers import (
    VALID_DAY_IDS,
    VALID_PAIR_NUMBERS,
    _parse_lesson_date,
    _active_semester_base,
    _date_context,
    _stage_add_error,
    _unique_group_ids,
    _normalize_group_ids_csv
)
from utils.journal._shared import (
    _student_count_map,
    _lesson_group_ids,
    _lesson_payload,
    _groups_map,
    _summary_for_session_groups
)


def _event_key_date(lesson_date) -> str:
    return f"date:{lesson_date.isoformat()}"


def _event_key_lesson(lesson_id: int, lesson_date) -> str:
    return f"lesson:{int(lesson_id)}:{lesson_date.isoformat()}"


def register_lesson_routes(app, db, Course, Group, Student, JournalLesson, JournalLessonSession, JournalAttendance, parse_int, ctx):
    runtime = ctx['runtime']
    attendance_events = ctx['attendance_events']

    _last_cleanup_time = 0.0

    def _cleanup_outdated_lessons(active_semester_key: str) -> None:
        nonlocal _last_cleanup_time
        import time
        from sqlalchemy import or_
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

    def _bump_date_event(lesson_date) -> None:
        attendance_events.bump(_event_key_date(lesson_date))

    def _bump_lesson_event(lesson_id: int, lesson_date) -> None:
        attendance_events.bump(_event_key_lesson(lesson_id, lesson_date))

    def _bump_related_events(lesson_id: int, lesson_date) -> None:
        _bump_date_event(lesson_date)
        _bump_lesson_event(lesson_id, lesson_date)

    def _get_active_public_session_key() -> str:
        return str(runtime.get("active_public_session_key") or "")

    def _set_active_public_session_key(value: str) -> None:
        runtime["active_public_session_key"] = str(value or "").strip()

    def _public_session_key(lesson_id: int, lesson_date) -> str:
        return f"{int(lesson_id)}:{lesson_date.isoformat()}"

    def _validate_lesson_date_for_attendance(lesson, lesson_date, active_semester):
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

    def _session_by_lesson_date(lesson, lesson_date):
        return JournalLessonSession.query.filter_by(lesson_id=lesson.id, session_date=lesson_date).first()

    @app.post("/api/journal/lessons")
    def api_journal_add_lesson():
        data = request.get_json(silent=True) or {}

        lesson_date = _parse_lesson_date(data.get("date"))
        if lesson_date is None:
            return jsonify({"success": False, "error": "Укажите корректную дату занятия"}), 400

        lesson_ctx = _date_context(lesson_date)
        if not lesson_ctx:
            return jsonify({"success": False, "error": "Не удалось определить параметры даты"}), 400

        day_of_week = int(lesson_ctx["day_of_week"])
        if day_of_week == 7:
            return jsonify({"success": False, "error": "Воскресенье недоступно для добавления пары"}), 400

        active_semester = _active_semester_base()
        active_semester_key = str(active_semester["key"])
        if str(lesson_ctx["semester_key"]) != active_semester_key:
            return (
                jsonify(
                    {
                        "success": False,
                        "error": f"Можно добавлять занятия только для активного семестра: {active_semester['label']}",
                    }
                ),
                400,
            )

        if str(lesson_ctx.get("stage")) not in ("classes_autumn", "classes_spring"):
            return jsonify({"success": False, "error": _stage_add_error(lesson_ctx)}), 400

        _cleanup_outdated_lessons(active_semester_key)

        week_parity = str(lesson_ctx["week_parity"] or "")
        semester_key = str(lesson_ctx["semester_key"])

        pair_number = parse_int(data.get("pair_number"), default=0)
        course_id = parse_int(data.get("course_id"), default=0)
        group_ids = []
        raw_group_ids = data.get("group_ids")
        if isinstance(raw_group_ids, list):
            for raw in raw_group_ids:
                gid = parse_int(raw, default=0)
                if gid > 0 and gid not in group_ids:
                    group_ids.append(int(gid))
        elif isinstance(raw_group_ids, str):
            for raw in raw_group_ids.split(","):
                gid = parse_int(raw, default=0)
                if gid > 0 and gid not in group_ids:
                    group_ids.append(int(gid))

        if not group_ids:
            single_group_id = parse_int(data.get("group_id"), default=0)
            if single_group_id > 0:
                group_ids = [int(single_group_id)]
        room = str(data.get("room") or "").strip()

        if day_of_week not in VALID_DAY_IDS:
            return jsonify({"success": False, "error": "Некорректный день недели"}), 400
        if pair_number not in VALID_PAIR_NUMBERS:
            return jsonify({"success": False, "error": "Некорректный номер пары"}), 400
        if not room:
            return jsonify({"success": False, "error": "Укажите номер аудитории"}), 400

        room = room[:40]

        course = db.session.get(Course, course_id)
        if not course:
            return jsonify({"success": False, "error": "Предмет не найден"}), 404
        if course.archived:
            return jsonify({"success": False, "error": "Нельзя добавить архивный предмет"}), 400

        if not group_ids:
            return jsonify({"success": False, "error": "Выберите хотя бы одну группу"}), 400

        groups = Group.query.filter(Group.id.in_(group_ids)).order_by(Group.name.asc()).all()
        groups_by_id = {int(group.id): group for group in groups}
        missing_groups = [gid for gid in group_ids if gid not in groups_by_id]
        if missing_groups:
            return jsonify({"success": False, "error": "Одна или несколько групп не найдены"}), 404

        conflict_group_ids = _slot_group_conflicts(
            semester_key=semester_key,
            week_parity=week_parity,
            day_of_week=day_of_week,
            pair_number=pair_number,
            group_ids=group_ids,
            exclude_lesson_id=0,
        )
        if conflict_group_ids:
            duplicate_list = ", ".join(
                sorted({groups_by_id[int(gid)].name for gid in conflict_group_ids if int(gid) in groups_by_id})
            )
            return (
                jsonify(
                    {
                        "success": False,
                        "error": f"Для выбранной пары уже есть занятия у групп: {duplicate_list}",
                    }
                ),
                409,
            )

        lesson = JournalLesson(
            week_parity=week_parity,
            day_of_week=day_of_week,
            pair_number=pair_number,
            semester_key=semester_key,
            course_id=course.id,
            group_id=int(group_ids[0]),
            group_ids=_normalize_group_ids_csv(group_ids),
            room=room,
        )
        db.session.add(lesson)
        db.session.commit()
        _bump_date_event(lesson_date)

        student_counts = _student_count_map()
        group_names_map = {int(group.id): group.name for group in groups}
        student_count = sum(int(student_counts.get(int(gid), 0)) for gid in _lesson_group_ids(lesson))
        payload = _lesson_payload(
            lesson,
            {int(course.id): course.title},
            group_names_map,
            student_counts,
            present_count=0,
            absent_count=student_count,
            excused_count=0,
            attendance_url="",
            attendance_date="",
        )

        return (
            jsonify(
                {
                    "success": True,
                    "lesson": payload,
                    "lessons": [payload],
                    "created_count": 1,
                }
            ),
            201,
        )

    @app.post("/api/journal/lessons/<int:lesson_id>/update")
    def api_journal_update_lesson(lesson_id: int):
        lesson = db.session.get(JournalLesson, lesson_id)
        if not lesson:
            return jsonify({"success": False, "error": "Занятие не найдено"}), 404

        data = request.get_json(silent=True) or {}
        lesson_date = _parse_lesson_date(
            data.get("date") or request.form.get("date") or request.args.get("date")
        )
        if lesson_date is None:
            return jsonify({"success": False, "error": "Укажите корректную дату занятия"}), 400

        active_semester = _active_semester_base()
        _cleanup_outdated_lessons(str(active_semester["key"]))
        _, validation_error = _validate_lesson_date_for_attendance(lesson, lesson_date, active_semester)
        if validation_error:
            return jsonify({"success": False, "error": validation_error}), 400

        pair_number = parse_int(data.get("pair_number"), default=0)
        course_id = parse_int(data.get("course_id"), default=0)
        room = str(data.get("room") or "").strip()

        raw_group_ids = data.get("group_ids")
        group_ids = []
        if isinstance(raw_group_ids, list):
            group_ids = _unique_group_ids(raw_group_ids)
        elif isinstance(raw_group_ids, str):
            group_ids = _unique_group_ids(raw_group_ids.split(","))
        if not group_ids:
            fallback_group_id = parse_int(data.get("group_id"), default=0)
            if fallback_group_id > 0:
                group_ids = [int(fallback_group_id)]

        if pair_number not in VALID_PAIR_NUMBERS:
            return jsonify({"success": False, "error": "Некорректный номер пары"}), 400
        if not room:
            return jsonify({"success": False, "error": "Укажите номер аудитории"}), 400
        room = room[:40]

        course = db.session.get(Course, course_id)
        if not course:
            return jsonify({"success": False, "error": "Предмет не найден"}), 404
        if course.archived:
            return jsonify({"success": False, "error": "Нельзя выбрать архивный предмет"}), 400

        if not group_ids:
            return jsonify({"success": False, "error": "Выберите хотя бы одну группу"}), 400

        groups = Group.query.filter(Group.id.in_(group_ids)).order_by(Group.name.asc()).all()
        groups_by_id = {int(group.id): group for group in groups}
        missing_groups = [gid for gid in group_ids if gid not in groups_by_id]
        if missing_groups:
            return jsonify({"success": False, "error": "Одна или несколько групп не найдены"}), 404

        existing_group_ids = _lesson_group_ids(lesson)
        has_history = (
            db.session.query(JournalLessonSession.id)
            .filter(JournalLessonSession.lesson_id == lesson.id)
            .first()
            is not None
        )
        if has_history and not set(existing_group_ids).issubset(set(group_ids)):
            removed_ids = [gid for gid in existing_group_ids if gid not in set(group_ids)]
            removed_groups_map = _groups_map(removed_ids)
            removed_names = ", ".join(
                sorted(
                    {
                        (removed_groups_map.get(int(gid)).name if removed_groups_map.get(int(gid)) else f"Группа #{gid}")
                        for gid in removed_ids
                    }
                )
            )
            return (
                jsonify(
                    {
                        "success": False,
                        "error": (
                            "Для занятия с сохраненной историей нельзя убирать существующие группы. "
                            f"Можно добавить новые. Удаляемые группы: {removed_names}"
                        ),
                    }
                ),
                409,
            )

        conflict_group_ids = _slot_group_conflicts(
            semester_key=str(lesson.semester_key),
            week_parity=str(lesson.week_parity),
            day_of_week=int(lesson.day_of_week),
            pair_number=int(pair_number),
            group_ids=group_ids,
            exclude_lesson_id=int(lesson.id),
        )
        if conflict_group_ids:
            conflict_names = ", ".join(
                sorted({groups_by_id[int(gid)].name for gid in conflict_group_ids if int(gid) in groups_by_id})
            )
            return (
                jsonify(
                    {
                        "success": False,
                        "error": f"Для выбранной пары уже есть занятия у групп: {conflict_names}",
                    }
                ),
                409,
            )

        lesson.pair_number = int(pair_number)
        lesson.course_id = int(course.id)
        lesson.group_id = int(group_ids[0])
        lesson.group_ids = _normalize_group_ids_csv(group_ids)
        lesson.room = room
        db.session.commit()
        _bump_related_events(lesson.id, lesson_date)

        student_counts = _student_count_map()
        all_group_names = {int(group_id): name for group_id, name in db.session.query(Group.id, Group.name).all()}
        overall_summary = _summary_for_session_groups(
            _session_by_lesson_date(lesson, lesson_date),
            _lesson_group_ids(lesson),
            student_counts,
        )
        payload = _lesson_payload(
            lesson,
            {int(course.id): course.title},
            all_group_names,
            student_counts,
            present_count=overall_summary["present_count"],
            absent_count=overall_summary["absent_count"],
            excused_count=overall_summary["excused_count"],
            attendance_url=url_for("journal_lesson_page", lesson_id=lesson.id, date=lesson_date.isoformat()),
            attendance_date=lesson_date.isoformat(),
        )
        return jsonify({"success": True, "lesson": payload})

    @app.post("/api/journal/lessons/<int:lesson_id>/delete")
    def api_journal_delete_lesson(lesson_id: int):
        lesson = db.session.get(JournalLesson, lesson_id)
        if not lesson:
            return jsonify({"success": False, "error": "Занятие не найдено"}), 404

        data = request.get_json(silent=True) or {}
        lesson_date = _parse_lesson_date(
            data.get("date") or request.form.get("date") or request.args.get("date")
        )

        db.session.delete(lesson)
        db.session.commit()

        if lesson_date is not None:
            _bump_related_events(lesson_id, lesson_date)

        return jsonify({"success": True})

    @app.post("/api/journal/lessons/<int:lesson_id>/delete-scope")
    def api_journal_delete_lesson_scope(lesson_id: int):
        lesson = db.session.get(JournalLesson, lesson_id)
        if not lesson:
            return jsonify({"success": False, "error": "Занятие не найдено"}), 404

        data = request.get_json(silent=True) or {}
        lesson_date = _parse_lesson_date(
            data.get("date") or request.form.get("date") or request.args.get("date")
        )
        scope = str(data.get("scope") or "").strip().lower() or "single"

        deleted_ids = []
        deleted_count = 0
        course_id = int(lesson.course_id)

        if scope == "single":
            deleted_ids = [int(lesson.id)]
            db.session.delete(lesson)
            deleted_count = 1
            message = "Занятие удалено"
        elif scope in {"course", "name", "all"}:
            lessons_to_delete = JournalLesson.query.filter_by(
                semester_key=str(lesson.semester_key),
                course_id=int(lesson.course_id),
            ).all()
            if not lessons_to_delete:
                return jsonify({"success": False, "error": "Занятия для удаления не найдены"}), 404
            for item in lessons_to_delete:
                deleted_ids.append(int(item.id))
                db.session.delete(item)
            deleted_count = len(deleted_ids)
            message = f"Удалено занятий по предмету: {deleted_count}"
            scope = "course"
        else:
            return jsonify({"success": False, "error": "Некорректный режим удаления"}), 400

        db.session.commit()

        if lesson_date is not None:
            _bump_date_event(lesson_date)
            for deleted_id in deleted_ids:
                _bump_lesson_event(deleted_id, lesson_date)

            active_key = _get_active_public_session_key()
            for deleted_id in deleted_ids:
                if active_key and active_key == _public_session_key(deleted_id, lesson_date):
                    _set_active_public_session_key("")
                    break

        return jsonify(
            {
                "success": True,
                "scope": scope,
                "deleted_count": int(deleted_count),
                "deleted_ids": [int(item_id) for item_id in deleted_ids],
                "course_id": int(course_id),
                "message": message,
            }
        )
