from datetime import date, datetime

from flask import render_template, request, jsonify, abort


def register_practice_routes(
    app,
    db,
    Course,
    Group,
    Student,
    Practice,
    PracticeGrade,
    PracticeGroupInterval,
    parse_group_ids,
):

    def _get_course_or_404(course_id: int):
        c = db.session.get(Course, course_id)
        if not c:
            abort(404)
        return c

    def _get_practice_or_404(practice_id: int):
        p = db.session.get(Practice, practice_id)
        if not p:
            abort(404)
        return p

    def _ensure_group_in_course(course, group_id: int):
        gids = set(parse_group_ids(course.group_ids))
        if group_id not in gids:
            abort(400, description="Group is not attached to this course")

    def _validate_score(score, min_s: float, max_s: float):
        if score is None or score == "":
            return None
        try:
            val = float(score)
        except Exception:
            abort(400, description="Invalid score")
        if val < float(min_s) or val > float(max_s):
            abort(400, description=f"Score must be between {min_s} and {max_s}")
        return val

    def _sanitize_comment(comment: str) -> str:
        c = (comment or "").strip()
        if len(c) > 1000:
            c = c[:1000]
        return c

    def _dt_iso(dt):
        if not dt:
            return None
        try:
            return dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        except Exception:
            return None

    def _date_iso(d):
        if not d:
            return None
        try:
            return d.isoformat()
        except Exception:
            return None

    def _parse_optional_interval(start_raw, end_raw):
        start_s = str(start_raw or "").strip()
        end_s = str(end_raw or "").strip()

        if not start_s and not end_s:
            return None, None
        if not start_s or not end_s:
            abort(400, description="Both start_date and end_date are required")

        try:
            start_d = date.fromisoformat(start_s)
            end_d = date.fromisoformat(end_s)
        except Exception:
            abort(400, description="Invalid date interval")

        if end_d < start_d:
            abort(400, description="End date must be >= start date")

        return start_d, end_d

    def _effective_interval(practice, group_id: int):
        override = PracticeGroupInterval.query.filter_by(
            practice_id=practice.id,
            group_id=group_id,
        ).first()
        if override:
            return override.start_date, override.end_date, True
        return practice.start_date, practice.end_date, False

    def _save_group_interval(practice, group_id: int, start_d, end_d):
        override = PracticeGroupInterval.query.filter_by(
            practice_id=practice.id,
            group_id=group_id,
        ).first()

        if practice.start_date == start_d and practice.end_date == end_d:
            if override:
                db.session.delete(override)
            return start_d, end_d, False

        if not override:
            override = PracticeGroupInterval(practice_id=practice.id, group_id=group_id)
            db.session.add(override)

        override.start_date = start_d
        override.end_date = end_d
        return start_d, end_d, True

    @app.get("/course/<int:course_id>/assessments")
    def course_assessments(course_id: int):
        course = _get_course_or_404(course_id)

        group_ids = parse_group_ids(course.group_ids)
        groups = []
        if group_ids:
            groups = Group.query.filter(Group.id.in_(group_ids)).order_by(Group.name).all()

        practices = Practice.query.filter_by(course_id=course.id).order_by(Practice.id.asc()).all()

        return render_template(
            "course_assessments.html",
            course=course,
            groups=groups,
            practices=practices,
        )

    @app.post("/api/course/<int:course_id>/practice_create")
    def api_practice_create(course_id: int):
        course = _get_course_or_404(course_id)
        data = request.get_json(silent=True) or {}

        title = (data.get("title") or "").strip()
        if not title:
            return jsonify({"success": False, "error": "Title is required"}), 400
        if len(title) > 120:
            title = title[:120]

        try:
            min_score = float(data.get("min_score", 0))
            max_score = float(data.get("max_score", 10))
        except Exception:
            return jsonify({"success": False, "error": "Invalid min/max"}), 400

        if max_score < min_score:
            return jsonify({"success": False, "error": "Max must be >= Min"}), 400

        start_date, end_date = _parse_optional_interval(
            data.get("start_date"),
            data.get("end_date"),
        )

        p = Practice(
            course_id=course.id,
            title=title,
            min_score=min_score,
            max_score=max_score,
            start_date=start_date,
            end_date=end_date,
        )
        db.session.add(p)
        db.session.commit()
        return jsonify({"success": True, "practice": p.to_dict()})

    @app.post("/api/practice/<int:practice_id>/update")
    def api_practice_update(practice_id: int):
        p = _get_practice_or_404(practice_id)
        data = request.get_json(silent=True) or {}

        title = (data.get("title") or "").strip()
        if not title:
            return jsonify({"success": False, "error": "Title is required"}), 400
        if len(title) > 120:
            title = title[:120]

        try:
            min_score = float(data.get("min_score", p.min_score))
            max_score = float(data.get("max_score", p.max_score))
        except Exception:
            return jsonify({"success": False, "error": "Invalid min/max"}), 400

        if max_score < min_score:
            return jsonify({"success": False, "error": "Max must be >= Min"}), 400

        start_date, end_date = _parse_optional_interval(
            data.get("start_date", _date_iso(p.start_date)),
            data.get("end_date", _date_iso(p.end_date)),
        )

        p.title = title
        p.min_score = min_score
        p.max_score = max_score
        p.start_date = start_date
        p.end_date = end_date

        overrides = PracticeGroupInterval.query.filter_by(practice_id=p.id).all()
        for ov in overrides:
            if ov.start_date == start_date and ov.end_date == end_date:
                db.session.delete(ov)

        db.session.commit()
        return jsonify({"success": True, "practice": p.to_dict()})

    @app.post("/api/practice/<int:practice_id>/delete")
    def api_practice_delete(practice_id: int):
        p = _get_practice_or_404(practice_id)
        db.session.delete(p)
        db.session.commit()
        return jsonify({"success": True})

    @app.get("/api/practice/<int:practice_id>/group/<int:group_id>/grades")
    def api_practice_group_grades(practice_id: int, group_id: int):
        practice = _get_practice_or_404(practice_id)
        course = _get_course_or_404(practice.course_id)

        _ensure_group_in_course(course, group_id)
        start_date, end_date, has_override = _effective_interval(practice, group_id)

        practice_payload = {
            "id": practice.id,
            "min_score": practice.min_score,
            "max_score": practice.max_score,
            "start_date": _date_iso(start_date),
            "end_date": _date_iso(end_date),
            "default_start_date": _date_iso(practice.start_date),
            "default_end_date": _date_iso(practice.end_date),
            "has_group_interval_override": has_override,
        }

        students = Student.query.filter_by(group_id=group_id).order_by(Student.fio).all()
        if not students:
            return jsonify({"success": True, "rows": [], "practice": practice_payload})

        student_ids = [s.id for s in students]

        grades = PracticeGrade.query.filter(
            PracticeGrade.practice_id == practice.id,
            PracticeGrade.student_id.in_(student_ids)
        ).all()
        gmap = {g.student_id: g for g in grades}

        rows = []
        for s in students:
            g = gmap.get(s.id)
            rows.append({
                "student_id": s.id,
                "fio": s.fio,
                "score": g.score if g else None,
                "comment": g.comment if g else "",
                "score_updated_at": _dt_iso(getattr(g, "score_updated_at", None)) if g else None,
                "comment_updated_at": _dt_iso(getattr(g, "comment_updated_at", None)) if g else None,
                "updated_at": _dt_iso(getattr(g, "updated_at", None)) if g else None,
            })

        return jsonify({"success": True, "rows": rows, "practice": practice_payload})

    @app.post("/api/practice/<int:practice_id>/group/<int:group_id>/interval")
    def api_practice_group_interval_update(practice_id: int, group_id: int):
        practice = _get_practice_or_404(practice_id)
        course = _get_course_or_404(practice.course_id)
        _ensure_group_in_course(course, group_id)

        data = request.get_json(silent=True) or {}
        start_d, end_d = _parse_optional_interval(
            data.get("start_date"),
            data.get("end_date"),
        )

        start_d, end_d, has_override = _save_group_interval(practice, group_id, start_d, end_d)
        db.session.commit()

        return jsonify({
            "success": True,
            "practice_id": practice.id,
            "group_id": group_id,
            "interval": {
                "start_date": _date_iso(start_d),
                "end_date": _date_iso(end_d),
            },
            "default_interval": {
                "start_date": _date_iso(practice.start_date),
                "end_date": _date_iso(practice.end_date),
            },
            "has_group_interval_override": has_override,
        })

    @app.post("/api/practice/<int:practice_id>/grade_one")
    def api_grade_one(practice_id: int):
        practice = _get_practice_or_404(practice_id)
        course = _get_course_or_404(practice.course_id)

        data = request.get_json(silent=True) or {}
        student_id = data.get("student_id")
        if not student_id:
            return jsonify({"success": False, "error": "student_id required"}), 400

        student = db.session.get(Student, int(student_id))
        if not student:
            return jsonify({"success": False, "error": "Student not found"}), 404

        _ensure_group_in_course(course, student.group_id)

        score = _validate_score(data.get("score"), practice.min_score, practice.max_score)
        comment = _sanitize_comment(data.get("comment", ""))

        grade = PracticeGrade.query.filter_by(practice_id=practice.id, student_id=student.id).first()
        if not grade:
            grade = PracticeGrade(practice_id=practice.id, student_id=student.id)
            db.session.add(grade)

        now = datetime.utcnow()

        old_score = grade.score
        old_comment = grade.comment or ""
        new_comment = comment or ""

        score_changed = (old_score != score)
        comment_changed = (old_comment != new_comment)

        grade.score = score
        grade.comment = new_comment

        if score_changed:
            grade.score_updated_at = now
        if comment_changed:
            grade.comment_updated_at = now

        db.session.commit()

        return jsonify({
            "success": True,
            "score_updated_at": _dt_iso(getattr(grade, "score_updated_at", None)),
            "comment_updated_at": _dt_iso(getattr(grade, "comment_updated_at", None)),
            "updated_at": _dt_iso(getattr(grade, "updated_at", None)),
        })

    @app.post("/api/practice/<int:practice_id>/grade_bulk_group")
    def api_grade_bulk_group(practice_id: int):
        practice = _get_practice_or_404(practice_id)
        course = _get_course_or_404(practice.course_id)

        data = request.get_json(silent=True) or {}
        group_id = data.get("group_id")
        if not group_id:
            return jsonify({"success": False, "error": "group_id required"}), 400
        group_id = int(group_id)

        _ensure_group_in_course(course, group_id)

        score = _validate_score(data.get("score"), practice.min_score, practice.max_score)
        comment = _sanitize_comment(data.get("comment", ""))

        students = Student.query.filter_by(group_id=group_id).all()
        if not students:
            return jsonify({"success": True, "updated": 0})

        student_ids = [s.id for s in students]
        existing = PracticeGrade.query.filter(
            PracticeGrade.practice_id == practice.id,
            PracticeGrade.student_id.in_(student_ids)
        ).all()
        gmap = {g.student_id: g for g in existing}

        updated = 0
        now = datetime.utcnow()
        for sid in student_ids:
            g = gmap.get(sid)
            if not g:
                g = PracticeGrade(practice_id=practice.id, student_id=sid)
                db.session.add(g)

            old_score = g.score
            old_comment = g.comment or ""

            g.score = score
            g.comment = comment

            if old_score != score:
                g.score_updated_at = now
            if old_comment != (comment or ""):
                g.comment_updated_at = now

            updated += 1

        db.session.commit()
        return jsonify({"success": True, "updated": updated})

    @app.post("/api/practice/<int:practice_id>/grade_bulk_students")
    def api_grade_bulk_students(practice_id: int):
        practice = _get_practice_or_404(practice_id)
        course = _get_course_or_404(practice.course_id)

        data = request.get_json(silent=True) or {}
        student_ids = data.get("student_ids") or []
        if not isinstance(student_ids, list) or not student_ids:
            return jsonify({"success": False, "error": "student_ids required"}), 400

        score = _validate_score(data.get("score"), practice.min_score, practice.max_score)
        comment = _sanitize_comment(data.get("comment", ""))

        students = Student.query.filter(Student.id.in_([int(x) for x in student_ids])).all()
        if not students:
            return jsonify({"success": True, "updated": 0})

        allowed_gids = set(parse_group_ids(course.group_ids))
        ok_ids = [s.id for s in students if s.group_id in allowed_gids]

        if not ok_ids:
            return jsonify({"success": True, "updated": 0})

        existing = PracticeGrade.query.filter(
            PracticeGrade.practice_id == practice.id,
            PracticeGrade.student_id.in_(ok_ids)
        ).all()
        gmap = {g.student_id: g for g in existing}

        updated = 0
        now = datetime.utcnow()
        for sid in ok_ids:
            g = gmap.get(sid)
            if not g:
                g = PracticeGrade(practice_id=practice.id, student_id=sid)
                db.session.add(g)

            old_score = g.score
            old_comment = g.comment or ""

            g.score = score
            g.comment = comment

            if old_score != score:
                g.score_updated_at = now
            if old_comment != (comment or ""):
                g.comment_updated_at = now

            updated += 1

        db.session.commit()
        return jsonify({"success": True, "updated": updated})

    @app.get("/api/course/<int:course_id>/student/<int:student_id>/stats")
    def api_student_stats(course_id: int, student_id: int):
        course = _get_course_or_404(course_id)
        allowed_gids = set(parse_group_ids(course.group_ids))

        student = db.session.get(Student, student_id)
        if not student:
            return jsonify({"success": False, "error": "Student not found"}), 404

        if student.group_id not in allowed_gids:
            return jsonify({"success": False, "error": "Student is not in this course"}), 403

        practices = Practice.query.filter_by(course_id=course.id).all()
        total_practices = len(practices)
        max_possible = float(sum(float(p.max_score) for p in practices)) if practices else 0.0

        if not practices:
            return jsonify({
                "success": True,
                "total_practices": 0,
                "completed_practices": 0,
                "total_score": 0,
                "max_possible": 0
            })

        practice_ids = [p.id for p in practices]
        grades = PracticeGrade.query.filter(
            PracticeGrade.practice_id.in_(practice_ids),
            PracticeGrade.student_id == student.id
        ).all()

        grade_map = {g.practice_id: g for g in grades}

        completed = 0
        total_score = 0.0
        last_score_update = None
        last_comment_update = None
        for p in practices:
            g = grade_map.get(p.id)
            if g and g.score is not None:
                completed += 1
                total_score += float(g.score)

            su = getattr(g, "score_updated_at", None) if g else None
            cu = getattr(g, "comment_updated_at", None) if g else None
            if su and (not last_score_update or su > last_score_update):
                last_score_update = su
            if cu and (not last_comment_update or cu > last_comment_update):
                last_comment_update = cu

        def nice(x):
            if abs(x - round(x)) < 1e-9:
                return int(round(x))
            return round(x, 2)

        return jsonify({
            "success": True,
            "total_practices": total_practices,
            "completed_practices": completed,
            "missing_practices": max(0, total_practices - completed),
            "total_score": nice(total_score),
            "max_possible": nice(max_possible)
            ,
            "last_score_updated_at": _dt_iso(last_score_update),
            "last_comment_updated_at": _dt_iso(last_comment_update),
        })
