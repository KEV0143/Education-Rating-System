from flask import render_template, request, jsonify, abort


def register_practice_routes(app, db, Course, Group, Student, Practice, PracticeGrade, parse_group_ids):

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

    @app.get("/course/<int:course_id>/assessments")
    def course_assessments(course_id: int):
        course = _get_course_or_404(course_id)

        group_ids = parse_group_ids(course.group_ids)
        groups = []
        if group_ids:
            groups = Group.query.filter(Group.id.in_(group_ids)).order_by(Group.name).all()

        practices = Practice.query.filter_by(course_id=course.id).order_by(Practice.id.desc()).all()

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

        p = Practice(course_id=course.id, title=title, min_score=min_score, max_score=max_score)
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

        p.title = title
        p.min_score = min_score
        p.max_score = max_score

        db.session.commit()
        return jsonify({"success": True, "practice": p.to_dict()})

    @app.post("/api/practice/<int:practice_id>/delete")
    def api_practice_delete(practice_id: int):
        p = _get_practice_or_404(practice_id)
        db.session.delete(p)  # cascade -> grades
        db.session.commit()
        return jsonify({"success": True})

    @app.get("/api/practice/<int:practice_id>/group/<int:group_id>/grades")
    def api_practice_group_grades(practice_id: int, group_id: int):
        practice = _get_practice_or_404(practice_id)
        course = _get_course_or_404(practice.course_id)

        _ensure_group_in_course(course, group_id)

        students = Student.query.filter_by(group_id=group_id).order_by(Student.fio).all()
        if not students:
            return jsonify({"success": True, "rows": []})

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
            })

        return jsonify({"success": True, "rows": rows})

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

        grade.score = score
        grade.comment = comment

        db.session.commit()
        return jsonify({"success": True})

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
        for sid in student_ids:
            g = gmap.get(sid)
            if not g:
                g = PracticeGrade(practice_id=practice.id, student_id=sid)
                db.session.add(g)
            g.score = score
            g.comment = comment
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
        for sid in ok_ids:
            g = gmap.get(sid)
            if not g:
                g = PracticeGrade(practice_id=practice.id, student_id=sid)
                db.session.add(g)
            g.score = score
            g.comment = comment
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
        for p in practices:
            g = grade_map.get(p.id)
            if g and g.score is not None:
                completed += 1
                total_score += float(g.score)

        def nice(x):
            if abs(x - round(x)) < 1e-9:
                return int(round(x))
            return round(x, 2)

        return jsonify({
            "success": True,
            "total_practices": total_practices,
            "completed_practices": completed,
            "total_score": nice(total_score),
            "max_possible": nice(max_possible)
        })
