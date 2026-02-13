from flask import abort, render_template
from sqlalchemy import func


def register_course_routes(
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
    @app.get("/course/<int:course_id>")
    def course_detail(course_id: int):
        course = db.session.get(Course, course_id)
        if not course:
            abort(404)

        group_ids = parse_group_ids(course.group_ids)
        groups = []
        student_counts = {}
        group_work_counts = {}
        group_score_sums = {}
        practices = Practice.query.filter_by(course_id=course.id).order_by(Practice.id.asc()).all()
        total_practices = len(practices)
        max_per_student = float(sum(float(p.max_score) for p in practices)) if practices else 0.0

        if group_ids:
            groups = Group.query.filter(Group.id.in_(group_ids)).order_by(Group.name).all()


            rows = (
                db.session.query(Student.group_id, func.count(Student.id))
                .filter(Student.group_id.in_(group_ids))
                .group_by(Student.group_id)
                .all()
            )
            student_counts = {gid: cnt for gid, cnt in rows}


            rows2 = (
                db.session.query(
                    Student.group_id,
                    func.count(PracticeGrade.id),
                    func.coalesce(func.sum(PracticeGrade.score), 0.0)
                )
                .join(Student, PracticeGrade.student_id == Student.id)
                .join(Practice, PracticeGrade.practice_id == Practice.id)
                .filter(
                    Practice.course_id == course.id,
                    Student.group_id.in_(group_ids),
                    PracticeGrade.score.isnot(None)
                )
                .group_by(Student.group_id)
                .all()
            )
            group_work_counts = {gid: int(cnt) for gid, cnt, _ in rows2}
            group_score_sums = {gid: float(s) for gid, _, s in rows2}

        group_max_possible = {}
        group_max_works = {}
        for gid in group_ids:
            sc = int(student_counts.get(gid, 0))
            group_max_possible[gid] = sc * max_per_student
            group_max_works[gid] = sc * total_practices

        return render_template(
            "course.html",
            course=course,
            groups=groups,
            student_counts=student_counts,
            total_practices=total_practices,
            group_work_counts=group_work_counts,
            group_score_sums=group_score_sums,
            group_max_possible=group_max_possible,
            group_max_works=group_max_works,
        )

    @app.get("/course/<int:course_id>/group/<int:group_id>")
    def course_group(course_id: int, group_id: int):
        course = db.session.get(Course, course_id)
        if not course:
            abort(404)

        allowed = set(parse_group_ids(course.group_ids))
        if group_id not in allowed:
            abort(404)

        group = db.session.get(Group, group_id)
        if not group:
            abort(404)

        practices = Practice.query.filter_by(course_id=course.id).order_by(Practice.id.asc()).all()
        if practices:
            pids = [p.id for p in practices]
            overrides = PracticeGroupInterval.query.filter(
                PracticeGroupInterval.practice_id.in_(pids),
                PracticeGroupInterval.group_id == group.id,
            ).all()
            override_map = {ov.practice_id: ov for ov in overrides}

            for p in practices:
                ov = override_map.get(p.id)
                p.effective_start_date = ov.start_date if ov else p.start_date
                p.effective_end_date = ov.end_date if ov else p.end_date

        return render_template(
            "course_group.html",
            course=course,
            group=group,
            practices=practices
        )
