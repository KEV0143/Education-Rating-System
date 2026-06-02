from flask import jsonify, request

def register_group_routes(app, db, Course, Group, Student, JournalLesson, JournalLessonSession, JournalAttendance, parse_int, ctx):
    @app.get("/api/journal/group/<int:group_id>/students")
    def api_journal_group_students(group_id: int):
        group = db.session.get(Group, group_id)
        if not group:
            return jsonify({"success": False, "error": "Группа не найдена"}), 404

        students = Student.query.filter_by(group_id=group_id).order_by(Student.fio.asc()).all()
        return jsonify(
            {
                "success": True,
                "group": {"id": group.id, "name": group.name},
                "students": [student.to_dict() for student in students],
            }
        )

    @app.get("/api/journal/groups/students")
    def api_journal_groups_students():
        raw_ids = str(request.args.get("ids") or "").strip()
        if not raw_ids:
            return jsonify({"success": False, "error": "Список групп не передан"}), 400

        parsed_ids = []
        for raw in raw_ids.split(","):
            gid = parse_int(raw, default=0)
            if gid > 0 and gid not in parsed_ids:
                parsed_ids.append(int(gid))
        if not parsed_ids:
            return jsonify({"success": False, "error": "Некорректный список групп"}), 400

        groups = Group.query.filter(Group.id.in_(parsed_ids)).order_by(Group.name.asc()).all()
        by_id = {int(group.id): group for group in groups}
        missing = [gid for gid in parsed_ids if gid not in by_id]
        if missing:
            return jsonify({"success": False, "error": "Одна или несколько групп не найдены"}), 404

        group_payload = []
        total_students = 0
        for gid in parsed_ids:
            group = by_id[gid]
            students = Student.query.filter_by(group_id=gid).order_by(Student.fio.asc()).all()
            total_students += len(students)
            group_payload.append(
                {
                    "group": {"id": int(group.id), "name": group.name},
                    "students": [student.to_dict() for student in students],
                }
            )

        return jsonify(
            {
                "success": True,
                "groups": group_payload,
                "group_count": len(group_payload),
                "total_students": int(total_students),
            }
        )
