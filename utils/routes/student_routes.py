import re
import csv
import io
import openpyxl
from flask import abort, jsonify, request
from sqlalchemy.exc import IntegrityError


def register_student_routes(
    app,
    db,
    Group,
    Student,
    Course,
    remove_group_id_from_csv,
    get_or_404,
    parse_int,
):
    def clean_student_name(raw_name: str) -> str:
        return re.sub(r"^\s*\d+[\.\)]\s*", "", (raw_name or "")).strip()

    @app.get("/api/groups")
    def api_groups():
        groups = Group.query.order_by(Group.name).all()
        return jsonify({"success": True, "groups": [group.to_dict() for group in groups]})

    @app.route("/api/create_group", methods=["POST"])
    def api_create_group():
        data = request.get_json(silent=True) or {}
        name = (data.get("name") or "").strip()
        if not name:
            return jsonify({"success": False, "error": "Name is required"}), 400

        group = Group(name=name)
        db.session.add(group)
        try:
            db.session.commit()
            return jsonify({"success": True, "group": group.to_dict()})
        except IntegrityError:
            db.session.rollback()
            return jsonify({"success": False, "error": "Group already exists"}), 409

    @app.route("/api/rename_group", methods=["POST"])
    def api_rename_group():
        data = request.get_json(silent=True) or {}
        gid = parse_int(data.get("id"), default=0)
        name = (data.get("name") or "").strip()
        if gid <= 0 or not name:
            return jsonify({"success": False, "error": "Invalid id or name"}), 400

        group = db.session.get(Group, gid)
        if not group:
            return jsonify({"success": False, "error": "Group not found"}), 404

        group.name = name
        try:
            db.session.commit()
            return jsonify({"success": True, "group": group.to_dict()})
        except IntegrityError:
            db.session.rollback()
            return jsonify({"success": False, "error": "Group name must be unique"}), 409

    @app.route("/api/delete_group/<int:group_id>", methods=["POST"])
    def api_delete_group(group_id: int):
        group = db.session.get(Group, group_id)
        if not group:
            return jsonify({"success": False, "error": "Group not found"}), 404

        touched = 0
        courses = Course.query.all()
        for course in courses:
            if not course.group_ids:
                continue
            old = course.group_ids
            new = remove_group_id_from_csv(old, group_id)
            if new != old:
                course.group_ids = new
                touched += 1

        db.session.delete(group)
        try:
            db.session.commit()
            return jsonify({"success": True, "courses_updated": touched})
        except Exception as exc:
            db.session.rollback()
            return jsonify({"success": False, "error": f"Ошибка СУБД при удалении группы: {str(exc)}"}), 500

    @app.route("/api/get_students/<int:group_id>")
    def api_get_students(group_id: int):
        return jsonify([s.to_dict() for s in Student.query.filter_by(group_id=group_id).order_by(Student.fio).all()])

    @app.route("/api/add_students_bulk", methods=["POST"])
    def api_add_students_bulk():
        data = request.get_json(silent=True) or {}
        group_id = parse_int(data.get("group_id"), default=0)
        text = data.get("text") or ""

        if group_id <= 0:
            return jsonify({"success": False, "error": "Invalid group_id"}), 400

        group = db.session.get(Group, group_id)
        if not group:
            return jsonify({"success": False, "error": "Group not found"}), 404

        if not str(text).strip():
            return jsonify({"success": True, "added": 0})

        existing_students = {s.fio.lower() for s in Student.query.filter_by(group_id=group_id).all()}

        added = 0
        for raw in str(text).splitlines():
            fio = clean_student_name(raw)
            if not fio:
                continue

            if fio.lower() in existing_students:
                continue

            db.session.add(Student(fio=fio, group_id=group_id))
            existing_students.add(fio.lower())
            added += 1

        if added > 0:
            try:
                db.session.commit()
            except Exception as exc:
                db.session.rollback()
                return jsonify({"success": False, "error": f"Ошибка СУБД при добавлении студентов: {str(exc)}"}), 500

        return jsonify({"success": True, "added": added})

    @app.route("/api/delete_student/<int:student_id>", methods=["POST"])
    def api_delete_student(student_id: int):
        student = get_or_404(Student, student_id)
        db.session.delete(student)
        try:
            db.session.commit()
            return jsonify({"success": True})
        except Exception as exc:
            db.session.rollback()
            return jsonify({"success": False, "error": f"Ошибка СУБД при удалении студента: {str(exc)}"}), 500

    @app.route("/api/update_student/<int:student_id>", methods=["POST"])
    def api_update_student(student_id: int):
        student = get_or_404(Student, student_id)
        data = request.get_json(silent=True) or {}

        fio = (data.get("fio") or "").strip()
        fio = re.sub(r"\s+", " ", fio)
        if not fio:
            return jsonify({"success": False, "error": "FIO is required"}), 400
        if len(fio) > 150:
            fio = fio[:150]

        exists = Student.query.filter(Student.group_id == student.group_id, Student.fio == fio, Student.id != student.id).first()
        if exists:
            return jsonify({"success": False, "error": "Такой студент уже есть в группе"}), 409

        student.fio = fio
        try:
            db.session.commit()
            return jsonify({"success": True, "student": student.to_dict()})
        except Exception as exc:
            db.session.rollback()
            return jsonify({"success": False, "error": f"Ошибка СУБД при обновлении студента: {str(exc)}"}), 500

    @app.route("/api/import_students_file", methods=["POST"])
    def api_import_students_file():
        group_id = parse_int(request.form.get("group_id"), default=0)
        if group_id <= 0:
            return jsonify({"success": False, "error": "Некорректный ID группы"}), 400

        group = db.session.get(Group, group_id)
        if not group:
            return jsonify({"success": False, "error": "Группа не найдена"}), 404

        if "file" not in request.files:
            return jsonify({"success": False, "error": "Файл не предоставлен"}), 400

        file = request.files["file"]
        if not file or not file.filename:
            return jsonify({"success": False, "error": "Файл пустой"}), 400

        filename = file.filename.lower()
        students_to_add = []

        try:
            if filename.endswith(".csv"):
                content = file.read().decode("utf-8", errors="replace")
                reader = csv.reader(io.StringIO(content))
                for row in reader:
                    if not row:
                        continue
                    fio = " ".join(row).strip()
                    fio = clean_student_name(fio)
                    if fio and len(fio) > 1 and not fio.lower().startswith("фио") and not fio.lower().startswith("имя"):
                        students_to_add.append(fio)
            elif filename.endswith((".xlsx", ".xls")):
                wb = openpyxl.load_workbook(io.BytesIO(file.read()), read_only=True)
                sheet = wb.active
                for row in sheet.iter_rows(values_only=True):
                    non_empty = [str(cell).strip() for cell in row if cell is not None and str(cell).strip()]
                    if not non_empty:
                        continue
                    fio = clean_student_name(non_empty[0])
                    if fio and len(fio) > 1 and not fio.lower().startswith("фио") and not fio.lower().startswith("имя"):
                        students_to_add.append(fio)
            else:
                return jsonify({"success": False, "error": "Поддерживаются только форматы .xlsx, .xls и .csv"}), 400
        except Exception as exc:
            return jsonify({"success": False, "error": f"Ошибка чтения файла: {str(exc)}"}), 500

        existing_students = {s.fio.lower() for s in Student.query.filter_by(group_id=group_id).all()}

        added = 0
        for fio in students_to_add:
            if len(fio) > 150:
                fio = fio[:150]
            if fio.lower() not in existing_students:
                db.session.add(Student(fio=fio, group_id=group_id))
                existing_students.add(fio.lower())
                added += 1

        if added > 0:
            try:
                db.session.commit()
            except Exception as exc:
                db.session.rollback()
                return jsonify({"success": False, "error": f"Ошибка СУБД при сохранении импортированных студентов: {str(exc)}"}), 500

        return jsonify({"success": True, "added": added, "total_found": len(students_to_add)})
