import io
import re
import secrets
from typing import Optional, Set

from flask import abort, jsonify, redirect, render_template, request, send_file, session, url_for
from PIL import Image
from sqlalchemy import literal
from sqlalchemy.exc import IntegrityError

from utils.image_store import find_legacy_image_path, load_legacy_image_bytes


def register_app_routes(
    app,
    db,
    Course,
    CourseImage,
    Group,
    Student,
    get_or_404,
    get_setting,
    set_setting,
    normalize_group_ids,
    parse_group_ids,
    remove_group_id_from_csv,
    upsert_course_image,
    parse_int,
    update_service,
    app_version: str,
    data_dir,
    resource_dir,
):
    def clean_student_name(raw_name: str) -> str:
        return re.sub(r"^\s*\d+[\.\)]\s*", "", (raw_name or "")).strip()

    def course_query_filter_by_group(query, group_id_str: Optional[str]):
        gid = parse_int(group_id_str, default=0)
        if gid <= 0:
            return query
        pattern = f"%,{gid},%"
        return query.filter((literal(",") + Course.group_ids + literal(",")).like(pattern))

    @app.before_request
    def _csrf_protect():
        if "csrf_token" not in session:
            session["csrf_token"] = secrets.token_urlsafe(32)

        if request.method in ("POST", "PUT", "PATCH", "DELETE"):
            token = request.headers.get("X-CSRFToken") or request.form.get("csrf_token")

            if token is None and request.is_json:
                payload = request.get_json(silent=True) or {}
                token = payload.get("csrf_token")

            if not token or token != session.get("csrf_token"):
                abort(400, description="CSRF token missing or invalid")

    @app.context_processor
    def _inject_csrf():
        update_info = update_service.context()
        return {
            "csrf_token": session.get("csrf_token", ""),
            "app_version": app_version,
            "update_remote_version": update_info.get("remote_version"),
            "update_available": update_info.get("available", False),
            "update_url": update_info.get("url"),
            "update_source_url": update_info.get("source_url"),
            "update_exe_url": update_info.get("exe_url"),
            "update_release_url": update_info.get("release_url"),
            "update_notes": update_info.get("notes"),
        }

    @app.errorhandler(400)
    def bad_request(e):
        if request.path.startswith("/api/"):
            return jsonify({"success": False, "error": str(e)}), 400
        return f"400 Bad Request: {e}", 400

    @app.get("/course_image/<int:course_id>")
    def course_image(course_id: int):
        course = db.session.get(Course, course_id)
        if not course:
            abort(404)

        rec = db.session.get(CourseImage, course_id)
        if rec and rec.image_data:
            return send_file(io.BytesIO(rec.image_data), mimetype=rec.mime_type or "image/jpeg")

        legacy_path = find_legacy_image_path(course.image_filename, data_dir=data_dir, resource_dir=resource_dir)
        if legacy_path:
            payload = load_legacy_image_bytes(legacy_path)
            if payload:
                img_bytes, mime = payload
                return send_file(io.BytesIO(img_bytes), mimetype=mime)

        return redirect(url_for("placeholder"))

    @app.post("/api/update/remind_later")
    def api_update_remind_later():
        update_service.mark_remind_later()
        return jsonify({"success": True})

    @app.route("/")
    def index():
        search = (request.args.get("search") or "").strip()
        greeting_name = get_setting("greeting_name")
        f_year = (request.args.get("year") or "").strip() or None
        f_sem = (request.args.get("semester") or "").strip() or None
        f_group_id = (request.args.get("group_id") or "").strip() or None

        selected_sort = (request.args.get("sort") or "").strip() or "title_asc"

        sort_options = {
            "title_asc": ("Название (А–Я)", [Course.title.asc()]),
            "title_desc": ("Название (Я–А)", [Course.title.desc()]),
            "year_desc": ("Год (новые)", [Course.year.desc(), Course.title.asc()]),
            "year_asc": ("Год (старые)", [Course.year.asc(), Course.title.asc()]),
            "semester_desc": ("Семестр (убыв.)", [Course.semester.desc(), Course.title.asc()]),
            "semester_asc": ("Семестр (возр.)", [Course.semester.asc(), Course.title.asc()]),
        }
        sort_label, sort_order = sort_options.get(selected_sort, sort_options["title_asc"])

        years = [
            y[0]
            for y in db.session.query(Course.year).filter(Course.archived.is_(False)).distinct().order_by(Course.year).all()
        ]

        groups = Group.query.order_by(Group.name).all()

        results = []
        added_course_ids: Set[int] = set()

        if search:
            search_lower = search.lower()
            courses = Course.query.filter(Course.archived.is_(False)).all()
            course_gid_map = {c.id: set(parse_group_ids(c.group_ids)) for c in courses}

            for student in Student.query.all():
                if search_lower in student.fio.lower():
                    for course in courses:
                        if student.group_id in course_gid_map.get(course.id, set()) and course.id not in added_course_ids:
                            results.append({"course": course, "reason": f"Студент: {student.fio} ({student.group.name})"})
                            added_course_ids.add(course.id)

            for group in groups:
                if search_lower in group.name.lower():
                    for course in courses:
                        if group.id in course_gid_map.get(course.id, set()) and course.id not in added_course_ids:
                            results.append({"course": course, "reason": f"Группа: {group.name}"})
                            added_course_ids.add(course.id)

            for course in courses:
                if search_lower in course.title.lower() and course.id not in added_course_ids:
                    results.append({"course": course, "reason": None})
                    added_course_ids.add(course.id)
        else:
            query = Course.query.filter(Course.archived.is_(False))
            if f_year:
                query = query.filter_by(year=f_year)
            if f_sem:
                query = query.filter_by(semester=parse_int(f_sem, default=0))
            if f_group_id:
                query = course_query_filter_by_group(query, f_group_id)

            for course in query.order_by(*sort_order).all():
                results.append({"course": course, "reason": None})

        archived_courses = (
            Course.query.filter(Course.archived.is_(True))
            .order_by(Course.year.desc(), Course.semester.desc(), Course.title.asc())
            .all()
        )

        return render_template(
            "index.html",
            results=results,
            years=years,
            groups=groups,
            search=search,
            selected_year=f_year,
            selected_sem=f_sem,
            selected_group_id=f_group_id,
            selected_sort=selected_sort,
            sort_label=sort_label,
            archived_courses=archived_courses,
            greeting_name=greeting_name,
        )

    @app.get("/api/greeting")
    def api_get_greeting():
        name = get_setting("greeting_name")
        return jsonify({"success": True, "name": name})

    @app.post("/api/greeting")
    def api_set_greeting():
        data = request.get_json(silent=True) or {}
        name = (data.get("name") or "").strip()
        if not name:
            abort(400, description="Name is required")
        if len(name) > 80:
            name = name[:80]

        set_setting("greeting_name", name)
        return jsonify({"success": True, "name": name})

    @app.route("/placeholder")
    def placeholder():
        img = Image.new("RGB", (250, 160), color="#f1f3f5")
        img_io = io.BytesIO()
        img.save(img_io, "JPEG")
        img_io.seek(0)
        return send_file(img_io, mimetype="image/jpeg")

    @app.route("/add_course", methods=["POST"])
    def add_course():
        title = (request.form.get("title") or "").strip()
        year = (request.form.get("year") or "").strip()
        semester = parse_int(request.form.get("semester"), default=1)
        group_ids = normalize_group_ids(request.form.getlist("groups"))

        if not title:
            abort(400, description="Title is required")
        if not year:
            year = "2024-2025"
        if semester not in range(1, 11):
            semester = 1

        course = Course(
            title=title,
            year=year,
            semester=semester,
            group_ids=group_ids,
        )
        db.session.add(course)
        db.session.flush()

        file = request.files.get("image")
        if file and file.filename:
            upsert_course_image(course.id, file)

        db.session.commit()
        return redirect(url_for("index"))

    @app.route("/edit_course/<int:course_id>", methods=["POST"])
    def edit_course(course_id: int):
        course = get_or_404(Course, course_id)

        title = (request.form.get("title") or "").strip()
        year = (request.form.get("year") or "").strip()
        semester = parse_int(request.form.get("semester"), default=course.semester)
        group_ids = normalize_group_ids(request.form.getlist("groups"))

        if title:
            course.title = title
        if year:
            course.year = year
        if semester in range(1, 11):
            course.semester = semester

        course.group_ids = group_ids

        file = request.files.get("image")
        if file and file.filename:
            upsert_course_image(course.id, file)

        db.session.commit()
        return redirect(url_for("index"))

    @app.route("/delete_course/<int:course_id>", methods=["POST"])
    def delete_course(course_id: int):
        course = get_or_404(Course, course_id)
        db.session.delete(course)
        db.session.commit()
        return redirect(url_for("index"))

    @app.route("/archive_course/<int:course_id>", methods=["POST"])
    def archive_course(course_id: int):
        course = get_or_404(Course, course_id)
        course.archived = True
        db.session.commit()
        return redirect(url_for("index"))

    @app.route("/unarchive_course/<int:course_id>", methods=["POST"])
    def unarchive_course(course_id: int):
        course = get_or_404(Course, course_id)
        course.archived = False
        db.session.commit()
        return redirect(url_for("index"))

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
        db.session.commit()
        return jsonify({"success": True, "courses_updated": touched})

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

        added = 0
        for raw in str(text).splitlines():
            fio = clean_student_name(raw)
            if not fio:
                continue

            exists = Student.query.filter_by(fio=fio, group_id=group_id).first()
            if exists:
                continue

            db.session.add(Student(fio=fio, group_id=group_id))
            added += 1

        db.session.commit()
        return jsonify({"success": True, "added": added})

    @app.route("/api/delete_student/<int:student_id>", methods=["POST"])
    def api_delete_student(student_id: int):
        student = get_or_404(Student, student_id)
        db.session.delete(student)
        db.session.commit()
        return jsonify({"success": True})

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
        db.session.commit()
        return jsonify({"success": True, "student": student.to_dict()})
