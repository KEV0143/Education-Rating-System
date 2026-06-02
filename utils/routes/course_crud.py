import io
from flask import abort, redirect, request, send_file, url_for
from PIL import Image
from utils.services.image_store import find_legacy_image_path, load_legacy_image_bytes


def register_course_crud_routes(
    app,
    db,
    Course,
    CourseImage,
    Group,
    get_or_404,
    normalize_group_ids,
    parse_group_ids,
    remove_group_id_from_csv,
    upsert_course_image,
    parse_int,
    data_dir,
    resource_dir,
):
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
        is_coursework = bool(request.form.get("is_coursework"))

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
            is_coursework=is_coursework,
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
        is_coursework = bool(request.form.get("is_coursework"))

        if title:
            course.title = title
        if year:
            course.year = year
        if semester in range(1, 11):
            course.semester = semester

        course.group_ids = group_ids
        course.is_coursework = is_coursework

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
