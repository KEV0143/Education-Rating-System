import re
import secrets
import gc
from typing import Optional, Set
from flask import abort, jsonify, render_template, request, session


def register_main_routes(
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
        from sqlalchemy import literal
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
            for y in db.session.execute(
                db.select(Course.year).filter(Course.archived.is_(False)).distinct().order_by(Course.year)
            ).all()
        ]

        groups = db.session.execute(db.select(Group).order_by(Group.name)).scalars().all()

        results = []
        added_course_ids: Set[int] = set()

        if search:
            search_lower = search.lower()
            courses = db.session.execute(db.select(Course).filter(Course.archived.is_(False))).scalars().all()
            course_gid_map = {c.id: set(parse_group_ids(c.group_ids)) for c in courses}

            matched_students = db.session.execute(
                db.select(Student).filter(Student.fio.ilike(f"%{search}%"))
            ).scalars().all()
            for student in matched_students:
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
            query = db.select(Course).filter(Course.archived.is_(False))
            if f_year:
                query = query.filter_by(year=f_year)
            if f_sem:
                query = query.filter_by(semester=parse_int(f_sem, default=0))
            if f_group_id:
                query = course_query_filter_by_group(query, f_group_id)

            for course in db.session.execute(query.order_by(*sort_order)).scalars().all():
                results.append({"course": course, "reason": None})

        archived_courses = db.session.execute(
            db.select(Course)
            .filter(Course.archived.is_(True))
            .order_by(Course.year.desc(), Course.semester.desc(), Course.title.asc())
        ).scalars().all()

        return render_template("main/index.html",
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

    @app.get("/api/search_suggestions")
    def api_search_suggestions():
        q = (request.args.get("q") or "").strip()
        if not q or len(q) < 1:
            return jsonify({"success": True, "suggestions": []})

        matched_courses = db.session.execute(
            db.select(Course)
            .filter(Course.title.ilike(f"%{q}%"), Course.archived.is_(False))
            .limit(5)
        ).scalars().all()

        matched_groups = db.session.execute(
            db.select(Group)
            .filter(Group.name.ilike(f"%{q}%"))
            .limit(5)
        ).scalars().all()

        matched_students = db.session.execute(
            db.select(Student)
            .filter(Student.fio.ilike(f"%{q}%"))
            .limit(5)
        ).scalars().all()

        suggestions = []
        for c in matched_courses:
            suggestions.append({
                "type": "course",
                "text": c.title,
                "subtext": f"{c.year} | {c.semester} семестр",
                "value": c.title
            })

        for g in matched_groups:
            suggestions.append({
                "type": "group",
                "text": g.name,
                "subtext": "Учебная группа",
                "value": g.name
            })

        for s in matched_students:
            suggestions.append({
                "type": "student",
                "text": s.fio,
                "subtext": f"Студент группы {s.group.name}",
                "value": s.fio
            })

        return jsonify({"success": True, "suggestions": suggestions})

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

    @app.get("/api/settings/databases")
    def api_settings_databases():
        import os
        import sqlite3

        db_dir = data_dir / "db"
        db_dir.mkdir(parents=True, exist_ok=True)

        databases = []
        for entry in os.listdir(str(db_dir)):
            if entry.endswith(".db") and not entry.startswith("temp_"):
                databases.append(entry)

        if "RatingSystemKev.db" not in databases:
            databases.append("RatingSystemKev.db")

        databases.sort()

        active_db = "RatingSystemKev.db"
        active_json = db_dir / "active_db.json"
        if active_json.exists():
            try:
                import json
                data = json.loads(active_json.read_text(encoding="utf-8"))
                content = data.get("active_database", "").strip()
                if content and content in databases:
                    active_db = content
            except Exception:
                pass

        return jsonify({
            "success": True,
            "active_db": active_db,
            "databases": databases
        })

    @app.post("/api/settings/switch-db")
    def api_settings_switch_db():
        import os
        import sqlite3
        import logging

        data = request.get_json(silent=True) or {}
        db_name = (data.get("db_name") or "").strip()
        if not db_name or not db_name.endswith(".db") or os.path.isabs(db_name) or ".." in db_name:
            return jsonify({"success": False, "error": "Некорректное имя базы данных"}), 400

        db_dir = data_dir / "db"
        target_path = db_dir / db_name
        if not target_path.exists():
            return jsonify({"success": False, "error": "Указанный файл базы данных не найден"}), 404

        master_path = db_dir / "RatingSystemKev.db"

        db.session.remove()
        for engine in list(db.engines.values()):
            try:
                engine.dispose()
            except Exception:
                pass



        active_json = db_dir / "active_db.json"
        try:
            import json
            active_json.write_text(json.dumps({"active_database": db_name}, indent=2, ensure_ascii=False), encoding="utf-8")
        except Exception:
            pass
        gc.collect()

        settings_file = data_dir / "settings.json"
        if settings_file.exists():
            try:
                settings_file.unlink()
            except Exception:
                pass

        from sqlalchemy import create_engine
        from sqlalchemy.pool import NullPool
        new_engine = create_engine(f"sqlite:///{target_path.as_posix()}", poolclass=NullPool)
        db.engines[None] = new_engine
        app.config["SQLALCHEMY_DATABASE_URI"] = f"sqlite:///{target_path.as_posix()}"

        db.session.remove()

        try:
            db.create_all()
        except Exception as exc:
            logging.warning("switch-db: create_all failed: %s", exc)

        try:
            from utils.core.schema import ensure_schema
            ensure_schema()
        except Exception as exc:
            logging.warning("switch-db: ensure_schema failed: %s", exc)
            try:
                db.session.rollback()
            except Exception:
                pass

        try:
            from utils.services.image_store import migrate_legacy_course_images
            migrate_legacy_course_images(db, Course, CourseImage, data_dir=data_dir, resource_dir=resource_dir)
        except Exception as exc:
            logging.warning("switch-db: migrate_legacy_course_images failed: %s", exc)

        try:
            set_setting("active_database_filename", db_name)
        except Exception as exc:
            logging.warning("switch-db: set_setting failed: %s", exc)
            try:
                db.session.rollback()
            except Exception:
                pass

        return jsonify({"success": True, "active_db": db_name})

    @app.get("/api/system/stats")
    def api_system_stats():
        total_courses = db.session.query(Course).count()
        active_courses = db.session.query(Course).filter_by(archived=False).count()
        total_groups = db.session.query(Group).count()
        total_students = db.session.query(Student).count()

        years_rows = db.session.query(Course.year, db.func.count(Course.id)).group_by(Course.year).all()
        years_data = {str(y): int(c) for y, c in years_rows if y}

        groups_rows = db.session.query(Group.name, db.func.count(Student.id)).outerjoin(Student).group_by(Group.id).order_by(db.func.count(Student.id).desc()).limit(10).all()
        groups_data = {str(name): int(c) for name, c in groups_rows}

        return jsonify({
            "success": True,
            "total_courses": total_courses,
            "active_courses": active_courses,
            "total_groups": total_groups,
            "total_students": total_students,
            "years_data": years_data,
            "groups_data": groups_data
        })

    @app.post("/api/settings/import-db")
    def api_settings_import_db():
        import os
        import time
        import sqlite3
        import logging

        if "db_file" not in request.files:
            return jsonify({"success": False, "error": "Файл не предоставлен"}), 400

        file = request.files["db_file"]
        if not file or not file.filename.endswith(".db"):
            return jsonify({"success": False, "error": "Некорректное расширение файла"}), 400

        db_dir = data_dir / "db"
        db_dir.mkdir(parents=True, exist_ok=True)

        import re
        import os
        original_name = file.filename
        base_name = os.path.basename(original_name)
        base_name = re.sub(r'[\\/*?:"<>|]', "", base_name)
        if not base_name.lower().endswith(".db"):
            base_name += ".db"

        if base_name.startswith("temp_"):
            base_name = base_name[5:]

        stem = base_name[:-3]
        if not stem:
            stem = "imported_database"
            base_name = "imported_database.db"

        new_filename = base_name
        counter = 1
        while (db_dir / new_filename).exists():
            new_filename = f"{stem}_{counter}.db"
            counter += 1

        temp_path = db_dir / f"temp_{new_filename}"

        try:
            file.save(str(temp_path))

            conn_check = None
            try:
                conn_check = sqlite3.connect(str(temp_path), timeout=30.0)
                cursor = conn_check.cursor()
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
                tables = [r[0] for r in cursor.fetchall()]
            finally:
                if conn_check:
                    conn_check.close()

            required_tables = {"course", "student", "app_setting"}
            if not required_tables.intersection(set(tables)):
                if temp_path.exists():
                    temp_path.unlink()
                return jsonify({"success": False, "error": "Выбранный файл не является базой данных этого приложения"}), 400

            target_path = db_dir / new_filename
            temp_path.rename(target_path)

            master_path = db_dir / "RatingSystemKev.db"

            db.session.remove()
            for engine in list(db.engines.values()):
                try:
                    engine.dispose()
                except Exception:
                    pass



            active_json = db_dir / "active_db.json"
            try:
                import json
                active_json.write_text(json.dumps({"active_database": new_filename}, indent=2, ensure_ascii=False), encoding="utf-8")
            except Exception:
                pass
            gc.collect()

            settings_file = data_dir / "settings.json"
            if settings_file.exists():
                try:
                    settings_file.unlink()
                except Exception:
                    pass

            from sqlalchemy import create_engine
            from sqlalchemy.pool import NullPool
            new_engine = create_engine(f"sqlite:///{target_path.as_posix()}", poolclass=NullPool)
            db.engines[None] = new_engine
            app.config["SQLALCHEMY_DATABASE_URI"] = f"sqlite:///{target_path.as_posix()}"

            db.session.remove()

            try:
                db.create_all()
            except Exception as exc:
                logging.warning("import-db: create_all failed: %s", exc)

            try:
                from utils.core.schema import ensure_schema
                ensure_schema()
            except Exception as exc:
                logging.warning("import-db: ensure_schema failed: %s", exc)
                try:
                    db.session.rollback()
                except Exception:
                    pass

            try:
                from utils.services.image_store import migrate_legacy_course_images
                migrate_legacy_course_images(db, Course, CourseImage, data_dir=data_dir, resource_dir=resource_dir)
            except Exception as exc:
                logging.warning("import-db: migrate_legacy_course_images failed: %s", exc)

            try:
                set_setting("active_database_filename", new_filename)
            except Exception as exc:
                logging.warning("import-db: set_setting failed: %s", exc)
                try:
                    db.session.rollback()
                except Exception:
                    pass

            return jsonify({"success": True, "filename": new_filename})

        except Exception as e:
            if temp_path.exists():
                try:
                    temp_path.unlink()
                except Exception:
                    pass
            return jsonify({"success": False, "error": str(e)}), 500


