from datetime import datetime, timezone
from utils.core.database import db


class Group(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(50), nullable=False, unique=True)

    students = db.relationship(
        "Student",
        backref="group",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )

    def to_dict(self):
        return {"id": self.id, "name": self.name}


class Student(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    fio = db.Column(db.String(150), nullable=False)
    group_id = db.Column(
        db.Integer,
        db.ForeignKey("group.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )

    def to_dict(self):
        return {"id": self.id, "fio": self.fio, "group_id": self.group_id}


class AppSetting(db.Model):
    key = db.Column(db.String(64), primary_key=True)
    value = db.Column(db.String(255), nullable=True)
    updated_at = db.Column(db.DateTime, nullable=False, default=lambda: datetime.now(timezone.utc).replace(tzinfo=None), onupdate=lambda: datetime.now(timezone.utc).replace(tzinfo=None))


class Course(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    title = db.Column(db.String(100), nullable=False)
    image_filename = db.Column(db.String(200), nullable=False, default="default.jpg")
    year = db.Column(db.String(20), nullable=False)
    semester = db.Column(db.Integer, nullable=False)
    group_ids = db.Column(db.String(200), nullable=False, default="")
    archived = db.Column(db.Boolean, nullable=False, default=False)
    is_coursework = db.Column(db.Boolean, nullable=False, default=False)

    @property
    def attached_group_ids(self):
        from utils.core.helpers import parse_group_ids
        return parse_group_ids(self.group_ids)

    @attached_group_ids.setter
    def attached_group_ids(self, values):
        from utils.core.helpers import normalize_group_ids
        self.group_ids = normalize_group_ids(str(v) for v in values)

    def get_group_names(self):
        from utils.core.helpers import parse_group_ids
        ids = parse_group_ids(self.group_ids)
        if not ids:
            return []

        import flask
        if flask.has_request_context():
            if not hasattr(flask.g, "group_name_cache"):
                flask.g.group_name_cache = {}

            missing_ids = [gid for gid in ids if gid not in flask.g.group_name_cache]
            if missing_ids:
                groups = Group.query.filter(Group.id.in_(missing_ids)).all()
                for g in groups:
                    flask.g.group_name_cache[g.id] = g.name

            names = []
            for gid in ids:
                name = flask.g.group_name_cache.get(gid)
                if name:
                    names.append(name)
            names.sort()
            return names

        groups = Group.query.filter(Group.id.in_(ids)).order_by(Group.name).all()
        return [g.name for g in groups]


class CourseImage(db.Model):
    __tablename__ = "course_image"

    course_id = db.Column(
        db.Integer,
        db.ForeignKey("course.id", ondelete="CASCADE"),
        primary_key=True,
    )
    image_data = db.Column(db.LargeBinary, nullable=False)
    mime_type = db.Column(db.String(64), nullable=False, default="image/jpeg")
    updated_at = db.Column(db.DateTime, nullable=False, default=lambda: datetime.now(timezone.utc).replace(tzinfo=None), onupdate=lambda: datetime.now(timezone.utc).replace(tzinfo=None))
