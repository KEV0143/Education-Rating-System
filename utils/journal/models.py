from sqlalchemy import func

ATTENDANCE_STATUS_PRESENT = "present"
ATTENDANCE_STATUS_ABSENT = "absent"
ATTENDANCE_STATUS_EXCUSED = "excused"
ATTENDANCE_STATUSES = (
    ATTENDANCE_STATUS_PRESENT,
    ATTENDANCE_STATUS_ABSENT,
    ATTENDANCE_STATUS_EXCUSED,
)


def init_journal_models(db):
    class JournalLesson(db.Model):
        __tablename__ = "journal_lesson"

        id = db.Column(db.Integer, primary_key=True)
        week_parity = db.Column(db.String(2), nullable=False, default="I", index=True)
        day_of_week = db.Column(db.Integer, nullable=False, index=True)
        pair_number = db.Column(db.Integer, nullable=False, index=True)
        semester_key = db.Column(db.String(16), nullable=False, default="", index=True)
        course_id = db.Column(db.Integer, db.ForeignKey("course.id", ondelete="CASCADE"), nullable=False, index=True)
        group_id = db.Column(db.Integer, db.ForeignKey("group.id", ondelete="CASCADE"), nullable=False, index=True)
        group_ids = db.Column(db.String(500), nullable=False, default="")
        room = db.Column(db.String(40), nullable=False, default="")

        created_at = db.Column(db.DateTime, nullable=False, server_default=func.current_timestamp())
        updated_at = db.Column(
            db.DateTime,
            nullable=False,
            server_default=func.current_timestamp(),
            onupdate=func.current_timestamp(),
        )

        sessions = db.relationship(
            "JournalLessonSession",
            backref="lesson",
            cascade="all, delete-orphan",
            passive_deletes=True,
        )

        __table_args__ = (
            db.UniqueConstraint(
                "week_parity",
                "day_of_week",
                "pair_number",
                "group_id",
                name="uq_journal_slot_group",
            ),
        )

        def to_dict(self):
            return {
                "id": self.id,
                "week_parity": self.week_parity,
                "day_of_week": self.day_of_week,
                "pair_number": self.pair_number,
                "semester_key": self.semester_key,
                "course_id": self.course_id,
                "group_id": self.group_id,
                "group_ids": self.group_ids or "",
                "room": self.room,
            }

    class JournalLessonSession(db.Model):
        __tablename__ = "journal_lesson_session"

        id = db.Column(db.Integer, primary_key=True)
        lesson_id = db.Column(
            db.Integer,
            db.ForeignKey("journal_lesson.id", ondelete="CASCADE"),
            nullable=False,
            index=True,
        )
        session_date = db.Column(db.Date, nullable=False, index=True)
        qr_token = db.Column(db.String(96), nullable=False, default="", index=True)
        qr_token_created_at = db.Column(db.DateTime, nullable=True)

        created_at = db.Column(db.DateTime, nullable=False, server_default=func.current_timestamp())
        updated_at = db.Column(
            db.DateTime,
            nullable=False,
            server_default=func.current_timestamp(),
            onupdate=func.current_timestamp(),
        )

        attendances = db.relationship(
            "JournalAttendance",
            backref="session",
            cascade="all, delete-orphan",
            passive_deletes=True,
        )

        __table_args__ = (
            db.UniqueConstraint("lesson_id", "session_date", name="uq_journal_session_date"),
        )

        def to_dict(self):
            return {
                "id": self.id,
                "lesson_id": self.lesson_id,
                "session_date": self.session_date.isoformat() if self.session_date else None,
                "qr_token": self.qr_token or "",
            }

    class JournalAttendance(db.Model):
        __tablename__ = "journal_attendance"

        id = db.Column(db.Integer, primary_key=True)
        session_id = db.Column(
            db.Integer,
            db.ForeignKey("journal_lesson_session.id", ondelete="CASCADE"),
            nullable=False,
            index=True,
        )
        student_id = db.Column(
            db.Integer,
            db.ForeignKey("student.id", ondelete="CASCADE"),
            nullable=False,
            index=True,
        )
        status = db.Column(db.String(16), nullable=False, default=ATTENDANCE_STATUS_ABSENT, index=True)
        source = db.Column(db.String(16), nullable=False, default="manual")
        source_ip = db.Column(db.String(64), nullable=False, default="")
        marked_at = db.Column(db.DateTime, nullable=False, server_default=func.current_timestamp())
        updated_at = db.Column(
            db.DateTime,
            nullable=False,
            server_default=func.current_timestamp(),
            onupdate=func.current_timestamp(),
        )

        __table_args__ = (
            db.UniqueConstraint("session_id", "student_id", name="uq_journal_session_student"),
        )

        def to_dict(self):
            return {
                "id": self.id,
                "session_id": self.session_id,
                "student_id": self.student_id,
                "status": self.status,
                "source": self.source,
                "source_ip": self.source_ip or "",
                "marked_at": self.marked_at.isoformat() if self.marked_at else None,
            }

    return JournalLesson, JournalLessonSession, JournalAttendance
