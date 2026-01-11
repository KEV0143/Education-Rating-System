from sqlalchemy import func


def init_practice_models(db):
    class Practice(db.Model):
        __tablename__ = "practice"

        id = db.Column(db.Integer, primary_key=True)
        course_id = db.Column(db.Integer, db.ForeignKey("course.id", ondelete="CASCADE"), nullable=False, index=True)

        title = db.Column(db.String(120), nullable=False)
        min_score = db.Column(db.Float, nullable=False, default=0)
        max_score = db.Column(db.Float, nullable=False, default=10)

        created_at = db.Column(db.DateTime, nullable=False, server_default=func.current_timestamp())

        def to_dict(self):
            return {
                "id": self.id,
                "course_id": self.course_id,
                "title": self.title,
                "min_score": self.min_score,
                "max_score": self.max_score,
            }

    class PracticeGrade(db.Model):
        __tablename__ = "practice_grade"

        id = db.Column(db.Integer, primary_key=True)
        practice_id = db.Column(db.Integer, db.ForeignKey("practice.id", ondelete="CASCADE"), nullable=False, index=True)
        student_id = db.Column(db.Integer, db.ForeignKey("student.id", ondelete="CASCADE"), nullable=False, index=True)

        score = db.Column(db.Float, nullable=True)
        comment = db.Column(db.String(1000), nullable=False, default="")
        score_updated_at = db.Column(db.DateTime, nullable=True)
        comment_updated_at = db.Column(db.DateTime, nullable=True)

        updated_at = db.Column(db.DateTime, nullable=False, server_default=func.current_timestamp(), onupdate=func.current_timestamp())

        __table_args__ = (
            db.UniqueConstraint("practice_id", "student_id", name="uq_practice_student"),
        )

        def to_dict(self):
            return {
                "id": self.id,
                "practice_id": self.practice_id,
                "student_id": self.student_id,
                "score": self.score,
                "comment": self.comment,
                "score_updated_at": self.score_updated_at,
                "comment_updated_at": self.comment_updated_at,
                "updated_at": self.updated_at,
            }

    return Practice, PracticeGrade
