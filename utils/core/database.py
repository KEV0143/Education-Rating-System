import sqlite3
from pathlib import Path
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import event
from sqlalchemy.engine import Engine
from utils.services.runtime import ensure_sqlite_file

db = SQLAlchemy()


@event.listens_for(Engine, "connect")
def _set_sqlite_pragma(dbapi_connection, _connection_record):
    try:
        if isinstance(dbapi_connection, sqlite3.Connection):
            cur = dbapi_connection.cursor()
            cur.execute("PRAGMA foreign_keys=ON;")
            cur.execute("PRAGMA journal_mode=WAL;")
            cur.execute("PRAGMA busy_timeout=30000;")
            cur.close()
    except Exception:
        pass



def prepare_sqlite_database(data_dir: Path, filename: str = "RatingSystemKev.db"):
    db_dir = data_dir / "db"
    db_dir.mkdir(parents=True, exist_ok=True)
    db_path = db_dir / filename
    ensure_sqlite_file(db_path)
    db_uri = f"sqlite:///{db_path.as_posix()}"
    return db_dir, db_path, db_uri


def init_db_app(app, db_uri: str) -> None:
    app.config["SQLALCHEMY_DATABASE_URI"] = db_uri
    app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
    db.init_app(app)
