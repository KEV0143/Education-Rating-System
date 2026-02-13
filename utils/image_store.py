import io
from pathlib import Path
from typing import Optional, Tuple

from PIL import Image


def _encode_image_to_jpeg(img: Image.Image, max_width: int = 800, quality: int = 85) -> Tuple[bytes, str]:
    img = img.convert("RGB")
    if img.width > max_width:
        ratio = max_width / img.width
        new_height = max(1, int(img.height * ratio))
        img = img.resize((max_width, new_height), Image.Resampling.LANCZOS)

    out = io.BytesIO()
    img.save(out, format="JPEG", quality=quality, optimize=True)
    return out.getvalue(), "image/jpeg"


def process_uploaded_image(file) -> Optional[Tuple[bytes, str]]:
    try:
        check = Image.open(file)
        check.verify()
        file.stream.seek(0)

        img = Image.open(file)
        return _encode_image_to_jpeg(img)
    except Exception:
        return None


def load_legacy_image_bytes(path: Path) -> Optional[Tuple[bytes, str]]:
    try:
        with path.open("rb") as f:
            raw = f.read()
        img = Image.open(io.BytesIO(raw))
        return _encode_image_to_jpeg(img)
    except Exception:
        return None


def find_legacy_image_path(filename: Optional[str], data_dir: Path, resource_dir: Path) -> Optional[Path]:
    name = (filename or "").strip()
    if not name or name.lower() == "default.jpg":
        return None

    candidates = [
        data_dir / "static" / "uploads" / name,
        resource_dir / "static" / "uploads" / name,
        Path.cwd() / "static" / "uploads" / name,
    ]
    for p in candidates:
        if p.exists() and p.is_file():
            return p
    return None


def migrate_legacy_course_images(db, Course, CourseImage, data_dir: Path, resource_dir: Path) -> int:
    migrated = 0
    courses = (
        db.session.query(Course)
        .outerjoin(CourseImage, Course.id == CourseImage.course_id)
        .filter(CourseImage.course_id.is_(None))
        .filter(Course.image_filename.isnot(None))
        .filter(Course.image_filename != "default.jpg")
        .all()
    )
    for course in courses:
        path = find_legacy_image_path(course.image_filename, data_dir=data_dir, resource_dir=resource_dir)
        if not path:
            continue

        payload = load_legacy_image_bytes(path)
        if not payload:
            continue

        img_bytes, mime = payload
        db.session.add(CourseImage(course_id=course.id, image_data=img_bytes, mime_type=mime))
        migrated += 1

    if migrated:
        db.session.commit()
    return migrated
