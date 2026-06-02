import base64
import io
import re
import urllib.error
import urllib.parse
import urllib.request
from datetime import date, datetime, timedelta, timezone

try:
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None

try:
    import segno
except Exception:
    segno = None

try:
    MOSCOW_TZ = ZoneInfo("Europe/Moscow") if ZoneInfo is not None else timezone(timedelta(hours=3), name="MSK")
except Exception:
    MOSCOW_TZ = timezone(timedelta(hours=3), name="MSK")

WEEK_PARITY_OPTIONS = ("I", "II")
DAY_OPTIONS = (
    {"id": 1, "name": "Понедельник"},
    {"id": 2, "name": "Вторник"},
    {"id": 3, "name": "Среда"},
    {"id": 4, "name": "Четверг"},
    {"id": 5, "name": "Пятница"},
    {"id": 6, "name": "Суббота"},
)
PAIR_SLOTS = (
    {"number": 1, "label": "1 пара", "time": "9:00-10:30"},
    {"number": 2, "label": "2 пара", "time": "10:40-12:10"},
    {"number": 3, "label": "3 пара", "time": "12:40-14:10"},
    {"number": 4, "label": "4 пара", "time": "14:20-15:50"},
    {"number": 5, "label": "5 пара", "time": "16:20-17:50"},
    {"number": 6, "label": "6 пара", "time": "18:00-19:30"},
    {"number": 7, "label": "7 пара", "time": "19:40-21:10"},
)
PAIR_SLOT_BY_NUMBER = {int(slot["number"]): slot for slot in PAIR_SLOTS}
VALID_DAY_IDS = {int(day["id"]) for day in DAY_OPTIONS}
VALID_PAIR_NUMBERS = {int(slot["number"]) for slot in PAIR_SLOTS}

ATTENDANCE_STATUS_PRESENT = "present"
ATTENDANCE_STATUS_ABSENT = "absent"
ATTENDANCE_STATUS_EXCUSED = "excused"
ATTENDANCE_STATUSES = {ATTENDANCE_STATUS_PRESENT, ATTENDANCE_STATUS_ABSENT, ATTENDANCE_STATUS_EXCUSED}

ATTENDANCE_STATUS_LABELS = {
    ATTENDANCE_STATUS_PRESENT: "Присутствовал",
    ATTENDANCE_STATUS_ABSENT: "Отсутствовал",
    ATTENDANCE_STATUS_EXCUSED: "Отсутствовал (уваж.)",
}
ATTENDANCE_STATUS_SHORT = {
    ATTENDANCE_STATUS_PRESENT: "П",
    ATTENDANCE_STATUS_ABSENT: "Н",
    ATTENDANCE_STATUS_EXCUSED: "У",
}
PUBLIC_ENDPOINTS = {"journal_checkin_page", "static", "favicon"}
AUTO_CAL_UPSTREAM = "https://schedule-of.mirea.ru"
AUTO_CAL_TIMEOUT_SEC = 20.0
AUTO_CAL_SEARCH_LIMIT_MAX = 50
AUTO_CAL_TEACHER_TARGET = 2


def _normalize_text_base(raw: str) -> str:
    text = str(raw or "").replace("\u00a0", " ")
    for dash in ("–", "—", "−", "‑", "‒"):
        text = text.replace(dash, "-")
    text = re.sub(r"\s*-\s*", "-", text)
    return text


def _normalize_course_title_for_import(raw_title: str) -> str:
    text = _normalize_text_base(raw_title)
    text = re.sub(r"\s+", " ", text).strip(" \t\r\n.,;:|")
    text = text.casefold().replace("ё", "е")
    return text


def _clean_course_title_for_import(raw_title: str) -> str:
    text = _normalize_text_base(raw_title)
    text = re.sub(r"\s+", " ", text).strip(" \t\r\n")
    return text


def _normalize_group_name_for_import(raw_name: str) -> str:
    text = _normalize_text_base(raw_name)
    text = re.sub(r"\s+", " ", text).strip(" \t\r\n,;")
    text = re.sub(r"[/\\]\s*\d+$", "", text)
    text = text.casefold().replace("ё", "е")
    text = re.sub(r"\((?:подгруппа|подгр\.?|подгр|подг|пг)\s*\d+\)", "", text)
    text = re.sub(r"(?:подгруппа|подгр\.?|подгр|подг|пг)\s*\d+$", "", text)
    text = re.sub(r"(?:гр\.?|группа)\s*\d+$", "", text)
    text = re.sub(r"\s+", " ", text).strip(" \t\r\n,;")
    return text


def _clean_group_name_for_import(raw_name: str) -> str:
    text = _normalize_text_base(raw_name)
    text = re.sub(r"\s+", " ", text).strip(" \t\r\n,;")
    text = re.sub(r"[/\\]\s*\d+$", "", text)
    return text


def _extract_group_stream_year(raw_name: str) -> str:
    text = _normalize_text_base(raw_name)
    text = re.sub(r"\s+", " ", text).strip(" \t\r\n,;")
    if not text:
        return ""
    parts = [part.strip() for part in text.split("-") if part.strip()]
    if not parts:
        return ""
    for part in reversed(parts):
        digits = re.sub(r"\D+", "", part)
        if not digits:
            continue
        if len(digits) >= 2:
            return digits[-2:]
    return ""


def _in_range(value: date, start_date: date, end_date: date) -> bool:
    return start_date <= value <= end_date


def _calendar_for_start_year(start_year: int):
    return {
        "autumn_classes_start": date(start_year, 9, 1),
        "autumn_classes_end": date(start_year, 12, 23),
        "autumn_credit_start": date(start_year, 12, 24),
        "autumn_credit_end": date(start_year, 12, 31),
        "new_year_break_start": date(start_year, 12, 31),
        "new_year_break_end": date(start_year + 1, 1, 9),
        "winter_gap_start": date(start_year + 1, 1, 10),
        "winter_gap_end": date(start_year + 1, 1, 11),
        "winter_exam_start": date(start_year + 1, 1, 12),
        "winter_exam_end": date(start_year + 1, 1, 31),
        "winter_holidays_start": date(start_year + 1, 2, 1),
        "winter_holidays_end": date(start_year + 1, 2, 8),
        "spring_classes_start": date(start_year + 1, 2, 9),
        "spring_classes_end": date(start_year + 1, 6, 6),
        "spring_gap_start": date(start_year + 1, 6, 7),
        "spring_gap_end": date(start_year + 1, 6, 10),
        "spring_credit_start": date(start_year + 1, 6, 11),
        "spring_credit_end": date(start_year + 1, 6, 20),
        "summer_exam_start": date(start_year + 1, 6, 21),
        "summer_exam_end": date(start_year + 1, 7, 6),
        "summer_holidays_start": date(start_year + 1, 7, 6),
        "summer_holidays_end": date(start_year + 1, 8, 31),
    }


def _semester_key(start_year: int, term: int) -> str:
    return f"{start_year}-{start_year + 1}:{term}"


def _day_id_from_date(value: date) -> int:
    return int(value.isoweekday())


def _semester_label(semester_key: str) -> str:
    if ":" not in str(semester_key):
        return semester_key
    years, _term = str(semester_key).split(":", 1)
    return str(years)


def _semester_base_for_date(value: date):
    month = int(value.month)
    year = int(value.year)

    start_year = year if month >= 9 else year - 1
    term = 1 if (month >= 9 or month == 1) else 2
    semester_key = _semester_key(start_year, term)

    return {
        "key": semester_key,
        "label": _semester_label(semester_key),
        "start_year": start_year,
        "term": term,
    }


def _date_context(value: date):
    semester_base = _semester_base_for_date(value)
    if not semester_base:
        return None

    calendar = _calendar_for_start_year(int(semester_base["start_year"]))
    day_of_week = _day_id_from_date(value)

    ctx = {
        "semester_key": semester_base["key"],
        "semester_label": semester_base["label"],
        "day_of_week": day_of_week,
        "stage": "unknown",
        "week_number": None,
        "week_parity": None,
    }

    stage = "unknown"
    class_start_date = None
    if _in_range(value, calendar["autumn_classes_start"], calendar["autumn_classes_end"]):
        stage = "classes_autumn"
        class_start_date = calendar["autumn_classes_start"]
    elif _in_range(value, calendar["spring_classes_start"], calendar["spring_classes_end"]):
        stage = "classes_spring"
        class_start_date = calendar["spring_classes_start"]
    elif _in_range(value, calendar["new_year_break_start"], calendar["new_year_break_end"]):
        stage = "new_year_break"
    elif _in_range(value, calendar["winter_holidays_start"], calendar["winter_holidays_end"]):
        stage = "winter_holidays"
    elif _in_range(value, calendar["summer_holidays_start"], calendar["summer_holidays_end"]):
        stage = "summer_holidays"
    elif _in_range(value, calendar["autumn_credit_start"], calendar["autumn_credit_end"]):
        stage = "autumn_credit"
    elif _in_range(value, calendar["winter_gap_start"], calendar["winter_gap_end"]):
        stage = "winter_gap"
    elif _in_range(value, calendar["winter_exam_start"], calendar["winter_exam_end"]):
        stage = "winter_exam"
    elif _in_range(value, calendar["spring_gap_start"], calendar["spring_gap_end"]):
        stage = "spring_gap"
    elif _in_range(value, calendar["spring_credit_start"], calendar["spring_credit_end"]):
        stage = "spring_credit"
    elif _in_range(value, calendar["summer_exam_start"], calendar["summer_exam_end"]):
        stage = "summer_exam"

    ctx["stage"] = stage

    if stage in ("classes_autumn", "classes_spring") and class_start_date:
        raw_week = ((value - class_start_date).days // 7) + 1
        week_number = min(max(1, int(raw_week)), 16)
        ctx["week_number"] = week_number
        ctx["week_parity"] = "I" if (week_number % 2 == 1) else "II"

    return ctx


def _active_semester_base():
    today = date.today()
    current = _semester_base_for_date(today)
    if current:
        return current
    start_year = today.year
    autumn_key = _semester_key(start_year, 1)
    return {"key": autumn_key, "label": _semester_label(autumn_key), "start_year": start_year, "term": 1}


def _stage_add_error(ctx) -> str:
    stage = str((ctx or {}).get("stage") or "")
    if stage == "autumn_credit":
        return "Идет зачетная сессия (24 декабря - 31 декабря), пары недоступны"
    if stage == "new_year_break":
        return "Идут новогодние выходные (31 декабря - 9 января), пары недоступны"
    if stage == "winter_gap":
        return "Период между праздниками и экзаменационной сессией, пары недоступны"
    if stage == "winter_exam":
        return "Идет зимняя экзаменационная сессия (12 января - 31 января), пары недоступны"
    if stage == "winter_holidays":
        return "Идут зимние каникулы (1 февраля - 8 февраля), пары недоступны"
    if stage == "spring_gap":
        return "Период между занятиями и зачетной сессией, пары недоступны"
    if stage == "spring_credit":
        return "Идет зачетная сессия (11 июня - 20 июня), пары недоступны"
    if stage == "summer_exam":
        return "Идет летняя экзаменационная сессия (21 июня - 6 июля), пары недоступны"
    if stage == "summer_holidays":
        return "Идут летние каникулы (6 июля - 31 августа), пары недоступны"
    return f"Дата вне учебного периода для {ctx['semester_label']}"


def _parse_lesson_date(raw_value):
    raw = str(raw_value or "").strip()
    if not raw:
        return None
    try:
        return date.fromisoformat(raw)
    except Exception:
        return None


def _auto_cal_proxy_get(url: str, headers=None):
    request_headers = dict(headers or {})
    req = urllib.request.Request(url, headers=request_headers, method="GET")
    with urllib.request.urlopen(req, timeout=AUTO_CAL_TIMEOUT_SEC) as response:
        payload = response.read()
        content_type = str(response.headers.get("Content-Type") or "application/octet-stream")
    return payload, content_type


def _pair_number_from_datetime(value: datetime):
    if value is None:
        return 0
    start_minutes = int(value.hour) * 60 + int(value.minute)
    best_pair = 0
    best_delta = 10**9
    for slot in PAIR_SLOTS:
        raw_time = str(slot.get("time") or "")
        if "-" not in raw_time:
            continue
        slot_start = raw_time.split("-", 1)[0].strip()
        if ":" not in slot_start:
            continue
        hh, mm = slot_start.split(":", 1)
        try:
            slot_minutes = int(hh) * 60 + int(mm)
        except Exception:
            continue
        delta = abs(start_minutes - slot_minutes)
        if delta < best_delta:
            best_delta = delta
            best_pair = int(slot.get("number") or 0)
    if best_pair in VALID_PAIR_NUMBERS and best_delta <= 120:
        return int(best_pair)
    return 0


def _parse_datetime_iso(raw_value):
    text = str(raw_value or "").strip()
    if not text:
        return None
    try:
        dt = datetime.fromisoformat(text)
    except Exception:
        return None
    if dt.tzinfo is not None:
        dt = dt.astimezone(MOSCOW_TZ)
    return dt


def _as_utc(value):
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _to_moscow(value):
    utc_value = _as_utc(value)
    if utc_value is None:
        return None
    return utc_value.astimezone(MOSCOW_TZ)


def _format_moscow(value, with_seconds: bool = False) -> str:
    local_value = _to_moscow(value)
    if local_value is None:
        return "-"
    pattern = "%d.%m.%Y %H:%M:%S" if with_seconds else "%d.%m.%Y %H:%M"
    return local_value.strftime(pattern)


def _build_qr_data_uri(link: str):
    if not link:
        return None, None
    if segno is None:
        return None, "Модуль segno не установлен. Выполните установку: pip install -r requirements.txt"

    try:
        qr = segno.make(link, error="m")
        buffer = io.BytesIO()
        qr.save(buffer, kind="png", scale=7, border=2)
        raw_png = buffer.getvalue()
        encoded = base64.b64encode(raw_png).decode("ascii")
        return f"data:image/png;base64,{encoded}", None
    except Exception as exc:
        return None, f"Не удалось сгенерировать QR: {exc}"


def _source_label(source_key: str) -> str:
    source = str(source_key or "").strip().lower()
    if source == "qr":
        return "QR"
    if source == "manual":
        return "Локально"
    if source == "unmarked":
        return "Не отмечено"
    return "-"


def _normalize_source_filters(values):
    out = []
    for raw in values or []:
        value = str(raw or "").strip().lower()
        if value in {"qr", "manual", "unmarked"} and value not in out:
            out.append(value)
    return out


def _normalize_status(raw_status: str):
    value = str(raw_status or "").strip().lower()
    if value in ATTENDANCE_STATUSES:
        return value
    return None


def _normalize_status_filters(values):
    out = []
    for raw in values or []:
        value = _normalize_status(raw)
        if value and value not in out:
            out.append(value)
    return out


def _safe_excel_filename(raw_name: str) -> str:
    safe = re.sub(r'[<>:"/\\|?*\n\r\t]+', "_", str(raw_name or "").strip())
    safe = re.sub(r"\s+", " ", safe).strip(" .")
    return safe or "journal_attendance"


def _unique_group_ids(values):
    from utils.services.runtime import parse_int
    out = []
    for raw in values or []:
        gid = parse_int(raw, default=0)
        if gid > 0 and gid not in out:
            out.append(int(gid))
    return out


def _parse_int_list(values):
    from utils.services.runtime import parse_int
    out = []
    for raw in values or []:
        value = parse_int(raw, default=0)
        if value > 0 and value not in out:
            out.append(int(value))
    return out


def _normalize_group_ids_csv(values):
    return ",".join(str(gid) for gid in _unique_group_ids(values))


def _group_stream_years_from_ids(group_ids):
    from utils.core.models import Group
    ids = _unique_group_ids(group_ids)
    if not ids:
        return set(), {}
    group_rows = Group.query.filter(Group.id.in_(ids)).all()
    name_map = {int(group.id): str(group.name or "") for group in group_rows}
    stream_set = set()
    for gid in ids:
        stream_year = _extract_group_stream_year(name_map.get(int(gid), ""))
        if stream_year:
            stream_set.add(stream_year)
    return stream_set, name_map


def _lesson_group_ids(lesson):
    from utils.services.runtime import parse_int
    if lesson is None:
        return []
    out = []
    raw_csv = str(getattr(lesson, "group_ids", "") or "").strip()
    if raw_csv:
        out.extend(_unique_group_ids(raw_csv.split(",")))
    primary_group_id = parse_int(getattr(lesson, "group_id", 0), default=0)
    if primary_group_id > 0 and primary_group_id not in out:
        out.insert(0, int(primary_group_id))
    return _unique_group_ids(out)


def _lesson_primary_group_id(lesson):
    from utils.services.runtime import parse_int
    group_ids = _lesson_group_ids(lesson)
    if group_ids:
        return int(group_ids[0])
    return parse_int(getattr(lesson, "group_id", 0), default=0)


def _pair_info(pair_number: int):
    return PAIR_SLOT_BY_NUMBER.get(int(pair_number)) or {}
