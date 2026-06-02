from datetime import datetime, timedelta, timezone

try:
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None

try:
    MOSCOW_TZ = ZoneInfo("Europe/Moscow") if ZoneInfo is not None else timezone(timedelta(hours=3), name="MSK")
except Exception:
    MOSCOW_TZ = timezone(timedelta(hours=3), name="MSK")


def _resolve_tz(tzid: str | None):
    if not tzid:
        return MOSCOW_TZ
    cleaned = str(tzid).strip().strip('"')
    if cleaned.startswith("/"):
        cleaned = cleaned[1:]
    if not cleaned:
        return MOSCOW_TZ
    if cleaned.upper() == "UTC":
        return timezone.utc
    try:
        return ZoneInfo(cleaned) if ZoneInfo is not None else MOSCOW_TZ
    except Exception:
        return MOSCOW_TZ


def _unfold_ics_lines(ics_text: str) -> list[str]:
    raw_lines = str(ics_text or "").replace("\r\n", "\n").replace("\r", "\n").split("\n")
    unfolded: list[str] = []
    for line in raw_lines:
        if (line.startswith(" ") or line.startswith("\t")) and unfolded:
            unfolded[-1] += line[1:]
        else:
            unfolded.append(line)
    return unfolded


def _parse_ics_line(line: str):
    if ":" not in line:
        return None
    left, value = line.split(":", 1)
    chunks = left.split(";")
    name = chunks[0].strip().upper()
    if not name:
        return None

    params = {}
    for chunk in chunks[1:]:
        if "=" in chunk:
            key, param_value = chunk.split("=", 1)
            params[key.strip().upper()] = param_value.strip()
        else:
            params[chunk.strip().upper()] = ""
    return name, params, value


def _unescape_ics_value(value: str | None) -> str:
    if value is None:
        return ""
    text = str(value)
    text = text.replace("\\n", "\n").replace("\\N", "\n")
    text = text.replace("\\,", ",").replace("\\;", ";")
    text = text.replace("\\\\", "\\")
    return text.strip()


def _parse_ics_datetime(raw_value: str, tzid: str | None):
    text = str(raw_value or "").strip()
    if not text:
        return None, False

    if len(text) == 8 and text.isdigit():
        dt = datetime.strptime(text, "%Y%m%d")
        return dt.replace(tzinfo=MOSCOW_TZ), True

    is_utc = text.endswith("Z")
    if is_utc:
        text = text[:-1]

    parsed = None
    for fmt in ("%Y%m%dT%H%M%S", "%Y%m%dT%H%M"):
        try:
            parsed = datetime.strptime(text, fmt)
            break
        except ValueError:
            continue
    if parsed is None:
        return None, False

    dt = parsed.replace(tzinfo=timezone.utc if is_utc else _resolve_tz(tzid))
    return dt.astimezone(MOSCOW_TZ), False


def _collect_vevent_blocks(lines: list[str]):
    blocks = []
    current = None
    for line in lines:
        if line == "BEGIN:VEVENT":
            current = []
            continue
        if line == "END:VEVENT":
            if current is not None:
                blocks.append(current)
            current = None
            continue
        if current is None:
            continue
        parsed = _parse_ics_line(line)
        if parsed is not None:
            current.append(parsed)
    return blocks


def _first_prop(props, name: str):
    values = props.get(name, [])
    if not values:
        return None
    return values[0]


def _parse_event(event_lines):
    props = {}
    for name, params, value in event_lines:
        props.setdefault(name, []).append((params, value))

    dtstart = _first_prop(props, "DTSTART")
    if dtstart is None:
        return None

    start_dt, is_date = _parse_ics_datetime(dtstart[1], dtstart[0].get("TZID"))
    if start_dt is None:
        return None

    dtend = _first_prop(props, "DTEND")
    if dtend is not None:
        end_dt, _ = _parse_ics_datetime(dtend[1], dtend[0].get("TZID"))
    else:
        end_dt = start_dt + timedelta(minutes=95)
    if end_dt is None:
        end_dt = start_dt + timedelta(minutes=95)

    duration = end_dt - start_dt
    if duration <= timedelta(0):
        duration = timedelta(minutes=95)

    exdates = set()
    for params, raw in props.get("EXDATE", []):
        for token in str(raw).split(","):
            ex_dt, _ = _parse_ics_datetime(token, params.get("TZID"))
            if ex_dt is None:
                continue
            exdates.add(ex_dt.astimezone(MOSCOW_TZ).replace(microsecond=0).isoformat())

    groups = []
    for _params, raw in props.get("X-META-GROUP", []):
        unescaped = _unescape_ics_value(raw)
        if not unescaped:
            continue
        for group in unescaped.split(","):
            name = group.strip()
            if name:
                groups.append(name)
    if groups:
        groups = list(dict.fromkeys(groups))

    teachers = []
    for _params, raw in props.get("X-META-TEACHER", []):
        unescaped = _unescape_ics_value(raw)
        if not unescaped:
            continue
        for teacher in unescaped.split(","):
            name = teacher.strip()
            if name:
                teachers.append(name)
    if teachers:
        teachers = list(dict.fromkeys(teachers))

    rrule_entry = _first_prop(props, "RRULE")
    rrule_value = rrule_entry[1].strip() if rrule_entry is not None else ""

    uid_entry = _first_prop(props, "UID")
    summary_entry = _first_prop(props, "SUMMARY")
    description_entry = _first_prop(props, "DESCRIPTION")
    location_entry = _first_prop(props, "LOCATION")
    url_entry = _first_prop(props, "URL")
    discipline_entry = _first_prop(props, "X-META-DISCIPLINE")
    lesson_type_entry = _first_prop(props, "X-META-LESSON_TYPE")
    lesson_type_full_entry = _first_prop(props, "X-META-FULL_LESSON_TYPE")

    return {
        "start": start_dt,
        "duration": duration,
        "is_date": is_date,
        "rrule": rrule_value,
        "exdates": exdates,
        "id": _unescape_ics_value(uid_entry[1] if uid_entry else ""),
        "title": _unescape_ics_value(summary_entry[1] if summary_entry else ""),
        "description": _unescape_ics_value(description_entry[1] if description_entry else ""),
        "location": _unescape_ics_value(location_entry[1] if location_entry else ""),
        "onlineLink": _unescape_ics_value(url_entry[1] if url_entry else ""),
        "discipline": _unescape_ics_value(discipline_entry[1] if discipline_entry else ""),
        "lessonType": _unescape_ics_value(lesson_type_entry[1] if lesson_type_entry else ""),
        "lessonTypeFull": _unescape_ics_value(lesson_type_full_entry[1] if lesson_type_full_entry else ""),
        "firstTeacher": teachers[0] if teachers else "",
        "teachers": teachers,
        "groups": groups,
    }


def _expand_rrule(rrule_value: str, start: datetime, range_start: datetime, range_end: datetime) -> list[datetime]:
    params = {}
    for part in str(rrule_value).upper().split(";"):
        if "=" in part:
            k, v = part.split("=", 1)
            params[k.strip()] = v.strip()

    freq = params.get("FREQ", "WEEKLY")
    try:
        interval = int(params.get("INTERVAL") or 1)
    except ValueError:
        interval = 1
    if interval <= 0:
        interval = 1

    until_val = params.get("UNTIL")
    until_dt = None
    if until_val:
        parsed_until, _ = _parse_ics_datetime(until_val, None)
        if parsed_until:
            until_dt = parsed_until.astimezone(start.tzinfo)

    count_val = params.get("COUNT")
    count = None
    if count_val and count_val.isdigit():
        try:
            count = int(count_val)
        except ValueError:
            count = None
    if count is not None and count <= 0:
        count = None

    byday_val = params.get("BYDAY")
    day_map = {"MO": 0, "TU": 1, "WE": 2, "TH": 3, "FR": 4, "SA": 5, "SU": 6}
    byday = [day_map[d] for d in byday_val.split(",") if d in day_map] if byday_val else None

    occurrences = []
    current = start
    occurred_count = 0
    max_loops = 5000
    loops = 0

    while current <= range_end and loops < max_loops:
        loops += 1
        if until_dt and current > until_dt:
            break

        if byday is None or current.weekday() in byday:
            if current >= range_start:
                occurrences.append(current)
            occurred_count += 1
            if count and occurred_count >= count:
                break

        if freq == "DAILY":
            current += timedelta(days=interval)
        elif freq == "WEEKLY":
            if byday:
                next_day = current + timedelta(days=1)
                if next_day.weekday() == 0 and interval > 1:
                    next_day += timedelta(weeks=interval - 1)
                current = next_day
            else:
                current += timedelta(weeks=interval)
        else:
            current += timedelta(weeks=interval)

    return occurrences


def _expand_occurrences(event: dict, range_start: datetime, range_end: datetime):
    if event["is_date"]:
        return []

    start = event["start"]
    duration = event["duration"]
    rrule_value = event["rrule"]
    exdates = event["exdates"]
    starts = []

    if rrule_value:
        try:
            starts = _expand_rrule(rrule_value, start, range_start - duration, range_end)
        except Exception:
            starts = [start]
    else:
        starts = [start]

    occurrences = []
    for item in starts:
        if not isinstance(item, datetime):
            continue
        occ_start = item
        if occ_start.tzinfo is None:
            occ_start = occ_start.replace(tzinfo=start.tzinfo or MOSCOW_TZ)
        occ_start = occ_start.astimezone(MOSCOW_TZ).replace(microsecond=0)
        if occ_start.isoformat() in exdates:
            continue

        occ_end = (occ_start + duration).replace(microsecond=0)
        if occ_end <= range_start or occ_start >= range_end:
            continue
        occurrences.append((occ_start, occ_end))

    return occurrences


def parse_ical_lessons(ics_text: str, range_start: datetime, range_end: datetime):
    lessons = []
    unique = set()
    lines = _unfold_ics_lines(ics_text)
    blocks = _collect_vevent_blocks(lines)

    for block in blocks:
        event = _parse_event(block)
        if event is None:
            continue

        for occ_start, occ_end in _expand_occurrences(event, range_start, range_end):
            lesson_id = event["id"] or f"{event['title']}_{event['location']}"
            uniq_key = f"{lesson_id}|{occ_start.isoformat()}|{occ_end.isoformat()}"
            if uniq_key in unique:
                continue
            unique.add(uniq_key)

            lessons.append(
                {
                    "id": lesson_id,
                    "start": occ_start.isoformat(),
                    "end": occ_end.isoformat(),
                    "title": event["title"],
                    "description": event["description"],
                    "location": event["location"],
                    "discipline": event["discipline"] or None,
                    "lessonType": event["lessonType"] or None,
                    "lessonTypeFull": event["lessonTypeFull"] or None,
                    "firstTeacher": event["firstTeacher"] or None,
                    "teachers": event["teachers"],
                    "groups": event["groups"],
                    "onlineLink": event["onlineLink"],
                }
            )

    lessons.sort(key=lambda item: (item["start"], item["title"]))
    return lessons
