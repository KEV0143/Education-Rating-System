import atexit
from utils.journal.realtime import RealtimeEventBus
from utils.journal.tunnel import JournalTunnelManager

def register_journal_routes(app, db, Course, Group, Student, JournalLesson, JournalLessonSession, JournalAttendance, parse_int):
    runtime = app.extensions.setdefault("journal_runtime", {})
    attendance_events = runtime.setdefault("attendance_events", RealtimeEventBus())
    tunnel_events = runtime.setdefault("tunnel_events", RealtimeEventBus())
    
    tunnel = runtime.get("tunnel")
    if tunnel is None:
        tunnel = JournalTunnelManager()
        runtime["tunnel"] = tunnel
        tunnel.set_on_change(lambda: tunnel_events.bump("tunnel"))

    if not runtime.get("tunnel_atexit_registered"):
        atexit.register(lambda: tunnel.close())
        runtime["tunnel_atexit_registered"] = True

    if "active_public_session_key" not in runtime:
        runtime["active_public_session_key"] = ""

    runtime["JournalLesson"] = JournalLesson
    runtime["JournalLessonSession"] = JournalLessonSession
    runtime["JournalAttendance"] = JournalAttendance

    ctx = {
        "runtime": runtime,
        "attendance_events": attendance_events,
        "tunnel_events": tunnel_events,
        "tunnel": tunnel
    }

    from utils.journal.routes_calendar import register_calendar_routes
    from utils.journal.routes_lessons import register_lesson_routes
    from utils.journal.routes_attendance import register_attendance_routes
    from utils.journal.routes_groups import register_group_routes
    from utils.journal.routes_auto_schedule import register_journal_auto_schedule_routes
    from utils.journal.routes_export import register_journal_export_routes

    register_calendar_routes(app, db, Course, Group, Student, JournalLesson, JournalLessonSession, JournalAttendance, parse_int, ctx)
    register_lesson_routes(app, db, Course, Group, Student, JournalLesson, JournalLessonSession, JournalAttendance, parse_int, ctx)
    register_attendance_routes(app, db, Course, Group, Student, JournalLesson, JournalLessonSession, JournalAttendance, parse_int, ctx)
    register_group_routes(app, db, Course, Group, Student, JournalLesson, JournalLessonSession, JournalAttendance, parse_int, ctx)
    register_journal_auto_schedule_routes(app, db, Course, Group, Student, JournalLesson, JournalLessonSession, JournalAttendance, parse_int)
    register_journal_export_routes(app, db, Course, Group, Student, JournalLesson, JournalLessonSession, JournalAttendance, parse_int)
