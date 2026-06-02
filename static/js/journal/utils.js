window.JournalApp.pad2 = function(value) {
  return String(value).padStart(2, '0');
};

window.JournalApp.toIsoDate = function(date) {
  return `${date.getFullYear()}-${window.JournalApp.pad2(date.getMonth() + 1)}-${window.JournalApp.pad2(date.getDate())}`;
};

window.JournalApp.fromIsoDate = function(iso) {
  if (!iso || !/^\d{4}-\d{2}-\d{2}$/.test(iso)) return null;
  const [year, month, day] = iso.split('-').map(Number);
  return new Date(year, month - 1, day);
};

window.JournalApp.formatRuDate = function(iso) {
  const date = window.JournalApp.fromIsoDate(iso);
  if (!date) return '-';
  return date.toLocaleDateString('ru-RU', {
    weekday: 'long',
    day: 'numeric',
    month: 'long',
    year: 'numeric'
  });
};

window.JournalApp.getDayIdByDate = function(date) {
  const nativeDay = date.getDay();
  return nativeDay === 0 ? 7 : nativeDay;
};

window.JournalApp.getUtcMidnightMs = function(dateObj) {
  return Date.UTC(dateObj.getFullYear(), dateObj.getMonth(), dateObj.getDate());
};

window.JournalApp.semesterLabelFromKey = function(semesterKey) {
  if (!semesterKey || !String(semesterKey).includes(':')) return String(semesterKey || '');
  const [years] = String(semesterKey).split(':');
  return years;
};

window.JournalApp.getSemesterBaseByDate = function(dateObj) {
  const month = dateObj.getMonth() + 1;
  const year = dateObj.getFullYear();
  const startYear = (month >= 9) ? year : (year - 1);
  const term = (month >= 9 || month === 1) ? 1 : 2;
  const semesterKey = `${startYear}-${startYear + 1}:${term}`;
  return {
    startYear,
    term,
    semesterKey,
    semesterLabel: window.JournalApp.semesterLabelFromKey(semesterKey),
  };
};

window.JournalApp.getAcademicCalendarByStartYear = function(startYear) {
  return {
    autumnClassesStart: new Date(startYear, 8, 1),
    autumnClassesEnd: new Date(startYear, 11, 23),
    autumnCreditStart: new Date(startYear, 11, 24),
    autumnCreditEnd: new Date(startYear, 11, 31),
    newYearBreakStart: new Date(startYear, 11, 31),
    newYearBreakEnd: new Date(startYear + 1, 0, 9),
    winterGapStart: new Date(startYear + 1, 0, 10),
    winterGapEnd: new Date(startYear + 1, 0, 11),
    winterExamStart: new Date(startYear + 1, 0, 12),
    winterExamEnd: new Date(startYear + 1, 0, 31),
    winterHolidayStart: new Date(startYear + 1, 1, 1),
    winterHolidayEnd: new Date(startYear + 1, 1, 8),
    springClassesStart: new Date(startYear + 1, 1, 9),
    springClassesEnd: new Date(startYear + 1, 5, 6),
    springGapStart: new Date(startYear + 1, 5, 7),
    springGapEnd: new Date(startYear + 1, 5, 10),
    springCreditStart: new Date(startYear + 1, 5, 11),
    springCreditEnd: new Date(startYear + 1, 5, 20),
    summerExamStart: new Date(startYear + 1, 5, 21),
    summerExamEnd: new Date(startYear + 1, 6, 6),
    summerHolidayStart: new Date(startYear + 1, 6, 6),
    summerHolidayEnd: new Date(startYear + 1, 7, 31),
  };
};

window.JournalApp.isDateInRange = function(dateObj, startDate, endDate) {
  const valueMs = window.JournalApp.getUtcMidnightMs(dateObj);
  return valueMs >= window.JournalApp.getUtcMidnightMs(startDate) && valueMs <= window.JournalApp.getUtcMidnightMs(endDate);
};

window.JournalApp.getDateAcademicInfo = function(dateObj) {
  const base = window.JournalApp.getSemesterBaseByDate(dateObj);
  const calendar = window.JournalApp.getAcademicCalendarByStartYear(base.startYear);

  const info = {
    ...base,
    dayId: window.JournalApp.getDayIdByDate(dateObj),
    stage: 'unknown',
    stageLabel: 'Вне учебного периода',
    weekNumber: null,
    weekParity: null,
    isTeachingPeriod: false,
  };

  let classStart = null;
  if (window.JournalApp.isDateInRange(dateObj, calendar.autumnClassesStart, calendar.autumnClassesEnd)) {
    info.stage = 'classes_autumn';
    info.stageLabel = 'Осенний учебный период';
    classStart = calendar.autumnClassesStart;
  } else if (window.JournalApp.isDateInRange(dateObj, calendar.springClassesStart, calendar.springClassesEnd)) {
    info.stage = 'classes_spring';
    info.stageLabel = 'Весенний учебный период';
    classStart = calendar.springClassesStart;
  } else if (window.JournalApp.isDateInRange(dateObj, calendar.newYearBreakStart, calendar.newYearBreakEnd)) {
    info.stage = 'new_year_break';
    info.stageLabel = 'Новогодние выходные';
  } else if (window.JournalApp.isDateInRange(dateObj, calendar.winterHolidayStart, calendar.winterHolidayEnd)) {
    info.stage = 'winter_holidays';
    info.stageLabel = 'Зимние каникулы';
  } else if (window.JournalApp.isDateInRange(dateObj, calendar.summerHolidayStart, calendar.summerHolidayEnd)) {
    info.stage = 'summer_holidays';
    info.stageLabel = 'Летние каникулы';
  } else if (window.JournalApp.isDateInRange(dateObj, calendar.autumnCreditStart, calendar.autumnCreditEnd)) {
    info.stage = 'autumn_credit';
    info.stageLabel = 'Зачетная сессия';
  } else if (window.JournalApp.isDateInRange(dateObj, calendar.winterGapStart, calendar.winterGapEnd)) {
    info.stage = 'winter_gap';
    info.stageLabel = 'Переходный период';
  } else if (window.JournalApp.isDateInRange(dateObj, calendar.winterExamStart, calendar.winterExamEnd)) {
    info.stage = 'winter_exam';
    info.stageLabel = 'Зимняя экзаменационная сессия';
  } else if (window.JournalApp.isDateInRange(dateObj, calendar.springGapStart, calendar.springGapEnd)) {
    info.stage = 'spring_gap';
    info.stageLabel = 'Переходный период';
  } else if (window.JournalApp.isDateInRange(dateObj, calendar.springCreditStart, calendar.springCreditEnd)) {
    info.stage = 'spring_credit';
    info.stageLabel = 'Зачетная сессия';
  } else if (window.JournalApp.isDateInRange(dateObj, calendar.summerExamStart, calendar.summerExamEnd)) {
    info.stage = 'summer_exam';
    info.stageLabel = 'Летняя экзаменационная сессия';
  }

  if (info.stage === 'classes_autumn' || info.stage === 'classes_spring') {
    const deltaDays = Math.floor((window.JournalApp.getUtcMidnightMs(dateObj) - window.JournalApp.getUtcMidnightMs(classStart)) / 86400000);
    const rawWeek = Math.floor(deltaDays / 7) + 1;
    const weekNumber = Math.min(Math.max(1, rawWeek), 16);
    info.weekNumber = weekNumber;
    info.weekParity = (weekNumber % 2 === 1) ? 'I' : 'II';
    info.isTeachingPeriod = true;
  }

  return info;
};

window.JournalApp.stageDisabledMessage = function(info) {
  if (!info) return 'Дата вне учебного периода';
  switch (String(info.stage || '')) {
    case 'autumn_credit':
      return 'Идет зачетная сессия (24 декабря - 31 декабря), пары недоступны';
    case 'new_year_break':
      return 'Идут новогодние выходные (31 декабря - 9 января), пары недоступны';
    case 'winter_gap':
      return 'Переходный период между праздниками и зимней сессией, пары недоступны';
    case 'winter_exam':
      return 'Идет зимняя экзаменационная сессия (12 января - 31 января), пары недоступны';
    case 'winter_holidays':
      return 'Идут зимние каникулы (1 февраля - 8 февраля), пары недоступны';
    case 'spring_gap':
      return 'Переходный период между занятиями и зачетной сессией, пары недоступны';
    case 'spring_credit':
      return 'Идет зачетная сессия (11 июня - 20 июня), пары недоступны';
    case 'summer_exam':
      return 'Идет летняя экзаменационная сессия (21 июня - 6 июля), пары недоступны';
    case 'summer_holidays':
      return 'Идут летние каникулы (6 июля - 31 августа), пары недоступны';
    default:
      return `Дата вне учебного периода для ${info.semesterLabel}`;
  }
};

window.JournalApp.parityWithLabel = function(parity) {
  if (String(parity) === 'I') return 'I нечётная';
  if (String(parity) === 'II') return 'II чётная';
  return String(parity || '');
};

window.JournalApp.sortLessons = function(lessons) {
  return lessons.slice().sort((a, b) => {
    const pairDiff = Number(a.pair_number || 0) - Number(b.pair_number || 0);
    if (pairDiff !== 0) return pairDiff;
    const groupA = String(a.group_name || '');
    const groupB = String(b.group_name || '');
    return groupA.localeCompare(groupB, 'ru');
  });
};

window.JournalApp.getLessonsForDate = function(dateIso) {
  const date = window.JournalApp.fromIsoDate(dateIso);
  if (!date) return [];

  const dayId = window.JournalApp.getDayIdByDate(date);
  if (dayId === 7) return [];

  const semesterInfo = window.JournalApp.getDateAcademicInfo(date);
  if (!semesterInfo || !semesterInfo.isTeachingPeriod) return [];
  if (semesterInfo.semesterKey !== window.JournalApp.ACTIVE_SEMESTER_KEY) return [];

  const filtered = window.JournalApp.LESSONS.filter((lesson) =>
    String(lesson.semester_key || '') === window.JournalApp.ACTIVE_SEMESTER_KEY &&
    String(lesson.week_parity) === semesterInfo.weekParity &&
    Number(lesson.day_of_week) === dayId
  );

  return window.JournalApp.sortLessons(filtered);
};

window.JournalApp.monthKeyFromDateIso = function(dateIso) {
  const safe = String(dateIso || '').trim();
  if (!/^\d{4}-\d{2}-\d{2}$/.test(safe)) return '';
  return safe.slice(0, 7);
};

window.JournalApp.autoCalPairNumberByStart = function(isoValue) {
  const safe = String(isoValue || '').trim();
  if (safe.length < 16 || !safe.includes('T')) return 0;
  const timePart = safe.slice(11, 16);
  if (!/^\d{2}:\d{2}$/.test(timePart)) return 0;
  const hh = Number(timePart.slice(0, 2));
  const mm = Number(timePart.slice(3, 5));
  if (!Number.isInteger(hh) || !Number.isInteger(mm)) return 0;

  const startMinutes = hh * 60 + mm;
  let bestPair = 0;
  let bestDelta = Number.MAX_SAFE_INTEGER;
  
  const pairStartMinutes = window.JournalApp.PAIR_SLOTS
    .map((slot) => {
      const number = Number(slot.number || 0);
      const rawTime = String(slot.time || '');
      if (!number || !rawTime.includes('-')) return null;
      const startPart = rawTime.split('-', 1)[0].trim();
      const parsed = /^(\d{1,2}):(\d{2})$/.exec(startPart);
      if (!parsed) return null;
      const sh = Number(parsed[1]);
      const sm = Number(parsed[2]);
      if (!Number.isInteger(sh) || !Number.isInteger(sm)) return null;
      return { number, minutes: sh * 60 + sm };
    })
    .filter((item) => item && item.number > 0);

  pairStartMinutes.forEach((slot) => {
    const delta = Math.abs(startMinutes - Number(slot.minutes || 0));
    if (delta < bestDelta) {
      bestDelta = delta;
      bestPair = Number(slot.number || 0);
    }
  });
  if (bestPair > 0 && bestDelta <= 120) return bestPair;
  return 0;
};

window.JournalApp.countPairsInAutoCalLessons = function(lessons) {
  const list = Array.isArray(lessons) ? lessons : [];
  if (!list.length) return 0;
  const uniquePairs = new Set();
  list.forEach((lesson) => {
    const pairNumber = window.JournalApp.autoCalPairNumberByStart(lesson && lesson.start);
    if (pairNumber > 0) {
      uniquePairs.add(`pair:${pairNumber}`);
      return;
    }
    const startText = String((lesson && lesson.start) || '').trim();
    const fallbackKey = startText.length >= 16 ? startText.slice(11, 16) : startText;
    if (fallbackKey) uniquePairs.add(`time:${fallbackKey}`);
  });
  return uniquePairs.size;
};

window.JournalApp.normalizeComparableText = function(value) {
  return String(value || '')
    .toLowerCase()
    .replace(/\u00a0/g, ' ')
    .replace(/[–—−‑‒]/g, '-')
    .replace(/\s*-\s*/g, '-')
    .replace(/ё/g, 'е')
    .replace(/\((?:подгруппа|подгр\.?|подгр|подг|пг)\s*\d+\)/g, '')
    .replace(/(?:подгруппа|подгр\.?|подгр|подг|пг)\s*\d+$/g, '')
    .replace(/\s+/g, ' ')
    .trim();
};

window.JournalApp.normalizedNameList = function(values) {
  const out = [];
  (Array.isArray(values) ? values : []).forEach((raw) => {
    const norm = window.JournalApp.normalizeComparableText(raw);
    if (!norm || out.includes(norm)) return;
    out.push(norm);
  });
  return out;
};

window.JournalApp.lessonGroupNames = function(lesson) {
  if (!lesson || typeof lesson !== 'object') return [];
  if (Array.isArray(lesson.group_names)) return lesson.group_names;
  const text = String(lesson.group_name || '').trim();
  if (!text) return [];
  return text.split(',').map((part) => part.trim()).filter((part) => part.length > 0);
};

window.JournalApp.autoCalGroupNames = function(lesson) {
  if (!lesson || typeof lesson !== 'object') return [];
  return Array.isArray(lesson.groups) ? lesson.groups : [];
};

window.JournalApp.intersectionSize = function(setA, setB) {
  if (!setA || !setB || !setA.size || !setB.size) return 0;
  let count = 0;
  setA.forEach((value) => {
    if (setB.has(value)) count += 1;
  });
  return count;
};

window.JournalApp.findImportedLessonForAutoPair = function(autoLesson, pairLessons) {
  const candidates = Array.isArray(pairLessons) ? pairLessons : [];
  if (!candidates.length) return null;

  const targetTitle = window.JournalApp.normalizeComparableText(autoLesson && (autoLesson.discipline || autoLesson.title || ''));
  const targetGroupSet = new Set(window.JournalApp.normalizedNameList(window.JournalApp.autoCalGroupNames(autoLesson)));
  const hasTargetSignals = Boolean(targetTitle) || targetGroupSet.size > 0;
  if (!hasTargetSignals && candidates.length === 1) return candidates[0];

  let bestLesson = null;
  let bestScore = -1;

  candidates.forEach((candidate) => {
    let score = 0;
    const candidateTitle = window.JournalApp.normalizeComparableText(candidate && (candidate.course_title || ''));
    if (targetTitle && candidateTitle === targetTitle) score += 4;
    if (targetTitle && candidateTitle && candidateTitle.includes(targetTitle)) score += 2;

    const candidateGroupSet = new Set(window.JournalApp.normalizedNameList(window.JournalApp.lessonGroupNames(candidate)));
    const overlap = window.JournalApp.intersectionSize(targetGroupSet, candidateGroupSet);
    if (overlap > 0) score += overlap * 6;
    if (
      targetGroupSet.size > 0 &&
      candidateGroupSet.size > 0 &&
      overlap === targetGroupSet.size &&
      overlap === candidateGroupSet.size
    ) {
      score += 3;
    }

    if (score > bestScore) {
      bestScore = score;
      bestLesson = candidate;
    }
  });

  if (bestScore <= 0) return null;
  return bestLesson;
};

window.JournalApp.getPairsCountForDate = function(dateIso) {
  const selectedTeacher = window.JournalApp.getSelectedAutoCalTeacherForJournal();
  if (selectedTeacher) {
    const monthKey = window.JournalApp.monthKeyFromDateIso(dateIso);
    const monthCacheKey = `${selectedTeacher.id}_${monthKey}`;
    const monthMap = window.JournalApp.autoCalMonthCountsCache.get(monthCacheKey);
    if (monthMap && monthMap.has(dateIso)) {
      return Number(monthMap.get(dateIso) || 0);
    }
    const dayCacheKey = `${selectedTeacher.id}_${dateIso}`;
    if (window.JournalApp.autoCalScheduleDayCache.has(dayCacheKey)) {
      return window.JournalApp.countPairsInAutoCalLessons(window.JournalApp.autoCalScheduleDayCache.get(dayCacheKey));
    }
    return 0;
  }
  const lessons = window.JournalApp.getLessonsForDate(dateIso);
  return new Set(lessons.map((lesson) => Number(lesson.pair_number))).size;
};

window.JournalApp.buildLessonAttendanceUrl = function(lesson, dateIso) {
  if (lesson && lesson.attendance_url) return String(lesson.attendance_url);
  return `/journal/lesson/${Number(lesson.id)}?date=${encodeURIComponent(dateIso)}`;
};

window.JournalApp.setDayMessage = function(text, type) {
  const el = document.getElementById('dayMessage');
  if (!el) return;

  if (!text) {
    el.className = 'alert py-2 px-3 mt-2 d-none';
    el.textContent = '';
    return;
  }

  const map = {
    info: 'alert-info',
    success: 'alert-success',
    warning: 'alert-warning',
    danger: 'alert-danger'
  };
  el.className = `alert ${map[type] || map.info} py-2 px-3 mt-2`;
  el.textContent = text;
};

window.JournalApp.setModalMessage = function(text, type) {
  const el = document.getElementById('modalMessage');
  if (!el) return;

  if (!text) {
    el.className = 'alert py-2 px-3 mt-3 mb-0 d-none';
    el.textContent = '';
    return;
  }

  const map = {
    info: 'alert-info',
    success: 'alert-success',
    warning: 'alert-warning',
    danger: 'alert-danger'
  };
  el.className = `alert ${map[type] || map.info} py-2 px-3 mt-3 mb-0`;
  el.textContent = text;
};

window.JournalApp.setExportModalMessage = function(text, type) {
  const el = document.getElementById('exportModalMessage');
  if (!el) return;

  if (!text) {
    el.className = 'alert py-2 px-3 mt-3 mb-0 d-none';
    el.textContent = '';
    return;
  }

  const map = {
    info: 'alert-info',
    success: 'alert-success',
    warning: 'alert-warning',
    danger: 'alert-danger'
  };
  el.className = `alert ${map[type] || map.info} py-2 px-3 mt-3 mb-0`;
  el.textContent = text;
};

window.JournalApp.setAutoCalMessage = function(text, type) {
  const el = document.getElementById('autoCalMessage');
  if (!el) return;
  if (!text) {
    el.className = 'alert py-2 px-3 mb-3 d-none';
    el.textContent = '';
    return;
  }
  const map = {
    info: 'alert-info',
    success: 'alert-success',
    warning: 'alert-warning',
    danger: 'alert-danger'
  };
  el.className = `alert ${map[type] || map.info} py-2 px-3 mb-3`;
  el.textContent = text;
};

window.JournalApp.autoCalTeacherKey = function(teacherId) {
  return `${window.JournalApp.AUTO_CAL_TARGET}_${String(teacherId)}`;
};

window.JournalApp.normalizeAutoCalTeacher = function(raw) {
  if (!raw || typeof raw !== 'object') return null;
  const id = Number(raw.id || 0);
  if (!id || id <= 0) return null;
  const targetTitle = String(raw.targetTitle || raw.fullTitle || '').trim();
  const fullTitle = String(raw.fullTitle || raw.targetTitle || '').trim();
  return {
    key: window.JournalApp.autoCalTeacherKey(id),
    id,
    scheduleTarget: Number(raw.scheduleTarget || window.JournalApp.AUTO_CAL_TARGET),
    targetTitle: targetTitle || fullTitle,
    fullTitle: fullTitle || targetTitle || `Преподаватель #${id}`,
    iCalLink: String(raw.iCalLink || '').trim(),
    scheduleImageLink: String(raw.scheduleImageLink || '').trim(),
    scheduleUpdateImageLink: String(raw.scheduleUpdateImageLink || '').trim(),
    addedAt: String(raw.addedAt || new Date().toISOString())
  };
};

window.JournalApp.loadAutoCalUsers = function() {
  try {
    const raw = localStorage.getItem(window.JournalApp.AUTO_CAL_STORAGE_KEY);
    if (!raw) return [];
    const parsed = JSON.parse(raw);
    if (!Array.isArray(parsed)) return [];
    return parsed
      .map((item) => window.JournalApp.normalizeAutoCalTeacher(item))
      .filter((item) => Boolean(item));
  } catch (_error) {
    return [];
  }
};

window.JournalApp.loadAutoCalSelectedKeys = function() {
  try {
    const raw = localStorage.getItem(window.JournalApp.AUTO_CAL_SELECTED_STORAGE_KEY);
    if (!raw) return new Set();
    const parsed = JSON.parse(raw);
    if (!Array.isArray(parsed)) return new Set();
    return new Set(
      parsed
        .map((value) => String(value || '').trim())
        .filter((value) => value.length > 0)
    );
  } catch (_error) {
    return new Set();
  }
};

window.JournalApp.loadAutoCalUseCustomSchedule = function() {
  try {
    const raw = localStorage.getItem(window.JournalApp.AUTO_CAL_USE_CUSTOM_STORAGE_KEY);
    if (raw === null) return true;
    return String(raw) !== '0';
  } catch (_error) {
    return true;
  }
};

window.JournalApp.saveAutoCalSettingsToServer = function() {
  const token = window.JournalApp.CSRF_TOKEN;
  const headers = { 'Content-Type': 'application/json' };
  if (token) headers['X-CSRFToken'] = token;

  const payload = {
    users: window.JournalApp.autoCalUsers,
    selected: Array.from(window.JournalApp.autoCalSelectedKeys),
    use_custom: window.JournalApp.autoCalUseCustomSchedule
  };

  fetch('/api/journal/auto-calendar/settings', {
    method: 'POST',
    headers: headers,
    body: JSON.stringify(payload)
  }).catch(() => {});
};

window.JournalApp.saveAutoCalUsers = function() {
  localStorage.setItem(window.JournalApp.AUTO_CAL_STORAGE_KEY, JSON.stringify(window.JournalApp.autoCalUsers));
  window.JournalApp.saveAutoCalSettingsToServer();
};

window.JournalApp.saveAutoCalSelectedKeys = function() {
  localStorage.setItem(window.JournalApp.AUTO_CAL_SELECTED_STORAGE_KEY, JSON.stringify(Array.from(window.JournalApp.autoCalSelectedKeys)));
  window.JournalApp.saveAutoCalSettingsToServer();
};

window.JournalApp.saveAutoCalUseCustomSchedule = function() {
  localStorage.setItem(window.JournalApp.AUTO_CAL_USE_CUSTOM_STORAGE_KEY, window.JournalApp.autoCalUseCustomSchedule ? '1' : '0');
  window.JournalApp.saveAutoCalSettingsToServer();
};

window.JournalApp.syncAutoCalSelectionWithUsers = function() {
  const valid = new Set(window.JournalApp.autoCalUsers.map((entry) => entry.key));
  let changed = false;

  Array.from(window.JournalApp.autoCalSelectedKeys).forEach((key) => {
    if (valid.has(key)) return;
    window.JournalApp.autoCalSelectedKeys.delete(key);
    changed = true;
  });

  if (window.JournalApp.autoCalSelectedKeys.size > 1) {
    const first = Array.from(window.JournalApp.autoCalSelectedKeys)[0];
    window.JournalApp.autoCalSelectedKeys = new Set([first]);
    changed = true;
  }

  if (!window.JournalApp.autoCalSelectedKeys.size && window.JournalApp.autoCalUsers.length && !window.JournalApp.autoCalUseCustomSchedule) {
    window.JournalApp.autoCalSelectedKeys = new Set([window.JournalApp.autoCalUsers[0].key]);
    changed = true;
  }

  if (!window.JournalApp.autoCalUsers.length) {
    window.JournalApp.autoCalUseCustomSchedule = true;
  }

  if (!window.JournalApp.autoCalUseCustomSchedule && !window.JournalApp.autoCalSelectedKeys.size) {
    window.JournalApp.autoCalUseCustomSchedule = true;
  }

  if (changed) {
    window.JournalApp.saveAutoCalSelectedKeys();
  }
  window.JournalApp.saveAutoCalUseCustomSchedule();
};

window.JournalApp.setAutoCalSelectedCount = function() {
  const el = document.getElementById('autoCalSelectedCount');
  if (!el) return;
  if (window.JournalApp.autoCalUseCustomSchedule) {
    el.textContent = 'Источник журнала: Пользовательское расписание';
    return;
  }
  const count = window.JournalApp.autoCalSelectedKeys.size;
  el.textContent = `Источник журнала: преподаватель (${count})`;
};

window.JournalApp.getActiveAutoCalUser = function() {
  return window.JournalApp.autoCalUsers.find((item) => item.key === window.JournalApp.autoCalActiveKey) || null;
};

window.JournalApp.getSelectedAutoCalTeacherForJournal = function() {
  if (window.JournalApp.autoCalUseCustomSchedule) return null;
  const selectedKey = Array.from(window.JournalApp.autoCalSelectedKeys)[0] || '';
  if (!selectedKey) return null;
  return window.JournalApp.autoCalUsers.find((entry) => entry.key === selectedKey) || null;
};

window.JournalApp.formatAutoCalTime = function(isoValue) {
  if (!isoValue) return '-';
  const date = new Date(String(isoValue));
  if (Number.isNaN(date.getTime())) return '-';
  return date.toLocaleTimeString('ru-RU', {
    hour: '2-digit',
    minute: '2-digit',
    timeZone: window.JournalApp.AUTO_CAL_TIMEZONE
  });
};

window.JournalApp.clearAutoCalScheduleCache = function(teacherId = 0) {
  const targetId = Number(teacherId || 0);
  if (targetId > 0) {
    const prefix = `${targetId}_`;
    Array.from(window.JournalApp.autoCalScheduleDayCache.keys()).forEach((key) => {
      if (key.startsWith(prefix)) {
        window.JournalApp.autoCalScheduleDayCache.delete(key);
      }
    });
    Array.from(window.JournalApp.autoCalMonthCountsCache.keys()).forEach((key) => {
      if (key.startsWith(prefix)) {
        window.JournalApp.autoCalMonthCountsCache.delete(key);
      }
    });
    Array.from(window.JournalApp.autoCalMonthPending).forEach((pendingKey) => {
      if (String(pendingKey).startsWith(prefix)) {
        window.JournalApp.autoCalMonthPending.delete(pendingKey);
      }
    });
    Array.from(window.JournalApp.autoCalWeekStatsCache.keys()).forEach((key) => {
      if (key.startsWith(prefix)) {
        window.JournalApp.autoCalWeekStatsCache.delete(key);
      }
    });
    Array.from(window.JournalApp.autoCalWeekStatsPending).forEach((pendingKey) => {
      if (String(pendingKey).startsWith(prefix)) {
        window.JournalApp.autoCalWeekStatsPending.delete(pendingKey);
      }
    });
    return;
  }
  window.JournalApp.autoCalScheduleDayCache.clear();
  window.JournalApp.autoCalMonthCountsCache.clear();
  window.JournalApp.autoCalMonthPending.clear();
  window.JournalApp.autoCalWeekStatsCache.clear();
  window.JournalApp.autoCalWeekStatsPending.clear();
};
