window.JournalApp.getAutoCalendarModal = function() {
  if (!window.JournalApp.autoCalendarModalInstance) {
    window.JournalApp.autoCalendarModalInstance = new bootstrap.Modal(document.getElementById('autoCalendarModal'), {
      backdrop: false
    });
  }
  return window.JournalApp.autoCalendarModalInstance;
};

window.JournalApp.openAutoCalendarModal = function(options = {}) {
  const fromAddLesson = Boolean(options.fromAddLesson);
  if (fromAddLesson) {
    window.JournalApp.autoCalOpenedFromAddLesson = true;
  }
  window.JournalApp.setAutoCalMessage('', 'info');
  window.JournalApp.renderAutoCalUsers();
  const addLessonEl = document.getElementById('addLessonModal');
  if (addLessonEl && addLessonEl.classList.contains('show')) {
    addLessonEl.classList.add('auto-cal-underlay');
  }
  window.JournalApp.getAutoCalendarModal().show();
  const active = window.JournalApp.getActiveAutoCalUser();
  if (active) void window.JournalApp.refreshAutoCalTeacherFromApi(active.id);
};

window.JournalApp.closeAutoCalendarModal = function() {
  window.JournalApp.getAutoCalendarModal().hide();
};

window.JournalApp.resetAutoCalendarModalState = function() {
  const addLessonEl = document.getElementById('addLessonModal');
  if (addLessonEl) addLessonEl.classList.remove('auto-cal-underlay');
  window.JournalApp.clearAutoCalSuggest();
  if (window.JournalApp.autoCalSearchTimer) {
    clearTimeout(window.JournalApp.autoCalSearchTimer);
    window.JournalApp.autoCalSearchTimer = null;
  }
  window.JournalApp.autoCalSearchSeq += 1;
  window.JournalApp.setAutoCalMessage('', 'info');
  window.JournalApp.autoCalOpenedFromAddLesson = false;
};

window.JournalApp.openAutoCalendarFromAddLesson = function() {
  window.JournalApp.openAutoCalendarModal({ fromAddLesson: true });
};

window.JournalApp.hideModalIfShown = function(modalId) {
  const modalEl = document.getElementById(modalId);
  if (!modalEl || !modalEl.classList.contains('show')) return;
  const modalInstance = bootstrap.Modal.getOrCreateInstance(modalEl);
  if (modalInstance) {
    modalInstance.hide();
  }
};

window.JournalApp.closeAllOverlaysAfterAutoCal = function() {
  window.JournalApp.hideModalIfShown('addLessonModal');
  window.JournalApp.hideModalIfShown('attendanceExportModal');
  window.JournalApp.hideModalIfShown('deleteCourseWarningModal');
};

window.JournalApp.loadAutoCalScheduleForDate = async function(teacherId, dateIso) {
  const tid = Number(teacherId || 0);
  const dateKey = String(dateIso || '').trim();
  if (tid <= 0 || !dateKey) return [];

  const cacheKey = `${tid}_${dateKey}`;
  if (window.JournalApp.autoCalScheduleDayCache.has(cacheKey)) {
    return window.JournalApp.autoCalScheduleDayCache.get(cacheKey);
  }

  const params = new URLSearchParams();
  params.set('teacher_id', String(tid));
  params.set('date', dateKey);
  const response = await fetch(`/api/journal/auto-calendar/schedule?${params.toString()}`);
  const data = await response.json();
  if (!response.ok || !data.success) {
    throw new Error((data && data.error) ? data.error : 'Не удалось загрузить расписание преподавателя');
  }

  const lessons = Array.isArray(data.lessons) ? data.lessons : [];
  window.JournalApp.autoCalScheduleDayCache.set(cacheKey, lessons);
  return lessons;
};

window.JournalApp.ensureAutoCalMonthCounts = async function(monthKey) {
  const teacher = window.JournalApp.getSelectedAutoCalTeacherForJournal();
  const safeMonthKey = String(monthKey || '').trim();
  if (!teacher || !safeMonthKey || safeMonthKey.length !== 7) return;

  const cacheKey = `${teacher.id}_${safeMonthKey}`;
  if (window.JournalApp.autoCalMonthCountsCache.has(cacheKey)) return;
  if (window.JournalApp.autoCalMonthPending.has(cacheKey)) return;
  window.JournalApp.autoCalMonthPending.add(cacheKey);

  try {
    const params = new URLSearchParams();
    params.set('teacher_id', String(teacher.id));
    params.set('month', safeMonthKey);
    const response = await fetch(`/api/journal/auto-calendar/month-counts?${params.toString()}`);
    const data = await response.json();
    if (!response.ok || !data.success) {
      return;
    }

    const countsRaw = data.counts && typeof data.counts === 'object' ? data.counts : {};
    const countsMap = new Map();
    Object.keys(countsRaw).forEach((dateKey) => {
      const countValue = Number(countsRaw[dateKey] || 0);
      if (countValue > 0) countsMap.set(dateKey, countValue);
    });
    window.JournalApp.autoCalMonthCountsCache.set(cacheKey, countsMap);
    window.JournalApp.renderCalendar();
  } catch (_error) {
  } finally {
    window.JournalApp.autoCalMonthPending.delete(cacheKey);
  }
};

window.JournalApp.importTeacherLesson = async function(teacher, lesson, dateIso, options = {}) {
  const teacherId = Number((teacher && teacher.id) || 0);
  if (!teacherId || !lesson || !dateIso) return null;
  const showProgress = options.showProgress !== false;
  const refreshDay = options.refreshDay !== false;

  if (showProgress) window.JournalApp.setDayMessage('Добавляю занятие в журнал...', 'info');
  try {
    const response = await fetch('/api/journal/auto-calendar/import-lesson', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'X-CSRFToken': window.JournalApp.CSRF_TOKEN
      },
      body: JSON.stringify({
        teacher_id: teacherId,
        date: dateIso,
        lesson: lesson
      })
    });
    const data = await response.json();
    if (!response.ok || !data.success) {
      window.JournalApp.setDayMessage((data && data.error) ? data.error : 'Не удалось импортировать пару', 'danger');
      return null;
    }

    if (data.lesson && typeof data.lesson === 'object') {
      window.JournalApp.upsertLessonInCache(data.lesson);
    }
    window.JournalApp.clearAutoCalScheduleCache(teacherId);
    window.JournalApp.renderCalendar();
    if (refreshDay) {
      await window.JournalApp.renderSelectedDay();
    }
    return data;
  } catch (_error) {
    window.JournalApp.setDayMessage('Ошибка сети при импорте пары', 'danger');
    return null;
  }
};

window.JournalApp.importTeacherLessonAndOpenAttendance = async function(teacher, lesson, dateIso) {
  const data = await window.JournalApp.importTeacherLesson(teacher, lesson, dateIso, {
    showProgress: true,
    refreshDay: false
  });
  if (!data) return;
  window.JournalApp.autoCalImportPerformed = true;

  window.JournalApp.setDayMessage('Занятие добавлено. Открываю страницу посещаемости...', 'success');
  const attendanceUrl = String(
    (data && data.attendance_url)
      || ((data && data.lesson && data.lesson.attendance_url) ? data.lesson.attendance_url : '')
  ).trim();
  if (attendanceUrl) {
    window.location.href = attendanceUrl;
    return;
  }
  if (data.lesson && data.lesson.id) {
    window.location.href = window.JournalApp.buildLessonAttendanceUrl(data.lesson, dateIso);
    return;
  }
  await window.JournalApp.renderSelectedDay();
};

window.JournalApp.importTeacherLessonAndOpenEdit = async function(teacher, lesson, dateIso) {
  if (!window.JournalApp.CAN_CREATE_LESSON) {
    window.JournalApp.setDayMessage('Редактирование недоступно для вашей роли', 'warning');
    return;
  }
  const data = await window.JournalApp.importTeacherLesson(teacher, lesson, dateIso, {
    showProgress: true,
    refreshDay: true
  });
  if (!data || !data.lesson || !data.lesson.id) {
    return;
  }
  window.JournalApp.autoCalImportPerformed = true;
  window.JournalApp.openEditLessonModal(data.lesson);
};

window.JournalApp.renderAutoCalTeacherDayInMain = async function(teacher, options = {}) {
  const listEl = options.listEl;
  const badgeEl = options.badgeEl;
  const metaEl = options.metaEl;
  const dateIso = String(options.dateIso || '');
  const requestSeq = Number(options.requestSeq || 0);
  if (!teacher || !listEl || !badgeEl || !metaEl || !dateIso) return;

  window.JournalApp.closeDateStream();
  listEl.innerHTML = '<div class="text-center text-muted py-4">Загрузка расписания преподавателя...</div>';

  try {
    const [lessons, journalLessons] = await Promise.all([
      window.JournalApp.loadAutoCalScheduleForDate(teacher.id, dateIso),
      window.JournalApp.loadLessonsForDate(dateIso).catch(() => [])
    ]);
    if (requestSeq !== window.JournalApp.renderRequestSeq) return;

    badgeEl.textContent = `Количество занятий: ${window.JournalApp.countPairsInAutoCalLessons(lessons)}`;
    metaEl.textContent = `${metaEl.textContent} | Источник: ${teacher.fullTitle || teacher.targetTitle || `ID ${teacher.id}`}`;

    listEl.innerHTML = '';
    const autoLessonsByPair = new Map();
    (Array.isArray(lessons) ? lessons : []).forEach((lesson) => {
      const pairNumber = window.JournalApp.autoCalPairNumberByStart(lesson && lesson.start);
      const pairKey = pairNumber > 0 ? pairNumber : 0;
      if (!autoLessonsByPair.has(pairKey)) autoLessonsByPair.set(pairKey, []);
      autoLessonsByPair.get(pairKey).push(lesson);
    });

    const journalLessonsByPair = new Map();
    (Array.isArray(journalLessons) ? journalLessons : []).forEach((lesson) => {
      const pairNumber = Number(lesson.pair_number || 0);
      if (!journalLessonsByPair.has(pairNumber)) journalLessonsByPair.set(pairNumber, []);
      journalLessonsByPair.get(pairNumber).push(lesson);
    });

    const pairByNumber = {};
    window.JournalApp.PAIR_SLOTS.forEach((slot) => {
      pairByNumber[Number(slot.number)] = slot;
    });

    const renderGapItem = (pairNum) => {
      const pairInfo = pairByNumber[pairNum] || {};
      const pairLabel = pairInfo.label || `${pairNum} пара`;
      const pairTime = pairInfo.time || '';
      const gap = document.createElement('div');
      gap.className = 'lesson-gap-item';

      const time = document.createElement('div');
      time.className = 'lesson-time';
      time.textContent = pairTime ? `${pairLabel} | ${pairTime}` : pairLabel;

      const text = document.createElement('div');
      text.className = 'lesson-meta';
      text.textContent = 'Пара отсутствует';

      gap.appendChild(time);
      gap.appendChild(text);
      listEl.appendChild(gap);
    };

    const renderAutoLessonItem = (lesson, pairNumber) => {
      const item = document.createElement('div');
      item.className = 'lesson-item clickable';

      const importedLesson = window.JournalApp.findImportedLessonForAutoPair(
        lesson,
        journalLessonsByPair.get(pairNumber) || []
      );
      const importedReady = Boolean(importedLesson && importedLesson.id);

      const pairInfo = pairByNumber[pairNumber] || {};
      const pairLabel = pairInfo.label || (pairNumber > 0 ? `${pairNumber} пара` : 'Пара');
      const pairTime = pairInfo.time || `${window.JournalApp.formatAutoCalTime(lesson.start)} - ${window.JournalApp.formatAutoCalTime(lesson.end)}`;

      const topRow = document.createElement('div');
      topRow.className = 'd-flex justify-content-between align-items-start gap-2';

      const time = document.createElement('div');
      time.className = 'lesson-time';
      time.textContent = `${pairLabel} | ${pairTime}`;

      const tools = document.createElement('div');
      tools.className = 'lesson-tools';

      const editBtn = document.createElement('button');
      editBtn.type = 'button';
      editBtn.className = 'lesson-action-btn';
      editBtn.title = importedReady ? 'Изменить занятие' : 'Добавить и изменить занятие';
      editBtn.innerHTML = '<i class="bi bi-pencil"></i>';
      if (!window.JournalApp.CAN_CREATE_LESSON) {
        editBtn.disabled = true;
        editBtn.title = 'Редактирование недоступно';
      }
      editBtn.addEventListener('click', (event) => {
        event.stopPropagation();
        if (!window.JournalApp.CAN_CREATE_LESSON) {
          window.JournalApp.setDayMessage('Редактирование недоступно для вашей роли', 'warning');
          return;
        }
        if (importedReady) {
          window.JournalApp.openEditLessonModal(importedLesson);
          return;
        }
        void window.JournalApp.importTeacherLessonAndOpenEdit(teacher, lesson, dateIso);
      });
      tools.appendChild(editBtn);

      if (lesson.onlineLink) {
        const linkBtn = document.createElement('button');
        linkBtn.type = 'button';
        linkBtn.className = 'lesson-action-btn';
        linkBtn.title = 'Открыть ссылку пары';
        linkBtn.innerHTML = '<i class="bi bi-box-arrow-up-right"></i>';
        linkBtn.addEventListener('click', (event) => {
          event.stopPropagation();
          window.open(String(lesson.onlineLink), '_blank', 'noopener,noreferrer');
        });
        tools.appendChild(linkBtn);
      }

      topRow.appendChild(time);
      topRow.appendChild(tools);

      const title = document.createElement('div');
      title.className = 'lesson-title';
      title.textContent = importedReady
        ? String(importedLesson.course_title || `Предмет #${importedLesson.course_id}`)
        : String(lesson.discipline || lesson.title || 'Без названия');

      const fallbackGroups = window.JournalApp.autoCalGroupNames(lesson);
      const groupsText = importedReady
        ? String(importedLesson.group_name || '-')
        : (fallbackGroups.length ? fallbackGroups.join(', ') : '-');
      const roomText = importedReady
        ? String(importedLesson.room || '-')
        : String(lesson.location || '-');

      const meta1 = document.createElement('div');
      meta1.className = 'lesson-meta';
      meta1.textContent = `Группа: ${groupsText} | Аудитория: ${roomText}`;

      const studentCount = importedReady ? Number(importedLesson.student_count || 0) : 0;
      const presentCount = importedReady ? Number(importedLesson.present_count || 0) : 0;
      const absentCount = importedReady ? Number(importedLesson.absent_count || 0) : 0;
      const excusedCount = importedReady ? Number(importedLesson.excused_count || 0) : 0;

      const meta2 = document.createElement('div');
      meta2.className = 'lesson-meta';
      meta2.textContent = `Студентов: ${studentCount} | Присутствовало: ${presentCount} | Отсутствовало: ${absentCount} | Отсутствовало (уваж.): ${excusedCount}`;

      const hint = document.createElement('div');
      hint.className = 'lesson-open-hint';
      hint.textContent = importedReady
        ? 'Нажмите, чтобы открыть страницу посещаемости'
        : 'Нажмите, чтобы добавить занятие и открыть посещаемость';

      item.appendChild(topRow);
      item.appendChild(title);
      item.appendChild(meta1);
      item.appendChild(meta2);
      item.appendChild(hint);
      item.addEventListener('click', () => {
        if (importedReady) {
          window.location.href = window.JournalApp.buildLessonAttendanceUrl(importedLesson, dateIso);
          return;
        }
        void window.JournalApp.importTeacherLessonAndOpenAttendance(teacher, lesson, dateIso);
      });
      listEl.appendChild(item);
    };

    const orderedPairs = Array.from(new Set([
      ...window.JournalApp.PAIR_SLOTS.map((slot) => Number(slot.number || 0)).filter((num) => num > 0),
      ...Array.from(autoLessonsByPair.keys()).filter((num) => Number(num) > 0)
    ])).sort((a, b) => a - b);
    if (!orderedPairs.length) {
      for (let pair = 1; pair <= 7; pair += 1) orderedPairs.push(pair);
    }

    orderedPairs.forEach((pairNumber) => {
      const pairLessons = autoLessonsByPair.get(pairNumber) || [];
      if (!pairLessons.length) {
        renderGapItem(pairNumber);
        return;
      }
      pairLessons.forEach((lesson) => renderAutoLessonItem(lesson, pairNumber));
    });

    const unboundLessons = autoLessonsByPair.get(0) || [];
    unboundLessons.forEach((lesson) => renderAutoLessonItem(lesson, 0));
  } catch (_error) {
    if (requestSeq !== window.JournalApp.renderRequestSeq) return;
    badgeEl.textContent = 'Количество занятий: 0';
    listEl.innerHTML = '<div class="text-center text-danger py-5">Не удалось загрузить расписание преподавателя</div>';
  }
};

window.JournalApp.buildAutoCalLinks = function(entry) {
  const fallbackIcs = `${window.location.origin}/api/journal/auto-calendar/ical?id=${encodeURIComponent(String(entry.id))}`;
  const raw = String((entry && entry.iCalLink) || '').trim();
  let icsHref = fallbackIcs;

  if (raw) {
    try {
      const parsed = new URL(raw, window.location.origin);
      if (!parsed.hostname || parsed.hostname === 'schedule-api') {
        icsHref = fallbackIcs;
      } else {
        icsHref = parsed.href;
      }
    } catch (_error) {
      icsHref = fallbackIcs;
    }
  }

  let webcalHref = icsHref;
  if (webcalHref.startsWith('https://')) {
    webcalHref = `webcal://${webcalHref.slice(8)}`;
  } else if (webcalHref.startsWith('http://')) {
    webcalHref = `webcal://${webcalHref.slice(7)}`;
  }
  return { icsHref, webcalHref };
};

window.JournalApp.setAutoCalActionLink = function(el, href) {
  if (!el) return;
  const safeHref = String(href || '').trim();
  if (!safeHref) {
    el.classList.add('disabled');
    el.setAttribute('aria-disabled', 'true');
    el.removeAttribute('href');
    return;
  }
  el.classList.remove('disabled');
  el.removeAttribute('aria-disabled');
  el.setAttribute('href', safeHref);
};

window.JournalApp.setAutoCalWeekStatsPlaceholder = function() {
  const pairsEl = document.getElementById('autoCalWeekPairs');
  const hoursEl = document.getElementById('autoCalWeekHours');
  const rangeEl = document.getElementById('autoCalWeekRange');
  if (pairsEl) pairsEl.textContent = '-';
  if (hoursEl) hoursEl.textContent = '-';
  if (rangeEl) rangeEl.textContent = 'Неделя: -';
};

window.JournalApp.applyAutoCalWeekStats = function(stats) {
  const pairsEl = document.getElementById('autoCalWeekPairs');
  const hoursEl = document.getElementById('autoCalWeekHours');
  const rangeEl = document.getElementById('autoCalWeekRange');
  if (!pairsEl || !hoursEl || !rangeEl) return;
  if (!stats || typeof stats !== 'object') {
    window.JournalApp.setAutoCalWeekStatsPlaceholder();
    return;
  }
  const pairs = Number(stats.week_pairs || 0);
  const hours = window.JournalApp.formatWeekHours(stats.week_hours || 0);
  const startIso = String(stats.week_start || '').trim();
  const endIso = String(stats.week_end || '').trim();
  const startLabel = startIso ? new Date(`${startIso}T00:00:00`).toLocaleDateString('ru-RU') : '-';
  const endLabel = endIso ? new Date(`${endIso}T00:00:00`).toLocaleDateString('ru-RU') : '-';
  pairsEl.textContent = String(Math.max(0, pairs));
  hoursEl.textContent = hours;
  rangeEl.textContent = `Неделя: ${startLabel} - ${endLabel}`;
};

window.JournalApp.ensureAutoCalWeekStats = async function(teacherId, referenceDateIso) {
  const tid = Number(teacherId || 0);
  if (tid <= 0) return null;
  const weekStartIso = window.JournalApp.weekStartFromDateIso(referenceDateIso);
  if (!weekStartIso) return null;
  const cacheKey = `${tid}_${weekStartIso}`;
  if (window.JournalApp.autoCalWeekStatsCache.has(cacheKey)) {
    return window.JournalApp.autoCalWeekStatsCache.get(cacheKey);
  }
  if (window.JournalApp.autoCalWeekStatsPending.has(cacheKey)) return null;
  window.JournalApp.autoCalWeekStatsPending.add(cacheKey);
  try {
    const params = new URLSearchParams();
    params.set('teacher_id', String(tid));
    params.set('date', weekStartIso);
    const response = await fetch(`/api/journal/auto-calendar/weekly-stats?${params.toString()}`);
    const data = await response.json();
    if (!response.ok || !data.success) return null;
    const payload = {
      week_start: String(data.week_start || weekStartIso),
      week_end: String(data.week_end || window.JournalApp.weekEndFromWeekStartIso(weekStartIso)),
      week_pairs: Number(data.week_pairs || 0),
      week_hours: Number(data.week_hours || 0)
    };
    window.JournalApp.autoCalWeekStatsCache.set(cacheKey, payload);
    return payload;
  } catch (_error) {
    return null;
  } finally {
    window.JournalApp.autoCalWeekStatsPending.delete(cacheKey);
  }
};

window.JournalApp.refreshAutoCalWeekStats = async function(activeTeacher) {
  if (!activeTeacher || !activeTeacher.id) {
    window.JournalApp.setAutoCalWeekStatsPlaceholder();
    return;
  }
  const targetTeacherId = Number(activeTeacher.id || 0);
  const referenceDateIso = window.JournalApp.selectedDateIso || window.JournalApp.todayIso;
  const seq = ++window.JournalApp.autoCalWeekStatsSeq;
  const stats = await window.JournalApp.ensureAutoCalWeekStats(targetTeacherId, referenceDateIso);
  if (seq !== window.JournalApp.autoCalWeekStatsSeq) return;
  const currentActive = window.JournalApp.getActiveAutoCalUser();
  if (!currentActive || Number(currentActive.id || 0) !== targetTeacherId) return;
  window.JournalApp.applyAutoCalWeekStats(stats);
};

window.JournalApp.refreshAutoCalWeekStatsIfModalOpen = function() {
  const modalEl = document.getElementById('autoCalendarModal');
  if (!modalEl || !modalEl.classList.contains('show')) return;
  const active = window.JournalApp.getActiveAutoCalUser();
  if (!active) return;
  void window.JournalApp.refreshAutoCalWeekStats(active);
};

window.JournalApp.renderActiveAutoCalUser = function() {
  const activeTitle = document.getElementById('autoCalActiveTitle');
  const activeMeta = document.getElementById('autoCalActiveMeta');
  const webcalInput = document.getElementById('autoCalWebcalInput');
  const icsBtn = document.getElementById('autoCalIcsBtn');
  const changesBtn = document.getElementById('autoCalChangesBtn');
  const photoBtn = document.getElementById('autoCalPhotoBtn');
  const copyBtn = document.getElementById('autoCalCopyBtn');
  if (!activeTitle || !activeMeta || !webcalInput || !copyBtn) return;

  const active = window.JournalApp.getActiveAutoCalUser();
  if (!active) {
    activeTitle.textContent = 'Не выбран';
    activeMeta.textContent = 'Выберите преподавателя из списка слева.';
    webcalInput.value = '';
    copyBtn.disabled = true;
    window.JournalApp.setAutoCalActionLink(icsBtn, '');
    window.JournalApp.setAutoCalActionLink(changesBtn, '');
    window.JournalApp.setAutoCalActionLink(photoBtn, '');
    window.JournalApp.setAutoCalWeekStatsPlaceholder();
    return;
  }

  const links = window.JournalApp.buildAutoCalLinks(active);
  activeTitle.textContent = active.fullTitle || active.targetTitle || `Преподаватель #${active.id}`;
  const selectedLabel = (!window.JournalApp.autoCalUseCustomSchedule && window.JournalApp.autoCalSelectedKeys.has(active.key)) ? 'используется в журнале' : 'не используется в журнале';
  activeMeta.textContent = `ID ${active.id} | Добавлен: ${new Date(active.addedAt).toLocaleString('ru-RU')} | ${selectedLabel}`;
  webcalInput.value = links.webcalHref;
  copyBtn.disabled = !String(links.webcalHref || '').trim();
  window.JournalApp.setAutoCalActionLink(icsBtn, links.icsHref);
  window.JournalApp.setAutoCalActionLink(changesBtn, active.scheduleUpdateImageLink);
  window.JournalApp.setAutoCalActionLink(photoBtn, active.scheduleImageLink);
  void window.JournalApp.refreshAutoCalWeekStats(active);
};

window.JournalApp.renderAutoCalUsers = function() {
  window.JournalApp.syncAutoCalSelectionWithUsers();
  const listEl = document.getElementById('autoCalUsersList');
  const customCheckbox = document.getElementById('autoCalUseCustomCheckbox');
  if (!listEl) return;
  listEl.innerHTML = '';
  if (customCheckbox) customCheckbox.checked = window.JournalApp.autoCalUseCustomSchedule;
  window.JournalApp.setAutoCalSelectedCount();

  if (!window.JournalApp.autoCalUsers.length) {
    listEl.innerHTML = '<div class="tiny-note p-3">Список пуст</div>';
    window.JournalApp.renderActiveAutoCalUser();
    return;
  }

  window.JournalApp.autoCalUsers.forEach((entry) => {
    const row = document.createElement('div');
    row.className = 'auto-cal-user-row';
    row.dataset.action = 'open';
    row.dataset.key = entry.key;
    if (entry.key === window.JournalApp.autoCalActiveKey) row.classList.add('is-active');
    const isSourceSelected = !window.JournalApp.autoCalUseCustomSchedule && window.JournalApp.autoCalSelectedKeys.has(entry.key);
    if (isSourceSelected) row.classList.add('is-selected-source');

    const top = document.createElement('div');
    top.className = 'd-flex justify-content-between align-items-start gap-2';

    const left = document.createElement('div');
    left.className = 'd-flex align-items-start gap-2 auto-cal-user-main';

    const check = document.createElement('input');
    check.type = 'checkbox';
    check.className = 'form-check-input auto-cal-user-check';
    check.dataset.action = 'toggle';
    check.dataset.key = entry.key;
    check.checked = !window.JournalApp.autoCalUseCustomSchedule && window.JournalApp.autoCalSelectedKeys.has(entry.key);

    const textWrap = document.createElement('div');
    const title = document.createElement('div');
    title.className = 'fw-semibold';
    title.textContent = entry.fullTitle || entry.targetTitle || `Преподаватель #${entry.id}`;
    const meta = document.createElement('div');
    meta.className = 'small text-muted';
    meta.textContent = `ID ${entry.id}`;
    textWrap.appendChild(title);
    textWrap.appendChild(meta);
    if (isSourceSelected) {
      const sourceBadge = document.createElement('div');
      sourceBadge.className = 'auto-cal-user-badge';
      sourceBadge.innerHTML = '<i class="bi bi-check2-circle"></i>Источник журнала';
      textWrap.appendChild(sourceBadge);
    }
    left.appendChild(check);
    left.appendChild(textWrap);

    const right = document.createElement('div');
    right.className = 'd-flex gap-1';

    const openBtn = document.createElement('button');
    openBtn.type = 'button';
    openBtn.className = 'btn btn-sm btn-outline-primary';
    openBtn.dataset.action = 'open';
    openBtn.dataset.key = entry.key;
    openBtn.textContent = 'Открыть';

    const removeBtn = document.createElement('button');
    removeBtn.type = 'button';
    removeBtn.className = 'btn btn-sm btn-outline-danger';
    removeBtn.dataset.action = 'remove';
    removeBtn.dataset.key = entry.key;
    removeBtn.innerHTML = '<i class="bi bi-trash3"></i>';
    removeBtn.title = 'Удалить';

    right.appendChild(openBtn);
    right.appendChild(removeBtn);
    top.appendChild(left);
    top.appendChild(right);
    row.appendChild(top);
    listEl.appendChild(row);
  });

  window.JournalApp.renderActiveAutoCalUser();
};

window.JournalApp.refreshAutoCalTeacherFromApi = async function(teacherId) {
  const targetId = Number(teacherId || 0);
  if (targetId <= 0) return;
  try {
    const response = await fetch(`/api/journal/auto-calendar/baseinfo?id=${encodeURIComponent(String(targetId))}`);
    const data = await response.json();
    if (!response.ok || !data.success || !data.teacher) return;
    const normalized = window.JournalApp.normalizeAutoCalTeacher(data.teacher);
    if (!normalized) return;

    const index = window.JournalApp.autoCalUsers.findIndex((entry) => Number(entry.id || 0) === targetId);
    if (index < 0) return;
    window.JournalApp.autoCalUsers[index] = {
      ...window.JournalApp.autoCalUsers[index],
      ...normalized,
      addedAt: window.JournalApp.autoCalUsers[index].addedAt || normalized.addedAt
    };
    window.JournalApp.saveAutoCalUsers();
    window.JournalApp.clearAutoCalScheduleCache(targetId);
    window.JournalApp.renderAutoCalUsers();
    window.JournalApp.renderCalendar();
    window.JournalApp.renderSelectedDay();
  } catch (_error) {
  }
};

window.JournalApp.addAutoCalUser = function(rawTeacher, options = {}) {
  const activate = options.activate !== false;
  const normalized = window.JournalApp.normalizeAutoCalTeacher(rawTeacher);
  if (!normalized) return;
  const existingIndex = window.JournalApp.autoCalUsers.findIndex((entry) => entry.key === normalized.key);

  if (existingIndex >= 0) {
    window.JournalApp.autoCalUsers[existingIndex] = {
      ...window.JournalApp.autoCalUsers[existingIndex],
      ...normalized,
      addedAt: window.JournalApp.autoCalUsers[existingIndex].addedAt || normalized.addedAt
    };
    if (activate) window.JournalApp.autoCalActiveKey = normalized.key;
    if (!window.JournalApp.autoCalUseCustomSchedule) {
      window.JournalApp.autoCalSelectedKeys = new Set([normalized.key]);
    }
    window.JournalApp.saveAutoCalUsers();
    window.JournalApp.saveAutoCalSelectedKeys();
    window.JournalApp.saveAutoCalUseCustomSchedule();
    window.JournalApp.renderAutoCalUsers();
    window.JournalApp.setAutoCalMessage('Пользователь обновлен.', 'info');
  } else {
    window.JournalApp.autoCalUsers.unshift(normalized);
    if (activate) window.JournalApp.autoCalActiveKey = normalized.key;
    if (!window.JournalApp.autoCalUseCustomSchedule) {
      window.JournalApp.autoCalSelectedKeys = new Set([normalized.key]);
    }
    window.JournalApp.saveAutoCalUsers();
    window.JournalApp.saveAutoCalSelectedKeys();
    window.JournalApp.saveAutoCalUseCustomSchedule();
    window.JournalApp.renderAutoCalUsers();
    window.JournalApp.setAutoCalMessage('Пользователь добавлен.', 'success');
  }

  window.JournalApp.clearAutoCalScheduleCache(normalized.id);
  window.JournalApp.renderCalendar();
  window.JournalApp.renderSelectedDay();
  void window.JournalApp.refreshAutoCalTeacherFromApi(normalized.id);
};

window.JournalApp.removeAutoCalUser = function(key) {
  const removed = window.JournalApp.autoCalUsers.find((entry) => entry.key === key) || null;
  window.JournalApp.autoCalUsers = window.JournalApp.autoCalUsers.filter((entry) => entry.key !== key);
  window.JournalApp.autoCalSelectedKeys.delete(key);
  if (!window.JournalApp.autoCalSelectedKeys.size) {
    window.JournalApp.autoCalUseCustomSchedule = true;
  }
  if (window.JournalApp.autoCalActiveKey === key) {
    window.JournalApp.autoCalActiveKey = window.JournalApp.autoCalUsers.length ? window.JournalApp.autoCalUsers[0].key : '';
  }
  window.JournalApp.saveAutoCalUsers();
  window.JournalApp.saveAutoCalSelectedKeys();
  window.JournalApp.saveAutoCalUseCustomSchedule();
  if (removed) {
    window.JournalApp.clearAutoCalScheduleCache(removed.id);
  }
  window.JournalApp.renderAutoCalUsers();
  window.JournalApp.renderCalendar();
  window.JournalApp.renderSelectedDay();
};

window.JournalApp.clearAutoCalSuggest = function() {
  const box = document.getElementById('autoCalSuggest');
  if (!box) return;
  box.innerHTML = '';
  box.classList.add('d-none');
};

window.JournalApp.renderAutoCalSuggest = function(items) {
  const box = document.getElementById('autoCalSuggest');
  if (!box) return;
  box.innerHTML = '';

  const list = Array.isArray(items) ? items : [];
  if (!list.length) {
    const empty = document.createElement('div');
    empty.className = 'list-group-item small text-muted';
    empty.textContent = 'По вашему запросу ничего не найдено';
    box.appendChild(empty);
    box.classList.remove('d-none');
    return;
  }

  list.forEach((teacher) => {
    const item = window.JournalApp.normalizeAutoCalTeacher(teacher);
    if (!item) return;

    const btn = document.createElement('button');
    btn.type = 'button';
    btn.className = 'list-group-item list-group-item-action';
    const title = document.createElement('div');
    title.className = 'fw-semibold';
    title.textContent = item.fullTitle;
    const meta = document.createElement('div');
    meta.className = 'small text-muted';
    meta.textContent = `${item.targetTitle} | ID ${item.id}`;
    btn.appendChild(title);
    btn.appendChild(meta);
    btn.addEventListener('click', () => {
      window.JournalApp.addAutoCalUser(item, { activate: true });
      const searchInput = document.getElementById('autoCalSearchInput');
      if (searchInput) searchInput.value = item.fullTitle;
      window.JournalApp.clearAutoCalSuggest();
    });
    box.appendChild(btn);
  });

  box.classList.remove('d-none');
};

window.JournalApp.runAutoCalSearch = async function() {
  const input = document.getElementById('autoCalSearchInput');
  if (!input) return;
  const query = String(input.value || '').trim();
  if (query.length < 2) {
    window.JournalApp.clearAutoCalSuggest();
    window.JournalApp.setAutoCalMessage('Введите ФИО преподавателя или часть ФИО (от 2 символов).', 'warning');
    return;
  }

  const requestId = ++window.JournalApp.autoCalSearchSeq;
  window.JournalApp.setAutoCalMessage('Поиск преподавателей...', 'info');
  try {
    const params = new URLSearchParams();
    params.set('q', query);
    params.set('limit', '15');
    const response = await fetch(`/api/journal/auto-calendar/search?${params.toString()}`);
    const data = await response.json();
    if (requestId !== window.JournalApp.autoCalSearchSeq) return;
    if (!response.ok || !data.success) {
      window.JournalApp.setAutoCalMessage((data && data.error) ? data.error : 'Не удалось выполнить поиск', 'danger');
      window.JournalApp.clearAutoCalSuggest();
      return;
    }
    window.JournalApp.renderAutoCalSuggest(data.data || []);
    window.JournalApp.setAutoCalMessage('', 'info');
  } catch (_error) {
    if (requestId !== window.JournalApp.autoCalSearchSeq) return;
    window.JournalApp.clearAutoCalSuggest();
    window.JournalApp.setAutoCalMessage('Ошибка сети при поиске преподавателя', 'danger');
  }
};

window.JournalApp.scheduleAutoCalSearch = function(delayMs = 250) {
  if (window.JournalApp.autoCalSearchTimer) {
    clearTimeout(window.JournalApp.autoCalSearchTimer);
    window.JournalApp.autoCalSearchTimer = null;
  }
  window.JournalApp.autoCalSearchTimer = setTimeout(() => {
    window.JournalApp.runAutoCalSearch();
  }, Math.max(0, Number(delayMs || 0)));
};

window.JournalApp.onAutoCalUsersClick = function(event) {
  const target = event.target;
  if (!(target instanceof Element)) return;
  const btn = target.closest('button[data-action][data-key]');
  if (!btn) {
    if (target.closest('input, label')) return;
    const row = target.closest('.auto-cal-user-row[data-action="open"][data-key]');
    if (!row) return;
    const key = String(row.dataset.key || '');
    if (!key) return;
    window.JournalApp.autoCalActiveKey = key;
    window.JournalApp.saveAutoCalUsers();
    window.JournalApp.renderAutoCalUsers();
    const active = window.JournalApp.getActiveAutoCalUser();
    if (active) void window.JournalApp.refreshAutoCalTeacherFromApi(active.id);
    return;
  }

  const action = String(btn.dataset.action || '');
  const key = String(btn.dataset.key || '');
  if (!key) return;

  if (action === 'remove') {
    window.JournalApp.removeAutoCalUser(key);
  } else if (action === 'open') {
    window.JournalApp.autoCalActiveKey = key;
    window.JournalApp.saveAutoCalUsers();
    window.JournalApp.renderAutoCalUsers();
    const active = window.JournalApp.getActiveAutoCalUser();
    if (active) void window.JournalApp.refreshAutoCalTeacherFromApi(active.id);
  }
};

window.JournalApp.onAutoCalUsersChange = function(event) {
  const target = event.target;
  if (!target || target.dataset.action !== 'toggle') return;
  const key = target.dataset.key;
  if (!key) return;
  const checked = target.checked;

  if (checked) {
    window.JournalApp.autoCalSelectedKeys = new Set([key]);
    window.JournalApp.autoCalUseCustomSchedule = false;
  } else {
    window.JournalApp.autoCalSelectedKeys.delete(key);
    if (!window.JournalApp.autoCalSelectedKeys.size) {
      window.JournalApp.autoCalUseCustomSchedule = true;
    }
  }
  window.JournalApp.saveAutoCalSelectedKeys();
  window.JournalApp.saveAutoCalUseCustomSchedule();
  window.JournalApp.renderAutoCalUsers();
  window.JournalApp.renderCalendar();
  window.JournalApp.renderSelectedDay();
};

window.JournalApp.onAutoCalCustomModeChange = function(event) {
  const target = event.target;
  window.JournalApp.autoCalUseCustomSchedule = Boolean(target.checked);
  if (window.JournalApp.autoCalUseCustomSchedule) {
    window.JournalApp.autoCalSelectedKeys.clear();
  } else {
    if (window.JournalApp.autoCalUsers.length) {
      window.JournalApp.autoCalSelectedKeys = new Set([window.JournalApp.autoCalUsers[0].key]);
    } else {
      window.JournalApp.autoCalUseCustomSchedule = true;
      target.checked = true;
    }
  }
  window.JournalApp.saveAutoCalSelectedKeys();
  window.JournalApp.saveAutoCalUseCustomSchedule();
  window.JournalApp.renderAutoCalUsers();
  window.JournalApp.renderCalendar();
  window.JournalApp.renderSelectedDay();
};

window.JournalApp.onAutoCalCopy = function() {
  const webcalInput = document.getElementById('autoCalWebcalInput');
  if (!webcalInput) return;
  const value = String(webcalInput.value || '').trim();
  if (!value) return;

  if (!navigator.clipboard || !navigator.clipboard.writeText) {
    window.JournalApp.setAutoCalMessage('Скопируйте ссылку вручную из поля.', 'warning');
    return;
  }
  navigator.clipboard.writeText(value)
    .then(() => window.JournalApp.setAutoCalMessage('Ссылка webcal скопирована.', 'success'))
    .catch(() => window.JournalApp.setAutoCalMessage('Не удалось скопировать ссылку.', 'danger'));
};

window.JournalApp.openDeleteCourseWarning = function(deleteContext) {
  window.JournalApp.pendingCourseDeleteContext = deleteContext || null;
  const lesson = (deleteContext && deleteContext.lesson) ? deleteContext.lesson : {};
  const titleEl = document.getElementById('deleteWarningCourseTitle');
  const semesterEl = document.getElementById('deleteWarningSemesterText');
  if (titleEl) {
    titleEl.textContent = lesson.course_title || 'Предмет';
  }
  if (semesterEl) {
    const semesterKey = String(lesson.semester_key || window.JournalApp.ACTIVE_SEMESTER_KEY || '');
    semesterEl.textContent = `Семестр: ${window.JournalApp.semesterLabelFromKey(semesterKey) || window.JournalApp.ACTIVE_SEMESTER_LABEL || '-'}`;
  }
  window.JournalApp.getDeleteWarningModal().show();

  const lessonId = Number((deleteContext && deleteContext.lessonId) || 0);
  if (lessonId > 0) {
    window.JournalApp.loadDeleteScopePreview(lessonId);
  }
};

window.JournalApp.loadDeleteScopePreview = async function(lessonId) {
  const previewEl = document.getElementById('deleteWarningPreview');
  if (!previewEl) return;
  previewEl.textContent = 'Подготовка данных для предупреждения...';

  try {
    const response = await fetch(`/api/journal/lessons/${lessonId}/delete-scope-preview?scope=course`);
    const data = await response.json();
    if (!response.ok || !data.success) {
      previewEl.textContent = (data && data.error) ? data.error : 'Не удалось получить данные о рисках удаления.';
      return;
    }
    window.JournalApp.renderDeleteWarningPreview(data);
  } catch (_error) {
    previewEl.textContent = 'Ошибка сети при подготовке предупреждения.';
  }
};

window.JournalApp.renderDeleteWarningPreview = function(payload) {
  const previewEl = document.getElementById('deleteWarningPreview');
  if (!previewEl) return;

  const safePayload = payload || {};
  const lessonCount = Number(safePayload.lessons_count || 0);
  const sessionsCount = Number(safePayload.sessions_count || 0);
  const marksCount = Number(safePayload.attendance_count || 0);
  const dateFrom = safePayload.date_from ? window.JournalApp.formatHumanDate(safePayload.date_from) : '-';
  const dateTo = safePayload.date_to ? window.JournalApp.formatHumanDate(safePayload.date_to) : '-';
  const groups = Array.isArray(safePayload.group_names) ? safePayload.group_names : [];
  const previewDates = Array.isArray(safePayload.session_dates_preview) ? safePayload.session_dates_preview : [];
  const hiddenDates = Number(safePayload.session_dates_hidden || 0);

  const lines = [];
  lines.push(`Занятий к удалению: ${lessonCount}`);
  lines.push(`Сессий посещаемости: ${sessionsCount}`);
  lines.push(`Отметок студентов: ${marksCount}`);
  lines.push(`Период с отметками: ${dateFrom} - ${dateTo}`);
  if (groups.length) {
    lines.push(`Группы: ${groups.join(', ')}`);
  }
  if (previewDates.length) {
    const shownDates = previewDates.slice(0, 8).map((value) => window.JournalApp.formatHumanDate(value)).join(', ');
    lines.push(`Затрагиваемые дни: ${shownDates}${hiddenDates > 0 ? ` и еще ${hiddenDates}` : ''}`);
  }
  previewEl.textContent = lines.join('\n');
};

window.JournalApp.formatHumanDate = function(isoDate) {
  const dateObj = window.JournalApp.fromIsoDate(isoDate);
  if (!dateObj) return String(isoDate || '-');
  return dateObj.toLocaleDateString('ru-RU');
};
