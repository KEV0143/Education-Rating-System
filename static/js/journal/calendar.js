window.JournalApp.closeDateStream = function() {
  if (window.JournalApp.dateStreamSource) {
    window.JournalApp.dateStreamSource.close();
    window.JournalApp.dateStreamSource = null;
    window.JournalApp.dateStreamIso = '';
  }
  window.JournalApp.stopDatePolling();
};

window.JournalApp.stopDatePolling = function() {
  if (window.JournalApp.datePollTimer) {
    clearInterval(window.JournalApp.datePollTimer);
    window.JournalApp.datePollTimer = null;
    window.JournalApp.datePollIso = '';
  }
};

window.JournalApp.startDatePolling = function(dateIso) {
  if (!dateIso) return;
  if (window.JournalApp.datePollTimer && window.JournalApp.datePollIso === dateIso) return;
  window.JournalApp.stopDatePolling();
  window.JournalApp.datePollIso = dateIso;
  window.JournalApp.datePollTimer = setInterval(() => {
    if (window.JournalApp.selectedDateIso !== dateIso) return;
    window.JournalApp.renderSelectedDay();
  }, 7000);
};

window.JournalApp.openDateStream = function(dateIso) {
  if (!dateIso) return;
  if (!window.EventSource) {
    window.JournalApp.startDatePolling(dateIso);
    return;
  }
  if (window.JournalApp.dateStreamSource && window.JournalApp.dateStreamIso === dateIso) return;

  window.JournalApp.closeDateStream();
  window.JournalApp.dateStreamIso = dateIso;
  window.JournalApp.dateStreamSource = new EventSource(`/stream/journal/date/${encodeURIComponent(dateIso)}`);
  window.JournalApp.dateStreamSource.addEventListener('lessons', () => {
    if (window.JournalApp.selectedDateIso !== dateIso) return;
    window.JournalApp.renderCalendar();
    window.JournalApp.renderSelectedDay();
  });
  window.JournalApp.dateStreamSource.onerror = () => {
    if (window.JournalApp.dateStreamSource && window.JournalApp.dateStreamSource.readyState === 2) {
      window.JournalApp.startDatePolling(dateIso);
    }
  };
};

window.JournalApp.weekStartFromDateIso = function(dateIso) {
  const base = window.JournalApp.fromIsoDate(dateIso) || window.JournalApp.fromIsoDate(window.JournalApp.selectedDateIso) || new Date();
  const weekDay = (base.getDay() + 6) % 7;
  const start = new Date(base.getFullYear(), base.getMonth(), base.getDate() - weekDay);
  return window.JournalApp.toIsoDate(start);
};

window.JournalApp.weekEndFromWeekStartIso = function(weekStartIso) {
  const start = window.JournalApp.fromIsoDate(weekStartIso);
  if (!start) return '';
  const end = new Date(start.getFullYear(), start.getMonth(), start.getDate() + 6);
  return window.JournalApp.toIsoDate(end);
};

window.JournalApp.formatWeekHours = function(hoursValue) {
  const value = Number(hoursValue || 0);
  if (!Number.isFinite(value) || value <= 0) return '0';
  const rounded = Math.round(value * 10) / 10;
  if (Math.abs(rounded - Math.round(rounded)) < 0.01) {
    return String(Math.round(rounded));
  }
  return rounded.toFixed(1).replace('.', ',');
};

window.JournalApp.loadLessonsForDate = async function(dateIso) {
  const response = await fetch(`/api/journal/date/${encodeURIComponent(dateIso)}/lessons`);
  const data = await response.json();
  if (!response.ok || !data || !data.success) {
    throw new Error((data && data.error) ? data.error : 'Не удалось загрузить занятия');
  }
  return Array.isArray(data.lessons) ? data.lessons : [];
};

window.JournalApp.setCurrentMonthLabel = function() {
  const label = window.JournalApp.currentMonthDate.toLocaleDateString('ru-RU', { month: 'long', year: 'numeric' });
  const monthLabel = document.getElementById('monthLabel');
  if (monthLabel) {
    monthLabel.textContent = label.charAt(0).toUpperCase() + label.slice(1);
  }
};

window.JournalApp.renderCalendar = function() {
  const grid = document.getElementById('calendarGrid');
  if (!grid) return;

  window.JournalApp.setCurrentMonthLabel();
  grid.innerHTML = '';

  const year = window.JournalApp.currentMonthDate.getFullYear();
  const month = window.JournalApp.currentMonthDate.getMonth();
  const firstDay = new Date(year, month, 1);
  const firstWeekdayIndex = (firstDay.getDay() + 6) % 7;
  const daysInMonth = new Date(year, month + 1, 0).getDate();
  const selectedTeacher = window.JournalApp.getSelectedAutoCalTeacherForJournal();
  if (selectedTeacher) {
    const monthKey = `${year}-${window.JournalApp.pad2(month + 1)}`;
    void window.JournalApp.ensureAutoCalMonthCounts(monthKey);
  }

  for (let i = 0; i < firstWeekdayIndex; i += 1) {
    const empty = document.createElement('button');
    empty.type = 'button';
    empty.className = 'calendar-day empty';
    empty.disabled = true;
    grid.appendChild(empty);
  }

  for (let day = 1; day <= daysInMonth; day += 1) {
    const currentDate = new Date(year, month, day);
    const dateIso = window.JournalApp.toIsoDate(currentDate);
    const pairsCount = window.JournalApp.getPairsCountForDate(dateIso);
    const isToday = dateIso === window.JournalApp.todayIso;
    const isSelected = dateIso === window.JournalApp.selectedDateIso;

    const dayBtn = document.createElement('button');
    dayBtn.type = 'button';
    dayBtn.className = 'calendar-day';
    if (isToday) dayBtn.classList.add('today');
    if (isSelected) dayBtn.classList.add('selected');

    const num = document.createElement('div');
    num.className = 'calendar-day-number';
    num.textContent = String(day);
    dayBtn.appendChild(num);

    if (pairsCount > 0) {
      const badge = document.createElement('div');
      badge.className = 'pairs-badge';
      badge.textContent = String(pairsCount);
      dayBtn.appendChild(badge);
    }

    dayBtn.addEventListener('click', () => {
      window.JournalApp.selectedDateIso = dateIso;
      window.JournalApp.setDayMessage('', 'info');
      window.JournalApp.renderCalendar();
      window.JournalApp.renderSelectedDay();
      window.JournalApp.refreshAutoCalWeekStatsIfModalOpen();
    });

    grid.appendChild(dayBtn);
  }

  while (grid.children.length % 7 !== 0) {
    const empty = document.createElement('button');
    empty.type = 'button';
    empty.className = 'calendar-day empty';
    empty.disabled = true;
    grid.appendChild(empty);
  }
};

window.JournalApp.renderSelectedDay = async function() {
  const titleEl = document.getElementById('selectedDateTitle');
  const metaEl = document.getElementById('selectedDateMeta');
  const badgeEl = document.getElementById('selectedDatePairsBadge');
  const listEl = document.getElementById('dayLessonsList');
  if (!titleEl || !metaEl || !badgeEl || !listEl) return;
  const requestSeq = ++window.JournalApp.renderRequestSeq;

  const date = window.JournalApp.fromIsoDate(window.JournalApp.selectedDateIso);
  if (!date) {
    window.JournalApp.closeDateStream();
    titleEl.textContent = '-';
    metaEl.textContent = '';
    badgeEl.textContent = 'Количество занятий: 0';
    listEl.innerHTML = '<div class="text-center text-muted py-5">Выберите дату</div>';
    return;
  }

  const dayId = window.JournalApp.getDayIdByDate(date);
  const dayName = window.JournalApp.DAY_NAMES[dayId] || '';
  const semesterInfo = window.JournalApp.getDateAcademicInfo(date);
  const dateTitle = window.JournalApp.formatRuDate(window.JournalApp.selectedDateIso);

  titleEl.textContent = dateTitle.charAt(0).toUpperCase() + dateTitle.slice(1);
  if (!semesterInfo) {
    metaEl.textContent = `${dayName} | Вне учебного периода`;
  } else if (semesterInfo.isTeachingPeriod) {
    metaEl.textContent = `${dayName} | Неделя ${semesterInfo.weekNumber} (${window.JournalApp.parityWithLabel(semesterInfo.weekParity)})`;
  } else {
    metaEl.textContent = `${dayName} | ${semesterInfo.stageLabel}`;
  }

  const selectedTeacher = window.JournalApp.getSelectedAutoCalTeacherForJournal();
  if (selectedTeacher) {
    await window.JournalApp.renderAutoCalTeacherDayInMain(selectedTeacher, {
      listEl,
      badgeEl,
      metaEl,
      dateIso: window.JournalApp.selectedDateIso,
      requestSeq
    });
    return;
  }

  if (dayId === 7) {
    window.JournalApp.closeDateStream();
    badgeEl.textContent = 'Количество занятий: 0';
    listEl.innerHTML = '<div class="text-center text-muted py-5">В воскресенье пары не проводятся</div>';
    return;
  }

  if (!semesterInfo) {
    window.JournalApp.closeDateStream();
    badgeEl.textContent = 'Количество занятий: 0';
    listEl.innerHTML = '<div class="text-center text-muted py-5">В июле и августе пары не проводятся</div>';
    return;
  }

  if (semesterInfo.semesterKey !== window.JournalApp.ACTIVE_SEMESTER_KEY) {
    window.JournalApp.closeDateStream();
    badgeEl.textContent = 'Количество занятий: 0';
    listEl.innerHTML = `<div class="text-center text-muted py-5">Дата вне активного семестра (${window.JournalApp.ACTIVE_SEMESTER_LABEL})</div>`;
    return;
  }

  if (!semesterInfo.isTeachingPeriod) {
    window.JournalApp.closeDateStream();
    badgeEl.textContent = 'Количество занятий: 0';
    listEl.innerHTML = `<div class="text-center text-muted py-5">${window.JournalApp.stageDisabledMessage(semesterInfo)}</div>`;
    return;
  }

  window.JournalApp.openDateStream(window.JournalApp.selectedDateIso);
  listEl.innerHTML = '<div class="text-center text-muted py-4">Загрузка занятий...</div>';

  try {
    const lessons = await window.JournalApp.loadLessonsForDate(window.JournalApp.selectedDateIso);
    if (requestSeq !== window.JournalApp.renderRequestSeq) return;

    const pairCount = new Set(lessons.map((lesson) => Number(lesson.pair_number))).size;
    badgeEl.textContent = `Количество занятий: ${pairCount}`;

    listEl.innerHTML = '';
    const lessonsByPair = new Map();
    lessons.forEach((lesson) => {
      const pairNum = Number(lesson.pair_number || 0);
      if (!lessonsByPair.has(pairNum)) lessonsByPair.set(pairNum, []);
      lessonsByPair.get(pairNum).push(lesson);
    });

    const pairByNumber = {};
    window.JournalApp.PAIR_SLOTS.forEach((slot) => {
      pairByNumber[Number(slot.number)] = slot;
    });

    const orderedPairs = Array.from(new Set([
      ...window.JournalApp.PAIR_SLOTS.map((slot) => Number(slot.number || 0)).filter((num) => num > 0),
      ...Array.from(lessonsByPair.keys()).filter((num) => Number(num) > 0)
    ])).sort((a, b) => a - b);
    if (!orderedPairs.length) {
      for (let pair = 1; pair <= 7; pair += 1) orderedPairs.push(pair);
    }

    const renderLessonItem = (lesson) => {
      const pairNum = Number(lesson.pair_number || 0);
      const pairInfo = pairByNumber[pairNum] || {};
      const pairLabel = pairInfo.label || `${pairNum} пара`;
      const pairTime = pairInfo.time || '';

      const item = document.createElement('div');
      item.className = 'lesson-item clickable';

      const topRow = document.createElement('div');
      topRow.className = 'd-flex justify-content-between align-items-start gap-2';

      const time = document.createElement('div');
      time.className = 'lesson-time';
      time.textContent = pairTime ? `${pairLabel} | ${pairTime}` : pairLabel;

      const tools = document.createElement('div');
      tools.className = 'lesson-tools';

      const editBtn = document.createElement('button');
      editBtn.type = 'button';
      editBtn.className = 'lesson-action-btn';
      editBtn.title = 'Изменить занятие';
      editBtn.innerHTML = '<i class="bi bi-pencil"></i>';
      editBtn.addEventListener('click', (event) => {
        event.stopPropagation();
        window.JournalApp.openEditLessonModal(lesson);
      });
      tools.appendChild(editBtn);

      topRow.appendChild(time);
      topRow.appendChild(tools);

      const title = document.createElement('div');
      title.className = 'lesson-title';
      title.textContent = lesson.course_title || `Предмет #${lesson.course_id}`;

      const meta1 = document.createElement('div');
      meta1.className = 'lesson-meta';
      meta1.textContent = `Группа: ${lesson.group_name || `Группа #${lesson.group_id}`} | Аудитория: ${lesson.room || '-'}`;

      const meta2 = document.createElement('div');
      meta2.className = 'lesson-meta';
      meta2.textContent = `Студентов: ${Number(lesson.student_count || 0)} | Присутствовало: ${Number(lesson.present_count || 0)} | Отсутствовало: ${Number(lesson.absent_count || 0)} | Отсутствовало (уваж.): ${Number(lesson.excused_count || 0)}`;

      const hint = document.createElement('div');
      hint.className = 'lesson-open-hint';
      hint.textContent = 'Нажмите, чтобы открыть страницу посещаемости';

      item.appendChild(topRow);
      item.appendChild(title);
      item.appendChild(meta1);
      item.appendChild(meta2);
      item.appendChild(hint);
      item.addEventListener('click', () => {
        window.location.href = window.JournalApp.buildLessonAttendanceUrl(lesson, window.JournalApp.selectedDateIso);
      });
      listEl.appendChild(item);
    };

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

    for (const pair of orderedPairs) {
      const pairLessons = lessonsByPair.get(pair) || [];
      if (!pairLessons.length) {
        renderGapItem(pair);
        continue;
      }
      pairLessons.forEach((lesson) => renderLessonItem(lesson));
    }
  } catch (error) {
    if (requestSeq !== window.JournalApp.renderRequestSeq) return;
    badgeEl.textContent = 'Количество занятий: 0';
    listEl.innerHTML = '<div class="text-center text-danger py-5">Не удалось загрузить занятия</div>';
  }
};

window.JournalApp.shiftMonth = function(offset) {
  window.JournalApp.currentMonthDate = new Date(
    window.JournalApp.currentMonthDate.getFullYear(),
    window.JournalApp.currentMonthDate.getMonth() + offset,
    1
  );

  const isCurrentMonthNow =
    window.JournalApp.currentMonthDate.getFullYear() === window.JournalApp.today.getFullYear() &&
    window.JournalApp.currentMonthDate.getMonth() === window.JournalApp.today.getMonth();

  window.JournalApp.selectedDateIso = isCurrentMonthNow
    ? window.JournalApp.todayIso
    : window.JournalApp.toIsoDate(new Date(window.JournalApp.currentMonthDate.getFullYear(), window.JournalApp.currentMonthDate.getMonth(), 1));

  window.JournalApp.setDayMessage('', 'info');
  window.JournalApp.renderCalendar();
  window.JournalApp.renderSelectedDay();
  window.JournalApp.refreshAutoCalWeekStatsIfModalOpen();
};
