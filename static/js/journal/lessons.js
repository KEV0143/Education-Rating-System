window.JournalApp.getAddLessonModal = function() {
  if (!window.JournalApp.addLessonModalInstance) {
    window.JournalApp.addLessonModalInstance = new bootstrap.Modal(document.getElementById('addLessonModal'));
  }
  return window.JournalApp.addLessonModalInstance;
};

window.JournalApp.getDeleteWarningModal = function() {
  if (!window.JournalApp.deleteWarningModalInstance) {
    window.JournalApp.deleteWarningModalInstance = new bootstrap.Modal(document.getElementById('deleteCourseWarningModal'));
  }
  return window.JournalApp.deleteWarningModalInstance;
};

window.JournalApp.getSelectedGroupIds = function() {
  return Array.from(document.querySelectorAll('#lessonGroupChecks .lesson-group-check:checked'))
    .map((input) => Number(input.value || 0))
    .filter((value) => value > 0);
};

window.JournalApp.setSelectedGroupIds = function(groupIds) {
  const selected = new Set((Array.isArray(groupIds) ? groupIds : [])
    .map((value) => Number(value || 0))
    .filter((value) => value > 0));
  document.querySelectorAll('#lessonGroupChecks .lesson-group-check').forEach((input, index) => {
    const gid = Number(input.value || 0);
    input.checked = selected.size ? selected.has(gid) : index === 0;
  });
};

window.JournalApp.lessonGroupIds = function(lesson) {
  const fromArray = Array.isArray(lesson && lesson.group_ids) ? lesson.group_ids : [];
  const ids = fromArray.map((value) => Number(value || 0)).filter((value) => value > 0);
  if (ids.length) return ids;
  const single = Number((lesson && lesson.group_id) || 0);
  return single > 0 ? [single] : [];
};

window.JournalApp.setLessonModalMode = function(isEdit) {
  const title = document.getElementById('lessonModalTitle');
  const saveBtn = document.getElementById('saveLessonBtn');
  const deleteSingleBtn = document.getElementById('deleteSingleLessonBtn');
  const deleteCourseBtn = document.getElementById('deleteCourseLessonsBtn');
  if (title) {
    title.innerHTML = isEdit
      ? '<i class="bi bi-pencil-square me-2"></i>Изменить занятие'
      : '<i class="bi bi-journal-plus me-2"></i>Добавить занятие';
  }
  if (saveBtn) {
    saveBtn.innerHTML = isEdit
      ? '<i class="bi bi-check2 me-1"></i> Сохранить'
      : '<i class="bi bi-check2 me-1"></i> Добавить';
  }
  if (deleteSingleBtn) deleteSingleBtn.classList.toggle('d-none', !isEdit);
  if (deleteCourseBtn) deleteCourseBtn.classList.toggle('d-none', !isEdit);
};

window.JournalApp.updateDerivedDateInfo = function() {
  const dateInput = document.getElementById('lessonDate');
  const infoEl = document.getElementById('derivedDateInfo');
  const saveBtn = document.getElementById('saveLessonBtn');
  if (!dateInput || !infoEl || !saveBtn) return false;

  const date = window.JournalApp.fromIsoDate(dateInput.value);
  if (!date) {
    infoEl.textContent = 'Выберите корректную дату';
    infoEl.className = 'mt-2 small text-danger';
    saveBtn.disabled = true;
    return false;
  }

  const dayId = window.JournalApp.getDayIdByDate(date);
  const dayName = window.JournalApp.DAY_NAMES[dayId] || '';
  const semesterInfo = window.JournalApp.getDateAcademicInfo(date);

  if (dayId === 7) {
    infoEl.textContent = 'Воскресенье: пары не проводятся. Выберите дату с понедельника по субботу.';
    infoEl.className = 'mt-2 small text-danger';
    saveBtn.disabled = true;
    return false;
  }

  if (!semesterInfo) {
    infoEl.textContent = 'В июле и августе пары не проводятся.';
    infoEl.className = 'mt-2 small text-danger';
    saveBtn.disabled = true;
    return false;
  }

  if (!semesterInfo.isTeachingPeriod) {
    infoEl.textContent = window.JournalApp.stageDisabledMessage(semesterInfo);
    infoEl.className = 'mt-2 small text-danger';
    saveBtn.disabled = true;
    return false;
  }

  if (semesterInfo.semesterKey !== window.JournalApp.ACTIVE_SEMESTER_KEY) {
    infoEl.textContent = `Доступен только активный семестр: ${window.JournalApp.ACTIVE_SEMESTER_LABEL}`;
    infoEl.className = 'mt-2 small text-danger';
    saveBtn.disabled = true;
    return false;
  }

  infoEl.textContent = `${dayName} | Неделя ${semesterInfo.weekNumber} (${window.JournalApp.parityWithLabel(semesterInfo.weekParity)}) | ${semesterInfo.semesterLabel}`;
  infoEl.className = 'mt-2 small text-muted';
  saveBtn.disabled = false;
  return true;
};

window.JournalApp.loadModalStudents = async function() {
  const area = document.getElementById('modalStudentsArea');
  const countEl = document.getElementById('modalStudentsCount');
  if (!area || !countEl) return;

  const selectedIds = window.JournalApp.getSelectedGroupIds();

  if (!selectedIds.length) {
    area.innerHTML = '<div class="text-center text-muted mt-4 mb-4">Выберите группу или несколько групп</div>';
    countEl.textContent = '';
    return;
  }

  area.innerHTML = '<div class="text-center text-muted mt-4 mb-4">Загрузка...</div>';
  countEl.textContent = '';

  try {
    const response = await fetch(`/api/journal/groups/students?ids=${encodeURIComponent(selectedIds.join(','))}`);
    const data = await response.json();

    if (!response.ok || !data.success) {
      area.innerHTML = '<div class="text-center text-danger mt-4 mb-4">Не удалось загрузить студентов</div>';
      return;
    }

    const groupsPayload = Array.isArray(data.groups) ? data.groups : [];
    const totalStudents = Number(data.total_students || 0);
    countEl.textContent = `Групп: ${groupsPayload.length} | Студентов: ${totalStudents}`;

    if (!groupsPayload.length) {
      area.innerHTML = '<div class="text-center text-muted mt-4 mb-4">По выбранным группам нет данных</div>';
      return;
    }

    area.innerHTML = '';
    groupsPayload.forEach((entry) => {
      const group = entry.group || {};
      const students = Array.isArray(entry.students) ? entry.students : [];

      const groupBlock = document.createElement('div');
      groupBlock.className = 'mb-3';

      const title = document.createElement('div');
      title.className = 'fw-semibold small mb-1';
      title.textContent = `${group.name || 'Группа'} (${students.length})`;
      groupBlock.appendChild(title);

      if (!students.length) {
        const empty = document.createElement('div');
        empty.className = 'small text-muted';
        empty.textContent = 'В группе нет студентов';
        groupBlock.appendChild(empty);
      } else {
        const ul = document.createElement('ul');
        ul.className = 'list-group list-group-flush border rounded-2';
        students.forEach((student, index) => {
          const li = document.createElement('li');
          li.className = 'list-group-item py-2';
          li.textContent = `${index + 1}. ${student.fio}`;
          ul.appendChild(li);
        });
        groupBlock.appendChild(ul);
      }

      area.appendChild(groupBlock);
    });
  } catch (error) {
    area.innerHTML = '<div class="text-center text-danger mt-4 mb-4">Ошибка сети</div>';
  }
};

window.JournalApp.openAddLessonModal = function() {
  if (!window.JournalApp.CAN_CREATE_LESSON) return;

  window.JournalApp.editingLessonId = 0;
  window.JournalApp.setLessonModalMode(false);

  const dateInput = document.getElementById('lessonDate');
  const roomInput = document.getElementById('lessonRoom');
  const pairInput = document.getElementById('lessonPair');
  if (dateInput) dateInput.value = window.JournalApp.selectedDateIso || window.JournalApp.todayIso;
  if (roomInput) roomInput.value = '';
  if (pairInput && pairInput.options.length) pairInput.selectedIndex = 0;
  window.JournalApp.setSelectedGroupIds([]);

  window.JournalApp.setModalMessage('', 'info');
  window.JournalApp.updateDerivedDateInfo();
  window.JournalApp.loadModalStudents();
  window.JournalApp.getAddLessonModal().show();
};

window.JournalApp.openEditLessonModal = function(lesson) {
  if (!window.JournalApp.CAN_CREATE_LESSON || !lesson) return;
  window.JournalApp.editingLessonId = Number(lesson.id || 0);
  if (!window.JournalApp.editingLessonId) return;
  window.JournalApp.setLessonModalMode(true);

  const dateInput = document.getElementById('lessonDate');
  const pairInput = document.getElementById('lessonPair');
  const courseInput = document.getElementById('lessonCourse');
  const roomInput = document.getElementById('lessonRoom');

  if (dateInput) dateInput.value = window.JournalApp.selectedDateIso || window.JournalApp.todayIso;
  if (pairInput) pairInput.value = String(Number(lesson.pair_number || 0) || '');
  if (courseInput) courseInput.value = String(Number(lesson.course_id || 0) || '');
  if (roomInput) roomInput.value = String(lesson.room || '');

  window.JournalApp.setSelectedGroupIds(window.JournalApp.lessonGroupIds(lesson));

  window.JournalApp.setModalMessage('', 'info');
  window.JournalApp.updateDerivedDateInfo();
  window.JournalApp.loadModalStudents();
  window.JournalApp.getAddLessonModal().show();
};

window.JournalApp.upsertLessonInCache = function(lessonPayload) {
  if (!lessonPayload || typeof lessonPayload !== 'object') return;
  const lessonId = Number(lessonPayload.id || 0);
  if (!lessonId) return;
  const existingIndex = window.JournalApp.LESSONS.findIndex((item) => Number(item.id) === lessonId);
  if (existingIndex >= 0) {
    window.JournalApp.LESSONS[existingIndex] = lessonPayload;
  } else {
    window.JournalApp.LESSONS.push(lessonPayload);
  }
};

window.JournalApp.removeLessonsFromCacheByIds = function(ids) {
  const setIds = new Set((Array.isArray(ids) ? ids : [])
    .map((value) => Number(value || 0))
    .filter((value) => value > 0));
  if (!setIds.size) return;
  for (let i = window.JournalApp.LESSONS.length - 1; i >= 0; i -= 1) {
    if (setIds.has(Number(window.JournalApp.LESSONS[i].id || 0))) {
      window.JournalApp.LESSONS.splice(i, 1);
    }
  }
};

window.JournalApp.saveLesson = async function() {
  if (!window.JournalApp.CAN_CREATE_LESSON) return;

  const dateInput = document.getElementById('lessonDate');
  const pairInput = document.getElementById('lessonPair');
  const courseInput = document.getElementById('lessonCourse');
  const roomInput = document.getElementById('lessonRoom');
  const saveBtn = document.getElementById('saveLessonBtn');

  if (!dateInput || !pairInput || !courseInput || !roomInput || !saveBtn) return;
  if (!window.JournalApp.updateDerivedDateInfo()) return;

  const date = window.JournalApp.fromIsoDate(dateInput.value);
  if (!date) return;

  const dayId = window.JournalApp.getDayIdByDate(date);
  if (dayId === 7) {
    window.JournalApp.setModalMessage('Воскресенье недоступно для добавления пары', 'warning');
    return;
  }

  const pairNumber = Number(pairInput.value || 0);
  const courseId = Number(courseInput.value || 0);
  const groupIds = window.JournalApp.getSelectedGroupIds();
  const room = String(roomInput.value || '').trim();

  if (!pairNumber || !courseId || !groupIds.length) {
    window.JournalApp.setModalMessage('Заполните обязательные поля', 'warning');
    return;
  }
  if (!room) {
    window.JournalApp.setModalMessage('Укажите аудиторию', 'warning');
    return;
  }

  const oldText = saveBtn.innerHTML;
  saveBtn.disabled = true;
  saveBtn.innerHTML = '<span class="spinner-border spinner-border-sm me-2"></span>Сохранение...';
  window.JournalApp.setModalMessage('', 'info');

  try {
    const isEdit = window.JournalApp.editingLessonId > 0;
    const endpoint = isEdit ? `/api/journal/lessons/${window.JournalApp.editingLessonId}/update` : '/api/journal/lessons';
    const response = await fetch(endpoint, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'X-CSRFToken': window.JournalApp.CSRF_TOKEN
      },
      body: JSON.stringify({
        date: dateInput.value,
        pair_number: pairNumber,
        course_id: courseId,
        group_ids: groupIds,
        room: room
      })
    });

    const data = await response.json();
    if (!response.ok || !data.success) {
      window.JournalApp.setModalMessage(data.error || (isEdit ? 'Не удалось обновить занятие' : 'Не удалось добавить занятие'), 'danger');
      return;
    }

    const resultLessons = Array.isArray(data.lessons)
      ? data.lessons
      : ((data.lesson && typeof data.lesson === 'object') ? [data.lesson] : []);
    resultLessons.forEach((lessonItem) => window.JournalApp.upsertLessonInCache(lessonItem));

    if (isEdit && data.lesson && typeof data.lesson === 'object') {
      window.JournalApp.upsertLessonInCache(data.lesson);
    }

    window.JournalApp.editingLessonId = 0;
    window.JournalApp.selectedDateIso = window.JournalApp.toIsoDate(date);
    window.JournalApp.currentMonthDate = new Date(date.getFullYear(), date.getMonth(), 1);

    window.JournalApp.getAddLessonModal().hide();
    window.JournalApp.setDayMessage(isEdit ? 'Занятие обновлено' : `Добавлено занятий: ${resultLessons.length || 1}`, 'success');
    window.JournalApp.renderCalendar();
    window.JournalApp.renderSelectedDay();
  } catch (error) {
    window.JournalApp.setModalMessage('Ошибка сети при сохранении занятия', 'danger');
  } finally {
    saveBtn.disabled = false;
    saveBtn.innerHTML = oldText;
  }
};

window.JournalApp.executeDeleteLessonScope = async function(deleteContext) {
  const context = deleteContext || {};
  const lessonId = Number(context.lessonId || 0);
  const lesson = context.lesson || null;
  const dateValue = String(context.dateValue || '');
  const isCourseDelete = Boolean(context.isCourseDelete);
  if (!lessonId || !lesson) return;

  const buttonId = isCourseDelete ? 'deleteCourseLessonsBtn' : 'deleteSingleLessonBtn';
  const btn = document.getElementById(buttonId);
  const oldHtml = btn ? btn.innerHTML : '';
  if (btn) {
    btn.disabled = true;
    btn.innerHTML = '<span class="spinner-border spinner-border-sm"></span>';
  }

  try {
    const response = await fetch(`/api/journal/lessons/${lessonId}/delete-scope`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'X-CSRFToken': window.JournalApp.CSRF_TOKEN
      },
      body: JSON.stringify({
        date: dateValue,
        scope: isCourseDelete ? 'course' : 'single'
      })
    });
    const data = await response.json();
    if (!response.ok || !data.success) {
      window.JournalApp.setModalMessage((data && data.error) ? data.error : 'Не удалось удалить занятие', 'danger');
      return;
    }

    const deletedIds = Array.isArray(data.deleted_ids) ? data.deleted_ids : [];
    if (deletedIds.length) {
      window.JournalApp.removeLessonsFromCacheByIds(deletedIds);
    } else if (isCourseDelete) {
      const deletedCourseId = Number(data.course_id || lesson.course_id || 0);
      for (let i = window.JournalApp.LESSONS.length - 1; i >= 0; i -= 1) {
        if (Number(window.JournalApp.LESSONS[i].course_id || 0) === deletedCourseId) {
          window.JournalApp.LESSONS.splice(i, 1);
        }
      }
    } else {
      window.JournalApp.removeLessonsFromCacheByIds([lessonId]);
    }

    window.JournalApp.editingLessonId = 0;
    window.JournalApp.getAddLessonModal().hide();
    window.JournalApp.setDayMessage(String(data.message || 'Удаление выполнено'), 'info');
    window.JournalApp.renderCalendar();
    window.JournalApp.renderSelectedDay();
  } catch (_error) {
    window.JournalApp.setModalMessage('Ошибка сети при удалении занятия', 'danger');
  } finally {
    if (btn) {
      btn.disabled = false;
      btn.innerHTML = oldHtml;
    }
  }
};

window.JournalApp.confirmPendingCourseDelete = async function() {
  const context = window.JournalApp.pendingCourseDeleteContext;
  if (!context) return;
  window.JournalApp.pendingCourseDeleteContext = null;
  window.JournalApp.getDeleteWarningModal().hide();
  await window.JournalApp.executeDeleteLessonScope(context);
};

window.JournalApp.deleteLessonFromEdit = async function(scope) {
  const lessonId = Number(window.JournalApp.editingLessonId || 0);
  if (!lessonId) return;

  const lesson = window.JournalApp.LESSONS.find((item) => Number(item.id) === lessonId);
  if (!lesson) {
    window.JournalApp.setModalMessage('Занятие не найдено в списке', 'danger');
    return;
  }

  const dateInput = document.getElementById('lessonDate');
  const dateValue = (dateInput && dateInput.value) ? String(dateInput.value) : String(window.JournalApp.selectedDateIso || '');
  const isCourseDelete = String(scope || '') === 'course';
  const deleteContext = {
    lessonId,
    lesson,
    dateValue,
    isCourseDelete
  };

  if (isCourseDelete) {
    window.JournalApp.openDeleteCourseWarning(deleteContext);
    return;
  }

  const confirmText = `Удалить только это занятие "${lesson.course_title || 'предмет'}"?`;
  if (!confirm(confirmText)) return;

  await window.JournalApp.executeDeleteLessonScope(deleteContext);
};

window.JournalApp.deleteLesson = async function(lessonId) {
  const lesson = window.JournalApp.LESSONS.find((item) => Number(item.id) === Number(lessonId));
  if (!lesson) return;

  const label = `${lesson.course_title || 'предмет'} (${lesson.group_name || 'группа'})`;
  if (!confirm(`Удалить занятие "${label}"?`)) return;

  try {
    const response = await fetch(`/api/journal/lessons/${lessonId}/delete`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'X-CSRFToken': window.JournalApp.CSRF_TOKEN
      },
      body: JSON.stringify({ date: window.JournalApp.selectedDateIso })
    });
    const data = await response.json();
    if (!response.ok || !data.success) {
      window.JournalApp.setDayMessage(data.error || 'Не удалось удалить занятие', 'danger');
      return;
    }

    const idx = window.JournalApp.LESSONS.findIndex((item) => Number(item.id) === Number(lessonId));
    if (idx >= 0) window.JournalApp.LESSONS.splice(idx, 1);

    window.JournalApp.setDayMessage('Занятие удалено', 'info');
    window.JournalApp.renderCalendar();
    window.JournalApp.renderSelectedDay();
  } catch (error) {
    window.JournalApp.setDayMessage('Ошибка сети при удалении занятия', 'danger');
  }
};
