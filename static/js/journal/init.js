document.addEventListener('DOMContentLoaded', () => {
  const BOOTSTRAP_RAW =
    (document.getElementById('journalBootstrapData') || { textContent: '{}' }).textContent || '{}';
  let BOOTSTRAP_DATA = {};
  try {
    BOOTSTRAP_DATA = JSON.parse(BOOTSTRAP_RAW);
  } catch (e) {
    BOOTSTRAP_DATA = {};
  }

  window.JournalApp.BOOTSTRAP_DATA = BOOTSTRAP_DATA;
  
  const csrfMeta = document.querySelector('meta[name="csrf-token"]');
  window.JournalApp.CSRF_TOKEN = csrfMeta ? csrfMeta.getAttribute('content') : '';
  
  window.JournalApp.LESSONS = Array.isArray(BOOTSTRAP_DATA.lessons) ? BOOTSTRAP_DATA.lessons : [];
  window.JournalApp.PAIR_SLOTS = Array.isArray(BOOTSTRAP_DATA.pair_slots) ? BOOTSTRAP_DATA.pair_slots : [];
  window.JournalApp.CAN_CREATE_LESSON = Boolean(BOOTSTRAP_DATA.can_create_lesson);
  window.JournalApp.ACTIVE_SEMESTER_KEY = String(BOOTSTRAP_DATA.active_semester_key || '');
  window.JournalApp.ACTIVE_SEMESTER_LABEL = String(BOOTSTRAP_DATA.active_semester_label || '');
  window.JournalApp.BOOTSTRAP_SELECTED_DATE = String(BOOTSTRAP_DATA.selected_date || '');

  window.JournalApp.today = new Date();
  window.JournalApp.todayIso = window.JournalApp.toIsoDate(window.JournalApp.today);
  const bootstrapDate = window.JournalApp.fromIsoDate(window.JournalApp.BOOTSTRAP_SELECTED_DATE);
  window.JournalApp.selectedDateIso = bootstrapDate ? window.JournalApp.toIsoDate(bootstrapDate) : window.JournalApp.todayIso;
  window.JournalApp.currentMonthDate = new Date(
    (bootstrapDate || window.JournalApp.today).getFullYear(),
    (bootstrapDate || window.JournalApp.today).getMonth(),
    1
  );

  if (Array.isArray(BOOTSTRAP_DATA.auto_cal_users)) {
    window.JournalApp.autoCalUsers = BOOTSTRAP_DATA.auto_cal_users;
  } else {
    window.JournalApp.autoCalUsers = window.JournalApp.loadAutoCalUsers();
  }

  if (Array.isArray(BOOTSTRAP_DATA.auto_cal_selected)) {
    window.JournalApp.autoCalSelectedKeys = new Set(BOOTSTRAP_DATA.auto_cal_selected);
  } else {
    window.JournalApp.autoCalSelectedKeys = window.JournalApp.loadAutoCalSelectedKeys();
  }

  if (BOOTSTRAP_DATA.auto_cal_use_custom !== undefined && BOOTSTRAP_DATA.auto_cal_use_custom !== null) {
    window.JournalApp.autoCalUseCustomSchedule = Boolean(BOOTSTRAP_DATA.auto_cal_use_custom);
  } else {
    window.JournalApp.autoCalUseCustomSchedule = window.JournalApp.loadAutoCalUseCustomSchedule();
  }

  window.JournalApp.autoCalActiveKey = window.JournalApp.autoCalUsers.length ? window.JournalApp.autoCalUsers[0].key : '';
  window.JournalApp.syncAutoCalSelectionWithUsers();

  const prevBtn = document.getElementById('prevMonthBtn');
  const nextBtn = document.getElementById('nextMonthBtn');
  const openAddBtn = document.getElementById('openAddLessonBtn');
  const openExportBtn = document.getElementById('openAttendanceExportBtn');
  const saveBtn = document.getElementById('saveLessonBtn');
  const deleteSingleBtn = document.getElementById('deleteSingleLessonBtn');
  const deleteCourseBtn = document.getElementById('deleteCourseLessonsBtn');
  const confirmDeleteCourseBtn = document.getElementById('confirmDeleteCourseBtn');
  const dateInput = document.getElementById('lessonDate');
  const groupChecks = document.getElementById('lessonGroupChecks');
  const openAutoCalBtn = document.getElementById('openAutoCalendarBtn');
  const autoCalModalEl = document.getElementById('autoCalendarModal');
  const autoCalSearchInput = document.getElementById('autoCalSearchInput');
  const autoCalSearchBtn = document.getElementById('autoCalSearchBtn');
  const autoCalUsersList = document.getElementById('autoCalUsersList');
  const autoCalUseCustomCheckbox = document.getElementById('autoCalUseCustomCheckbox');
  const autoCalCopyBtn = document.getElementById('autoCalCopyBtn');
  const autoCalClearBtn = document.getElementById('autoCalClearBtn');
  const addLessonModalEl = document.getElementById('addLessonModal');
  const exportModalEl = document.getElementById('attendanceExportModal');
  const exportForm = document.getElementById('attendanceExportForm');
  const deleteWarningModalEl = document.getElementById('deleteCourseWarningModal');

  const exportGroupsSelectAll = document.getElementById('exportGroupsSelectAll');
  const exportGroupsClearAll = document.getElementById('exportGroupsClearAll');
  const exportCoursesSelectAll = document.getElementById('exportCoursesSelectAll');
  const exportCoursesClearAll = document.getElementById('exportCoursesClearAll');
  const exportGroupsList = document.getElementById('exportGroupsList');
  const exportStudentInput = document.getElementById('exportStudentQuery');
  const exportStudentLookup = document.getElementById('exportStudentLookup');
  const exportStudentId = document.getElementById('exportStudentId');

  if (prevBtn) prevBtn.addEventListener('click', () => window.JournalApp.shiftMonth(-1));
  if (nextBtn) nextBtn.addEventListener('click', () => window.JournalApp.shiftMonth(1));
  if (openAddBtn) openAddBtn.addEventListener('click', window.JournalApp.openAddLessonModal);
  if (openAutoCalBtn) openAutoCalBtn.addEventListener('click', window.JournalApp.openAutoCalendarFromAddLesson);
  if (openExportBtn) openExportBtn.addEventListener('click', window.JournalApp.openAttendanceExportModal);
  if (saveBtn) saveBtn.addEventListener('click', window.JournalApp.saveLesson);
  if (deleteSingleBtn) deleteSingleBtn.addEventListener('click', () => window.JournalApp.deleteLessonFromEdit('single'));
  if (deleteCourseBtn) deleteCourseBtn.addEventListener('click', () => window.JournalApp.deleteLessonFromEdit('course'));
  if (confirmDeleteCourseBtn) confirmDeleteCourseBtn.addEventListener('click', window.JournalApp.confirmPendingCourseDelete);
  if (dateInput) dateInput.addEventListener('change', window.JournalApp.updateDerivedDateInfo);
  if (autoCalSearchBtn) autoCalSearchBtn.addEventListener('click', () => window.JournalApp.runAutoCalSearch());
  if (autoCalUsersList) autoCalUsersList.addEventListener('click', window.JournalApp.onAutoCalUsersClick);
  if (autoCalUsersList) autoCalUsersList.addEventListener('change', window.JournalApp.onAutoCalUsersChange);
  if (autoCalUseCustomCheckbox) autoCalUseCustomCheckbox.addEventListener('change', window.JournalApp.onAutoCalCustomModeChange);
  if (autoCalCopyBtn) autoCalCopyBtn.addEventListener('click', window.JournalApp.onAutoCalCopy);
  
  if (autoCalClearBtn) {
    autoCalClearBtn.addEventListener('click', () => {
      if (!window.JournalApp.autoCalUsers.length) return;
      if (!confirm('Очистить список преподавателей?')) return;
      window.JournalApp.autoCalUsers = [];
      window.JournalApp.autoCalSelectedKeys = new Set();
      window.JournalApp.autoCalUseCustomSchedule = true;
      window.JournalApp.autoCalActiveKey = '';
      window.JournalApp.saveAutoCalUsers();
      window.JournalApp.saveAutoCalSelectedKeys();
      window.JournalApp.saveAutoCalUseCustomSchedule();
      window.JournalApp.clearAutoCalScheduleCache();
      window.JournalApp.renderAutoCalUsers();
      window.JournalApp.setAutoCalMessage('Список очищен.', 'info');
      window.JournalApp.renderCalendar();
      window.JournalApp.renderSelectedDay();
    });
  }

  if (autoCalSearchInput) {
    autoCalSearchInput.addEventListener('input', () => {
      const text = String(autoCalSearchInput.value || '').trim();
      if (text.length < 2) {
        window.JournalApp.clearAutoCalSuggest();
        window.JournalApp.setAutoCalMessage('', 'info');
        return;
      }
      window.JournalApp.scheduleAutoCalSearch();
    });
    autoCalSearchInput.addEventListener('keydown', (event) => {
      if (event.key === 'Escape') {
        window.JournalApp.clearAutoCalSuggest();
        return;
      }
      if (event.key === 'Enter') {
        event.preventDefault();
        window.JournalApp.runAutoCalSearch();
      }
    });
  }

  if (exportForm) exportForm.addEventListener('submit', window.JournalApp.validateAttendanceExportForm);
  if (exportGroupsSelectAll) exportGroupsSelectAll.addEventListener('click', () => window.JournalApp.setChecks('.export-group-check', true));
  if (exportGroupsClearAll) exportGroupsClearAll.addEventListener('click', () => window.JournalApp.setChecks('.export-group-check', false));
  if (exportCoursesSelectAll) exportCoursesSelectAll.addEventListener('click', () => window.JournalApp.setChecks('.export-course-check', true));
  if (exportCoursesClearAll) exportCoursesClearAll.addEventListener('click', () => window.JournalApp.setChecks('.export-course-check', false));

  if (exportGroupsList) {
    exportGroupsList.addEventListener('change', () => {
      const sid = Number((exportStudentId && exportStudentId.value) || 0);
      if (sid > 0) {
        const selectedGid = Number((exportStudentInput && exportStudentInput.dataset.selectedGroupId) || 0);
        if (selectedGid > 0 && !window.JournalApp.checkedExportGroupIds().includes(selectedGid)) {
          window.JournalApp.clearExportStudentSelection({ keepText: true });
          window.JournalApp.setExportStudentHint('Фильтр по конкретному студенту снят, потому что его группа не выбрана.');
        }
      }
      if (exportStudentInput && String(exportStudentInput.value || '').trim().length >= 2) {
        window.JournalApp.scheduleExportStudentSuggest(0);
      }
    });
  }

  if (exportStudentInput) {
    exportStudentInput.addEventListener('input', () => {
      const selectedName = String(exportStudentInput.dataset.selectedName || '').trim();
      const currentValue = String(exportStudentInput.value || '').trim();
      if (selectedName && currentValue !== selectedName) {
        if (exportStudentId) exportStudentId.value = '';
        exportStudentInput.dataset.selectedId = '';
        exportStudentInput.dataset.selectedGroupId = '';
        exportStudentInput.dataset.selectedName = '';
        window.JournalApp.setExportStudentHint(window.JournalApp.EXPORT_STUDENT_HINT_DEFAULT);
      }
      window.JournalApp.scheduleExportStudentSuggest();
    });
    exportStudentInput.addEventListener('focus', () => {
      if (String(exportStudentInput.value || '').trim().length >= 2) {
        window.JournalApp.scheduleExportStudentSuggest(0);
      }
    });
    exportStudentInput.addEventListener('keydown', (event) => {
      if (event.key === 'Escape') {
        window.JournalApp.closeExportStudentSuggest();
        return;
      }
      if (event.key === 'Enter') {
        const first = document.querySelector('#exportStudentSuggest button[data-student-id]');
        if (first) {
          event.preventDefault();
          first.click();
        }
      }
    });
  }

  document.addEventListener('click', (event) => {
    const target = event.target;
    if (exportStudentLookup) {
      if (!(target instanceof Node && exportStudentLookup.contains(target))) {
        window.JournalApp.closeExportStudentSuggest();
      }
    }

    const suggestBox = document.getElementById('autoCalSuggest');
    const searchInput = document.getElementById('autoCalSearchInput');
    if (!suggestBox || !searchInput) return;
    if (!(target instanceof Node)) return;
    if (suggestBox.contains(target) || searchInput.contains(target)) return;
    window.JournalApp.clearAutoCalSuggest();
  });

  if (groupChecks) {
    groupChecks.addEventListener('change', (event) => {
      const target = event.target;
      if (target && target.classList && target.classList.contains('lesson-group-check')) {
        window.JournalApp.loadModalStudents();
      }
    });
  }

  if (addLessonModalEl) {
    addLessonModalEl.addEventListener('hidden.bs.modal', () => {
      window.JournalApp.editingLessonId = 0;
      window.JournalApp.setLessonModalMode(false);
      window.JournalApp.setModalMessage('', 'info');
    });
  }

  if (exportModalEl) {
    exportModalEl.addEventListener('hidden.bs.modal', () => {
      window.JournalApp.clearExportStudentSelection({ keepText: false });
      window.JournalApp.closeExportStudentSuggest();
      if (window.JournalApp.exportStudentSearchTimer) {
        clearTimeout(window.JournalApp.exportStudentSearchTimer);
        window.JournalApp.exportStudentSearchTimer = null;
      }
      window.JournalApp.exportStudentSearchSeq += 1;
      window.JournalApp.setExportModalMessage('', 'info');
    });
  }

  if (deleteWarningModalEl) {
    deleteWarningModalEl.addEventListener('hidden.bs.modal', () => {
      window.JournalApp.pendingCourseDeleteContext = null;
      const previewEl = document.getElementById('deleteWarningPreview');
      if (previewEl) {
        previewEl.textContent = 'Подготовка данных для предупреждения...';
      }
    });
  }

  if (autoCalModalEl) {
    autoCalModalEl.addEventListener('hidden.bs.modal', () => {
      if (window.JournalApp.autoCalOpenedFromAddLesson && window.JournalApp.autoCalImportPerformed) {
        window.JournalApp.closeAllOverlaysAfterAutoCal();
      }
      window.JournalApp.resetAutoCalendarModalState();
    });
  }

  window.JournalApp.renderCalendar();
  window.JournalApp.renderSelectedDay();
  window.addEventListener('beforeunload', window.JournalApp.closeDateStream);
});
