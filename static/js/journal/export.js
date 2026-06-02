window.JournalApp.getAttendanceExportModal = function() {
  if (!window.JournalApp.attendanceExportModalInstance) {
    window.JournalApp.attendanceExportModalInstance = new bootstrap.Modal(document.getElementById('attendanceExportModal'));
  }
  return window.JournalApp.attendanceExportModalInstance;
};

window.JournalApp.setChecks = function(selector, checked) {
  document.querySelectorAll(selector).forEach((input) => {
    input.checked = Boolean(checked);
  });
};

window.JournalApp.checkedExportGroupIds = function() {
  return Array.from(document.querySelectorAll('.export-group-check:checked'))
    .map((input) => Number(input.value || 0))
    .filter((value) => value > 0);
};

window.JournalApp.setExportStudentHint = function(text) {
  const hint = document.getElementById('exportStudentHint');
  if (!hint) return;
  hint.textContent = String(text || window.JournalApp.EXPORT_STUDENT_HINT_DEFAULT);
};

window.JournalApp.closeExportStudentSuggest = function() {
  const box = document.getElementById('exportStudentSuggest');
  if (!box) return;
  box.innerHTML = '';
  box.classList.add('d-none');
};

window.JournalApp.clearExportStudentSelection = function(options = {}) {
  const keepText = Boolean(options.keepText);
  const input = document.getElementById('exportStudentQuery');
  const hidden = document.getElementById('exportStudentId');
  if (hidden) hidden.value = '';
  if (input) {
    if (!keepText) input.value = '';
    input.dataset.selectedId = '';
    input.dataset.selectedGroupId = '';
    input.dataset.selectedName = '';
  }
  window.JournalApp.setExportStudentHint(window.JournalApp.EXPORT_STUDENT_HINT_DEFAULT);
};

window.JournalApp.bindExportStudentGroup = function(groupId) {
  const targetGroupId = Number(groupId || 0);
  if (!targetGroupId) return;
  const checks = Array.from(document.querySelectorAll('.export-group-check'));
  if (!checks.length) return;
  checks.forEach((input) => {
    input.checked = Number(input.value || 0) === targetGroupId;
  });
};

window.JournalApp.selectExportStudent = function(student) {
  const input = document.getElementById('exportStudentQuery');
  const hidden = document.getElementById('exportStudentId');
  if (!input || !hidden || !student) return;
  const sid = Number(student.id || 0);
  const gid = Number(student.group_id || 0);
  const fio = String(student.fio || '').trim();
  const groupName = String(student.group_name || '');
  if (!sid || !fio) return;

  input.value = fio;
  input.dataset.selectedId = String(sid);
  input.dataset.selectedGroupId = String(gid || 0);
  input.dataset.selectedName = fio;
  hidden.value = String(sid);
  window.JournalApp.bindExportStudentGroup(gid);
  window.JournalApp.setExportStudentHint(groupName ? `Выбран студент: ${fio} (${groupName})` : `Выбран студент: ${fio}`);
  window.JournalApp.closeExportStudentSuggest();
};

window.JournalApp.renderExportStudentSuggest = function(items) {
  const box = document.getElementById('exportStudentSuggest');
  if (!box) return;
  box.innerHTML = '';

  const list = Array.isArray(items) ? items : [];
  if (!list.length) {
    const empty = document.createElement('div');
    empty.className = 'list-group-item small text-muted';
    empty.textContent = 'Студенты не найдены';
    box.appendChild(empty);
    box.classList.remove('d-none');
    return;
  }

  list.forEach((item) => {
    const btn = document.createElement('button');
    btn.type = 'button';
    btn.className = 'list-group-item list-group-item-action';
    btn.dataset.studentId = String(item.id || 0);
    const name = document.createElement('div');
    name.className = 'fw-semibold';
    name.textContent = String(item.fio || '-');
    const meta = document.createElement('div');
    meta.className = 'student-meta';
    meta.textContent = String(item.group_name || '-');
    btn.appendChild(name);
    btn.appendChild(meta);
    btn.addEventListener('click', () => window.JournalApp.selectExportStudent(item));
    box.appendChild(btn);
  });
  box.classList.remove('d-none');
};

window.JournalApp.loadExportStudentSuggest = async function(queryText) {
  const query = String(queryText || '').trim();
  if (query.length < 2) {
    window.JournalApp.closeExportStudentSuggest();
    return;
  }
  const requestId = ++window.JournalApp.exportStudentSearchSeq;
  const params = new URLSearchParams();
  params.set('q', query);
  params.set('limit', '10');

  try {
    const response = await fetch(`/journal/export/attendance?${params.toString()}`);
    const data = await response.json();
    if (requestId !== window.JournalApp.exportStudentSearchSeq) return;
    if (!response.ok || !data.success) {
      window.JournalApp.closeExportStudentSuggest();
      return;
    }
    window.JournalApp.renderExportStudentSuggest(data.students || []);
  } catch (_err) {
    if (requestId !== window.JournalApp.exportStudentSearchSeq) return;
    window.JournalApp.closeExportStudentSuggest();
  }
};

window.JournalApp.scheduleExportStudentSuggest = function(delayMs = 220) {
  if (window.JournalApp.exportStudentSearchTimer) {
    clearTimeout(window.JournalApp.exportStudentSearchTimer);
    window.JournalApp.exportStudentSearchTimer = null;
  }
  const input = document.getElementById('exportStudentQuery');
  const value = input ? String(input.value || '').trim() : '';
  window.JournalApp.exportStudentSearchTimer = setTimeout(() => {
    window.JournalApp.loadExportStudentSuggest(value);
  }, Math.max(0, Number(delayMs || 0)));
};

window.JournalApp.openAttendanceExportModal = function() {
  const fromInput = document.getElementById('exportDateFrom');
  const toInput = document.getElementById('exportDateTo');
  if (fromInput) fromInput.value = window.JournalApp.selectedDateIso || window.JournalApp.todayIso;
  if (toInput) toInput.value = window.JournalApp.selectedDateIso || window.JournalApp.todayIso;
  window.JournalApp.clearExportStudentSelection({ keepText: false });
  window.JournalApp.closeExportStudentSuggest();
  window.JournalApp.setExportModalMessage('', 'info');
  window.JournalApp.getAttendanceExportModal().show();
};

window.JournalApp.validateAttendanceExportForm = function(event) {
  const form = document.getElementById('attendanceExportForm');
  if (!form) return true;

  const fromInput = document.getElementById('exportDateFrom');
  const toInput = document.getElementById('exportDateTo');
  const fromDate = fromInput ? window.JournalApp.fromIsoDate(fromInput.value) : null;
  const toDate = toInput ? window.JournalApp.fromIsoDate(toInput.value) : null;

  if (!fromDate || !toDate) {
    window.JournalApp.setExportModalMessage('Укажите корректные даты начала и конца периода.', 'warning');
    if (event) event.preventDefault();
    return false;
  }

  if (fromDate.getTime() > toDate.getTime()) {
    window.JournalApp.setExportModalMessage('Дата "с" не может быть позже даты "по".', 'warning');
    if (event) event.preventDefault();
    return false;
  }

  const hasStatus = document.querySelectorAll('#exportStatusList input[type="checkbox"]:checked').length > 0;
  if (!hasStatus) {
    window.JournalApp.setExportModalMessage('Выберите хотя бы один статус.', 'warning');
    if (event) event.preventDefault();
    return false;
  }

  const hasSource = document.querySelectorAll('#exportSourceList input[type="checkbox"]:checked').length > 0;
  if (!hasSource) {
    window.JournalApp.setExportModalMessage('Выберите хотя бы один источник отметки.', 'warning');
    if (event) event.preventDefault();
    return false;
  }

  window.JournalApp.setExportModalMessage('', 'info');
  return true;
};
