// Frontend behavior:
// 1) bulk enable/disable on the dashboard
// 2) periodic polling of status/logs on the run page
// 3) live log feed for the selected run on the logs page
(() => {
  const panel = document.getElementById("run-panel");
  const livePanel = document.getElementById("live-logs-panel");
  const resticSnapshotsPanel = document.getElementById("restic-snapshots-panel");
  const resticQueuePanel = document.getElementById("restic-queue-panel");
  const resticQueueBody = document.getElementById("restic-queue-tbody");
  const resticQueueFilterInput = document.getElementById("restic-queue-filter-input");
  const dashboardRunsBody = document.getElementById("dashboard-runs-tbody");
  const logsHistoryBody = document.getElementById("logs-history-tbody");
  const bulkForm = document.getElementById("bulk-target-form");
  const activateBtn = document.getElementById("bulk-activate");
  const deactivateBtn = document.getElementById("bulk-deactivate");
  const selectAll = document.getElementById("target-select-all");
  const rowChecks = Array.from(document.querySelectorAll(".target-select"));

  if (bulkForm && activateBtn && deactivateBtn) {
    // The dashboard bulk action form is assembled from checked rows.
    const collectSelectedIds = () =>
      rowChecks
        .filter((x) => x.checked)
        .map((x) => x.value);

    const syncSelectAll = () => {
      if (!selectAll || rowChecks.length === 0) return;
      const allChecked = rowChecks.every((x) => x.checked);
      const someChecked = rowChecks.some((x) => x.checked);
      selectAll.checked = allChecked;
      selectAll.indeterminate = !allChecked && someChecked;
    };

    const submitBulk = (action) => {
      const ids = collectSelectedIds();
      if (ids.length === 0) {
        alert("Выберите хотя бы одну задачу");
        return;
      }

      bulkForm.innerHTML = "";
      const actionInput = document.createElement("input");
      actionInput.type = "hidden";
      actionInput.name = "action";
      actionInput.value = action;
      bulkForm.appendChild(actionInput);

      for (const id of ids) {
        const idInput = document.createElement("input");
        idInput.type = "hidden";
        idInput.name = "target_ids";
        idInput.value = id;
        bulkForm.appendChild(idInput);
      }
      bulkForm.submit();
    };

    if (selectAll) {
      selectAll.addEventListener("change", () => {
        for (const row of rowChecks) row.checked = selectAll.checked;
      });
    }
    for (const row of rowChecks) row.addEventListener("change", syncSelectAll);

    activateBtn.addEventListener("click", () => submitBulk("activate"));
    deactivateBtn.addEventListener("click", () => submitBulk("deactivate"));
    syncSelectAll();
  }

  if (dashboardRunsBody) {
    // Auto-refresh for the "Recent runs" block on the dashboard.
    const escapeHtml = (value) =>
      String(value ?? "")
        .replaceAll("&", "&amp;")
        .replaceAll("<", "&lt;")
        .replaceAll(">", "&gt;")
        .replaceAll("\"", "&quot;")
        .replaceAll("'", "&#39;");

    const formatRunTime = (iso) => {
      if (!iso) return "-";
      const date = new Date(iso);
      if (Number.isNaN(date.getTime())) return iso;
      return date.toLocaleString("ru-RU");
    };

    let dashboardRunsInFlight = false;
    const dashboardRunsLimit = Number(dashboardRunsBody.dataset.limit || "20");

    const renderDashboardRuns = (rows) => {
      if (!rows || rows.length === 0) {
        dashboardRunsBody.innerHTML = '<tr><td colspan="6">Запусков пока нет.</td></tr>';
        return;
      }
      dashboardRunsBody.innerHTML = rows
        .map((r) => {
          const runId = Number(r.id || 0);
          const status = escapeHtml(r.status || "-");
          const targetName = escapeHtml(r.target_name || `ID ${r.target_id}`);
          const progress = Number(r.progress || 0);
          const step = escapeHtml(r.step || "-");
          const displayTime = escapeHtml(formatRunTime(r.display_time || r.finished_at || r.started_at));
          return (
            `<tr>` +
            `<td><a href="/runs/${runId}">${runId}</a></td>` +
            `<td>${targetName}</td>` +
            `<td><span class="pill ${status}">${status}</span></td>` +
            `<td>${progress}%</td>` +
            `<td>${step}</td>` +
            `<td>${displayTime}</td>` +
            `</tr>`
          );
        })
        .join("");
    };

    const pollDashboardRuns = async () => {
      if (dashboardRunsInFlight) return;
      dashboardRunsInFlight = true;
      try {
        const resp = await fetch(`/api/dashboard/runs?limit=${dashboardRunsLimit}`);
        if (!resp.ok) return;
        const data = await resp.json();
        renderDashboardRuns(data.runs || []);
      } finally {
        dashboardRunsInFlight = false;
      }
    };

    setInterval(pollDashboardRuns, 5000);
    pollDashboardRuns();
  }

  if (logsHistoryBody) {
    // Auto-refresh for the "Run history" table on the logs page.
    const escapeHtml = (value) =>
      String(value ?? "")
        .replaceAll("&", "&amp;")
        .replaceAll("<", "&lt;")
        .replaceAll(">", "&gt;")
        .replaceAll("\"", "&quot;")
        .replaceAll("'", "&#39;");

    const formatRunTime = (iso) => {
      if (!iso) return "-";
      const date = new Date(iso);
      if (Number.isNaN(date.getTime())) return iso;
      return date.toLocaleString("ru-RU");
    };

    let logsHistoryInFlight = false;
    const logsHistoryLimit = Number(logsHistoryBody.dataset.limit || "100");

    const renderLogsHistory = (rows) => {
      if (!rows || rows.length === 0) {
        logsHistoryBody.innerHTML = '<tr><td colspan="6">Нет данных.</td></tr>';
        return;
      }
      logsHistoryBody.innerHTML = rows
        .map((r) => {
          const runId = Number(r.id || 0);
          const status = escapeHtml(r.status || "-");
          const targetName = escapeHtml(r.target_name || `ID ${r.target_id}`);
          const startedAt = escapeHtml(formatRunTime(r.started_at));
          const finishedAt = escapeHtml(formatRunTime(r.finished_at));
          const error = escapeHtml(r.error_message || "-");
          return (
            `<tr>` +
            `<td><a href="/runs/${runId}">${runId}</a></td>` +
            `<td>${targetName}</td>` +
            `<td><span class="pill ${status}">${status}</span></td>` +
            `<td>${startedAt}</td>` +
            `<td>${finishedAt}</td>` +
            `<td>${error}</td>` +
            `</tr>`
          );
        })
        .join("");
    };

    const pollLogsHistory = async () => {
      if (logsHistoryInFlight) return;
      logsHistoryInFlight = true;
      try {
        const resp = await fetch(`/api/logs/history?limit=${logsHistoryLimit}`);
        if (!resp.ok) return;
        const data = await resp.json();
        renderLogsHistory(data.runs || []);
      } finally {
        logsHistoryInFlight = false;
      }
    };

    setInterval(pollLogsHistory, 5000);
    pollLogsHistory();
  }

  if (resticQueuePanel && resticQueueBody) {
    // Auto-refresh for the "Restic send queue" table.
    const escapeHtml = (value) =>
      String(value ?? "")
        .replaceAll("&", "&amp;")
        .replaceAll("<", "&lt;")
        .replaceAll(">", "&gt;")
        .replaceAll("\"", "&quot;")
        .replaceAll("'", "&#39;");

    let resticQueueInFlight = false;
    const resticQueueLimit = Number(resticQueueBody.dataset.limit || "200");

    const renderResticQueue = (rows) => {
      if (!rows || rows.length === 0) {
        resticQueueBody.innerHTML = '<tr><td colspan="8">Нет завершенных запусков.</td></tr>';
        return;
      }
      resticQueueBody.innerHTML = rows
        .map((r) => {
          const runId = Number(r.id || 0);
          const targetName = escapeHtml(r.target_name || `ID ${r.target_id}`);
          const statusRaw = String(r.status || "-");
          const status = escapeHtml(statusRaw);
          const resticStatus = escapeHtml(r.restic_status || "-");
          const snapshotId = escapeHtml(r.restic_snapshot_id || "-");
          const repository = escapeHtml(r.repository || "-");
          const archive = escapeHtml(r.archive_or_dir || "-");
          const hasPayload = Boolean(r.has_payload);
          const actionCell = hasPayload && statusRaw === "success"
            ? `<form method="post" action="/restic/runs/${runId}/send"><button type="submit">Отправить</button></form>`
            : "-";
          return (
            `<tr>` +
            `<td><a href="/runs/${runId}">${runId}</a></td>` +
            `<td>${targetName}</td>` +
            `<td><span class="pill ${status}">${status}</span></td>` +
            `<td>${resticStatus}</td>` +
            `<td>${snapshotId}</td>` +
            `<td>${repository}</td>` +
            `<td>${archive}</td>` +
            `<td class="actions">${actionCell}</td>` +
            `</tr>`
          );
        })
        .join("");
    };

    const pollResticQueue = async () => {
      if (resticQueueInFlight) return;
      resticQueueInFlight = true;
      try {
        const queueFilter = (resticQueueFilterInput?.value || "").trim();
        const params = new URLSearchParams();
        params.set("limit", String(resticQueueLimit));
        if (queueFilter) params.set("queue_filter", queueFilter);
        const resp = await fetch(`/api/restic/queue?${params.toString()}`);
        if (!resp.ok) return;
        const data = await resp.json();
        renderResticQueue(data.runs || []);
      } finally {
        resticQueueInFlight = false;
      }
    };

    setInterval(pollResticQueue, 5000);
    pollResticQueue();
  }

  if (!panel && !livePanel && !resticSnapshotsPanel && !resticQueuePanel && !dashboardRunsBody && !logsHistoryBody) return;

  if (panel) {
    // Polling loop on the run page (status + incremental logs).
    const runId = panel.dataset.runId;
    const logsEl = document.getElementById("run-logs");
    const statusEl = document.getElementById("run-status");
    const stepEl = document.getElementById("run-step");
    const archiveEl = document.getElementById("run-archive");
    const progressEl = document.getElementById("run-progress");
    const errorEl = document.getElementById("run-error");

    let timer = null;

    const poll = async () => {
      const lastId = Number(logsEl.dataset.lastId || "0");

      const [statusResp, logsResp] = await Promise.all([
        fetch(`/api/runs/${runId}/status`),
        fetch(`/api/runs/${runId}/logs?after_id=${lastId}`),
      ]);

      if (!statusResp.ok || !logsResp.ok) return;

      const statusData = await statusResp.json();
      const logsData = await logsResp.json();

      statusEl.textContent = statusData.status;
      statusEl.className = `pill ${statusData.status}`;
      stepEl.textContent = statusData.step;
      archiveEl.textContent = statusData.archive_file || "-";
      progressEl.textContent = `${statusData.progress}%`;
      progressEl.style.width = `${statusData.progress}%`;
      errorEl.textContent = statusData.error_message || "";

      for (const row of logsData.logs) {
        logsEl.textContent += `[${row.created_at}] ${row.level.toUpperCase()} ${row.message}\n`;
        logsEl.dataset.lastId = String(row.id);
      }

      logsEl.scrollTop = logsEl.scrollHeight;

      if (["success", "failed", "canceled"].includes(statusData.status)) {
        clearInterval(timer);
      }
    };

    timer = setInterval(poll, 2000);
    poll();
  }

  if (livePanel) {
    // Live polling loop on the logs page for an arbitrary run ID.
    const logsEl = document.getElementById("live-run-logs");
    const runInput = document.getElementById("live-run-id");
    let selectedRunId = Number(livePanel.dataset.runId || "0");
    let liveTimer = null;
    let inFlight = false;

    const pollLive = async () => {
      if (!selectedRunId || inFlight) return;
      inFlight = true;
      try {
        const statusResp = await fetch(`/api/runs/${selectedRunId}/status`);
        if (!statusResp.ok) return;
        const statusData = await statusResp.json();

        const lastId = Number(logsEl.dataset.lastId || "0");
        const logsResp = await fetch(`/api/runs/${selectedRunId}/logs?after_id=${lastId}`);
        if (!logsResp.ok) return;
        const data = await logsResp.json();

        for (const row of data.logs) {
          logsEl.textContent += `[${row.created_at}] ${row.level.toUpperCase()} ${row.message}\n`;
          logsEl.dataset.lastId = String(row.id);
        }
        logsEl.scrollTop = logsEl.scrollHeight;

        if (["success", "failed", "canceled"].includes(statusData.status) && liveTimer) {
          clearInterval(liveTimer);
          liveTimer = null;
        }
      } finally {
        inFlight = false;
      }
    };

    const startLivePolling = () => {
      if (liveTimer) {
        clearInterval(liveTimer);
      }
      liveTimer = setInterval(pollLive, 2000);
      pollLive();
    };

    runInput.addEventListener("change", () => {
      selectedRunId = Number(runInput.value || "0");
      logsEl.textContent = "";
      logsEl.dataset.lastId = "0";
      startLivePolling();
    });

    startLivePolling();
  }

  if (resticSnapshotsPanel) {
    // Auto-refresh the Restic snapshots table through the API without page reload.
    const tbody = document.getElementById("restic-snapshots-tbody");
    const infoEl = document.getElementById("restic-snapshots-info");
    const errorEl = document.getElementById("restic-snapshots-error");
    const filterInput = document.getElementById("restic-snapshots-filter-input");
    const targetInput = document.getElementById("restic-snapshots-target-id");
    let resticTimer = null;
    let inFlight = false;

    const escapeHtml = (value) =>
      String(value ?? "")
        .replaceAll("&", "&amp;")
        .replaceAll("<", "&lt;")
        .replaceAll(">", "&gt;")
        .replaceAll("\"", "&quot;")
        .replaceAll("'", "&#39;");

    const renderSnapshots = (rows) => {
      if (!tbody) return;
      if (!rows || rows.length === 0) {
        tbody.innerHTML = '<tr><td colspan="4">Снапшоты не найдены.</td></tr>';
        return;
      }
      const html = rows
        .map((s) => {
          const id = escapeHtml(s.short_id || s.id || "-");
          const time = escapeHtml(s.time || "-");
          const paths = escapeHtml((s.paths || []).join(", "));
          const tags = escapeHtml((s.tags || []).join(", "));
          return `<tr><td>${id}</td><td>${time}</td><td>${paths}</td><td>${tags}</td></tr>`;
        })
        .join("");
      tbody.innerHTML = html;
    };

    const pollSnapshots = async () => {
      if (inFlight) return;
      inFlight = true;
      try {
        const params = new URLSearchParams();
        const targetId = (targetInput?.value || "").trim();
        const snapshotFilter = (filterInput?.value || "").trim();
        if (targetId) params.set("target_id", targetId);
        if (snapshotFilter) params.set("snapshot_filter", snapshotFilter);
        const resp = await fetch(`/api/restic/snapshots?${params.toString()}`);
        if (!resp.ok) return;
        const data = await resp.json();
        renderSnapshots(data.snapshots || []);
        if (infoEl) infoEl.textContent = data.snapshots_info || "";
        if (errorEl) errorEl.textContent = data.snapshots_error || "";
      } finally {
        inFlight = false;
      }
    };

    const startSnapshotsPolling = () => {
      if (resticTimer) clearInterval(resticTimer);
      resticTimer = setInterval(pollSnapshots, 5000);
      pollSnapshots();
    };

    if (filterInput) {
      filterInput.addEventListener("change", () => {
        pollSnapshots();
      });
    }

    startSnapshotsPolling();
  }
})();
