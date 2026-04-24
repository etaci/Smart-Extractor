"use strict";

window.SmartExtractorDashboardTaskRuntime = function createDashboardTaskRuntime(
  deps
) {
  const {
    apiFetch,
    fetchJsonOrNull,
    showToast,
    escHtml,
    setText,
    showSection,
    renderInsightList,
    statusLabel,
    formatQuality,
    parseBatchUrls,
    collectBatchGroupOptions,
    syncContinueBatchOptions,
    syncTaskBatchFilterOptions,
    getCurrentSelectedFields,
    getInitialJson,
    getDashboardModules,
    applyRuntimeStatus,
    state,
  } = deps;
  let refreshInFlight = null;

  function parsePercent(value) {
    const numeric = Number.parseFloat(String(value).replace("%", ""));
    return Number.isFinite(numeric) ? Math.max(0, Math.min(100, numeric)) : 0;
  }

  function sortTasksForDisplay(tasks) {
    return [...(tasks || [])].sort((left, right) => {
      const leftIsBatch = left && left.task_kind === "batch";
      const rightIsBatch = right && right.task_kind === "batch";
      if (leftIsBatch !== rightIsBatch) {
        return leftIsBatch ? -1 : 1;
      }

      const leftId = Number.parseInt(
        String(left.task_id || "").replace(/\D/g, ""),
        10
      );
      const rightId = Number.parseInt(
        String(right.task_id || "").replace(/\D/g, ""),
        10
      );
      if (Number.isFinite(leftId) && Number.isFinite(rightId) && leftId !== rightId) {
        return rightId - leftId;
      }

      return String(right.created_at || "").localeCompare(
        String(left.created_at || "")
      );
    });
  }

  function updateDonut(stats) {
    const donut = document.getElementById("donut-chart");
    if (!donut) {
      return;
    }

    const total = Number(stats.total || 0);
    const success = Number(stats.success || 0);
    const failed = Number(stats.failed || 0);
    const running = Number(stats.running || 0);
    const pending = Number(stats.pending || 0);

    donut.style.setProperty("--progress", total > 0 ? ((success / total) * 100).toFixed(2) : "0");
    donut.style.setProperty("--failed-ratio", total > 0 ? ((failed / total) * 100).toFixed(2) : "0");
    donut.style.setProperty("--running-ratio", total > 0 ? ((running / total) * 100).toFixed(2) : "0");
    donut.style.setProperty("--pending-ratio", total > 0 ? ((pending / total) * 100).toFixed(2) : "0");

    setText("legend-success", success);
    setText("legend-failed", failed);
    setText("legend-running", running);
    setText("legend-pending", pending);
  }

  function applyStats(stats) {
    setText("stat-total", stats.total);
    setText("stat-success", stats.success);
    setText("stat-failed", stats.failed);
    setText("stat-running", stats.running);
    setText("stat-pending", stats.pending);
    setText("stat-rate", stats.success_rate);
    setText("hero-success-rate", stats.success_rate);
    updateDonut(stats);
  }

  function applyInsightSummary(insights) {
    const summary = (insights && insights.summary) || {};
    setText("hero-memory-hits", summary.memory_ready_pages || summary.learned_profile_hits || 0);
    setText("hero-saved-runs", summary.site_memory_saved_runs || summary.rule_based_tasks || 0);
    setText("hero-active-monitors", summary.active_monitors || 0);
    setText("hero-high-priority-alerts", summary.high_priority_alerts || 0);
  }

  function updateInsights(tasks, stats, insights) {
    const container = document.getElementById("announcement-list");
    if (!container) {
      return;
    }

    const summary = (insights && insights.summary) || {};
    const total = Number(stats.total || 0);
    const running = Number(stats.running || 0);
    const successRate = parsePercent(stats.success_rate);
    const latestTask = tasks && tasks.length > 0 ? tasks[0] : null;
    const topScenario =
      Array.isArray(insights?.scenario_summary) && insights.scenario_summary.length
        ? insights.scenario_summary[0]
        : null;

    const insightItems = [
      {
        title: "站点记忆",
        body:
          total === 0
            ? "当前还没有任务，先跑一次页面分析，系统才能开始积累站点记忆。"
            : `最近站点记忆命中 ${summary.memory_ready_pages || 0} 次，已经替你节省约 ${summary.site_memory_saved_runs || 0} 次 LLM 调用。`,
        emphasis: true,
      },
      {
        title: "变化闭环",
        body:
          Number(summary.changed_tasks || 0) > 0
            ? `最近检测到 ${summary.changed_tasks} 条有字段变化的任务，其中高优先级提醒 ${summary.high_priority_alerts || 0} 条。`
            : "当前还没有检测到历史变化，多对同一 URL 重复抽取后更容易形成监控闭环。",
      },
      {
        title: "运行状态",
        body:
          running > 0
            ? `当前有 ${running} 个任务正在运行，任务列表会自动刷新。`
            : "当前无运行中任务，可以继续提交新任务或回看历史变化。",
      },
    ];

    if (latestTask) {
      insightItems.push({
        title: `最近任务：${latestTask.task_id}`,
        body: `状态：${statusLabel(latestTask.status)}，模式：${latestTask.schema_name}，质量 ${formatQuality(latestTask.quality_score)}`,
      });
    }
    if (topScenario) {
      insightItems.push({
        title: `高频场景：${topScenario.label}`,
        body: `当前该场景下已有 ${topScenario.count} 个监控方案，适合继续沉淀成标准模板和可转发输出。`,
      });
    }

    container.innerHTML = insightItems
      .map(
        (item) => `
          <div class="insight-item${item.emphasis ? " emphasis" : ""}">
            <span class="insight-title">${escHtml(item.title)}</span>
            <p>${escHtml(item.body)}</p>
          </div>
        `
      )
      .join("");
  }

  function formatTaskProgress(task) {
    const percent = Math.max(0, Math.min(100, Math.round(Number(task.progress_percent || 0))));
    const stage = String(task.progress_stage || "").trim();
    if (!stage && !["running", "pending", "queued"].includes(task.status)) {
      return "";
    }
    if (stage) {
      return `${percent}% · ${stage}`;
    }
    return ["pending", "queued"].includes(task.status) ? "等待调度中" : `${percent}% · 正在执行`;
  }

  function renderTasksTable(tasks) {
    const tbody = document.getElementById("tasks-body");
    if (!tbody) {
      return;
    }
    if (!tasks || tasks.length === 0) {
      tbody.innerHTML = '<tr><td colspan="9" class="empty-msg">暂无任务</td></tr>';
      return;
    }

    tbody.innerHTML = tasks
      .map((task) => {
        const elapsed = task.elapsed_ms > 0 ? `${Math.round(task.elapsed_ms)}ms` : "-";
        const isBatch = task.task_kind === "batch";
        const urlLabel = isBatch ? `批量任务 · ${Number(task.total_items || 0)} 个 URL` : task.url;
        const urlShort = urlLabel.length > 54 ? `${urlLabel.slice(0, 54)}...` : urlLabel;
        const modeLabel = isBatch
          ? `batch (${Number(task.completed_items || 0)}/${Number(task.total_items || 0)})`
          : task.schema_name || "auto";
        const progressText = formatTaskProgress(task);
        return `
          <tr class="task-row-${task.status}">
            <td><code>${escHtml(task.task_id)}</code></td>
            <td>${task.batch_group_id ? `<span class="badge badge-batch">${escHtml(task.batch_group_id)}</span>` : "-"}</td>
            <td class="url-cell" title="${escHtml(urlLabel)}">${escHtml(urlShort)}</td>
            <td>${escHtml(modeLabel)}</td>
            <td class="task-status-cell">
              <span class="badge badge-${task.status}">${escHtml(statusLabel(task.status))}</span>
              ${progressText ? `<span class="task-status-meta">${escHtml(progressText)}</span>` : ""}
            </td>
            <td>${formatQuality(task.quality_score)}</td>
            <td>${elapsed}</td>
            <td>${escHtml(task.created_at || "-")}</td>
            <td><a href="/task/${encodeURIComponent(task.task_id)}" class="table-link">详情</a></td>
          </tr>
        `;
      })
      .join("");
  }

  function renderWatchlist(insights) {
    renderInsightList(
      "watchlist-board",
      insights.watchlist || [],
      "当前还没有重复抽取的 URL，可先多次抽取同一页面来形成历史，再升级成监控。",
      (item) => `
        <div class="insight-item insight-item-row">
          <div>
            <span class="insight-title">${escHtml(item.domain)}</span>
            <p class="line-clamp">${escHtml(item.url)}</p>
            <p class="panel-note">${escHtml(item.monitor_readiness || "继续积累历史")}</p>
          </div>
          <div class="insight-side">
            <strong>${item.total_runs} 次</strong>
            <span>${formatQuality(item.latest_quality)} / ${escHtml(statusLabel(item.latest_status))}</span>
          </div>
        </div>
      `
    );
  }

  async function submitExtract(event) {
    event.preventDefault();
    const url = document.getElementById("url").value.trim();
    const format = document.getElementById("format").value;
    const useStatic = document.getElementById("static-mode").value === "true";
    const selectedFields = getCurrentSelectedFields();
    if (!url) {
      showToast("请输入目标 URL", "error");
      return;
    }

    const button = document.getElementById("submit-btn");
    const result = document.getElementById("submit-result");
    button.disabled = true;
    button.innerHTML = '<span class="spinner"></span>提交中';
    result.style.display = "none";

    try {
      const response = await apiFetch("/api/extract", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ url, storage_format: format, use_static: useStatic, selected_fields: selectedFields }),
      });
      const data = await response.json();
      if (!response.ok) {
        throw new Error(data.detail || "提交失败");
      }
      result.className = "submit-result success";
      result.textContent = `任务已创建：${data.task_id}`;
      result.style.display = "inline-flex";
      showToast(`任务 ${data.task_id} 已提交`, "success");
      setTimeout(() => showSection("tasks"), 500);
      setTimeout(refreshDashboard, 650);
    } catch (error) {
      result.className = "submit-result error";
      result.textContent = `错误：${error.message}`;
      result.style.display = "inline-flex";
      showToast(`提交失败：${error.message}`, "error");
    } finally {
      button.disabled = false;
      button.textContent = "提交任务";
    }
  }

  async function submitBatch() {
    const rawValue = document.getElementById("batch-urls").value.trim();
    const format = document.getElementById("format").value;
    const submitMode = document.querySelector('input[name="batch-submit-mode"]:checked')?.value || "new";
    const batchGroupIdInput = document.getElementById("batch-group-select");
    const batchGroupId = submitMode === "continue" ? String(batchGroupIdInput?.value || "").trim() : "";
    if (!rawValue) {
      showToast("请至少输入一个 URL", "error");
      return;
    }
    if (submitMode === "continue" && !batchGroupId) {
      showToast("续接已有批次时，请先选择批次", "error");
      return;
    }

    const parsed = parseBatchUrls(rawValue);
    if (!parsed.uniqueUrls.length) {
      showToast("没有识别到合法 URL", "error");
      return;
    }

    const button = document.getElementById("batch-btn");
    button.disabled = true;
    button.innerHTML = '<span class="spinner"></span>提交中';
    try {
      const response = await apiFetch("/api/batch", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ urls: parsed.uniqueUrls, storage_format: format, batch_group_id: batchGroupId }),
      });
      const data = await response.json();
      if (!response.ok) {
        throw new Error(data.detail || "批量提交失败");
      }
      document.getElementById("batch-urls").value = "";
      if (batchGroupIdInput) {
        batchGroupIdInput.value = data.batch_group_id || batchGroupId;
      }
      collectBatchGroupOptions([{ batch_group_id: data.batch_group_id || batchGroupId }]);
      syncContinueBatchOptions([]);
      showToast(`已创建批量任务 ${data.task_id}，包含 ${data.count} 个 URL`, "success");
      setTimeout(() => showSection("tasks"), 500);
      setTimeout(refreshDashboard, 650);
    } catch (error) {
      showToast(`批量提交失败：${error.message}`, "error");
    } finally {
      button.disabled = false;
      button.textContent = "提交批量任务";
    }
  }

  function applyDashboardPayload(stats, tasks, insights) {
    const orderedTasks = sortTasksForDisplay(tasks || []);
    state.latestInsightsState = insights || state.latestInsightsState;
    syncTaskBatchFilterOptions(orderedTasks);
    syncContinueBatchOptions(orderedTasks);
    applyStats(stats || { total: 0, success: 0, failed: 0, running: 0, pending: 0, success_rate: "0%" });
    renderTasksTable(orderedTasks);
    applyInsightSummary(insights || {});
    updateInsights(orderedTasks, stats || {}, insights || {});
    renderWatchlist(insights || {});

    const modules = getDashboardModules() || {};
    if (modules.learnedProfilesModule) {
      modules.learnedProfilesModule.renderLearnedProfileBoard(state.latestLearnedProfiles);
    }
    if (modules.dashboardAssetsModule) {
      modules.dashboardAssetsModule.renderMonitorAlerts(insights || {});
      modules.dashboardAssetsModule.renderMonitorBoard(state.latestMonitors);
      modules.dashboardAssetsModule.renderNotificationBoard(
        state.latestNotifications
      );
      modules.dashboardAssetsModule.renderTemplateBoard(state.latestTemplates);
      modules.dashboardAssetsModule.renderMarketTemplateBoard(state.latestMarketTemplates);
    }
  }

  async function refreshDashboard(options = {}) {
    const isBackgroundRefresh = Boolean(options.background);
    if (refreshInFlight) {
      return refreshInFlight;
    }

    refreshInFlight = (async () => {
      const params = new URLSearchParams({
        task_limit: "15",
        notification_limit: "12",
        digest_window_hours: "24",
      });
      if (state.activeTaskBatchFilter && state.activeTaskBatchFilter !== "all") {
        params.set("batch_group_id", state.activeTaskBatchFilter);
      }
      if (
        state.activeNotificationFilter &&
        state.activeNotificationFilter !== "all"
      ) {
        params.set("notification_status", state.activeNotificationFilter);
      }

      const response = await apiFetch(`/api/dashboard?${params.toString()}`, {
        suppressRateLimitToast: isBackgroundRefresh,
      });
      if (response.status === 429) {
        return { ok: false, rateLimited: true };
      }
      if (!response.ok) {
        return { ok: false, rateLimited: false };
      }
      const dashboardData = await response.json();
      if (!dashboardData) {
        return { ok: false, rateLimited: false };
      }

      const tasks = dashboardData.tasks || getInitialJson("initial-tasks", []);
      const stats =
        dashboardData.stats ||
        getInitialJson("initial-stats", {
          total: 0,
          success: 0,
          failed: 0,
          running: 0,
          pending: 0,
          success_rate: "0%",
        });
      const insights =
        dashboardData.insights || getInitialJson("initial-insights", {});
      state.latestTemplates =
        dashboardData.templates || state.latestTemplates;
      state.latestMonitors =
        dashboardData.monitors || state.latestMonitors;
      state.latestNotifications =
        dashboardData.notifications || state.latestNotifications;
      state.latestMarketTemplates =
        dashboardData.market_templates || state.latestMarketTemplates;
      state.latestLearnedProfiles =
        dashboardData.learned_profiles || state.latestLearnedProfiles;
      if (dashboardData.runtime_status) {
        applyRuntimeStatus(dashboardData.runtime_status);
      }
      applyDashboardPayload(stats, tasks, insights);
      return { ok: true, rateLimited: false };
    })();

    try {
      return await refreshInFlight;
    } finally {
      refreshInFlight = null;
    }
  }

  return {
    submitExtract,
    submitBatch,
    applyDashboardPayload,
    refreshDashboard,
  };
};
