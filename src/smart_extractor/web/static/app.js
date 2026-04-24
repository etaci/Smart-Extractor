"use strict";

const {
  getApiToken,
  setApiToken,
  getAuthHeaders,
  showToast,
  initTheme,
  downloadTextFile,
} = window.SmartExtractorShared;
const STATUS_LABELS = {
  success: "提取完成",
  running: "运行中",
  failed: "执行失败",
  pending: "等待执行",
  queued: "队列中",
};

let authErrorNotified = false;
let lastRateLimitToastAt = 0;
let selectedFieldState = [];
let latestAnalyzedFields = [];
let latestFieldLabels = {};
let analyzeProgressTimer = null;
let analyzeProgressValue = 0;
let insightProgressTimer = null;
let insightProgressValue = 0;
let dashboardRefreshTimer = null;
let dashboardRefreshBackoffMs = 0;
let currentBasicConfig = null;
let batchRelationFilterValue = "all";
let collapsedBatchDomains = new Set();
let activeTaskBatchFilter = "all";
let latestBatchGroupOptions = [];
let latestInsightsState = {
  summary: {
    repeat_urls: 0,
    changed_tasks: 0,
    active_monitors: 0,
    notification_ready_monitors: 0,
    notification_success_monitors: 0,
    rule_based_tasks: 0,
    fallback_tasks: 0,
    learned_profile_hits: 0,
    site_memory_saved_runs: 0,
    memory_ready_pages: 0,
    high_priority_alerts: 0,
    avg_quality: "-",
  },
  recent_changes: [],
  scenario_summary: [],
  domain_leaderboard: [],
  watchlist: [],
  monitor_alerts: [],
  monitors: [],
};
let latestInsightAnalysis = null;
let latestInsightAnalysisByMode = {
  single: null,
  compare: null,
};
let latestInsightFieldLabels = {};
let latestInsightCandidateFields = [];
let activeInsightMode = "single";
let latestComparePreview = [];
let latestTemplates = [];
let latestMonitors = [];
let latestLearnedProfiles = [];
let latestNlTaskPlan = null;
let latestMarketTemplates = [];
let activeTemplateMarketFilter = "all";
let activeLearnedProfileFilter = "all";
let learnedProfileSearchKeyword = "";
let activeLearnedProfileDetailId = "";
let activeLearnedProfileDetail = null;
let latestNotifications = [];
let activeNotificationFilter = "all";
let dashboardAssetsModule = null;
let dashboardAnalysis = null;
let learnedProfilesModule = null;
let dashboardTaskRuntime = null;

const RATE_LIMIT_TOAST_COOLDOWN_MS = 20000;
const DASHBOARD_REFRESH_INTERVAL_MS = 15000;

const dashboardAnalysisState = {
  get selectedFieldState() {
    return selectedFieldState;
  },
  set selectedFieldState(value) {
    selectedFieldState = Array.isArray(value) ? value : [];
  },
  get latestAnalyzedFields() {
    return latestAnalyzedFields;
  },
  set latestAnalyzedFields(value) {
    latestAnalyzedFields = Array.isArray(value) ? value : [];
  },
  get latestFieldLabels() {
    return latestFieldLabels;
  },
  set latestFieldLabels(value) {
    latestFieldLabels = value || {};
  },
  get insightProgressTimer() {
    return insightProgressTimer;
  },
  set insightProgressTimer(value) {
    insightProgressTimer = value;
  },
  get insightProgressValue() {
    return insightProgressValue;
  },
  set insightProgressValue(value) {
    insightProgressValue = value;
  },
  get latestInsightAnalysis() {
    return latestInsightAnalysis;
  },
  set latestInsightAnalysis(value) {
    latestInsightAnalysis = value;
  },
  get latestInsightAnalysisByMode() {
    return latestInsightAnalysisByMode;
  },
  set latestInsightAnalysisByMode(value) {
    latestInsightAnalysisByMode = {
      single: value && value.single ? value.single : null,
      compare: value && value.compare ? value.compare : null,
    };
  },
  get latestInsightFieldLabels() {
    return latestInsightFieldLabels;
  },
  set latestInsightFieldLabels(value) {
    latestInsightFieldLabels = value || {};
  },
  get latestInsightCandidateFields() {
    return latestInsightCandidateFields;
  },
  set latestInsightCandidateFields(value) {
    latestInsightCandidateFields = Array.isArray(value) ? value : [];
  },
  get activeInsightMode() {
    return activeInsightMode;
  },
  set activeInsightMode(value) {
    activeInsightMode = value === "compare" ? "compare" : "single";
  },
  get latestComparePreview() {
    return latestComparePreview;
  },
  set latestComparePreview(value) {
    latestComparePreview = Array.isArray(value) ? value : [];
  },
  get latestNlTaskPlan() {
    return latestNlTaskPlan;
  },
  set latestNlTaskPlan(value) {
    latestNlTaskPlan = value;
  },
};

const learnedProfilesState = {
  get latestLearnedProfiles() {
    return latestLearnedProfiles;
  },
  set latestLearnedProfiles(value) {
    latestLearnedProfiles = Array.isArray(value) ? value : [];
  },
  get activeLearnedProfileFilter() {
    return activeLearnedProfileFilter;
  },
  set activeLearnedProfileFilter(value) {
    activeLearnedProfileFilter = value || "all";
  },
  get learnedProfileSearchKeyword() {
    return learnedProfileSearchKeyword;
  },
  set learnedProfileSearchKeyword(value) {
    learnedProfileSearchKeyword = String(value || "");
  },
  get activeLearnedProfileDetailId() {
    return activeLearnedProfileDetailId;
  },
  set activeLearnedProfileDetailId(value) {
    activeLearnedProfileDetailId = String(value || "");
  },
  get activeLearnedProfileDetail() {
    return activeLearnedProfileDetail;
  },
  set activeLearnedProfileDetail(value) {
    activeLearnedProfileDetail = value || null;
  },
};

const dashboardAssetsState = {
  get latestTemplates() {
    return latestTemplates;
  },
  set latestTemplates(value) {
    latestTemplates = Array.isArray(value) ? value : [];
  },
  get latestMonitors() {
    return latestMonitors;
  },
  set latestMonitors(value) {
    latestMonitors = Array.isArray(value) ? value : [];
  },
  get latestNotifications() {
    return latestNotifications;
  },
  set latestNotifications(value) {
    latestNotifications = Array.isArray(value) ? value : [];
  },
  get latestMarketTemplates() {
    return latestMarketTemplates;
  },
  set latestMarketTemplates(value) {
    latestMarketTemplates = Array.isArray(value) ? value : [];
  },
  get activeTemplateMarketFilter() {
    return activeTemplateMarketFilter;
  },
  set activeTemplateMarketFilter(value) {
    activeTemplateMarketFilter = value || "all";
  },
  get activeNotificationFilter() {
    return activeNotificationFilter;
  },
  set activeNotificationFilter(value) {
    activeNotificationFilter = value || "all";
  },
  get latestFieldLabels() {
    return latestFieldLabels;
  },
  set latestFieldLabels(value) {
    latestFieldLabels = value || {};
  },
  get latestAnalyzedFields() {
    return latestAnalyzedFields;
  },
  set latestAnalyzedFields(value) {
    latestAnalyzedFields = Array.isArray(value) ? value : [];
  },
  get selectedFieldState() {
    return selectedFieldState;
  },
  set selectedFieldState(value) {
    selectedFieldState = Array.isArray(value) ? value : [];
  },
};

const dashboardTaskRuntimeState = {
  get latestInsightsState() {
    return latestInsightsState;
  },
  set latestInsightsState(value) {
    latestInsightsState = value || latestInsightsState;
  },
  get latestTemplates() {
    return latestTemplates;
  },
  set latestTemplates(value) {
    latestTemplates = Array.isArray(value) ? value : [];
  },
  get latestMonitors() {
    return latestMonitors;
  },
  set latestMonitors(value) {
    latestMonitors = Array.isArray(value) ? value : [];
  },
  get latestNotifications() {
    return latestNotifications;
  },
  set latestNotifications(value) {
    latestNotifications = Array.isArray(value) ? value : [];
  },
  get latestMarketTemplates() {
    return latestMarketTemplates;
  },
  set latestMarketTemplates(value) {
    latestMarketTemplates = Array.isArray(value) ? value : [];
  },
  get latestLearnedProfiles() {
    return latestLearnedProfiles;
  },
  set latestLearnedProfiles(value) {
    latestLearnedProfiles = Array.isArray(value) ? value : [];
  },
  get activeTaskBatchFilter() {
    return activeTaskBatchFilter;
  },
  set activeTaskBatchFilter(value) {
    activeTaskBatchFilter = value || "all";
  },
  get activeNotificationFilter() {
    return activeNotificationFilter;
  },
  set activeNotificationFilter(value) {
    activeNotificationFilter = value || "all";
  },
};

if (!window.SmartExtractorDashboardAssets) {
  throw new Error("SmartExtractorDashboardAssets 未加载");
}

dashboardAssetsModule = window.SmartExtractorDashboardAssets({
  apiFetch,
  fetchJsonOrNull,
  showToast,
  escHtml,
  setText,
  setInputValue,
  showSection,
  renderInsightList,
  renderFieldSelector,
  applyMonitorProfile,
  refreshDashboard,
  state: dashboardAssetsState,
});

if (!window.SmartExtractorDashboardAnalysis) {
  throw new Error("SmartExtractorDashboardAnalysis 未加载");
}

dashboardAnalysis = window.SmartExtractorDashboardAnalysis({
  apiFetch,
  showToast,
  downloadTextFile,
  escHtml,
  parseBatchUrls,
  setText,
  setInputValue,
  showSection,
  updateBatchSummary,
  renderFieldSelector,
  applyMonitorProfile: (...args) => applyMonitorProfile(...args),
  loadMonitors: (...args) => dashboardAssetsModule.loadMonitors(...args),
  refreshDashboard,
  startAnalyzeProgress,
  finishAnalyzeProgress,
  updateAnalysisPreview,
  state: dashboardAnalysisState,
});

if (!window.SmartExtractorLearnedProfiles) {
  throw new Error("SmartExtractorLearnedProfiles 未加载");
}

learnedProfilesModule = window.SmartExtractorLearnedProfiles({
  apiFetch,
  showToast,
  escHtml,
  setText,
  showSection,
  refreshDashboard,
  renderInsightList,
  statusLabel,
  formatQuality,
  formatExtractionStrategy: (...args) => dashboardAssetsModule.formatExtractionStrategy(...args),
  state: learnedProfilesState,
});

if (!window.SmartExtractorDashboardTaskRuntime) {
  throw new Error("SmartExtractorDashboardTaskRuntime 未加载");
}

dashboardTaskRuntime = window.SmartExtractorDashboardTaskRuntime({
  apiFetch,
  fetchJsonOrNull,
  showToast,
  escHtml,
  setText,
  showSection,
  renderInsightList,
  statusLabel,
  formatQuality,
  parseBatchUrls: (...args) => parseBatchUrls(...args),
  collectBatchGroupOptions: (...args) => collectBatchGroupOptions(...args),
  syncContinueBatchOptions: (...args) => syncContinueBatchOptions(...args),
  syncTaskBatchFilterOptions: (...args) => syncTaskBatchFilterOptions(...args),
  getCurrentSelectedFields: (...args) => currentSelectedFields(...args),
  getInitialJson,
  getDashboardModules: () => ({
    dashboardAssetsModule,
    learnedProfilesModule,
  }),
  applyRuntimeStatus,
  state: dashboardTaskRuntimeState,
});

function getRuntimeStatus() {
  return getInitialJson("runtime-status", {
    ready: true,
    issues: [],
    warnings: [],
    api_token_required: false,
    startup_check_enabled: false,
    startup_check_verify_model: false,
    services: {
      monitor_scheduler: {
        enabled: false,
        alive: false,
        poll_interval_seconds: 0,
        total_runs: 0,
        last_run_completed_at: "",
        last_claimed_count: 0,
        last_triggered_count: 0,
        last_failed_count: 0,
        last_reclaimed_count: 0,
        last_error: "",
      },
      task_worker: {
        enabled: false,
        alive: false,
        task_dispatch_mode: "inline",
      },
      notification_retry: {
        enabled: false,
        alive: false,
        poll_interval_seconds: 0,
        total_runs: 0,
      },
      notification_digest: {
        enabled: false,
        alive: false,
        poll_interval_seconds: 0,
        total_runs: 0,
      },
    },
  });
}

function runtimeBadgeClass(isAlive, isEnabled) {
  if (isAlive) {
    return "badge-success";
  }
  if (isEnabled) {
    return "badge-running";
  }
  return "badge-pending";
}

function setClassName(id, className) {
  const element = document.getElementById(id);
  if (element) {
    element.className = className;
  }
}

function renderRuntimeList(id, items) {
  const element = document.getElementById(id);
  if (!element) {
    return;
  }
  const normalizedItems = Array.isArray(items)
    ? items.filter((item) => String(item || "").trim())
    : [];
  element.hidden = normalizedItems.length === 0;
  element.innerHTML = normalizedItems
    .map((item) => `<div class="startup-item">${escHtml(item)}</div>`)
    .join("");
}

function applyRuntimeStatus(status) {
  const runtime = status || getRuntimeStatus();
  const services = runtime.services || {};
  const scheduler = services.monitor_scheduler || {};
  const worker = services.task_worker || {};
  const notificationRetry = services.notification_retry || {};
  const notificationDigest = services.notification_digest || {};
  const ready = !!runtime.ready;

  setClassName(
    "runtime-startup-panel",
    `panel startup-panel ${ready ? "startup-panel-ok" : "startup-panel-warning"}`
  );
  setText(
    "runtime-summary-caption",
    ready ? "当前可直接提交任务" : "还有配置项需要处理"
  );
  setText(
    "runtime-startup-title",
    ready ? "系统已就绪" : "请先完成基础配置"
  );
  setText(
    "runtime-startup-description",
    ready
      ? "Web 服务已启动，你可以直接分析页面、提交任务并查看历史结果。"
      : "现在仍可打开界面查看任务和历史数据，但缺失配置会阻止新任务提交。"
  );

  setClassName(
    "runtime-api-token-badge",
    `badge ${runtime.api_token_required ? "badge-running" : "badge-pending"}`
  );
  setText(
    "runtime-api-token-badge",
    runtime.api_token_required ? "API Token 已启用" : "API Token 未启用"
  );

  setClassName(
    "runtime-startup-check-badge",
    `badge ${runtime.startup_check_enabled ? "badge-success" : "badge-pending"}`
  );
  setText(
    "runtime-startup-check-badge",
    runtime.startup_check_enabled ? "启动自检开启" : "启动自检关闭"
  );

  setClassName(
    "runtime-monitor-scheduler-badge",
    `badge ${runtimeBadgeClass(!!scheduler.alive, !!scheduler.enabled)}`
  );
  setText(
    "runtime-monitor-scheduler-badge",
    scheduler.alive
      ? "监控调度器运行中"
      : scheduler.enabled
        ? "监控调度器待启动"
        : "监控调度器未启用"
  );

  setClassName(
    "runtime-task-worker-badge",
    `badge ${runtimeBadgeClass(!!worker.alive, !!worker.enabled)}`
  );
  setText(
    "runtime-task-worker-badge",
    worker.alive
      ? "队列 Worker 运行中"
      : worker.enabled
        ? "队列 Worker 待启动"
        : "队列 Worker 未启用"
  );

  setClassName(
    "runtime-notification-retry-badge",
    `badge ${runtimeBadgeClass(
      !!notificationRetry.alive,
      !!notificationRetry.enabled
    )}`
  );
  setText(
    "runtime-notification-retry-badge",
    notificationRetry.alive
      ? "通知重试服务运行中"
      : notificationRetry.enabled
        ? "通知重试服务待启动"
        : "通知重试服务未启用"
  );

  setClassName(
    "runtime-notification-digest-badge",
    `badge ${runtimeBadgeClass(
      !!notificationDigest.alive,
      !!notificationDigest.enabled
    )}`
  );
  setText(
    "runtime-notification-digest-badge",
    notificationDigest.alive
      ? "Digest 服务运行中"
      : notificationDigest.enabled
        ? "Digest 服务待启动"
        : "Digest 服务未启用"
  );

  if (scheduler.enabled) {
    const schedulerBits = [
      `轮询 ${scheduler.poll_interval_seconds || 0}s`,
      `累计运行 ${scheduler.total_runs || 0} 轮`,
    ];
    if (scheduler.last_run_completed_at) {
      schedulerBits.push(`最近完成 ${scheduler.last_run_completed_at}`);
    }
    schedulerBits.push(
      `最近 claim ${scheduler.last_claimed_count || 0} / 触发 ${scheduler.last_triggered_count || 0}`
    );
    if (scheduler.last_failed_count) {
      schedulerBits.push(`失败 ${scheduler.last_failed_count}`);
    }
    if (scheduler.last_reclaimed_count) {
      schedulerBits.push(`回收 ${scheduler.last_reclaimed_count}`);
    }
    if (scheduler.last_error) {
      schedulerBits.push(`错误 ${scheduler.last_error}`);
    }
    setText("runtime-monitor-scheduler-note", schedulerBits.join(" · "));
  } else {
    setText(
      "runtime-monitor-scheduler-note",
      "当前实例不承担监控调度角色，可用于只读查看或 API 接入。"
    );
  }

  if (worker.enabled) {
    const workerBits = [
      `分发模式 ${worker.task_dispatch_mode || "inline"}`,
      `轮询 ${worker.worker_poll_interval_seconds || 0}s`,
      `超时接管 ${worker.worker_stale_after_seconds || 0}s`,
    ];
    if (worker.alive) {
      workerBits.push("状态 正在运行");
    }
    setText("runtime-task-worker-note", workerBits.join(" · "));
  } else {
    setText("runtime-task-worker-note", "当前未启用内置队列 Worker。");
  }

  const notificationBits = [
    notificationRetry.enabled
      ? `通知重试：轮询 ${notificationRetry.poll_interval_seconds || 0}s，累计 ${notificationRetry.total_runs || 0} 轮`
      : "通知重试：未启用",
    notificationDigest.enabled
      ? `Digest：轮询 ${notificationDigest.poll_interval_seconds || 0}s，累计 ${notificationDigest.total_runs || 0} 轮`
      : "Digest：未启用",
  ];
  if (notificationDigest.last_run_completed_at) {
    notificationBits.push(`Digest 最近完成 ${notificationDigest.last_run_completed_at}`);
  }
  if (notificationDigest.last_error) {
    notificationBits.push(`Digest 错误 ${notificationDigest.last_error}`);
  }
  setText("runtime-notification-services-note", notificationBits.join(" · "));

  renderRuntimeList("runtime-issues-list", runtime.issues || []);
  renderRuntimeList("runtime-warnings-list", runtime.warnings || []);
}
function resetApiTokenState(token) {
  setApiToken(token);
  authErrorNotified = false;
}

async function apiFetch(url, options = {}) {
  const opts = { ...options };
  const suppressRateLimitToast = Boolean(opts.suppressRateLimitToast);
  delete opts.suppressRateLimitToast;
  opts.headers = getAuthHeaders(options.headers || {});
  const response = await fetch(url, opts);
  if (response.status === 401 && !authErrorNotified) {
    authErrorNotified = true;
    showToast("鉴权失败，请检查 API Token", "error");
  } else if (response.status === 429) {
    const now = Date.now();
    if (!suppressRateLimitToast && now - lastRateLimitToastAt >= RATE_LIMIT_TOAST_COOLDOWN_MS) {
      lastRateLimitToastAt = now;
      showToast("请求过于频繁，已降低重复提示频率，请稍后重试", "error");
    }
  } else if (response.ok) {
    authErrorNotified = false;
  }
  return response;
}

async function fetchJsonOrNull(url, options = {}) {
  try {
    const response = await apiFetch(url, options);
    if (!response.ok) {
      return null;
    }
    return await response.json();
  } catch (_) {
    return null;
  }
}

function getInitialJson(id, fallback) {
  const element = document.getElementById(id);
  if (!element) {
    return fallback;
  }
  try {
    return JSON.parse(element.textContent);
  } catch (_) {
    return fallback;
  }
}

function initApiTokenInput() {
  const input = document.getElementById("api-token");
  if (!input) {
    return;
  }
  input.value = getApiToken();
  input.addEventListener("change", () => {
    resetApiTokenState(input.value);
    showToast("API Token 已更新", "success");
    refreshDashboard();
  });
}

function setInputValue(id, value) {
  const element = document.getElementById(id);
  if (element) {
    element.value = value == null ? "" : String(value);
  }
}

function setBasicConfigResult(message, type) {
  const result = document.getElementById("basic-config-result");
  if (!result) {
    return;
  }
  result.className = `submit-result ${type}`;
  result.textContent = message;
  result.style.display = "inline-flex";
}

function describeConfigSources(config) {
  const env = (config && config.env_overrides) || {};
  const overridden = [];
  if (env.api_key) overridden.push("api_key");
  if (env.base_url) overridden.push("base_url");
  if (env.model) overridden.push("model");

  if (!overridden.length) {
    return `当前编辑的是文件值：${config.config_path}`;
  }

  return `已检测到环境变量覆盖：${overridden.join("、")}。保存文件后，新值仍可能被环境变量覆盖。`;
}

function describeEffectiveConfig(config) {
  const effective = (config && config.effective) || {};
  const apiKeyState = effective.api_key_masked || "未配置";
  return `当前生效值：api_key=${apiKeyState}；base_url=${effective.base_url || "-"}；model=${effective.model || "-"}；temperature=${effective.temperature ?? "-"}`;
}

function applyBasicConfig(config) {
  currentBasicConfig = config;
  setInputValue("basic-api-key", config.api_key || "");
  setInputValue("basic-base-url", config.base_url || "");
  setInputValue("basic-model", config.model || "");
  setInputValue("basic-temperature", config.temperature ?? 0);
  setText("basic-config-source-note", describeConfigSources(config));
  setText("basic-config-effective-note", describeEffectiveConfig(config));
}

async function loadBasicConfig(showFeedback = false) {
  const data = await fetchJsonOrNull("/api/config/basic");
  if (!data) {
    if (showFeedback) {
      showToast("读取基础配置失败，请检查 API Token 或后端状态", "error");
    }
    return;
  }
  applyBasicConfig(data);
  if (showFeedback) {
    showToast("基础配置已刷新", "success");
  }
}

async function saveBasicConfig(event) {
  event.preventDefault();

  const button = document.getElementById("save-basic-config-btn");
  const payload = {
    api_key: document.getElementById("basic-api-key").value.trim(),
    base_url: document.getElementById("basic-base-url").value.trim(),
    model: document.getElementById("basic-model").value.trim(),
    temperature: Number(document.getElementById("basic-temperature").value || 0),
  };

  if (!payload.base_url || !payload.model) {
    setBasicConfigResult("请完整填写 base_url 和 model", "error");
    showToast("请完整填写基础配置", "error");
    return;
  }

  button.disabled = true;
  button.innerHTML = '<span class="spinner"></span>保存中';

  try {
    const response = await apiFetch("/api/config/basic", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    const data = await response.json();
    if (!response.ok) {
      throw new Error(data.detail || "保存失败");
    }
    applyBasicConfig(data.config || currentBasicConfig || payload);
    setBasicConfigResult(data.message || "基础配置已保存", "success");
    showToast("基础配置已保存", "success");
    const runtimeData = await fetchJsonOrNull("/api/runtime");
    if (runtimeData) {
      applyRuntimeStatus(runtimeData);
      if (runtimeData.ready) {
        showSection("overview");
      }
    }
    await refreshDashboard();
  } catch (error) {
    setBasicConfigResult(`错误：${error.message}`, "error");
    showToast(`保存失败：${error.message}`, "error");
  } finally {
    button.disabled = false;
    button.textContent = "保存基础配置";
  }
}

function scheduleDashboardRefresh() {
  if (dashboardRefreshTimer) {
    clearTimeout(dashboardRefreshTimer);
  }
  const delay = Math.max(
    DASHBOARD_REFRESH_INTERVAL_MS,
    dashboardRefreshBackoffMs || DASHBOARD_REFRESH_INTERVAL_MS
  );
  dashboardRefreshTimer = setTimeout(async () => {
    if (document.hidden) {
      scheduleDashboardRefresh();
      return;
    }
    try {
      const refreshResult = await refreshDashboard({ background: true });
      dashboardRefreshBackoffMs = refreshResult && refreshResult.rateLimited
        ? Math.min(
            Math.max(
              DASHBOARD_REFRESH_INTERVAL_MS * 4,
              dashboardRefreshBackoffMs
                ? dashboardRefreshBackoffMs * 2
                : DASHBOARD_REFRESH_INTERVAL_MS * 4
            ),
            120000
          )
        : 0;
    } catch (_) {
      dashboardRefreshBackoffMs = Math.max(
        dashboardRefreshBackoffMs || 0,
        DASHBOARD_REFRESH_INTERVAL_MS * 2
      );
    } finally {
      scheduleDashboardRefresh();
    }
  }, delay);
}

function showSection(name) {
  const sections = ["overview", "extract", "analyzer", "tasks", "assets"];
  document.body.dataset.activeSection = sections.includes(name) ? name : "overview";
  document.querySelectorAll(".nav-item").forEach((item) => {
    item.classList.toggle("active", item.dataset.section === name);
  });

  sections.forEach((sectionName) => {
    const el = document.getElementById(`section-${sectionName}`);
    if (el) {
      el.classList.toggle("section-hidden", sectionName !== name);
    }
  });
}

function setText(id, value) {
  const element = document.getElementById(id);
  if (element) {
    element.textContent = value;
  }
}

function escHtml(value) {
  return String(value)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}

function parsePercent(value) {
  const numeric = Number.parseFloat(String(value).replace("%", ""));
  return Number.isFinite(numeric) ? Math.max(0, Math.min(100, numeric)) : 0;
}

function statusLabel(status) {
  return STATUS_LABELS[status] || status || "-";
}

function formatQuality(value) {
  const numeric = Number(value || 0);
  return numeric > 0 ? `${Math.round(numeric * 100)}%` : "-";
}

function parseBatchUrls(rawValue) {
  const lines = String(rawValue || "")
    .split(/\r?\n/)
    .map((item) => item.trim())
    .filter(Boolean);
  const validUrls = lines.filter((item) => item.startsWith("http://") || item.startsWith("https://"));
  const uniqueUrls = [...new Set(validUrls)];
  return {
    totalLines: lines.length,
    validUrls,
    uniqueUrls,
    duplicateCount: Math.max(validUrls.length - uniqueUrls.length, 0),
    invalidCount: Math.max(lines.length - validUrls.length, 0),
  };
}

function safeDecodeUrl(url) {
  try {
    return decodeURIComponent(url);
  } catch (_) {
    return url;
  }
}

function clusterLabel(pathname) {
  if (!pathname || pathname === "/") {
    return "首页或根路径";
  }
  const parts = pathname.split("/").filter(Boolean);
  if (!parts.length) {
    return "首页或根路径";
  }
  return `/${parts.slice(0, Math.min(parts.length, 2)).join("/")}`;
}

function summarizePath(pathname) {
  if (!pathname || pathname === "/") {
    return "根路径";
  }
  const parts = pathname.split("/").filter(Boolean);
  const summary = parts.slice(0, 3).join("/");
  return safeDecodeUrl(summary || pathname);
}

function buildBatchRelations(urls) {
  const domains = new Map();

  urls.forEach((itemUrl, index) => {
    try {
      const parsed = new URL(itemUrl);
      const domain = parsed.hostname.toLowerCase();
      const pathname = parsed.pathname || "/";
      const pathParts = pathname.split("/").filter(Boolean);
      const clusterKey = pathParts.slice(0, 2).join("/") || "__root__";

      if (!domains.has(domain)) {
        domains.set(domain, {
          domain,
          total: 0,
          items: [],
          clusters: new Map(),
        });
      }

      const domainEntry = domains.get(domain);
      domainEntry.total += 1;
      domainEntry.items.push({
        url: itemUrl,
        pathname,
        label: summarizePath(pathname),
        order: index + 1,
      });

      if (!domainEntry.clusters.has(clusterKey)) {
        domainEntry.clusters.set(clusterKey, {
          key: clusterKey,
          label: clusterLabel(pathname),
          count: 0,
          items: [],
        });
      }

      const clusterEntry = domainEntry.clusters.get(clusterKey);
      clusterEntry.count += 1;
      clusterEntry.items.push({
        url: itemUrl,
        pathname,
        label: summarizePath(pathname),
        order: index + 1,
      });
    } catch (_) {
      // ignore invalid urls here; parseBatchUrls already counts them elsewhere
    }
  });

  const groupedDomains = [...domains.values()]
    .map((entry) => ({
      domain: entry.domain,
      total: entry.total,
      items: entry.items,
      clusters: [...entry.clusters.values()].sort((left, right) => {
        if (right.count !== left.count) {
          return right.count - left.count;
        }
        return left.label.localeCompare(right.label);
      }),
    }))
    .sort((left, right) => {
      if (right.total !== left.total) {
        return right.total - left.total;
      }
      return left.domain.localeCompare(right.domain);
    });

  return {
    domains: groupedDomains,
    domainCount: groupedDomains.length,
    clusterCount: groupedDomains.reduce((sum, entry) => sum + entry.clusters.length, 0),
    primaryDomain: groupedDomains[0] || null,
  };
}

function relationDomainOptionLabel(entry) {
  return `${entry.domain} (${entry.total})`;
}

function setBatchGroupInputVisibility() {
  const mode = document.querySelector('input[name="batch-submit-mode"]:checked')?.value || "new";
  const input = document.getElementById("batch-group-select");
  if (!input) {
    return;
  }
  input.hidden = mode !== "continue";
}

function collectBatchGroupOptions(tasks) {
  const groups = [...new Set((tasks || []).map((task) => String(task.batch_group_id || "").trim()).filter(Boolean))];
  groups.sort((left, right) => right.localeCompare(left));
  latestBatchGroupOptions = groups;
  return groups;
}

function syncContinueBatchOptions(tasks) {
  const select = document.getElementById("batch-group-select");
  if (!select) {
    return;
  }
  const groups = latestBatchGroupOptions.length ? latestBatchGroupOptions : collectBatchGroupOptions(tasks);
  const currentValue = select.value;
  select.innerHTML = ['<option value="">选择已有批次</option>']
    .concat(groups.map((groupId) => `<option value="${escHtml(groupId)}">${escHtml(groupId)}</option>`))
    .join("");

  if (currentValue && groups.includes(currentValue)) {
    select.value = currentValue;
  } else if (groups.length === 1) {
    select.value = groups[0];
  } else {
    select.value = "";
  }

  select.disabled = groups.length === 0;
}

function chooseLatestBatchForContinue() {
  const modeInput = document.querySelector('input[name="batch-submit-mode"][value="continue"]');
  const batchGroupSelect = document.getElementById("batch-group-select");
  if (!modeInput || !batchGroupSelect || !latestBatchGroupOptions.length) {
    return false;
  }
  modeInput.checked = true;
  batchGroupSelect.value = latestBatchGroupOptions[0];
  setBatchGroupInputVisibility();
  return true;
}

function syncTaskBatchFilterOptions(tasks) {
  const select = document.getElementById("task-batch-filter");
  if (!select) {
    return;
  }
  const groups = collectBatchGroupOptions(tasks);
  select.innerHTML = ['<option value="all">全部批次</option>']
    .concat(groups.map((groupId) => `<option value="${escHtml(groupId)}">${escHtml(groupId)}</option>`))
    .join("");

  if (activeTaskBatchFilter !== "all" && groups.includes(activeTaskBatchFilter)) {
    select.value = activeTaskBatchFilter;
  } else {
    activeTaskBatchFilter = "all";
    select.value = "all";
  }
}

function syncBatchDomainFilter(relations) {
  const select = document.getElementById("batch-domain-filter");
  if (!select) {
    return;
  }

  const currentValue = batchRelationFilterValue;
  const options = ['<option value="all">全部域名</option>']
    .concat(
      relations.domains.map(
        (entry) => `<option value="${escHtml(entry.domain)}">${escHtml(relationDomainOptionLabel(entry))}</option>`
      )
    )
    .join("");
  select.innerHTML = options;

  const availableDomains = new Set(relations.domains.map((entry) => entry.domain));
  if (currentValue !== "all" && availableDomains.has(currentValue)) {
    select.value = currentValue;
  } else {
    batchRelationFilterValue = "all";
    select.value = "all";
  }
}

function removeBatchUrl(targetUrl) {
  const textarea = document.getElementById("batch-urls");
  if (!textarea) {
    return;
  }
  const nextUrls = parseBatchUrls(textarea.value).uniqueUrls.filter((item) => item !== targetUrl);
  setBatchUrls(nextUrls, { focus: true });
  showToast("已从批量列表移除 URL", "success");
}

function bindBatchRelationBoardEvents() {
  const board = document.getElementById("batch-relation-board");
  if (!board) {
    return;
  }

  board.querySelectorAll("[data-remove-url]").forEach((button) => {
    button.addEventListener("click", () => removeBatchUrl(button.dataset.removeUrl || ""));
  });

  board.querySelectorAll("[data-domain-toggle]").forEach((button) => {
    button.addEventListener("click", () => {
      const domain = button.dataset.domainToggle || "";
      if (!domain) {
        return;
      }
      if (collapsedBatchDomains.has(domain)) {
        collapsedBatchDomains.delete(domain);
      } else {
        collapsedBatchDomains.add(domain);
      }
      updateBatchRelationBoard(parseBatchUrls(document.getElementById("batch-urls")?.value || ""));
    });
  });
}

function updateBatchRelationBoard(parsed) {
  const summary = document.getElementById("batch-relation-summary");
  const board = document.getElementById("batch-relation-board");
  if (!summary || !board) {
    return;
  }

  const relations = buildBatchRelations(parsed.uniqueUrls);
  syncBatchDomainFilter(relations);
  setText("batch-domain-count", relations.domainCount);
  setText("batch-cluster-count", relations.clusterCount);
  setText("batch-primary-domain", relations.primaryDomain ? relations.primaryDomain.domain : "-");

  if (parsed.uniqueUrls.length === 0) {
    summary.textContent = "输入多个 URL 后，这里会提示哪些链接属于同一站点、同一栏目，适合串联追踪。";
    board.innerHTML = '<div class="relation-empty">还没有可分析的 URL 关系。</div>';
    return;
  }

  const hints = [];
  if (relations.primaryDomain && relations.primaryDomain.total >= 2) {
    hints.push(`${relations.primaryDomain.domain} 占 ${relations.primaryDomain.total} 个 URL，可作为本次批量的主追踪站点`);
    const strongestCluster = relations.primaryDomain.clusters[0];
    if (strongestCluster && strongestCluster.count >= 2) {
      hints.push(`${strongestCluster.label} 下有 ${strongestCluster.count} 个页面，适合同栏目连续追踪`);
    }
  }
  if (relations.domainCount > 1) {
    hints.push(`共覆盖 ${relations.domainCount} 个域名，可对比不同来源的抽取稳定性`);
  }
  if (parsed.duplicateCount > 0) {
    hints.push(`已自动识别 ${parsed.duplicateCount} 个重复 URL，提交时会自动去重`);
  }
  summary.textContent = hints.join("；") || "当前 URL 更偏向跨站点探索，系统已按域名和路径自动建立关系预览。";

  const visibleDomains = relations.domains.filter(
    (entry) => batchRelationFilterValue === "all" || entry.domain === batchRelationFilterValue
  );

  if (!visibleDomains.length) {
    board.innerHTML = '<div class="relation-empty">当前筛选条件下没有可展示的域名分组。</div>';
    return;
  }

  board.innerHTML = visibleDomains
    .map((entry) => {
      const collapsed = collapsedBatchDomains.has(entry.domain);
      const clusterMarkup = entry.clusters
        .slice(0, 3)
        .map((cluster) => {
          const sample = cluster.items
            .slice(0, 3)
            .map(
              (item) => `
                <li>
                  <span>#${item.order} · ${escHtml(item.label || item.url)}</span>
                  <button class="mini-link danger" type="button" data-remove-url="${escHtml(item.url)}">移除</button>
                </li>
              `
            )
            .join("");
          return `
            <article class="relation-cluster ${cluster.count > 1 ? "is-linked" : ""}">
              <div class="relation-cluster-head">
                <strong>${escHtml(cluster.label)}</strong>
                <span>${cluster.count} 个 URL</span>
              </div>
              <ul class="relation-sample-list">${sample}</ul>
            </article>
          `;
        })
        .join("");

        return `
        <article class="relation-domain-card ${collapsed ? "collapsed" : ""}">
          <div class="relation-domain-head">
            <div>
              <strong>${escHtml(entry.domain)}</strong>
              <p>共 ${entry.total} 个 URL，建议先提交同域页面，后续历史更连续。</p>
            </div>
            <div class="relation-domain-actions">
              <span class="relation-domain-badge">${entry.clusters.length} 个关联簇</span>
              <button class="mini-link" type="button" data-domain-toggle="${escHtml(entry.domain)}">${collapsed ? "展开" : "折叠"}</button>
            </div>
          </div>
          <div class="relation-cluster-grid">${collapsed ? "" : clusterMarkup}</div>
        </article>
      `;
    })
    .join("");

  bindBatchRelationBoardEvents();
}

function formatTaskProgress(task) {
  const percent = Math.max(0, Math.min(100, Math.round(Number(task.progress_percent || 0))));
  const stage = String(task.progress_stage || "").trim();
  if (!stage && task.status !== "running" && task.status !== "pending" && task.status !== "queued") {
    return "";
  }
  if (stage) {
    return `${percent}% · ${stage}`;
  }
  if (task.status === "pending" || task.status === "queued") {
    return "等待调度中";
  }
  return `${percent}% · 正在执行`;
}

function updateBatchSummary() {
  const textarea = document.getElementById("batch-urls");
  const summary = document.getElementById("batch-url-summary");
  const button = document.getElementById("batch-btn");
  if (!textarea || !summary || !button) {
    return;
  }

  const parsed = parseBatchUrls(textarea.value);
  const hasInput = parsed.totalLines > 0;
  const hasValid = parsed.uniqueUrls.length > 0;
  button.disabled = hasInput && !hasValid;
  summary.classList.toggle("warning", hasInput && !hasValid);

  if (!hasInput) {
    summary.textContent = "输入 URL 后，这里会显示本次批量提交的有效数量、重复项和无效行。";
    updateBatchRelationBoard(parsed);
    return;
  }

  const fragments = [
    `共 ${parsed.totalLines} 行`,
    `有效 ${parsed.validUrls.length} 个`,
    `实际将提交 ${parsed.uniqueUrls.length} 个`,
  ];
  if (parsed.duplicateCount > 0) {
    fragments.push(`重复 ${parsed.duplicateCount} 个`);
  }
  if (parsed.invalidCount > 0) {
    fragments.push(`无效 ${parsed.invalidCount} 行`);
  }
  summary.textContent = fragments.join("，");
  updateBatchRelationBoard(parsed);
}

function setBatchUrls(nextUrls, options = {}) {
  const textarea = document.getElementById("batch-urls");
  if (!textarea) {
    return;
  }
  textarea.value = nextUrls.join("\n");
  updateBatchSummary();
  if (options.focus) {
    textarea.focus();
  }
}

function appendUrlsToBatch(urls) {
  const textarea = document.getElementById("batch-urls");
  if (!textarea) {
    return { added: 0, total: 0 };
  }
  const existing = parseBatchUrls(textarea.value).uniqueUrls;
  const merged = [...new Set([...existing, ...urls.filter(Boolean)])];
  setBatchUrls(merged, { focus: true });
  return {
    added: Math.max(merged.length - existing.length, 0),
    total: merged.length,
  };
}

function appendCurrentUrlToBatch() {
  const currentUrl = document.getElementById("url")?.value.trim() || "";
  if (!currentUrl) {
    showToast("请先在上方输入一个目标 URL", "error");
    return;
  }
  const result = appendUrlsToBatch([currentUrl]);
  showToast(result.added > 0 ? "已将当前 URL 加入批量列表" : "当前 URL 已在批量列表中", result.added > 0 ? "success" : "info");
}

function appendWatchlistToBatch() {
  const watchlist = (latestInsightsState && latestInsightsState.watchlist) || [];
  if (!watchlist.length) {
    showToast("当前没有可加入的重复监控候选", "error");
    return;
  }
  const result = appendUrlsToBatch(watchlist.map((item) => item.url));
  if (latestBatchGroupOptions.length > 0) {
    chooseLatestBatchForContinue();
  }
  showToast(`已补充 ${result.added} 个监控候选 URL`, result.added > 0 ? "success" : "info");
}

function normalizeBatchUrls() {
  const textarea = document.getElementById("batch-urls");
  if (!textarea) {
    return;
  }
  const parsed = parseBatchUrls(textarea.value);
  const relations = buildBatchRelations(parsed.uniqueUrls);
  const ordered = relations.domains.flatMap((entry) =>
    entry.clusters.flatMap((cluster) => cluster.items.map((item) => item.url))
  );
  setBatchUrls(ordered, { focus: true });
  showToast(ordered.length > 0 ? "已按域名和路径关系整理批量 URL" : "没有可整理的合法 URL", ordered.length > 0 ? "success" : "error");
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
  const topScenario = Array.isArray(insights?.scenario_summary) && insights.scenario_summary.length
    ? insights.scenario_summary[0]
    : null;

  const insightItems = [
    {
      title: "系统总览",
      body:
        total === 0
          ? "当前还没有任务，建议先提交一个页面看看自动字段识别和结果润色链路。"
          : `累计任务 ${total} 个，成功率 ${successRate.toFixed(1)}%，平均质量 ${summary.avg_quality || "-"}`,
      emphasis: true,
    },
    {
      title: "变化追踪",
      body:
        Number(summary.changed_tasks || 0) > 0
          ? `最近检测到 ${summary.changed_tasks} 条有字段变化的成功任务，已经适合向“监控”方向使用。`
          : "当前还没有检测到历史变化，多对同一 URL 重复抽取后这里会更有价值。",
      emphasis: false,
    },
    {
      title: "重复 URL",
      body:
        Number(summary.repeat_urls || 0) > 0
          ? `最近有 ${summary.repeat_urls} 个 URL 出现重复抽取，它们最适合做持续追踪。`
          : "目前重复抽取的 URL 还不多，仪表盘会随着历史积累变得更有洞察。",
      emphasis: false,
    },
    {
      title: "监控概览",
      body:
        Number(summary.active_monitors || 0) > 0
          ? `当前已保存 ${summary.active_monitors} 个监控项，其中 ${summary.notification_ready_monitors || 0} 个已具备通知通道。`
          : "当前还没有保存监控项，建议从高频重复 URL 里挑几个先加入监控。",
      emphasis: false,
    },
    {
      title: "通知闭环",
      body:
        Number(summary.notification_success_monitors || 0) > 0
          ? `最近已有 ${summary.notification_success_monitors} 个监控成功发出通知，闭环已经开始形成。`
          : "当前还没有成功发出的监控通知，可在“监控闭环配置”里补上 Webhook。",
      emphasis: false,
    },
    {
      title: "规则复用",
      body:
        Number(summary.rule_based_tasks || 0) > 0
          ? `最近已有 ${summary.rule_based_tasks} 次任务直接复用了学习档案，本地规则抽取开始替代部分 LLM 调用。`
          : "当前还没有触发规则复用；同站点重复任务积累后，会优先走本地规则抽取。",
      emphasis: false,
    },
    {
      title: "学习沉淀",
      body:
        Number(summary.learned_profile_hits || 0) > 0
          ? `最近已有 ${summary.learned_profile_hits} 次任务命中学习档案，系统正在形成站点级抽取记忆。`
          : "当前还没有命中学习档案；完成一次学习后，后续同类页面会更快进入规则复用。",
      emphasis: false,
    },
    {
      title: "运行状态",
      body:
        running > 0
          ? `当前有 ${running} 个任务正在运行，任务列表会自动刷新。`
          : "当前无运行中任务，可以继续提交新任务或回看历史变化。",
      emphasis: false,
    },
  ];

  if (latestTask) {
    insightItems.push({
      title: `最近任务：${latestTask.task_id}`,
      body: `状态：${statusLabel(latestTask.status)}，模式：${latestTask.schema_name}`,
      emphasis: false,
    });
  }
  if (topScenario) {
    insightItems.push({
      title: `高频场景：${topScenario.label}`,
      body: `当前该场景下已有 ${topScenario.count} 个监控方案，适合继续沉淀成标准模板。`,
      emphasis: false,
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
      const urlLabel = isBatch
        ? `批量任务 · ${Number(task.total_items || 0)} 个 URL`
        : task.url;
      const urlShort = urlLabel.length > 54 ? `${urlLabel.slice(0, 54)}...` : urlLabel;
      const progressText = formatTaskProgress(task);
      const modeLabel = isBatch
        ? `batch (${Number(task.completed_items || 0)}/${Number(task.total_items || 0)})`
        : (task.schema_name || "auto");
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

function renderInsightList(containerId, items, emptyText, renderer) {
  const container = document.getElementById(containerId);
  if (!container) {
    return;
  }
  if (!items || items.length === 0) {
    container.innerHTML = `<div class="insight-item"><p>${escHtml(emptyText)}</p></div>`;
    return;
  }
  container.innerHTML = items.map(renderer).join("");
}

function renderRecentChanges(insights) {
  renderInsightList(
    "recent-change-list",
    insights.recent_changes || [],
    "当前还没有检测到同 URL 的字段变化。",
    (item) => `
      <div class="insight-item">
        <span class="insight-title">${escHtml(item.domain || "unknown")} · ${escHtml(item.task_id)}</span>
        <p>${escHtml(item.summary || `发现 ${item.changed_fields_count} 个字段变化`)}</p>
      </div>
    `
  );
}

function renderDomainBoard(insights) {
  renderInsightList(
    "domain-board",
    insights.domain_leaderboard || [],
    "当前还没有足够的域名历史数据。",
    (item) => `
      <div class="insight-item">
        <span class="insight-title">${escHtml(item.domain)}</span>
        <p>最近 ${item.total} 个任务，成功率 ${escHtml(item.success_rate)}，最新任务 ${escHtml(item.latest_task_id)}</p>
      </div>
    `
  );
}

function currentSelectedFields() {
  return selectedFieldState
    .filter((item) => item.checked)
    .map((item) => item.name);
}

function parseNotifyOnValue(value) {
  return String(value || "")
    .split(",")
    .map((item) => item.trim().toLowerCase())
    .filter(Boolean);
}

function parseNotificationChannelsInput(value) {
  return String(value || "")
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter((line) => line && !line.startsWith("#"))
    .map((line) => {
      const [channelType, name, target, secret] = line.split("|").map((item) => item.trim());
      return {
        channel_type: channelType || "webhook",
        name: name || "",
        target: target || "",
        secret: secret || "",
        enabled: true,
      };
    })
    .filter((item) => item.target);
}

function dedupeNotificationChannels(channels) {
  const normalized = [];
  (Array.isArray(channels) ? channels : []).forEach((item) => {
    const channelType = String(item.channel_type || "webhook").trim().toLowerCase() || "webhook";
    const target = String(item.target || "").trim();
    if (!target) {
      return;
    }
    const existing = normalized.find(
      (entry) => entry.channel_type === channelType && entry.target === target
    );
    if (existing) {
      if (!existing.name && item.name) {
        existing.name = String(item.name || "").trim();
      }
      if (!existing.secret && item.secret) {
        existing.secret = String(item.secret || "").trim();
      }
      if (item.enabled !== false) {
        existing.enabled = true;
      }
      return;
    }
    normalized.push({
      channel_type: channelType,
      name: String(item.name || "").trim(),
      target,
      secret: String(item.secret || "").trim(),
      enabled: item.enabled !== false,
    });
  });
  return normalized;
}

function formatNotificationChannels(channels, primaryTarget) {
  return dedupeNotificationChannels(channels)
    .filter(
      (item) =>
        !(
          item.channel_type === "webhook" &&
          item.target === String(primaryTarget || "").trim()
        )
    )
    .map((item) =>
      [
        item.channel_type || "webhook",
        item.name || "",
        item.target || "",
        item.secret || "",
      ].join("|")
    )
    .join("\n");
}

function collectMonitorProfile() {
  const primaryWebhookUrl =
    document.getElementById("monitor-webhook-url")?.value.trim() || "";
  const advancedChannels = parseNotificationChannelsInput(
    document.getElementById("monitor-notification-channels")?.value || ""
  );
  const notificationChannels = dedupeNotificationChannels([
    ...(primaryWebhookUrl
      ? [
          {
            channel_type: "webhook",
            name: "默认 Webhook",
            target: primaryWebhookUrl,
            secret: "",
            enabled: true,
          },
        ]
      : []),
    ...advancedChannels,
  ]);
  return {
    scenario_label: document.getElementById("monitor-scenario-label")?.value.trim() || "",
    business_goal: document.getElementById("monitor-business-goal")?.value.trim() || "",
    alert_focus: document.getElementById("monitor-alert-focus")?.value.trim() || "",
    notify_on: parseNotifyOnValue(document.getElementById("monitor-notify-on")?.value || "changed,error"),
    summary_style: document.getElementById("monitor-summary-style")?.value || "brief",
    webhook_url: primaryWebhookUrl,
    notification_channels: notificationChannels,
    digest_enabled: Boolean(document.getElementById("monitor-digest-enabled")?.checked),
    digest_hour: Number(document.getElementById("monitor-digest-hour")?.value || 9),
  };
}

function collectMonitorSchedule() {
  return {
    schedule_enabled: Boolean(document.getElementById("monitor-schedule-enabled")?.checked),
    schedule_interval_minutes: Number(
      document.getElementById("monitor-schedule-interval")?.value || 60
    ),
  };
}

function applyMonitorProfile(profile = {}) {
  const normalizedNotifyOn = Array.isArray(profile.notify_on) && profile.notify_on.length
    ? profile.notify_on.join(",")
    : "changed,error";
  const notificationChannels = dedupeNotificationChannels(profile.notification_channels || []);
  const primaryWebhookUrl =
    String(profile.webhook_url || "").trim() ||
    (notificationChannels[0] && notificationChannels[0].target) ||
    "";
  setInputValue("monitor-scenario-label", profile.scenario_label || "");
  setInputValue("monitor-business-goal", profile.business_goal || "");
  setInputValue("monitor-alert-focus", profile.alert_focus || "");
  setInputValue("monitor-notify-on", normalizedNotifyOn);
  setInputValue("monitor-summary-style", profile.summary_style || "brief");
  setInputValue("monitor-webhook-url", primaryWebhookUrl);
  setInputValue(
    "monitor-notification-channels",
    formatNotificationChannels(notificationChannels, primaryWebhookUrl)
  );
  const digestEnabledInput = document.getElementById("monitor-digest-enabled");
  if (digestEnabledInput) {
    digestEnabledInput.checked = Boolean(profile.digest_enabled);
  }
  setInputValue(
    "monitor-digest-hour",
    Number.isFinite(Number(profile.digest_hour)) ? Number(profile.digest_hour) : 9
  );
}

function applyMonitorSchedule(schedule = {}) {
  const enabled = Boolean(schedule.schedule_enabled);
  const interval = Number(schedule.schedule_interval_minutes || 60);
  const scheduleEnabledInput = document.getElementById("monitor-schedule-enabled");
  if (scheduleEnabledInput) {
    scheduleEnabledInput.checked = enabled;
  }
  setInputValue("monitor-schedule-interval", interval > 0 ? interval : 60);
}

function inferCurrentPageType() {
  const text = document.getElementById("detected-page-type")?.textContent || "";
  const matched = text.split("：")[1] || "";
  return matched.trim() || "unknown";
}

function buildTemplatePayload(existingTemplateId = "") {
  return {
    template_id: existingTemplateId,
    name: "",
    url: document.getElementById("url")?.value.trim() || "",
    page_type: inferCurrentPageType(),
    schema_name: "auto",
    storage_format: document.getElementById("format")?.value || "json",
    use_static: document.getElementById("static-mode")?.value === "true",
    selected_fields: currentSelectedFields(),
    field_labels: latestFieldLabels || {},
    profile: collectMonitorProfile(),
  };
}

async function saveCurrentTemplate() {
  const payload = buildTemplatePayload();
  if (!payload.url) {
    showToast("请先输入 URL 再保存模板", "error");
    return;
  }
  if (!payload.selected_fields.length) {
    showToast("请先分析页面并至少勾选一个字段", "error");
    return;
  }

  const name = window.prompt("给这个模板起个名字：", `${payload.page_type || "page"} 模板`);
  if (!name) {
    return;
  }
  payload.name = name.trim();

  const response = await apiFetch("/api/templates", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
  const data = await response.json();
  if (!response.ok) {
    showToast(`保存模板失败：${data.detail || "未知错误"}`, "error");
    return;
  }
  showToast("模板已保存，可随时复用", "success");
  await dashboardAssetsModule.loadTemplates();
}

async function saveCurrentMonitor() {
  const payload = buildTemplatePayload();
  if (!payload.url) {
    showToast("请先输入 URL 再加入监控", "error");
    return;
  }
  if (!payload.selected_fields.length) {
    showToast("请先分析页面并勾选要追踪的字段", "error");
    return;
  }

  const defaultName = payload.profile.scenario_label || "页面变化监控";
  const name = window.prompt("给这个监控起个名字：", defaultName);
  if (!name) {
    return;
  }

  const response = await apiFetch("/api/monitors", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      monitor_id: "",
      name: name.trim(),
      url: payload.url,
      schema_name: payload.schema_name,
      storage_format: payload.storage_format,
      use_static: payload.use_static,
      selected_fields: payload.selected_fields,
      field_labels: payload.field_labels,
      profile: payload.profile,
      ...collectMonitorSchedule(),
    }),
  });
  const data = await response.json();
  if (!response.ok) {
    showToast(`保存监控失败：${data.detail || "未知错误"}`, "error");
    return;
  }
  showToast("监控已保存，可立即手动检查", "success");
  await dashboardAssetsModule.loadMonitors();
  await refreshDashboard();
}

async function parseNaturalLanguageTask() {
  const textarea = document.getElementById("nl-task-request");
  const result = document.getElementById("nl-task-result");
  const button = document.getElementById("parse-nl-task-btn");
  const requestText = textarea?.value.trim() || "";

  if (!requestText) {
    showToast("请先输入任务描述", "error");
    return;
  }

  button.disabled = true;
  button.innerHTML = '<span class="spinner"></span>解析中';
  result.style.display = "none";
  try {
    const response = await apiFetch("/api/nl_task", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ request_text: requestText }),
    });
    const data = await response.json();
    if (!response.ok) {
      throw new Error(data.detail || "解析失败");
    }
    dashboardAnalysis.applyNaturalLanguagePlan(data.plan || null);
    result.className = "submit-result success";
    result.textContent = `已生成草案：${data.plan?.task_type || "-"}`;
    result.style.display = "inline-flex";
    showToast("自然语言任务草案已生成", "success");
  } catch (error) {
    result.className = "submit-result error";
    result.textContent = `错误：${error.message}`;
    result.style.display = "inline-flex";
    showToast(`解析失败：${error.message}`, "error");
  } finally {
    button.disabled = false;
    button.textContent = "解析任务";
  }
}

function renderFieldSelector() {
  const container = document.getElementById("field-selector");
  if (!container) {
    return;
  }

  if (!selectedFieldState.length) {
    container.innerHTML = '<span class="empty-chip">候选字段将在这里显示</span>';
    return;
  }

  container.innerHTML = selectedFieldState
    .map((field) => {
      const label = latestFieldLabels[field.name] || field.name;
      return `
        <label class="field-chip ${field.checked ? "active" : ""}">
          <input type="checkbox" data-field="${escHtml(field.name)}" ${field.checked ? "checked" : ""}>
          <span>${escHtml(label)}</span>
        </label>
      `;
    })
    .join("");

  container.querySelectorAll("input[type='checkbox']").forEach((input) => {
    input.addEventListener("change", () => {
      selectedFieldState = selectedFieldState.map((field) =>
        field.name === input.dataset.field
          ? { ...field, checked: input.checked }
          : field
      );
      renderFieldSelector();
    });
  });
}

function resetSelectedFields() {
  selectedFieldState = latestAnalyzedFields.map((name) => ({ name, checked: false }));
  renderFieldSelector();
  setText("detected-page-type", latestAnalyzedFields.length ? "已恢复全自动模式" : "尚未分析页面");
}

function updateAnalysisPreview(text) {
  const preview = document.getElementById("analysis-preview");
  if (!preview) {
    return;
  }
  if (!text) {
    preview.classList.add("preview-empty");
    preview.textContent = "分析预览将在这里显示";
    return;
  }
  preview.classList.remove("preview-empty");
  preview.textContent = text;
}

function setAnalyzeProgress(value, text) {
  const panel = document.getElementById("analyze-progress-panel");
  const bar = document.getElementById("analyze-progress-bar");
  const percent = document.getElementById("analyze-progress-percent");
  const message = document.getElementById("analyze-progress-text");
  const track = document.querySelector(".analyze-progress-track");

  if (!panel || !bar || !percent || !message || !track) {
    return;
  }

  const numeric = Math.max(0, Math.min(100, Math.round(value)));
  analyzeProgressValue = numeric;
  panel.hidden = false;
  bar.style.width = `${numeric}%`;
  percent.textContent = `${numeric}%`;
  message.textContent = text;
  track.setAttribute("aria-valuenow", String(numeric));
}

function stopAnalyzeProgressTimer() {
  if (analyzeProgressTimer) {
    clearInterval(analyzeProgressTimer);
    analyzeProgressTimer = null;
  }
}

function startAnalyzeProgress() {
  stopAnalyzeProgressTimer();
  setAnalyzeProgress(8, "已发送请求，准备抓取并清洗页面...");

  analyzeProgressTimer = setInterval(() => {
    let nextValue = analyzeProgressValue;
    let nextText = "正在整理页面文本...";

    if (analyzeProgressValue < 28) {
      nextValue += 7;
      nextText = "正在抓取页面内容...";
    } else if (analyzeProgressValue < 56) {
      nextValue += 5;
      nextText = "正在清洗页面并提取可分析文本...";
    } else if (analyzeProgressValue < 82) {
      nextValue += 3;
      nextText = "正在分析候选字段与页面类型...";
    } else if (analyzeProgressValue < 92) {
      nextValue += 1;
      nextText = "正在生成分析预览...";
    } else {
      nextValue = 92;
      nextText = "分析结果即将返回...";
    }

    setAnalyzeProgress(nextValue, nextText);
  }, 450);
}

function finishAnalyzeProgress(text) {
  stopAnalyzeProgressTimer();
  setAnalyzeProgress(100, text);
}


function bindEvents() {
  document.querySelectorAll(".nav-item[data-section]").forEach((item) => {
    item.addEventListener("click", () => {
      showSection(item.dataset.section);
    });
  });

  document.querySelectorAll("[data-open-section]").forEach((item) => {
    item.addEventListener("click", () => {
      const section = item.dataset.openSection || "extract";
      showSection(section);
    });
  });

  document.querySelectorAll("[data-quick-template]").forEach((item) => {
    item.addEventListener("click", () => {
      const templateId = item.dataset.quickTemplate || "";
      if (!templateId) {
        return;
      }
      dashboardAssetsModule.applyMarketTemplate(templateId);
    });
  });

  const extractForm = document.getElementById("extract-form");
  if (extractForm) {
    extractForm.addEventListener("submit", submitExtract);
  }

  const batchButton = document.getElementById("batch-btn");
  if (batchButton) {
    batchButton.addEventListener("click", submitBatch);
  }

  const appendCurrentButton = document.getElementById("batch-append-current-btn");
  if (appendCurrentButton) {
    appendCurrentButton.addEventListener("click", appendCurrentUrlToBatch);
  }

  const loadWatchlistButton = document.getElementById("batch-load-watchlist-btn");
  if (loadWatchlistButton) {
    loadWatchlistButton.addEventListener("click", appendWatchlistToBatch);
  }

  const normalizeButton = document.getElementById("batch-normalize-btn");
  if (normalizeButton) {
    normalizeButton.addEventListener("click", normalizeBatchUrls);
  }

  const domainFilter = document.getElementById("batch-domain-filter");
  if (domainFilter) {
    domainFilter.addEventListener("change", () => {
      batchRelationFilterValue = domainFilter.value || "all";
      updateBatchRelationBoard(parseBatchUrls(document.getElementById("batch-urls")?.value || ""));
    });
  }

  const expandAllButton = document.getElementById("batch-expand-all-btn");
  if (expandAllButton) {
    expandAllButton.addEventListener("click", () => {
      collapsedBatchDomains = new Set();
      updateBatchRelationBoard(parseBatchUrls(document.getElementById("batch-urls")?.value || ""));
    });
  }

  const collapseAllButton = document.getElementById("batch-collapse-all-btn");
  if (collapseAllButton) {
    collapseAllButton.addEventListener("click", () => {
      const relations = buildBatchRelations(parseBatchUrls(document.getElementById("batch-urls")?.value || "").uniqueUrls);
      collapsedBatchDomains = new Set(relations.domains.map((entry) => entry.domain));
      updateBatchRelationBoard(parseBatchUrls(document.getElementById("batch-urls")?.value || ""));
    });
  }

  const batchTextarea = document.getElementById("batch-urls");
  if (batchTextarea) {
    batchTextarea.addEventListener("input", updateBatchSummary);
  }

  document.querySelectorAll('input[name="batch-submit-mode"]').forEach((input) => {
    input.addEventListener("change", setBatchGroupInputVisibility);
  });

  const taskBatchFilter = document.getElementById("task-batch-filter");
  if (taskBatchFilter) {
    taskBatchFilter.addEventListener("change", () => {
      activeTaskBatchFilter = taskBatchFilter.value || "all";
      refreshDashboard();
    });
  }

  const notificationFilter = document.getElementById("notification-filter");
  if (notificationFilter) {
    notificationFilter.addEventListener("change", () => {
      dashboardAssetsState.activeNotificationFilter =
        notificationFilter.value || "all";
      refreshDashboard();
    });
  }

  const notificationRefreshButton = document.getElementById(
    "notification-refresh-btn"
  );
  if (notificationRefreshButton) {
    notificationRefreshButton.addEventListener("click", () => {
      refreshDashboard();
    });
  }

  const learnedProfileFilter = document.getElementById("learned-profile-filter");
  if (learnedProfileFilter) {
    learnedProfileFilter.addEventListener("change", () => {
      learnedProfilesState.activeLearnedProfileFilter = learnedProfileFilter.value || "all";
      learnedProfilesModule.renderLearnedProfileBoard(learnedProfilesState.latestLearnedProfiles);
    });
  }

  const learnedProfileSearch = document.getElementById("learned-profile-search");
  if (learnedProfileSearch) {
    learnedProfileSearch.addEventListener("input", () => {
      learnedProfilesState.learnedProfileSearchKeyword = learnedProfileSearch.value || "";
      learnedProfilesModule.renderLearnedProfileBoard(learnedProfilesState.latestLearnedProfiles);
    });
  }
  const bulkDisableButton = document.getElementById("learned-profile-bulk-disable-btn");
  if (bulkDisableButton) {
    bulkDisableButton.addEventListener("click", learnedProfilesModule.bulkDisableRiskyLearnedProfiles);
  }
  const bulkRelearnButton = document.getElementById("learned-profile-bulk-relearn-btn");
  if (bulkRelearnButton) {
    bulkRelearnButton.addEventListener("click", learnedProfilesModule.bulkRelearnRiskyLearnedProfiles);
  }

  document.querySelectorAll("[data-close-learned-profile-drawer]").forEach((button) => {
    button.addEventListener("click", learnedProfilesModule.closeLearnedProfileDrawer);
  });
  const relearnButton = document.getElementById("learned-profile-relearn-btn");
  if (relearnButton) {
    relearnButton.addEventListener("click", () => {
      const profile = learnedProfilesState.activeLearnedProfileDetail && learnedProfilesState.activeLearnedProfileDetail.profile;
      const profileId = String((profile && profile.profile_id) || learnedProfilesState.activeLearnedProfileDetailId || "").trim();
      learnedProfilesModule.relearnLearnedProfile(profileId);
    });
  }
  const toggleButton = document.getElementById("learned-profile-toggle-btn");
  if (toggleButton) {
    toggleButton.addEventListener("click", () => {
      const profile = (learnedProfilesState.activeLearnedProfileDetail && learnedProfilesState.activeLearnedProfileDetail.profile) || {};
      const profileId = String(profile.profile_id || learnedProfilesState.activeLearnedProfileDetailId || "").trim();
      if (!profileId) {
        return;
      }
      if (profile.is_active) {
        learnedProfilesModule.disableLearnedProfile(profileId);
      } else {
        learnedProfilesModule.enableLearnedProfile(profileId);
      }
    });
  }
  const resetButton = document.getElementById("learned-profile-reset-btn");
  if (resetButton) {
    resetButton.addEventListener("click", () => {
      const profile = learnedProfilesState.activeLearnedProfileDetail && learnedProfilesState.activeLearnedProfileDetail.profile;
      const profileId = String((profile && profile.profile_id) || learnedProfilesState.activeLearnedProfileDetailId || "").trim();
      learnedProfilesModule.resetLearnedProfile(profileId);
    });
  }
  document.addEventListener("keydown", (event) => {
    if (event.key === "Escape") {
      learnedProfilesModule.closeLearnedProfileDrawer();
    }
  });

  const analyzeButton = document.getElementById("analyze-page-btn");
  if (analyzeButton) {
    analyzeButton.addEventListener("click", dashboardAnalysis.analyzePage);
  }

  const insightAnalyzeButton = document.getElementById("insight-analyze-page-btn");
  if (insightAnalyzeButton) {
    insightAnalyzeButton.addEventListener("click", dashboardAnalysis.analyzeInsightPage);
  }

  const compareAnalyzeButton = document.getElementById("compare-analyze-btn");
  if (compareAnalyzeButton) {
    compareAnalyzeButton.addEventListener("click", dashboardAnalysis.previewCompareTargets);
  }

  const exportBriefButton = document.getElementById("insight-export-brief-btn");
  if (exportBriefButton) {
    exportBriefButton.addEventListener("click", dashboardAnalysis.exportInsightBrief);
  }

  const exportMarkdownButton = document.getElementById("insight-export-markdown-btn");
  if (exportMarkdownButton) {
    exportMarkdownButton.addEventListener("click", dashboardAnalysis.exportInsightMarkdown);
  }

  const exportCsvButton = document.getElementById("insight-export-csv-btn");
  if (exportCsvButton) {
    exportCsvButton.addEventListener("click", dashboardAnalysis.exportInsightCsv);
  }

  const exportJsonButton = document.getElementById("insight-export-json-btn");
  if (exportJsonButton) {
    exportJsonButton.addEventListener("click", dashboardAnalysis.exportInsightJson);
  }

  document.querySelectorAll(".mode-tab[data-mode]").forEach((button) => {
    button.addEventListener("click", () => dashboardAnalysis.setInsightMode(button.dataset.mode));
  });

  const compareUrls = document.getElementById("compare-urls");
  if (compareUrls) {
    compareUrls.addEventListener("input", dashboardAnalysis.updateCompareUrlSummary);
  }

  const insightForm = document.getElementById("insight-form");
  if (insightForm) {
    insightForm.addEventListener("submit", dashboardAnalysis.submitInsightAnalysis);
  }

  const clearFieldsButton = document.getElementById("clear-fields-btn");
  if (clearFieldsButton) {
    clearFieldsButton.addEventListener("click", resetSelectedFields);
  }

  const saveTemplateButton = document.getElementById("save-template-btn");
  if (saveTemplateButton) {
    saveTemplateButton.addEventListener("click", saveCurrentTemplate);
  }

  const saveMonitorButton = document.getElementById("save-monitor-btn");
  if (saveMonitorButton) {
    saveMonitorButton.addEventListener("click", saveCurrentMonitor);
  }

  const basicConfigForm = document.getElementById("basic-config-form");
  if (basicConfigForm) {
    basicConfigForm.addEventListener("submit", saveBasicConfig);
  }

  const parseNlTaskButton = document.getElementById("parse-nl-task-btn");
  if (parseNlTaskButton) {
    parseNlTaskButton.addEventListener("click", parseNaturalLanguageTask);
  }

  const templateMarketFilter = document.getElementById("template-market-filter");
  if (templateMarketFilter) {
    templateMarketFilter.addEventListener("change", () => {
      dashboardAssetsState.activeTemplateMarketFilter = templateMarketFilter.value || "all";
      dashboardAssetsModule.renderMarketTemplateBoard(dashboardAssetsState.latestMarketTemplates);
      dashboardAssetsModule.renderTemplateBoard(dashboardAssetsState.latestTemplates);
    });
  }

  const nlApplyButton = document.getElementById("nl-apply-btn");
  if (nlApplyButton) {
    nlApplyButton.addEventListener("click", () => {
      if (!latestNlTaskPlan) {
        showToast("请先生成任务草案", "error");
        return;
      }
      dashboardAnalysis.applyNaturalLanguagePlan(latestNlTaskPlan);
      showToast("草案已重新应用到页面", "success");
    });
  }

  const nlRunTaskButton = document.getElementById("nl-run-task-btn");
  if (nlRunTaskButton) {
    nlRunTaskButton.addEventListener("click", dashboardAnalysis.runNaturalLanguagePlan);
  }

  const nlSaveMonitorButton = document.getElementById("nl-save-monitor-btn");
  if (nlSaveMonitorButton) {
    nlSaveMonitorButton.addEventListener("click", dashboardAnalysis.saveMonitorFromNaturalLanguagePlan);
  }

  const nlOpenCompareButton = document.getElementById("nl-open-compare-btn");
  if (nlOpenCompareButton) {
    nlOpenCompareButton.addEventListener("click", dashboardAnalysis.openCompareFromNaturalLanguagePlan);
  }

  const refreshBasicConfigButton = document.getElementById("refresh-basic-config-btn");
  if (refreshBasicConfigButton) {
    refreshBasicConfigButton.addEventListener("click", () => loadBasicConfig(true));
  }
}

async function submitExtract(event) {
  return dashboardTaskRuntime.submitExtract(event);
}

async function submitBatch() {
  return dashboardTaskRuntime.submitBatch();
}

function applyDashboardPayload(stats, tasks, insights) {
  return dashboardTaskRuntime.applyDashboardPayload(stats, tasks, insights);
}

async function refreshDashboard(options = {}) {
  return dashboardTaskRuntime.refreshDashboard(options);
}

function initDashboard() {
  initTheme();
  initApiTokenInput();
  bindEvents();
  setBatchGroupInputVisibility();
  const runtimeStatus = getRuntimeStatus();
  applyRuntimeStatus(runtimeStatus);
  showSection("overview");

  const initialStats = getInitialJson("initial-stats", {
    total: 0,
    success: 0,
    failed: 0,
    running: 0,
    pending: 0,
    success_rate: "0%",
  });
  const initialTasks = getInitialJson("initial-tasks", []);
  const initialInsights = getInitialJson("initial-insights", {
    summary: {
      repeat_urls: 0,
      changed_tasks: 0,
      active_monitors: 0,
      notification_ready_monitors: 0,
      notification_success_monitors: 0,
      rule_based_tasks: 0,
      fallback_tasks: 0,
      learned_profile_hits: 0,
      site_memory_saved_runs: 0,
      memory_ready_pages: 0,
      high_priority_alerts: 0,
      avg_quality: "-",
    },
    recent_changes: [],
    scenario_summary: [],
    domain_leaderboard: [],
    watchlist: [],
    monitor_alerts: [],
    monitors: [],
  });
  latestInsightsState = initialInsights;

  applyDashboardPayload(initialStats, initialTasks, initialInsights);
  loadBasicConfig();
  renderFieldSelector();
  updateAnalysisPreview("");
  dashboardAnalysis.setInsightMode("single");
  dashboardAnalysis.setInsightFieldPreview([], {});
  dashboardAnalysis.updateInsightPreview("");
  dashboardAnalysis.updateCompareUrlSummary();
  dashboardAnalysis.renderComparePreviewBoard([]);
  updateBatchSummary();
  dashboardAssetsModule.renderTemplateBoard([]);
  dashboardAssetsModule.renderMarketTemplateBoard([]);
  dashboardAssetsModule.renderMonitorBoard([]);
  dashboardAssetsModule.renderNotificationBoard([]);
  learnedProfilesModule.renderLearnedProfileSummary([], []);
  learnedProfilesModule.renderLearnedProfileBoard([]);
  dashboardAssetsModule.renderMonitorAlerts(initialInsights);
  dashboardAnalysis.renderNlTaskPreview(null);
  refreshDashboard();
  scheduleDashboardRefresh();
}

document.addEventListener("DOMContentLoaded", initDashboard);
