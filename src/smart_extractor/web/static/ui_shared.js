"use strict";

window.SmartExtractorShared = (() => {
  const API_TOKEN_STORAGE_KEY = "smart_extractor_api_token";
  const SESSION_TOKEN_STORAGE_KEY = "smart_extractor_session_token";
  const THEME_STORAGE_KEY = "smart_extractor_ui_theme";

  function showToast(message, type = "info") {
    let container = document.getElementById("toast-container");
    if (!container) {
      container = document.createElement("div");
      container.id = "toast-container";
      container.className = "toast-container";
      document.body.appendChild(container);
    }

    const toast = document.createElement("div");
    toast.className = `toast toast-${type}`;
    toast.textContent = message;
    container.appendChild(toast);

    requestAnimationFrame(() => {
      requestAnimationFrame(() => toast.classList.add("show"));
    });

    setTimeout(() => {
      toast.classList.remove("show");
      setTimeout(() => toast.remove(), 220);
    }, 2600);
  }

  function getApiToken() {
    return localStorage.getItem(API_TOKEN_STORAGE_KEY) || "";
  }

  function getSessionToken() {
    return localStorage.getItem(SESSION_TOKEN_STORAGE_KEY) || "";
  }

  function setApiToken(token) {
    const value = String(token || "").trim();
    if (value) {
      localStorage.setItem(API_TOKEN_STORAGE_KEY, value);
    } else {
      localStorage.removeItem(API_TOKEN_STORAGE_KEY);
    }
  }

  function setSessionToken(token) {
    const value = String(token || "").trim();
    if (value) {
      localStorage.setItem(SESSION_TOKEN_STORAGE_KEY, value);
    } else {
      localStorage.removeItem(SESSION_TOKEN_STORAGE_KEY);
    }
  }

  function getAuthHeaders(extraHeaders = {}) {
    const headers = { ...extraHeaders };
    const sessionToken = getSessionToken();
    if (sessionToken) {
      headers.Authorization = `Bearer ${sessionToken}`;
    }
    const token = getApiToken();
    if (token) {
      headers["X-API-Token"] = token;
    }
    return headers;
  }

  function setTheme(theme) {
    document.body.dataset.theme = theme;
    localStorage.setItem(THEME_STORAGE_KEY, theme);
    document.querySelectorAll(".theme-option").forEach((button) => {
      button.classList.toggle("active", button.dataset.theme === theme);
    });
  }

  function initTheme(defaultTheme = "aurora") {
    const savedTheme = localStorage.getItem(THEME_STORAGE_KEY) || defaultTheme;
    setTheme(savedTheme);
    document.querySelectorAll(".theme-option").forEach((button) => {
      button.addEventListener("click", () => setTheme(button.dataset.theme));
    });
  }

  function downloadTextFile(
    fileName,
    content,
    mimeType = "text/plain;charset=utf-8"
  ) {
    const blob = new Blob([content], { type: mimeType });
    const link = document.createElement("a");
    const href = URL.createObjectURL(blob);
    link.href = href;
    link.download = fileName;
    document.body.appendChild(link);
    link.click();
    link.remove();
    setTimeout(() => URL.revokeObjectURL(href), 0);
  }

  async function downloadBinaryFile(url, fileName) {
    const response = await fetch(url, {
      headers: getAuthHeaders(),
    });
    if (!response.ok) {
      throw new Error(
        response.status === 401 ? "鉴权失败，请检查 API Token" : "下载失败"
      );
    }

    const blob = await response.blob();
    const link = document.createElement("a");
    const href = URL.createObjectURL(blob);
    link.href = href;
    link.download = fileName;
    document.body.appendChild(link);
    link.click();
    link.remove();
    setTimeout(() => URL.revokeObjectURL(href), 0);
  }

  function escapeHtml(value) {
    return String(value)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;");
  }

  function diagnosisCategoryLabel(category) {
    const normalized = String(category || "").trim().toLowerCase();
    if (normalized === "403") return "403";
    if (normalized === "captcha") return "验证码";
    if (normalized === "timeout") return "超时";
    if (normalized === "missing_fields") return "字段缺失";
    if (normalized === "model_error") return "模型异常";
    return "任务失败";
  }

  function diagnosisActions(category, fallbackSuggestion) {
    const catalog = {
      "403": [
        "切换代理或代理分组，确认出口 IP 未被目标站限制。",
        "降低站点级并发和访问频率，必要时加入冷却时间。",
        "检查请求头、登录态、robots/条款限制后再重试。",
      ],
      captcha: [
        "启用代理池和会话/profile 池化，降低触发挑战页的概率。",
        "将该站点加入验证码/挑战页恢复策略，再手动复核一次。",
        "必要时转人工处理登录态或授权流程。",
      ],
      timeout: [
        "提高 fetch 或 Playwright 超时时间并重试。",
        "若页面静态可读，切换为静态抓取。",
        "动态页延长关键选择器等待时间，检查站点响应速度。",
      ],
      missing_fields: [
        "发起字段级人工反馈，标记正确、错误和缺失字段。",
        "检查模板字段选择器或字段提示词，补充缺失字段样例。",
        "重新运行任务，让反馈沉淀到模板评分和站点记忆。",
      ],
      model_error: [
        "检查模型 API Key、base_url、模型名和限流状态。",
        "降低并发，启用重试，并保留原始响应用于排查。",
        "确认模型输出 JSON/schema 格式满足当前字段结构。",
      ],
      unknown: [
        "查看任务详情和运行日志，先确认网络、代理、站点限速和模型配置。",
        "用相同 URL 手动重试一次，并记录复现条件。",
      ],
    };
    const normalized = String(category || "unknown").trim().toLowerCase();
    return catalog[normalized] || [fallbackSuggestion || "查看任务详情和运行日志后重试。"];
  }

  function diagnosisBadgeClass(severity) {
    const normalized = String(severity || "").trim().toLowerCase();
    if (normalized === "danger" || normalized === "critical") return "badge-failed";
    if (normalized === "warning") return "badge-running";
    return "badge-pending";
  }

  function showFailureDiagnosis(diagnosis) {
    const dialog = document.getElementById("failure-diagnosis-dialog");
    if (!dialog) return;
    const payload = diagnosis || {};
    const category = String(payload.category || "unknown").trim().toLowerCase();
    const title = document.getElementById("failure-diagnosis-title");
    const categoryBadge = document.getElementById("failure-diagnosis-category");
    const message = document.getElementById("failure-diagnosis-message");
    const actions = document.getElementById("failure-diagnosis-actions");
    if (title) title.textContent = payload.title || "任务失败诊断";
    if (categoryBadge) {
      categoryBadge.className = `badge ${diagnosisBadgeClass(payload.severity)}`;
      categoryBadge.textContent = diagnosisCategoryLabel(category);
    }
    if (message) {
      message.textContent = payload.message || payload.suggestion || "暂无错误详情，建议查看运行日志。";
    }
    if (actions) {
      actions.innerHTML = diagnosisActions(category, payload.suggestion)
        .map(
          (item) => `
            <div class="insight-item">
              <span class="insight-title">处理动作</span>
              <p>${escapeHtml(item)}</p>
            </div>
          `
        )
        .join("");
    }
    dialog.hidden = false;
    document.body.classList.add("diagnosis-open");
  }

  function hideFailureDiagnosis() {
    const dialog = document.getElementById("failure-diagnosis-dialog");
    if (!dialog) return;
    dialog.hidden = true;
    document.body.classList.remove("diagnosis-open");
  }

  function initFailureDiagnosisDialog() {
    document.querySelectorAll("[data-close-failure-diagnosis]").forEach((item) => {
      item.addEventListener("click", hideFailureDiagnosis);
    });
    document.addEventListener("keydown", (event) => {
      if (event.key === "Escape") {
        hideFailureDiagnosis();
      }
    });
  }

  return {
    API_TOKEN_STORAGE_KEY,
    SESSION_TOKEN_STORAGE_KEY,
    THEME_STORAGE_KEY,
    showToast,
    getApiToken,
    getSessionToken,
    setApiToken,
    setSessionToken,
    getAuthHeaders,
    setTheme,
    initTheme,
    downloadTextFile,
    downloadBinaryFile,
    showFailureDiagnosis,
    hideFailureDiagnosis,
    initFailureDiagnosisDialog,
  };
})();
