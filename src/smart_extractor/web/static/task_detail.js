"use strict";

const {
  getApiToken,
  setApiToken,
  showToast,
  initTheme,
  downloadTextFile,
  downloadBinaryFile,
} = window.SmartExtractorShared;
const { applyTaskDetail } = window.SmartExtractorTaskDetailRender;
const TASK_STATUS_LABELS = {
  success: "success",
  running: "running",
  failed: "failed",
  pending: "pending",
  queued: "pending",
};
let taskRefreshTimer = null;
let taskRefreshBackoffMs = 0;
const TASK_REFRESH_INTERVAL_MS = 5000;

async function fetchTaskDetail(taskId) {
  const token = getApiToken();
  if (!token) {
    return { error_type: "missing-token" };
  }

  const response = await fetch(`/api/task/${encodeURIComponent(taskId)}`, {
    headers: { "X-API-Token": token },
  });
  if (response.status === 401) {
    return { error_type: "invalid-token" };
  }
  if (response.status === 429) {
    return { error_type: "rate-limited" };
  }
  if (!response.ok) {
    return { error_type: "request-failed" };
  }
  return response.json();
}

function showTaskAuthMessage(message) {
  const box = document.getElementById("task-auth-box");
  const text = document.getElementById("task-auth-text");
  if (!box || !text) {
    return;
  }
  box.hidden = false;
  text.textContent = message;
}

function hideTaskAuthMessage() {
  const box = document.getElementById("task-auth-box");
  const text = document.getElementById("task-auth-text");
  if (!box || !text) {
    return;
  }
  box.hidden = true;
  text.textContent = "";
}

function initTaskTokenInput() {
  const input = document.getElementById("task-api-token");
  if (!input) {
    return;
  }
  input.value = getApiToken();
  input.addEventListener("change", () => {
    setApiToken(input.value);
    showTaskAuthMessage("Token 已更新，详情页会在下一次轮询时自动重试。");
  });
}

async function copyFormattedText() {
  const content = document.getElementById("task-formatted-text")?.textContent || "";
  const text = String(content || "").trim();
  if (!text || text === "暂无润色文本" || text === "暂无结果数据") {
    showToast("当前没有可复制的润色结果", "error");
    return;
  }

  try {
    await navigator.clipboard.writeText(text);
    showToast("润色结果已复制", "success");
  } catch (_) {
    showToast("复制失败，请手动选择文本后复制", "error");
  }
}

function buildTaskFileStamp() {
  const taskId = document.body.dataset.taskId || "task";
  return String(taskId).replace(/[^a-zA-Z0-9_-]+/g, "-");
}

function downloadFormattedText() {
  const content = document.getElementById("task-formatted-text")?.textContent || "";
  const text = String(content || "").trim();
  if (!text || text === "暂无润色文本" || text === "暂无结果数据") {
    showToast("当前没有可下载的润色结果", "error");
    return;
  }

  downloadTextFile(`${buildTaskFileStamp()}-formatted.txt`, `${text}\n`);
  showToast("润色结果已下载", "success");
}

function downloadRawJson() {
  const content = document.getElementById("task-raw-json")?.textContent || "";
  const text = String(content || "").trim();
  if (!text || text === "暂无结果数据") {
    showToast("当前没有可下载的 JSON 数据", "error");
    return;
  }

  downloadTextFile(
    `${buildTaskFileStamp()}-raw.json`,
    `${text}\n`,
    "application/json;charset=utf-8"
  );
  showToast("JSON 数据已下载", "success");
}

function initCopyAction() {
  const button = document.getElementById("copy-formatted-text-btn");
  if (!button) {
    return;
  }
  button.addEventListener("click", copyFormattedText);
}

function initDownloadActions() {
  const formattedButton = document.getElementById("download-formatted-text-btn");
  if (formattedButton) {
    formattedButton.addEventListener("click", downloadFormattedText);
  }

  const jsonButton = document.getElementById("download-raw-json-btn");
  if (jsonButton) {
    jsonButton.addEventListener("click", downloadRawJson);
  }

  const docxButton = document.getElementById("download-docx-btn");
  if (docxButton) {
    docxButton.addEventListener("click", async () => {
      try {
        const taskStamp = buildTaskFileStamp();
        await downloadBinaryFile(
          `/api/task/${encodeURIComponent(taskStamp)}/export?format=docx`,
          `${taskStamp}.docx`
        );
        showToast("Word 导出已开始", "success");
      } catch (error) {
        showToast(error.message, "error");
      }
    });
  }

  const xlsxButton = document.getElementById("download-xlsx-btn");
  if (xlsxButton) {
    xlsxButton.addEventListener("click", async () => {
      try {
        const taskStamp = buildTaskFileStamp();
        await downloadBinaryFile(
          `/api/task/${encodeURIComponent(taskStamp)}/export?format=xlsx`,
          `${taskStamp}.xlsx`
        );
        showToast("Excel 导出已开始", "success");
      } catch (error) {
        showToast(error.message, "error");
      }
    });
  }
}


function initTaskDetail() {
  const taskId = document.body.dataset.taskId;
  if (!taskId) {
    return;
  }

  initTheme();
  initTaskTokenInput();
  initCopyAction();
  initDownloadActions();

  let stopped = false;

  const refresh = async () => {
    if (stopped) {
      return;
    }

    const detail = await fetchTaskDetail(taskId);
    if (detail && detail.error_type === "rate-limited") {
      showTaskAuthMessage("详情页请求过于频繁，请稍后重试。");
      return;
    }
    if (!detail || detail.error_type === "request-failed") {
      showTaskAuthMessage("详情页暂时无法刷新，请稍后重试。若已启用鉴权，也请确认 Token 是否正确。");
      return;
    }

    if (detail.error_type === "missing-token") {
      showTaskAuthMessage("当前详情页未检测到 API Token。若后端启用了鉴权，请先在左侧输入 Token。");
      return;
    }

    if (detail.error_type === "invalid-token") {
      showTaskAuthMessage("当前 API Token 无效，详情页无法继续刷新。请更新左侧 Token 后重试。");
      return;
    }

    hideTaskAuthMessage();
    applyTaskDetail(detail, { statusLabels: TASK_STATUS_LABELS });
    if (detail.status === "success" || detail.status === "failed") {
      stopped = true;
      if (taskRefreshTimer) {
        clearInterval(taskRefreshTimer);
      }
    }
  };

  refresh();
  taskRefreshTimer = setInterval(refresh, TASK_REFRESH_INTERVAL_MS);
  window.addEventListener("beforeunload", () => clearInterval(taskRefreshTimer), { once: true });
}

document.addEventListener("DOMContentLoaded", initTaskDetail);
