"use strict";

window.SmartExtractorTaskDetailRender = (() => {
  function escapeHtml(value) {
    return String(value)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;");
  }

  function formatQuality(value) {
    const numeric = Number(value || 0);
    return numeric > 0 ? `${(numeric * 100).toFixed(1)}%` : "-";
  }

  function formatElapsed(value) {
    const numeric = Number(value || 0);
    return numeric > 0 ? `${Math.round(numeric)} ms` : "-";
  }

  function normalizeFormattedText(value) {
    const text = String(value || "");
    if (!text.trim()) {
      return "";
    }

    const normalized = text
      .replace(/\r\n/g, "\n")
      .replace(/[\u00a0\u2000-\u200b\u3000]/g, " ")
      .replace(/[ \t]+\n/g, "\n")
      .replace(/\n{3,}/g, "\n\n")
      .replace(/[\t ]{2,}/g, " ")
      .replace(/^[ \t]+|[ \t]+$/gm, "")
      .replace(
        /\n\s*([A-Za-z\u4e00-\u9fa5][A-Za-z0-9_\-\u4e00-\u9fa5\s]{0,30}[：:])/g,
        "\n$1"
      )
      .trim();

    const lines = normalized.split("\n");
    const paragraphs = [];
    let buffer = [];

    for (const line of lines) {
      const trimmed = line.trim();
      if (!trimmed) {
        if (buffer.length) {
          paragraphs.push(buffer.join(" "));
          buffer = [];
        }
        continue;
      }

      if (/^[\-*=#]{3,}$/.test(trimmed)) {
        continue;
      }

      const compact = trimmed.replace(/\s+/g, " ");
      const isBullet =
        /^[\-•*]\s+/.test(compact) || /^\d+[.)、]\s+/.test(compact);
      const isHeading = /[：:]$/.test(compact) && compact.length <= 24;
      const isFieldLine =
        /^[A-Za-z\u4e00-\u9fa5][A-Za-z0-9_\-\u4e00-\u9fa5\s]{0,30}[：:]/.test(
          compact
        );

      if (isBullet || isHeading) {
        if (buffer.length) {
          paragraphs.push(buffer.join(" "));
          buffer = [];
        }
        paragraphs.push(compact);
        continue;
      }

      if (isFieldLine) {
        if (buffer.length) {
          paragraphs.push(buffer.join(" "));
          buffer = [];
        }

        const parts = compact.split(/([：:])/);
        const label = `${parts[0] || ""}${parts[1] || ""}`.trim();
        const rest = parts
          .slice(2)
          .join("")
          .replace(/\s+/g, " ")
          .trim();

        if (
          /(?:页面文本|正文|内容|详细说明|原文|描述|文章内容|全文|文本内容)[：:]$/.test(
            label
          ) &&
          rest
        ) {
          paragraphs.push(`${label}\n${rest}`);
        } else {
          paragraphs.push(rest ? `${label} ${rest}`.trim() : label);
        }
        continue;
      }

      buffer.push(compact);
    }

    if (buffer.length) {
      paragraphs.push(buffer.join(" "));
    }

    return paragraphs.join("\n\n").trim();
  }

  function renderHistory(items, labels) {
    const container = document.getElementById("recent-history-container");
    if (!container) {
      return;
    }

    if (!items || items.length === 0) {
      container.innerHTML = '<p class="plain-note">暂无同 URL 历史记录。</p>';
      return;
    }

    container.innerHTML = `
      <div class="history-list">
        ${items
          .map(
            (item) => `
              <div class="history-item">
                <div>
                  <strong>${escapeHtml(item.task_id)}</strong>
                  <p>${escapeHtml(item.created_at || "-")}</p>
                </div>
                <div class="history-meta">
                  <span class="badge badge-${escapeHtml(item.status)}">${escapeHtml(
              item.status
            )}</span>
                  <span>${escapeHtml(labels.formatQuality(item.quality_score))}</span>
                </div>
              </div>
            `
          )
          .join("")}
      </div>
    `;
  }

  function renderBatchChildren(items, labels) {
    const box = document.getElementById("batch-children-box");
    const container = document.getElementById("batch-children-container");
    if (!box || !container) {
      return;
    }

    if (!items || items.length === 0) {
      box.hidden = false;
      container.innerHTML =
        '<p class="plain-note">该批量任务下还没有子任务明细。</p>';
      return;
    }

    box.hidden = false;
    container.innerHTML = `
      <div class="history-list">
        ${items
          .map(
            (item) => `
              <div class="history-item">
                <div>
                  <strong>${escapeHtml(item.task_id)}</strong>
                  <p>${escapeHtml(item.url || "-")}</p>
                </div>
                <div class="history-meta">
                  <span class="badge badge-${escapeHtml(item.status)}">${escapeHtml(
              item.status
            )}</span>
                  <span>${escapeHtml(labels.formatQuality(item.quality_score))}</span>
                </div>
              </div>
            `
          )
          .join("")}
      </div>
    `;
  }

  function renderComparison(comparison) {
    const container = document.getElementById("comparison-container");
    if (!container) {
      return;
    }

    if (!comparison || !comparison.has_previous) {
      container.innerHTML =
        '<p class="plain-note">当前还没有可对比的上一条成功记录。</p>';
      return;
    }

    if (!comparison.changed) {
      container.innerHTML =
        '<div class="insight-item"><p>本次结果与上一次成功结果相比没有字段变化。</p></div>';
      return;
    }

    const changedFields = Array.isArray(comparison.changed_fields)
      ? comparison.changed_fields
      : [];
    const suggestedActions = Array.isArray(comparison.suggested_actions)
      ? comparison.suggested_actions
      : [];
    const breakdown = comparison.change_breakdown || {};
    container.innerHTML = `
      <div class="insight-list compact-list">
        ${
          comparison.impact_summary
            ? `
          <div class="insight-item emphasis">
            <span class="insight-title">变化摘要</span>
            <p>${escapeHtml(comparison.impact_summary)}</p>
            <p class="panel-note">更新 ${Number(
              breakdown.updated || 0
            )} 项 · 新增 ${Number(breakdown.added || 0)} 项 · 消失 ${Number(
                breakdown.removed || 0
              )} 项</p>
          </div>
        `
            : ""
        }
        ${changedFields
          .map(
            (item) => `
          <div class="insight-item">
            <span class="insight-title">${escapeHtml(
              item.label || item.field || "字段"
            )}</span>
            <p>${escapeHtml(item.summary || "")}</p>
            <p class="panel-note">之前：${escapeHtml(
              item.before_text || "空"
            )} / 现在：${escapeHtml(item.after_text || "空")}</p>
          </div>
        `
          )
          .join("")}
        ${suggestedActions
          .map(
            (item) => `
          <div class="insight-item">
            <span class="insight-title">建议动作</span>
            <p>${escapeHtml(item)}</p>
          </div>
        `
          )
          .join("")}
      </div>
    `;
  }

  function applyTaskDetail(detail, config) {
    const badge = document.getElementById("task-status-badge");
    const progressBar = document.getElementById("task-progress-bar");
    const progressText = document.getElementById("task-progress-text");
    const progressPercent = document.getElementById("task-progress-percent");
    const progressTrack = document.querySelector(".task-progress-track");
    const errorBox = document.getElementById("task-error-box");
    const errorText = document.getElementById("task-error-text");

    const progress = detail.progress || { percent: 0, stage: "" };
    const statusClass = config.statusLabels[detail.status] || "pending";

    document.getElementById("task-url").textContent = `URL: ${detail.url}`;
    document.getElementById("task-domain").textContent = `域名：${
      detail.domain || "unknown"
    }`;
    document.getElementById(
      "task-total-runs"
    ).textContent = `累计运行：${detail.history_summary.total_runs || 0}`;
    const kindLabel = document.getElementById("task-kind-label");
    if (kindLabel) {
      kindLabel.textContent =
        detail.task_kind === "batch"
          ? `类型：批量任务 (${Number(detail.completed_items || 0)}/${Number(
              detail.total_items || 0
            )})`
          : "类型：单任务";
    }
    document.getElementById("task-storage-format").textContent =
      detail.storage_format || "-";
    document.getElementById("task-quality-score").textContent = formatQuality(
      detail.quality_score
    );
    document.getElementById("task-elapsed-ms").textContent = formatElapsed(
      detail.elapsed_ms
    );
    document.getElementById("task-created-at").textContent =
      detail.created_at || "-";
    document.getElementById("task-success-runs").textContent = String(
      detail.history_summary.success_runs || 0
    );
    document.getElementById("task-extraction-strategy").textContent =
      detail.data && detail.data.extraction_strategy
        ? detail.data.extraction_strategy
        : "-";
    document.getElementById("task-learned-profile").textContent =
      detail.data && detail.data.learned_profile_id
        ? detail.data.learned_profile_id
        : "-";

    if (badge) {
      badge.className = `badge badge-${statusClass}`;
      badge.textContent = detail.status || "-";
    }
    if (progressBar) {
      progressBar.className = `task-progress-bar task-progress-bar-${statusClass}`;
      progressBar.style.width = `${Math.round(progress.percent || 0)}%`;
    }
    if (progressText) {
      progressText.textContent = progress.stage || "";
    }
    if (progressPercent) {
      progressPercent.textContent = `${Math.round(progress.percent || 0)}%`;
    }
    if (progressTrack) {
      progressTrack.setAttribute(
        "aria-valuenow",
        String(Math.round(progress.percent || 0))
      );
    }

    if (errorBox && errorText) {
      if (detail.error) {
        errorBox.hidden = false;
        errorText.textContent = detail.error;
      } else {
        errorBox.hidden = true;
        errorText.textContent = "";
      }
    }

    const formattedText = normalizeFormattedText(
      detail.data && detail.data.formatted_text
    );
    document.getElementById("task-formatted-text").textContent =
      formattedText || (detail.data ? "暂无润色文本" : "暂无结果数据");
    document.getElementById("task-raw-json").textContent = detail.data
      ? JSON.stringify(detail.data, null, 2)
      : "暂无结果数据";
    renderHistory(detail.recent_history || [], { formatQuality });
    renderComparison(detail.comparison || {});
    if (detail.task_kind === "batch") {
      renderBatchChildren(detail.batch_children || [], { formatQuality });
    } else {
      const batchBox = document.getElementById("batch-children-box");
      if (batchBox) {
        batchBox.hidden = true;
      }
    }
  }

  return {
    escapeHtml,
    formatQuality,
    formatElapsed,
    normalizeFormattedText,
    renderHistory,
    renderBatchChildren,
    renderComparison,
    applyTaskDetail,
  };
})();
