"use strict";

window.SmartExtractorDashboardAssets = function createDashboardAssets(deps) {
  const {
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
    state,
  } = deps;

  function formatExtractionStrategy(strategy) {
    const normalized = String(strategy || "").trim().toLowerCase();
    if (normalized === "rule") return "规则复用";
    if (normalized === "llm") return "LLM 学习";
    if (normalized === "fallback") return "回退抽取";
    if (normalized === "rule_fallback") return "规则回退";
    if (!normalized || normalized === "unknown") return "暂无记录";
    return normalized;
  }

  function alertLevelLabel(level) {
    const normalized = String(level || "").trim().toLowerCase();
    if (normalized === "changed") return "检测到变化";
    if (normalized === "stable") return "页面稳定";
    if (normalized === "error") return "执行失败";
    return "等待检查";
  }

  function severityBadgeClass(level) {
    const normalized = String(level || "").trim().toLowerCase();
    if (normalized === "critical") return "badge-failed";
    if (normalized === "high") return "badge-running";
    if (normalized === "medium") return "badge-pending";
    return "badge-success";
  }

  function notificationStatusBadgeClass(status) {
    const normalized = String(status || "").trim().toLowerCase();
    if (normalized === "retry_pending") return "badge-running";
    if (normalized === "failed") return "badge-failed";
    if (normalized === "skipped") return "badge-pending";
    return "badge-success";
  }

  function summaryStyleLabel(style) {
    const normalized = String(style || "").trim().toLowerCase();
    if (normalized === "report") return "汇报型";
    if (normalized === "ops") return "运营型";
    return "摘要型";
  }

  function joinParts(parts) {
    return parts.filter(Boolean).join(" · ");
  }

  function monitorScheduleSummary(item) {
    if (!item || !item.schedule_enabled) {
      return "自动巡检：已关闭";
    }
    return joinParts([
      `自动巡检：${item.schedule_status_label || "运行中"}`,
      item.schedule_interval_label || "每 60 分钟",
      item.schedule_next_run_at ? `下次 ${item.schedule_next_run_at}` : "",
      item.schedule_paused_at ? `暂停于 ${item.schedule_paused_at}` : "",
    ]);
  }

  function monitorDispatchSummary(item) {
    return joinParts([
      item.schedule_claimed_by ? `调度者：${item.schedule_claimed_by}` : "",
      item.schedule_last_error ? `最近调度错误：${item.schedule_last_error}` : "",
    ]);
  }

  function monitorLastRunSummary(item) {
    return joinParts([
      `最近触发：${item.last_trigger_source_label || "暂无"}`,
      item.schedule_last_run_at || item.last_checked_at || "",
    ]);
  }

  function applyTemplatePayload(template, urlValue) {
    setInputValue("url", urlValue || "");
    setInputValue("format", template.storage_format || "json");
    setInputValue("static-mode", template.use_static ? "true" : "false");
    state.latestFieldLabels = template.field_labels || {};
    state.latestAnalyzedFields = Array.isArray(template.selected_fields)
      ? template.selected_fields
      : [];
    state.selectedFieldState = state.latestAnalyzedFields.map((name) => ({
      name,
      checked: true,
    }));
    renderFieldSelector();
    applyMonitorProfile(template.profile || {});
    setText("detected-page-type", `页面类型：${template.page_type || "unknown"}`);
  }

  function renderTemplateBoard(templates) {
    const filteredTemplates = (templates || []).filter((item) => {
      if (state.activeTemplateMarketFilter === "all") {
        return true;
      }
      const pageType = String(item.page_type || "").toLowerCase();
      const fields = (item.selected_fields || []).join(",").toLowerCase();
      if (state.activeTemplateMarketFilter === "monitor") {
        return /price|stock|brand|summary|change/.test(fields);
      }
      if (state.activeTemplateMarketFilter === "compare") {
        return /salary|price|requirements|summary/.test(fields);
      }
      if (state.activeTemplateMarketFilter === "batch") {
        return item.storage_format === "csv";
      }
      return pageType.includes(state.activeTemplateMarketFilter);
    });

    renderInsightList(
      "template-board",
      filteredTemplates,
      "当前还没有保存模板。先分析页面并勾选字段，再保存为模板即可复用。",
      (item) => {
        const selectedFields = (item.selected_fields || []).join("、") || "全自动";
        return `
          <div class="insight-item insight-item-row">
            <div>
              <span class="insight-title">${escHtml(item.name || item.template_id)}</span>
              <p>${escHtml((item.profile && item.profile.business_goal) || selectedFields)}</p>
              <p class="panel-note">${escHtml(joinParts([
                "我的模板",
                (item.profile && item.profile.scenario_label) || "通用方案",
                item.page_type || "unknown",
                item.storage_format || "json",
              ]))}</p>
              <p class="panel-note">${escHtml(
                `默认字段：${selectedFields} · 输出风格：${summaryStyleLabel(
                  item.profile && item.profile.summary_style
                )}`
              )}</p>
            </div>
            <div class="insight-side">
              <strong>${escHtml(
                (item.profile && item.profile.scenario_label) || item.page_type || "unknown"
              )}</strong>
              <button class="btn btn-ghost btn-inline" type="button" data-apply-template="${escHtml(
                item.template_id
              )}">套用</button>
            </div>
          </div>
        `;
      }
    );

    document.querySelectorAll("[data-apply-template]").forEach((button) => {
      button.addEventListener("click", () => applyTemplate(button.dataset.applyTemplate || ""));
    });
  }

  function renderMarketTemplateBoard(templates) {
    const filteredTemplates = (templates || []).filter((item) => {
      return (
        state.activeTemplateMarketFilter === "all" ||
        item.category === state.activeTemplateMarketFilter
      );
    });

    renderInsightList(
      "market-template-board",
      filteredTemplates,
      "当前筛选条件下还没有精选模板。",
      (item) => {
        const targetUsers = (item.target_users || []).join("、") || "通用团队";
        const outputs = (item.default_outputs || []).join("、") || "变化摘要";
        return `
          <div class="insight-item">
            <div class="insight-item-row">
              <div>
                <span class="insight-title">${escHtml(item.name || item.template_id)}</span>
                <p>${escHtml(item.description || "")}</p>
                <p class="panel-note">${escHtml(
                  joinParts([
                    "精选方案",
                    item.category || "-",
                    (item.tags || []).map((tag) => `#${tag}`).join(" "),
                  ])
                )}</p>
                <p class="panel-note">${escHtml(
                  `适合：${targetUsers} · 默认输出：${outputs}`
                )}</p>
              </div>
              <div class="insight-side">
                <strong>${escHtml(item.page_type || "unknown")}</strong>
              </div>
            </div>
            <div class="detail-action-row">
              <button class="btn btn-ghost btn-inline" type="button" data-install-market-template="${escHtml(
                item.template_id
              )}">安装到我的模板</button>
              <button class="btn btn-ghost btn-inline" type="button" data-apply-market-template="${escHtml(
                item.template_id
              )}">直接套用</button>
            </div>
          </div>
        `;
      }
    );

    document.querySelectorAll("[data-install-market-template]").forEach((button) => {
      button.addEventListener("click", () =>
        installMarketTemplate(button.dataset.installMarketTemplate || "")
      );
    });
    document.querySelectorAll("[data-apply-market-template]").forEach((button) => {
      button.addEventListener("click", () =>
        applyMarketTemplate(button.dataset.applyMarketTemplate || "")
      );
    });
  }

  function renderMonitorAlerts(insights) {
    renderInsightList(
      "monitor-alert-board",
      (insights && insights.monitor_alerts) || [],
      "当前还没有监控告警。保存监控并执行检查后，这里会显示变化或失败提醒。",
      (item) => `
        <div class="insight-item">
          <div class="insight-item-row">
            <div>
              <span class="insight-title">${escHtml(item.name || item.monitor_id)}</span>
              <p>${escHtml(item.business_summary || item.last_alert_message || "暂无告警详情")}</p>
              <p class="panel-note">${escHtml(
                joinParts([
                  (item.recommended_actions || [])[0] || "暂无建议动作",
                  `通知状态：${item.notification_status_label || item.last_notification_status || "未发送"}`,
                ])
              )}</p>
            </div>
            <div class="insight-side">
              <strong>${escHtml(item.alert_label || alertLevelLabel(item.last_alert_level))}</strong>
              <span class="badge ${severityBadgeClass(item.severity)}">${escHtml(
                item.severity_label || "常规关注"
              )}</span>
            </div>
          </div>
        </div>
      `
    );
  }

  function renderMonitorBoard(monitors) {
    renderInsightList(
      "monitor-board",
      monitors || [],
      "当前还没有保存监控。把 URL 加入监控后，就能持续检查页面变化。",
      (item) => {
        const summary = item.business_summary || (item.profile && item.profile.business_goal) || "未设置业务目标";
        const scenario = (item.profile && item.profile.scenario_label) || "通用监控";
        const focus = (item.profile && item.profile.alert_focus) || "未设置重点";
        const extraction = formatExtractionStrategy(item.last_extraction_strategy || "unknown");
        const learnedProfile = item.last_learned_profile_id
          ? ` · 站点记忆：${item.last_learned_profile_id}${
              item.learned_profile ? ` (${item.learned_profile.status_label || ""})` : ""
            }`
          : "";
        const siteMemory = item.site_memory
          ? `站点记忆 ${item.site_memory.memory_strength_label || ""} · 稳定率 ${Math.round(
              Number(item.site_memory.stability_rate || 0) * 100
            )}%`
          : "";
        const notificationSummary = joinParts([
          siteMemory,
          `通知：${item.notification_status_label || item.last_notification_status || "未发送"}`,
          item.last_notification_at || "",
          item.notification_channel_count ? `通道 ${item.notification_channel_count}` : "",
        ]);
        return `
          <div class="insight-item insight-item-row">
            <div>
              <span class="insight-title">${escHtml(item.name || item.monitor_id)}</span>
              <p class="line-clamp">${escHtml(item.url || "")}</p>
              <p class="panel-note">${escHtml(summary)}</p>
              <p class="panel-note">${escHtml(`场景：${scenario} · 关注：${focus}`)}</p>
              <p class="panel-note">${escHtml(`抽取策略：${extraction}${learnedProfile}`)}</p>
              <p class="panel-note">${escHtml(notificationSummary)}</p>
              <p class="panel-note">${escHtml(monitorScheduleSummary(item))}</p>
              ${
                monitorDispatchSummary(item)
                  ? `<p class="panel-note">${escHtml(monitorDispatchSummary(item))}</p>`
                  : ""
              }
              <p class="panel-note">${escHtml(monitorLastRunSummary(item))}</p>
              <p class="panel-note">${escHtml(
                `建议动作：${(item.recommended_actions || [])[0] || "继续观察"}`
              )}</p>
            </div>
            <div class="insight-side">
              <strong>${escHtml(item.alert_label || alertLevelLabel(item.last_alert_level))}</strong>
              <span class="badge ${severityBadgeClass(item.severity)}">${escHtml(
                item.severity_label || "常规关注"
              )}</span>
              <button class="btn btn-ghost btn-inline" type="button" data-run-monitor="${escHtml(
                item.monitor_id
              )}">立即检查</button>
              ${
                item.schedule_enabled
                  ? item.schedule_status === "paused"
                    ? `<button class="btn btn-soft btn-inline" type="button" data-resume-monitor="${escHtml(
                        item.monitor_id
                      )}">恢复巡检</button>`
                    : `<button class="btn btn-soft btn-inline" type="button" data-pause-monitor="${escHtml(
                        item.monitor_id
                      )}">暂停巡检</button>`
                  : ""
              }
            </div>
          </div>
        `;
      }
    );

    document.querySelectorAll("[data-run-monitor]").forEach((button) => {
      button.addEventListener("click", () => runMonitor(button.dataset.runMonitor || ""));
    });
    document.querySelectorAll("[data-pause-monitor]").forEach((button) => {
      button.addEventListener("click", () => pauseMonitor(button.dataset.pauseMonitor || ""));
    });
    document.querySelectorAll("[data-resume-monitor]").forEach((button) => {
      button.addEventListener("click", () => resumeMonitor(button.dataset.resumeMonitor || ""));
    });
  }

  function renderNotificationBoard(notifications) {
    const items = Array.isArray(notifications) ? notifications : [];
    setText("notification-total", items.length);
    setText("notification-retry-pending", items.filter((item) => item.status === "retry_pending").length);
    setText("notification-failed", items.filter((item) => item.status === "failed").length);
    setText("notification-sent", items.filter((item) => item.status === "sent").length);

    renderInsightList(
      "notification-board",
      items,
      "当前还没有通知记录。保存监控并触发检查后，这里会看到通知历史。",
      (item) => `
        <div class="insight-item">
          <div class="insight-item-row">
            <div>
              <span class="insight-title">${escHtml(item.monitor_id || item.notification_id || "通知事件")}</span>
              <p>${escHtml(item.status_message || item.error_message || "暂无通知详情")}</p>
              <p class="panel-note">${escHtml(
                joinParts([
                  `触发方式：${item.triggered_by_label || item.triggered_by || "系统触发"}`,
                  `通道：${item.channel_type || "webhook"}`,
                  item.response_code ? `响应码：${item.response_code}` : "",
                ])
              )}</p>
              <p class="panel-note">${escHtml(
                joinParts([
                  `目标：${item.target || "未配置"}`,
                  item.task_id ? `任务：${item.task_id}` : "",
                  item.next_retry_at ? `下次重试：${item.next_retry_at}` : "",
                ])
              )}</p>
              <p class="panel-note">${escHtml(
                joinParts([
                  `创建时间：${item.created_at || "-"}`,
                  item.retry_of_notification_id ? `来源：${item.retry_of_notification_id}` : "",
                ])
              )}</p>
            </div>
            <div class="insight-side">
              <strong>${escHtml(item.status_label || item.status || "-")}</strong>
              <span class="badge ${notificationStatusBadgeClass(item.status)}">${escHtml(
                item.status_label || item.status || "-"
              )}</span>
              ${
                item.can_resend
                  ? `<button class="btn btn-ghost btn-inline" type="button" data-resend-notification="${escHtml(
                      item.notification_id
                    )}">手动补发</button>`
                  : ""
              }
            </div>
          </div>
        </div>
      `
    );

    document.querySelectorAll("[data-resend-notification]").forEach((button) => {
      button.addEventListener("click", () =>
        resendNotification(button.dataset.resendNotification || "")
      );
    });
  }

  async function loadTemplates(showFeedback = false) {
    const data = await fetchJsonOrNull("/api/templates");
    if (!data) {
      if (showFeedback) showToast("读取模板列表失败", "error");
      return;
    }
    state.latestTemplates = Array.isArray(data.templates) ? data.templates : [];
    renderTemplateBoard(state.latestTemplates);
    if (showFeedback) showToast("模板列表已刷新", "success");
  }

  async function loadMarketTemplates(showFeedback = false) {
    const data = await fetchJsonOrNull("/api/template_market");
    if (!data) {
      if (showFeedback) showToast("读取模板市场失败", "error");
      return;
    }
    state.latestMarketTemplates = Array.isArray(data.templates) ? data.templates : [];
    renderMarketTemplateBoard(state.latestMarketTemplates);
    if (showFeedback) showToast("模板市场已刷新", "success");
  }

  async function loadMonitors(showFeedback = false) {
    const data = await fetchJsonOrNull("/api/monitors");
    if (!data) {
      if (showFeedback) showToast("读取监控列表失败", "error");
      return;
    }
    state.latestMonitors = Array.isArray(data.monitors) ? data.monitors : [];
    renderMonitorBoard(state.latestMonitors);
    if (showFeedback) showToast("监控列表已刷新", "success");
  }

  async function loadNotifications(showFeedback = false) {
    const query =
      state.activeNotificationFilter && state.activeNotificationFilter !== "all"
        ? `?limit=12&status=${encodeURIComponent(state.activeNotificationFilter)}`
        : "?limit=12";
    const data = await fetchJsonOrNull(`/api/notifications${query}`);
    if (!data) {
      if (showFeedback) showToast("读取通知列表失败", "error");
      return;
    }
    state.latestNotifications = Array.isArray(data.notifications) ? data.notifications : [];
    renderNotificationBoard(state.latestNotifications);
    if (showFeedback) showToast("通知列表已刷新", "success");
  }

  function applyTemplate(templateId) {
    const template = state.latestTemplates.find((item) => item.template_id === templateId);
    if (!template) {
      showToast("未找到对应模板", "error");
      return;
    }
    applyTemplatePayload(template, template.url || "");
    setText(
      "field-selector-help",
      `已套用模板《${template.name || template.template_id}》，可以直接提交或微调字段。`
    );
    showSection("extract");
    showToast("模板已套用到当前表单", "success");
  }

  function applyMarketTemplate(templateId) {
    const template = state.latestMarketTemplates.find((item) => item.template_id === templateId);
    if (!template) {
      showToast("未找到精选模板", "error");
      return;
    }
    applyTemplatePayload(template, template.sample_url || "");
    setText(
      "field-selector-help",
      `已套用精选模板《${template.name || template.template_id}》，可以直接提交或微调字段。`
    );
    showSection("extract");
    showToast("精选模板已套用", "success");
  }

  async function installMarketTemplate(templateId) {
    const response = await apiFetch("/api/template_market/install", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ template_id: templateId }),
    });
    const data = await response.json();
    if (!response.ok) {
      showToast(`安装模板失败：${data.detail || "未知错误"}`, "error");
      return;
    }
    showToast("模板已安装到我的模板", "success");
    await loadTemplates();
  }

  async function runMonitor(monitorId) {
    if (!monitorId) return;
    const response = await apiFetch(`/api/monitors/${encodeURIComponent(monitorId)}/run`, {
      method: "POST",
    });
    const data = await response.json();
    if (!response.ok) {
      showToast(`触发监控失败：${data.detail || "未知错误"}`, "error");
      return;
    }
    const taskId = data.task_id || monitorId;
    showToast(
      data.reused_existing_task
        ? `已有运行中的任务，已复用：${taskId}`
        : `监控检查已启动：${taskId}`,
      "success"
    );
    showSection("tasks");
    setTimeout(refreshDashboard, 500);
  }

  async function pauseMonitor(monitorId) {
    if (!monitorId) return;
    const response = await apiFetch(`/api/monitors/${encodeURIComponent(monitorId)}/pause`, {
      method: "POST",
    });
    const data = await response.json();
    if (!response.ok) {
      showToast(`暂停失败：${data.detail || "未知错误"}`, "error");
      return;
    }
    showToast("已暂停自动巡检", "success");
    await loadMonitors();
    await refreshDashboard();
  }

  async function resumeMonitor(monitorId) {
    if (!monitorId) return;
    const response = await apiFetch(`/api/monitors/${encodeURIComponent(monitorId)}/resume`, {
      method: "POST",
    });
    const data = await response.json();
    if (!response.ok) {
      showToast(`恢复失败：${data.detail || "未知错误"}`, "error");
      return;
    }
    showToast("已恢复自动巡检", "success");
    await loadMonitors();
    await refreshDashboard();
  }

  async function resendNotification(notificationId) {
    if (!notificationId) return;
    const response = await apiFetch(
      `/api/notifications/${encodeURIComponent(notificationId)}/resend`,
      {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ reason: "dashboard manual resend" }),
      }
    );
    const data = await response.json();
    if (!response.ok) {
      showToast(`补发失败：${data.detail || "未知错误"}`, "error");
      return;
    }
    showToast("通知已补发", "success");
    await refreshDashboard();
  }

  return {
    formatExtractionStrategy,
    renderTemplateBoard,
    renderMarketTemplateBoard,
    renderMonitorAlerts,
    renderMonitorBoard,
    renderNotificationBoard,
    loadTemplates,
    loadMarketTemplates,
    loadMonitors,
    loadNotifications,
    applyTemplate,
    applyMarketTemplate,
    installMarketTemplate,
    runMonitor,
    pauseMonitor,
    resumeMonitor,
    resendNotification,
  };
};
