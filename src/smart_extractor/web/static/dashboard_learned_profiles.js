"use strict";

window.SmartExtractorLearnedProfiles = function createLearnedProfiles(deps) {
  const {
    apiFetch,
    showToast,
    escHtml,
    setText,
    showSection,
    refreshDashboard,
    renderInsightList,
    statusLabel,
    formatQuality,
    formatExtractionStrategy,
    state,
  } = deps;

  function getLearnedProfileRiskLevel(item) {
    const payloadRisk = String((item && item.risk_level) || "").trim().toLowerCase();
    if (payloadRisk) {
      return payloadRisk;
    }
    const ruleFailures = Number(item.rule_failure_count || 0);
    const ruleSuccess = Number(item.rule_success_count || 0);
    const completeness = Number(item.last_completeness || 0);
    if (!item.is_active) {
      return "paused";
    }
    if (ruleFailures >= Math.max(2, ruleSuccess + 1) || completeness < 0.35) {
      return "high";
    }
    if (ruleFailures > 0 || completeness < 0.6) {
      return "medium";
    }
    return "low";
  }

  function learnedProfileRiskLabel(level) {
    if (level === "high") {
      return "高风险";
    }
    if (level === "medium") {
      return "需观察";
    }
    if (level === "paused") {
      return "已暂停";
    }
    return "稳定";
  }

  function learnedProfileRiskBadgeClass(level) {
    if (level === "high") {
      return "badge-failed";
    }
    if (level === "medium") {
      return "badge-running";
    }
    if (level === "paused") {
      return "badge-pending";
    }
    return "badge-success";
  }

  function renderLearnedProfileSummary(allProfiles, filteredProfiles) {
    const element = document.getElementById("learned-profile-summary");
    if (!element) {
      return;
    }
    const profiles = allProfiles || [];
    const filtered = filteredProfiles || [];
    const activeCount = profiles.filter((item) => item.is_active).length;
    const riskyCount = profiles.filter((item) => getLearnedProfileRiskLevel(item) === "high").length;
    const disabledCount = profiles.filter((item) => !item.is_active).length;
    const totalMonitorHits = profiles.reduce((sum, item) => sum + Number(item.monitor_hits || 0), 0);
    const estimatedSavedCalls = profiles.reduce(
      (sum, item) => sum + Number(item.estimated_saved_llm_calls || 0),
      0
    );
    element.innerHTML = `
      <div class="metric-chip">
        <span>当前视图</span>
        <strong>${filtered.length} / ${profiles.length}</strong>
      </div>
      <div class="metric-chip">
        <span>可复用记忆</span>
        <strong>${activeCount}</strong>
      </div>
      <div class="metric-chip">
        <span>高风险记忆</span>
        <strong>${riskyCount}</strong>
      </div>
      <div class="metric-chip">
        <span>节省 LLM 调用</span>
        <strong>${estimatedSavedCalls}</strong>
      </div>
      <div class="metric-chip">
        <span>已停用记忆</span>
        <strong>${disabledCount}</strong>
      </div>
      <div class="metric-chip">
        <span>监控命中总数</span>
        <strong>${totalMonitorHits}</strong>
      </div>
    `;
  }

  function closeLearnedProfileDrawer() {
    state.activeLearnedProfileDetailId = "";
    state.activeLearnedProfileDetail = null;
    const drawer = document.getElementById("learned-profile-drawer");
    if (!drawer) {
      return;
    }
    drawer.hidden = true;
    document.body.classList.remove("drawer-open");
  }

  function renderLearnedProfileDetail(detail) {
    const profile = (detail && detail.profile) || {};
    const activity = (detail && detail.activity) || {};
    const summary = activity.summary || {};
    state.activeLearnedProfileDetail = detail || null;
    const subtitle = document.getElementById("learned-profile-drawer-subtitle");
    const summaryContainer = document.getElementById("learned-profile-drawer-summary");
    const relearnButton = document.getElementById("learned-profile-relearn-btn");
    const toggleButton = document.getElementById("learned-profile-toggle-btn");
    const resetButton = document.getElementById("learned-profile-reset-btn");

    setText(
      "learned-profile-drawer-title",
      `${profile.domain || "unknown"} · ${profile.profile_id || "站点记忆详情"}`
    );
    if (subtitle) {
      subtitle.textContent = `${profile.page_type || "unknown"} · ${profile.path_prefix || "/"} · ${profile.status_label || "-"} · ${profile.memory_strength_label || "刚开始学习"}`;
    }
    if (summaryContainer) {
      summaryContainer.innerHTML = `
        <div class="metric-chip"><span>最近任务命中</span><strong>${Number(summary.task_hits || 0)}</strong></div>
        <div class="metric-chip"><span>关联监控</span><strong>${Number(summary.monitor_links || 0)}</strong></div>
        <div class="metric-chip"><span>规则复用</span><strong>${Number(summary.rule_hits || 0)}</strong></div>
        <div class="metric-chip"><span>LLM 学习</span><strong>${Number(summary.llm_hits || 0)}</strong></div>
        <div class="metric-chip"><span>命中后发生变化</span><strong>${Number(summary.changed_hits || 0)}</strong></div>
        <div class="metric-chip"><span>稳定率</span><strong>${Math.round(Number(profile.stability_rate || 0) * 100)}%</strong></div>
        <div class="metric-chip"><span>节省调用</span><strong>${Number(profile.estimated_saved_llm_calls || 0)}</strong></div>
      `;
    }
    if (relearnButton) {
      relearnButton.disabled = !profile.profile_id;
    }
    if (toggleButton) {
      toggleButton.textContent = profile.is_active ? "停用档案" : "恢复档案";
      toggleButton.disabled = !profile.profile_id;
    }
    if (resetButton) {
      resetButton.disabled = !profile.profile_id;
    }
    renderInsightList(
      "learned-profile-action-list",
      profile.recommended_actions || [],
      "当前没有额外建议动作。",
      (item) => `
        <div class="insight-item">
          <span class="insight-title">${escHtml(item)}</span>
        </div>
      `
    );

    renderInsightList(
      "learned-profile-hit-list",
      activity.recent_hits || [],
      "当前还没有这条站点记忆的命中时间线。",
      (item) => `
        <div class="insight-item">
          <span class="insight-title">${escHtml(item.task_id || "-")} · ${escHtml(item.domain || "unknown")}</span>
          <p class="line-clamp">${escHtml(item.url || "")}</p>
          <p class="panel-note">
            ${escHtml(statusLabel(item.status || ""))} · ${escHtml(formatExtractionStrategy(item.extraction_strategy || ""))}
            · 质量 ${escHtml(formatQuality(item.quality_score || 0))}
            · ${item.changed ? "命中后出现变化" : "命中后未见变化"}
          </p>
        </div>
      `
    );
    renderInsightList(
      "learned-profile-monitor-list",
      activity.related_monitors || [],
      "当前还没有监控在使用这条站点记忆。",
      (item) => `
        <div class="insight-item">
          <span class="insight-title">${escHtml(item.name || item.monitor_id)}</span>
          <p class="line-clamp">${escHtml(item.url || "")}</p>
          <p class="panel-note">
            ${escHtml((item.profile && item.profile.scenario_label) || "通用监控")}
            · 最近策略 ${escHtml(formatExtractionStrategy(item.last_extraction_strategy || ""))}
            · 通知 ${escHtml(item.last_notification_status || "未发送")}
          </p>
        </div>
      `
    );
  }

  async function openLearnedProfileDetail(profileId) {
    const normalizedId = String(profileId || "").trim();
    if (!normalizedId) {
      return;
    }
    const drawer = document.getElementById("learned-profile-drawer");
    if (!drawer) {
      return;
    }
    state.activeLearnedProfileDetailId = normalizedId;
    state.activeLearnedProfileDetail = null;
    drawer.hidden = false;
    document.body.classList.add("drawer-open");
    setText("learned-profile-drawer-title", `${normalizedId} · 加载中`);
    setText("learned-profile-drawer-subtitle", "正在读取最近命中任务、关联监控和稳定指标...");
    renderInsightList("learned-profile-action-list", [], "正在加载...", () => "");
    renderInsightList("learned-profile-hit-list", [], "正在加载...", () => "");
    renderInsightList("learned-profile-monitor-list", [], "正在加载...", () => "");

    const response = await apiFetch(`/api/learned_profiles/${encodeURIComponent(normalizedId)}`);
    const data = await response.json();
    if (!response.ok) {
      showToast(`学习档案详情读取失败：${data.detail || "未知错误"}`, "error");
      closeLearnedProfileDrawer();
      return;
    }
    if (state.activeLearnedProfileDetailId !== normalizedId) {
      return;
    }
    renderLearnedProfileDetail(data);
  }

  async function relearnLearnedProfile(profileId) {
    const normalizedId = String(profileId || "").trim();
    if (!normalizedId) {
      return;
    }
    const response = await apiFetch(`/api/learned_profiles/${encodeURIComponent(normalizedId)}/relearn`, {
      method: "POST",
    });
    const data = await response.json();
    if (!response.ok) {
      showToast(`重新学习失败：${data.detail || "未知错误"}`, "error");
      return;
    }
    showToast(data.message || "已启动重新学习任务", "success");
    showSection("tasks");
    setTimeout(refreshDashboard, 500);
  }

  async function refreshAndReopenDetailIfNeeded(profileId) {
    await refreshDashboard();
    if (state.activeLearnedProfileDetailId === profileId) {
      await openLearnedProfileDetail(profileId);
    }
  }

  async function disableLearnedProfile(profileId) {
    const normalizedId = String(profileId || "").trim();
    if (!normalizedId) {
      return;
    }
    const promptValue = window.prompt("可选：记录一下停用原因，方便团队回看。", "规则命中不稳定");
    if (promptValue === null) {
      return;
    }
    const reason = promptValue || "";
    const response = await apiFetch(`/api/learned_profiles/${encodeURIComponent(normalizedId)}/disable`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ reason }),
    });
    const data = await response.json();
    if (!response.ok) {
      showToast(`停用失败：${data.detail || "未知错误"}`, "error");
      return;
    }
    showToast(data.message || "学习档案已停用", "success");
    await refreshAndReopenDetailIfNeeded(normalizedId);
  }

  async function enableLearnedProfile(profileId) {
    const normalizedId = String(profileId || "").trim();
    if (!normalizedId) {
      return;
    }
    const response = await apiFetch(`/api/learned_profiles/${encodeURIComponent(normalizedId)}/enable`, {
      method: "POST",
    });
    const data = await response.json();
    if (!response.ok) {
      showToast(`恢复失败：${data.detail || "未知错误"}`, "error");
      return;
    }
    showToast(data.message || "学习档案已恢复", "success");
    await refreshAndReopenDetailIfNeeded(normalizedId);
  }

  async function resetLearnedProfile(profileId) {
    const normalizedId = String(profileId || "").trim();
    if (!normalizedId || !window.confirm(`确认重置 ${normalizedId} 的统计数据吗？`)) {
      return;
    }
    const response = await apiFetch(`/api/learned_profiles/${encodeURIComponent(normalizedId)}/reset`, {
      method: "POST",
    });
    const data = await response.json();
    if (!response.ok) {
      showToast(`重置失败：${data.detail || "未知错误"}`, "error");
      return;
    }
    showToast(data.message || "学习档案统计已重置", "success");
    await refreshAndReopenDetailIfNeeded(normalizedId);
  }

  async function bulkDisableRiskyLearnedProfiles() {
    if (!window.confirm("确认批量停用当前所有高风险学习档案吗？")) {
      return;
    }
    const promptValue = window.prompt("可选：记录一下本次批量停用原因，方便团队回看。", "批量停用高风险学习档案");
    if (promptValue === null) {
      return;
    }
    const response = await apiFetch("/api/learned_profiles/bulk/disable_risky", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ reason: promptValue || "" }),
    });
    const data = await response.json();
    if (!response.ok) {
      showToast(`批量停用失败：${data.detail || "未知错误"}`, "error");
      return;
    }
    showToast(data.message || "已批量停用高风险学习档案", "success");
    await refreshDashboard();
    if (!state.activeLearnedProfileDetailId) {
      return;
    }
    const stillExists = state.latestLearnedProfiles.some((item) => item.profile_id === state.activeLearnedProfileDetailId);
    if (stillExists) {
      await openLearnedProfileDetail(state.activeLearnedProfileDetailId);
    }
  }

  async function bulkRelearnRiskyLearnedProfiles() {
    if (!window.confirm("确认批量重新学习当前所有高风险学习档案吗？")) {
      return;
    }
    const promptValue = window.prompt("可选：记录一下本次批量重新学习的说明。", "批量重新学习高风险学习档案");
    if (promptValue === null) {
      return;
    }
    const response = await apiFetch("/api/learned_profiles/bulk/relearn_risky", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ reason: promptValue || "" }),
    });
    const data = await response.json();
    if (!response.ok) {
      showToast(`批量重新学习失败：${data.detail || "未知错误"}`, "error");
      return;
    }
    showToast(data.message || "已启动高风险学习档案的重新学习任务", "success");
    await refreshDashboard();
    showSection("tasks");
  }

  async function deleteLearnedProfile(profileId) {
    const normalizedId = String(profileId || "").trim();
    if (!normalizedId || !window.confirm(`确认删除学习档案 ${normalizedId} 吗？`)) {
      return;
    }
    const response = await apiFetch(`/api/learned_profiles/${encodeURIComponent(normalizedId)}`, {
      method: "DELETE",
    });
    const data = await response.json();
    if (!response.ok) {
      showToast(`删除失败：${data.detail || "未知错误"}`, "error");
      return;
    }
    showToast(data.message || "学习档案已删除", "success");
    if (state.activeLearnedProfileDetailId === normalizedId) {
      closeLearnedProfileDrawer();
    }
    await refreshDashboard();
  }

  function renderLearnedProfileBoard(profiles) {
    const filteredProfiles = (profiles || []).filter((item) => {
      const riskLevel = getLearnedProfileRiskLevel(item);
      if (state.activeLearnedProfileFilter === "active" && !item.is_active) {
        return false;
      }
      if (state.activeLearnedProfileFilter === "disabled" && item.is_active) {
        return false;
      }
      if (state.activeLearnedProfileFilter === "risky" && riskLevel !== "high") {
        return false;
      }

      const keyword = state.learnedProfileSearchKeyword.trim().toLowerCase();
      if (!keyword) {
        return true;
      }
      const haystack = [
        item.profile_id,
        item.domain,
        item.page_type,
        item.path_prefix,
        item.last_matched_url,
        item.disabled_reason,
        ...(item.selected_fields || []),
      ]
        .join(" ")
        .toLowerCase();
      return haystack.includes(keyword);
    });

    renderLearnedProfileSummary(profiles || [], filteredProfiles);
    renderInsightList(
      "learned-profile-board",
      filteredProfiles,
      "当前还没有形成站点记忆；同站点重复执行后，这里会沉淀可复用的字段方案。",
      (item) => `
        <div class="insight-item">
          <div class="insight-item-row">
            <div>
              <span class="insight-title">${escHtml(item.domain || "unknown")} · ${escHtml(item.page_type || "unknown")}</span>
              <p>${escHtml((item.selected_fields || []).join("、") || "暂无字段方案")}</p>
              <p class="panel-note">
                <span class="badge ${item.is_active ? "badge-success" : "badge-pending"}">${escHtml(item.status_label || (item.is_active ? "可复用" : "已停用"))}</span>
                <span class="badge ${learnedProfileRiskBadgeClass(getLearnedProfileRiskLevel(item))}">${escHtml(item.risk_label || learnedProfileRiskLabel(getLearnedProfileRiskLevel(item)))}</span>
                <span class="badge ${item.memory_strength === "strong" ? "badge-success" : item.memory_strength === "warming" ? "badge-running" : "badge-pending"}">${escHtml(item.memory_strength_label || "刚开始学习")}</span>
                ${item.disabled_reason ? ` · ${escHtml(item.disabled_reason)}` : ""}
              </p>
              <p class="panel-note">
                路径前缀：${escHtml(item.path_prefix || "/")}
                · 最近策略：${escHtml(formatExtractionStrategy(item.last_strategy || "llm"))}
                ${item.last_matched_url ? ` · 最近命中：${escHtml(item.last_matched_url)}` : ""}
              </p>
              <p class="panel-note">
                规则复用 ${Number(item.rule_success_count || 0)} 次 / LLM 学习 ${Number(item.llm_success_count || 0)} 次 / 监控命中 ${Number(item.monitor_hits || 0)} 个 / 稳定率 ${Math.round(Number(item.stability_rate || 0) * 100)}%
              </p>
              <p class="panel-note">节省调用约 ${Number(item.estimated_saved_llm_calls || 0)} 次 · ${escHtml(((item.recommended_actions || [])[0]) || "保持当前策略")}</p>
            </div>
            <div class="insight-side">
              <strong>${Number(item.rule_success_count || 0)} 次</strong>
              <span>${escHtml(item.is_active ? "正在参与规则复用" : "已暂停复用")}</span>
            </div>
          </div>
          <div class="detail-action-row">
            <button class="btn btn-soft btn-inline" type="button" data-open-learned-profile="${escHtml(item.profile_id)}">查看详情</button>
            <button class="btn btn-ghost btn-inline" type="button" data-reset-learned-profile="${escHtml(item.profile_id)}">重置统计</button>
            ${
              item.is_active
                ? `<button class="btn btn-ghost btn-inline" type="button" data-disable-learned-profile="${escHtml(item.profile_id)}">停用</button>`
                : `<button class="btn btn-ghost btn-inline" type="button" data-enable-learned-profile="${escHtml(item.profile_id)}">恢复</button>`
            }
            <button class="btn btn-ghost btn-inline" type="button" data-delete-learned-profile="${escHtml(item.profile_id)}">删除</button>
          </div>
        </div>
      `
    );

    document.querySelectorAll("[data-disable-learned-profile]").forEach((button) => {
      button.addEventListener("click", () => disableLearnedProfile(button.dataset.disableLearnedProfile || ""));
    });
    document.querySelectorAll("[data-enable-learned-profile]").forEach((button) => {
      button.addEventListener("click", () => enableLearnedProfile(button.dataset.enableLearnedProfile || ""));
    });
    document.querySelectorAll("[data-reset-learned-profile]").forEach((button) => {
      button.addEventListener("click", () => resetLearnedProfile(button.dataset.resetLearnedProfile || ""));
    });
    document.querySelectorAll("[data-delete-learned-profile]").forEach((button) => {
      button.addEventListener("click", () => deleteLearnedProfile(button.dataset.deleteLearnedProfile || ""));
    });
    document.querySelectorAll("[data-open-learned-profile]").forEach((button) => {
      button.addEventListener("click", () => openLearnedProfileDetail(button.dataset.openLearnedProfile || ""));
    });
  }

  return {
    renderLearnedProfileBoard,
    renderLearnedProfileSummary,
    closeLearnedProfileDrawer,
    openLearnedProfileDetail,
    relearnLearnedProfile,
    disableLearnedProfile,
    enableLearnedProfile,
    resetLearnedProfile,
    bulkDisableRiskyLearnedProfiles,
    bulkRelearnRiskyLearnedProfiles,
    deleteLearnedProfile,
    getLearnedProfileRiskLevel,
  };
};
