"use strict";

window.SmartExtractorDashboardAnalysis = function createDashboardAnalysis(deps) {
  const {
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
    applyMonitorProfile,
    loadMonitors,
    refreshDashboard,
    startAnalyzeProgress,
    finishAnalyzeProgress,
    updateAnalysisPreview,
    state,
  } = deps;

  function normalizeInsightMode(mode) {
    return mode === "compare" ? "compare" : "single";
  }

  function getInsightAnalysisByMode(mode) {
    const insightStore = state.latestInsightAnalysisByMode || {};
    return insightStore[normalizeInsightMode(mode)] || null;
  }

  function syncInsightModeControls() {
    const isCompareMode = state.activeInsightMode === "compare";
    const submitButton = document.getElementById("insight-submit-btn");
    const submitResult = document.getElementById("insight-submit-result");
    const progressTitle = document.getElementById("insight-progress-title");

    if (submitButton && !submitButton.disabled) {
      submitButton.textContent = isCompareMode ? "生成对比分析" : "生成分析";
    }
    if (submitResult) {
      submitResult.style.display = "none";
    }
    if (progressTitle) {
      progressTitle.textContent = isCompareMode ? "正在生成对比分析" : "正在生成分析";
    }
  }

  function refreshInsightResultPanel() {
    const panel = document.getElementById("insight-results-panel");
    const activeAnalysis = getInsightAnalysisByMode(state.activeInsightMode);
    state.latestInsightAnalysis = activeAnalysis;
    if (!panel) {
      return;
    }

    if (!activeAnalysis) {
      panel.hidden = true;
      renderCompareMatrix([], state.activeInsightMode);
      renderCompareReport({}, state.activeInsightMode);
      return;
    }

    renderInsightResult(activeAnalysis, state.activeInsightMode);
  }

  function setInsightMode(mode) {
    state.activeInsightMode = normalizeInsightMode(mode);
    document.querySelectorAll(".mode-tab[data-mode]").forEach((button) => {
      button.classList.toggle("active", button.dataset.mode === state.activeInsightMode);
    });

    const singlePanel = document.getElementById("insight-single-mode-panel");
    const comparePanel = document.getElementById("insight-compare-mode-panel");
    if (singlePanel) {
      singlePanel.classList.toggle("section-hidden", state.activeInsightMode !== "single");
    }
    if (comparePanel) {
      comparePanel.classList.toggle("section-hidden", state.activeInsightMode !== "compare");
    }
    syncInsightModeControls();
    refreshInsightResultPanel();
  }

  function updateCompareUrlSummary() {
    const summary = document.getElementById("compare-url-summary");
    if (!summary) {
      return [];
    }

    const parsed = parseBatchUrls(document.getElementById("compare-urls")?.value || "");
    if (!parsed.hasInput) {
      summary.textContent = "输入多个 URL 后，这里会显示有效数量、重复项和无效行。";
      return parsed.uniqueUrls;
    }

    const fragments = [`有效 ${parsed.uniqueUrls.length} 条`];
    if (parsed.duplicateCount > 0) {
      fragments.push(`重复 ${parsed.duplicateCount} 条`);
    }
    if (parsed.invalidCount > 0) {
      fragments.push(`无效 ${parsed.invalidCount} 条`);
    }
    summary.textContent = fragments.join("，");
    return parsed.uniqueUrls;
  }

  function renderComparePreviewBoard(items) {
    const board = document.getElementById("compare-preview-board");
    if (!board) {
      return;
    }

    if (!items || items.length === 0) {
      board.innerHTML = '<div class="relation-empty">还没有可预览的对比对象。</div>';
      return;
    }

    board.innerHTML = items
      .map((item, index) => {
        const fields = (item.candidate_fields || []).slice(0, 4);
        return `
          <article class="compare-preview-card">
            <header>
              <strong>对象 ${index + 1} · ${escHtml(item.page_type_label || item.page_type || "未知页面")}</strong>
              <span class="badge badge-pending">${escHtml(item.url || "")}</span>
            </header>
            <p>${escHtml(item.preview || "暂未生成页面预览")}</p>
            <div class="compare-chip-row">
              ${fields.length ? fields.map((field) => `<span>${escHtml((item.field_labels && item.field_labels[field]) || field)}</span>`).join("") : "<span>暂无字段线索</span>"}
            </div>
          </article>
        `;
      })
      .join("");
  }

  function collectComparePayload() {
    const urls = updateCompareUrlSummary();
    return {
      urls,
      use_static: document.getElementById("compare-static-mode")?.value === "true",
      goal: document.getElementById("compare-goal")?.value || "comparison",
      role: document.getElementById("compare-role")?.value || "consumer",
      focus: document.getElementById("compare-focus")?.value.trim() || "",
      must_have: document.getElementById("compare-must-have")?.value.trim() || "",
      elimination: document.getElementById("compare-elimination")?.value.trim() || "",
      notes: document.getElementById("compare-notes")?.value.trim() || "",
      output_format: document.getElementById("compare-output-format")?.value || "table",
    };
  }

  function renderNlTaskPreview(plan) {
    const preview = document.getElementById("nl-task-preview");
    const actions = document.getElementById("nl-task-actions");
    const runTaskButton = document.getElementById("nl-run-task-btn");
    const saveMonitorButton = document.getElementById("nl-save-monitor-btn");
    const openCompareButton = document.getElementById("nl-open-compare-btn");
    if (!preview) {
      return;
    }
    if (!plan) {
      preview.classList.add("preview-empty");
      preview.textContent = "解析后的任务草案会显示在这里，并自动回填到下方表单。";
      if (actions) {
        actions.hidden = true;
      }
      return;
    }

    const warnings = Array.isArray(plan.warnings) && plan.warnings.length
      ? `\n注意事项：\n- ${plan.warnings.join("\n- ")}`
      : "";
    preview.classList.remove("preview-empty");
      preview.textContent = [
      `任务类型：${plan.task_type || "-"}`,
      `任务名：${plan.name || "-"}`,
      `摘要：${plan.summary || "-"}`,
      `URL 数量：${(plan.urls || []).length}`,
      `字段：${(plan.selected_fields || []).join("、") || "未指定"}`,
      `存储格式：${plan.storage_format || "json"}`,
      `抓取模式：${plan.use_static ? "静态" : "动态"}`,
      warnings,
    ].join("\n");

    if (actions) {
      actions.hidden = false;
    }
    const taskType = plan.task_type || "single_extract";
    if (runTaskButton) {
      runTaskButton.hidden = false;
      runTaskButton.textContent = taskType === "compare_analysis" ? "直接生成报告" : "直接执行";
    }
    if (saveMonitorButton) {
      saveMonitorButton.hidden = taskType !== "monitor";
    }
    if (openCompareButton) {
      openCompareButton.hidden = taskType !== "compare_analysis";
    }
  }

  function applyNaturalLanguagePlan(plan) {
    if (!plan) {
      return;
    }
    state.latestNlTaskPlan = plan;
    renderNlTaskPreview(plan);

    const taskType = plan.task_type || "single_extract";
    const urls = Array.isArray(plan.urls) ? plan.urls : [];
    state.latestFieldLabels = Object.fromEntries((plan.selected_fields || []).map((field) => [field, field]));
    state.latestAnalyzedFields = plan.selected_fields || [];
    state.selectedFieldState = state.latestAnalyzedFields.map((name) => ({ name, checked: true }));
    renderFieldSelector();

    if (taskType === "compare_analysis") {
      setInsightMode("compare");
      document.getElementById("compare-urls").value = urls.join("\n");
      document.getElementById("compare-focus").value = (plan.selected_fields || []).join("、");
      updateCompareUrlSummary();
      showSection("analyzer");
    } else if (taskType === "batch_extract") {
      setInputValue("format", plan.storage_format || "json");
      setInputValue("static-mode", plan.use_static ? "true" : "false");
      document.getElementById("batch-urls").value = urls.join("\n");
      updateBatchSummary();
      setText("detected-page-type", `任务草案：${plan.name || "批量提取"}`);
      setText("field-selector-help", "系统已根据自然语言需求回填批量任务草案，你可以继续调整后提交。");
      showSection("extract");
    } else {
      setInputValue("url", urls[0] || "");
      setInputValue("format", plan.storage_format || "json");
      setInputValue("static-mode", plan.use_static ? "true" : "false");
      applyMonitorProfile({
        scenario_label: taskType === "monitor" ? (plan.name || "自然语言监控") : "",
        business_goal: taskType === "monitor" ? (plan.summary || "") : "",
        alert_focus: (plan.selected_fields || []).join("、"),
        notify_on: ["changed", "error"],
      });
      setText("detected-page-type", `任务草案：${plan.name || "自然语言任务"}`);
      setText(
        "field-selector-help",
        taskType === "monitor"
          ? "系统已回填监控草案，你可以直接保存为监控，或继续修改后再提交。"
          : "系统已根据自然语言需求回填任务草案，你可以直接提交，或继续微调。"
      );
      showSection("extract");
    }
  }

  async function createMonitorFromPlan(plan) {
    const selectedFields = plan.selected_fields || [];
    const scenarioLabel = plan.name || "自然语言监控";
    const response = await apiFetch("/api/monitors", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        monitor_id: "",
        name: scenarioLabel,
        url: (plan.urls || [])[0] || "",
        schema_name: plan.schema_name || "auto",
        storage_format: plan.storage_format || "json",
        use_static: !!plan.use_static,
        selected_fields: selectedFields,
        field_labels: Object.fromEntries(selectedFields.map((field) => [field, field])),
        profile: {
          scenario_label: scenarioLabel,
          business_goal: plan.summary || "根据自然语言需求生成的监控方案",
          alert_focus: selectedFields.join("、"),
          notify_on: ["changed", "error"],
        },
      }),
    });
    const data = await response.json();
    if (!response.ok) {
      throw new Error(data.detail || "保存监控失败");
    }
    return data;
  }

  async function runNaturalLanguagePlan() {
    const plan = state.latestNlTaskPlan;
    if (!plan) {
      showToast("请先生成任务草案", "error");
      return;
    }

    const taskType = plan.task_type || "single_extract";
    try {
      if (taskType === "compare_analysis") {
        applyNaturalLanguagePlan(plan);
        await submitCompareAnalysis();
        return;
      }

      if (taskType === "monitor") {
        const data = await createMonitorFromPlan(plan);
        showToast(`监控已保存：${data.monitor?.monitor_id || plan.name}`, "success");
        await loadMonitors();
        await refreshDashboard();
        return;
      }

      if (taskType === "batch_extract") {
        const urls = Array.isArray(plan.urls) ? plan.urls.filter(Boolean) : [];
        if (!urls.length) {
          showToast("褰撳墠鑽夋娌℃湁鍙墽琛岀殑 URL", "error");
          return;
        }
        const response = await apiFetch("/api/batch", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            urls,
            schema_name: plan.schema_name || "auto",
            storage_format: plan.storage_format || "json",
          }),
        });
        const data = await response.json();
        if (!response.ok) {
          throw new Error(data.detail || "批量任务提交失败");
        }
        showToast(`批量任务已创建：${data.task_id}`, "success");
        showSection("tasks");
        setTimeout(refreshDashboard, 500);
        return;
      }

      const firstUrl = Array.isArray(plan.urls) ? plan.urls[0] : "";
      if (!firstUrl) {
        showToast("褰撳墠鑽夋娌℃湁鍙墽琛岀殑 URL", "error");
        return;
      }
      const response = await apiFetch("/api/extract", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          url: firstUrl,
          schema_name: plan.schema_name || "auto",
          storage_format: plan.storage_format || "json",
          use_static: !!plan.use_static,
          selected_fields: plan.selected_fields || [],
        }),
      });
      const data = await response.json();
      if (!response.ok) {
        throw new Error(data.detail || "任务提交失败");
      }
      showToast(`任务已创建：${data.task_id}`, "success");
      showSection("tasks");
      setTimeout(refreshDashboard, 500);
    } catch (error) {
      showToast(`执行失败：${error.message}`, "error");
    }
  }

  async function saveMonitorFromNaturalLanguagePlan() {
    const plan = state.latestNlTaskPlan;
    if (!plan) {
      showToast("请先生成任务草案", "error");
      return;
    }
    try {
      const data = await createMonitorFromPlan(plan);
      showToast(`监控已保存：${data.monitor?.monitor_id || plan.name}`, "success");
      await loadMonitors();
      await refreshDashboard();
    } catch (error) {
      showToast(`保存监控失败：${error.message}`, "error");
    }
  }

  function openCompareFromNaturalLanguagePlan() {
    const plan = state.latestNlTaskPlan;
    if (!plan) {
      showToast("请先生成任务草案", "error");
      return;
    }
    applyNaturalLanguagePlan({
      ...plan,
      task_type: "compare_analysis",
    });
    showToast("已切换到对比分析并回填草案", "success");
  }

  function buildInsightBriefText(data, mode) {
    const analysis = (data && data.analysis) || {};
    const report = (data && data.report) || {};
    const lines = [
      `标题：${analysis.headline || "网页智能分析"}`,
      `模式：${mode === "compare" ? "多 URL 对比分析" : "单 URL 深度分析"}`,
      `页面类型：${data.page_type_label || data.page_type || "未知"}`,
      `分析信心：${analysis.confidence || "medium"}`,
      "",
      `一句话结论：${analysis.summary || "-"}`,
      "",
      "核心发现：",
      ...(analysis.key_points || []).map((item, index) => `${index + 1}. ${item}`),
      "",
      "风险/注意点：",
      ...((analysis.risks || []).length ? analysis.risks : ["当前没有检测到明显风险"]).map(
        (item, index) => `${index + 1}. ${item}`
      ),
      "",
      "下一步建议：",
      ...((analysis.recommended_actions || []).length
        ? analysis.recommended_actions
        : ["当前没有给出后续建议"]).map((item, index) => `${index + 1}. ${item}`),
    ];

    if (mode === "compare" && data.comparison_matrix && data.comparison_matrix.length) {
      lines.push("", "横向比较：");
      data.comparison_matrix.forEach((item, index) => {
        lines.push(`${index + 1}. ${item.label}: ${item.summary}`);
      });
    }

    if (mode === "compare" && report && report.executive_summary) {
      lines.push("", "差异对比报告：", `报告标题：${report.title || "差异对比报告"}`, `执行摘要：${report.executive_summary}`);
      (report.common_points || []).forEach((item, index) => lines.push(`共同点 ${index + 1}. ${item}`));
      (report.difference_points || []).forEach((item, index) => lines.push(`差异点 ${index + 1}. ${item}`));
      if (report.recommendation) {
        lines.push(`建议结论：${report.recommendation}`);
      }
    }

    if (analysis.evidence_spans && analysis.evidence_spans.length) {
      lines.push("", "证据片段：");
      analysis.evidence_spans.forEach((item, index) => {
        lines.push(`${index + 1}. ${item.label}: ${item.snippet}`);
      });
    }

    return lines.join("\n");
  }

  function buildInsightMarkdownText(data, mode) {
    const analysis = (data && data.analysis) || {};
    const report = (data && data.report) || {};
    const lines = [
      `# ${analysis.headline || "网页智能分析"}`,
      "",
      `- 模式：${mode === "compare" ? "多 URL 对比分析" : "单 URL 深度分析"}`,
      `- 页面类型：${data.page_type_label || data.page_type || "未知"}`,
      `- 分析信心：${analysis.confidence || "medium"}`,
      "",
      "## 一句话结论",
      "",
      analysis.summary || "-",
      "",
      "## 核心发现",
      "",
      ...((analysis.key_points || []).length
        ? analysis.key_points.map((item) => `- ${item}`)
        : ["- 当前没有提炼出核心发现。"]),
      "",
      "## 风险与注意点",
      "",
      ...((analysis.risks || []).length
        ? analysis.risks.map((item) => `- ${item}`)
        : ["- 当前没有检测到明显风险。"]),
      "",
      "## 下一步建议",
      "",
      ...((analysis.recommended_actions || []).length
        ? analysis.recommended_actions.map((item) => `- ${item}`)
        : ["- 当前没有给出后续建议。"]),
    ];

    if ((analysis.missing_information || []).length) {
      lines.push("", "## 建议继续补充", "", ...analysis.missing_information.map((item) => `- ${item}`));
    }

    if (mode === "compare" && data.comparison_matrix && data.comparison_matrix.length) {
      lines.push("", "## 横向比较", "");
      data.comparison_matrix.forEach((item) => {
        lines.push(`- **${item.label || "比较维度"}**：${item.summary || "-"}`);
      });
    }

    if (mode === "compare" && report && report.executive_summary) {
      lines.push("", `## ${report.title || "差异对比报告"}`, "", report.executive_summary || "-");
      lines.push("", "### 共同点", "");
      lines.push(...((report.common_points || []).length ? report.common_points.map((item) => `- ${item}`) : ["- 暂无"]));
      lines.push("", "### 核心差异", "");
      lines.push(...((report.difference_points || []).length ? report.difference_points.map((item) => `- ${item}`) : ["- 暂无"]));
      lines.push("", "### 建议结论", "", report.recommendation || "-");
      if ((report.next_steps || []).length) {
        lines.push("", "### 下一步", "", ...report.next_steps.map((item) => `- ${item}`));
      }
    }

    if (analysis.evidence_spans && analysis.evidence_spans.length) {
      lines.push("", "## 证据片段", "");
      analysis.evidence_spans.forEach((item) => {
        lines.push(`- **${item.label || "证据"}**：${item.snippet || "-"}`);
      });
    }

    return lines.join("\n");
  }

  function csvEscape(value) {
    const text = String(value ?? "");
    return `"${text.replace(/"/g, '""')}"`;
  }

  function buildInsightCsvText(data, mode) {
    const analysis = (data && data.analysis) || {};
    const report = (data && data.report) || {};
    const rows = [
      ["section", "label", "value"],
      ["meta", "headline", analysis.headline || "网页智能分析"],
      ["meta", "mode", mode === "compare" ? "多 URL 对比分析" : "单 URL 深度分析"],
      ["meta", "page_type", data.page_type_label || data.page_type || "未知"],
      ["meta", "confidence", analysis.confidence || "medium"],
      ["summary", "summary", analysis.summary || ""],
    ];

    (analysis.key_points || []).forEach((item, index) => {
      rows.push(["key_points", String(index + 1), item]);
    });
    (analysis.risks || []).forEach((item, index) => {
      rows.push(["risks", String(index + 1), item]);
    });
    (analysis.recommended_actions || []).forEach((item, index) => {
      rows.push(["recommended_actions", String(index + 1), item]);
    });
    (analysis.missing_information || []).forEach((item, index) => {
      rows.push(["missing_information", String(index + 1), item]);
    });
    (analysis.evidence_spans || []).forEach((item, index) => {
      rows.push(["evidence_spans", `${index + 1}:${item.label || "证据"}`, item.snippet || ""]);
    });

    if (mode === "compare") {
      (data.comparison_matrix || []).forEach((item, index) => {
        rows.push(["comparison_matrix", `${index + 1}:${item.label || "比较维度"}`, item.summary || ""]);
      });
      if (report.executive_summary) {
        rows.push(["report", "title", report.title || "差异对比报告"]);
        rows.push(["report", "executive_summary", report.executive_summary || ""]);
        (report.common_points || []).forEach((item, index) => rows.push(["report_common_points", String(index + 1), item]));
        (report.difference_points || []).forEach((item, index) => rows.push(["report_difference_points", String(index + 1), item]));
        rows.push(["report", "recommendation", report.recommendation || ""]);
        (report.next_steps || []).forEach((item, index) => rows.push(["report_next_steps", String(index + 1), item]));
      }
    }

    return rows.map((row) => row.map(csvEscape).join(",")).join("\n");
  }

  function exportInsightBrief() {
    const currentAnalysis = getInsightAnalysisByMode(state.activeInsightMode);
    if (!currentAnalysis) {
      showToast("当前没有可导出的分析结果", "error");
      return;
    }
    const content = buildInsightBriefText(currentAnalysis, state.activeInsightMode);
    const stamp = new Date().toISOString().slice(0, 19).replace(/[:T]/g, "-");
    downloadTextFile(`analysis-brief-${stamp}.txt`, content);
    showToast("已导出汇报文本", "success");
  }

  function exportInsightJson() {
    const currentAnalysis = getInsightAnalysisByMode(state.activeInsightMode);
    if (!currentAnalysis) {
      showToast("当前没有可导出的分析结果", "error");
      return;
    }
    const stamp = new Date().toISOString().slice(0, 19).replace(/[:T]/g, "-");
    downloadTextFile(
      `analysis-result-${stamp}.json`,
      JSON.stringify(currentAnalysis, null, 2),
      "application/json;charset=utf-8"
    );
    showToast("已导出 JSON", "success");
  }

  function exportInsightMarkdown() {
    const currentAnalysis = getInsightAnalysisByMode(state.activeInsightMode);
    if (!currentAnalysis) {
      showToast("当前没有可导出的分析结果", "error");
      return;
    }
    const content = buildInsightMarkdownText(currentAnalysis, state.activeInsightMode);
    const stamp = new Date().toISOString().slice(0, 19).replace(/[:T]/g, "-");
    downloadTextFile(`analysis-result-${stamp}.md`, content, "text/markdown;charset=utf-8");
    showToast("已导出 Markdown", "success");
  }

  function exportInsightCsv() {
    const currentAnalysis = getInsightAnalysisByMode(state.activeInsightMode);
    if (!currentAnalysis) {
      showToast("当前没有可导出的分析结果", "error");
      return;
    }
    const content = buildInsightCsvText(currentAnalysis, state.activeInsightMode);
    const stamp = new Date().toISOString().slice(0, 19).replace(/[:T]/g, "-");
    downloadTextFile(`analysis-result-${stamp}.csv`, content, "text/csv;charset=utf-8");
    showToast("已导出 CSV", "success");
  }

  async function previewCompareTargets() {
    const payload = collectComparePayload();
    const button = document.getElementById("compare-analyze-btn");
    if (!payload.urls.length) {
      showToast("请先输入至少两个合法 URL", "error");
      return;
    }

    const originalText = button.textContent;
    button.disabled = true;
    button.innerHTML = '<span class="spinner"></span>预览中';

    try {
      const response = await apiFetch("/api/analyze_compare_preview", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          urls: payload.urls,
          use_static: payload.use_static,
        }),
      });
      const data = await response.json();
      if (!response.ok) {
        throw new Error(data.detail || "对比预览失败");
      }

      state.latestComparePreview = data.items || [];
      renderComparePreviewBoard(state.latestComparePreview);
      setText("compare-board-title", `已生成 ${state.latestComparePreview.length} 个对比对象预览`);
      setText("compare-board-help", "现在可以继续填写比较目标、必须项和淘汰项，系统会输出横向结论。");
      showToast("对比对象预览已生成", "success");
    } catch (error) {
      showToast(`对比预览失败：${error.message}`, "error");
    } finally {
      button.disabled = false;
      button.textContent = originalText;
    }
  }

  function setInsightFieldPreview(fields, labels) {
    const container = document.getElementById("insight-candidate-fields");
    if (!container) {
      return;
    }

    if (!fields || fields.length === 0) {
      container.innerHTML = '<span class="empty-chip">候选字段将在这里显示</span>';
      return;
    }

    container.innerHTML = fields
      .map((field) => {
        const label = (labels && labels[field]) || field;
        return `<span class="field-chip active"><span>${escHtml(label)}</span><small>${escHtml(field)}</small></span>`;
      })
      .join("");
  }

  function updateInsightPreview(text) {
    const preview = document.getElementById("insight-preview");
    if (!preview) {
      return;
    }
    if (!text) {
      preview.classList.add("preview-empty");
      preview.textContent = "页面预览将在这里显示";
      return;
    }
    preview.classList.remove("preview-empty");
    preview.textContent = text;
  }

  function collectInsightPayload() {
    return {
      url: document.getElementById("insight-url")?.value.trim() || "",
      use_static: document.getElementById("insight-static-mode")?.value === "true",
      goal: document.getElementById("insight-goal")?.value || "summary",
      role: document.getElementById("insight-role")?.value || "consumer",
      priority: document.getElementById("insight-priority")?.value.trim() || "",
      constraints: document.getElementById("insight-constraints")?.value.trim() || "",
      notes: document.getElementById("insight-notes")?.value.trim() || "",
      output_format: document.getElementById("insight-output-format")?.value || "cards",
    };
  }

  function setInsightProgress(value, text) {
    const panel = document.getElementById("insight-progress-panel");
    const bar = document.getElementById("insight-progress-bar");
    const percent = document.getElementById("insight-progress-percent");
    const message = document.getElementById("insight-progress-text");
    const track = panel ? panel.querySelector(".analyze-progress-track") : null;

    if (!panel || !bar || !percent || !message || !track) {
      return;
    }

    const numeric = Math.max(0, Math.min(100, Math.round(value)));
    state.insightProgressValue = numeric;
    panel.hidden = false;
    bar.style.width = `${numeric}%`;
    percent.textContent = `${numeric}%`;
    message.textContent = text;
    track.setAttribute("aria-valuenow", String(numeric));
  }

  function stopInsightProgressTimer() {
    if (state.insightProgressTimer) {
      clearInterval(state.insightProgressTimer);
      state.insightProgressTimer = null;
    }
  }

  function startInsightProgress() {
    stopInsightProgressTimer();
    setInsightProgress(10, "正在抓取页面并整理结构化信息...");

    state.insightProgressTimer = setInterval(() => {
      let nextValue = state.insightProgressValue;
      let nextText = "正在融合页面内容与用户输入...";

      if (state.insightProgressValue < 34) {
        nextValue += 8;
        nextText = "正在抓取并清洗页面...";
      } else if (state.insightProgressValue < 68) {
        nextValue += 5;
        nextText = "正在提取字段并构建分析上下文...";
      } else if (state.insightProgressValue < 88) {
        nextValue += 3;
        nextText = "正在生成结论、风险与建议...";
      } else {
        nextValue = 92;
        nextText = "正在整理最终结果...";
      }

      setInsightProgress(nextValue, nextText);
    }, 420);
  }

  function finishInsightProgress(text) {
    stopInsightProgressTimer();
    setInsightProgress(100, text);
  }

  function renderSimpleInsightList(containerId, items, fallbackText) {
    const container = document.getElementById(containerId);
    if (!container) {
      return;
    }

    if (!items || items.length === 0) {
      container.innerHTML = `<div class="insight-plain-item">${escHtml(fallbackText)}</div>`;
      return;
    }

    container.innerHTML = items
      .map((item) => `<div class="insight-item"><p>${escHtml(item)}</p></div>`)
      .join("");
  }

  function renderEvidenceList(items) {
    const container = document.getElementById("insight-result-evidence");
    if (!container) {
      return;
    }

    if (!items || items.length === 0) {
      container.innerHTML = '<div class="insight-plain-item">当前没有足够的证据片段可展示。</div>';
      return;
    }

    container.innerHTML = items
      .map(
        (item) => `
          <div class="insight-item">
            <span class="insight-title">${escHtml(item.label || "证据")}</span>
            <div class="insight-evidence-snippet">${escHtml(item.snippet || "")}</div>
          </div>
        `
      )
      .join("");
  }

  function renderCompareMatrix(items, mode = state.activeInsightMode) {
    const container = document.getElementById("compare-result-matrix");
    const card = document.getElementById("compare-result-card");
    if (!container || !card) {
      return;
    }

    if (mode !== "compare" || !items || items.length === 0) {
      card.classList.add("section-hidden");
      container.innerHTML = "";
      return;
    }

    card.classList.remove("section-hidden");
    container.innerHTML = items
      .map(
        (item) => `
          <div class="compare-matrix-item">
            <strong>${escHtml(item.label || "比较维度")}</strong>
            <p>${escHtml(item.summary || "")}</p>
          </div>
        `
      )
      .join("");
  }

  function renderCompareReport(report, mode = state.activeInsightMode) {
    const container = document.getElementById("compare-report-content");
    const card = document.getElementById("compare-report-card");
    if (!container || !card) {
      return;
    }

    if (mode !== "compare" || !report || !report.executive_summary) {
      card.classList.add("section-hidden");
      container.innerHTML = "";
      return;
    }

    card.classList.remove("section-hidden");
    const commonPoints = (report.common_points || []).map((item) => `<div class="insight-item"><span class="insight-title">共同点</span><p>${escHtml(item)}</p></div>`).join("");
    const differencePoints = (report.difference_points || []).map((item) => `<div class="insight-item"><span class="insight-title">差异点</span><p>${escHtml(item)}</p></div>`).join("");
    const nextSteps = (report.next_steps || []).map((item) => `<div class="insight-item"><span class="insight-title">下一步</span><p>${escHtml(item)}</p></div>`).join("");

    container.innerHTML = `
      <div class="insight-item">
        <span class="insight-title">${escHtml(report.title || "差异对比报告")}</span>
        <p>${escHtml(report.executive_summary || "")}</p>
      </div>
      ${commonPoints}
      ${differencePoints}
      <div class="insight-item">
        <span class="insight-title">建议结论</span>
        <p>${escHtml(report.recommendation || "-")}</p>
      </div>
      ${nextSteps}
    `;
  }

  function renderInsightResult(data, mode = state.activeInsightMode) {
    const analysis = (data && data.analysis) || {};
    const panel = document.getElementById("insight-results-panel");
    if (panel) {
      panel.hidden = false;
    }

    setText("insight-result-headline", analysis.headline || "网页智能分析");
    setText("insight-result-page-type", data.page_type_label || data.page_type || "未知");
    setText("insight-result-confidence", analysis.confidence || "medium");
    setText("insight-result-summary", analysis.summary || "当前没有可展示的分析摘要。");

    renderSimpleInsightList(
      "insight-result-key-points",
      analysis.key_points || [],
      "当前没有提炼出核心发现。"
    );
    renderSimpleInsightList(
      "insight-result-risks",
      analysis.risks || [],
      "当前没有检测到明显风险。"
    );
    renderSimpleInsightList(
      "insight-result-actions",
      analysis.recommended_actions || [],
      "当前没有给出后续建议。"
    );
    renderSimpleInsightList(
      "insight-result-missing",
      analysis.missing_information || [],
      "当前信息已经足够形成基础分析。"
    );
    renderEvidenceList(analysis.evidence_spans || []);
    renderCompareMatrix(data.comparison_matrix || [], mode);
    renderCompareReport(data.report || {}, mode);
  }

  function applyInsightResult(data, mode = state.activeInsightMode) {
    const normalizedMode = normalizeInsightMode(mode);
    state.latestInsightAnalysisByMode = {
      ...(state.latestInsightAnalysisByMode || {}),
      [normalizedMode]: data,
    };
    state.latestInsightAnalysis = data;
    renderInsightResult(data, normalizedMode);
  }

  async function analyzeInsightPage() {
    const payload = collectInsightPayload();
    const button = document.getElementById("insight-analyze-page-btn");
    if (!payload.url) {
      showToast("请先输入分析 URL", "error");
      return;
    }

    const originalText = button.textContent;
    button.disabled = true;
    button.innerHTML = '<span class="spinner"></span>预分析中';

    try {
      const response = await apiFetch("/api/analyze_page", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          url: payload.url,
          use_static: payload.use_static,
        }),
      });
      const data = await response.json();
      if (!response.ok) {
        throw new Error(data.detail || "预分析失败");
      }

      state.latestInsightCandidateFields = data.candidate_fields || [];
      state.latestInsightFieldLabels = data.field_labels || {};
      setInsightFieldPreview(state.latestInsightCandidateFields, state.latestInsightFieldLabels);
      updateInsightPreview(data.preview || "");
      setText(
        "insight-detected-page-type",
        `页面类型：${data.page_type_label || data.page_type || "未知"}`
      );
      setText(
        "insight-page-help",
        "页面预览已生成。现在可以补充目标、限制条件和关注点，系统会输出更定制化的分析。"
      );
      showToast("URL 预分析完成，已填入页面线索", "success");
    } catch (error) {
      showToast(`预分析失败：${error.message}`, "error");
    } finally {
      button.disabled = false;
      button.textContent = originalText;
    }
  }

  async function submitInsightAnalysis(event) {
    event.preventDefault();

    if (state.activeInsightMode === "compare") {
      await submitCompareAnalysis(event);
      return;
    }

    const payload = collectInsightPayload();
    if (!payload.url) {
      showToast("请输入要分析的 URL", "error");
      return;
    }

    const button = document.getElementById("insight-submit-btn");
    const result = document.getElementById("insight-submit-result");

    button.disabled = true;
    button.innerHTML = '<span class="spinner"></span>分析中';
    result.style.display = "none";
    startInsightProgress();

    try {
      const response = await apiFetch("/api/analyze_insight", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });
      const data = await response.json();
      if (!response.ok) {
        throw new Error(data.detail || "分析失败");
      }

      state.latestInsightCandidateFields = data.candidate_fields || state.latestInsightCandidateFields;
      state.latestInsightFieldLabels = data.field_labels || state.latestInsightFieldLabels;
      setInsightFieldPreview(state.latestInsightCandidateFields, state.latestInsightFieldLabels);
      updateInsightPreview(data.page_preview || "");
      setText(
        "insight-detected-page-type",
        `页面类型：${data.page_type_label || data.page_type || "未知"}`
      );
      applyInsightResult(data, "single");
      finishInsightProgress("智能分析已完成，结论与建议已更新。");

      result.className = "submit-result success";
      result.textContent = "分析完成";
      result.style.display = "inline-flex";
      showToast("智能分析已生成", "success");
    } catch (error) {
      finishInsightProgress(`智能分析失败：${error.message}`);
      result.className = "submit-result error";
      result.textContent = `错误：${error.message}`;
      result.style.display = "inline-flex";
      showToast(`智能分析失败：${error.message}`, "error");
    } finally {
      button.disabled = false;
      button.textContent = "生成分析";
    }
  }

  async function submitCompareAnalysis(event) {
    if (event && typeof event.preventDefault === "function") {
      event.preventDefault();
    }

    const payload = collectComparePayload();
    if (payload.urls.length < 2) {
      showToast("请至少输入两个合法 URL 再进行对比分析", "error");
      return;
    }

    const button = document.getElementById("insight-submit-btn");
    const result = document.getElementById("insight-submit-result");

    button.disabled = true;
    button.innerHTML = '<span class="spinner"></span>对比中';
    result.style.display = "none";
    startInsightProgress();

    try {
      const response = await apiFetch("/api/analyze_compare", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });
      const data = await response.json();
      if (!response.ok) {
        throw new Error(data.detail || "对比分析失败");
      }

      state.latestComparePreview = data.items || state.latestComparePreview;
      renderComparePreviewBoard(state.latestComparePreview);
      applyInsightResult(data, "compare");
      finishInsightProgress("多 URL 对比分析已完成。");
      result.className = "submit-result success";
      result.textContent = "对比分析完成";
      result.style.display = "inline-flex";
      showToast("多 URL 对比分析已生成", "success");
    } catch (error) {
      finishInsightProgress(`对比分析失败：${error.message}`);
      result.className = "submit-result error";
      result.textContent = `错误：${error.message}`;
      result.style.display = "inline-flex";
      showToast(`对比分析失败：${error.message}`, "error");
    } finally {
      button.disabled = false;
      button.textContent = "生成对比分析";
    }
  }

  async function analyzePage() {
    const url = document.getElementById("url").value.trim();
    const useStatic = document.getElementById("static-mode").value === "true";
    const button = document.getElementById("analyze-page-btn");

    if (!url) {
      showToast("请先输入目标 URL", "error");
      return;
    }

    const originalText = button.textContent;
    button.disabled = true;
    button.innerHTML = '<span class="spinner"></span>分析中';

    startAnalyzeProgress();

    try {
      const response = await apiFetch("/api/analyze_page", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          url,
          use_static: useStatic,
        }),
      });
      const data = await response.json();
      if (!response.ok) {
        throw new Error(data.detail || "页面分析失败");
      }

      state.latestAnalyzedFields = data.candidate_fields || [];
      state.latestFieldLabels = data.field_labels || {};
      state.selectedFieldState = state.latestAnalyzedFields.map((name) => ({ name, checked: true }));
      renderFieldSelector();
      setText("detected-page-type", `页面类型：${data.page_type_label || data.page_type || "未知"}`);
      setText("field-selector-help", "系统已默认勾选全部候选字段，你可以取消部分字段后再提交任务。");
      updateAnalysisPreview(data.preview || "");
      finishAnalyzeProgress("页面分析完成，候选字段和预览已更新。");
      showToast("页面分析完成，已填入候选字段", "success");
    } catch (error) {
      finishAnalyzeProgress(`页面分析失败：${error.message}`);
      showToast(`页面分析失败：${error.message}`, "error");
    } finally {
      button.disabled = false;
      button.textContent = originalText;
    }
  }

  return {
    setInsightMode,
    updateCompareUrlSummary,
    renderComparePreviewBoard,
    renderNlTaskPreview,
    applyNaturalLanguagePlan,
    runNaturalLanguagePlan,
    saveMonitorFromNaturalLanguagePlan,
    openCompareFromNaturalLanguagePlan,
    exportInsightBrief,
    exportInsightJson,
    exportInsightMarkdown,
    exportInsightCsv,
    previewCompareTargets,
    setInsightFieldPreview,
    updateInsightPreview,
    analyzeInsightPage,
    submitInsightAnalysis,
    analyzePage,
  };
};
