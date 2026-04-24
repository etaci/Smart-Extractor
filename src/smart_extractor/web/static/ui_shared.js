"use strict";

window.SmartExtractorShared = (() => {
  const API_TOKEN_STORAGE_KEY = "smart_extractor_api_token";
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

  function setApiToken(token) {
    const value = String(token || "").trim();
    if (value) {
      localStorage.setItem(API_TOKEN_STORAGE_KEY, value);
    } else {
      localStorage.removeItem(API_TOKEN_STORAGE_KEY);
    }
  }

  function getAuthHeaders(extraHeaders = {}) {
    const headers = { ...extraHeaders };
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

  return {
    API_TOKEN_STORAGE_KEY,
    THEME_STORAGE_KEY,
    showToast,
    getApiToken,
    setApiToken,
    getAuthHeaders,
    setTheme,
    initTheme,
    downloadTextFile,
    downloadBinaryFile,
  };
})();
