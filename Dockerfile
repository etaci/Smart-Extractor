# ============================================================
# Smart Data Extractor — Dockerfile
# 多阶段构建：builder 安装依赖，runtime 运行服务
# ============================================================

# ---------- 阶段一：依赖构建 ----------
FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim AS builder

WORKDIR /app

# 复制依赖描述文件（利用 Docker 缓存层）
COPY pyproject.toml uv.lock README.md ./

# 先安装第三方依赖（不安装本项目包）
RUN uv sync --frozen --no-dev --no-install-project

# 再复制源码并安装项目包（非 editable）
COPY src/ ./src/
RUN uv sync --frozen --no-dev --no-editable


# ---------- 阶段二：运行时镜像 ----------
FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim AS runtime

ARG DEBIAN_MIRROR=https://mirrors.ustc.edu.cn/debian
ARG DEBIAN_SECURITY_MIRROR=https://mirrors.ustc.edu.cn/debian-security

RUN set -eux; \
    if [ -f /etc/apt/sources.list.d/debian.sources ]; then \
        sed -i "s|http://deb.debian.org/debian|${DEBIAN_MIRROR}|g; s|http://deb.debian.org/debian-security|${DEBIAN_SECURITY_MIRROR}|g" /etc/apt/sources.list.d/debian.sources; \
    else \
        sed -i "s|http://deb.debian.org/debian|${DEBIAN_MIRROR}|g; s|http://deb.debian.org/debian-security|${DEBIAN_SECURITY_MIRROR}|g" /etc/apt/sources.list; \
    fi

# 安装 Playwright 所需的系统依赖（Chromium）
RUN set -eux; \
    for attempt in 1 2 3; do \
        apt-get update && apt-get install -y --no-install-recommends -o Acquire::Retries=3 \
            libnss3 \
            libnspr4 \
            libatk1.0-0 \
            libatk-bridge2.0-0 \
            libcups2 \
            libdrm2 \
            libdbus-1-3 \
            libxcb1 \
            libxkbcommon0 \
            libx11-6 \
            libxcomposite1 \
            libxdamage1 \
            libxext6 \
            libxfixes3 \
            libxrandr2 \
            libgbm1 \
            libpango-1.0-0 \
            libcairo2 \
            libasound2 \
            libatspi2.0-0 \
            fonts-noto-cjk && break; \
        if [ "$attempt" -eq 3 ]; then \
            exit 1; \
        fi; \
        rm -rf /var/lib/apt/lists/*; \
        sleep 5; \
    done; \
    rm -rf /var/lib/apt/lists/*

# 创建非 root 运行用户（安全最佳实践）
RUN groupadd --gid 1001 appuser && \
    useradd --uid 1001 --gid appuser --shell /bin/bash --create-home appuser

WORKDIR /app

# 从 builder 阶段复制虚拟环境
COPY --from=builder /app/.venv /app/.venv

# 复制项目源码和配置
COPY src/ ./src/
COPY config/ ./config/

# 安装 Playwright Chromium 浏览器（在虚拟环境 Python 下运行）
RUN /app/.venv/bin/python -m playwright install chromium

# 创建数据卷目录并设置权限
RUN mkdir -p /app/output /app/logs && \
    chown -R appuser:appuser /app

# 切换非 root 用户
USER appuser

# 将虚拟环境 bin 加入 PATH
ENV PATH="/app/.venv/bin:$PATH" \
    PYTHONPATH="/app/src" \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    # 默认 LLM 配置（可通过 docker run -e 覆盖）
    SMART_EXTRACTOR_API_KEY="" \
    SMART_EXTRACTOR_BASE_URL="" \
    SMART_EXTRACTOR_MODEL=""

# 暴露 Web 服务端口
EXPOSE 8000

# 数据持久化卷
VOLUME ["/app/output", "/app/logs"]

# 默认启动：Web 仪表盘
# 可通过 docker run ... smart-extractor <command> 覆盖
CMD ["smart-extractor", "web", "--host", "0.0.0.0", "--port", "8000"]
