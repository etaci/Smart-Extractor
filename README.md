# Smart Extractor：基于 LLM 的复杂网页数据智能提取与变化监控系统。

把网页抓取、正文清洗、结构化抽取、质量校验、结果存储、历史对比、监控调度与通知集合成一条可持续运行的工作流，适用于信息采集、竞品监控、价格跟踪、资讯归档和结构化数据沉淀。


## 主要功能与特色

### 1.抽取与分析

- 单 URL 提取及批量 URL 提取
- 自选Schema 提取与进行 `auto` 自动识别模式
- 单页分析与多页对比的分析
- 动态抓取与静态抓取的双模式

### 2.运行与存储

- 可以选择CLI 命令行入口或者Web本地网页
- 支持提取结果JSON / CSV / SQLite 多格式存储与导出
- 队列 worker 模式
- 任务历史、运行状态与统计面板

### 3.监控与运维

- 页面变化监控
- 自检 / 重试 / 冷却时间等通知策略
- Host、CSRF、请求体大小、安全响应头等基础安全控制
- 支持Docker / Docker Compose 部署

### 4.核心亮点

- 对页面结构变化更有韧性
- 更适合多站点、长周期、弱规则场景
- 支持从“单次提取”自然升级到“持续监控”

## 快速开始

### 0.环境要求

- Python `>= 3.12`
- [uv](https://docs.astral.sh/uv/)
- 首次使用动态抓取时需要安装 Playwright 浏览器

### 1. 安装依赖

```bash
uv sync --dev
uv run playwright install chromium
```

### 2. 启动Web端服务器

*（可选）如果需要把“提交任务”和“执行任务”拆开，可以改为队列模式：*

在`config/local.yaml`中编辑：

```yaml
web:
  task_dispatch_mode: "queue"
  start_builtin_worker: false
  worker_poll_interval_seconds: 2.0
```

然后启动：
（如果没有修改上述的队列模式，直接输入下面两句中的最下面这句命令即可）

```bash
uv run smart-extractor web
uv run smart-extractor web-worker
```

接下来访问本地地址：[`http://127.0.0.1:8000`](http://127.0.0.1:8000)

在网页中输入自己的API key、Base URL、MODEL与TEMPERATURE，保存基础配置后即可正常使用。

### 3. 配置本地参数（进阶用法）

配置示例：

```powershell
Copy-Item config/local.example.yaml config/local.yaml
```

然后编辑 `config/local.yaml`：

```yaml
llm:
  api_key: "your-api-key-here"
  base_url: "https://api.openai.com/v1"
  model: "gpt-4o-mini"
  temperature: 0.0

web:
  api_token: "your-web-api-token"
  task_dispatch_mode: "inline"
```

也可以直接使用环境变量覆盖：

```powershell
$env:SMART_EXTRACTOR_API_KEY="your-api-key-here"
$env:SMART_EXTRACTOR_WEB_API_TOKEN="your-web-api-token"
```

配置优先级：

`环境变量 > config/local.yaml > config/default.yaml`

然后测试 API 连通性

```bash
uv run smart-extractor test-api
```

执行一次提取

```bash
uv run smart-extractor extract "https://example.com/article" --schema news
```

设置完后，便可以启动 Web 仪表盘，也可以继续使用cli工作（下文的常用命令）

```bash
uv run smart-extractor web
```

然后打开本地地址：[`http://127.0.0.1:8000`](http://127.0.0.1:8000)，之后输入自己的API key等参数保存基础配置。


## 常用命令

### 查看可用 Schema

```bash
uv run smart-extractor schemas
```

### 自动识别页面类型并提取

```bash
uv run smart-extractor extract "https://example.com/page"
```

### 指定字段进行自动提取

```bash
uv run smart-extractor extract "https://example.com/page" --fields title,content,publish_date
```

### 静态抓取模式

```bash
uv run smart-extractor extract "https://example.com/page" --schema news --static
```

### 批量提取

```bash
uv run smart-extractor batch urls.txt --schema product --format csv
```

### 查看运行时概览

```bash
uv run smart-extractor runtime
```

### 查看监控与模板

```bash
uv run smart-extractor monitors
uv run smart-extractor templates
```

### 启动队列 worker

```bash
uv run smart-extractor web-worker
```

### 预置 Schema

项目内置以下常用 Schema，一般会自动选择：

- `auto`：自动判断页面类型并生成结构化结果
- `news`：新闻 / 博客文章
- `product`：商品详情页
- `job`：招聘信息页

自定义 Schema 放在 [`config/schemas`](./config/schemas) 目录下，使用 YAML 描述字段定义即可。


## Docker 部署

### 启动 Web 服务

```powershell
$env:SMART_EXTRACTOR_API_KEY="your-api-key-here"
$env:SMART_EXTRACTOR_WEB_API_TOKEN="your-web-api-token"
docker compose up -d --build
```

启动后访问：

[`http://localhost:8000`](http://localhost:8000)

### 启动队列模式

```powershell
$env:SMART_EXTRACTOR_API_KEY="your-api-key-here"
$env:SMART_EXTRACTOR_WEB_API_TOKEN="your-web-api-token"
$env:SMART_EXTRACTOR_WEB_TASK_DISPATCH_MODE="queue"
docker compose --profile queue up -d --build
```

### 常用容器命令

```bash
docker compose ps
docker compose logs -f web
docker compose run --rm extractor schemas
docker compose run --rm extractor extract "https://example.com/article" --schema news
```


## 技术栈
- Python 3.12
- `uv`
- Playwright
- httpx
- BeautifulSoup4 + lxml
- Pydantic v2 + pydantic-settings
- FastAPI + Uvicorn + Jinja2
- OpenAI SDK + Instructor
- Loguru
- Pytest


## 仓库结构

```Smart Extractor
.
├─ config/                     # 默认配置、自定义 Schema、示例本地配置
├─ docs/                       # API 文档、用户手册、产品规划等补充文档
├─ src/smart_extractor/
│  ├─ cleaner/                 # HTML 清洗
│  ├─ extractor/               # LLM 抽取、规则兜底、学习型画像
│  ├─ fetcher/                 # Playwright / httpx 抓取
│  ├─ models/                  # Pydantic 数据模型
│  ├─ scheduler/               # 批量任务调度
│  ├─ storage/                 # JSON / CSV / SQLite 存储
│  ├─ utils/                   # 日志、编码、重试等工具
│  ├─ validator/               # 数据质量校验
│  └─ web/                     # FastAPI 应用、管理路由、静态资源
├─ tests/                      # 测试用例
├─ Dockerfile
├─ docker-compose.yml
└─ pyproject.toml
```


## License

本项目在[MIT License](LICENSE)下发布。

## 联系我们

如有问题或建议，请通过以下方式联系：yitachi081@gmail.com

## 免责声明

本项目仅供个人使用，禁止用于进行恶意爬取等违法行为。若经发现则与本开发者无关。