# Smart Extractor 项目产品规划书 (Product Roadmap & Execution Plan)

## 一、 项目愿景与定位

**项目愿景**：打造一款开箱即用、高度智能化、具备反检测能力的**现代化 Web 数据提取引擎**。
**核心价值**：通过结合大语言模型 (LLM) 和传统的静态/动态网页抓取技术，让非技术人员或开发者能够以最低的学习成本，高效、准确、稳定地从任何复杂的网页中提取结构化数据（如商品、新闻、招聘信息等）。
**产品基调**：质量优先于速度，注重系统的高可用性、易用性和扩展性（Production-Ready）。

---

## 二、 当前项目进展盘点（V0.1 基础设施搭建期）

目前项目已经完成了**核心骨架的搭建与基础提取链路的闭环**，主要包含以下模块：

1. **抓取模块 (Fetcher)**
   - 实现了基于静态页面的抓取 (`static.py`)
   - 实现了基于 Playwright 的动态页面渲染与抓取 (`playwright.py`)
   - 抽象了统一的抓取接口 (`base.py`)
2. **数据清洗模块 (Cleaner)**
   - 实现了 HTML 的基础清洗与降噪 (`html_cleaner.py`)，为后续 LLM 提取准备高质量输入。
3. **数据模型提取 (Models)**
   - 构建了基于 Pydantic 的基础数据结构 (`base.py`)
   - 预设了针对特定垂直领域的提取模型：商品 (`product.py`)、新闻 (`news.py`)、招聘 (`job.py`)。
   - 支持自定义数据结构推断提取 (`custom.py`)。
4. **验证与存储 (Validator & Storage)**
   - 实现了提取数据的合规性验证 (`data_validator.py`)。
   - 支持多种持久化方式：CSV (`csv_storage.py`)、JSON (`json_storage.py`)、SQLite (`sqlite_storage.py`)。
5. **系统调度与应用层 (Scheduler & Pipeline)**
   - 具备基础的防检测机制 (`anti_detect.py`) 和 重试机制 (`retry.py`)。
   - 设计了完整的提取流水线 (`pipeline.py`) 和命令行交互入口 (`cli.py`, `main.py`)。
   - 已编写测试用例覆盖主流程可行性，并通过 `tests/` 目录中的集成测试验证关键链路。

**目前所处阶段结语**：0 到 1 的“可用性”已经验证，底层工程结构非常清晰，为后续的“智能化”和“工程化”打下了极佳的基础。

---

## 三、 短期规划：LLM 深度融合与工程强化（第 1-2 个月）

**目标**：将 LLM 真正作为数据提取的“大脑”，并增强爬虫的生存能力（反反爬）与批量处理能力，达到**准商业可用级**。

### 1. LLM 核心提取逻辑落地 (AI Extractor)
- **痛点**：目前结构有模型定义，但如何将 HTML 配合 Prompt 稳定送入 LLM 并获得 JSON 格式输出，需要精细化控制。
- **Action**：
  - 引入 `LLMExtractor`，对接 OpenAI / 阿里千问 / 智谱 等主流大模型 API。
  - **Context Window 优化**：针对超长网页，开发 HTML 截断、DOM 树精简与分块策略（Chunking）。
  - **Prompt Engineering**：沉淀不同垂直领域（商品/新闻/招聘）的固定 System Prompt，提升 JSON 格式化输出的成功率。

### 2. 反检测机制 (Anti-Detect) 升级
- **Action**：
  - 深度定制 Playwright：集成 `playwright-stealth`，修改浏览器指纹（UserAgent, Canvas, WebGL, WebDriver 属性）。
  - 代理 IP 池接入：在 Fetcher 层支持轮询代理（Proxy Pool API 接入）。
  - 拟人化操作模拟：在抓取时加入随机滚动、鼠标悬停、随机等待时间等动作。

### 3. 任务调度与高并发 (Batch Processing)
- **Action**：
  - 升级 `task_manager.py`，从单线程/简单异步升级为基于 `asyncio` 结合任务队列的批量处理架构。
  - 支持从外部文件（如 TXT, CSV）批量导入 URL 进行并发抓取。
  - 完善失败重试机制（针对 LLM 断连、IP 被封、元素未加载等细分错误采取不同策略）。

---

## 四、 中期规划：可视化与易用性提升（第 3 个月）

**目标**：降低使用门槛，让不懂代码的业务人员（如运营、数据分析师）也能流畅使用，实现从“工具”到“产品”的跨越。

### 1. Web UI 控制台 (Dashboard)
- **Action**：
  - 基于 FastAPI (后端) + Vue3 / React (前端) 搭建可视化管理台。
  - **任务管理看板**：可视化创建抓取任务、配置并发数、选择代理策略。
  - **运行监控概览**：实时查看抓取成功率、LLM Token 消耗成本、任务进度条。

### 2. 交互式提取配置 (No-Code Extraction)
- **Action**：
  - 用户只需输入目标 URL 和期望获得的 JSON 结构（或用自然语言描述：“帮我提取这个页面的商品价格、标题和所有评论”）。
  - 系统自动后台利用 LLM 推断并在可视化界面中高亮提取结果，用户确认无误后保存为“提取模板”进行批量任务。

### 3. 数据导出与 Webhook
- **Action**：
  - 支持将清洗好的数据直接通过 Webhook 推送至用户的内部系统（如钉钉、飞书机器人、企业自建 CRM）。
  - 数据库直连：支持一键导出到 MySQL / PostgreSQL / MongoDB 等常用业务数据库。

---

## 五、 长期规划：平台化与数据资产化（第 6 个月及以后）

**目标**：将 Smart Extractor 打造成企业级的数据采集基础设施或云端 SaaS 服务。

### 1. 云端分布式部署 (Cloud-Native Deployment)
- **Action**：
  - **Docker 化与 K8s 支持**：将 Fetcher, LLM Provider, Scheduler 完全解耦微服务化。
  - 支持通过 Celery / RabbitMQ / Redis 实现多台机器分布式抓取网络，突破单机性能瓶颈。

### 2. 智能异常自愈 (Self-Healing Extraction)
- **Action**：
  - 当目标网站改版导致传统 XPath/CSS Selector 失效时，系统通过对比历史数据，利用视觉模型 (VLM) 和 LLM 自动重新定位数据节点，实现**代码免维护**的自修复抓取。

### 3. 数据洞察与二次加工 (Data Insights)
- **Action**：
  - 不仅做“搬运工”，还做“分析师”。在数据入库前，利用 LLM 进行情感分析（如商品评论正负面）、数据摘要生成（如长篇新闻提取核心观点）、数据去重与翻译。

---

## 六、 PM 给研发团队的当前行动建议 (Next Steps)

为了不偏离上述规划，针对当前的 V0.1 代码，我建议接下来的 **Step 1 实施重点**为：

1. **打通大模型 API**：在 `base.py` 旁新增 `llm_extractor.py`，尝试把 `html_cleaner.py` 洗干净的 HTML 丢给 LLM，测试能否成功反射出 `models/product.py` 中的数据结构。
2. **完善 Playwright 隐身能力**：目前的动态页面抓取还要应对常见的 Cloudflare / 验证码 拦截，需要丰富 `anti_detect.py` 的策略。
3. **跑通一个真实业务场景的 Demo**：比如用目前的架构去批量抓取 10 个京东/亚马逊商品页，或 10 篇新闻，将抓取成功率、解析准确率、Token 成本计算出来，作为评估 Baseline。

这三点夯实后，我们再向“高并发调度”和“可视化 WebUI”迈进！
