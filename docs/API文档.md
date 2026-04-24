# Smart Data Extractor API 文档

本文档描述 Web 仪表盘后端提供的 HTTP API。  
默认服务地址：`http://127.0.0.1:8000`。

## 1. 总览

### 1.1 API 特性

- 接口风格：RESTful + JSON
- 后台任务：支持进程内异步执行，也支持 `queue` 队列模式交给独立 worker 消费
- 任务存储：SQLite 持久化（重启服务后保留）
- 身份认证：API Token（`X-API-Token` 或 `Authorization: Bearer <token>`）
- 限流：按客户端 IP 每分钟限流（默认 `120/min`）
- 管理能力：除基础提取外，还包含运行状态、基础配置、模板市场、站点记忆、监控与通知管理接口

### 1.2 状态说明

- `pending`：已创建，等待执行
- `running`：执行中
- `success`：执行成功
- `failed`：执行失败

## 2. 页面路由

### 2.1 仪表盘首页

- 方法：`GET`
- 路径：`/`
- 返回：HTML 页面

### 2.2 任务详情页

- 方法：`GET`
- 路径：`/task/{task_id}`
- 返回：HTML 页面
- 异常：任务不存在时返回 `404`

## 3. REST API

> 当配置了 `SMART_EXTRACTOR_WEB_API_TOKEN` 时，所有 `/api/*` 接口都要求携带 API Token。  
> 推荐请求头：`X-API-Token: <your-token>`。

### 3.1 提交单任务

- 方法：`POST`
- 路径：`/api/extract`
- 请求体：

```json
{
  "url": "https://example.com/article",
  "schema_name": "auto",
  "storage_format": "json",
  "use_static": false,
  "selected_fields": ["title", "content"]
}
```

- `schema_name` 说明：
  - `auto`：由 AI 自动判断页面类型与字段
  - `news` / `product` / `job` / 自定义：按固定 Schema 输出
- `selected_fields` 仅在 `auto` 模式下生效

- 响应体：

```json
{
  "task_id": "task-0001",
  "status": "pending",
  "message": "任务已创建: task-0001"
}
```

### 3.2 提交批量任务

- 方法：`POST`
- 路径：`/api/batch`
- 请求体：

```json
{
  "urls": [
    "https://example.com/a1",
    "https://example.com/a2"
  ],
  "schema_name": "news",
  "storage_format": "json"
}
```

- 响应体：

```json
{
  "task_ids": ["task-0002", "task-0003"],
  "count": 2,
  "message": "已创建 2 个任务"
}
```

### 3.3 查询任务列表

- 方法：`GET`
- 路径：`/api/tasks`
- 查询参数：
  - `limit`：返回数量上限，默认 `50`
- 响应体：

```json
{
  "tasks": [
    {
      "task_id": "task-0001",
      "url": "https://example.com/article",
      "schema_name": "news",
      "storage_format": "json",
      "status": "success",
      "created_at": "2026-03-27 09:30:01",
      "completed_at": "2026-03-27 09:30:06",
      "elapsed_ms": 4970.0,
      "quality_score": 0.92,
      "data": {},
      "error": null
    }
  ],
  "total": 1
}
```

### 3.4 查询单任务详情

- 方法：`GET`
- 路径：`/api/task/{task_id}`
- 响应体：任务对象（字段同上）
- 异常：任务不存在时返回 `404`

### 3.5 查询统计数据

- 方法：`GET`
- 路径：`/api/stats`
- 响应体：

```json
{
  "total": 10,
  "success": 7,
  "failed": 1,
  "running": 1,
  "pending": 1,
  "success_rate": "70.0%"
}
```

### 3.6 查询可用 Schema

- 方法：`GET`
- 路径：`/api/schemas`
- 响应体：

```json
{
  "schemas": [
    {
      "name": "auto",
      "class_name": "DynamicExtractResult",
      "field_count": 0,
      "fields": []
    },
    {
      "name": "news",
      "class_name": "NewsArticle",
      "field_count": 5,
      "fields": ["title", "author", "content", "publish_date", "tags"]
    }
  ]
}
```

### 3.7 查询当前配置（脱敏）

- 方法：`GET`
- 路径：`/api/config`
- 响应体：当前运行配置（`llm.api_key` 已脱敏）

### 3.8 读取/保存基础 LLM 配置

- `GET /api/config/basic`
  - 返回首页“基础配置”面板可编辑的字段，以及默认配置 / 本地覆盖配置 / 环境变量接管情况
- `POST /api/config/basic`
  - 保存目标：`config/local.yaml`
  - 说明：不会改动 `config/default.yaml`

### 3.9 查询运行状态

- 方法：`GET`
- 路径：`/api/runtime`
- 响应体：就绪状态、问题/告警列表，以及监控调度器、队列 worker、通知重试、Digest 服务的运行快照

### 3.10 模板、站点记忆、监控与通知接口

当前代码还提供以下管理类接口组，详细字段以实际响应为准：

- 模板市场：`/api/template_market`
- 站点记忆：`/api/learned_profiles`、`/api/learned_profiles/{profile_id}`
- 监控管理：`/api/monitors`、`/api/monitors/{monitor_id}`
- 通知中心：`/api/notifications`、`/api/notification_digest`
- 任务导出：`/api/task/{task_id}/export/docx`、`/api/task/{task_id}/export/xlsx`

### 3.11 从成功任务生成模板

- `GET /api/task/{task_id}/template_draft`
  - 根据成功任务生成模板草案
- `POST /api/task/{task_id}/template`
  - 直接将成功任务沉淀为模板

### 3.12 任务导出格式

`GET /api/task/{task_id}/export?format=...`

支持格式：

- `docx`
- `xlsx`
- `md`
- `json`

### 3.13 成本与收益字段

`/api/insights` 的 `summary` 现包含：

- `llm_total_calls`
- `llm_prompt_tokens`
- `llm_completion_tokens`
- `llm_total_tokens`
- `llm_estimated_cost_usd`
- `site_memory_estimated_saved_cost_usd`

说明：当前为估算值，便于做趋势观察和收益展示。

## 4. 错误处理

### 4.1 HTTP 错误

- `404`：任务不存在（`/api/task/{task_id}` 或 `/task/{task_id}`）
- `401`：API Token 无效
- `429`：触发限流
- `422`：请求体参数校验失败（FastAPI/Pydantic 自动返回）
- `500`：服务内部异常

说明：若服务未配置 Web API Token，则不会触发 `401`，接口将以无鉴权模式运行。

### 4.2 任务级错误

即使接口返回 `200`，任务仍可能在后台失败，请以任务状态为准。

- 失败时字段：
  - `status = "failed"`
  - `error` 包含错误类型和错误信息

## 5. 调用示例

### 5.1 `curl` 提交并轮询

```bash
# 1) 提交任务
curl -X POST "http://127.0.0.1:8000/api/extract" \
  -H "X-API-Token: your-web-api-token" \
  -H "Content-Type: application/json" \
  -d "{\"url\":\"https://example.com/article\",\"schema_name\":\"auto\",\"storage_format\":\"json\",\"use_static\":false,\"selected_fields\":[\"title\",\"content\"]}"

# 2) 查询列表
curl "http://127.0.0.1:8000/api/tasks?limit=20" \
  -H "X-API-Token: your-web-api-token"
```

### 5.2 健康检查

`docker-compose.yml` 中健康检查使用：

```bash
curl "http://127.0.0.1:8000/api/stats" \
  -H "X-API-Token: your-web-api-token"
```

## 6. 版本说明

- 文档版本：`v0.2.0`
- 对应代码分支：当前工作目录代码快照
