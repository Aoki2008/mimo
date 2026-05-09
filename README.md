# MiMo Manager

小米 MiMo Studio (`aistudio.xiaomimimo.com`) 的自动化管理工具。纯 HTTP / WebSocket 实现，无需浏览器。

包含三块功能：
1. **Claw 自动化** — 登录、Cookie 管理、对话、定时部署。
2. **API Gateway** — 把 MiMo 包装成 OpenAI / Anthropic 兼容的 API 端点。
3. **管理面板** — Web UI，查看账号、部署、流量、节点状态。

---

## 快速开始

```bash
pip install -r requirements.txt

# 1. 登录小米账号（可重复加多个账号）
python claw/mimo_auth.py login

# 2. 启动管理面板（默认 8088）
python app.py
# 或
bash run.sh
```

打开 `http://localhost:8088`，默认密码 `Aoki-MiMo`。
公开统计页（无需登录）：`http://localhost:8088/stats`

环境变量：
- `MIMO_EMAIL`、`MIMO_PASSWORD` — 跳过交互输入
- `GATEWAY_CONFIG` — 新版 pipeline 的 yaml 配置路径（见下文「Gateway 状态」）

---

## 项目结构

```
mimo/
├── app.py                  # 管理面板 + 旧版 gateway 入口（生产使用）
├── run.sh                  # 启动脚本
├── requirements.txt
├── gateway.example.yaml    # 新版 pipeline 配置示例
│
├── claw/                   # MiMo 自动化（原 core/）
│   ├── mimo_auth.py        # 小米 SSO 登录 + Cookie 管理
│   ├── mimo_chat.py        # HTTP/SSE 对话客户端
│   ├── mimo_ws_client.py   # WebSocket 对话客户端
│   ├── auto_deploy.py      # 定时部署调度器（10 步流程）
│   └── deploy.py           # 手动单次部署
│
├── gateway/                # API gateway（OpenAI / Anthropic 兼容）
│   ├── proxy.py            # ⭐ 旧版代理（生产路径，app.py 使用中）
│   ├── router.py           # ⭐ 旧版路由：从 auto_deploy.json 自动发现后端
│   ├── converter.py        # ⭐ 协议转换 Anthropic / Responses → Chat
│   ├── health.py           # ⭐ 后端健康巡检
│   ├── metrics.py          # ⭐ SQLite 指标存储 + 聚合查询
│   ├── vps_probe.py        # ⭐ VPS 节点 TCP 探针
│   │
│   ├── server.py           # 新版 FastAPI 入口（未接入 app.py，参考）
│   ├── handler.py          # 新版终端 handler
│   ├── transport.py        # 新版 httpx 上游
│   ├── adapters/           # 新版协议适配器
│   ├── core/               # 新版 pipeline / context / errors
│   ├── middleware/         # 新版中间件（auth/rate_limit/logging/timing）
│   ├── routing/            # 新版评分路由 + decision log
│   └── config/             # 新版 yaml 配置 + APIKeyStore
│
├── templates/
│   ├── index.html          # 管理面板（受密码保护）
│   └── stats.html          # 公开统计页（无需登录）
│
├── scripts/
│   └── api-proxy.py        # 独立 asyncio 代理（零依赖，遗留参考）
│
├── tests/                  # pytest，180+ 用例
├── docs/                   # 设计笔记
├── data/                   # 运行时（已 gitignore）
│   ├── api_keys.db         # API key 存储
│   ├── metrics.db          # 请求指标
│   ├── auto_deploy.json    # 部署配置 + 后端发现源
│   ├── deploy_history/
│   └── deploy_logs/
└── accounts/               # Cookie 存储（已 gitignore）
    ├── _current.json       # 当前活跃账号
    └── <email>.json
```

---

## Gateway 状态（必读）

`gateway/` 目录里同时存在 **两套** 实现，请勿混淆：

| 项 | 旧版（生产）| 新版（参考）|
|---|---|---|
| 入口 | `app.py` 的路由 | `gateway/server.py:create_app()` |
| 代理 | `gateway/proxy.py` | `gateway/handler.py` + `transport.py` |
| 路由 | `gateway/router.py` | `gateway/routing/router.py` |
| 后端发现 | 从 `data/auto_deploy.json` 自动读取 | 从 yaml 配置静态加载 |
| 协议 | `converter.py` 把 Anthropic/Responses 转成 Chat | `adapters/` 各协议独立 |
| 配置 | 全局常量 + json | `gateway.yaml` |
| 是否运行 | ✅ 是 | ❌ 否（仅 `gateway.metrics`、`vps_probe` 被 app.py 引用）|

新版 pipeline 测试齐全（180 用例通过），但 **未接入 `app.py`**。生产流量目前完全走旧版。
两者通过 `gateway/metrics.py` 的 SQLite 表共享指标，所以面板 / 公开统计页拿到的数据一致。

后续是合并新旧、还是删掉新版回归单一实现，待用户决定。

---

## 主要 API 端点

### 兼容端点（gateway 提供，app.py 路由）
- `POST /v1/chat/completions` — OpenAI Chat
- `POST /v1/messages` — Anthropic Messages
- `POST /v1/responses` — OpenAI Responses
- 鉴权：`Authorization: Bearer sk-...`，key 在管理面板创建

### 管理面板（密码保护）
- `GET /` — 主面板
- `GET /api/accounts` / `POST /api/account/*` — 账号管理
- `GET /api/deploy/*` — 部署调度
- `GET /api/gateway/metrics/{summary|hourly|backends|status}` — 指标
- `GET /api/gateway/vps` / `POST /api/gateway/vps/refresh` — 节点状态

### 公开端点（无需登录）
- `GET /stats` — 公开统计页
- `GET /api/public/stats` — 公开统计 JSON

---

## CLI 命令（claw）

```bash
python claw/mimo_auth.py status         # 查看 Cookie 状态
python claw/mimo_auth.py login          # 交互式登录
python claw/mimo_auth.py cookie-header  # 输出 Cookie Header
python claw/mimo_auth.py auto-refresh   # 自动续期（cron 友好）

python claw/mimo_chat.py "你好"         # 单轮对话
python claw/mimo_ws_client.py "你好"    # WebSocket 对话
```

---

## 登录流程（小米 SSO 逆向）

1. `GET /open-apis/v1/genLoginUrl` → 动态 callback URL
2. `GET /pass/serviceLogin` → 提取 `_sign`
3. `POST /pass/serviceLoginAuth2` → 提交账号密码
4. 若触发二次验证：`identity/list` → `verifyEmail` → `sendEmailTicket` → 输入验证码 → `result/check`
5. 跟随 302 跳转链拿到 `serviceToken`

Cookie 跨 `.account.xiaomi.com` / `.xiaomi.com` / `.xiaomimimo.com` 三个域，关键凭证：
| Cookie | 域 | 作用 |
|---|---|---|
| `serviceToken` | `.xiaomimimo.com` | API 鉴权 |
| `userId` | `.xiaomimimo.com` | 用户 ID |
| `xiaomichatbot_ph` | `.xiaomimimo.com` | 会话 |

---

## 测试

```bash
python -m pytest tests/ -q     # 全部
python -m pytest tests/test_metrics.py tests/test_vps_probe.py -v
```

---

## 依赖

- Python 3.8+
- `fastapi`、`uvicorn`、`jinja2`、`httpx`、`requests`、`websockets`、`croniter`、`pyyaml`

详见 `requirements.txt`。
