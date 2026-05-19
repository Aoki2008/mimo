# 验证报告

- 日期：2026-05-19
- 执行者：Codex
- 范围：MiMo Studio gateway 生产错误日志审查、本地代码修复、Claw 轮换安全重构与自动化验证

## 结论

本地修复已覆盖生产 `error.log` 中最高频和可归因的错误组，并额外重构了 Claw/backend 轮换安全策略：没有可接管的健康后端时，自动部署会跳过销毁旧 Claw，避免在新 Claw 创建的 5-10 分钟内主动制造 `no backend` 窗口。云端仅用于只读拉取日志，没有修改远程代码。

## 生产问题与修复映射

| 生产日志问题 | 修复位置 | 验证 |
|---|---|---|
| `reasoning_content` 在 thinking 模式多轮工具调用中缺失，导致 400 | `gateway/adapters/openai_chat.py`, `gateway/handler.py` | `tests/test_openai_chat.py` |
| 上游 `401 Invalid API Key` 未作为 backend 故障处理 | `gateway/handler.py` | `tests/test_handler.py::test_non_stream_401_marks_backend_failure_and_retries` |
| Anthropic passthrough `tool_choice` 传入字符串导致 400 | `gateway/anthropic_passthrough.py`, `gateway/handler.py` | `tests/test_handler.py::test_anthropic_passthrough_normalizes_string_tool_choice` |
| TTS 请求缺少 assistant role 导致上游 400 | `gateway/handler.py` | `tests/test_handler.py::test_tts_requests_are_rejected_before_upstream` |
| 图片输入路由到非视觉模型导致 404 | `gateway/handler.py` | `tests/test_handler.py::test_image_requests_are_rejected_for_non_vision_models` |
| warming backend 与探针失败状态影响路由稳定性 | `gateway/routing/backend.py`, `gateway/routing/router.py`, `gateway/runtime.py` | `tests/test_lifecycle_rotation.py`, `tests/test_routing.py` |
| 自动部署在无接管后端时销毁唯一 Claw，造成 no backend 空窗 | `gateway/runtime.py`, `claw/auto_deploy.py` | `tests/test_auto_deploy_safety.py`, `tests/test_lifecycle_rotation.py` |

## Claw 轮换机制调整

- 默认 backend 轮换年龄从 50 分钟改为 40 分钟，可通过 `GATEWAY_ROTATION_INTERVAL_S` 覆盖。
- `prepare_account_deploy()` 只允许由当前可选的同模型 backend 接管；处于 detection、dead、breaker open、disabled、draining、未 ready warming 或饱和状态的 peer 不再视为可接管。
- `auto_deploy` 在 `safe_to_destroy=False` 时将本次部署标记为 `skipped` 并直接退出，不进入 Step 0 销毁旧 Claw。
- `skipped` 已加入部署终态，避免安全跳过后调度器/UI 误判为仍在运行。

## 执行命令

```powershell
python -m pytest tests/test_lifecycle_rotation.py -q
python -m pytest tests/test_auto_deploy_safety.py -q
python -m pytest tests/ -q
git diff --check
```

## 风险与后续

- 单账号、单 Claw 无法做到无感续命：如果上游不允许在同账号旧 Claw 存活时创建新 Claw，代码只能选择保留旧 Claw并跳过危险销毁，不能凭空产生接管容量。
- 要彻底避免 `no backend mimo-v2.5-pro`，生产上至少需要两个可用账号/后端同时覆盖 `mimo-v2.5-pro`，让自动部署能先 drain 再销毁。
- FastAPI `on_event` 弃用警告不是本次缺陷路径，后续可单独迁移到 lifespan。
