# 验证报告

- 日期：2026-05-19
- 执行者：Codex
- 范围：MiMo Studio gateway 生产错误日志审查、本地代码修复与自动化验证

## 结论

本地修复已覆盖生产 `error.log` 中最高频和可归因的错误组。全量 pytest 通过，diff whitespace 校验通过。云端仅用于只读拉取日志，没有修改远程代码。

## 生产问题与修复映射

| 生产日志问题 | 修复位置 | 验证 |
|---|---|---|
| `reasoning_content` 在 thinking 模式多轮工具调用中缺失，导致 400 | `gateway/adapters/openai_chat.py`, `gateway/handler.py` | `tests/test_openai_chat.py` 新增 stripped thinking 回填测试 |
| 上游 `401 Invalid API Key` 未作为 backend 故障处理 | `gateway/handler.py` | `tests/test_handler.py::test_non_stream_401_marks_backend_failure_and_retries` |
| Anthropic passthrough `tool_choice` 传入字符串导致 400 | `gateway/anthropic_passthrough.py`, `gateway/handler.py` | `tests/test_handler.py::test_anthropic_passthrough_normalizes_string_tool_choice` |
| TTS 请求缺少 assistant role 导致上游 400 | `gateway/handler.py` | `tests/test_handler.py::test_tts_requests_are_rejected_before_upstream` |
| 图片输入路由到非视觉模型导致 404 | `gateway/handler.py` | `tests/test_handler.py::test_image_requests_are_rejected_for_non_vision_models` |
| warming backend 与探针失败状态影响路由稳定性 | `gateway/routing/backend.py`, `gateway/routing/router.py`, `gateway/runtime.py` | `tests/test_lifecycle_rotation.py`, `tests/test_routing.py` |

## 执行命令

```powershell
python -m pytest tests/ -q
git diff --check
```

## 执行结果

- `python -m pytest tests/ -q`：247 passed, 4 warnings in 3.95s
- `git diff --check`：通过；仅有 LF/CRLF 工作区提示

## 风险与后续

- 生产端仍需在部署后观察新 `error.log`，确认 2026-05-19 00:04:54 至 13:42:44 期间的错误签名不再增长。
- 401 的根因通常是某个 backend 的上游 API Key 失效；本次修复会隔离并重试，但仍建议运维替换失效 key。
- FastAPI `on_event` 弃用警告不是本次缺陷路径，后续可单独迁移到 lifespan。
