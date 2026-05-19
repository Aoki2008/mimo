# 测试记录

- 日期：2026-05-19
- 执行者：Codex
- 任务：生产环境 gateway 缺陷审查与修复

## 生产日志输入

- 只读来源：`root@149.88.90.137:/root/mimo/logs/error.log`
- 本地原始副本：`C:\Users\sikuai\Desktop\develop\mimo\.scratch_remote_logs\20260519_143202_149.88.90.137_error.log_raw_error.log`
- 本地摘要：`C:\Users\sikuai\Desktop\develop\mimo\.scratch_remote_logs\20260519_143202_error_analysis.md`
- 时间范围：2026-05-19 00:04:54 至 2026-05-19 13:42:44
- 统计：284 个错误块，12 类唯一签名；HTTP 400=243，401=38，404=3

## 关键验证命令

```powershell
python -m pytest tests/ -q
```

结果：

```text
247 passed, 4 warnings in 3.95s
```

警告均为 FastAPI `on_event` 弃用警告，位于 `app.py:147` 与 `app.py:179`，不属于本次 gateway 缺陷修复路径。

```powershell
git diff --check
```

结果：通过，无 whitespace error。PowerShell 输出了 Git 在 Windows 上的 LF/CRLF 提示，不影响 diff 校验。

## 回归覆盖

- reasoning cache 在 OpenAI Chat thinking 字段被客户端剥离后的回填。
- OpenAI Chat reasoning cache 的不同 thinking 配置隔离。
- 上游 401 Invalid API Key 标记 backend failure 并重试下一 backend。
- 普通上游 400 仍不污染 backend 健康状态。
- Anthropic passthrough `tool_choice` 字符串简写归一化。
- TTS 请求在上游前拒绝无 assistant role 的错误消息形态。
- 非视觉模型在上游前拒绝图片输入。
- warming backend 仅在 readiness 成功后可参与路由。
- backend routing score 对高失败率 backend 增加惩罚。
- probe failure 与流量失败计数分开跟踪。

## 清理

- 已删除本地临时私钥副本：`C:\Users\sikuai\Desktop\develop\mimo\.scratch_remote_logs\id_ed25519`
- 未修改云端代码。
