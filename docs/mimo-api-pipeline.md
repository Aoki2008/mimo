# MiMo API 对外转发流程

## 链路

```
149跳板机:8317 (CLIProxyAPI) → 149:8800 (SSH隧道) → ECS:18800 (API代理) → MiMo API
```

## 一、ECS 侧

### 1. 生成 SSH 密钥

```bash
ssh-keygen -t ed25519 -f /root/.ssh/id_ed25519 -N "" -C "tunnel-ecs"
```

### 2. 部署公钥到跳板机

把 `/root/.ssh/id_ed25519.pub` 内容追加到跳板机 `/root/.ssh/authorized_keys`。

### 3. 设置权限

```bash
chmod 700 ~/.ssh
chmod 600 ~/.ssh/authorized_keys ~/.ssh/id_ed25519
chmod 644 ~/.ssh/id_ed25519.pub
```

### 4. 修改 sshd 配置（⚠️ 必须在建隧道之前完成）

编辑 `/etc/ssh/sshd_config`：

```
GatewayPorts clientspecified
```

**必须重启 sshd 使配置生效后再建隧道**，否则隧道端口只绑 127.0.0.1：

```bash
systemctl restart ssh
```

### 5. 创建 API 代理脚本

`/root/.openclaw/workspace/scripts/api-proxy.py`：

- 监听 `0.0.0.0:18800`
- 从 openclaw-gateway 进程环境读取 `MIMO_API_KEY` 和 `MIMO_API_ENDPOINT`
- **⚠️ 关键：`MIMO_API_ENDPOINT` 可能是完整路径（如 `https://api-sgp-oc.xiaomimimo.com/v1/chat/completions`），必须用 `urlparse` 提取 base URL（`scheme + netloc`）再拼接 `self.path`，否则路径重复导致 404**
- 自动注入 Bearer token，调用者无需提供 key
- 支持 GET/POST/PUT/DELETE，根路径 `/` 返回状态和模型列表

### 6. 注册 systemd 服务

```bash
cat > /etc/systemd/system/api-proxy.service << 'EOF'
[Unit]
Description=MiMo API Proxy
After=network.target

[Service]
Type=simple
ExecStart=/usr/bin/python3 /root/.openclaw/workspace/scripts/api-proxy.py
Restart=always
RestartSec=5
WorkingDirectory=/root/.openclaw/workspace/scripts

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable api-proxy
systemctl start api-proxy
```

### 7. 创建反向隧道脚本

`/root/.openclaw/workspace/scripts/reverse-tunnel.sh`：

通过跳板机暴露两个端口：

| 跳板机端口 | ECS 端口 | 用途 |
|-----------|---------|------|
| 8022 | 22 | SSH |
| 8800 | 18800 | API 代理 |

### 8. 创建保活脚本

`/root/.openclaw/workspace/scripts/tunnel-keepalive.sh`：检测断线自动重连，最多重试 3 次。

### 9. 设置 crontab

```bash
(crontab -l 2>/dev/null; echo "*/5 * * * * /root/.openclaw/workspace/scripts/tunnel-keepalive.sh") | crontab -
```

### 10. 建立隧道（⚠️ 必须在步骤 4 sshd 重启之后）

```bash
/root/.openclaw/workspace/scripts/reverse-tunnel.sh
```

### 11. 验证隧道绑定

```bash
# 在跳板机上验证端口绑定了 0.0.0.0（不是 127.0.0.1）
ssh root@149.88.90.137 "ss -tlnp | grep 8800"
# 应该看到 0.0.0.0:8800，不是 127.0.0.1:8800
```

## 二、跳板机侧（149.88.90.137）

### 1. 放行防火墙端口

```bash
firewall-cmd --add-port=8022/tcp --permanent
firewall-cmd --add-port=8800/tcp --permanent
firewall-cmd --reload
```

### 2. sshd 配置

`/etc/ssh/sshd_config`：

```
PubkeyAuthentication yes
GatewayPorts clientspecified
```

## 三、使用方式

```powershell
# 查看模型
curl.exe http://149.88.90.137:8317/v1/models -H "Authorization: Bearer 你的CLIP...Key"

# 调用聊天
curl.exe -X POST http://149.88.90.137:8317/v1/chat/completions `
  -H "Authorization: Bearer 你的CLIP...Key" `
  -H "Content-Type: application/json" `
  -d '{"model":"mimo-v2-pro","messages":[{"role":"user","content":"你好"}]}'
```

## 四、⚠️ 已知坑点

1. **API 代理路径重复** — `MIMO_API_ENDPOINT` 可能是完整路径，代理必须用 urlparse 提取 base URL 再拼接
2. **隧道绑 127.0.0.1** — sshd 的 `GatewayPorts` 必须在建隧道之前重启生效

## 五、相关文件

| 文件 | 位置 |
|------|------|
| API 代理 | ECS: `/root/.openclaw/workspace/scripts/api-proxy.py` |
| 隧道脚本 | ECS: `/root/.openclaw/workspace/scripts/reverse-tunnel.sh` |
| 保活脚本 | ECS: `/root/.openclaw/workspace/scripts/tunnel-keepalive.sh` |
| CLIProxyAPI 配置 | 跳板机: `/www/wwwroot/cliproxyapi/config.yaml` |
| 本文档 | ECS: `/root/.openclaw/workspace/docs/mimo-api-pipeline.md` |
