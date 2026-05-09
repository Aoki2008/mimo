"""
Deployment orchestrator for MiMo API pipeline.
Coordinates between Claw (ECS) and jump server.
"""
import json
import subprocess
import shlex
import re
import time
from dataclasses import dataclass, field
from typing import Optional
from enum import Enum
from pathlib import Path

JUMP_SERVER = "149.88.90.137"
JUMP_USER = "root"
DEPLOY_DOC = Path(__file__).parent.parent / "docs" / "mimo-api-pipeline.md"


class DeployState(Enum):
    IDLE = "idle"
    CLAW_CREATED = "claw_created"
    INSTRUCTIONS_SENT = "instructions_sent"
    WAITING_REPLY = "waiting_reply"
    DEPLOYING_KEY = "deploying_key"
    KEY_DEPLOYED = "key_deployed"
    TUNNEL_VERIFYING = "tunnel_verifying"
    DONE = "done"
    ERROR = "error"


@dataclass
class DeployContext:
    state: DeployState = DeployState.IDLE
    claw_session: Optional[str] = None
    public_key: str = ""
    ports: list = field(default_factory=list)
    error: str = ""
    log: list = field(default_factory=list)

    def add_log(self, msg: str):
        self.log.append(msg)

    def to_dict(self):
        return {
            "state": self.state.value,
            "public_key": self.public_key,
            "ports": self.ports,
            "error": self.error,
            "log": self.log[-20:],
        }


_deploy_ctx = DeployContext()


def get_deploy_context() -> DeployContext:
    return _deploy_ctx


def reset_deploy():
    global _deploy_ctx
    _deploy_ctx = DeployContext()


def parse_claw_reply(text: str) -> Optional[dict]:
    """Try to extract structured data from Claw's reply."""
    # 1. Try JSON block first
    patterns = [
        r'```json\s*(\{.*?\})\s*```',
        r'```\s*(\{.*?\})\s*```',
        r'(\{[^{}]*"action"[^{}]*\})',
    ]
    for pattern in patterns:
        match = re.search(pattern, text, re.DOTALL)
        if match:
            try:
                data = json.loads(match.group(1))
                if "action" in data:
                    return data
            except json.JSONDecodeError:
                continue

    # 2. Try to extract SSH public key from natural language
    ssh_key_match = re.search(r'(ssh-(?:rsa|ed25519|ecdsa)\s+[A-Za-z0-9+/=]+(?:\s+\S+)?)', text)
    if ssh_key_match:
        public_key = ssh_key_match.group(1).strip()
        # Try to find port numbers
        ports = []
        port_matches = re.findall(r'(?:端口|port)[:\s]*(\d{2,5})', text, re.IGNORECASE)
        for p in port_matches:
            ports.append(int(p))
        if not ports:
            ports = [8800]  # Default

        return {
            "action": "need_ssh_key_deploy",
            "public_key": public_key,
            "ports": ports,
            "message": text[:200],
        }

    # 3. Check for tunnel established keywords
    tunnel_keywords = ["隧道已建立", "tunnel_established", "隧道已连通", "tunnel active",
                       "隧道建立成功", "全链路验证通过", "隧道.*成功", "tunnel.*success"]
    for kw in tunnel_keywords:
        if re.search(kw, text, re.IGNORECASE):
            return {"action": "tunnel_established", "message": text[:200]}

    # 4. Check for error keywords
    if any(kw in text.lower() for kw in ["错误", "error", "失败", "failed"]):
        return {"action": "error", "message": text[:200]}

    return None


def ssh_jump(command: str, timeout: int = 30) -> tuple:
    """Execute a command on the jump server via SSH."""
    cmd = [
        "ssh", "-o", "StrictHostKeyChecking=no",
        "-o", "ConnectTimeout=10",
        f"{JUMP_USER}@{JUMP_SERVER}",
        command
    ]
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
        return r.stdout, r.stderr, r.returncode
    except subprocess.TimeoutExpired:
        return "", "SSH timeout", 1
    except Exception as e:
        return "", str(e), 1


def deploy_ssh_key(public_key: str) -> tuple:
    """Deploy SSH public key to jump server's authorized_keys."""
    ctx = get_deploy_context()
    ctx.add_log(f"Deploying SSH key to {JUMP_SERVER}...")

    check_cmd = f'grep -qF {shlex.quote(public_key.strip())} /root/.ssh/authorized_keys 2>/dev/null && echo "EXISTS" || echo "NEW"'
    stdout, stderr, rc = ssh_jump(check_cmd)

    if "EXISTS" in stdout:
        ctx.add_log("Key already exists in authorized_keys")
        return True, "Key already deployed"

    add_cmd = f'echo {shlex.quote(public_key.strip())} >> /root/.ssh/authorized_keys && chmod 600 /root/.ssh/authorized_keys && echo "OK"'
    stdout, stderr, rc = ssh_jump(add_cmd)

    if rc != 0 or "OK" not in stdout:
        ctx.add_log(f"Failed to deploy key: {stderr}")
        return False, f"Failed: {stderr}"

    ctx.add_log("SSH key deployed successfully")
    return True, "Key deployed"


def open_firewall_ports(ports: list) -> tuple:
    """Open firewall ports on jump server."""
    ctx = get_deploy_context()
    results = []

    for port in ports:
        ctx.add_log(f"Opening port {port} on {JUMP_SERVER}...")
        cmd = f"firewall-cmd --add-port={port}/tcp --permanent && firewall-cmd --reload && echo 'OK_{port}'"
        stdout, stderr, rc = ssh_jump(cmd)

        if f"OK_{port}" in stdout:
            results.append((port, True, "Opened"))
            ctx.add_log(f"Port {port} opened")
        elif "ALREADY_ENABLED" in stderr or "already" in stderr.lower():
            results.append((port, True, "Already open"))
            ctx.add_log(f"Port {port} already open")
        else:
            results.append((port, False, stderr[:100]))
            ctx.add_log(f"Failed to open port {port}: {stderr[:100]}")

    all_ok = all(r[1] for r in results)
    return all_ok, results


def verify_tunnel(port: int = 8800) -> tuple:
    """Verify that the tunnel port is accessible on jump server."""
    ctx = get_deploy_context()
    ctx.add_log(f"Verifying tunnel on port {port}...")

    # 1. Check port binding is 0.0.0.0 (not 127.0.0.1)
    bind_cmd = f"ss -tlnp | grep {port} | grep -q '0.0.0.0:{port}' && echo OK || echo BIND_FAIL"
    stdout, stderr, rc = ssh_jump(bind_cmd, timeout=10)
    if "BIND_FAIL" in stdout:
        ctx.add_log(f"Tunnel port {port} bound to 127.0.0.1, not 0.0.0.0 — need to re-establish tunnel")
        return False, f"Port {port} bound to 127.0.0.1 (GatewayPorts not active)"

    # 2. Try root endpoint first, then /v1/models
    for endpoint in ["/", "/v1/models"]:
        cmd = f"curl -s -o /dev/null -w '%{{http_code}}' --connect-timeout 5 http://172.17.0.1:{port}{endpoint}"
        stdout, stderr, rc = ssh_jump(cmd, timeout=15)

        if stdout.strip() in ("200", "401", "403"):
            ctx.add_log(f"Tunnel verified on port {port} (endpoint={endpoint})")
            return True, f"Tunnel active (HTTP {stdout.strip()})"

    ctx.add_log(f"Tunnel not ready on port {port}")
    return False, f"Not ready (HTTP {stdout.strip()})"


def generate_deploy_instructions(port: int = 8800) -> str:
    """Load deployment instructions from the documentation file."""
    # 根据端口选择对应的文档
    doc_path = DEPLOY_DOC.parent / f"mimo-api-pipeline-{port}.md"
    if doc_path.exists():
        return doc_path.read_text(encoding="utf-8")
    # 回退到默认文档
    if DEPLOY_DOC.exists():
        return DEPLOY_DOC.read_text(encoding="utf-8")
    # Fallback
    return "请参照 MiMo API 管道部署文档执行部署。"


def generate_confirm_message() -> str:
    """Generate confirmation message after key deployment."""
    return """跳板机侧已配置完成：
- SSH 公钥已部署到 149.88.90.137
- 防火墙端口已开放

请建立反向隧道（⚠️ 确保 GatewayPorts 已重启生效后再建隧道）。

完成后回复：
```json
{
  "action": "tunnel_established",
  "message": "隧道已建立"
}
```
"""
