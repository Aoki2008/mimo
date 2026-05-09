"""
Auto-deploy engine: per-account scheduled deployment with 10-step flow.

Flow per account:
  0. Destroy claw (skip if no claw)
  1. Create claw
  2. Wait for claw creation to complete
  3. Send deploy text to claw
  4. Wait for claw reply to complete
  5. Capture SSH key from claw reply
  6. Add SSH key on jump server
  7. Reply to claw that key is added
  8. Test API endpoint
  9. Done → enter silent mode
"""
import json
import time
import threading
import subprocess
import re
import shlex
import uuid
from pathlib import Path
from datetime import datetime, timedelta
from croniter import croniter
from typing import Optional

CONFIG_PATH = Path(__file__).parent.parent / "data" / "auto_deploy.json"
LOG_DIR = Path(__file__).parent.parent / "data" / "deploy_logs"
HISTORY_DIR = Path(__file__).parent.parent / "data" / "deploy_history"

JUMP_SERVER = "149.88.90.137"
JUMP_USER = "root"


def _ensure_dirs():
    CONFIG_PATH.parent.mkdir(parents=True, exist_ok=True)
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    HISTORY_DIR.mkdir(parents=True, exist_ok=True)


def _save_run_history(account_filename: str, status: str, log_lines: list):
    """Save a completed run to history."""
    _ensure_dirs()
    safe_name = account_filename.replace("/", "_").replace("\\", "_")
    history_file = HISTORY_DIR / f"{safe_name}.json"
    
    history = []
    if history_file.exists():
        try:
            history = json.loads(history_file.read_text(encoding="utf-8"))
        except Exception:
            history = []
    
    history.append({
        "id": uuid.uuid4().hex[:8],
        "started_at": datetime.now().isoformat(),
        "status": status,
        "lines": log_lines,
    })
    
    # Keep last 50 runs
    history = history[-50:]
    history_file.write_text(json.dumps(history, indent=2, ensure_ascii=False), encoding="utf-8")


def get_run_history(account_filename: str) -> list:
    """Get run history for an account, newest first."""
    _ensure_dirs()
    safe_name = account_filename.replace("/", "_").replace("\\", "_")
    history_file = HISTORY_DIR / f"{safe_name}.json"
    
    if not history_file.exists():
        return []
    
    try:
        history = json.loads(history_file.read_text(encoding="utf-8"))
        history.reverse()
        return history
    except Exception:
        return []


def load_config() -> dict:
    _ensure_dirs()
    if CONFIG_PATH.exists():
        try:
            return json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {
        "accounts": {},
    }


def save_config(cfg: dict):
    _ensure_dirs()
    CONFIG_PATH.write_text(json.dumps(cfg, indent=2, ensure_ascii=False), encoding="utf-8")


def get_account_config(account_filename: str) -> dict:
    cfg = load_config()
    return cfg.get("accounts", {}).get(account_filename, {
        "enabled": False,
        "cron": "0 3 * * *",
    })


# ─── Log management ───

class DeployLogger:
    def __init__(self, account_filename: str):
        self.account = account_filename
        self.lines = []
        self._file = LOG_DIR / f"{account_filename.replace('/', '_')}.log"

    def log(self, msg: str):
        ts = datetime.now().strftime("%H:%M:%S")
        line = f"[{ts}] {msg}"
        self.lines.append(line)
        print(f"[deploy:{self.account}] {line}", flush=True)
        try:
            with open(self._file, "a", encoding="utf-8") as f:
                f.write(line + "\n")
        except Exception:
            pass

    def get_recent(self, n: int = 50) -> list:
        return self.lines[-n:]

    def clear(self):
        self.lines = []
        try:
            self._file.write_text("")
        except Exception:
            pass


# ─── Active deployments tracking ───

_active_deploys: dict = {}  # account_filename -> {"thread": Thread, "logger": DeployLogger, "state": str, "cancel": Event}
_scheduler_running = False
_scheduler_thread: Optional[threading.Thread] = None


def get_deploy_status(account_filename: str = None) -> dict:
    if account_filename:
        d = _active_deploys.get(account_filename)
        if d:
            return {
                "running": True,
                "state": d["state"],
                "log": d["logger"].get_recent(50),
            }
        return {"running": False, "state": "idle", "log": []}
    # All
    result = {}
    for acc, d in _active_deploys.items():
        result[acc] = {
            "running": True,
            "state": d["state"],
            "log": d["logger"].get_recent(20),
        }
    return result


# ─── SSH jump helper ───

def ssh_jump(command: str, timeout: int = 30) -> tuple:
    cmd = [
        "ssh", "-o", "StrictHostKeyChecking=no",
        "-o", "ConnectTimeout=10",
        f"{JUMP_USER}@{JUMP_SERVER}",
        command,
    ]
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
        return r.stdout, r.stderr, r.returncode
    except subprocess.TimeoutExpired:
        return "", "SSH timeout", 1
    except Exception as e:
        return "", str(e), 1


# ─── Core deploy flow ───

def _curl_api(method: str, path: str, body=None, with_ph: bool = True):
    """Import curl_api from app module at runtime to avoid circular imports."""
    import importlib
    app_mod = importlib.import_module("app")
    return app_mod.curl_api(method, path, body, with_ph)


def _claw_chat(message: str, session_key: str = None) -> str:
    """Call the claw chat endpoint synchronously."""
    if not session_key:
        session_key = "agent:main:auto-deploy-" + uuid.uuid4().hex[:8]
    import urllib.request
    try:
        req = urllib.request.Request(
            "http://127.0.0.1:8088/api/claw/chat",
            data=json.dumps({"message": message, "session_key": session_key}).encode(),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        req.add_header("Cookie", "mimo_panel_auth=aoki_mimo_2026")
        with urllib.request.urlopen(req, timeout=300) as resp:
            data = json.loads(resp.read())
            return data.get("reply", "")
    except Exception as e:
        return f"[ERROR] {e}"


def _parse_ssh_key(text: str) -> Optional[str]:
    """Extract SSH public key from text."""
    match = re.search(r'(ssh-(?:rsa|ed25519|ecdsa)\s+[A-Za-z0-9+/=]+(?:\s+\S+)?)', text)
    if match:
        return match.group(1).strip()
    return None


def _deploy_ssh_key(public_key: str, logger: DeployLogger) -> tuple:
    """Deploy SSH public key to jump server."""
    check_cmd = f'grep -qF {shlex.quote(public_key.strip())} /root/.ssh/authorized_keys 2>/dev/null && echo "EXISTS" || echo "NEW"'
    stdout, stderr, rc = ssh_jump(check_cmd)
    if "EXISTS" in stdout:
        logger.log("SSH key already exists")
        return True, "Key already deployed"

    add_cmd = f'echo {shlex.quote(public_key.strip())} >> /root/.ssh/authorized_keys && chmod 600 /root/.ssh/authorized_keys && echo "OK"'
    stdout, stderr, rc = ssh_jump(add_cmd)
    if rc != 0 or "OK" not in stdout:
        logger.log(f"Failed to deploy SSH key: {stderr}")
        return False, f"Failed: {stderr}"

    logger.log("SSH key deployed to jump server")
    return True, "Key deployed"



def run_deploy(account_filename: str, force: bool = False):
    """Execute the full 10-step deployment flow for one account."""
    cfg = load_config()
    acc_cfg = cfg.get("accounts", {}).get(account_filename, {})
    deploy_text = acc_cfg.get("deploy_text", "")
    logger = DeployLogger(account_filename)
    cancel_event = threading.Event()

    _active_deploys[account_filename] = {
        "thread": threading.current_thread(),
        "logger": logger,
        "state": "starting",
        "cancel": cancel_event,
        "started_at": datetime.now().isoformat(),
    }

    def set_state(s):
        _active_deploys[account_filename]["state"] = s

    def check_cancel():
        return cancel_event.is_set()

    try:
        logger.log("=== 开始部署 ===")

        # Switch to this account first
        import importlib
        app_mod = importlib.import_module("app")
        switch_result = app_mod.switch_to_account(account_filename)
        if not switch_result:
            logger.log(f"❌ 无法切换到账号 {account_filename}")
            set_state("error")
            return
        logger.log(f"已切换到账号 {account_filename}")

        # Step 0: Destroy existing claw
        set_state("step0_destroy")
        logger.log("Step 0: 检查并销毁旧 Claw...")
        code, data = _curl_api("GET", "/open-apis/user/mimo-claw/status", with_ph=False)
        has_claw = False
        if code == "HTTP_200" and isinstance(data, dict) and data.get("code") == 0:
            info = data.get("data", {})
            status = info.get("status", "")
            if status not in ("", "DESTROYED", "DESTROYING"):
                has_claw = True

        if has_claw:
            logger.log("发现旧 Claw，销毁中...")
            _curl_api("POST", "/open-apis/user/mimo-claw/destroy", body={})
            for _ in range(12):
                if check_cancel():
                    logger.log("⚠️ 部署已取消")
                    set_state("cancelled")
                    return
                time.sleep(5)
                code, data = _curl_api("GET", "/open-apis/user/mimo-claw/status", with_ph=False)
                if code == "HTTP_200" and isinstance(data, dict):
                    info = data.get("data", {})
                    if info.get("status") in ("DESTROYED", ""):
                        break
            logger.log("旧 Claw 已销毁")
        else:
            logger.log("无旧 Claw，跳过销毁")

        # Step 0.5: Clean up old tunnel processes on jump server
        set_state("step0_cleanup")
        ssh_port = acc_cfg.get("ssh_port", 8022)
        api_port = acc_cfg.get("api_port", 8800)
        ports_to_clean = [ssh_port, api_port]
        port_pattern = "|".join(str(p) for p in ports_to_clean)
        logger.log(f"Step 0.5: 清理跳板机旧隧道进程 (端口 {port_pattern})...")
        stdout, stderr, rc = ssh_jump(
            f"ss -tlnp | grep -E '{port_pattern}' | grep sshd | "
            f"grep -oP 'pid=\\K[0-9]+' | sort -u | xargs -r kill 2>/dev/null; echo DONE"
        )
        if rc == 0:
            logger.log(f"跳板机旧隧道已清理")
        else:
            logger.log(f"清理旧隧道: {stderr or '无残留'}")

        if check_cancel():
            set_state("cancelled")
            return

        # Step 1: Create claw
        set_state("step1_create")
        logger.log("Step 1: 创建新 Claw...")
        code, data = _curl_api("POST", "/open-apis/user/mimo-claw/create", body={})
        if not (isinstance(data, dict) and data.get("code") == 0):
            logger.log(f"❌ 创建 Claw 失败: {data}")
            set_state("error")
            return
        logger.log("Claw 创建请求已发送")

        # Step 2: Wait for claw to be ready
        set_state("step2_wait")
        logger.log("Step 2: 等待 Claw 就绪...")
        claw_ready = False
        for i in range(24):  # max 2 minutes
            if check_cancel():
                set_state("cancelled")
                return
            time.sleep(5)
            code, data = _curl_api("GET", "/open-apis/user/mimo-claw/status", with_ph=False)
            if code == "HTTP_200" and isinstance(data, dict) and data.get("code") == 0:
                info = data.get("data", {})
                if info.get("status") == "AVAILABLE":
                    claw_ready = True
                    break
            logger.log(f"  等待中... ({(i+1)*5}s)")

        if not claw_ready:
            logger.log("❌ Claw 启动超时")
            set_state("error")
            return
        logger.log("✅ Claw 就绪")

        if check_cancel():
            set_state("cancelled")
            return

        # Step 3: Send deploy text to claw
        set_state("step3_send")
        session_key = f"agent:main:auto-{account_filename}-{uuid.uuid4().hex[:8]}"

        if not deploy_text:
            logger.log("❌ 未配置部署文案，请在面板中填写")
            set_state("error")
            return

        logger.log(f"Step 3: 发送部署文案到 Claw ({len(deploy_text)} 字符)...")
        reply3 = _claw_chat(deploy_text, session_key)
        logger.log(f"Claw 回复: {reply3[:200]}...")

        if check_cancel():
            set_state("cancelled")
            return

        # Step 4: Capture SSH key from reply
        set_state("step4_capture")
        logger.log("Step 4: 从回复中提取 SSH 公钥...")

        public_key = _parse_ssh_key(reply3)
        if not public_key:
            # Ask claw for the key
            logger.log("未找到 SSH key，再次询问...")
            reply5 = _claw_chat("请把你的 SSH 公钥发给我，格式为 ssh-ed25519 或 ssh-rsa 开头的完整公钥。", session_key)
            logger.log(f"Claw 回复: {reply5[:200]}...")
            public_key = _parse_ssh_key(reply5)

        if not public_key:
            logger.log("❌ 无法从 Claw 回复中提取 SSH 公钥")
            set_state("error")
            return
        logger.log(f"✅ 提取到 SSH 公钥: {public_key[:50]}...")

        if check_cancel():
            set_state("cancelled")
            return

        # Step 5: Add SSH key on jump server
        set_state("step5_deploy_key")
        logger.log("Step 6: 在跳板机上添加 SSH 公钥...")
        key_ok, key_msg = _deploy_ssh_key(public_key, logger)
        if not key_ok:
            logger.log(f"❌ 部署公钥失败: {key_msg}")
            set_state("error")
            return
        logger.log(f"✅ 公钥部署成功: {key_msg}")

        if check_cancel():
            set_state("cancelled")
            return

        # Step 6: Reply to claw that key is added
        # Clean up tunnel ports AGAIN right before telling claw to build tunnel,
        # because keepalive scripts from previous deploys may have restarted them.
        set_state("step6_confirm")
        logger.log("Step 6: 再次清理跳板机隧道端口...")
        stdout2, stderr2, rc2 = ssh_jump(
            f"ss -tlnp | grep -E '{port_pattern}' | grep sshd | "
            f"grep -oP 'pid=\\K[0-9]+' | sort -u | xargs -r kill 2>/dev/null; echo DONE"
        )
        if rc2 == 0:
            logger.log(f"端口再次清理完成")
        else:
            logger.log(f"再次清理: {stderr2 or '无残留'}")

        logger.log("Step 6: 通知 Claw 公钥已添加...")
        confirm_msg = f"我已经把公钥添加到跳板机"
        reply7 = _claw_chat(confirm_msg, session_key)
        logger.log(f"Claw 回复: {reply7[:200]}...")

        if check_cancel():
            set_state("cancelled")
            return

        # Step 7: Done
        set_state("done")
        logger.log("=== ✅ 部署完成，进入静默模式 ===")
        _save_run_history(account_filename, "done", logger.lines[:])

    except Exception as e:
        logger.log(f"❌ 部署异常: {e}")
        set_state("error")
        _save_run_history(account_filename, "error", logger.lines[:])
    finally:
        # Keep status for 5 minutes, then clean up
        def cleanup():
            time.sleep(300)
            if account_filename in _active_deploys:
                del _active_deploys[account_filename]
        threading.Thread(target=cleanup, daemon=True).start()


def trigger_deploy(account_filename: str) -> dict:
    """Manually trigger deployment for an account."""
    if account_filename in _active_deploys and _active_deploys[account_filename]["state"] not in ("done", "error", "cancelled", "idle"):
        return {"success": False, "error": "该账号正在部署中"}

    t = threading.Thread(target=run_deploy, args=(account_filename,), daemon=True)
    t.start()
    return {"success": True, "message": f"已启动 {account_filename} 的部署"}


def cancel_deploy(account_filename: str) -> dict:
    """Cancel an active deployment."""
    d = _active_deploys.get(account_filename)
    if d and d["state"] not in ("done", "error", "cancelled"):
        d["cancel"].set()
        d["state"] = "cancelling"
        return {"success": True, "message": "正在取消..."}
    return {"success": False, "error": "没有进行中的部署"}


# ─── Scheduler ───

def _scheduler_loop():
    """Background loop that checks every minute if any account needs deployment."""
    global _scheduler_running
    _scheduler_running = True
    print("[scheduler] 启动自动部署调度器", flush=True)

    while _scheduler_running:
        try:
            cfg = load_config()
            accounts = cfg.get("accounts", {})

            for acc_filename, acc_cfg in accounts.items():
                if not acc_cfg.get("enabled", False):
                    continue

                cron_expr = acc_cfg.get("cron", "0 3 * * *")
                last_run = acc_cfg.get("last_run", 0)

                now = datetime.now()

                # Parse cron expression
                try:
                    cron = croniter(cron_expr, now)
                except (ValueError, KeyError):
                    continue

                # Get previous fire time (when this cron should have last fired)
                prev_fire = cron.get_prev(datetime)
                next_fire = cron.get_next(datetime)

                # Check if we should run: prev_fire is within last 2 minutes and we haven't run since then
                diff = abs((now - prev_fire).total_seconds())
                if diff <= 120:
                    if acc_filename not in _active_deploys or _active_deploys[acc_filename]["state"] in ("done", "error", "cancelled"):
                        print(f"[scheduler] 触发 {acc_filename} 的部署", flush=True)
                        # Update last_run
                        cfg["accounts"][acc_filename]["last_run"] = now.timestamp()
                        save_config(cfg)
                        trigger_deploy(acc_filename)

        except Exception as e:
            print(f"[scheduler] 错误: {e}", flush=True)

        time.sleep(60)  # Check every minute


def start_scheduler():
    global _scheduler_thread
    if _scheduler_thread and _scheduler_thread.is_alive():
        return
    _scheduler_thread = threading.Thread(target=_scheduler_loop, daemon=True)
    _scheduler_thread.start()


def stop_scheduler():
    global _scheduler_running
    _scheduler_running = False


def get_scheduler_status() -> dict:
    cfg = load_config()
    accounts = cfg.get("accounts", {})
    schedule_info = {}
    now = datetime.now()

    for acc_filename, acc_cfg in accounts.items():
        if not acc_cfg.get("enabled", False):
            schedule_info[acc_filename] = {"enabled": False}
            continue

        cron_expr = acc_cfg.get("cron", "0 3 * * *")
        last_run = acc_cfg.get("last_run", 0)

        try:
            cron = croniter(cron_expr, now)
            next_run = cron.get_next(datetime)
        except (ValueError, KeyError):
            schedule_info[acc_filename] = {"enabled": True, "cron": cron_expr, "error": "Cron 表达式格式错误"}
            continue

        schedule_info[acc_filename] = {
            "enabled": True,
            "cron": cron_expr,
            "last_run": datetime.fromtimestamp(last_run).strftime("%Y-%m-%d %H:%M") if last_run else "从未运行",
            "next_run": next_run.strftime("%Y-%m-%d %H:%M"),
        }

    return {
        "scheduler_running": _scheduler_running,
        "accounts": schedule_info,
    }
