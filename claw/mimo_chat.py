#!/usr/bin/env python3
"""
MiMo Chat HTTP/SSE 客户端
========================
纯 HTTP 调用 MiMo Chat API，支持流式输出、多轮对话和完整 API 操作。

用法:
  python3 mimo_chat.py "你好"                    # 单轮对话
  python3 mimo_chat.py --session my "1+1=?"      # 多轮对话
  python3 mimo_chat.py --thinking "解释量子计算"   # 深度思考
  python3 mimo_chat.py --quiet --usage "hello"   # 静默+用量
  python3 mimo_chat.py --model mimo-v2-flash "hi" # 指定模型
  python3 mimo_chat.py --list-conversations       # 列出所有会话
  python3 mimo_chat.py --history <convId>         # 查看会话历史
  python3 mimo_chat.py --user-info                # 用户信息
  python3 mimo_chat.py --bot-config               # Bot 配置
  python3 mimo_chat.py --delete <convId>          # 删除会话
  python3 mimo_chat.py --host-files               # 宿主文件列表
  python3 mimo_chat.py --ws-ticket                # WebSocket 票据
  python3 mimo_chat.py --claw-status              # MiMo Claw 状态
  python3 mimo_chat.py --share <convId>           # 创建分享链接
  python3 mimo_chat.py --upload <file> <type>     # 获取上传 URL
  python3 mimo_chat.py --cookie-header            # 输出 cookie
"""

import json
import os
import re
import sys
import time
import uuid
import subprocess
from pathlib import Path
from urllib.parse import quote

COOKIE_FILE = Path("/tmp/mimo_cookies.json")
MIMO_BASE = "https://aistudio.xiaomimimo.com"
SESSION_FILE = Path("/tmp/mimo_chat_sessions.json")
DEFAULT_MODEL = "mimo-v2.5-pro"


def _clean_val(v):
    return v[1:-1] if v.startswith('"') and v.endswith('"') else v


def get_cookie_parts():
    if not COOKIE_FILE.exists():
        print("❌ Cookie 文件不存在，先运行: python3 mimo_auth.py login", file=sys.stderr)
        sys.exit(1)
    with open(COOKIE_FILE) as f:
        cookies = json.load(f)
    parts, ph = [], None
    for c in cookies:
        if "xiaomimimo" in c.get("domain", ""):
            val = _clean_val(c["value"])
            parts.append(f"{c['name']}={val}")
            if c["name"] == "xiaomichatbot_ph":
                ph = val
    if not parts:
        print("❌ 无有效 cookie", file=sys.stderr)
        sys.exit(1)
    return "; ".join(parts), ph


def _api(method, path, body=None, raw=False):
    """通用 API 调用，返回 (status_code, response_json_or_text)"""
    cookie_header, ph = get_cookie_parts()
    ph_enc = quote(ph, safe="")
    url = f"{MIMO_BASE}/open-apis{path}?xiaomichatbot_ph={ph_enc}"
    cmd = ["curl", "-s", "-w", "\nHTTP_%{http_code}", "-X", method, url,
           "-H", f"cookie: {cookie_header}", "-H", "content-type: application/json"]
    if body:
        cmd += ["-d", json.dumps(body, ensure_ascii=False)]
    r = subprocess.run(cmd, capture_output=True, text=True, timeout=15)
    lines = r.stdout.strip().split("\n")
    code = lines[-1] if lines[-1].startswith("HTTP_") else "?"
    resp_text = "\n".join(lines[:-1])
    if raw:
        return code, resp_text
    try:
        return code, json.loads(resp_text)
    except json.JSONDecodeError:
        return code, resp_text


class SSEParser:
    """解析 MiMo Chat SSE 流，分离 thinking 和 reply"""

    def __init__(self):
        self.in_thinking = False
        self.thinking_buf = ""
        self.reply_buf = ""
        self.full_reply = ""
        self.usage = {}
        self.dialog_id = None

    def feed(self, event, data):
        """处理一个 SSE 事件，返回是否有新回复文本"""
        if event == "message":
            try:
                j = json.loads(data)
                content = j.get("content", "")
            except json.JSONDecodeError:
                return ""
            if not content:
                return ""
            content = content.replace("\u0000", "")

            if "<think>" in content:
                self.in_thinking = True
                after = content.split("<think>", 1)[1]
                self.thinking_buf += after
                return ""

            if self.in_thinking:
                if "</think>" in content:
                    parts = content.split("</think>")
                    self.thinking_buf += parts[0]
                    self.in_thinking = False
                    remaining = parts[1] if len(parts) > 1 else ""
                    if remaining:
                        self.full_reply += remaining
                        return remaining
                    return ""
                else:
                    self.thinking_buf += content
                    return ""

            self.full_reply += content
            return content

        elif event == "usage":
            try:
                self.usage = json.loads(data)
            except json.JSONDecodeError:
                pass
            return ""

        elif event == "dialogId":
            try:
                self.dialog_id = data.strip()
            except Exception:
                pass
            return ""

        return ""


# ──────────────── 聊天核心 ────────────────

def chat(query, model=DEFAULT_MODEL, thinking=False,
         session_name=None, quiet=False, show_usage=False):
    cookie_header, ph = get_cookie_parts()
    ph_enc = quote(ph, safe="")

    sessions = {}
    if SESSION_FILE.exists():
        with open(SESSION_FILE) as f:
            sessions = json.load(f)

    conv_id = None
    if session_name:
        conv_id = sessions.get(session_name, {}).get("conversation_id")
        if not conv_id:
            conv_id = uuid.uuid4().hex
            url = f"{MIMO_BASE}/open-apis/chat/conversation/save?xiaomichatbot_ph={ph_enc}"
            body = {"conversationId": conv_id, "title": "New conversation", "type": "chat"}
            subprocess.run([
                "curl", "-s", url,
                "-H", f"cookie: {cookie_header}",
                "-H", "content-type: application/json",
                "-d", json.dumps(body, ensure_ascii=False)
            ], capture_output=True, timeout=10)
            sessions[session_name] = {"conversation_id": conv_id, "created": time.strftime("%Y-%m-%d %H:%M:%S")}
            with open(SESSION_FILE, "w") as f:
                json.dump(sessions, f, indent=2, ensure_ascii=False)
    else:
        conv_id = uuid.uuid4().hex

    chat_body = json.dumps({
        "msgId": uuid.uuid4().hex,
        "conversationId": conv_id,
        "query": query,
        "isEditedQuery": False,
        "modelConfig": {
            "enableThinking": thinking,
            "webSearchStatus": "disabled",
            "model": model,
        },
        "multiMedias": [],
    }, ensure_ascii=False)

    url = f"{MIMO_BASE}/open-apis/bot/chat?xiaomichatbot_ph={ph_enc}"
    proc = subprocess.Popen([
        "curl", "-s", "-N", url,
        "-H", f"cookie: {cookie_header}",
        "-H", "content-type: application/json",
        "-H", "accept: text/event-stream",
        "-d", chat_body,
    ], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

    parser = SSEParser()
    event = None

    if not quiet:
        print("🤖 ", end="", flush=True)

    try:
        while True:
            line = proc.stdout.readline()
            if not line:
                break
            line = line.rstrip()
            if line.startswith("event:"):
                event = line[6:].strip()
            elif line.startswith("data:"):
                data = line[5:].strip()
                text = parser.feed(event, data)
                if text and not quiet:
                    print(text, end="", flush=True)
                if event == "finish":
                    break
    except KeyboardInterrupt:
        proc.kill()
        if not quiet:
            print("\n\n⚠️ 已中断")
    finally:
        try:
            proc.stdout.close()
            proc.wait(timeout=5)
        except:
            proc.kill()

    if not quiet:
        print()

    if show_usage and parser.usage:
        pt = parser.usage.get("promptTokens", 0)
        ct = parser.usage.get("completionTokens", 0)
        tt = parser.usage.get("totalTokens", 0)
        print(f"📊 Tokens: {pt} prompt + {ct} completion = {tt} total")

    return {"reply": parser.full_reply, "usage": parser.usage, "conversation_id": conv_id}


# ──────────────── API 操作 ────────────────

def list_conversations(page=1, size=20):
    """列出所有会话"""
    code, data = _api("POST", "/chat/conversation/list",
                      {"pageInfo": {"pageNum": page, "pageSize": size}})
    if code == "HTTP_200" and isinstance(data, dict) and data.get("code") == 0:
        conversations = data["data"].get("dataList", [])
        total = data["data"].get("total", 0)
        print(f"📋 共 {total} 个会话:\n")
        for c in conversations:
            cid = c.get("conversationId", "?")
            title = c.get("title", "(无标题)")
            created = c.get("createTime", "?")
            updated = c.get("updateTime", "?")
            ctype = c.get("type", "?")
            print(f"  [{ctype}] {title}")
            print(f"    ID: {cid}")
            print(f"    创建: {created}  更新: {updated}")
            print()
        return conversations
    else:
        print(f"❌ {code}: {data}")
        return []


def get_history(conversation_id, page=1, size=50):
    """获取会话消息历史"""
    code, data = _api("POST", "/chat/dialog/list",
                      {"queryParam": {"conversationId": conversation_id},
                       "pageInfo": {"pageNum": page, "pageSize": size}})
    if code == "HTTP_200" and isinstance(data, dict) and data.get("code") == 0:
        dialogs = data.get("data", [])
        print(f"💬 会话 {conversation_id[:16]}... 共 {len(dialogs)} 轮:\n")
        for i, dialog in enumerate(dialogs):
            input_info = dialog.get("inputInfo", {})
            query = input_info.get("query", "")
            details = dialog.get("dialogLogDetailList", [])
            created = dialog.get("createTime", "?")

            print(f"  ─── 第 {i+1} 轮 ({created}) ───")
            print(f"  🧑 {query}")

            for detail in details:
                result = detail.get("result", "")
                # 清理 <think> 标签
                result = re.sub(r'<think>.*?</think>', '', result, flags=re.DOTALL)
                result = result.replace("\u0000", "").strip()
                if result:
                    print(f"  🤖 {result[:300]}")
            print()
        return dialogs
    else:
        print(f"❌ {code}: {data}")
        return []


def delete_conversation(conversation_ids):
    """删除会话（支持单个或列表）"""
    if isinstance(conversation_ids, str):
        conversation_ids = [conversation_ids]
    code, data = _api("POST", "/chat/conversation/delete", conversation_ids)
    if code == "HTTP_200" and isinstance(data, dict) and data.get("code") == 0:
        print(f"✅ 已删除 {len(conversation_ids)} 个会话")
        return True
    else:
        print(f"❌ {code}: {data}")
        return False


def gen_title(conversation_id, content):
    """生成会话标题"""
    code, data = _api("POST", "/chat/conversation/genTitle",
                      {"conversationId": conversation_id, "content": content})
    if code == "HTTP_200" and isinstance(data, dict) and data.get("code") == 0:
        title = data.get("data", "")
        print(f"📝 标题: {title}")
        return title
    else:
        print(f"❌ {code}: {data}")
        return None


def get_user_info():
    """获取当前用户信息"""
    code, data = _api("GET", "/user/mi/get")
    if code == "HTTP_200" and isinstance(data, dict) and data.get("code") == 0:
        info = data.get("data", {})
        print(f"👤 用户信息:")
        print(f"  用户ID: {info.get('userId', '?')}")
        print(f"  用户码: {info.get('userCode', '?')}")
        for k, v in info.items():
            if k not in ("userId", "userCode"):
                print(f"  {k}: {v}")
        return info
    else:
        print(f"❌ {code}: {data}")
        return None


def get_bot_config():
    """获取 Bot 配置（可用模型等）"""
    code, data = _api("GET", "/bot/config")
    if code == "HTTP_200" and isinstance(data, dict) and data.get("code") == 0:
        config = data.get("data", {})
        models = config.get("modelConfigList", [])
        print(f"⚙️ Bot 配置:")
        print(f"  可用模型 ({len(models)}):")
        for m in models:
            print(f"    - {m.get('name', '?')}")
            if m.get("description"):
                print(f"      {m['description'][:80]}")

        voice_config = config.get("voiceConfig", {})
        if voice_config:
            voices = voice_config.get("voice", [])
            print(f"  可用语音 ({len(voices)}):")
            for v in voices:
                print(f"    - {v.get('name', '?')} ({v.get('type', '?')})")

        return config
    else:
        print(f"❌ {code}: {data}")
        return None


def get_host_files(path=None):
    """列出 MiMo Claw 宿主文件"""
    params = {}
    if path:
        params["path"] = path
    code, data = _api("GET", "/host-files/list" + ("?path=" + quote(path) if path else ""))
    if code == "HTTP_200" and isinstance(data, dict) and data.get("code") == 0:
        info = data.get("data", {})
        cwd = info.get("path", "?")
        items = info.get("items", [])
        print(f"📁 宿主文件 (路径: {cwd}):")
        for item in items:
            is_dir = item.get("isDir", False)
            name = item.get("name", "?")
            size = item.get("size", "")
            icon = "📁" if is_dir else "📄"
            size_str = f" ({size} bytes)" if size else ""
            print(f"  {icon} {name}{size_str}")
        return items
    else:
        print(f"❌ {code}: {data}")
        return []


def get_ws_ticket():
    """获取 WebSocket 连接票据"""
    code, data = _api("GET", "/user/ws/ticket")
    if code == "HTTP_200" and isinstance(data, dict) and data.get("code") == 0:
        ticket = data.get("data", {}).get("ticket", "?")
        user_id = None
        # 尝试获取 userId
        code2, data2 = _api("GET", "/user/mi/get")
        if code2 == "HTTP_200" and isinstance(data2, dict) and data2.get("code") == 0:
            user_id = data2["data"].get("userId")
        print(f"🔑 WS 票据: {ticket}")
        if user_id:
            ws_url = f"wss://aistudio.xiaomimimo.com/ws/proxy?ticket={ticket}&userId={user_id}"
            print(f"🌐 WS URL: {ws_url}")
        return ticket
    else:
        print(f"❌ {code}: {data}")
        return None


def get_claw_status():
    """获取 MiMo Claw 状态"""
    code, data = _api("GET", "/user/mimo-claw/status")
    if code == "HTTP_200" and isinstance(data, dict) and data.get("code") == 0:
        info = data.get("data", {})
        status = info.get("status", "?")
        message = info.get("message", "")
        expire = info.get("expireTime", 0)
        if expire:
            expire_str = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(expire / 1000))
        else:
            expire_str = "?"
        print(f"🦀 MiMo Claw 状态:")
        print(f"  状态: {status}")
        print(f"  信息: {message}")
        print(f"  过期: {expire_str}")
        return info
    else:
        print(f"❌ {code}: {data}")
        return None


def share_conversation(conversation_id):
    """创建分享链接"""
    code, data = _api("POST", "/share/createShare",
                      {"conversationId": conversation_id})
    if code == "HTTP_200" and isinstance(data, dict) and data.get("code") == 0:
        share_url = data.get("data", {}).get("shareUrl", "?")
        print(f"🔗 分享链接: {share_url}")
        return share_url
    else:
        print(f"❌ {code}: {data}")
        return None


def upload_file_get_url(filename, file_type="image"):
    """获取文件上传 URL"""
    code, data = _api("POST", "/resource/genUploadInfo",
                      {"fileName": filename, "fileType": file_type})
    if code == "HTTP_200" and isinstance(data, dict) and data.get("code") == 0:
        info = data.get("data", {})
        print(f"📤 上传信息:")
        print(f"  Resource ID: {info.get('resourceId', '?')}")
        print(f"  Upload URL: {info.get('resourceUrl', '?')}")
        return info
    else:
        print(f"❌ {code}: {data}")
        return None


def generate_tts(text, msg_id=None):
    """生成 TTS 语音"""
    if not msg_id:
        msg_id = uuid.uuid4().hex
    code, data = _api("POST", "/tts/v2/generate",
                      {"text": text, "msgId": msg_id})
    if code == "HTTP_200" and isinstance(data, dict) and data.get("code") == 0:
        print(f"🔊 TTS 生成成功")
        return data.get("data")
    else:
        print(f"❌ {code}: {data}")
        return None


# ──────────────── CLI ────────────────

def main():
    import argparse
    p = argparse.ArgumentParser(
        description="MiMo Chat HTTP/SSE 客户端",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  %(prog)s "你好"                          # 单轮对话
  %(prog)s --session my "1+1=?"            # 多轮对话
  %(prog)s --thinking "解释量子计算"         # 深度思考
  %(prog)s --model mimo-v2-flash "hi"      # 指定模型
  %(prog)s --list-conversations            # 列出会话
  %(prog)s --history <convId>              # 查看历史
  %(prog)s --user-info                     # 用户信息
  %(prog)s --bot-config                    # 可用模型
  %(prog)s --delete <convId>               # 删除会话
  %(prog)s --host-files                    # 宿主文件
  %(prog)s --ws-ticket                     # WS 票据
  %(prog)s --claw-status                   # Claw 状态
""")
    p.add_argument("query", nargs="*", help="消息")
    p.add_argument("--model", default=DEFAULT_MODEL)
    p.add_argument("--thinking", action="store_true", help="深度思考")
    p.add_argument("--session", help="多轮会话名")
    p.add_argument("--quiet", "-q", action="store_true")
    p.add_argument("--usage", action="store_true")
    p.add_argument("--cookie-header", action="store_true")
    # API 操作
    p.add_argument("--list-conversations", action="store_true", help="列出所有会话")
    p.add_argument("--history", metavar="CONV_ID", help="查看会话消息历史")
    p.add_argument("--user-info", action="store_true", help="用户信息")
    p.add_argument("--bot-config", action="store_true", help="Bot 配置（可用模型等）")
    p.add_argument("--delete", metavar="CONV_ID", nargs="+", help="删除会话")
    p.add_argument("--gen-title", nargs=2, metavar=("CONV_ID", "CONTENT"), help="生成标题")
    p.add_argument("--host-files", action="store_true", help="宿主文件列表")
    p.add_argument("--ws-ticket", action="store_true", help="WebSocket 票据")
    p.add_argument("--claw-status", action="store_true", help="MiMo Claw 状态")
    p.add_argument("--share", metavar="CONV_ID", help="创建分享链接")
    p.add_argument("--upload", nargs=2, metavar=("FILENAME", "TYPE"), help="获取上传 URL")
    p.add_argument("--tts", metavar="TEXT", help="生成 TTS 语音")
    args = p.parse_args()

    if args.cookie_header:
        header, _ = get_cookie_parts()
        print(header)
        return

    if args.list_conversations:
        list_conversations()
        return

    if args.history:
        get_history(args.history)
        return

    if args.user_info:
        get_user_info()
        return

    if args.bot_config:
        get_bot_config()
        return

    if args.delete:
        delete_conversation(args.delete)
        return

    if args.gen_title:
        gen_title(args.gen_title[0], args.gen_title[1])
        return

    if args.host_files:
        get_host_files()
        return

    if args.ws_ticket:
        get_ws_ticket()
        return

    if args.claw_status:
        get_claw_status()
        return

    if args.share:
        share_conversation(args.share)
        return

    if args.upload:
        upload_file_get_url(args.upload[0], args.upload[1])
        return

    if args.tts:
        generate_tts(args.tts)
        return

    if not args.query:
        print("MiMo Chat (输入 quit 退出)")
        while True:
            try:
                q = input("\nYou: ").strip()
                if q.lower() in ("quit", "exit", "q"):
                    break
                if q:
                    chat(q, model=args.model, thinking=args.thinking,
                         session_name=args.session, show_usage=args.usage)
            except (KeyboardInterrupt, EOFError):
                break
        print("\nBye!")
        return

    query = " ".join(args.query)
    chat(query, model=args.model, thinking=args.thinking,
         session_name=args.session, quiet=args.quiet, show_usage=args.usage)


if __name__ == "__main__":
    main()
