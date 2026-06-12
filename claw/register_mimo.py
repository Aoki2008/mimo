#!/usr/bin/env python3
"""
小米账号邮箱注册脚本 (纯 HTTP，无浏览器)
=========================================
从 HAR 逆向的完整注册流程:
  1. genLoginUrl → 获取 callback + sign
  2. sendEmailRegTicket → 提交加密邮箱/密码 (触发验证码)
  3. captcha 流程 → 解决图形/滑块验证
  4. sms/quota → 发送邮箱验证码
  5. verifyEmailRegTicket → 提交验证码完成注册
  6. serviceLogin → 自动登录
  7. STS → 换取 MiMo session

用法:
  python3 register_mimo.py                    # 交互式注册
  python3 register_mimo.py --email X --password Y  # 命令行注册

环境变量:
  MIMO_EMAIL     - 邮箱 (可选，交互输入)
  MIMO_PASSWORD  - 密码 (可选，交互输入)

依赖: pip install curl_cffi pycryptodome
"""

import base64
import hashlib
import json
import os
import sys
import time
import uuid
from pathlib import Path
from urllib.parse import parse_qs, quote, urlencode, urlparse

# curl_cffi: 伪造真实浏览器 TLS 指纹，绕过小米风控
from curl_cffi.requests import Session as CurlSession

# ── 路径 ──────────────────────────────────────────────
ACCOUNTS_DIR = Path(__file__).resolve().parent / "accounts"

# ── 常量 ──────────────────────────────────────────────
MIMO_BASE = "https://aistudio.xiaomimimo.com"
SID = "xiaomichatbot"

# 全球版注册 (HK region)
GLOBAL_ACCOUNT = "https://global.account.xiaomi.com"
GLOBAL_REGISTER_URL = f"{GLOBAL_ACCOUNT}/fe/service/register"
GLOBAL_SEND_EMAIL = f"{GLOBAL_ACCOUNT}/pass/sendEmailRegTicket"
GLOBAL_VERIFY_EMAIL = f"{GLOBAL_ACCOUNT}/pass/verifyEmailRegTicket"
GLOBAL_SMS_QUOTA = f"{GLOBAL_ACCOUNT}/pass/sms/quota"
GLOBAL_SERVICE_LOGIN = f"{GLOBAL_ACCOUNT}/pass/serviceLogin"

# 验证码
CAPTCHA_CONFIG = "https://verify.sec.xiaomi.com/captcha/v2/config"
CAPTCHA_DATA = "https://verify.sec.xiaomi.com/captcha/v2/data"
CAPTCHA_VERIFY = "https://verify.sec.xiaomi.com/captcha/v2/recaptcha/verify"
CAPTCHA_SITE_KEY = "8027422fb0eb42fbac1b521ec4a7961f"

# RSA 公钥 (生产环境)
RSA_PUBLIC_KEY = (
    "-----BEGIN PUBLIC KEY-----\n"
    "MIGfMA0GCSqGSIb3DQEBAQUAA4GNADCBiQKBgQCYEVrK/4Mahiv0pUJgTybx4J9P\n"
    "5dUT/Y0PuwMbk+gMU+jrZnBiXGv6/hCH1avIhoBcE535F8nJQQN3UavZdFkYidso\n"
    "XuEnat3+eVTp3FslyhRwIBDF09v4vDhRtxFOT+R7uH7h/mzmyA2/+lfIMWGIrffX\n"
    "prYizbV76+YQKhoqFQIDAQAB\n"
    "-----END PUBLIC KEY-----"
)

# AES-CBC IV (浏览器硬编码)
AES_IV = b"0102030405060708"

ESSENTIAL_COOKIES = {
    "serviceToken", "userId", "cUserId", "xiaomichatbot_ph", "xiaomichatbot_slh",
}


# ── 响应解析 ──────────────────────────────────────────

def _parse(resp) -> dict:
    """解析小米 API 响应 (去掉 &&&START&&& 前缀)"""
    text = resp.text
    if text.startswith("&&&START&&&"):
        text = text[len("&&&START&&&"):]
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return {"raw": resp.text[:500], "status": resp.status_code}


# ── 加密 ──────────────────────────────────────────────

def _random_aes_key(length: int = 16) -> str:
    """生成 16 字符随机 AES key (与浏览器 encryptAes 一致)"""
    chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789!@#$%^&*"
    return "".join(chars[os.urandom(1)[0] % len(chars)] for _ in range(length))


def _rsa_encrypt(plaintext: str, public_key_pem: str) -> bytes:
    """RSA 加密 (PKCS1_v1.5)"""
    from Crypto.PublicKey import RSA
    from Crypto.Cipher import PKCS1_v1_5
    key = RSA.import_key(public_key_pem)
    cipher = PKCS1_v1_5.new(key)
    return cipher.encrypt(plaintext.encode("utf-8"))


def _aes_cbc_encrypt(plaintext: str, key_str: str, iv_bytes: bytes) -> bytes:
    """AES-128-CBC 加密 (PKCS7 padding)"""
    from Crypto.Cipher import AES
    key_bytes = key_str.encode("utf-8")
    data = plaintext.encode("utf-8")
    # PKCS7 padding
    pad_len = 16 - (len(data) % 16)
    data += bytes([pad_len] * pad_len)
    cipher = AES.new(key_bytes, AES.MODE_CBC, iv_bytes)
    return cipher.encrypt(data)


def encrypt_params(params: dict) -> dict:
    """小米 encryptAes 加密: AES-CBC 加密参数值, RSA 加密 AES key

    返回:
      {
        "eui": "<RSA(AES_KEY)>.<base64(param_names)>",
        "encryptedParams": {key: base64(AES(value)), ...}
      }
    """
    aes_key = _random_aes_key(16)

    # RSA 加密 AES key
    rsa_encrypted = _rsa_encrypt(aes_key, RSA_PUBLIC_KEY)
    rsa_b64 = base64.b64encode(rsa_encrypted).decode()

    # 参数名列表 (base64)
    param_names_b64 = base64.b64encode(",".join(params.keys()).encode()).decode()

    # AES-CBC 加密每个参数值
    encrypted_params = {}
    for key, value in params.items():
        encrypted = _aes_cbc_encrypt(str(value), aes_key, AES_IV)
        encrypted_params[key] = base64.b64encode(encrypted).decode()

    eui = f"{rsa_b64}.{param_names_b64}"
    return {"eui": eui, "encryptedParams": encrypted_params}


# ── Cookie 管理 ───────────────────────────────────────

def _mimo_cookie(name: str, value: str, expires: int = -1) -> dict:
    return {
        "name": name, "value": value, "domain": ".xiaomimimo.com", "path": "/",
        "expires": expires, "httpOnly": name in ("serviceToken", "cUserId"),
        "secure": False, "sameSite": "Lax",
    }


def save_account(email: str, cookies: list, user_info: dict = None):
    """保存账号到 accounts/<email>.json"""
    ACCOUNTS_DIR.mkdir(parents=True, exist_ok=True)
    payload = {
        "name": email,
        "user_id": (user_info or {}).get("userId", ""),
        "user_info": user_info or {},
        "cookies": cookies,
        "exported_at": int(time.time()),
        "source": "register_mimo.py",
    }
    path = ACCOUNTS_DIR / f"{email}.json"
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)
    return path


def get_cookie_header(cookies: list) -> str:
    parts = [f"{c['name']}={c['value']}" for c in cookies if "xiaomimimo" in c.get("domain", "")]
    return "; ".join(parts)


def fetch_user_info(cookies: list) -> dict:
    header = get_cookie_header(cookies)
    if not header:
        return {}
    try:
        s = CurlSession(impersonate="chrome120")
        r = s.get(
            f"{MIMO_BASE}/open-apis/user/mi/get",
            headers={"Cookie": header, "Content-Type": "application/json"},
            timeout=15,
        )
        j = r.json()
        if j.get("code") == 0 and isinstance(j.get("data"), dict):
            return j["data"]
    except Exception:
        pass
    return {}


# ── 注册流程 ──────────────────────────────────────────

def step1_gen_login_url(session: CurlSession) -> tuple[str, str]:
    """Step 1: 获取 callback URL 和 sign

    Returns: (callback_url, sign)
    """
    print("[reg] Step 1: 获取 callback URL...")
    resp = session.get(f"{MIMO_BASE}/open-apis/v1/genLoginUrl", allow_redirects=False, timeout=10)
    location = resp.headers.get("Location", "")
    if not location:
        raise Exception("genLoginUrl 未返回 redirect")

    parsed = urlparse(location)
    params = parse_qs(parsed.query)
    callback = params.get("callback", [None])[0]
    if not callback:
        raise Exception(f"redirect URL 中无 callback: {location[:200]}")

    # 从 callback URL 中提取 sign
    cb_parsed = urlparse(callback)
    cb_params = parse_qs(cb_parsed.query)
    sign = cb_params.get("sign", [""])[0]

    print(f"[reg] callback: {callback[:80]}...")
    return callback, sign


def step2_get_sign(session: CurlSession, callback: str) -> dict:
    """Step 2: 访问 serviceLogin 获取 _sign 和登录参数"""
    print("[reg] Step 2: 获取 _sign...")
    resp = session.get(
        f"{GLOBAL_ACCOUNT}/pass/serviceLogin",
        params={"callback": callback, "sid": SID, "_group": "DEFAULT"},
        allow_redirects=False,
        timeout=15,
    )

    location = resp.headers.get("Location", "")
    if not location:
        # 尝试带 _json=true
        resp = session.get(
            f"{GLOBAL_ACCOUNT}/pass/serviceLogin",
            params={"sid": SID, "callback": callback, "_json": "true"},
            allow_redirects=True,
            timeout=15,
        )
        text = resp.text.replace("&&&START&&&", "")
        try:
            data = json.loads(text)
            return {"_sign": data.get("_sign", ""), "callback": callback}
        except json.JSONDecodeError:
            raise Exception(f"获取 _sign 失败: {resp.text[:200]}")

    params = parse_qs(urlparse(location).query, keep_blank_values=True)
    values = {k: v[0] for k, v in params.items() if v}
    values.setdefault("callback", callback)
    print(f"[reg] _sign: {values.get('_sign', 'N/A')[:30]}...")
    return values


def step3_send_email_reg(
    session: CurlSession,
    email: str,
    password: str,
    sign: str,
    callback: str,
    captcha_code: str = "",
    region: str = "HK",
) -> dict:
    """Step 3: 提交注册 (加密邮箱/密码)

    sendEmailRegTicket 会:
    - 首次调用: 返回验证码要求 (captcha)
    - 带验证码调用: 触发发送邮箱验证码
    """
    print(f"[reg] Step 3: 提交注册 (region={region})...")

    # 加密参数
    enc = encrypt_params({
        "email": email,
        "password": password,
    })

    # 构造 body
    body = {
        "email": enc["encryptedParams"]["email"],
        "password": enc["encryptedParams"]["password"],
        "region": region,
        "sid": SID,
        "icode": captcha_code,
    }

    # 构造 referer (注册页面 URL)
    qs = quote(f"?callback={quote(callback, safe='')}&sid={SID}", safe="")
    service_param = json.dumps({"checkSafePhone": False, "checkSafeAddress": False, "lsrp_score": 0.0}, separators=(",", ":"))
    register_page_params = {
        "_group": "DEFAULT",
        "_sign": sign,
        "serviceParam": service_param,
        "showActiveX": "false",
        "theme": "",
        "needTheme": "false",
        "bizDeviceType": "",
        "_locale": "zh_CN",
        "source": "",
        "region": region,
        "sid": SID,
        "qs": qs,
        "callback": callback,
    }
    referer = f"{GLOBAL_ACCOUNT}/fe/service/register/email?{urlencode(register_page_params)}"

    headers = {
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "Origin": GLOBAL_ACCOUNT,
        "Referer": referer,
        "eui": enc["eui"],
        "x-requested-with": "XMLHttpRequest",
        "Accept": "application/json, text/plain, */*",
    }

    resp = session.post(GLOBAL_SEND_EMAIL, data=body, headers=headers, timeout=15)

    data = _parse(resp)
    print(f"[reg] sendEmailRegTicket: status={resp.status_code}, code={data.get('code', 'N/A')}")
    return data


def step4_handle_captcha(session, captcha_api_key: str = None) -> str:
    """Step 4: 获取并解决验证码

    有 API key → 2Captcha 自动识别
    无 API key → 保存图片手动输入

    Returns: 验证码文字
    """
    print("[reg] Step 4: 获取验证码...")
    ts = int(time.time() * 1000)

    # 建立 captcha session
    session.get(
        CAPTCHA_CONFIG,
        params={"type": "1", "locale": "zh_CN", "callback": f"miVerify_{ts}"},
        timeout=10,
    )

    # 获取验证码图片
    resp = session.get(f"{GLOBAL_ACCOUNT}/pass/getCode?icodeType=register", timeout=10)
    img_bytes = resp.content
    img_b64 = base64.b64encode(img_bytes).decode()

    if captcha_api_key:
        # ── 2Captcha 自动识别 ──
        print("[reg]   → 发送到 2Captcha...")
        try:
            # 上传图片
            upload_resp = session.post(
                "https://2captcha.com/in.php",
                data={"key": captcha_api_key, "method": "base64", "body": img_b64},
                timeout=30,
            )
            if "OK|" not in upload_resp.text:
                print(f"[reg]   ❌ 上传失败: {upload_resp.text}")
                return _manual_captcha(img_bytes)

            captcha_id = upload_resp.text.split("|")[1]
            print(f"[reg]   ⏳ 等待识别 (id={captcha_id})...")

            # 轮询结果 (最多等 30 秒)
            for i in range(15):
                time.sleep(2)
                result_resp = session.get(
                    "https://2captcha.com/res.php",
                    params={"key": captcha_api_key, "action": "get", "id": captcha_id},
                    timeout=10,
                )
                if "OK|" in result_resp.text:
                    code = result_resp.text.split("|")[1]
                    print(f"[reg]   ✅ 识别结果: {code}")
                    return code
                elif result_resp.text == "CAPCHA_NOT_READY":
                    continue
                else:
                    print(f"[reg]   ❌ 识别失败: {result_resp.text}")
                    return _manual_captcha(img_bytes)

            print("[reg]   ❌ 识别超时")
            return _manual_captcha(img_bytes)

        except Exception as ex:
            print(f"[reg]   ❌ 2Captcha 异常: {ex}")
            return _manual_captcha(img_bytes)
    else:
        return _manual_captcha(img_bytes)


def _manual_captcha(img_bytes: bytes) -> str:
    """手动输入验证码"""
    captcha_path = Path(__file__).resolve().parent / "captcha_now.png"
    with open(captcha_path, "wb") as f:
        f.write(img_bytes)
    print(f"[reg]   验证码已保存: {captcha_path}")
    code = input("[reg]   请输入验证码: ").strip()
    return code


def step5_check_sms_quota(session, email: str, region: str = "HK") -> dict:
    """Step 5: 检查邮箱验证码配额 (防限流)

    注意: 验证码邮件由 sendEmailRegTicket 自动触发发送,
    此步仅检查配额，不是发送验证码。
    放在 verifyEmailRegTicket 之前调用更稳妥。
    """
    print("[reg] Step 5: 检查邮箱验证码配额...")
    body = {
        "address": email,
        "templateId": "CI93714_EM_153",
    }
    resp = session.post(
        GLOBAL_SMS_QUOTA,
        data=body,
        headers={
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
            "Origin": GLOBAL_ACCOUNT,
            "x-requested-with": "XMLHttpRequest",
        },
        timeout=15,
    )
    data = _parse(resp)
    print(f"[reg] sms/quota: status={resp.status_code}")
    return data


def step6_verify_email(
    session: CurlSession,
    email: str,
    password: str,
    code: str,
    sign: str,
    callback: str,
    region: str = "HK",
) -> dict:
    """Step 6: 提交邮箱验证码完成注册"""
    print(f"[reg] Step 6: 提交验证码 {code}...")

    enc = encrypt_params({
        "email": email,
        "password": password,
    })

    qs = quote(f"?callback={quote(callback, safe='')}&sid={SID}", safe="")
    service_param = json.dumps({"checkSafePhone": False, "checkSafeAddress": False, "lsrp_score": 0.0}, separators=(",", ":"))

    body = {
        "ticket": code,
        "region": region,
        "email": enc["encryptedParams"]["email"],
        "env": "web",
        "qs": qs,
        "isAcceptLicense": "true",
        "sid": SID,
        "password": enc["encryptedParams"]["password"],
        "policyName": "globalmiaccount",
        "callback": callback,
    }

    register_page_params = {
        "_group": "DEFAULT",
        "_sign": sign,
        "serviceParam": service_param,
        "showActiveX": "false",
        "theme": "",
        "needTheme": "false",
        "bizDeviceType": "",
        "_locale": "zh_CN",
        "source": "",
        "region": region,
        "sid": SID,
        "qs": qs,
        "callback": callback,
    }
    referer = f"{GLOBAL_ACCOUNT}/fe/service/register/email/verify?{urlencode(register_page_params)}"

    headers = {
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "Origin": GLOBAL_ACCOUNT,
        "Referer": referer,
        "eui": enc["eui"],
        "x-requested-with": "XMLHttpRequest",
    }

    resp = session.post(GLOBAL_VERIFY_EMAIL, data=body, headers=headers, timeout=15)

    data = _parse(resp)
    print(f"[reg] verifyEmailRegTicket: status={resp.status_code}, code={data.get('code', 'N/A')}")
    return data


def step7_auto_login(session: CurlSession, callback: str) -> str | None:
    """Step 7: 注册完成后自动登录 (serviceLogin → STS)"""
    print("[reg] Step 7: 自动登录...")
    resp = session.get(
        GLOBAL_SERVICE_LOGIN,
        params={"callback": callback, "sid": SID},
        allow_redirects=True,
        timeout=15,
    )

    # 检查是否拿到 serviceToken
    for c in session.cookies:
        if c.name == "serviceToken" and "xiaomimimo" in (c.domain or ""):
            print("[reg] 已获取 serviceToken")
            return resp.url

    # serviceToken 可能在 redirect 链中
    if resp.history:
        for h in resp.history:
            loc = h.headers.get("Location", "")
            if "sts" in loc or "aistudio" in loc:
                return loc

    return resp.url


def step8_exchange_sts(session: CurlSession, sts_url: str) -> list:
    """Step 8: 访问 STS 换取 MiMo session cookies"""
    print("[reg] Step 8: 换取 MiMo session...")
    resp = session.get(sts_url, allow_redirects=True, timeout=15)

    # 收集 .xiaomimimo.com cookies
    cookies = []
    seen = set()
    for c in session.cookies:
        if c.name in ESSENTIAL_COOKIES and "xiaomimimo" in (c.domain or ""):
            cookies.append(_mimo_cookie(c.name, c.value, c.expires))
            seen.add(c.name)
    for c in session.cookies:
        if c.name in ("serviceToken", "userId") and c.name not in seen:
            cookies.append(_mimo_cookie(c.name, c.value, c.expires))
            seen.add(c.name)

    # 获取 xiaomichatbot_ph
    if "xiaomichatbot_ph" not in seen:
        try:
            r = session.get(f"{MIMO_BASE}/open-apis/user/mi/get", timeout=10)
            for c in session.cookies:
                if c.name == "xiaomichatbot_ph":
                    val = c.value.strip('"')
                    cookies.append(_mimo_cookie("xiaomichatbot_ph", val))
                    break
        except Exception:
            pass

    return cookies


# ── 主流程 ────────────────────────────────────────────

def register(email: str, password: str, region: str = "HK", captcha_api_key: str = None) -> dict:
    """完整注册流程

    Args:
        email: 注册邮箱
        password: 注册密码
        region: 区域 (HK/CN/SG/...)
        captcha_api_key: 2Captcha API Key (可选，不传则手动输入验证码)

    Returns:
      {"status": "ok", "cookies": [...], "user_info": {...}}
      {"status": "error", "error": "..."}
    """
    # curl_cffi: 伪造 Chrome 120 的 TLS 指纹 (JA3, Akamai fingerprint)
    session = CurlSession(impersonate="chrome120")
    session.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    })

    try:
        # Step 1: 获取 callback
        callback, sign = step1_gen_login_url(session)

        # Step 2: 获取 _sign
        login_params = step2_get_sign(session, callback)
        sign = login_params.get("_sign", sign)

        # Step 3: 提交注册 (首次，可能触发验证码)
        result = step3_send_email_reg(session, email, password, sign, callback, region=region)

        # 检查是否需要验证码
        code = result.get("code", -1)
        if code == 87006 or code == 87001:
            # 需要验证码
            captcha_code = step4_handle_captcha(session, captcha_api_key)
            if not captcha_code:
                return {"status": "error", "error": "未输入验证码"}

            # 带验证码重新提交
            result = step3_send_email_reg(session, email, password, sign, callback, captcha_code=captcha_code, region=region)
            code = result.get("code", -1)

        if code != 0 and code != 70016:
            desc = result.get("desc", result.get("raw", str(result)))
            return {"status": "error", "error": f"注册提交失败: {desc}"}

        # Step 5: 检查邮箱验证码配额 (在 verifyEmailRegTicket 之前)
        step5_check_sms_quota(session, email, region)

        # Step 6: 等待并提交验证码
        for attempt in range(3):
            code_input = input(f"[reg] 请输入邮箱验证码 (第{attempt+1}次): ").strip()
            if not code_input:
                print("[reg] 验证码不能为空")
                continue

            verify_result = step6_verify_email(
                session, email, password, code_input, sign, callback, region
            )

            if verify_result.get("code") == 0:
                print("[reg] 注册成功！")
                break
            elif verify_result.get("code") == 87001:
                print("[reg] 验证码错误，请重试")
                continue
            else:
                desc = verify_result.get("desc", str(verify_result))
                return {"status": "error", "error": f"验证码验证失败: {desc}"}
        else:
            return {"status": "error", "error": "验证码错误次数过多"}

        # Step 7: 自动登录
        sts_url = step7_auto_login(session, callback)

        # Step 8: 换取 MiMo session
        cookies = step8_exchange_sts(session, sts_url)

        if not cookies:
            return {"status": "error", "error": "未获取到 MiMo cookies"}

        # 验证并保存
        user_info = fetch_user_info(cookies)
        if user_info:
            # Step 9: 同意用户协议和免责声明
            cookie_header = get_cookie_header(cookies)
            if cookie_header:
                print("[reg] Step 9: 同意协议...")
                try:
                    s = CurlSession(impersonate="chrome120")
                    h = {"Cookie": cookie_header, "Content-Type": "application/json"}
                    r1 = s.post(f"{MIMO_BASE}/open-apis/agreement", headers=h, timeout=10)
                    print(f"[reg]   用户协议: {r1.json().get('msg', r1.status_code)}")
                    r2 = s.post(f"{MIMO_BASE}/open-apis/agreement/user/mimo-claw", headers=h, timeout=10)
                    print(f"[reg]   免责声明: {r2.json().get('msg', r2.status_code)}")
                    # 刷新 user_info
                    user_info = fetch_user_info(cookies)
                except Exception as ex:
                    print(f"[reg]   协议签署异常: {ex}")

            path = save_account(email, cookies, user_info)
            uid = user_info.get("userId", "")
            print(f"[reg] 保存成功: {path} (userId={uid})")
        else:
            print("[reg] 警告: 无法获取用户信息，cookies 可能无效")

        return {"status": "ok", "cookies": cookies, "user_info": user_info}

    except Exception as e:
        return {"status": "error", "error": f"{type(e).__name__}: {e}"}


def main():
    import argparse
    parser = argparse.ArgumentParser(description="小米账号邮箱注册")
    parser.add_argument("--email", help="注册邮箱")
    parser.add_argument("--password", help="注册密码")
    parser.add_argument("--region", default="HK", help="注册地区 (HK/CN/SG/...)")
    parser.add_argument("--captcha-key", help="2Captcha API Key (自动识别验证码)")
    args = parser.parse_args()

    email = args.email or os.environ.get("MIMO_EMAIL") or input("[reg] 邮箱: ").strip()
    password = args.password or os.environ.get("MIMO_PASSWORD")
    captcha_key = args.captcha_key or os.environ.get("CAPTCHA_API_KEY")

    if not password:
        import getpass
        password = getpass.getpass("[reg] 密码: ")

    if not email or not password:
        print("邮箱和密码不能为空")
        sys.exit(1)

    result = register(email, password, args.region, captcha_key)

    if result["status"] == "ok":
        uid = result.get("user_info", {}).get("userId", "?")
        print(f"\n注册成功! userId={uid}")
    else:
        print(f"\n注册失败: {result['error']}")
        sys.exit(1)


if __name__ == "__main__":
    main()
