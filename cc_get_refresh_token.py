#!/usr/bin/env python3
"""
One-time setup: get a CC refresh token via OAuth authorization code + PKCE.

Run locally (Chrome must be open with --remote-debugging-port=9222):
    python3 ~/vip-report-app/cc_get_refresh_token.py

What it does:
1. Generates a PKCE code_verifier + code_challenge
2. Navigates Chrome to the CC OAuth page
3. You log in (Google SSO is tried automatically)
4. Captures the authorization code from the redirect
5. Exchanges the code for access_token + refresh_token
6. Saves CC_REFRESH_TOKEN to ~/.streamlit/secrets.toml
   AND prints it to paste into Streamlit Cloud secrets

The refresh token lasts 60 days of non-use. With weekly report runs it
effectively never expires. If it does, just run this script again.
"""
import asyncio
import base64
import hashlib
import json
import os
import re
import time
import urllib.parse
import urllib.request

import requests
import websockets

CC_CLIENT_ID  = "ae507531-707f-4bd1-9eb0-ae6685b01e6a"
CC_TOKEN_URL  = "https://authz.constantcontact.com/oauth2/default/v1/token"
REDIRECT_URI  = "https://localhost"
SECRETS_PATH  = os.path.expanduser("~/.streamlit/secrets.toml")


# ── PKCE helpers ─────────────────────────────────────────────────────────────
def _pkce():
    verifier  = base64.urlsafe_b64encode(os.urandom(32)).rstrip(b"=").decode()
    challenge = base64.urlsafe_b64encode(
        hashlib.sha256(verifier.encode()).digest()
    ).rstrip(b"=").decode()
    return verifier, challenge


# ── Chrome CDP helpers ────────────────────────────────────────────────────────
_cmd_id = 0

async def _send(ws, method, params=None):
    global _cmd_id
    _cmd_id += 1
    cid = _cmd_id
    await ws.send(json.dumps({"id": cid, "method": method, "params": params or {}}))
    return cid


async def _wait_for(ws, target_id, timeout=10):
    deadline = asyncio.get_event_loop().time() + timeout
    while asyncio.get_event_loop().time() < deadline:
        try:
            raw  = await asyncio.wait_for(ws.recv(), timeout=1)
            data = json.loads(raw)
            if data.get("id") == target_id:
                return data.get("result", {})
        except asyncio.TimeoutError:
            continue
    return {}


async def _js(ws, expr):
    cid    = await _send(ws, "Runtime.evaluate", {"expression": expr, "returnByValue": True, "awaitPromise": True})
    result = await _wait_for(ws, cid, timeout=8)
    return result.get("result", {}).get("value")


async def _click(ws, selector):
    rect = await _js(ws, f"""
        (() => {{
            const el = document.querySelector({json.dumps(selector)});
            if (!el) return null;
            el.scrollIntoView({{block: 'center'}});
            const r = el.getBoundingClientRect();
            return {{x: r.left + r.width/2, y: r.top + r.height/2, found: true}};
        }})()
    """)
    if not rect or not rect.get("found"):
        return False
    x, y = int(rect["x"]), int(rect["y"])
    for etype in ["mousePressed", "mouseReleased"]:
        await _send(ws, "Input.dispatchMouseEvent",
                    {"type": etype, "x": x, "y": y, "button": "left", "clickCount": 1})
        await asyncio.sleep(0.05)
    return True


async def _find_google_btn(ws):
    selectors = [
        "[data-se='social-auth-google-button']",
        ".social-auth-button.link-button--social",
        "a[href*='google']",
        "button[class*='google']",
        "[class*='google-btn']",
    ]
    for sel in selectors:
        found = await _js(ws, f"document.querySelector({json.dumps(sel)})?.outerHTML?.slice(0,80)")
        if found:
            print(f"  Found Google btn via: {sel}")
            if await _click(ws, sel):
                return True

    result = await _js(ws, """
        (() => {
            const all = Array.from(document.querySelectorAll('a, button, [role=button]'));
            const g = all.find(el => /google/i.test(el.textContent + el.getAttribute('data-label') + el.className));
            if (!g) return null;
            g.scrollIntoView({block:'center'});
            const r = g.getBoundingClientRect();
            return {x: r.left + r.width/2, y: r.top + r.height/2, text: g.textContent.trim().slice(0,50)};
        })()
    """)
    if result:
        print(f"  Found Google btn by text: '{result.get('text')}'")
        x, y = int(result["x"]), int(result["y"])
        for etype in ["mousePressed", "mouseReleased"]:
            await _send(ws, "Input.dispatchMouseEvent",
                        {"type": etype, "x": x, "y": y, "button": "left", "clickCount": 1})
            await asyncio.sleep(0.05)
        return True
    return False


# ── Token exchange ────────────────────────────────────────────────────────────
def _exchange_code(code, verifier):
    resp = requests.post(
        CC_TOKEN_URL,
        data={
            "grant_type":    "authorization_code",
            "client_id":     CC_CLIENT_ID,
            "code":          code,
            "redirect_uri":  REDIRECT_URI,
            "code_verifier": verifier,
        },
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        timeout=15,
    )
    resp.raise_for_status()
    return resp.json()


# ── Save to secrets.toml ──────────────────────────────────────────────────────
def _save_refresh_token(refresh_token):
    os.makedirs(os.path.dirname(SECRETS_PATH), exist_ok=True)
    if os.path.exists(SECRETS_PATH):
        with open(SECRETS_PATH) as f:
            content = f.read()
        # Update or add CC_REFRESH_TOKEN
        if "CC_REFRESH_TOKEN" in content:
            content = re.sub(
                r'CC_REFRESH_TOKEN\s*=\s*"[^"]*"',
                f'CC_REFRESH_TOKEN = "{refresh_token}"',
                content
            )
        else:
            content += f'\nCC_REFRESH_TOKEN = "{refresh_token}"\n'
        # Remove old short-lived access token line
        content = re.sub(r'\nCC_ACCESS_TOKEN\s*=\s*"[^"]*"', '', content)
    else:
        content = f'CC_REFRESH_TOKEN = "{refresh_token}"\n'

    with open(SECRETS_PATH, "w") as f:
        f.write(content)


# ── Main ──────────────────────────────────────────────────────────────────────
async def main():
    verifier, challenge = _pkce()

    auth_url = (
        "https://authz.constantcontact.com/oauth2/default/v1/authorize"
        "?response_type=code"
        f"&client_id={CC_CLIENT_ID}"
        f"&redirect_uri={urllib.parse.quote(REDIRECT_URI, safe='')}"
        "&scope=account_read+contact_data+campaign_data+offline_access"
        f"&code_challenge={challenge}"
        "&code_challenge_method=S256"
        "&state=vip_setup"
    )

    # Connect to Chrome
    try:
        with urllib.request.urlopen("http://localhost:9222/json") as r:
            tabs = json.loads(r.read())
    except Exception:
        print("ERROR: Chrome not running with --remote-debugging-port=9222")
        print("Start Chrome with: open -a 'Google Chrome' --args --remote-debugging-port=9222")
        return

    ws_url = tabs[0]["webSocketDebuggerUrl"]
    print(f"Connected to Chrome tab: {tabs[0].get('title', '?')[:60]}")

    async with websockets.connect(ws_url, max_size=10_000_000) as ws:
        await _send(ws, "Page.enable")
        await _send(ws, "Network.enable")

        # Intercept the localhost redirect to capture the code
        await _send(ws, "Fetch.enable", {
            "patterns": [
                {"urlPattern": "https://localhost*", "requestStage": "Request"},
                {"urlPattern": "http://localhost*",  "requestStage": "Request"},
            ]
        })
        await asyncio.sleep(0.3)

        print(f"Navigating to CC OAuth page...")
        await _send(ws, "Page.navigate", {"url": auth_url})

        auth_code    = None
        google_done  = False
        account_done = False
        deadline     = asyncio.get_event_loop().time() + 120

        while asyncio.get_event_loop().time() < deadline and not auth_code:
            try:
                raw  = await asyncio.wait_for(ws.recv(), timeout=1)
            except asyncio.TimeoutError:
                continue

            data   = json.loads(raw)
            method = data.get("method", "")
            params = data.get("params", {})

            # ── Intercept localhost redirect with code ──────────────────────
            if method == "Fetch.requestPaused":
                req_url = params.get("request", {}).get("url", "")
                req_id  = params.get("requestId")
                if req_url.startswith("https://localhost") or req_url.startswith("http://localhost"):
                    print(f"  Intercepted: {req_url[:120]}")
                    await _send(ws, "Fetch.continueRequest", {"requestId": req_id})
                    parsed = urllib.parse.urlparse(req_url)
                    qs     = urllib.parse.parse_qs(parsed.query)
                    if "code" in qs:
                        auth_code = qs["code"][0]
                        print(f"  Got authorization code!")
                        break
                    if "error" in qs:
                        print(f"  OAuth error: {qs.get('error_description', qs.get('error'))}")
                        return

            # ── Network redirect header fallback ───────────────────────────
            elif method == "Network.responseReceivedExtraInfo":
                location = (params.get("headers", {}).get("location") or
                            params.get("headers", {}).get("Location", ""))
                if location and "localhost" in location:
                    print(f"  Redirect: {location[:120]}")
                    parsed = urllib.parse.urlparse(location)
                    qs     = urllib.parse.parse_qs(parsed.query)
                    if "code" in qs:
                        auth_code = qs["code"][0]
                        print(f"  Got authorization code!")
                        break

            # ── Page loaded — drive the login UI ───────────────────────────
            elif method == "Page.frameStoppedLoading":
                await asyncio.sleep(0.5)
                url   = await _js(ws, "window.location.href") or ""
                title = await _js(ws, "document.title") or ""
                print(f"  Page: [{title[:40]}] {url[:80]}")

                # Okta login page — click Google
                if "identity.constantcontact.com" in url and not google_done:
                    print("  On Okta page. Looking for Google button...")
                    await asyncio.sleep(1)
                    if await _find_google_btn(ws):
                        google_done = True
                        print("  Google button clicked.")
                    else:
                        body = await _js(ws, "document.body.innerText.slice(0, 300)")
                        print(f"  Could not find Google btn. Page:\n{body}")

                # Google account chooser
                elif "accounts.google.com" in url and not account_done:
                    print("  On Google account chooser...")
                    await asyncio.sleep(1.5)
                    result = await _js(ws, """
                        (() => {
                            const items = Array.from(document.querySelectorAll('[data-email], [data-identifier], .jLZDPb, li[class]'));
                            const cc = items.find(el =>
                                (el.textContent||'').includes('nickfisher518') ||
                                (el.getAttribute('data-email')||'').includes('nickfisher518')
                            ) || items[0];
                            if (!cc) return null;
                            const r = cc.getBoundingClientRect();
                            return {x: r.left+r.width/2, y: r.top+r.height/2, text: cc.textContent.slice(0,40)};
                        })()
                    """)
                    if result:
                        print(f"  Clicking account: {result.get('text','')[:40]}")
                        x, y = int(result["x"]), int(result["y"])
                        for etype in ["mousePressed", "mouseReleased"]:
                            await _send(ws, "Input.dispatchMouseEvent",
                                        {"type": etype, "x": x, "y": y,
                                         "button": "left", "clickCount": 1})
                            await asyncio.sleep(0.1)
                        account_done = True

                # Google consent/continue screen
                elif "accounts.google.com" in url and account_done:
                    result = await _js(ws, """
                        (() => {
                            const btn = Array.from(document.querySelectorAll('button'))
                                .find(b => /continue|next|allow|yes/i.test(b.textContent));
                            if (!btn) return null;
                            const r = btn.getBoundingClientRect();
                            return {x: r.left+r.width/2, y: r.top+r.height/2, text: btn.textContent.trim()};
                        })()
                    """)
                    if result:
                        print(f"  Clicking: {result.get('text')}")
                        x, y = int(result["x"]), int(result["y"])
                        for etype in ["mousePressed", "mouseReleased"]:
                            await _send(ws, "Input.dispatchMouseEvent",
                                        {"type": etype, "x": x, "y": y,
                                         "button": "left", "clickCount": 1})

                # CC consent / Allow Access screen
                elif "authz.constantcontact.com" in url or "identity.constantcontact.com" in url:
                    body = await _js(ws, "document.body.innerText.slice(0, 200)")
                    if "allow" in (body or "").lower() or "access" in (body or "").lower():
                        result = await _js(ws, """
                            (() => {
                                const btns = Array.from(document.querySelectorAll('button,input[type=submit],a[class*=btn]'));
                                const allow = btns.find(b => /allow|approve|grant|accept|continue/i.test(b.textContent+(b.value||'')));
                                if (!allow) return null;
                                const r = allow.getBoundingClientRect();
                                return {x: r.left+r.width/2, y: r.top+r.height/2, text: (allow.textContent||allow.value||'').trim()};
                            })()
                        """)
                        if result:
                            print(f"  Clicking Allow: {result.get('text')}")
                            x, y = int(result["x"]), int(result["y"])
                            for etype in ["mousePressed", "mouseReleased"]:
                                await _send(ws, "Input.dispatchMouseEvent",
                                            {"type": etype, "x": x, "y": y,
                                             "button": "left", "clickCount": 1})

        if not auth_code:
            print("\nTimeout — could not get authorization code.")
            print("If you see a prompt in Chrome, complete it manually and re-run.")
            return

        # Exchange code for tokens
        print("\nExchanging code for tokens...")
        try:
            token_data = _exchange_code(auth_code, verifier)
        except Exception as e:
            print(f"Token exchange failed: {e}")
            return

        access_token  = token_data.get("access_token", "")
        refresh_token = token_data.get("refresh_token", "")
        expires_in    = token_data.get("expires_in", "?")

        if not refresh_token:
            print("WARNING: No refresh_token returned. CC may not have granted offline_access.")
            print(f"Access token (expires in {expires_in}s): {access_token[:60]}...")
            return

        print(f"\n{'='*60}")
        print("SUCCESS!")
        print(f"Access token  (expires {expires_in}s): {access_token[:50]}...")
        print(f"Refresh token (long-lived):             {refresh_token[:50]}...")
        print(f"{'='*60}")

        # Save refresh token
        _save_refresh_token(refresh_token)
        print(f"\nSaved CC_REFRESH_TOKEN to {SECRETS_PATH}")
        print("\n--- Paste this into Streamlit Cloud secrets ---")
        print(f'CC_REFRESH_TOKEN = "{refresh_token}"')
        print("----------------------------------------------")
        print("\nDone! The app will now auto-refresh CC tokens on every run.")


if __name__ == "__main__":
    asyncio.run(main())
