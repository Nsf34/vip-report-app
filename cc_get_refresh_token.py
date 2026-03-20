#!/usr/bin/env python3
"""
One-time CC OAuth setup. Run this locally:
    python3 ~/vip-report-app/cc_get_refresh_token.py

It will:
1. Open your browser to CC login
2. Ask you to paste the redirect URL from your address bar
3. Exchange the code instantly and save everywhere
"""
import base64
import os
import re as _re
import sys
import urllib.parse
import webbrowser

import requests

CC_CLIENT_ID     = "ae507531-707f-4bd1-9eb0-ae6685b01e6a"
CC_CLIENT_SECRET = "aMVpumtSXDF7LXhyo1UAFg"
CC_TOKEN_URL     = "https://identity.constantcontact.com/oauth2/aus1lm3ry9mF7x2Ja0h8/v1/token"
REDIRECT_URI     = "https://localhost"
SECRETS_PATH     = os.path.expanduser("~/.streamlit/secrets.toml")
GIST_ID          = "e0905bc2fa7192d0618ffc4926332bfe"

def _load_pat():
    """Read GITHUB_PAT from secrets.toml (never hardcode in source)."""
    try:
        content = open(SECRETS_PATH).read()
        m = _re.search(r'GITHUB_PAT\s*=\s*"([^"]+)"', content)
        if m:
            return m.group(1)
    except Exception:
        pass
    return os.environ.get("GITHUB_PAT", "")


def _pkce():
    verifier  = base64.urlsafe_b64encode(os.urandom(32)).rstrip(b"=").decode()
    challenge = base64.urlsafe_b64encode(
        hashlib.sha256(verifier.encode()).digest()
    ).rstrip(b"=").decode()
    return verifier, challenge


def _exchange(code, verifier):
    basic = base64.b64encode(f"{CC_CLIENT_ID}:{CC_CLIENT_SECRET}".encode()).decode()
    resp = requests.post(CC_TOKEN_URL,
        data={"grant_type": "authorization_code", "code": code,
              "redirect_uri": REDIRECT_URI, "code_verifier": verifier},
        headers={"Content-Type": "application/x-www-form-urlencoded",
                 "Authorization": f"Basic {basic}"},
        timeout=15)
    resp.raise_for_status()
    return resp.json()


def _save_local(refresh_token):
    os.makedirs(os.path.dirname(SECRETS_PATH), exist_ok=True)
    if os.path.exists(SECRETS_PATH):
        content = open(SECRETS_PATH).read()
        if "CC_REFRESH_TOKEN" in content:
            content = re.sub(r'CC_REFRESH_TOKEN\s*=\s*"[^"]*"',
                             f'CC_REFRESH_TOKEN = "{refresh_token}"', content)
        else:
            content += f'\nCC_REFRESH_TOKEN = "{refresh_token}"\n'
        content = re.sub(r'\nCC_ACCESS_TOKEN\s*=\s*"[^"]*"', '', content)
    else:
        content = f'CC_REFRESH_TOKEN = "{refresh_token}"\n'
    with open(SECRETS_PATH, "w") as f:
        f.write(content)
    print(f"  ✓ Saved to {SECRETS_PATH}")


def _save_gist(refresh_token):
    try:
        r = requests.patch(
            f"https://api.github.com/gists/{GIST_ID}",
            headers={"Authorization": f"token {_load_pat()}",
                     "Accept": "application/vnd.github+json"},
            json={"files": {"cc_refresh_token.txt": {"content": refresh_token}}},
            timeout=10)
        if r.status_code == 200:
            print(f"  ✓ Saved to GitHub Gist")
        else:
            print(f"  ✗ Gist write failed: {r.status_code}")
    except Exception as e:
        print(f"  ✗ Gist write error: {e}")


def main():
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

    print()
    print("Opening browser to Constant Contact login...")
    webbrowser.open(auth_url)
    print()
    print("=" * 60)
    print("After you log in and click Allow, your browser will show")
    print("a 'This site can't be reached' error — that's expected.")
    print()
    print("IMMEDIATELY copy the full URL from the address bar and")
    print("paste it here, then press Enter.")
    print("=" * 60)
    print()

    try:
        raw = input("Paste redirect URL here: ").strip()
    except (KeyboardInterrupt, EOFError):
        print("\nCancelled.")
        sys.exit(0)

    # Extract code
    parsed = urllib.parse.urlparse(raw)
    qs = urllib.parse.parse_qs(parsed.query)
    code = qs.get("code", [None])[0]

    if not code:
        # Maybe they pasted just the code value
        if raw and "?" not in raw and "&" not in raw:
            code = raw
        else:
            print("Could not find code= in that URL. Please try again.")
            sys.exit(1)

    print(f"\nExchanging code for tokens...")
    try:
        data = _exchange(code, verifier)
    except Exception as e:
        print(f"Token exchange failed: {e}")
        sys.exit(1)

    refresh_token = data.get("refresh_token", "")
    if not refresh_token:
        print("ERROR: No refresh_token in response:", data)
        sys.exit(1)

    print()
    print("=" * 60)
    print("SUCCESS!")
    print(f"Refresh token: {refresh_token[:50]}...")
    print("=" * 60)
    print()
    print("Saving...")
    _save_local(refresh_token)
    _save_gist(refresh_token)

    print()
    print("Now add these 3 lines to Streamlit Cloud secrets")
    print("(go to vip-report.streamlit.app → Manage app → Secrets):")
    print()
    print(f'CC_REFRESH_TOKEN = "{refresh_token}"')
    print(f'CC_GIST_ID = "{GIST_ID}"')
    print(f'GITHUB_PAT = "{_load_pat() or "<your-github-pat>"}"')
    print()
    print("After that, the app handles token rotation permanently.")


if __name__ == "__main__":
    main()
