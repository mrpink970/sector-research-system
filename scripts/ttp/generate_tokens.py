#!/usr/bin/env python3
"""
One-time script to generate Schwab OAuth tokens
Run this ONCE on your local machine (or GitHub Codespaces)
Tokens will be saved and then used in GitHub Actions
"""

import os
import json
from pathlib import Path
from schwab import auth

# These should be set as environment variables or entered directly
API_KEY = os.environ.get("SCHWAB_API_KEY", "YOUR_API_KEY_HERE")
APP_SECRET = os.environ.get("SCHWAB_APP_SECRET", "YOUR_SECRET_HERE")
CALLBACK_URL = os.environ.get("SCHWAB_CALLBACK_URL", "https://127.0.0.1:8182")

print("🔐 Starting Schwab OAuth flow...")
print("A browser will open. Log in to Schwab and authorize.")
print("After redirect, tokens will be saved to schwab_tokens.json")

auth.easy_client(
    api_key=API_KEY,
    app_secret=APP_SECRET,
    callback_url=CALLBACK_URL,
    token_path="schwab_tokens.json"
)

print("✅ Tokens saved to schwab_tokens.json")
print("Add this file's content as a GitHub Secret named SCHWAB_TOKENS")
