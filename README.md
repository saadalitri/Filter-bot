# Telegram Filter Bot — Render Deployment

## ⚠️ First: rotate your credentials
The version of this file you shared had a real bot token and MongoDB
password hardcoded in it. Once credentials appear in a chat/file that
leaves your machine, treat them as burned:

1. In Telegram, message **@BotFather** → `/revoke` → pick your bot → get
   a new token.
2. In MongoDB Atlas → Database Access, reset the password for the
   `echoharmonic21_db_user` user (or delete it and create a new one).
3. Never paste the new token/password into the source file — only ever
   set them as environment variables (steps below).

## What changed in this version
- All secrets (`BOT_TOKEN`, `MONGO_URI`, `DB_NAME`, `SUDO_USERS`,
  `LOG_CHANNEL_ID`, and the branding URLs) now come **only** from
  environment variables. The bot refuses to start with a clear error
  if `BOT_TOKEN`, `MONGO_URI`, or `DB_NAME` is missing.
- Fixed the "Add me to your group" button: `BOT_USERNAME_FOR_ADD` must
  be set **without** the `@` (e.g. `Filter_Assistbot`, not
  `@Filter_Assistbot`) — the old hardcoded value had the `@` baked in,
  which produced a broken `t.me/@...` link.
- Added optional webhook mode so this can run on a Render **Web
  Service** (which requires binding to `$PORT`), not just a
  Background Worker. It auto-detects which mode to use based on
  whether `WEBHOOK_URL` is set.

## Deploy on Render

### Option A — Background Worker (simplest, recommended)
1. Push this folder to a GitHub repo.
2. In Render: **New → Background Worker**, connect the repo.
3. Build command: `pip install -r requirements.txt`
4. Start command: `python filterbot.py`
5. Under **Environment**, add:
   - `BOT_TOKEN`
   - `MONGO_URI`
   - `DB_NAME`
   - `SUDO_USERS` (optional, comma-separated numeric Telegram IDs)
   - `LOG_CHANNEL_ID` (optional)
   - `BOT_USERNAME_FOR_ADD` (optional, no `@`)
   - `OWNER_URL`, `UPDATES_URL`, `START_IMAGE_URL`, `BOT_NAME` (optional branding)
6. Deploy. No `WEBHOOK_URL` is set, so it runs in polling mode — no
   port required.

### Option B — Web Service (webhook mode)
Use this if you're on a plan/setup that only offers Web Services.
1. Same repo/build/start commands as above.
2. Set the same env vars as Option A, **plus**:
   - `WEBHOOK_URL` = `https://<your-service-name>.onrender.com`
     (fill this in *after* your first deploy gives you the URL, then
     redeploy or restart)
3. Render sets `PORT` automatically — you don't need to add it.
4. On startup the bot detects `WEBHOOK_URL` and calls
   `run_webhook(...)`, binding to `0.0.0.0:$PORT` so Render's health
   check passes.

### `render.yaml` (Blueprint)
A `render.yaml` is included for Render's Blueprint deploy — it
defaults to the Background Worker setup. Edit it if you want the Web
Service/webhook variant (comment at the bottom shows the change).

## Local testing
```bash
pip install -r requirements.txt
export BOT_TOKEN="..."
export MONGO_URI="..."
export DB_NAME="..."
python filterbot.py
```
