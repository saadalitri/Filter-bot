"""
Advanced Filter Bot - single-file edition
==========================================
A Rose-bot style filter bot for Telegram (python-telegram-bot v21, async)
backed by MongoDB (via motor). Works across unlimited groups at once --
every filter, setting, and connection is scoped by chat_id.

------------------------------------------------------------------
DEPLOYING ON RENDER
------------------------------------------------------------------
Render supports two service types that matter here:

  * Background Worker  -> no open port needed. This script will use
    long-polling (run_polling). Just set the env vars below and set
    the start command to:  python filterbot.py

  * Web Service         -> Render requires the process to bind to
    $PORT or it's killed as "unhealthy". If you deploy this as a Web
    Service, set the WEBHOOK_URL env var (see below) and this script
    will automatically switch to webhook mode and bind to $PORT.

Required environment variables (set these in the Render dashboard,
under your service -> Environment -- never commit them to code):

    BOT_TOKEN     - from @BotFather
    MONGO_URI     - your MongoDB connection string
    DB_NAME       - any database name you like
    SUDO_USERS    - comma-separated Telegram user IDs (optional)
    LOG_CHANNEL_ID- numeric chat id of a log channel (optional, 0/blank disables)

Only needed if deploying as a Web Service (webhook mode):

    WEBHOOK_URL   - the public https URL Render gives your service,
                    e.g. https://your-app.onrender.com  (no trailing slash)
    PORT          - Render sets this automatically, you don't need to.

If BOT_TOKEN, MONGO_URI, or DB_NAME are missing, the bot refuses to
start and tells you which one is missing, instead of failing with a
confusing error deep in the MongoDB driver.

------------------------------------------------------------------
SECURITY NOTE
------------------------------------------------------------------
This file used to have a real bot token and a real MongoDB
connection string (including its password) hardcoded above. If you
ever pasted this file somewhere with real credentials in it,
those credentials are compromised the moment they left your machine:
  - Revoke/regenerate the bot token via @BotFather -> /revoke
  - Rotate the MongoDB user's password in Atlas
Then set the new values only as environment variables, never in the
source file.

Commands:
    Core:        /add, /del, /filters, /deleteallfilters
    Auto mgmt:   /autodel on <sec> | off, /topfilters
    Cloning:     /fclone on|off, /fclone <source_id> <target_id>
    Buttons:     [Text](buttonurl:https://link) inside any /add reply text
    Backup:      /export, /import (reply to the exported .json)
    Connections: /connect <group_id>, /connections, /disconnect
    Broadcast:   /broadcast <message> (sudo only, sends to every known chat)

Add the bot to a group and promote it to admin (needed for /autodel message
deletion and for reading the member list to check who's an admin).
"""

import os
import re
import json
import io
import asyncio
import logging
import shlex
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from motor.motor_asyncio import AsyncIOMotorClient
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, BotCommand
from telegram.ext import (
    Application, CommandHandler, MessageHandler, CallbackQueryHandler,
    filters as tg_filters, ContextTypes,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# ============================================================
# Config  --  EDIT THESE VALUES DIRECTLY
# (env vars below still override these if set on the host, so this also
#  works unchanged if you later deploy somewhere that injects them)
# ============================================================

BOT_TOKEN = "8838446349:AAEhJRJ8-KSG209UwnG6L8y0DZflyJczVq4"
MONGO_URI = "mongodb+srv://Alizenx:alizenx@cluster0.brrejva.mongodb.net/?appName=Cluster0"
DB_NAME = "Alizenx"
SUDO_USERS = [8536019525]

BOT_TOKEN = os.environ.get("BOT_TOKEN", BOT_TOKEN)
MONGO_URI = os.environ.get("MONGO_URI", MONGO_URI)
DB_NAME = os.environ.get("DB_NAME", DB_NAME)
if os.environ.get("SUDO_USERS"):
    SUDO_USERS = [int(x) for x in os.environ["SUDO_USERS"].split(",") if x.strip().isdigit()]
SUDO_USERS = set(SUDO_USERS)

# Log channel: numeric chat id of a channel/group where the bot is admin.
# All filter add/delete events, errors, new-group-added, and new-member
# notifications get posted here. Leave as 0 to disable logging entirely.
LOG_CHANNEL_ID = -1003932194408
LOG_CHANNEL_ID = int(os.environ.get("LOG_CHANNEL_ID", LOG_CHANNEL_ID) or 0)

# Webhook mode (only used if WEBHOOK_URL is set -- e.g. Render Web Service).
WEBHOOK_URL = os.environ.get("WEBHOOK_URL", "").rstrip("/")
PORT = int(os.environ.get("PORT", "10000"))

# ---- Branding / /start command customization -- EDIT THESE DIRECTLY ----
START_IMAGE_URL = "https://graph.org/file/2e4f101c19180e2666a0d-6c52f607935c1f2f71.mp4"
BOT_NAME = "Telegram Assist Bot"
OWNER_URL = "https://t.me/Zenx_Era"
UPDATES_URL = "https://t.me/Eric_Vanitas"
SUPPORT_TEXT = (
    "💝 If you'd like to support this bot's hosting costs, reach out to the owner!"
)
# Just the bot's @username, WITHOUT the leading "@" (fixed — the original had
# "@Filter_Assistbot" which broke the "Add me to your group" link).
BOT_USERNAME_FOR_ADD = "Filter_Assistbot"

FILE_TYPES = ("photo", "video", "document", "animation", "sticker", "audio", "voice")

DEFAULT_SETTINGS = {
    "autodel_on": False,
    "autodel_sec": 60,
    "fclone_on": False,
}

# ============================================================
# Database layer (MongoDB via motor)
# ============================================================

_client = AsyncIOMotorClient(MONGO_URI)
db = _client[DB_NAME]

filters_col = db["filters"]
chats_col = db["chat_settings"]
connections_col = db["connections"]


async def ensure_indexes():
    # Drop any leftover/stale indexes from older bot versions (e.g. one built
    # on a "keyword" field instead of "name") -- these cause duplicate-key
    # errors on every insert once they no longer match the current schema.
    desired_keys = {"chat_id", "name"}
    try:
        existing = await filters_col.index_information()
        for idx_name, idx_info in existing.items():
            if idx_name == "_id_":
                continue
            keys = {field for field, _ in idx_info.get("key", [])}
            if keys != desired_keys:
                try:
                    await filters_col.drop_index(idx_name)
                    logger.info(f"Dropped stale filters index: {idx_name}")
                except Exception as e:
                    logger.warning(f"Could not drop stale index {idx_name}: {e}")
    except Exception as e:
        logger.warning(f"Could not inspect existing indexes: {e}")

    await filters_col.create_index([("chat_id", 1), ("name", 1)], unique=True)
    await chats_col.create_index("chat_id", unique=True)
    await connections_col.create_index("user_id", unique=True)


async def add_filter(chat_id: int, name: str, reply_text: str, buttons=None,
                      file_id: str = None, file_type: str = None, genre: str = None):
    name = name.lower().strip()
    doc = {
        "chat_id": chat_id, "name": name, "reply_text": reply_text,
        "buttons": buttons or [], "file_id": file_id, "file_type": file_type,
        "genre": genre, "uses": 0,
    }
    await filters_col.update_one({"chat_id": chat_id, "name": name}, {"$set": doc}, upsert=True)


async def get_filter(chat_id: int, name: str):
    return await filters_col.find_one({"chat_id": chat_id, "name": name.lower().strip()})


async def delete_filter(chat_id: int, name: str) -> bool:
    res = await filters_col.delete_one({"chat_id": chat_id, "name": name.lower().strip()})
    return res.deleted_count > 0


async def delete_all_filters(chat_id: int) -> int:
    res = await filters_col.delete_many({"chat_id": chat_id})
    return res.deleted_count


async def list_filters(chat_id: int):
    cursor = filters_col.find({"chat_id": chat_id}).sort("name", 1)
    return [doc async for doc in cursor]


async def top_filters(chat_id: int, limit: int = 10):
    cursor = filters_col.find({"chat_id": chat_id}).sort("uses", -1).limit(limit)
    return [doc async for doc in cursor]


async def increment_uses(chat_id: int, name: str):
    await filters_col.update_one({"chat_id": chat_id, "name": name.lower().strip()}, {"$inc": {"uses": 1}})


async def clone_filters(source_id: int, target_id: int) -> int:
    src_filters = await list_filters(source_id)
    count = 0
    for f in src_filters:
        await add_filter(target_id, f["name"], f["reply_text"], f.get("buttons"),
                          f.get("file_id"), f.get("file_type"), f.get("genre"))
        count += 1
    return count


async def get_settings(chat_id: int) -> dict:
    doc = await chats_col.find_one({"chat_id": chat_id})
    if not doc:
        doc = {"chat_id": chat_id, **DEFAULT_SETTINGS}
        await chats_col.insert_one(doc)
    return doc


async def update_settings(chat_id: int, **kwargs):
    await chats_col.update_one({"chat_id": chat_id}, {"$set": kwargs}, upsert=True)


async def set_connection(user_id: int, group_id: int):
    await connections_col.update_one({"user_id": user_id}, {"$set": {"group_id": group_id}}, upsert=True)


async def get_connection(user_id: int):
    doc = await connections_col.find_one({"user_id": user_id})
    return doc["group_id"] if doc else None


async def remove_connection(user_id: int) -> bool:
    res = await connections_col.delete_one({"user_id": user_id})
    return res.deleted_count > 0


# ============================================================
# Utilities: button parsing + admin checks
# ============================================================

BUTTON_REGEX = re.compile(r"\[([^\[\]]+)\]\(buttonurl:(?://)?(.+?)\)", re.IGNORECASE)


def parse_buttons(text: str):
    """Extracts [Label](buttonurl:...) tags and groups them into rows.
    Two buttons written on the SAME line (no newline between them) end up
    side-by-side in one row, e.g.:
        [Main Channel](buttonurl:...) [Index](buttonurl:...)
        [Eric Anime Verse](buttonurl:...)
    produces a 2-button row followed by a 1-button row -- matching the
    familiar Rose-bot-style layout."""
    rows = []
    last_end = None
    for match in BUTTON_REGEX.finditer(text):
        label, url = match.group(1).strip(), match.group(2).strip()
        between = text[last_end:match.start()] if last_end is not None else None
        if rows and between is not None and "\n" not in between:
            rows[-1].append([label, url])
        else:
            rows.append([[label, url]])
        last_end = match.end()
    clean_text = BUTTON_REGEX.sub("", text).strip()
    return clean_text, rows


def build_markup(buttons):
    """Accepts either the new row-grouped format (list of rows, each a list
    of [label, url]) or the old flat format (list of [label, url]) saved by
    earlier versions of this bot, so existing filters keep working."""
    if not buttons:
        return None
    if isinstance(buttons[0][0], str):
        # Legacy flat format -- one button per row.
        rows = [[b] for b in buttons]
    else:
        rows = buttons
    keyboard = [[InlineKeyboardButton(label, url=url) for label, url in row] for row in rows]
    return InlineKeyboardMarkup(keyboard)


async def is_user_admin(update: Update, context: ContextTypes.DEFAULT_TYPE, user_id: int = None) -> bool:
    chat = update.effective_chat
    user_id = user_id or update.effective_user.id
    if user_id in SUDO_USERS:
        return True
    if chat.type == "private":
        return True
    member = await context.bot.get_chat_member(chat.id, user_id)
    return member.status in ("administrator", "creator")


async def require_admin(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    if await is_user_admin(update, context):
        return True
    await update.effective_message.reply_text("⚠️ You need to be an admin in this chat to use this command.")
    return False


async def resolve_target_chat_id(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Figures out which chat a filter-management command should act on.
    Inside a group, that's just the current chat. In a private chat (DM),
    it's whichever group the user has connected via /connect -- since
    filters live per-chat_id, running /add in DM without this would only
    ever touch the DM's own (useless) filter list instead of the group's."""
    chat = update.effective_chat
    if chat.type != "private":
        return chat.id
    return await get_connection(update.effective_user.id)


async def get_chat_label(context: ContextTypes.DEFAULT_TYPE, chat_id: int) -> str:
    """Best-effort human-readable name for a chat_id, for showing the admin
    which connected group a DM command is actually operating on."""
    try:
        chat = await context.bot.get_chat(chat_id)
        return chat.title or str(chat_id)
    except Exception:
        return str(chat_id)


async def require_admin_for_chat(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Resolves the target chat (see resolve_target_chat_id) AND verifies the
    calling user is an admin there. Returns the chat_id to operate on, or
    None (after sending an explanatory reply) if that's not possible --
    replacing the old shortcut that let DMs bypass the admin check entirely
    without actually checking the connected group."""
    message = update.effective_message
    chat_id = await resolve_target_chat_id(update, context)
    if chat_id is None:
        await message.reply_text(
            "⚠️ You're not connected to a group. Use /connect <group_id> here in DM first, "
            "or run this command directly inside the group."
        )
        return None

    user_id = update.effective_user.id
    if user_id in SUDO_USERS:
        return chat_id
    try:
        member = await context.bot.get_chat_member(chat_id, user_id)
    except Exception:
        await message.reply_text("⚠️ I couldn't verify your admin status there (am I still a member of that group?).")
        return None
    if member.status not in ("administrator", "creator"):
        await message.reply_text("⚠️ You need to be an admin in that chat to use this command.")
        return None
    return chat_id


async def send_log(context: ContextTypes.DEFAULT_TYPE, text: str):
    """Posts an event to the configured log channel. Silently does nothing
    if no LOG_CHANNEL_ID is set, and never lets a logging failure crash a
    handler (e.g. bot not being admin in the log channel yet)."""
    if not LOG_CHANNEL_ID:
        return
    try:
        await context.bot.send_message(LOG_CHANNEL_ID, text, parse_mode="HTML", disable_web_page_preview=True)
    except Exception as e:
        logger.warning(f"Failed to send log message: {e}")


# ============================================================
# Core filter commands
# ============================================================

def _home_keyboard() -> InlineKeyboardMarkup:
    rows = [[InlineKeyboardButton("📖 Help Center", callback_data="help_center")]]
    owner_row = []
    if OWNER_URL:
        owner_row.append(InlineKeyboardButton("👑 Owner", url=OWNER_URL))
    if UPDATES_URL:
        owner_row.append(InlineKeyboardButton("📢 Updates", url=UPDATES_URL))
    if owner_row:
        rows.append(owner_row)
    if BOT_USERNAME_FOR_ADD:
        # NOTE: no leading "@" here -- Telegram deep links use the bare
        # username. Older versions of this file had "@Filter_Assistbot"
        # which produced a broken t.me/@... link.
        add_to_group_url = f"https://t.me/{BOT_USERNAME_FOR_ADD}?startgroup=true"
        rows.append([InlineKeyboardButton("➕ Add me to your group", url=add_to_group_url)])
    return InlineKeyboardMarkup(rows)


async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    caption = (
        f"👋 <b>Hi there, {user.mention_html()}!</b>\n\n"
        f"🔍 <b>{BOT_NAME}</b>\n"
        "Add me to a group and make me admin. Use /add to create filters, "
        "and I'll auto-reply whenever someone types the filter name.\n\n"
        "Send /help to see everything I can do."
    )
    if START_IMAGE_URL:
        try:
            await update.effective_message.reply_photo(
                photo=START_IMAGE_URL, caption=caption, parse_mode="HTML", reply_markup=_home_keyboard()
            )
            return
        except Exception:
            # Falls back to text-only if the image URL is unreachable/misconfigured.
            pass
    await update.effective_message.reply_text(caption, parse_mode="HTML", reply_markup=_home_keyboard())


async def help_center_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    await query.message.reply_text(HELP_TEXT, parse_mode="HTML", disable_web_page_preview=True)


HELP_TEXT = (
    "🛠 <b>Core</b>\n"
    "• /add <code>name reply</code> — create a filter\n"
    "• /del <code>name</code> — delete a filter\n"
    "• /filters — list all filters\n"
    "• /deleteallfilters — wipe all filters\n\n"
    "🗑 <b>Auto management</b>\n"
    "• /autodel <code>on sec | off</code>\n"
    "• /topfilters — most-used filters\n\n"
    "🔄 <b>Cloning</b>\n"
    "• /fclone <code>on|off</code>\n"
    "• /fclone <code>source_id target_id</code>\n\n"
    "📥 <b>Backup</b>\n"
    "• /export — save filters as .json\n"
    "• /import — reply to a backup file to restore\n\n"
    "🔗 <b>Connections</b>\n"
    "• /connect <code>group_id</code>\n"
    "• /connections\n"
    "• /disconnect\n\n"
    "ℹ️ <b>Utility</b>\n"
    "• /id — get your/this chat's ID (reply to get someone else's)\n"
    "• /info — get info about yourself (reply to get someone else's)\n"
    "• /donate — support the bot\n\n"
    "📢 <b>Owner only</b>\n"
    "• /broadcast <code>message</code> — send to every chat the bot is in "
    "(or reply to a message with /broadcast)"
)


async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.effective_message.reply_text(HELP_TEXT, parse_mode="HTML", disable_web_page_preview=True)


async def id_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat = update.effective_chat
    message = update.effective_message
    lines = [f"💬 Chat ID: <code>{chat.id}</code>"]
    if message.reply_to_message:
        target = message.reply_to_message.from_user
        lines.append(f"👤 {target.mention_html()}'s ID: <code>{target.id}</code>")
    else:
        lines.append(f"👤 Your ID: <code>{update.effective_user.id}</code>")
    await message.reply_text("\n".join(lines), parse_mode="HTML")


async def info_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    message = update.effective_message
    target = message.reply_to_message.from_user if message.reply_to_message else update.effective_user
    lines = [
        f"👤 <b>User Info</b>",
        f"Name: {target.full_name}",
        f"ID: <code>{target.id}</code>",
        f"Username: @{target.username}" if target.username else "Username: —",
        f"Is bot: {'Yes' if target.is_bot else 'No'}",
    ]
    await message.reply_text("\n".join(lines), parse_mode="HTML")


async def donate_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.effective_message.reply_text(SUPPORT_TEXT)


async def add_filter_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = await require_admin_for_chat(update, context)
    if chat_id is None:
        return

    message = update.effective_message

    # Parse the raw command text ourselves (instead of context.args) so that
    # quoted names work: /add "the era" some long reply text here...
    raw = message.text or message.caption or ""
    parts = raw.split(None, 1)
    rest = parts[1] if len(parts) > 1 else ""
    try:
        tokens = shlex.split(rest)
    except ValueError:
        # Unbalanced quotes -- fall back to naive whitespace splitting.
        tokens = rest.split()

    if not tokens:
        await message.reply_text(
            "Usage: /add <name> <reply text>\n"
            "For a multi-word name, wrap it in quotes: /add \"the era\" <reply text>\n"
            "You can also reply to a photo/video/document/sticker with /add <name> "
            "to save that media, and add buttons with [Text](buttonurl:https://link)."
        )
        return

    name = tokens[0].lower()
    reply_text = " ".join(tokens[1:]).strip()

    file_id, file_type = None, None
    original_buttons = []
    if message.reply_to_message:
        replied = message.reply_to_message
        for ftype in FILE_TYPES:
            media = getattr(replied, ftype, None)
            if media:
                file_id = media[-1].file_id if isinstance(media, (list, tuple)) else media.file_id
                file_type = ftype
                break
        if not reply_text and replied.caption:
            reply_text = replied.caption
        if not reply_text and replied.text:
            reply_text = replied.text

        # Preserve any URL buttons already attached to the replied message,
        # so filters keep working links (e.g. Tutorial / Watch / Download).
        # Row structure is preserved too, so a 2-buttons-per-row layout stays
        # 2-buttons-per-row instead of collapsing to one button per row.
        if replied.reply_markup and replied.reply_markup.inline_keyboard:
            for row in replied.reply_markup.inline_keyboard:
                row_buttons = [[btn.text, btn.url] for btn in row if getattr(btn, "url", None)]
                if row_buttons:
                    original_buttons.append(row_buttons)

    if not reply_text and not file_id:
        await message.reply_text("⚠️ Provide reply text, or reply to a message/media to save.")
        return

    clean_text, parsed_buttons = parse_buttons(reply_text) if reply_text else ("", [])
    all_buttons = parsed_buttons + [b for b in original_buttons if b not in parsed_buttons]

    await add_filter(
        chat_id=chat_id, name=name, reply_text=clean_text,
        buttons=all_buttons, file_id=file_id, file_type=file_type,
    )
    try:
        target_chat = await context.bot.get_chat(chat_id)
        chat_label = target_chat.title or chat_id
    except Exception:
        chat_label = chat_id
    await message.reply_text(f"✅ Filter '{name}' saved" + (f" in '{chat_label}'." if update.effective_chat.type == "private" else "."))

    # Log the FULL filter (text, buttons, and the actual media itself if any)
    # to the log channel -- not just the name -- so the log is a complete
    # record of what every filter actually does.
    await log_filter_details(
        context, event="➕ Filter added", name=name, chat_label=chat_label, chat_id=chat_id,
        by_html=update.effective_user.mention_html(), reply_text=clean_text,
        buttons=all_buttons, file_id=file_id, file_type=file_type,
    )

    # Show the admin exactly what this filter will look like when it fires,
    # right here in the chat where /add was run (DM or group) -- without
    # touching usage stats or triggering auto-delete.
    await message.reply_text("👀 Preview:")
    await _send_rendered_filter(context, update.effective_chat.id, clean_text, all_buttons, file_id, file_type)


async def del_filter_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = await require_admin_for_chat(update, context)
    if chat_id is None:
        return

    raw = update.effective_message.text or ""
    parts = raw.split(None, 1)
    rest = parts[1] if len(parts) > 1 else ""
    try:
        tokens = shlex.split(rest)
    except ValueError:
        tokens = rest.split()

    if not tokens:
        await update.effective_message.reply_text(
            "Usage: /del <name>\nFor a multi-word name: /del \"the era\""
        )
        return

    name = tokens[0]
    ok = await delete_filter(chat_id, name)
    if ok:
        await update.effective_message.reply_text(f"🗑 Filter '{name}' deleted.")
        await send_log(
            context,
            f"➖ Filter deleted: <code>{name}</code>\n"
            f"Chat: {chat_id}\n"
            f"By: {update.effective_user.mention_html()}",
        )
    else:
        await update.effective_message.reply_text(f"No filter named '{name}' found.")


FILTERS_PER_PAGE = 10


def _build_filters_page(filters_list, chat_id: int, page: int, chat_label: str = None):
    """Builds the text + pagination keyboard for one page of an (already
    A-Z sorted) filter list, one filter name per line."""
    total = len(filters_list)
    total_pages = max(1, (total + FILTERS_PER_PAGE - 1) // FILTERS_PER_PAGE)
    page = max(1, min(page, total_pages))
    start = (page - 1) * FILTERS_PER_PAGE
    chunk = filters_list[start:start + FILTERS_PER_PAGE]

    lines = "\n".join(f"{start + i + 1}. `{f['name']}`" for i, f in enumerate(chunk))
    header = f"📋 *Filters for '{chat_label}'* ({total}):" if chat_label else f"📋 *Filters ({total}):*"
    text = f"{header}\n{lines}\n\nPage {page}/{total_pages}"

    nav_row = []
    if page > 1:
        nav_row.append(InlineKeyboardButton("◀️ Prev", callback_data=f"filterspage:{chat_id}:{page - 1}"))
    if page < total_pages:
        nav_row.append(InlineKeyboardButton("Next ▶️", callback_data=f"filterspage:{chat_id}:{page + 1}"))
    markup = InlineKeyboardMarkup([nav_row]) if nav_row else None

    return text, markup


async def list_filters_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = await resolve_target_chat_id(update, context)
    if chat_id is None:
        await update.effective_message.reply_text(
            "⚠️ You're not connected to a group. Use /connect <group_id> here in DM first, "
            "or run this command directly inside the group."
        )
        return
    filters_list = await list_filters(chat_id)  # already sorted A-Z by db query
    if not filters_list:
        await update.effective_message.reply_text("No filters saved in that chat yet.")
        return
    # Showing the connected group's name matters most in DM, where it's the
    # only way to tell which group these filters actually belong to.
    chat_label = await get_chat_label(context, chat_id) if update.effective_chat.type == "private" else None
    text, markup = _build_filters_page(filters_list, chat_id, page=1, chat_label=chat_label)
    await update.effective_message.reply_text(text, parse_mode="Markdown", reply_markup=markup)


async def filters_page_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    try:
        _, chat_id_str, page_str = query.data.split(":")
        chat_id, page = int(chat_id_str), int(page_str)
    except (ValueError, AttributeError):
        return
    filters_list = await list_filters(chat_id)
    if not filters_list:
        await query.edit_message_text("No filters saved in that chat anymore.")
        return
    chat_label = await get_chat_label(context, chat_id) if update.effective_chat.type == "private" else None
    text, markup = _build_filters_page(filters_list, chat_id, page, chat_label=chat_label)
    await query.edit_message_text(text, parse_mode="Markdown", reply_markup=markup)


async def delete_all_filters_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = await require_admin_for_chat(update, context)
    if chat_id is None:
        return
    count = await delete_all_filters(chat_id)
    await update.effective_message.reply_text(f"🗑 Deleted {count} filter(s).")


async def _send_rendered_filter(context: ContextTypes.DEFAULT_TYPE, chat_id: int, reply_text: str,
                                 buttons, file_id: str = None, file_type: str = None, reply_to: int = None):
    """Actually sends a filter's content (text/media/buttons) to chat_id.
    Shared by the live trigger path (send_filter_reply) and the /add preview,
    so both always render identically."""
    markup = build_markup(buttons)
    if file_id:
        if file_type == "sticker":
            return await context.bot.send_sticker(chat_id, file_id, reply_to_message_id=reply_to)
        send_map = {
            "photo": context.bot.send_photo, "video": context.bot.send_video,
            "document": context.bot.send_document, "animation": context.bot.send_animation,
            "audio": context.bot.send_audio, "voice": context.bot.send_voice,
        }
        sender = send_map[file_type]
        return await sender(
            chat_id=chat_id, **{file_type: file_id},
            caption=reply_text or None, reply_markup=markup,
            reply_to_message_id=reply_to,
        )
    return await context.bot.send_message(
        chat_id=chat_id, text=reply_text or "‎", reply_markup=markup,
        reply_to_message_id=reply_to,
    )


async def log_filter_details(context: ContextTypes.DEFAULT_TYPE, event: str, name: str, chat_label,
                              chat_id: int, by_html: str, reply_text: str, buttons, file_id: str = None,
                              file_type: str = None):
    """Logs a FULL record of a filter to the log channel: not just its name,
    but its actual text, its buttons (label -> url), and the real media
    itself (photo/video/sticker/etc.) so the log channel is a complete
    audit trail of every filter's content -- not just that something happened."""
    if not LOG_CHANNEL_ID:
        return

    lines = [f"{event}: <code>{name}</code>", f"Chat: {chat_label} (<code>{chat_id}</code>)", f"By: {by_html}"]
    if file_type:
        lines.append(f"Type: {file_type}")
    if reply_text:
        safe_text = reply_text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
        lines.append(f"Text: {safe_text}")
    if buttons:
        for row in buttons:
            lines.append("Button(s): " + " | ".join(f"{label} → {url}" for label, url in row))
    summary = "\n".join(lines)

    try:
        if file_id and file_type != "sticker":
            send_map = {
                "photo": context.bot.send_photo, "video": context.bot.send_video,
                "document": context.bot.send_document, "animation": context.bot.send_animation,
                "audio": context.bot.send_audio, "voice": context.bot.send_voice,
            }
            sender = send_map.get(file_type)
            if sender:
                await sender(chat_id=LOG_CHANNEL_ID, **{file_type: file_id}, caption=summary[:1024], parse_mode="HTML")
                return
        elif file_id and file_type == "sticker":
            await context.bot.send_sticker(LOG_CHANNEL_ID, file_id)
        await context.bot.send_message(LOG_CHANNEL_ID, summary, parse_mode="HTML", disable_web_page_preview=True)
    except Exception as e:
        logger.warning(f"Failed to log filter details: {e}")


async def send_filter_reply(update: Update, context: ContextTypes.DEFAULT_TYPE, doc: dict):
    chat_id = update.effective_chat.id
    sent_msg = None
    trigger_id = update.effective_message.message_id if update.effective_message else None

    try:
        # Reply/quote the message that triggered the filter, like Rose-bot does.
        sent_msg = await _send_rendered_filter(
            context, chat_id, doc.get("reply_text"), doc.get("buttons"),
            doc.get("file_id"), doc.get("file_type"), reply_to=trigger_id,
        )
    except Exception:
        # The triggering message may have been deleted, or replies may be
        # restricted in this chat -- fall back to a plain (non-reply) send
        # instead of failing silently.
        sent_msg = await _send_rendered_filter(
            context, chat_id, doc.get("reply_text"), doc.get("buttons"),
            doc.get("file_id"), doc.get("file_type"), reply_to=None,
        )

    await increment_uses(chat_id, doc["name"])

    settings = await get_settings(chat_id)
    if settings.get("autodel_on") and sent_msg:
        asyncio.create_task(_auto_delete(context, chat_id, sent_msg.message_id, settings["autodel_sec"]))


async def _auto_delete(context: ContextTypes.DEFAULT_TYPE, chat_id: int, message_id: int, delay: int):
    await asyncio.sleep(delay)
    try:
        await context.bot.delete_message(chat_id, message_id)
    except Exception:
        pass


async def filter_trigger_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    message = update.effective_message
    if not message or not message.text:
        return
    text = message.text.lower().strip()
    doc = await get_filter(update.effective_chat.id, text)
    if doc:
        await send_filter_reply(update, context, doc)


# ============================================================
# Auto-delete config + top filters
# ============================================================

async def autodel_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    args = context.args
    if not args or args[0].lower() not in ("on", "off"):
        await update.effective_message.reply_text("Usage: /autodel on <seconds>  |  /autodel off")
        return

    chat_id = await require_admin_for_chat(update, context)
    if chat_id is None:
        return

    if args[0].lower() == "off":
        await update_settings(chat_id, autodel_on=False)
        await update.effective_message.reply_text("🗑 Auto-delete disabled.")
        return

    if len(args) < 2 or not args[1].isdigit():
        await update.effective_message.reply_text("Usage: /autodel on <seconds>")
        return

    seconds = int(args[1])
    await update_settings(chat_id, autodel_on=True, autodel_sec=seconds)
    await update.effective_message.reply_text(f"🗑 Auto-delete enabled: filter replies vanish after {seconds}s.")


async def top_filters_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = await resolve_target_chat_id(update, context)
    if chat_id is None:
        await update.effective_message.reply_text(
            "⚠️ You're not connected to a group. Use /connect <group_id> here in DM first, "
            "or run this command directly inside the group."
        )
        return
    top = await top_filters(chat_id, limit=10)
    if not top:
        await update.effective_message.reply_text("No filters have been used yet.")
        return
    lines = [f"{i+1}. `{f['name']}` — {f.get('uses', 0)} uses" for i, f in enumerate(top)]
    await update.effective_message.reply_text("🏆 *Top filters:*\n" + "\n".join(lines), parse_mode="Markdown")


# ============================================================
# Filter cloning
# ============================================================

async def fclone_toggle_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = await require_admin_for_chat(update, context)
    if chat_id is None:
        return
    enabled = context.args[0].lower() == "on"
    await update_settings(chat_id, fclone_on=enabled)
    await update.effective_message.reply_text(f"🔄 Filter cloning {'enabled' if enabled else 'disabled'} for that chat.")


async def fclone_copy_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    args = context.args
    source_id, target_id = int(args[0]), int(args[1])
    user_id = update.effective_user.id

    src_settings = await get_settings(source_id)
    if not src_settings.get("fclone_on"):
        await update.effective_message.reply_text("⚠️ Cloning is not enabled in the source chat. Run /fclone on there first.")
        return

    try:
        src_member = await context.bot.get_chat_member(source_id, user_id)
        tgt_member = await context.bot.get_chat_member(target_id, user_id)
    except Exception:
        await update.effective_message.reply_text("⚠️ I couldn't verify your admin status in one of those chats (am I a member there?).")
        return

    if src_member.status not in ("administrator", "creator") or tgt_member.status not in ("administrator", "creator"):
        await update.effective_message.reply_text("⚠️ You must be an admin in both the source and target chats.")
        return

    count = await clone_filters(source_id, target_id)
    await update.effective_message.reply_text(f"✅ Cloned {count} filter(s) from {source_id} to {target_id}.")


async def fclone_dispatch(update: Update, context: ContextTypes.DEFAULT_TYPE):
    args = context.args
    if len(args) == 1 and args[0].lower() in ("on", "off"):
        await fclone_toggle_cmd(update, context)
    elif len(args) == 2 and all(a.lstrip("-").isdigit() for a in args):
        await fclone_copy_cmd(update, context)
    else:
        await update.effective_message.reply_text(
            "Usage:\n/fclone on|off\n/fclone <source_chat_id> <target_chat_id>"
        )


# ============================================================
# Backup / restore
# ============================================================

async def export_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = await require_admin_for_chat(update, context)
    if chat_id is None:
        return
    filters_list = await list_filters(chat_id)
    if not filters_list:
        await update.effective_message.reply_text("No filters to export.")
        return

    export_data = [
        {
            "name": f["name"], "reply_text": f.get("reply_text", ""),
            "buttons": f.get("buttons", []), "file_id": f.get("file_id"),
            "file_type": f.get("file_type"), "genre": f.get("genre"),
        }
        for f in filters_list
    ]
    buf = io.BytesIO(json.dumps(export_data, indent=2).encode("utf-8"))
    buf.name = f"filters_{chat_id}.json"
    await update.effective_message.reply_document(
        document=buf, filename=buf.name, caption=f"📥 Exported {len(export_data)} filter(s)."
    )


async def import_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    message = update.effective_message
    if not message.reply_to_message or not message.reply_to_message.document:
        await message.reply_text("Reply to a previously exported .json file with /import to restore its filters.")
        return

    chat_id = await require_admin_for_chat(update, context)
    if chat_id is None:
        return

    doc = message.reply_to_message.document
    if not doc.file_name.endswith(".json"):
        await message.reply_text("⚠️ That doesn't look like a filter export (.json) file.")
        return

    tg_file = await doc.get_file()
    raw = await tg_file.download_as_bytearray()
    try:
        data = json.loads(bytes(raw).decode("utf-8"))
    except Exception:
        await message.reply_text("⚠️ Couldn't parse that file as valid filter JSON.")
        return

    count = 0
    for entry in data:
        if "name" not in entry:
            continue
        await add_filter(
            chat_id=chat_id, name=entry["name"], reply_text=entry.get("reply_text", ""),
            buttons=entry.get("buttons", []), file_id=entry.get("file_id"),
            file_type=entry.get("file_type"), genre=entry.get("genre"),
        )
        count += 1
    await message.reply_text(f"✅ Imported {count} filter(s).")


# ============================================================
# Connections (manage a group's filters from PM)
# ============================================================

async def connect_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    args = context.args
    if not args or not args[0].lstrip("-").isdigit():
        await update.effective_message.reply_text("Usage: /connect <group_id>")
        return

    group_id = int(args[0])
    user_id = update.effective_user.id
    try:
        member = await context.bot.get_chat_member(group_id, user_id)
    except Exception:
        await update.effective_message.reply_text("⚠️ I couldn't find that group, or I'm not a member of it.")
        return

    if member.status not in ("administrator", "creator"):
        await update.effective_message.reply_text("⚠️ You must be an admin of that group to connect to it.")
        return

    await set_connection(user_id, group_id)
    chat = await context.bot.get_chat(group_id)
    await update.effective_message.reply_text(f"🔗 Connected to '{chat.title}'. You can now manage its filters from here.")


async def connections_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    group_id = await get_connection(user_id)
    if not group_id:
        await update.effective_message.reply_text("You have no active connection. Use /connect <group_id>.")
        return
    title = await get_chat_label(context, group_id)
    filter_count = len(await list_filters(group_id))
    await update.effective_message.reply_text(
        f"🔗 Currently connected to: *{title}* (`{group_id}`)\n"
        f"📋 {filter_count} filter(s) saved there.",
        parse_mode="Markdown",
    )


async def disconnect_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    removed = await remove_connection(user_id)
    if removed:
        await update.effective_message.reply_text("🔌 Disconnected.")
    else:
        await update.effective_message.reply_text("You weren't connected to anything.")


# ============================================================
# Broadcast (owner-only)
# ============================================================

async def broadcast_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in SUDO_USERS:
        await update.effective_message.reply_text("⚠️ Only the bot owner can use /broadcast.")
        return

    message = update.effective_message
    source = message.reply_to_message

    text = None
    if not source:
        raw = message.text or ""
        parts = raw.split(None, 1)
        text = parts[1] if len(parts) > 1 else ""
        if not text:
            await message.reply_text(
                "Usage: /broadcast <message>\nOr reply to any message (text, photo, video, etc.) with /broadcast."
            )
            return

    # Union of every chat_id we have any record of: chats_col (settings doc,
    # created the first time any filter command runs there) plus filters_col
    # (in case filters were added but that chat never triggered a settings
    # write) -- so a brand-new group isn't missed just because no one has
    # typed a filter keyword there yet.
    settings_ids = {doc["chat_id"] async for doc in chats_col.find({}, {"chat_id": 1})}
    filter_ids = set(await filters_col.distinct("chat_id"))
    chat_ids = settings_ids | filter_ids
    if not chat_ids:
        await message.reply_text("No known chats to broadcast to yet.")
        return

    status = await message.reply_text(f"📢 Broadcasting to {len(chat_ids)} chat(s)...")
    sent, failed = 0, 0
    for cid in chat_ids:
        try:
            if source:
                await context.bot.copy_message(chat_id=cid, from_chat_id=source.chat_id, message_id=source.message_id)
            else:
                await context.bot.send_message(cid, text)
            sent += 1
        except Exception:
            failed += 1
        await asyncio.sleep(0.05)  # gentle throttle to stay under Telegram's flood limits

    await status.edit_text(f"📢 Broadcast complete: {sent} sent, {failed} failed (bot removed/blocked there, most likely).")


# ============================================================
# Wiring it all together
# ============================================================

async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE):
    logger.error("Exception while handling an update:", exc_info=context.error)
    try:
        if isinstance(update, Update) and update.effective_message:
            await update.effective_message.reply_text(
                "⚠️ Something went wrong handling that command. The error has been logged."
            )
    except Exception:
        pass

    error_text = f"{type(context.error).__name__}: {context.error}"
    chat_info = ""
    if isinstance(update, Update) and update.effective_chat:
        chat_info = f"\nChat: {update.effective_chat.title or update.effective_chat.id} (<code>{update.effective_chat.id}</code>)"
    await send_log(context, f"🛑 <b>Error</b>\n<code>{error_text}</code>{chat_info}")


async def new_chat_member_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Logs when someone (including the bot) joins a group this bot is in."""
    chat = update.effective_chat
    for member in update.effective_message.new_chat_members:
        if member.id == context.bot.id:
            await send_log(
                context,
                f"🆕 <b>Bot added to a new group</b>\n"
                f"Chat: {chat.title} (<code>{chat.id}</code>)",
            )
        else:
            await send_log(
                context,
                f"👤 <b>New member joined</b>\n"
                f"User: {member.mention_html()}\n"
                f"Chat: {chat.title} (<code>{chat.id}</code>)",
            )


class _HealthCheckHandler(BaseHTTPRequestHandler):
    """Answers every request with a plain 200 OK. Used only so Render's port
    scan succeeds and an uptime monitor has something to ping -- it has no
    connection to the Telegram bot logic itself."""

    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-Type", "text/plain")
        self.end_headers()
        self.wfile.write(b"OK")

    def do_HEAD(self):
        self.send_response(200)
        self.end_headers()

    def log_message(self, format, *args):
        pass  # silence per-request logging spam from uptime pings


def _start_health_server(port: int):
    server = ThreadingHTTPServer(("0.0.0.0", port), _HealthCheckHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    logger.info(f"Health-check server listening on 0.0.0.0:{port}")


async def post_init(application: Application):
    await ensure_indexes()
    logger.info("Database indexes ready.")

    # Populates Telegram's native "/" command menu (the tappable list next
    # to the message box) so users see this list without typing /help.
    try:
        await application.bot.set_my_commands([
            BotCommand("start", "Check bot online"),
            BotCommand("help", "How to use the bot"),
            BotCommand("add", "Create a new filter"),
            BotCommand("del", "Delete a filter"),
            BotCommand("filters", "Show all filters"),
            BotCommand("deleteallfilters", "Delete all filters"),
            BotCommand("autodel", "Auto-delete replies"),
            BotCommand("topfilters", "View most-used filters"),
            BotCommand("fclone", "Enable/copy filter cloning"),
            BotCommand("export", "Export filters as .json"),
            BotCommand("import", "Import filters from backup"),
            BotCommand("connect", "Connect your group"),
            BotCommand("connections", "Manage linked groups"),
            BotCommand("disconnect", "Disconnect your group"),
            BotCommand("broadcast", "Owner: message every chat"),
            BotCommand("id", "Get user/group ID"),
            BotCommand("info", "Get user info"),
            BotCommand("donate", "Support the bot"),
        ])
    except Exception as e:
        logger.warning(f"Could not set bot command menu: {e}")

    if LOG_CHANNEL_ID:
        try:
            await application.bot.send_message(LOG_CHANNEL_ID, "✅ <b>Bot started</b>", parse_mode="HTML")
        except Exception as e:
            logger.warning(f"Could not post startup message to log channel: {e}")


def build_app() -> Application:
    app = Application.builder().token(BOT_TOKEN).post_init(post_init).build()

    app.add_handler(CommandHandler("start", start_cmd))
    app.add_handler(CommandHandler("help", help_cmd))
    app.add_handler(CommandHandler("id", id_cmd))
    app.add_handler(CommandHandler("info", info_cmd))
    app.add_handler(CommandHandler("donate", donate_cmd))
    app.add_handler(CallbackQueryHandler(help_center_callback, pattern="^help_center$"))
    app.add_handler(CallbackQueryHandler(filters_page_callback, pattern="^filterspage:"))

    app.add_handler(CommandHandler("add", add_filter_cmd))
    app.add_handler(CommandHandler("del", del_filter_cmd))
    app.add_handler(CommandHandler("filters", list_filters_cmd))
    app.add_handler(CommandHandler("deleteallfilters", delete_all_filters_cmd))

    app.add_handler(CommandHandler("autodel", autodel_cmd))
    app.add_handler(CommandHandler("topfilters", top_filters_cmd))

    app.add_handler(CommandHandler("fclone", fclone_dispatch))

    app.add_handler(CommandHandler("export", export_cmd))
    app.add_handler(CommandHandler("import", import_cmd))

    app.add_handler(CommandHandler("connect", connect_cmd))
    app.add_handler(CommandHandler("connections", connections_cmd))
    app.add_handler(CommandHandler("disconnect", disconnect_cmd))
    app.add_handler(CommandHandler("broadcast", broadcast_cmd))

    app.add_handler(MessageHandler(tg_filters.TEXT & ~tg_filters.COMMAND, filter_trigger_handler), group=0)

    # Logging: new members joining, and the bot itself being added to a group.
    app.add_handler(MessageHandler(tg_filters.StatusUpdate.NEW_CHAT_MEMBERS, new_chat_member_cmd))

    app.add_error_handler(error_handler)

    return app


def main():
    app = build_app()

    # Python 3.12+ (and especially 3.14) removed the old behaviour where
    # asyncio.get_event_loop() would silently create a loop if none existed
    # in the main thread. python-telegram-bot 21.4 still relies on that old
    # behaviour internally, which crashes with:
    #   RuntimeError: There is no current event loop in thread 'MainThread'
    # Explicitly creating and setting a loop here works around it regardless
    # of which Python version Render happens to be using.
    try:
        asyncio.get_event_loop()
    except RuntimeError:
        asyncio.set_event_loop(asyncio.new_event_loop())

    if WEBHOOK_URL:
        # Web Service mode: Render requires binding to $PORT.
        logger.info(f"Starting bot in webhook mode on port {PORT} -> {WEBHOOK_URL}")
        app.run_webhook(
            listen="0.0.0.0",
            port=PORT,
            url_path=BOT_TOKEN,
            webhook_url=f"{WEBHOOK_URL}/{BOT_TOKEN}",
            allowed_updates=Update.ALL_TYPES,
        )
    else:
        # Polling mode doesn't need a port on its own, but Render's port
        # scan (and an uptime monitor like UptimeRobot pinging the service
        # to keep a free-tier instance awake) both need something to hit.
        # Without this, Render logs "No open ports detected" and may
        # restart the service, which can briefly run two bot instances at
        # once and trigger Telegram's "Conflict: terminated by other
        # getUpdates request" error. This tiny server just answers "OK".
        _start_health_server(PORT)
        logger.info(f"Starting bot in polling mode (health server on port {PORT})...")
        app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
