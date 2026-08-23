"""
Advanced Filter Bot - single-file edition
==========================================
A Rose-bot style filter bot for Telegram (python-telegram-bot v21, async)
backed by MongoDB (via motor). Works across unlimited groups at once --
every filter, setting, and connection is scoped by chat_id.

Setup:
    pip install python-telegram-bot==21.4 motor==3.5.1 pymongo==4.8.0

    export BOT_TOKEN="123456:ABC-your-token"
    export MONGO_URI="mongodb+srv://user:pass@cluster.mongodb.net"
    export DB_NAME="filterbot"
    export SUDO_USERS="123456789"   # optional, comma-separated Telegram user IDs

    python filterbot.py

Commands:
    Core:        /add, /del, /filters, /deleteallfilters
    Auto mgmt:   /autodel on <sec> | off, /topfilters
    Cloning:     /fclone on|off, /fclone <source_id> <target_id>
    Smart:       /suggestmode on|off, genre keywords, random-pick keywords, /syncgenre
    Buttons:     [Text](buttonurl:https://link) inside any /add reply text
    Backup:      /export, /import (reply to the exported .json)
    Connections: /connect <group_id>, /connections, /disconnect

Add the bot to a group and promote it to admin (needed for /autodel message
deletion and for reading the member list to check who's an admin).
"""

import os
import re
import json
import io
import random
import asyncio
import logging
import shlex

from motor.motor_asyncio import AsyncIOMotorClient
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application, CommandHandler, MessageHandler, ChatMemberHandler, CallbackQueryHandler,
    filters as tg_filters, ContextTypes,
)
from telegram import BotCommand

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# ============================================================
# Config  --  EDIT THESE VALUES DIRECTLY
# ============================================================

BOT_TOKEN = "123456:ABC-your-bot-token-here"          # from @BotFather
MONGO_URI = "mongodb://localhost:27017"                # your MongoDB connection string
DB_NAME = "filterbot"                                  # database name (any name you like)
SUDO_USERS = [123456789]                               # your Telegram user id(s), as integers

# (env vars still override the above if set, so this also works with hosting
#  platforms that inject BOT_TOKEN / MONGO_URI / DB_NAME / SUDO_USERS themselves)
BOT_TOKEN = os.environ.get("BOT_TOKEN", BOT_TOKEN)
MONGO_URI = os.environ.get("MONGO_URI", MONGO_URI)
DB_NAME = os.environ.get("DB_NAME", DB_NAME)
if os.environ.get("SUDO_USERS"):
    SUDO_USERS = [int(x) for x in os.environ["SUDO_USERS"].split(",") if x.strip().isdigit()]
SUDO_USERS = set(SUDO_USERS)

# Log channel: numeric chat id of a channel/group where the bot is admin.
# All filter add/delete events, errors, new-group-added, and new-member
# notifications get posted here. Leave as 0 to disable logging entirely.
LOG_CHANNEL_ID = 0                                     # e.g. -1001234567890
LOG_CHANNEL_ID = int(os.environ.get("LOG_CHANNEL_ID", LOG_CHANNEL_ID) or 0)

# ---- Branding / /start command customization -- EDIT THESE DIRECTLY ----
START_IMAGE_URL = "https://telegra.ph/file/example-banner.jpg"   # any public image URL
BOT_NAME = "Advanced Filter Bot"
OWNER_URL = "https://t.me/your_username"          # shown as the "Owner" button
UPDATES_URL = "https://t.me/your_updates_channel"  # shown as the "Updates" button
SUPPORT_TEXT = (
    "💝 If you'd like to support this bot's hosting costs, reach out to the owner!"
)
BOT_USERNAME_FOR_ADD = "your_bot_username"  # without @, used for the "Add me to your group" button

GENRES = {"action", "romance", "comedy", "horror", "drama", "thriller", "sci-fi", "animation"}
RANDOM_KEYWORDS = {"suggest", "best", "random", "recommend", "surprise me"}
FILE_TYPES = ("photo", "video", "document", "animation", "sticker", "audio", "voice")

DEFAULT_SETTINGS = {
    "autodel_on": False,
    "autodel_sec": 60,
    "fclone_on": False,
    "suggestmode_on": False,
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


async def filters_by_genre(chat_id: int, genre: str):
    cursor = filters_col.find({"chat_id": chat_id, "genre": genre.lower().strip()})
    return [doc async for doc in cursor]


async def all_filters_missing_genre(chat_id: int):
    cursor = filters_col.find({"chat_id": chat_id, "genre": None})
    return [doc async for doc in cursor]


async def set_genre(chat_id: int, name: str, genre: str):
    await filters_col.update_one({"chat_id": chat_id, "name": name.lower().strip()}, {"$set": {"genre": genre}})


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
    buttons = []
    for match in BUTTON_REGEX.finditer(text):
        label, url = match.group(1).strip(), match.group(2).strip()
        buttons.append([label, url])
    clean_text = BUTTON_REGEX.sub("", text).strip()
    return clean_text, buttons


def build_markup(buttons):
    if not buttons:
        return None
    keyboard = [[InlineKeyboardButton(label, url=url)] for label, url in buttons]
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
    add_to_group_url = f"https://t.me/{BOT_USERNAME_FOR_ADD}?startgroup=true"
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📖 Help Center", callback_data="help_center")],
        [InlineKeyboardButton("👑 Owner", url=OWNER_URL), InlineKeyboardButton("📢 Updates", url=UPDATES_URL)],
        [InlineKeyboardButton("➕ Add me to your group", url=add_to_group_url)],
    ])


async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    caption = (
        f"👋 <b>Hi there, {user.mention_html()}!</b>\n\n"
        f"🔍 <b>{BOT_NAME}</b>\n"
        "Add me to a group and make me admin. Use /add to create filters, "
        "and I'll auto-reply whenever someone types the filter name.\n\n"
        "Send /help to see everything I can do."
    )
    try:
        await update.effective_message.reply_photo(
            photo=START_IMAGE_URL, caption=caption, parse_mode="HTML", reply_markup=_home_keyboard()
        )
    except Exception:
        # Falls back to text-only if the image URL is unreachable/misconfigured.
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
    "🎲 <b>Smart</b>\n"
    "• /suggestmode <code>on|off</code>\n"
    "• Type a genre (action, romance, comedy...) for matches\n"
    "• Type suggest/best/random for a random pick\n"
    "• /syncgenre — tag old filters\n\n"
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
    "• /donate — support the bot"
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
    if not await require_admin(update, context):
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
        if replied.reply_markup and replied.reply_markup.inline_keyboard:
            for row in replied.reply_markup.inline_keyboard:
                for btn in row:
                    if getattr(btn, "url", None):
                        original_buttons.append([btn.text, btn.url])

    if not reply_text and not file_id:
        await message.reply_text("⚠️ Provide reply text, or reply to a message/media to save.")
        return

    clean_text, parsed_buttons = parse_buttons(reply_text) if reply_text else ("", [])
    all_buttons = parsed_buttons + [b for b in original_buttons if b not in parsed_buttons]

    await add_filter(
        chat_id=update.effective_chat.id, name=name, reply_text=clean_text,
        buttons=all_buttons, file_id=file_id, file_type=file_type,
    )
    await message.reply_text(f"✅ Filter '{name}' saved.")
    await send_log(
        context,
        f"➕ Filter added: <code>{name}</code>\n"
        f"Chat: {update.effective_chat.title or update.effective_chat.id} (<code>{update.effective_chat.id}</code>)\n"
        f"By: {update.effective_user.mention_html()}",
    )


async def del_filter_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await require_admin(update, context):
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
    ok = await delete_filter(update.effective_chat.id, name)
    if ok:
        await update.effective_message.reply_text(f"🗑 Filter '{name}' deleted.")
        await send_log(
            context,
            f"➖ Filter deleted: <code>{name}</code>\n"
            f"Chat: {update.effective_chat.title or update.effective_chat.id} (<code>{update.effective_chat.id}</code>)\n"
            f"By: {update.effective_user.mention_html()}",
        )
    else:
        await update.effective_message.reply_text(f"No filter named '{name}' found.")


async def list_filters_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    filters_list = await list_filters(update.effective_chat.id)
    if not filters_list:
        await update.effective_message.reply_text("No filters saved in this chat yet.")
        return
    names = ", ".join(f"`{f['name']}`" for f in filters_list)
    await update.effective_message.reply_text(
        f"📋 *Filters in this chat ({len(filters_list)}):*\n{names}", parse_mode="Markdown"
    )


async def delete_all_filters_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await require_admin(update, context):
        return
    count = await delete_all_filters(update.effective_chat.id)
    await update.effective_message.reply_text(f"🗑 Deleted {count} filter(s) from this chat.")


async def send_filter_reply(update: Update, context: ContextTypes.DEFAULT_TYPE, doc: dict):
    chat_id = update.effective_chat.id
    markup = build_markup(doc.get("buttons"))
    sent_msg = None

    if doc.get("file_id"):
        if doc["file_type"] == "sticker":
            sent_msg = await context.bot.send_sticker(chat_id, doc["file_id"])
        else:
            send_map = {
                "photo": context.bot.send_photo, "video": context.bot.send_video,
                "document": context.bot.send_document, "animation": context.bot.send_animation,
                "audio": context.bot.send_audio, "voice": context.bot.send_voice,
            }
            sender = send_map[doc["file_type"]]
            sent_msg = await sender(
                chat_id=chat_id, **{doc["file_type"]: doc["file_id"]},
                caption=doc.get("reply_text") or None, reply_markup=markup,
            )
    else:
        sent_msg = await context.bot.send_message(
            chat_id=chat_id, text=doc.get("reply_text") or "‎", reply_markup=markup
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
    if not await require_admin(update, context):
        return
    args = context.args
    if not args or args[0].lower() not in ("on", "off"):
        await update.effective_message.reply_text("Usage: /autodel on <seconds>  |  /autodel off")
        return

    chat_id = update.effective_chat.id
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
    chat_id = update.effective_chat.id
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
    if not await require_admin(update, context):
        return
    chat_id = update.effective_chat.id
    enabled = context.args[0].lower() == "on"
    await update_settings(chat_id, fclone_on=enabled)
    await update.effective_message.reply_text(f"🔄 Filter cloning {'enabled' if enabled else 'disabled'} for this chat.")


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
# Smart features: suggest mode, genre matching, syncgenre
# ============================================================

async def suggestmode_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await require_admin(update, context):
        return
    args = context.args
    if not args or args[0].lower() not in ("on", "off"):
        await update.effective_message.reply_text("Usage: /suggestmode on|off")
        return
    enabled = args[0].lower() == "on"
    await update_settings(update.effective_chat.id, suggestmode_on=enabled)
    await update.effective_message.reply_text(f"🎲 Suggest mode {'enabled' if enabled else 'disabled'}.")


async def syncgenre_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await require_admin(update, context):
        return
    chat_id = update.effective_chat.id
    missing = await all_filters_missing_genre(chat_id)
    updated = 0
    for f in missing:
        haystack = f"{f['name']} {f.get('reply_text', '')}".lower()
        for genre in GENRES:
            if genre in haystack:
                await set_genre(chat_id, f["name"], genre)
                updated += 1
                break
    await update.effective_message.reply_text(f"🔄 Sync complete. Tagged {updated} of {len(missing)} un-tagged filter(s).")


async def smart_suggest_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    message = update.effective_message
    if not message or not message.text:
        return

    chat_id = update.effective_chat.id
    settings = await get_settings(chat_id)
    if not settings.get("suggestmode_on"):
        return

    text = message.text.lower().strip()

    matched_genre = next((g for g in GENRES if g in text.split()), None)
    if matched_genre:
        candidates = await filters_by_genre(chat_id, matched_genre)
        if candidates:
            await send_filter_reply(update, context, random.choice(candidates))
        return

    if any(word in text for word in RANDOM_KEYWORDS):
        all_f = await list_filters(chat_id)
        if all_f:
            await send_filter_reply(update, context, random.choice(all_f))


# ============================================================
# Backup / restore
# ============================================================

async def export_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await require_admin(update, context):
        return
    chat_id = update.effective_chat.id
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
    if not await require_admin(update, context):
        return
    message = update.effective_message
    if not message.reply_to_message or not message.reply_to_message.document:
        await message.reply_text("Reply to a previously exported .json file with /import to restore its filters.")
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

    chat_id = update.effective_chat.id
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
    try:
        chat = await context.bot.get_chat(group_id)
        title = chat.title
    except Exception:
        title = str(group_id)
    await update.effective_message.reply_text(f"🔗 Currently connected to: {title} (`{group_id}`)", parse_mode="Markdown")


async def disconnect_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    removed = await remove_connection(user_id)
    if removed:
        await update.effective_message.reply_text("🔌 Disconnected.")
    else:
        await update.effective_message.reply_text("You weren't connected to anything.")


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
            BotCommand("suggestmode", "Toggle random suggestions"),
            BotCommand("syncgenre", "Tag old filters with a genre"),
            BotCommand("export", "Export filters as .json"),
            BotCommand("import", "Import filters from backup"),
            BotCommand("connect", "Connect your group"),
            BotCommand("connections", "Manage linked groups"),
            BotCommand("disconnect", "Disconnect your group"),
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
    if not BOT_TOKEN:
        raise RuntimeError("BOT_TOKEN environment variable is not set.")

    app = Application.builder().token(BOT_TOKEN).post_init(post_init).build()

    app.add_handler(CommandHandler("start", start_cmd))
    app.add_handler(CommandHandler("help", help_cmd))
    app.add_handler(CommandHandler("id", id_cmd))
    app.add_handler(CommandHandler("info", info_cmd))
    app.add_handler(CommandHandler("donate", donate_cmd))
    app.add_handler(CallbackQueryHandler(help_center_callback, pattern="^help_center$"))

    app.add_handler(CommandHandler("add", add_filter_cmd))
    app.add_handler(CommandHandler("del", del_filter_cmd))
    app.add_handler(CommandHandler("filters", list_filters_cmd))
    app.add_handler(CommandHandler("deleteallfilters", delete_all_filters_cmd))

    app.add_handler(CommandHandler("autodel", autodel_cmd))
    app.add_handler(CommandHandler("topfilters", top_filters_cmd))

    app.add_handler(CommandHandler("fclone", fclone_dispatch))

    app.add_handler(CommandHandler("suggestmode", suggestmode_cmd))
    app.add_handler(CommandHandler("syncgenre", syncgenre_cmd))

    app.add_handler(CommandHandler("export", export_cmd))
    app.add_handler(CommandHandler("import", import_cmd))

    app.add_handler(CommandHandler("connect", connect_cmd))
    app.add_handler(CommandHandler("connections", connections_cmd))
    app.add_handler(CommandHandler("disconnect", disconnect_cmd))

    app.add_handler(MessageHandler(tg_filters.TEXT & ~tg_filters.COMMAND, filter_trigger_handler), group=0)
    app.add_handler(MessageHandler(tg_filters.TEXT & ~tg_filters.COMMAND, smart_suggest_handler), group=1)

    # Logging: new members joining, and the bot itself being added to a group.
    app.add_handler(MessageHandler(tg_filters.StatusUpdate.NEW_CHAT_MEMBERS, new_chat_member_cmd))

    app.add_error_handler(error_handler)

    return app


def main():
    app = build_app()
    logger.info("Starting bot...")
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
