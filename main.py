# ============================================================
# PRO MEDIA DOWNLOADER BOT
# ============================================================
# Professional Telegram Media Downloader
#
# Features:
# - Telegram post download
# - Story download
# - Batch post download
# - Batch story download
# - Media groups
# - Auto forwarding
# - Progress tracking
# - FloodWait handling
# - Retry system
# - Global task cancellation
# - Per-user batch protection
# - Disk cleanup
# - System statistics
# - Professional UI
#
# Existing helper modules supported:
# helpers.utils
# helpers.forward
# helpers.files
# helpers.msg
# config
# logger
# ============================================================

import os
import shutil
import psutil
import asyncio
from time import time
from typing import Optional

from pyleaves import Leaves

from pyrogram.enums import ParseMode
from pyrogram import Client, filters
from pyrogram.errors import (
    PeerIdInvalid,
    BadRequest,
    FloodWait,
)
from pyrogram.types import (
    Message,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
)

from helpers.utils import (
    processMediaGroup,
    progressArgs,
    send_media,
)

from helpers.forward import (
    check_forward_permission,
    resolve_forward_chat_id,
)

from helpers.files import (
    get_download_path,
    fileSizeLimit,
    get_readable_file_size,
    get_readable_time,
    cleanup_download,
    cleanup_downloads_root,
)

from helpers.msg import (
    getChatMsgID,
    getStoryChatMsgID,
    is_story_link,
    get_file_name,
    get_story_file_name,
    get_raw_text,
)

from config import PyroConf
from logger import LOGGER


# ============================================================
# CLIENTS
# ============================================================

bot = Client(
    "media_bot",
    api_id=PyroConf.API_ID,
    api_hash=PyroConf.API_HASH,
    bot_token=PyroConf.BOT_TOKEN,
    workers=100,
    parse_mode=ParseMode.MARKDOWN,
    max_concurrent_transmissions=1,
    sleep_threshold=30,
)

user = Client(
    "user_session",
    workers=100,
    session_string=PyroConf.SESSION_STRING,
    max_concurrent_transmissions=1,
    sleep_threshold=30,
)


# ============================================================
# GLOBAL STATE
# ============================================================

RUNNING_TASKS = set()

download_semaphore: Optional[asyncio.Semaphore] = None
forward_chat_id = None

# One active batch per Telegram user.
ACTIVE_BATCHES = set()

# Lock for state changes.
STATE_LOCK = asyncio.Lock()


# ============================================================
# CONFIG HELPERS
# ============================================================

def cfg(name, default):
    """
    Safely read optional PyroConf settings.
    """
    return getattr(PyroConf, name, default)


MAX_BDL_RANGE = cfg("MAX_BDL_RANGE", 2000)
BATCH_SIZE = max(1, min(cfg("BATCH_SIZE", 5), 20))
BDL_RETRIES = max(1, cfg("BDL_RETRIES", 3))
BDL_PROGRESS_EVERY = max(1, cfg("BDL_PROGRESS_EVERY", 10))
FLOOD_WAIT_DELAY = max(0, cfg("FLOOD_WAIT_DELAY", 2))
MAX_CONCURRENT_DOWNLOADS = max(
    1,
    cfg("MAX_CONCURRENT_DOWNLOADS", 1),
)


# ============================================================
# TASK MANAGEMENT
# ============================================================

def track_task(coro):
    """
    Create and track a background task.
    """
    task = asyncio.create_task(coro)

    RUNNING_TASKS.add(task)

    def remove_task(done_task):
        RUNNING_TASKS.discard(done_task)

    task.add_done_callback(remove_task)

    return task


def get_running_tasks():
    return [
        task
        for task in list(RUNNING_TASKS)
        if not task.done()
    ]


async def safe_delete(message):
    try:
        await message.delete()
    except Exception:
        pass


async def safe_edit(message, text):
    try:
        await message.edit(text)
        return True
    except Exception:
        return False


# ============================================================
# PROFESSIONAL UI
# ============================================================

def main_keyboard():
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    "📥 Download",
                    callback_data="menu_download",
                ),
                InlineKeyboardButton(
                    "📦 Batch",
                    callback_data="menu_batch",
                ),
            ],
            [
                InlineKeyboardButton(
                    "📖 Stories",
                    callback_data="menu_story",
                ),
                InlineKeyboardButton(
                    "📊 Status",
                    callback_data="menu_status",
                ),
            ],
            [
                InlineKeyboardButton(
                    "❓ Help",
                    callback_data="menu_help",
                ),
            ],
        ]
    )


def help_keyboard():
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    "🏠 Home",
                    callback_data="menu_home",
                ),
            ],
        ]
    )


def progress_bar(current, total, length=12):
    if total <= 0:
        return "░" * length

    ratio = max(0, min(current / total, 1))
    filled = int(length * ratio)

    return "█" * filled + "░" * (length - filled)


# ============================================================
# START
# ============================================================

@bot.on_message(
    filters.command("start") & filters.private
)
async def start(_, message: Message):

    text = (
        "╭━━━━━━━━━━━━━━━━━━━━╮\n"
        "   **⚡ MEDIA DOWNLOADER**\n"
        "╰━━━━━━━━━━━━━━━━━━━━╯\n\n"
        "🚀 **Fast • Reliable • Professional**\n\n"
        "Send me a Telegram post or story link and "
        "I'll download the available media for you.\n\n"
        "✨ **Supported**\n"
        "• Photos\n"
        "• Videos\n"
        "• Documents\n"
        "• Audio\n"
        "• Voice\n"
        "• GIF / Animation\n"
        "• Media Groups\n"
        "• Telegram Stories\n\n"
        "📦 **Batch Support**\n"
        "Download multiple posts or stories using a range.\n\n"
        "🔐 The connected user account must have access "
        "to the requested chat/story.\n\n"
        "👇 **Choose an option below**"
    )

    await message.reply(
        text,
        reply_markup=main_keyboard(),
        disable_web_page_preview=True,
    )


# ============================================================
# HELP
# ============================================================

HELP_TEXT = (
    "╭━━━━━━━━━━━━━━━━━━━━╮\n"
    "      **📚 HELP CENTER**\n"
    "╰━━━━━━━━━━━━━━━━━━━━╯\n\n"

    "**📥 SINGLE DOWNLOAD**\n"
    "`/dl <telegram_post_url>`\n\n"
    "Or simply paste a Telegram post URL.\n\n"

    "**📦 BATCH DOWNLOAD**\n"
    "`/bdl <start_url> <end_url>`\n\n"
    "Example:\n"
    "`/bdl https://t.me/channel/100 https://t.me/channel/150`\n\n"

    "**📖 STORY DOWNLOAD**\n"
    "`/dls <story_url>`\n\n"
    "Example:\n"
    "`/dls https://t.me/username/s/12`\n\n"

    "**📚 BATCH STORY**\n"
    "`/bdls <start_story> <end_story>`\n\n"

    "**🛑 CANCEL**\n"
    "`/killall`\n"
    "Stops all active downloads.\n\n"

    "**🧹 CLEANUP**\n"
    "`/cleanup`\n"
    "Removes temporary downloaded files.\n\n"

    "**📊 STATUS**\n"
    "`/stats`\n"
    "Shows bot/system information.\n\n"

    "**📜 LOGS**\n"
    "`/logs`\n"
    "Downloads the current log file.\n\n"

    "━━━━━━━━━━━━━━━━━━━━\n"
    "💡 **Tip:** For restricted content, the connected "
    "user account must have permission to access it."
)


@bot.on_message(
    filters.command("help") & filters.private
)
async def help_command(_, message: Message):

    await message.reply(
        HELP_TEXT,
        reply_markup=help_keyboard(),
        disable_web_page_preview=True,
    )


# ============================================================
# CALLBACK MENU
# ============================================================

@bot.on_callback_query()
async def callbacks(client, query):

    data = query.data

    try:

        if data == "menu_home":

            await query.message.edit(
                "╭━━━━━━━━━━━━━━━━━━━━╮\n"
                "   **⚡ MEDIA DOWNLOADER**\n"
                "╰━━━━━━━━━━━━━━━━━━━━╯\n\n"
                "Ready to download.\n\n"
                "📥 Send a Telegram post link\n"
                "📖 Send a Telegram story link\n"
                "📦 Use batch commands for ranges.",
                reply_markup=main_keyboard(),
            )

        elif data == "menu_help":

            await query.message.edit(
                HELP_TEXT,
                reply_markup=help_keyboard(),
            )

        elif data == "menu_download":

            await query.message.edit(
                "📥 **Single Download**\n\n"
                "Send a Telegram post link directly or use:\n\n"
                "`/dl https://t.me/channel/123`\n\n"
                "Supported media includes photos, videos, "
                "audio, documents and media groups.",
                reply_markup=help_keyboard(),
            )

        elif data == "menu_batch":

            await query.message.edit(
                "📦 **Batch Download**\n\n"
                "Use:\n"
                "`/bdl start_link end_link`\n\n"
                "Example:\n"
                "`/bdl https://t.me/channel/100 "
                "https://t.me/channel/150`\n\n"
                f"Maximum range: `{MAX_BDL_RANGE}` posts.",
                reply_markup=help_keyboard(),
            )

        elif data == "menu_story":

            await query.message.edit(
                "📖 **Story Downloader**\n\n"
                "Single story:\n"
                "`/dls https://t.me/user/s/12`\n\n"
                "Story range:\n"
                "`/bdls https://t.me/user/s/10 "
                "https://t.me/user/s/20`",
                reply_markup=help_keyboard(),
            )

        elif data == "menu_status":

            uptime = get_readable_time(
                time() - PyroConf.BOT_START_TIME
            )

            running = len(get_running_tasks())

            await query.message.edit(
                "📊 **SYSTEM STATUS**\n\n"
                f"🟢 Bot: `ONLINE`\n"
                f"⏱ Uptime: `{uptime}`\n"
                f"⚙️ Active Tasks: `{running}`\n"
                f"📦 Batch Limit: `{MAX_BDL_RANGE}`\n"
                f"🚀 Concurrency: `{MAX_CONCURRENT_DOWNLOADS}`",
                reply_markup=help_keyboard(),
            )

        await query.answer()

    except Exception as e:
        LOGGER(__name__).error(
            f"Callback error: {e}"
        )

        try:
            await query.answer(
                "Something went wrong.",
                show_alert=True,
            )
        except Exception:
            pass


# ============================================================
# FORWARD TARGET
# ============================================================

async def get_forward_target(client):
    """
    Resolve/check forwarding destination.

    The result is cached globally so every media item doesn't
    repeatedly resolve the destination.
    """
    global forward_chat_id

    if not forward_chat_id:
        return None

    try:
        ok, err_msg = await check_forward_permission(
            client,
            forward_chat_id,
        )

        if not ok:
            LOGGER(__name__).warning(
                f"Forward permission check failed: {err_msg}"
            )
            return None

        return forward_chat_id

    except Exception as e:
        LOGGER(__name__).error(
            f"Forward target check failed: {e}"
        )
        return None


# ============================================================
# FILE CLEANUP
# ============================================================

async def cleanup_file(path):
    if not path:
        return

    try:
        cleanup_download(path)
    except Exception as e:
        LOGGER(__name__).warning(
            f"Cleanup failed for {path}: {e}"
        )


# ============================================================
# SINGLE POST DOWNLOAD
# ============================================================

async def handle_download(
    client: Client,
    message: Message,
    post_url: str,
):
    """
    Download a single Telegram post.

    IMPORTANT:
    Returns True/False instead of silently swallowing errors.
    This makes batch counters accurate.
    """

    global download_semaphore

    if download_semaphore is None:
        download_semaphore = asyncio.Semaphore(
            MAX_CONCURRENT_DOWNLOADS
        )

    async with download_semaphore:

        media_path = None
        progress_message = None

        if "?" in post_url:
            post_url = post_url.split("?", 1)[0]

        try:

            # ------------------------------------------------
            # Parse URL
            # ------------------------------------------------

            chat_id, message_id = getChatMsgID(
                post_url
            )

            chat_message = await user.get_messages(
                chat_id=chat_id,
                message_ids=message_id,
            )

            if not chat_message:
                await message.reply(
                    "❌ **Message not found.**"
                )
                return False

            LOGGER(__name__).info(
                f"Processing post: {post_url}"
            )

            # ------------------------------------------------
            # File size validation
            # ------------------------------------------------

            file_size = None

            if chat_message.document:
                file_size = chat_message.document.file_size

            elif chat_message.video:
                file_size = chat_message.video.file_size

            elif chat_message.audio:
                file_size = chat_message.audio.file_size

            if file_size is not None:

                allowed = await fileSizeLimit(
                    file_size,
                    message,
                    "download",
                    user.me.is_premium,
                )

                if not allowed:
                    return False

            # ------------------------------------------------
            # Text / caption
            # ------------------------------------------------

            raw_caption, raw_caption_entities = get_raw_text(
                chat_message.caption,
                chat_message.caption_entities,
            )

            raw_text, raw_text_entities = get_raw_text(
                chat_message.text,
                chat_message.entities,
            )

            # ------------------------------------------------
            # Media Group
            # ------------------------------------------------

            if chat_message.media_group_id:

                target = await get_forward_target(client)

                result = await processMediaGroup(
                    chat_message,
                    client,
                    message,
                    forward_chat_id=target,
                )

                if not result:

                    await message.reply(
                        "❌ **Could not extract media "
                        "from this media group.**"
                    )

                    return False

                return True

            # ------------------------------------------------
            # Downloadable media
            # ------------------------------------------------

            has_downloadable_media = any(
                [
                    chat_message.photo,
                    chat_message.video,
                    chat_message.audio,
                    chat_message.document,
                    chat_message.voice,
                    chat_message.video_note,
                    chat_message.animation,
                    chat_message.sticker,
                ]
            )

            if has_downloadable_media:

                start_time = time()

                progress_message = await message.reply(
                    "╭━━━━━━━━━━━━━━━━━━━━╮\n"
                    "      **📥 DOWNLOADING**\n"
                    "╰━━━━━━━━━━━━━━━━━━━━╯\n\n"
                    "⏳ Preparing file..."
                )

                filename = get_file_name(
                    message_id,
                    chat_message,
                )

                download_path = get_download_path(
                    message.id,
                    filename,
                )

                # --------------------------------------------
                # Actual Telegram download
                # --------------------------------------------

                for attempt in range(2):

                    try:

                        media_path = await chat_message.download(
                            file_name=download_path,
                            progress=Leaves.progress_for_pyrogram,
                            progress_args=progressArgs(
                                "📥 Downloading",
                                progress_message,
                                start_time,
                            ),
                        )

                        break

                    except FloodWait as e:

                        wait_time = int(
                            getattr(e, "value", 0) or 0
                        )

                        LOGGER(__name__).warning(
                            f"Download FloodWait: "
                            f"{wait_time}s"
                        )

                        if attempt == 0 and wait_time > 0:

                            await asyncio.sleep(
                                wait_time + 1
                            )

                            continue

                        raise

                # --------------------------------------------
                # Validate downloaded file
                # --------------------------------------------

                if (
                    not media_path
                    or not os.path.exists(media_path)
                ):

                    await safe_edit(
                        progress_message,
                        "❌ **Download failed.**\n\n"
                        "File was not saved correctly.",
                    )

                    return False

                downloaded_size = os.path.getsize(
                    media_path
                )

                if downloaded_size <= 0:

                    await safe_edit(
                        progress_message,
                        "❌ **Download failed.**\n\n"
                        "The downloaded file is empty.",
                    )

                    await cleanup_file(media_path)

                    return False

                LOGGER(__name__).info(
                    f"Downloaded: {media_path} "
                    f"({downloaded_size} bytes)"
                )

                # --------------------------------------------
                # Detect media type
                # --------------------------------------------

                if chat_message.photo:
                    media_type = "photo"

                elif chat_message.video:
                    media_type = "video"

                elif chat_message.audio:
                    media_type = "audio"

                elif chat_message.document:
                    media_type = "document"

                elif chat_message.voice:
                    media_type = "audio"

                elif chat_message.video_note:
                    media_type = "video"

                elif chat_message.animation:
                    media_type = "video"

                else:
                    media_type = "document"

                # --------------------------------------------
                # Forward target
                # --------------------------------------------

                target = await get_forward_target(client)

                # --------------------------------------------
                # Send result
                # --------------------------------------------

                await send_media(
                    client,
                    message,
                    media_path,
                    media_type,
                    raw_caption,
                    raw_caption_entities,
                    progress_message,
                    start_time,
                    forward_chat_id=target,
                )

                return True

            # ------------------------------------------------
            # Poll
            # ------------------------------------------------

            if chat_message.poll:

                await message.reply(
                    "📊 **This post contains a poll.**\n\n"
                    "Telegram does not provide it as a "
                    "downloadable media file."
                )

                return False

            # ------------------------------------------------
            # Text only
            # ------------------------------------------------

            if (
                chat_message.text
                or chat_message.caption
            ):

                text = (
                    raw_text
                    if raw_text
                    else raw_caption
                )

                entities = (
                    raw_text_entities
                    if raw_text
                    else raw_caption_entities
                )

                if not text:
                    return False

                try:

                    sent = await message.reply(
                        text,
                        entities=entities or None,
                    )

                except BadRequest as e:

                    if "ENTITY_TEXT_INVALID" not in str(e):
                        raise

                    sent = await message.reply(text)

                # --------------------------------------------
                # Copy text to forwarding target
                # --------------------------------------------

                target = await get_forward_target(client)

                if target and sent:

                    try:

                        await client.copy_message(
                            chat_id=target,
                            from_chat_id=sent.chat.id,
                            message_id=sent.id,
                        )

                    except Exception as e:

                        LOGGER(__name__).error(
                            f"Text forwarding failed: {e}"
                        )

                return True

            # ------------------------------------------------
            # Nothing
            # ------------------------------------------------

            await message.reply(
                "ℹ️ **No downloadable media or text "
                "was found in this post.**"
            )

            return False

        except asyncio.CancelledError:

            LOGGER(__name__).warning(
                f"Download cancelled: {post_url}"
            )

            raise

        except FloodWait as e:

            wait_time = int(
                getattr(e, "value", 0) or 0
            )

            LOGGER(__name__).warning(
                f"FloodWait in handle_download: "
                f"{wait_time}s"
            )

            if wait_time > 0:
                await asyncio.sleep(
                    wait_time + 1
                )

            return False

        except PeerIdInvalid as e:

            LOGGER(__name__).error(
                f"PeerIdInvalid: {post_url}: {e}"
            )

            await message.reply(
                "🔒 **ACCESS DENIED**\n\n"
                "The connected user account cannot "
                "access this chat.\n\n"
                "Make sure the account has joined "
                "the channel/group."
            )

            return False

        except BadRequest as e:

            LOGGER(__name__).error(
                f"BadRequest: {post_url}: {e}"
            )

            await message.reply(
                "❌ **TELEGRAM REQUEST FAILED**\n\n"
                f"`{e}`\n\n"
                "The message may be deleted, inaccessible "
                "or the URL may be invalid."
            )

            return False

        except (KeyError, ValueError) as e:

            LOGGER(__name__).error(
                f"Invalid URL: {post_url}: {e}"
            )

            await message.reply(
                "❌ **INVALID TELEGRAM URL**\n\n"
                f"`{e}`"
            )

            return False

        except Exception as e:

            LOGGER(__name__).exception(
                f"Unexpected download error: {post_url}"
            )

            await message.reply(
                "⚠️ **DOWNLOAD FAILED**\n\n"
                "Something went wrong while processing "
                "this post.\n\n"
                "Check `/logs` for details."
            )

            return False

        finally:

            if media_path:
                await cleanup_file(media_path)

            if progress_message:
                await safe_delete(progress_message)


# ============================================================
# STORY DOWNLOAD
# ============================================================

async def handle_story_download(
    client: Client,
    message: Message,
    story_url: str,
):

    global download_semaphore

    if download_semaphore is None:
        download_semaphore = asyncio.Semaphore(
            MAX_CONCURRENT_DOWNLOADS
        )

    async with download_semaphore:

        media_path = None
        progress_message = None

        if "?" in story_url:
            story_url = story_url.split("?", 1)[0]

        try:

            # ------------------------------------------------
            # Parse story URL
            # ------------------------------------------------

            chat_username, story_id = (
                getStoryChatMsgID(story_url)
            )

            story = None

            for attempt in range(2):

                try:

                    story = await user.get_stories(
                        chat_id=chat_username,
                        story_ids=story_id,
                    )

                    break

                except FloodWait as e:

                    wait_time = int(
                        getattr(e, "value", 0) or 0
                    )

                    LOGGER(__name__).warning(
                        f"Story FloodWait: "
                        f"{wait_time}s"
                    )

                    if (
                        attempt == 0
                        and wait_time > 0
                    ):

                        await asyncio.sleep(
                            wait_time + 1
                        )

                        continue

                    raise

            if not story:

                await message.reply(
                    "❌ **STORY NOT FOUND**\n\n"
                    "The story may have expired, been deleted, "
                    "or the connected account may not have "
                    "permission to view it."
                )

                return False

            # ------------------------------------------------
            # Validate media
            # ------------------------------------------------

            if story.video:

                allowed = await fileSizeLimit(
                    story.video.file_size,
                    message,
                    "download",
                    user.me.is_premium,
                )

                if not allowed:
                    return False

            if not (
                story.photo
                or story.video
            ):

                await message.reply(
                    "ℹ️ **This story does not contain "
                    "downloadable media.**"
                )

                return False

            raw_caption, raw_caption_entities = (
                get_raw_text(
                    story.caption,
                    story.caption_entities,
                )
            )

            # ------------------------------------------------
            # Progress
            # ------------------------------------------------

            start_time = time()

            progress_message = await message.reply(
                "╭━━━━━━━━━━━━━━━━━━━━╮\n"
                "      **📖 STORY DOWNLOAD**\n"
                "╰━━━━━━━━━━━━━━━━━━━━╯\n\n"
                "⏳ Preparing story..."
            )

            filename = get_story_file_name(
                story_id,
                story,
                chat_username,
            )

            download_path = get_download_path(
                message.id,
                filename,
            )

            # ------------------------------------------------
            # Download
            # ------------------------------------------------

            for attempt in range(2):

                try:

                    media_path = await story.download(
                        file_name=download_path,
                        progress=Leaves.progress_for_pyrogram,
                        progress_args=progressArgs(
                            "📖 Downloading Story",
                            progress_message,
                            start_time,
                        ),
                    )

                    break

                except FloodWait as e:

                    wait_time = int(
                        getattr(e, "value", 0) or 0
                    )

                    if (
                        attempt == 0
                        and wait_time > 0
                    ):

                        await asyncio.sleep(
                            wait_time + 1
                        )

                        continue

                    raise

            # ------------------------------------------------
            # Validate
            # ------------------------------------------------

            if (
                not media_path
                or not os.path.exists(media_path)
            ):

                await safe_edit(
                    progress_message,
                    "❌ **Story download failed.**",
                )

                return False

            if os.path.getsize(media_path) <= 0:

                await safe_edit(
                    progress_message,
                    "❌ **Downloaded story is empty.**",
                )

                return False

            # ------------------------------------------------
            # Send
            # ------------------------------------------------

            media_type = (
                "video"
                if story.video
                else "photo"
            )

            target = await get_forward_target(
                client
            )

            await send_media(
                client,
                message,
                media_path,
                media_type,
                raw_caption,
                raw_caption_entities,
                progress_message,
                start_time,
                forward_chat_id=target,
            )

            return True

        except asyncio.CancelledError:

            LOGGER(__name__).warning(
                f"Story cancelled: {story_url}"
            )

            raise

        except FloodWait as e:

            wait_time = int(
                getattr(e, "value", 0) or 0
            )

            if wait_time > 0:
                await asyncio.sleep(
                    wait_time + 1
                )

            return False

        except PeerIdInvalid as e:

            LOGGER(__name__).error(
                f"Story PeerIdInvalid: {e}"
            )

            await message.reply(
                "🔒 **STORY ACCESS DENIED**\n\n"
                "The connected account cannot access "
                "this user/story."
            )

            return False

        except BadRequest as e:

            LOGGER(__name__).error(
                f"Story BadRequest: {e}"
            )

            await message.reply(
                "❌ **STORY REQUEST FAILED**\n\n"
                f"`{e}`"
            )

            return False

        except (ValueError, KeyError) as e:

            await message.reply(
                "❌ **INVALID STORY URL**\n\n"
                f"`{e}`"
            )

            return False

        except Exception as e:

            LOGGER(__name__).exception(
                f"Story error: {story_url}"
            )

            await message.reply(
                "⚠️ **STORY DOWNLOAD FAILED**\n\n"
                "Check `/logs` for details."
            )

            return False

        finally:

            if media_path:
                await cleanup_file(media_path)

            if progress_message:
                await safe_delete(progress_message)


# ============================================================
# /DL
# ============================================================

@bot.on_message(
    filters.command("dl") & filters.private
)
async def download_media(client, message):

    if len(message.command) < 2:

        await message.reply(
            "📥 **Single Download**\n\n"
            "Usage:\n"
            "`/dl <telegram_post_url>`"
        )

        return

    url = message.command[1].strip()

    if not url.startswith("https://t.me/"):

        await message.reply(
            "❌ **Invalid Telegram URL.**\n\n"
            "Only `https://t.me/...` links are supported."
        )

        return

    await track_task(
        handle_download(
            client,
            message,
            url,
        )
    )


# ============================================================
# /DLS
# ============================================================

@bot.on_message(
    filters.command("dls") & filters.private
)
async def download_story(client, message):

    if len(message.command) < 2:

        await message.reply(
            "📖 **Story Download**\n\n"
            "Usage:\n"
            "`/dls https://t.me/username/s/12`"
        )

        return

    url = message.command[1].strip()

    if not is_story_link(url):

        await message.reply(
            "❌ **Invalid story URL.**\n\n"
            "Expected:\n"
            "`https://t.me/<username>/s/<story_id>`"
        )

        return

    await track_task(
        handle_story_download(
            client,
            message,
            url,
        )
    )


# ============================================================
# BATCH LOCK
# ============================================================

async def acquire_batch(user_id):
    async with STATE_LOCK:

        if user_id in ACTIVE_BATCHES:
            return False

        ACTIVE_BATCHES.add(user_id)
        return True


async def release_batch(user_id):
    async with STATE_LOCK:
        ACTIVE_BATCHES.discard(user_id)


# ============================================================
# BATCH STORY
# ============================================================

@bot.on_message(
    filters.command("bdls") & filters.private
)
async def download_story_range(client, message):

    args = message.text.split()

    if (
        len(args) != 3
        or not all(
            is_story_link(x)
            for x in args[1:]
        )
    ):

        await message.reply(
            "📚 **Batch Story Download**\n\n"
            "Usage:\n"
            "`/bdls start_story end_story`\n\n"
            "Example:\n"
            "`/bdls https://t.me/user/s/10 "
            "https://t.me/user/s/25`"
        )

        return

    user_id = message.from_user.id

    if not await acquire_batch(user_id):

        await message.reply(
            "⚠️ **You already have an active batch.**\n\n"
            "Use `/killall` to stop running tasks."
        )

        return

    loading = None

    try:

        start_chat, start_id = (
            getStoryChatMsgID(args[1])
        )

        end_chat, end_id = (
            getStoryChatMsgID(args[2])
        )

        if str(start_chat).lower() != str(
            end_chat
        ).lower():

            await message.reply(
                "❌ **Both story links must belong "
                "to the same user/channel.**"
            )

            return

        if start_id > end_id:

            await message.reply(
                "❌ **Invalid range.**\n\n"
                "Start story ID cannot be greater "
                "than end story ID."
            )

            return

        total = end_id - start_id + 1

        if total > MAX_BDL_RANGE:

            await message.reply(
                "❌ **Batch limit exceeded.**\n\n"
                f"Maximum: `{MAX_BDL_RANGE}` stories\n"
                f"Requested: `{total}` stories"
            )

            return

        loading = await message.reply(
            "╭━━━━━━━━━━━━━━━━━━━━╮\n"
            "      **📖 STORY BATCH**\n"
            "╰━━━━━━━━━━━━━━━━━━━━╯\n\n"
            f"📦 Total: `{total}`\n"
            f"🆔 Range: `{start_id} → {end_id}`\n\n"
            "⏳ Starting..."
        )

        downloaded = 0
        failed = 0
        processed = 0

        prefix = (
            f"https://t.me/{start_chat}/s"
        )

        # ----------------------------------------------------
        # Controlled batches
        # ----------------------------------------------------

        for batch_start in range(
            start_id,
            end_id + 1,
            BATCH_SIZE,
        ):

            batch_end = min(
                batch_start + BATCH_SIZE - 1,
                end_id,
            )

            tasks = []

            for story_id in range(
                batch_start,
                batch_end + 1,
            ):

                url = f"{prefix}/{story_id}"

                task = track_task(
                    handle_story_download(
                        client,
                        message,
                        url,
                    )
                )

                tasks.append(task)

            results = await asyncio.gather(
                *tasks,
                return_exceptions=True,
            )

            for result in results:

                processed += 1

                if (
                    isinstance(
                        result,
                        asyncio.CancelledError,
                    )
                ):
                    continue

                if (
                    isinstance(
                        result,
                        Exception,
                    )
                    or result is not True
                ):

                    failed += 1

                else:

                    downloaded += 1

            bar = progress_bar(
                processed,
                total,
            )

            await safe_edit(
                loading,
                "╭━━━━━━━━━━━━━━━━━━━━╮\n"
                "      **📖 STORY BATCH**\n"
                "╰━━━━━━━━━━━━━━━━━━━━╯\n\n"
                f"{bar}\n\n"
                f"📊 Progress: `{processed}/{total}`\n"
                f"📥 Success: `{downloaded}`\n"
                f"❌ Failed: `{failed}`\n\n"
                "⚙️ Processing...",
            )

            if FLOOD_WAIT_DELAY > 0:
                await asyncio.sleep(
                    FLOOD_WAIT_DELAY
                )

        await safe_delete(loading)

        await message.reply(
            "╭━━━━━━━━━━━━━━━━━━━━╮\n"
            "    **✅ STORY BATCH DONE**\n"
            "╰━━━━━━━━━━━━━━━━━━━━╯\n\n"
            f"📦 Requested: `{total}`\n"
            f"📥 Downloaded: `{downloaded}`\n"
            f"❌ Failed: `{failed}`\n\n"
            f"🆔 Range: `{start_id} → {end_id}`"
        )

    except asyncio.CancelledError:

        await safe_delete(loading)

        await message.reply(
            "🛑 **Story batch stopped.**"
        )

        raise

    except Exception as e:

        LOGGER(__name__).exception(
            f"BDLS fatal error: {e}"
        )

        await safe_delete(loading)

        await message.reply(
            "⚠️ **Story batch stopped unexpectedly.**\n\n"
            f"`{e}`"
        )

    finally:

        await release_batch(user_id)


# ============================================================
# BATCH POST
# ============================================================

@bot.on_message(
    filters.command("bdl") & filters.private
)
async def download_range(client, message):

    args = message.text.split()

    # --------------------------------------------------------
    # Validation
    # --------------------------------------------------------

    if len(args) != 3:

        await message.reply(
            "╭━━━━━━━━━━━━━━━━━━━━╮\n"
            "       **📦 BATCH DOWNLOAD**\n"
            "╰━━━━━━━━━━━━━━━━━━━━╯\n\n"
            "**Usage**\n"
            "`/bdl start_link end_link`\n\n"
            "**Example**\n"
            "`/bdl https://t.me/channel/100 "
            "https://t.me/channel/150`\n\n"
            f"📌 Maximum range: `{MAX_BDL_RANGE}` posts"
        )

        return

    start_url = args[1].strip()
    end_url = args[2].strip()

    if not (
        start_url.startswith("https://t.me/")
        and end_url.startswith("https://t.me/")
    ):

        await message.reply(
            "❌ **Invalid Telegram URL.**\n\n"
            "Only `https://t.me/...` links are supported."
        )

        return

    # --------------------------------------------------------
    # Parse
    # --------------------------------------------------------

    try:

        start_chat, start_id = (
            getChatMsgID(start_url)
        )

        end_chat, end_id = (
            getChatMsgID(end_url)
        )

    except Exception as e:

        LOGGER(__name__).error(
            f"BDL parse error: {e}"
        )

        await message.reply(
            "❌ **Could not parse Telegram URLs.**\n\n"
            f"`{e}`"
        )

        return

    # --------------------------------------------------------
    # Same chat
    # --------------------------------------------------------

    if str(start_chat).lower() != str(
        end_chat
    ).lower():

        await message.reply(
            "❌ **Different chats detected.**\n\n"
            "Both links must belong to the same "
            "channel/group."
        )

        return

    # --------------------------------------------------------
    # Range
    # --------------------------------------------------------

    if start_id > end_id:

        await message.reply(
            "❌ **Invalid range.**\n\n"
            "Start message ID cannot be greater "
            "than end message ID."
        )

        return

    total_posts = end_id - start_id + 1

    if total_posts > MAX_BDL_RANGE:

        await message.reply(
            "❌ **Batch limit exceeded.**\n\n"
            f"Maximum allowed: `{MAX_BDL_RANGE}`\n"
            f"Requested: `{total_posts}`"
        )

        return

    user_id = message.from_user.id

    if not await acquire_batch(user_id):

        await message.reply(
            "⚠️ **You already have an active batch.**\n\n"
            "Finish it first or use `/killall`."
        )

        return

    loading = None

    try:

        # ----------------------------------------------------
        # Resolve chat
        # ----------------------------------------------------

        try:

            await user.get_chat(
                start_chat
            )

        except Exception as e:

            LOGGER(__name__).warning(
                f"Chat pre-resolution failed: "
                f"{start_chat}: {e}"
            )

        # ----------------------------------------------------
        # Start UI
        # ----------------------------------------------------

        loading = await message.reply(
            "╭━━━━━━━━━━━━━━━━━━━━╮\n"
            "       **📦 BATCH DOWNLOAD**\n"
            "╰━━━━━━━━━━━━━━━━━━━━╯\n\n"
            f"🆔 Range: `{start_id} → {end_id}`\n"
            f"📦 Total: `{total_posts}` posts\n"
            f"⚙️ Batch Size: `{BATCH_SIZE}`\n\n"
            "🔎 Scanning messages..."
        )

        downloaded = 0
        skipped = 0
        failed = 0
        processed = 0

        # Track media groups.
        processed_media_groups = set()

        # Correct URL prefix.
        prefix = start_url.rsplit(
            "/",
            1,
        )[0]

        # ----------------------------------------------------
        # Fetch with retry
        # ----------------------------------------------------

        async def get_message_with_retry(
            msg_id: int,
        ):

            for attempt in range(
                BDL_RETRIES
            ):

                try:

                    return await user.get_messages(
                        chat_id=start_chat,
                        message_ids=msg_id,
                    )

                except FloodWait as e:

                    wait_time = int(
                        getattr(
                            e,
                            "value",
                            0,
                        )
                        or 0
                    )

                    LOGGER(__name__).warning(
                        f"FloodWait reading "
                        f"{msg_id}: {wait_time}s"
                    )

                    if wait_time > 0:

                        await asyncio.sleep(
                            wait_time + 1
                        )

                except Exception as e:

                    LOGGER(__name__).warning(
                        f"Read failed for {msg_id}, "
                        f"attempt "
                        f"{attempt + 1}/"
                        f"{BDL_RETRIES}: {e}"
                    )

                    if (
                        attempt
                        < BDL_RETRIES - 1
                    ):

                        await asyncio.sleep(
                            1.5 * (
                                attempt + 1
                            )
                        )

                    else:

                        raise

            return None

        # ----------------------------------------------------
        # Process messages
        # ----------------------------------------------------

        for batch_start in range(
            start_id,
            end_id + 1,
            BATCH_SIZE,
        ):

            batch_end = min(
                batch_start + BATCH_SIZE - 1,
                end_id,
            )

            tasks = []

            # -----------------------------------------------
            # Scan current batch
            # -----------------------------------------------

            for msg_id in range(
                batch_start,
                batch_end + 1,
            ):

                try:

                    chat_msg = (
                        await get_message_with_retry(
                            msg_id
                        )
                    )

                    processed += 1

                    if not chat_msg:

                        skipped += 1
                        continue

                    # ---------------------------------------
                    # Media group protection
                    # ---------------------------------------

                    if chat_msg.media_group_id:

                        group_id = str(
                            chat_msg.media_group_id
                        )

                        if (
                            group_id
                            in processed_media_groups
                        ):

                            skipped += 1
                            continue

                        processed_media_groups.add(
                            group_id
                        )

                    # ---------------------------------------
                    # Content validation
                    # ---------------------------------------

                    has_media = bool(
                        chat_msg.media
                    )

                    has_text = bool(
                        chat_msg.text
                        or chat_msg.caption
                    )

                    if not (
                        has_media
                        or has_text
                    ):

                        skipped += 1
                        continue

                    url = (
                        f"{prefix}/{msg_id}"
                    )

                    # ---------------------------------------
                    # Create task
                    # ---------------------------------------

                    tasks.append(
                        track_task(
                            handle_download(
                                client,
                                message,
                                url,
                            )
                        )
                    )

                except asyncio.CancelledError:

                    raise

                except Exception as e:

                    failed += 1

                    LOGGER(__name__).error(
                        f"BDL scan error "
                        f"{msg_id}: {e}"
                    )

            # -----------------------------------------------
            # Execute batch
            # -----------------------------------------------

            if tasks:

                results = await asyncio.gather(
                    *tasks,
                    return_exceptions=True,
                )

                for result in results:

                    if isinstance(
                        result,
                        asyncio.CancelledError,
                    ):
                        continue

                    if (
                        isinstance(
                            result,
                            Exception,
                        )
                        or result is not True
                    ):

                        failed += 1

                    else:

                        downloaded += 1

            # -----------------------------------------------
            # Progress
            # -----------------------------------------------

            bar = progress_bar(
                processed,
                total_posts,
            )

            if (
                processed % BDL_PROGRESS_EVERY == 0
                or processed == total_posts
            ):

                await safe_edit(
                    loading,
                    "╭━━━━━━━━━━━━━━━━━━━━╮\n"
                    "       **📦 BATCH DOWNLOAD**\n"
                    "╰━━━━━━━━━━━━━━━━━━━━╯\n\n"
                    f"{bar}\n\n"
                    f"📊 Progress: "
                    f"`{processed}/{total_posts}`\n"
                    f"📥 Downloaded: `{downloaded}`\n"
                    f"⏭️ Skipped: `{skipped}`\n"
                    f"❌ Failed: `{failed}`\n\n"
                    f"🆔 Current ID: `{batch_end}`"
                )

            if FLOOD_WAIT_DELAY > 0:

                await asyncio.sleep(
                    FLOOD_WAIT_DELAY
                )

        # ----------------------------------------------------
        # Complete
        # ----------------------------------------------------

        await safe_delete(loading)

        await message.reply(
            "╭━━━━━━━━━━━━━━━━━━━━╮\n"
            "      **✅ BATCH COMPLETED**\n"
            "╰━━━━━━━━━━━━━━━━━━━━╯\n\n"
            f"📦 Requested: `{total_posts}`\n"
            f"📥 Downloaded: `{downloaded}`\n"
            f"⏭️ Skipped: `{skipped}`\n"
            f"❌ Failed: `{failed}`\n\n"
            f"🆔 Range: `{start_id} → {end_id}`\n\n"
            "✨ **Processing finished successfully.**"
        )

    except asyncio.CancelledError:

        LOGGER(__name__).warning(
            f"BDL cancelled by user "
            f"{user_id}"
        )

        await safe_delete(loading)

        await message.reply(
            "╭━━━━━━━━━━━━━━━━━━━━╮\n"
            "       **🛑 BATCH STOPPED**\n"
            "╰━━━━━━━━━━━━━━━━━━━━╯\n\n"
            "The active batch was cancelled.\n\n"
            "Use `/stats` to check the bot status."
        )

        raise

    except Exception as e:

        LOGGER(__name__).exception(
            f"Fatal BDL error: {e}"
        )

        await safe_delete(loading)

        await message.reply(
            "⚠️ **BATCH PROCESS FAILED**\n\n"
            "The batch was stopped because of an "
            "unexpected error.\n\n"
            f"`{e}`\n\n"
            "Check `/logs` for details."
        )

    finally:

        await release_batch(user_id)


# ============================================================
# DIRECT URL HANDLER
# ============================================================

@bot.on_message(
    filters.private
    & ~filters.command(
        [
            "start",
            "help",
            "dl",
            "bdl",
            "dls",
            "bdls",
            "stats",
            "logs",
            "killall",
            "cleanup",
        ]
    )
)
async def handle_any_message(
    client,
    message: Message,
):

    if not message.text:
        return

    text = message.text.strip()

    if not text:
        return

    if text.startswith("/"):
        return

    # --------------------------------------------------------
    # Story
    # --------------------------------------------------------

    if is_story_link(text):

        await track_task(
            handle_story_download(
                client,
                message,
                text,
            )
        )

        return

    # --------------------------------------------------------
    # Post
    # --------------------------------------------------------

    if text.startswith(
        (
            "https://t.me/",
            "http://t.me/",
            "https://telegram.me/",
            "http://telegram.me/",
        )
    ):

        await track_task(
            handle_download(
                client,
                message,
                text,
            )
        )

        return

    await message.reply(
        "👋 **Send me a Telegram link to start.**\n\n"
        "📥 Post:\n"
        "`https://t.me/channel/123`\n\n"
        "📖 Story:\n"
        "`https://t.me/user/s/12`\n\n"
        "Or use `/help` for all commands."
    )


# ============================================================
# STATS
# ============================================================

@bot.on_message(
    filters.command("stats") & filters.private
)
async def stats(_, message):

    try:

        uptime = get_readable_time(
            time() - PyroConf.BOT_START_TIME
        )

        disk_total, disk_used, disk_free = (
            shutil.disk_usage(".")
        )

        process = psutil.Process(
            os.getpid()
        )

        memory_info = process.memory_info()

        net = psutil.net_io_counters()

        cpu = psutil.cpu_percent(
            interval=0.5
        )

        ram = psutil.virtual_memory().percent

        disk = psutil.disk_usage("/").percent

        running = len(
            get_running_tasks()
        )

        active_batches = len(
            ACTIVE_BATCHES
        )

        text = (
            "╭━━━━━━━━━━━━━━━━━━━━╮\n"
            "       **📊 SYSTEM STATUS**\n"
            "╰━━━━━━━━━━━━━━━━━━━━╯\n\n"

            "🟢 **BOT**\n"
            f"├ Status: `ONLINE`\n"
            f"├ Uptime: `{uptime}`\n"
            f"├ Active Tasks: `{running}`\n"
            f"└ Active Batches: `{active_batches}`\n\n"

            "💾 **STORAGE**\n"
            f"├ Total: `{get_readable_file_size(disk_total)}`\n"
            f"├ Used: `{get_readable_file_size(disk_used)}`\n"
            f"├ Free: `{get_readable_file_size(disk_free)}`\n"
            f"└ Usage: `{disk}%`\n\n"

            "🧠 **SYSTEM**\n"
            f"├ CPU: `{cpu}%`\n"
            f"├ RAM: `{ram}%`\n"
            f"└ Process RAM: "
            f"`{round(memory_info.rss / 1024**2)} MiB`\n\n"

            "🌐 **NETWORK**\n"
            f"├ Upload: `{get_readable_file_size(net.bytes_sent)}`\n"
            f"└ Download: `{get_readable_file_size(net.bytes_recv)}`\n\n"

            "⚙️ **BOT CONFIG**\n"
            f"├ Batch Limit: `{MAX_BDL_RANGE}`\n"
            f"├ Batch Size: `{BATCH_SIZE}`\n"
            f"├ Download Slots: `{MAX_CONCURRENT_DOWNLOADS}`\n"
            f"└ Retries: `{BDL_RETRIES}`"
        )

        await message.reply(text)

    except Exception as e:

        LOGGER(__name__).exception(
            f"Stats error: {e}"
        )

        await message.reply(
            "❌ **Could not collect system statistics.**"
        )


# ============================================================
# CLEANUP
# ============================================================

@bot.on_message(
    filters.command("cleanup") & filters.private
)
async def cleanup_storage(_, message):

    try:

        files_removed, bytes_freed = (
            cleanup_downloads_root()
        )

        if files_removed == 0:

            await message.reply(
                "🧹 **Storage is already clean.**\n\n"
                "No temporary downloads were found."
            )

            return

        await message.reply(
            "╭━━━━━━━━━━━━━━━━━━━━╮\n"
            "       **🧹 CLEANUP COMPLETE**\n"
            "╰━━━━━━━━━━━━━━━━━━━━╯\n\n"
            f"🗑 Files removed: `{files_removed}`\n"
            f"💾 Space freed: "
            f"`{get_readable_file_size(bytes_freed)}`"
        )

    except Exception as e:

        LOGGER(__name__).exception(
            f"Cleanup failed: {e}"
        )

        await message.reply(
            "❌ **Cleanup failed.**\n\n"
            "Check `/logs` for details."
        )


# ============================================================
# LOGS
# ============================================================

@bot.on_message(
    filters.command("logs") & filters.private
)
async def logs(_, message):

    if not os.path.exists(
        "logs.txt"
    ):

        await message.reply(
            "ℹ️ **No logs file found.**"
        )

        return

    try:

        await message.reply_document(
            document="logs.txt",
            caption="📜 **Bot Logs**",
        )

    except Exception as e:

        LOGGER(__name__).error(
            f"Could not send logs: {e}"
        )

        await message.reply(
            "❌ **Could not send logs file.**"
        )


# ============================================================
# KILL ALL
# ============================================================

@bot.on_message(
    filters.command("killall") & filters.private
)
async def cancel_all_tasks(_, message):

    tasks = get_running_tasks()

    if not tasks:

        await message.reply(
            "ℹ️ **No active download tasks.**"
        )

        return

    count = len(tasks)

    for task in tasks:

        if not task.done():

            task.cancel()

    results = await asyncio.gather(
        *tasks,
        return_exceptions=True,
    )

    cancelled = sum(
        1
        for result in results
        if isinstance(
            result,
            asyncio.CancelledError,
        )
    )

    # Clear active batch locks.
    async with STATE_LOCK:
        ACTIVE_BATCHES.clear()

    await message.reply(
        "╭━━━━━━━━━━━━━━━━━━━━╮\n"
        "       **🛑 TASK MANAGER**\n"
        "╰━━━━━━━━━━━━━━━━━━━━╯\n\n"
        "All active download tasks have been "
        "requested to stop.\n\n"
        f"⚙️ Active tasks: `{count}`\n"
        f"🛑 Cancelled: `{cancelled}`"
    )


# ============================================================
# INITIALIZATION
# ============================================================

async def initialize():

    global download_semaphore
    global forward_chat_id

    # --------------------------------------------------------
    # Semaphore
    # --------------------------------------------------------

    download_semaphore = asyncio.Semaphore(
        MAX_CONCURRENT_DOWNLOADS
    )

    LOGGER(__name__).info(
        "Download semaphore initialized: "
        f"{MAX_CONCURRENT_DOWNLOADS}"
    )

    # --------------------------------------------------------
    # Forward target
    # --------------------------------------------------------

    configured_forward = cfg(
        "FORWARD_CHAT_ID",
        None,
    )

    if configured_forward:

        try:

            forward_chat_id = (
                await resolve_forward_chat_id(
                    configured_forward
                )
            )

            LOGGER(__name__).info(
                "Auto-forward enabled. "
                f"Target: {forward_chat_id}"
            )

        except Exception as e:

            forward_chat_id = None

            LOGGER(__name__).error(
                "Could not resolve forward target: "
                f"{e}"
            )

    else:

        LOGGER(__name__).info(
            "Auto-forward disabled."
        )


# ============================================================
# MAIN
# ============================================================

if __name__ == "__main__":

    try:

        LOGGER(__name__).info(
            "========================================"
        )

        LOGGER(__name__).info(
            "PRO MEDIA DOWNLOADER STARTING"
        )

        LOGGER(__name__).info(
            f"Batch limit: {MAX_BDL_RANGE}"
        )

        LOGGER(__name__).info(
            f"Batch size: {BATCH_SIZE}"
        )

        LOGGER(__name__).info(
            f"Download concurrency: "
            f"{MAX_CONCURRENT_DOWNLOADS}"
        )

        LOGGER(__name__).info(
            "========================================"
        )

        asyncio.get_event_loop().run_until_complete(
            initialize()
        )

        # Start user session first.
        user.start()

        LOGGER(__name__).info(
            "User session started."
        )

        # Start bot.
        bot.run()

    except KeyboardInterrupt:

        LOGGER(__name__).info(
            "Shutdown requested by user."
        )

    except Exception as err:

        LOGGER(__name__).exception(
            f"Fatal startup error: {err}"
        )

    finally:

        LOGGER(__name__).info(
            "Media Downloader stopped."
        )