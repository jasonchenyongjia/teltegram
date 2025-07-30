import os
import json
import logging
import pytz
import random
import time
import asyncio
import aiofiles
import portalocker
from telegram import Bot, Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes, MessageHandler, filters, CallbackQueryHandler
from apscheduler.schedulers.background import BackgroundScheduler

# 配置
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

TOKEN = "8247017702:AAEJ93EVkp5P_uxyU9DZso_9r_ZHMxcgXv0"
DATA_FILE = "C:\\Users\\Administrator\\Documents\\WPSDrive\\data.json"
STATE_FILE = "C:\\Users\Administrator\\Documents\\WPSDrive\\send_state.json"
POST_INTERVAL_MINUTES = 1  # 每分钟发送
TIMEZONE = pytz.timezone('Asia/Shanghai')

# 带锁的文件操作
async def load_json(file_path, default_data):
    try:
        async with aiofiles.open(file_path, mode='r', encoding='utf-8') as f:
            data = json.loads(await f.read())
            if not isinstance(data, dict):
                raise ValueError("JSON 数据不是有效的字典")
            logger.info(f"从 {file_path} 加载数据: {json.dumps(data, indent=2)}")
            return data
    except (json.JSONDecodeError, FileNotFoundError, ValueError):
        logger.warning(f"创建 {file_path} 并使用默认数据")
        async with aiofiles.open(file_path, mode='w', encoding='utf-8') as f:
            with portalocker.Lock(file_path + ".lock", mode='wb', timeout=5):
                await f.write(json.dumps(default_data, indent=2, ensure_ascii=False))
        logger.info(f"初始化 {file_path} 以默认数据: {json.dumps(default_data, indent=2)}")
        return default_data

async def save_json(file_path, data):
    try:
        directory = os.path.dirname(file_path) or '.'
        if not os.access(directory, os.W_OK):
            logger.error(f"无权写入目录：{directory}")
            raise PermissionError(f"无权写入 {directory}")
        max_retries = 3
        for attempt in range(max_retries):
            try:
                async with aiofiles.open(file_path, mode='w', encoding='utf-8') as f:
                    with portalocker.Lock(file_path + ".lock", mode='wb', timeout=5):
                        await f.write(json.dumps(data, indent=2, ensure_ascii=False))
                logger.info(f"数据已保存至 {file_path}, 内容: {json.dumps(data, indent=2)}")
                return
            except Exception as e:
                logger.warning(f"保存 {file_path} 失败（尝试 {attempt + 1}/{max_retries}）：{e}")
                if attempt < max_retries - 1:
                    await asyncio.sleep(1)  # 等待 1 秒后重试
                else:
                    raise
    except Exception as e:
        logger.error(f"保存 {file_path} 最终失败：{e}")
        raise

# 加载初始数据
async def load_data():
    default_data = {"channels": [], "posts": [], "tags": [], "state": {}}
    data = await load_json(DATA_FILE, default_data)
    if not data["posts"]:
        default_buttons = [
            [{"text": "▶️点击观看此影片", "url": "https://t.me/ttttt04111"}],
            [{"text": "🆕MK在线投注", "url": "https://t.me/MK7777bot"}]
        ]
        data["posts"].append({"photos": [], "text": "", "buttons": default_buttons})
        await save_json(DATA_FILE, data)
        logger.info("初始化默认帖子")
    return data

async def load_send_state():
    default_state = {"current_post_index": 0, "last_round_time": 0}
    return await load_json(STATE_FILE, default_state)

# 菜单功能
async def send_main_menu(chat_id: int, context: ContextTypes.DEFAULT_TYPE):
    keyboard = [
        [InlineKeyboardButton("📢 频道管理", callback_data="manage_channels")],
        [InlineKeyboardButton("📝 内容管理", callback_data="manage_content")],
        [InlineKeyboardButton("🏷 标签管理", callback_data="add_tag")],
        [InlineKeyboardButton("📑 查看已添加标签", callback_data="view_tags")],
        [InlineKeyboardButton("📤 手动群发", callback_data="post_now")]
    ]
    await context.bot.send_message(chat_id, "请选择操作：", reply_markup=InlineKeyboardMarkup(keyboard))

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await send_main_menu(update.effective_chat.id, context)

# 频道管理
async def manage_channels(chat_id: int, context: ContextTypes.DEFAULT_TYPE):
    keyboard = [
        [InlineKeyboardButton("➕ 添加频道", callback_data="add_channel")],
        [InlineKeyboardButton("📋 已添加频道", callback_data="view_channels")]
    ]
    await context.bot.send_message(chat_id, "📢 频道管理菜单：", reply_markup=InlineKeyboardMarkup(keyboard))

async def add_channel(chat_id: int, context: ContextTypes.DEFAULT_TYPE):
    await context.bot.send_message(chat_id, "请将频道的一条消息转发给我，我会自动提取频道 ID。", reply_markup=back_to_channel_menu())

async def view_channels(chat_id: int, context: ContextTypes.DEFAULT_TYPE):
    data = await load_data()
    if not data["channels"]:
        await context.bot.send_message(chat_id, "⚠️ 暂无已添加频道。", reply_markup=back_to_channel_menu())
    else:
        channels_text = "\n".join(data["channels"])
        keyboard = [[InlineKeyboardButton("❌ 删除", callback_data=f"delete_channel_{cid}")] for cid in data["channels"]]
        keyboard.append([InlineKeyboardButton("⬅ 返回", callback_data="manage_channels")])
        await context.bot.send_message(chat_id, f"📋 已添加频道：\n{channels_text}", reply_markup=InlineKeyboardMarkup(keyboard))

# 菜单导航
def back_to_content_menu():
    return InlineKeyboardMarkup([[InlineKeyboardButton("⬅ 返回内容管理", callback_data="manage_content")]])

def back_to_channel_menu():
    return InlineKeyboardMarkup([[InlineKeyboardButton("⬅ 返回频道管理", callback_data="manage_channels")]])

# 回调查询处理
async def button_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    chat_id = query.message.chat_id
    data = await load_data()

    if query.data == "manage_channels":
        await manage_channels(chat_id, context)
    elif query.data == "add_channel":
        await add_channel(chat_id, context)
    elif query.data == "view_channels":
        await view_channels(chat_id, context)
    elif query.data.startswith("delete_channel_"):
        cid = query.data.replace("delete_channel_", "")
        if cid in data["channels"]:
            data["channels"].remove(cid)
            await save_json(DATA_FILE, data)
            updated_data = await load_json(DATA_FILE, {"channels": [], "posts": [], "tags": [], "state": {}})
            if cid not in updated_data["channels"]:
                await context.bot.send_message(chat_id, f"✅ 已删除频道：{cid}")
            else:
                await context.bot.send_message(chat_id, f"⚠️ 删除频道 {cid} 失败，请重试")
            await view_channels(chat_id, context)
        else:
            await context.bot.send_message(chat_id, f"⚠️ 未找到频道：{cid}")
    elif query.data == "manage_content":
        keyboard = [
            [InlineKeyboardButton("🖋 添加图文内容", callback_data="add_mixed")],
            [InlineKeyboardButton("🔗 添加跳转按钮", callback_data="add_buttons")],
            [InlineKeyboardButton("📂 查看/修改文本", callback_data="view_posts")],
            [InlineKeyboardButton("⬅ 返回", callback_data="main_menu")]
        ]
        await context.bot.send_message(chat_id, "📝 内容管理菜单：", reply_markup=InlineKeyboardMarkup(keyboard))
    elif query.data == "post_now":
        await send_post(context.bot)
        await context.bot.send_message(chat_id, "✅ 内容已立即发送到频道")
    elif query.data == "main_menu":
        await send_main_menu(chat_id, context)
    elif query.data == "add_mixed":
        data['state'][str(chat_id)] = "waiting_for_photos"
        await save_json(DATA_FILE, data)
        await context.bot.send_message(chat_id, "请使用相册模式（长按选择多张图片）上传图片。每次相册最多10张，需分多次发送。上传完成后，Bot会提示你输入主文案。")
    elif query.data == "add_buttons":
        if not data["posts"]:
            await context.bot.send_message(chat_id, "⚠️ 请先添加图文内容。")
        else:
            data['state'][str(chat_id)] = "waiting_for_buttons"
            await save_json(DATA_FILE, data)
            keyboard = [[InlineKeyboardButton(f"编辑帖子 {i+1}", callback_data=f"edit_buttons_{i}")] for i in range(len(data["posts"]))]
            keyboard.append([InlineKeyboardButton("⬅ 返回", callback_data="main_menu")])
            await context.bot.send_message(chat_id, "请选择要编辑的帖子以添加跳转按钮：", reply_markup=InlineKeyboardMarkup(keyboard))
    elif query.data == "add_tag":
        data['state'][str(chat_id)] = "waiting_for_tag"
        await save_json(DATA_FILE, data)
        await context.bot.send_message(chat_id, "请输入标签（以 # 开头，例如 #tag1 #tag2），支持多行输入。")
    elif query.data == "view_tags":
        if not data["tags"]:
            await context.bot.send_message(chat_id, "⚠️ 暂无已添加标签。", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅ 返回主菜单", callback_data="main_menu")]]))
        else:
            tags_text = "\n".join(data["tags"])
            keyboard = [[InlineKeyboardButton(f"❌ 删除 {tag}", callback_data=f"delete_tag_{tag}")] for tag in data["tags"]]
            keyboard.append([InlineKeyboardButton("⬅ 返回主菜单", callback_data="main_menu")])
            await context.bot.send_message(chat_id, f"📑 已添加标签：\n{tags_text}", reply_markup=InlineKeyboardMarkup(keyboard))
    elif query.data.startswith("delete_tag_"):
        tag_to_delete = query.data.replace("delete_tag_", "")
        if tag_to_delete in data["tags"]:
            data["tags"].remove(tag_to_delete)
            await save_json(DATA_FILE, data)
            updated_data = await load_json(DATA_FILE, {"channels": [], "posts": [], "tags": [], "state": {}})
            if tag_to_delete not in updated_data["tags"]:
                await context.bot.send_message(chat_id, f"✅ 已删除标签：{tag_to_delete}")
            else:
                await context.bot.send_message(chat_id, f"⚠️ 删除标签 {tag_to_delete} 失败，请重试")
            await view_tags(chat_id, context)
        else:
            await context.bot.send_message(chat_id, f"⚠️ 标签 {tag_to_delete} 不存在。")
    elif query.data.startswith("edit_buttons_"):
        index = int(query.data.split("_")[-1])
        if 0 <= index < len(data["posts"]):
            data['state'][str(chat_id)] = f"editing_buttons_{index}"
            await save_json(DATA_FILE, data)
            await context.bot.send_message(chat_id, f"请输入按钮格式，格式如下：\n\n按钮文本-https://链接\n按钮文本1-https://链接&&按钮文本2-https://链接\n\n支持多行，一行代表一排按钮。")
        else:
            await context.bot.send_message(chat_id, "⚠️ 帖子索引无效。")
    elif query.data.startswith("edit_post_"):
        index = int(query.data.split("_")[-1])
        if 0 <= index < len(data["posts"]):
            data['state'][str(chat_id)] = f"editing_post_{index}"
            await save_json(DATA_FILE, data)
            await context.bot.send_message(chat_id, f"请输入新的文本内容（当前为：{data['posts'][index].get('text', '')}）")
        else:
            await context.bot.send_message(chat_id, "⚠️ 帖子索引无效，请重新选择帖子。")
    elif query.data.startswith("delete_post_"):
        index = int(query.data.split("_")[-1])
        if 0 <= index < len(data["posts"]):
            deleted_post = data["posts"].pop(index)
            await save_json(DATA_FILE, data)
            updated_data = await load_json(DATA_FILE, {"channels": [], "posts": [], "tags": [], "state": {}})
            if len(updated_data["posts"]) == len(data["posts"]):
                await context.bot.send_message(chat_id, f"✅ 已删除帖子：{deleted_post.get('text', '无文本')}")
            else:
                await context.bot.send_message(chat_id, "⚠️ 删除帖子失败，请重试")
        await send_main_menu(chat_id, context)
    elif query.data == "view_posts":
        keyboard = []
        for i, post in enumerate(data['posts']):
            if 'text' in post or 'photos' in post:
                keyboard.append([InlineKeyboardButton(f"✏ 修改内容 {i+1}", callback_data=f"edit_post_{i}"), InlineKeyboardButton("❌ 删除", callback_data=f"delete_post_{i}")])
        keyboard.append([InlineKeyboardButton("⬅ 返回", callback_data="manage_content")])
        await context.bot.send_message(chat_id, "📂 当前内容列表：", reply_markup=InlineKeyboardMarkup(keyboard))

# 消息处理
async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message:
        return

    chat_id = str(update.effective_chat.id)
    data = await load_data()
    user_state = data.get("state", {}).get(chat_id)

    if user_state == "waiting_for_photos":
        if update.message.photo:
            if update.message.media_group_id:
                media_group = context.bot_data.get(update.message.media_group_id, [])
                if not media_group:
                    context.bot_data[update.message.media_group_id] = [update.message]
                    return
                else:
                    media_group.append(update.message)
                    if all(msg.photo for msg in media_group):
                        photo_ids = [photo.file_id for msg in media_group for photo in msg.photo]
                        data["posts"].append({"photos": photo_ids, "text": "", "buttons": [
                            [{"text": "▶️点击观看此影片", "url": "https://t.me/ttttt04111"}],
                            [{"text": "🆕MK在线投注", "url": "https://t.me/MK7777bot"}]
                        ]})
                        data["state"][str(chat_id)] = "waiting_for_text"
                        await save_json(DATA_FILE, data)
                        await update.message.reply_text(f"✅ 已上传{len(photo_ids)}张图片，请输入主文案。", reply_markup=back_to_content_menu())
                        del context.bot_data[update.message.media_group_id]
                    return
            else:
                photo_ids = [photo.file_id for photo in update.message.photo]
                data["posts"].append({"photos": photo_ids, "text": "", "buttons": [
                    [{"text": "▶️点击观看此影片", "url": "https://t.me/ttttt04111"}],
                    [{"text": "🆕MK在线投注", "url": "https://t.me/MK7777bot"}]
                ]})
                data["state"][str(chat_id)] = "waiting_for_text"
                await save_json(DATA_FILE, data)
                await update.message.reply_text(f"✅ 已上传{len(photo_ids)}张图片，请输入主文案。", reply_markup=back_to_content_menu())
        else:
            await update.message.reply_text("⚠️ 请使用相册模式（长按选择多张图片）上传图片。每次相册最多10张，需分多次发送。")

    elif user_state == "waiting_for_text":
        text = update.message.text
        if data["posts"] and not data["posts"][-1].get("text"):
            data["posts"][-1]["text"] = text
            data["state"].pop(str(chat_id), None)
            await save_json(DATA_FILE, data)
            await update.message.reply_text("✅ 主文案已保存。", reply_markup=back_to_content_menu())
        else:
            await update.message.reply_text("⚠️ 请先上传图片。", reply_markup=back_to_content_menu())

    elif user_state and user_state.startswith("editing_post_"):
        index = int(user_state.split("_")[-1])
        text = update.message.text
        if 0 <= index < len(data["posts"]):
            data["posts"][index]["text"] = text
            data["state"].pop(chat_id, None)
            await save_json(DATA_FILE, data)
            await update.message.reply_text("✅ 文本已更新", reply_markup=back_to_content_menu())
        else:
            data["state"].pop(chat_id, None)
            await save_json(DATA_FILE, data)
            await update.message.reply_text("⚠️ 帖子索引无效，请重新选择帖子。", reply_markup=back_to_content_menu())

    elif user_state and (user_state == "waiting_for_buttons" or user_state.startswith("editing_buttons_")):
        text = update.message.text.strip()
        lines = text.split("\n")
        buttons = []
        for line in lines:
            row = []
            parts = line.split("&&")
            for part in parts:
                if "-" in part:
                    title, url = part.split("-", 1)
                    row.append({"text": title.strip(), "url": url.strip()})
            if row:
                buttons.append(row)
        if buttons:
            if user_state == "waiting_for_buttons":
                if not data["posts"]:
                    data["posts"].append({"photos": [], "text": "", "buttons": buttons})
                else:
                    data["posts"][-1]["buttons"] = buttons
            elif user_state.startswith("editing_buttons_"):
                index = int(user_state.split("_")[-1])
                if 0 <= index < len(data["posts"]):
                    data["posts"][index]["buttons"] = buttons
            data["state"].pop(str(chat_id), None)
            await save_json(DATA_FILE, data)
            await update.message.reply_text("✅ 按钮已保存。", reply_markup=back_to_content_menu())
        else:
            await update.message.reply_text("⚠️ 按钮格式无效，请按要求输入。", reply_markup=back_to_content_menu())

    elif user_state and user_state == "waiting_for_tag":
        text = update.message.text.strip()
        lines = text.split("\n")
        new_tags = [line.strip() for line in lines if line.strip().startswith("#") and line.strip() not in data["tags"]]
        if new_tags:
            data["tags"].extend(new_tags)
            await save_json(DATA_FILE, data)
            keyboard = [
                [InlineKeyboardButton("📑 查看已添加标签", callback_data="view_tags")],
                [InlineKeyboardButton("⬅ 返回主菜单", callback_data="main_menu")]
            ]
            await update.message.reply_text(f"✅ 已添加标签：{', '.join(new_tags)}", reply_markup=InlineKeyboardMarkup(keyboard))
        else:
            await update.message.reply_text("⚠️ 没有有效的标签添加（需以 # 开头且不重复）。", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅ 返回主菜单", callback_data="main_menu")]]))
        data["state"].pop(chat_id, None)

    elif update.message.forward_from_chat:
        chat = update.message.forward_from_chat
        channel_id = str(chat.id)  # 使用数字 ID
        logger.info(f"收到转发消息，尝试添加频道 ID: {channel_id}")
        if channel_id not in data["channels"]:
            data["channels"].append(channel_id)
            try:
                await save_json(DATA_FILE, data)
                logger.info(f"频道 {channel_id} 已保存到 data.json")
                await update.message.reply_text(f"✅ 已添加频道：{channel_id}", reply_markup=back_to_channel_menu())
            except Exception as e:
                logger.error(f"保存频道 {channel_id} 失败：{e}")
                await update.message.reply_text(f"⚠️ 添加频道 {channel_id} 失败，请重试。", reply_markup=back_to_channel_menu())
        else:
            await update.message.reply_text(f"⚠️ 频道 {channel_id} 已存在。", reply_markup=back_to_channel_menu())

# 发送帖子逻辑
async def send_post(bot: Bot):
    logger.info("触发 send_post")
    data = await load_data()
    send_state = await load_send_state()

    if not data["channels"]:
        logger.warning("没有可发送的频道")
        return
    if not data["posts"]:
        logger.warning("没有可发送的帖子")
        return

    current_time = time.time()
    last_round_time = send_state.get("last_round_time", 0)
    if current_time - last_round_time < 60:  # 60秒速率限制
        logger.info(f"速率限制：需等待 {60 - (current_time - last_round_time):.1f} 秒")
        return

    def escape_markdown_v2(text: str) -> str:
        if not text:
            return ""
        escape_chars = ['_', '*', '[', ']', '(', ')', '~', '`', '>', '#', '+', '-', '=', '|', '{', '}', '.', '!']
        return ''.join(f'\\{ch}' if ch in escape_chars else ch for ch in text)

    current_index = send_state["current_post_index"]
    if current_index >= len(data["posts"]):
        current_index = 0

    post = data["posts"][current_index]
    base_text = str(post.get("text", "")).strip()
    photos = post.get("photos", [])
    buttons = post.get("buttons", [
        [{"text": "▶️点击观看此影片", "url": "https://t.me/ttttt04111"}],
        [{"text": "🆕MK在线投注", "url": "https://t.me/MK7777bot"}]
    ])

    if not base_text and not photos:
        logger.warning(f"帖子 {current_index + 1} 无内容，跳过...")
        return

    reply_markup = InlineKeyboardMarkup([
        [InlineKeyboardButton(b["text"], url=b["url"]) for b in row]
        for row in buttons
    ])
    tags = data["tags"]
    num_tags = min(4, len(tags))
    random_tags = random.sample(tags, num_tags) if num_tags > 0 else []
    tag_line = "\n" + " ".join([f"||{escape_markdown_v2(t)}||" for t in random_tags]) if random_tags else ""
    full_text = escape_markdown_v2(base_text) + tag_line

    max_retries = 5
    for channel in data["channels"]:
        for attempt in range(max_retries):
            try:
                if photos:
                    await bot.send_photo(
                        chat_id=channel,
                        photo=random.choice(photos),
                        caption=full_text or None,
                        reply_markup=reply_markup,
                        parse_mode='MarkdownV2'
                    )
                elif full_text:
                    await bot.send_message(
                        chat_id=channel,
                        text=full_text,
                        reply_markup=reply_markup,
                        parse_mode='MarkdownV2'
                    )
                logger.info(f"成功发送帖子 {current_index + 1} 至 {channel}")
                break
            except Exception as e:
                error_msg = str(e).lower()
                if "flood control exceeded" in error_msg:
                    retry_delay = float(error_msg.split("retry in ")[1].split(" seconds")[0])
                    logger.warning(f"{channel} 触发速率限制，将在 {retry_delay} 秒后重试")
                    await asyncio.sleep(retry_delay)
                elif any(x in error_msg for x in ["chat not found", "permission"]):
                    logger.error(f"移除无效频道 {channel}：{e}")
                    data["channels"].remove(channel)
                    await save_json(DATA_FILE, data)
                    break
                elif "bad request" in error_msg:
                    logger.warning(f"{channel} 请求错误，尝试发送纯文本")
                    try:
                        await bot.send_message(chat_id=channel, text=full_text.replace("\\", ""), reply_markup=reply_markup)
                        logger.info(f"成功发送纯文本至 {channel}")
                        break
                    except Exception as e2:
                        logger.error(f"纯文本发送失败 {channel}：{e2}")
                        break
                else:
                    logger.error(f"发送至 {channel} 失败：{e}")
                    break
            await asyncio.sleep(1)  # 频道间短暂停顿

    send_state["current_post_index"] = (current_index + 1) % len(data["posts"])
    send_state["last_round_time"] = current_time
    await save_json(STATE_FILE, send_state)
    logger.info("完成一轮发送，60秒后进行下一轮")

# 手动测试命令
async def send_test(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await send_post(context.bot)
    await update.message.reply_text("测试发送已触发。")

def main():
    # 检查文件权限
    for file_path in [DATA_FILE, STATE_FILE]:
        directory = os.path.dirname(file_path) or '.'
        if not os.access(directory, os.W_OK):
            logger.error(f"无权写入目录：{directory}")
            raise PermissionError(f"无权写入 {directory}")
        logger.info(f"目录 {directory} 可写")

    application = ApplicationBuilder().token(TOKEN).build()

    application.add_handler(CommandHandler("start", start))
    application.add_handler(CallbackQueryHandler(button_callback))
    application.add_handler(MessageHandler(filters.TEXT | filters.PHOTO | filters.FORWARDED, handle_message))
    application.add_handler(CommandHandler("send", send_test))

    scheduler = BackgroundScheduler(timezone=TIMEZONE)
    scheduler.add_job(lambda: asyncio.run(send_post(application.bot)), trigger='interval', minutes=POST_INTERVAL_MINUTES)
    scheduler.start()

    logger.info("机器人已启动")
    application.run_polling()

if __name__ == '__main__':
    main()