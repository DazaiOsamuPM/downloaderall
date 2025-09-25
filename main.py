# ===========================================
#  Video Downloader Bot (VIP Version) - ПРОФЕССИОНАЛЬНАЯ ВЕРСИЯ
#  Автор: @frastiel (Telegram)
#  Aiogram v3, Python 3.11
#  Включает:
#   - Асинхронную загрузку (несколько загрузок одновременно)
#   - Кэширование (экономия времени и трафика)
#   - Поддержку Instagram, Facebook, Twitter/X, VK, Reddit, Pinterest, Dailymotion, Vimeo, SoundCloud
#   - Улучшенный визуальный прогресс-бар с интерактивом
#   - Кнопку "Повторить загрузку"
#   - Историю загрузок
#   - Автоочистку кэша
#   - Фильтрацию сообщений (бот отвечает на команды и ссылки в группах)
#   - Улучшенная обработка ошибок с рекомендациями
# ===========================================
from __future__ import annotations
import os
import re
import json
import asyncio
import tempfile
import logging
import shutil
import time
import sqlite3
import uuid
import subprocess
import aiohttp
from datetime import datetime, timedelta
from functools import partial
from typing import Dict, Optional, List, Tuple, Any
from urllib.parse import urlparse, urlunparse
import requests
from dotenv import load_dotenv
from yt_dlp import YoutubeDL
from yt_dlp.utils import DownloadError, UnsupportedError
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command, BaseFilter
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.exceptions import TelegramAPIError, TelegramBadRequest
from aiogram.enums import ChatAction
from aiogram.types import FSInputFile
from aiohttp import web

# ---- config ----
load_dotenv()
BOT_TOKEN = os.getenv("BOT_TOKEN")
if not BOT_TOKEN:
    raise SystemExit("Установите переменную окружения BOT_TOKEN (или добавьте .env)")

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# ---- bot & dispatcher ----
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

# ---- state ----
PENDING_LINKS: Dict[int, str] = {}
ACTIVE_DOWNLOADS: Dict[int, Dict[str, Any]] = {}  # Хранит информацию о текущих загрузках

# ---- regex ----
URL_RE = re.compile(r"https?://[^\s<>'\"()\[\]{}]+", re.IGNORECASE)

# расширенный набор паттернов для TikTok
TIKTOK_ANY_RE = re.compile(
    r"(?:https?://)?(?:vm\.tiktok\.com|vt\.tiktok\.com|m\.tiktok\.com|www\.tiktok\.com|tiktok\.com)"
    r"/(?:@[\w\.]+/video/\d+|video/\d+|v/\d+|t/\w+|share/\w+|embed/\d+|tag/[^/]+|hashtag/[^/]+|music/\d+|@[\w\.]+)",
    re.IGNORECASE,
)

# регулярные выражения для Instagram и Facebook
INSTAGRAM_RE = re.compile(
    r"(?:https?://)?(?:www\.)?instagram\.com/"
    r"(?:p/[^/]+|reel/[^/]+|tv/[^/]+|stories/[^/]+/[^/]+)",
    re.IGNORECASE
)
FACEBOOK_RE = re.compile(
    r"(?:https?://)?(?:www\.)?facebook\.com/"
    r"(?:[^/]+/videos/\d+|video\.php\?v=\d+|watch/\?v=\d+)",
    re.IGNORECASE
)

# регулярные выражения для Twitter/X
TWITTER_RE = re.compile(
    r"(?:https?://)?(?:twitter\.com|x\.com)/[^/]+/status/\d+",
    re.IGNORECASE
)

# регулярные выражения для VK (обновлено!)
VK_RE = re.compile(
    r"(?:https?://)?(?:vk\.com|m\.vkvideo\.ru)/"
    r"(?:video-?\d+_\d+|clip-?\d+_\d+|wall-?\d+_\d+|z=video-?\d+_\d+)",
    re.IGNORECASE
)

# регулярные выражения для Reddit
REDDIT_RE = re.compile(
    r"(?:https?://)?(?:www\.)?reddit\.com/"
    r"(?:r/[^/]+/comments/[\w]+/[^/]+/[\w]+|comments/[\w]+/[^/]+/[\w]+)",
    re.IGNORECASE
)

# регулярные выражения для Pinterest (обновлено!)
PINTEREST_RE = re.compile(
    r"(?:https?://)?(?:pinterest\.(?:com|ru|ca|de|fr|jp|uk|it|es|nl|se|pl|br|mx|co\.uk)|pin\.it)/[\w/-]+",
    re.IGNORECASE
)

# регулярные выражения для Dailymotion
DAILYMOTION_RE = re.compile(
    r"(?:https?://)?(?:www\.)?dailymotion\.com/(?:video/[\w-]+|embed/video/[\w-]+)",
    re.IGNORECASE
)

# регулярные выражения для Vimeo
VIMEO_RE = re.compile(
    r"(?:https?://)?(?:www\.)?vimeo\.com/"
    r"(?:\d+|album/\d+/video/\d+|channels/[^/]+/\d+|ondemand/[^/]+/\d+)",
    re.IGNORECASE
)

# регулярные выражения для SoundCloud
SOUNDCLOUD_RE = re.compile(
    r"(?:https?://)?(?:www\.)?soundcloud\.com/[^/]+/(?:[^/]+|sets/[^/]+)",
    re.IGNORECASE
)

# регулярные выражения для прямых ссылок на файлы
DIRECT_FILE_RE = re.compile(
    r"(?:https?://)?[^\s]+\.(?:mp4|mkv|webm|avi|mov|wmv|flv|mp3|m4a|wav|aac|ogg)"
    r"(?:\?[^#\s]*)?(?:#[^\s]*)?$",
    re.IGNORECASE
)

# короткие/редирект домены (добавлен pin.it)
SHORTENER_DOMAINS = (
    "t.co", "t.me", "bit.ly", "tinyurl.com", "lnkd.in", "goo.gl", "rb.gy",
    "vm.tiktok.com", "vt.tiktok.com", "m.tiktok.com", "www.tiktok.com", "tiktok.com",
    "x.com", "twitter.com", "vk.com", "m.vkvideo.ru", "reddit.com", "pinterest.com", "pin.it",
    "dailymotion.com", "vimeo.com", "soundcloud.com"
)

# YouTube patterns
YOUTUBE_VIDEO_RE = re.compile(
    r"(?:https?://)?(?:youtu\.be/|youtube\.com/(?:watch\?v=|shorts/|embed/|v/))[\w-]+",
    re.IGNORECASE
)

# ---- yt-dlp base opts ----
YTDL_BASE_OPTS = {"nocheckcertificate": True, "quiet": True, "no_warnings": True}

# ===== ТИПЫ ОШИБОК И СИСТЕМА ОБРАБОТКИ =====
class DownloadErrorType:
    """Типы ошибок для классификации"""
    UNSUPPORTED_URL = "unsupported_url"
    NETWORK_ERROR = "network_error"
    FILE_TOO_LARGE = "file_too_large"
    INTERNAL_ERROR = "internal_error"
    RATE_LIMITED = "rate_limited"
    URL_NOT_FOUND = "url_not_found"
    NO_VIDEO_IN_POST = "no_video_in_post"
    DIRECT_FILE_DOWNLOAD = "direct_file_download"

class ErrorManager:
    def __init__(self, default_lang="ru"):
        self.default_lang = default_lang
        self.error_messages = {
            "ru": {
                DownloadErrorType.UNSUPPORTED_URL: {
                    "title": "🔗 Не поддерживаемый URL",
                    "description": "К сожалению, я не могу обработать этот URL. Убедитесь, что:",
                    "details": [
                        "• Это прямая ссылка на видео (не профиль или хештег)",
                        "• Ссылка ведет на поддерживаемую платформу (YouTube, TikTok, Instagram, Facebook, Twitter/X, VK, Reddit, Pinterest, Dailymotion, Vimeo, SoundCloud)",
                        "• Ссылка не содержит ошибок в написании"
                    ],
                    "example": "Пример правильной ссылки: https://x.com/username/status/123456789"
                },
                DownloadErrorType.NETWORK_ERROR: {
                    "title": "🌐 Проблемы с сетью",
                    "description": "Не удалось загрузить видео из-за временных проблем с сетью. Попробуйте:",
                    "details": [
                        "1. Повторить через несколько минут",
                        "2. Проверить статус сервисов",
                        "3. Использовать команду /retry для повторной попытки"
                    ],
                    "additional": "Эти ошибки обычно временные и решаются автоматически"
                },
                DownloadErrorType.FILE_TOO_LARGE: {
                    "title": "💾 Файл слишком большой",
                    "description": "Видео превышает лимит Telegram (2 ГБ). Доступные варианты:",
                    "details": [
                        "1. Выберите аудио вместо видео (меньше по размеру)",
                        "2. Используйте кнопку 'Скачать через transfer.sh' для получения ссылки",
                        "3. Обрежьте видео до нужного фрагмента с помощью команды /trim"
                    ]
                },
                DownloadErrorType.RATE_LIMITED: {
                    "title": "⏱️ Слишком много запросов",
                    "description": "Вы достигли лимита запросов. Пожалуйста:",
                    "details": [
                        "1. Подождите 5 минут перед следующей попыткой",
                        "2. Не отправляйте одинаковые запросы подряд",
                        "3. Для срочных загрузок используйте прямую ссылку"
                    ],
                    "additional": "Лимиты существуют для защиты сервиса от перегрузок"
                },
                DownloadErrorType.INTERNAL_ERROR: {
                    "title": "⚙️ Внутренняя ошибка",
                    "description": "Произошла непредвиденная ошибка. Это может быть связано с:",
                    "details": [
                        "• Временными проблемами на сервере",
                        "• Изменениями в API платформы",
                        "• Внутренними ошибками бота"
                    ],
                    "additional": "Пожалуйста, попробуйте позже или сообщите об ошибке: @frastiel"
                },
                DownloadErrorType.URL_NOT_FOUND: {
                    "title": "❌ Ссылка не найдена",
                    "description": "Видео по этой ссылке не найдено. Возможно:",
                    "details": [
                        "1. Видео было удалено автором",
                        "2. Ссылка содержит ошибку",
                        "3. Видео недоступно в вашем регионе"
                    ],
                    "additional": "Проверьте ссылку и попробуйте снова"
                },
                DownloadErrorType.NO_VIDEO_IN_POST: {
                    "title": "🖼️ Нет видео в публикации",
                    "description": "В этой публикации не найдено видео. Возможно, там только:",
                    "details": [
                        "• Текстовое сообщение",
                        "• Фотография или картинка",
                        "• GIF-анимация (не поддерживается)",
                        "• Ссылка на внешний ресурс"
                    ],
                    "additional": "Попробуйте найти другой пост или пин с видео."
                },
                DownloadErrorType.DIRECT_FILE_DOWNLOAD: {
                    "title": "📥 Прямая загрузка файла",
                    "description": "Я обнаружил прямую ссылку на файл. Начинаю загрузку:",
                    "additional": "Это может занять некоторое время в зависимости от размера файла и скорости сети."
                }
            },
            "en": {
                DownloadErrorType.UNSUPPORTED_URL: {
                    "title": "🔗 Unsupported URL",
                    "description": "Unfortunately, I cannot process this URL. Make sure that:",
                    "details": [
                        "• It's a direct link to a video (not a profile or hashtag)",
                        "• The link points to a supported platform (YouTube, TikTok, Instagram, Facebook, Twitter/X, VK, Reddit, Pinterest, Dailymotion, Vimeo, SoundCloud)",
                        "• The link doesn't contain typing errors"
                    ],
                    "example": "Example of a correct link: https://x.com/username/status/123456789"
                },
                DownloadErrorType.NETWORK_ERROR: {
                    "title": "🌐 Network issues",
                    "description": "Failed to download video due to temporary network problems. Try:",
                    "details": [
                        "1. Retry in a few minutes",
                        "2. Check service status",
                        "3. Use the /retry command for another attempt"
                    ],
                    "additional": "These errors are usually temporary and resolve automatically"
                },
                DownloadErrorType.FILE_TOO_LARGE: {
                    "title": "💾 File too large",
                    "description": "Video exceeds Telegram limit (2 GB). Available options:",
                    "details": [
                        "1. Choose audio instead of video (smaller size)",
                        "2. Use 'Download via transfer.sh' button to get a link",
                        "3. Trim video to needed fragment using /trim command"
                    ]
                },
                DownloadErrorType.RATE_LIMITED: {
                    "title": "⏱️ Too many requests",
                    "description": "You've reached the request limit. Please:",
                    "details": [
                        "1. Wait 5 minutes before next attempt",
                        "2. Don't send identical requests in a row",
                        "3. For urgent downloads use direct link"
                    ],
                    "additional": "Limits exist to protect the service from overload"
                },
                DownloadErrorType.INTERNAL_ERROR: {
                    "title": "⚙️ Internal error",
                    "description": "An unexpected error occurred. It may be related to:",
                    "details": [
                        "• Temporary server issues",
                        "• Changes in platform API",
                        "• Bot internal errors"
                    ],
                    "additional": "Please try again later or report the error: @frastiel"
                },
                DownloadErrorType.URL_NOT_FOUND: {
                    "title": "❌ URL not found",
                    "description": "Video not found by this link. Possibly:",
                    "details": [
                        "1. Video was deleted by author",
                        "2. Link contains an error",
                        "3. Video is unavailable in your region"
                    ],
                    "additional": "Check the link and try again"
                },
                DownloadErrorType.NO_VIDEO_IN_POST: {
                    "title": "🖼️ No video in post",
                    "description": "No video was found in this post. It might contain only:",
                    "details": [
                        "• Text message",
                        "• Photo or image",
                        "• GIF animation (not supported)",
                        "• Link to external resource"
                    ],
                    "additional": "Try finding another post or pin with video content."
                },
                DownloadErrorType.DIRECT_FILE_DOWNLOAD: {
                    "title": "📥 Direct file download",
                    "description": "I detected a direct link to a file. Starting download:",
                    "additional": "This may take some time depending on file size and network speed."
                }
            }
        }

    def get_error_type(self, error: Exception, url: str = None) -> DownloadErrorType:
        """Определяет тип ошибки на основе исключения"""
        error_msg = str(error).lower()
        if "unsupported url" in error_msg or isinstance(error, UnsupportedError):
            return DownloadErrorType.UNSUPPORTED_URL
        elif "network" in error_msg or "timeout" in error_msg or "connection" in error_msg:
            return DownloadErrorType.NETWORK_ERROR
        elif "file too large" in error_msg or "exceeds file size limit" in error_msg:
            return DownloadErrorType.FILE_TOO_LARGE
        elif "429" in error_msg or "rate limit" in error_msg:
            return DownloadErrorType.RATE_LIMITED
        elif "not found" in error_msg or "unable to download" in error_msg:
            return DownloadErrorType.URL_NOT_FOUND
        elif "no video could be found" in error_msg:
            return DownloadErrorType.NO_VIDEO_IN_POST
        elif "direct file" in error_msg or "direct link" in error_msg:
            return DownloadErrorType.DIRECT_FILE_DOWNLOAD
        else:
            return DownloadErrorType.INTERNAL_ERROR

    def format_error_message(self, error_type: DownloadErrorType, lang: str = None, url: str = None) -> str:
        """Форматирует сообщение об ошибке с использованием шаблона"""
        if lang is None or lang not in self.error_messages:
            lang = self.default_lang
        error_data = self.error_messages.get(lang, {}).get(error_type)
        if not error_data:
            # Если для данного языка нет сообщения, используем русский
            error_data = self.error_messages["ru"].get(error_type)
        if not error_data:
            return "⚠️ Произошла неизвестная ошибка. Пожалуйста, попробуйте позже."

        # Формируем сообщение
        message = f"<b>{error_data['title']}</b>\n"
        message += f"{error_data['description']}\n"
        for detail in error_data.get("details", []):
            message += f"{detail}\n"
        if "example" in error_data:
            message += f"\n<i>Пример:</i>\n<code>{error_data['example']}</code>"
        if "additional" in error_data:
            message += f"\nℹ️ {error_data['additional']}"

        # Добавляем кнопку для повторной попытки для некоторых типов ошибок
        if error_type in [DownloadErrorType.NETWORK_ERROR, DownloadErrorType.RATE_LIMITED, DownloadErrorType.URL_NOT_FOUND]:
            message += "\n🔄 Чтобы попробовать снова, нажмите кнопку ниже"
        return message

# Инициализация менеджера ошибок
error_manager = ErrorManager(default_lang="ru")

# ===== ФИЛЬТРЫ ДЛЯ СООБЩЕНИЙ =====
class GroupFilter(BaseFilter):
    """Фильтр для обработки сообщений:
    - В личных чатах обрабатываем все сообщения
    - В групповых чатах обрабатываем команды бота и сообщения со ссылками
    """
    def __init__(self, bot_username: str):
        self.bot_username = bot_username.lower()
        # Поддерживаемые домены
        self.supported_domains = [
            "tiktok.com", "vm.tiktok.com", "vt.tiktok.com", "m.tiktok.com",
            "youtube.com", "youtu.be", "instagram.com", "facebook.com",
            "twitter.com", "x.com", "vk.com", "m.vkvideo.ru", "reddit.com", "pinterest.com", "pin.it",
            "dailymotion.com", "vimeo.com", "soundcloud.com"
        ]

    def find_first_url(self, text: str) -> Optional[str]:
        if not text:
            return None
        m = URL_RE.search(text)
        return m.group(0) if m else None

    def is_supported_url(self, url: str) -> bool:
        if not url:
            return False
        url_low = url.lower()
        return any(domain in url_low for domain in self.supported_domains)

    async def __call__(self, message: types.Message) -> bool:
        # Личные сообщения всегда обрабатываем
        if message.chat.type == "private":
            logger.debug(f"Принято личное сообщение от {message.from_user.id}")
            return True

        # В групповых чатах проверяем:
        # 1. Является ли сообщение командой бота
        # 2. Содержит ли сообщение ссылку на поддерживаемую платформу

        # Проверяем, является ли сообщение командой бота
        if message.text and message.text.startswith("/"):
            command_parts = message.text.split()
            if command_parts:
                # Удаляем / и возможное упоминание бота
                command = command_parts[0][1:].split("@")[0]
                # Список поддерживаемых команд
                supported_commands = ["start", "help", "history", "addnews"]
                if command in supported_commands:
                    logger.debug(f"Найдена поддерживаемая команда в группе от {message.from_user.id}: /{command}")
                    return True

        # Проверяем наличие ссылок на поддерживаемые платформы
        url = self.find_first_url(message.text or "")
        if url and self.is_supported_url(url):
            logger.debug(f"Найдена поддерживаемая ссылка в группе от {message.from_user.id}")
            return True

        logger.debug(f"Игнорируется сообщение в группе от {message.from_user.id} (не команда и не ссылка)")
        return False

# ===== МЕНЕДЖЕР НАСТРОЕК ПОЛЬЗОВАТЕЛЕЙ =====
class UserSettings:
    DEFAULT_SETTINGS = {
        "default_format": "video",  # "video" или "audio"
        "max_concurrent_downloads": 3,
        "preferred_quality": "best",  # "best", "1080p", "720p", "480p"
        "auto_retry": True,
        "trim_enabled": False,
        "language": "ru",
        "notification_level": "important"  # "all", "important", "none"
    }

    def __init__(self, db_path="user_settings.db"):
        self.db_path = db_path
        self._init_db()

    def _init_db(self):
        """Инициализация базы данных для настроек пользователей"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS user_settings (
            user_id INTEGER PRIMARY KEY,
            settings TEXT NOT NULL,
            last_updated DATETIME DEFAULT CURRENT_TIMESTAMP
        )
        """)
        conn.commit()
        conn.close()

    def get_settings(self, user_id):
        """Получает настройки пользователя, создает запись если не существует"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT settings FROM user_settings WHERE user_id = ?", (user_id,))
        result = cursor.fetchone()
        
        if result:
            try:
                user_settings = json.loads(result[0])
                conn.close()
                return {**self.DEFAULT_SETTINGS, **user_settings}
            except Exception as e:
                logger.error(f"Error parsing user settings: {e}")
        else:
            # Создаем новую запись с настройками по умолчанию
            try:
                cursor.execute(
                    "INSERT INTO user_settings (user_id, settings) VALUES (?, ?)",
                    (user_id, json.dumps(self.DEFAULT_SETTINGS))
                )
                conn.commit()
            except Exception as e:
                logger.error(f"Error creating user settings: {e}")
            finally:
                conn.close()
        return self.DEFAULT_SETTINGS.copy()

    def update_setting(self, user_id, key, value):
        """Обновляет одну настройку пользователя"""
        if key not in self.DEFAULT_SETTINGS:
            return False
        settings = self.get_settings(user_id)
        settings[key] = value
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        try:
            cursor.execute(
                "INSERT OR REPLACE INTO user_settings (user_id, settings) VALUES (?, ?)",
                (user_id, json.dumps(settings))
            )
            conn.commit()
            return True
        except Exception as e:
            logger.error(f"Error updating user setting: {e}")
            return False
        finally:
            conn.close()
            
    def get_all_user_ids(self) -> List[int]:
        """Получает список всех пользователей, которые использовали бота"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT DISTINCT user_id FROM user_settings")
            result = cursor.fetchall()
            return [row[0] for row in result]
        except Exception as e:
            logger.error(f"Error getting all user IDs: {e}")
            return []
        finally:
            conn.close()

# Инициализация менеджера настроек
user_settings = UserSettings()

# ===== НОВЫЕ КЛАССЫ ДЛЯ УПРАВЛЕНИЯ ЗАГРУЗКАМИ =====
class DownloadManager:
    def __init__(self, max_concurrent=3):
        self.queue = asyncio.Queue()
        self.active_tasks: Dict[int, List[int]] = {}
        self.max_concurrent = max_concurrent
        self.lock = asyncio.Lock()
        self.processing = 0
        self.task_counter = 0
        asyncio.create_task(self._process_queue())

    async def _process_queue(self):
        """Основной процесс обработки очереди загрузок"""
        while True:
            try:
                callback_query, url, mode = await self.queue.get()
                user_id = callback_query.from_user.id

                # Проверяем, не превышен ли лимит для этого пользователя
                async with self.lock:
                    if user_id not in self.active_tasks:
                        self.active_tasks[user_id] = []
                    if len(self.active_tasks[user_id]) >= self.max_concurrent:
                        await callback_query.message.answer(
                            "Вы достигли лимита одновременных загрузок (3). "
                            "Пожалуйста, дождитесь завершения текущих загрузок."
                        )
                        self.queue.task_done()
                        continue

                # Добавляем в активные задачи
                self.task_counter += 1
                task_id = self.task_counter
                self.active_tasks[user_id].append(task_id)
                self.processing += 1

                # Сохраняем информацию о загрузке
                ACTIVE_DOWNLOADS[task_id] = {
                    "callback_query": callback_query,
                    "url": url,
                    "mode": mode,
                    "user_id": user_id,
                    "status": "processing",
                    "start_time": time.time()
                }

                # Запускаем загрузку в фоне
                asyncio.create_task(self._handle_download(callback_query, url, mode, user_id, task_id))
                self.queue.task_done()
            except Exception as e:
                logger.error(f"Error in download queue processor: {e}")
                if 'callback_query' in locals():
                    try:
                        await callback_query.message.answer(f"Ошибка обработки очереди: {str(e)}")
                    except:
                        pass
                if 'self' in locals():
                    self.queue.task_done()

    async def _handle_download(self, callback_query: types.CallbackQuery, url: str, mode: str, user_id: int, task_id: int):
        """Обработка отдельной загрузки (исправленная версия)"""
        try:
            # Проверяем кэш перед началом загрузки
            cached_file = cache_manager.get_cached_file(url, mode)
            if cached_file:
                await self._send_cached_file(callback_query, cached_file, mode)
                return

            # Если нет в кэше, начинаем загрузку
            target_chat_id = callback_query.message.chat.id
            status_msg = await bot.send_message(
                target_chat_id,
                f"Готовлюсь к скачиванию: {url}\n(Загрузка #{task_id})"
            )

            # Обновляем информацию о загрузке
            ACTIVE_DOWNLOADS.setdefault(task_id, {})
            ACTIVE_DOWNLOADS[task_id]["status"] = "downloading"
            ACTIVE_DOWNLOADS[task_id]["status_msg_id"] = status_msg.message_id
            ACTIVE_DOWNLOADS[task_id]["url"] = url
            ACTIVE_DOWNLOADS[task_id]["mode"] = mode
            ACTIVE_DOWNLOADS[task_id]["user_id"] = user_id
            ACTIVE_DOWNLOADS[task_id]["start_time"] = time.time()

            loop = asyncio.get_running_loop()
            progress_hook = make_progress_hook(loop, target_chat_id, status_msg.message_id, task_id)
            tempdir = tempfile.mkdtemp(prefix="tgdl_")
            filepath = None

            # Проверяем свободное место на диске
            if not has_enough_disk_space(tempdir, required_mb=500):
                try:
                    await bot.edit_message_text(
                        chat_id=target_chat_id,
                        message_id=status_msg.message_id,
                        text="⚠️ На сервере недостаточно места для загрузки. Попробуйте позже."
                    )
                except Exception:
                    # fallback
                    await callback_query.message.answer("⚠️ На сервере недостаточно места для загрузки. Попробуйте позже.")
                return

            # Проверяем, не слишком ли большой файл (ограничение 1 ГБ)
            try:
                head = requests.head(url, timeout=10)
                if 'content-length' in head.headers:
                    content_length = int(head.headers['content-length'])
                    if content_length > 1024 * 1024 * 1024:  # 1 ГБ
                        await bot.edit_message_text(
                            chat_id=target_chat_id,
                            message_id=status_msg.message_id,
                            text=f"❌ Файл слишком большой ({content_length/(1024*1024):.1f} MB). "
                                f"Максимальный размер: 1 ГБ."
                        )
                        ACTIVE_DOWNLOADS[task_id]["status"] = "failed"
                        return
            except Exception as e:
                logger.warning(f"Не удалось определить размер файла: {e}")

            # Выполняем скачивание (единую логику для direct / instagram / yt-dlp)
            try:
                if DIRECT_FILE_RE.search(url):
                    # прямая ссылка на файл
                    try:
                        await bot.edit_message_text(
                            chat_id=target_chat_id,
                            message_id=status_msg.message_id,
                            text="📥 Обнаружена прямая ссылка на файл. Начинаю загрузку..."
                        )
                    except Exception:
                        pass
                    # _download_direct_file у вас определён как async
                    filepath = await self._download_direct_file(url, tempdir)
                elif "instagram.com" in url.lower():
                    try:
                        await bot.edit_message_text(
                            chat_id=target_chat_id,
                            message_id=status_msg.message_id,
                            text="📥 Скачиваю видео с Instagram..."
                        )
                    except Exception:
                        pass
                    # скачиваем в потоковом исполнении, т.к. download_instagram_video блокирующая
                    async with aiohttp.ClientSession() as session:
                        filepath = await download_instagram_video_async(url, tempdir, mode, session=session)
                else:
                    # Используем yt-dlp для всех остальных платформ (с прогресс-хуком)
                    func = partial(ytdl_download, url, tempdir, mode, progress_hook)
                    filepath = await asyncio.wait_for(loop.run_in_executor(None, func), timeout=420)

                # Проверяем, что файл получен
                if not filepath or not os.path.exists(filepath):
                    raise FileNotFoundError("Файл не найден после загрузки.")

                # Обновляем запись о задаче
                ACTIVE_DOWNLOADS[task_id]["filepath"] = filepath
                ACTIVE_DOWNLOADS[task_id]["status"] = "saving"

                # Сохраняем в кэш (не критично — если упадёт, просто логируем)
                try:
                    cache_manager.add_to_cache(url, filepath, mode)
                except Exception as e:
                    logger.warning(f"Не удалось добавить в кэш: {e}")

                # Добавляем в историю (не критично)
                try:
                    history_manager.add_to_history(user_id, url, mode)
                except Exception as e:
                    logger.warning(f"Не удалось добавить в историю: {e}")

                # Отправляем файл пользователю
                await self._send_file(callback_query, url, filepath, mode, status_msg.message_id)
                ACTIVE_DOWNLOADS[task_id]["status"] = "done"
                ACTIVE_DOWNLOADS[task_id]["end_time"] = time.time()

            except Exception as e:
                ACTIVE_DOWNLOADS[task_id]["status"] = "failed"
                logger.exception("Ошибка при скачивании/обработке файла")
                await self._handle_download_error(callback_query, e, url, status_msg.message_id)

            finally:
                # Чистим временную директорию
                try:
                    if tempdir and os.path.isdir(tempdir):
                        shutil.rmtree(tempdir)
                except Exception:
                    pass

        except Exception as e:
            logger.exception("Ошибка при обработке загрузки (внешняя)")
            try:
                await callback_query.message.answer(f"Произошла ошибка: {str(e)}")
            except Exception:
                pass

        finally:
            # Удаляем задачу из активных
            async with self.lock:
                if user_id in self.active_tasks:
                    if task_id in self.active_tasks[user_id]:
                        self.active_tasks[user_id].remove(task_id)
                    if not self.active_tasks[user_id]:
                        del self.active_tasks[user_id]
                # Защита от отрицательных значений
                if self.processing > 0:
                    self.processing -= 1
                # Удаляем информацию о загрузке
                if task_id in ACTIVE_DOWNLOADS:
                    del ACTIVE_DOWNLOADS[task_id]


    async def _download_direct_file(self, url: str, tempdir: str) -> str:
        """Скачивание прямой ссылки на файл"""
        filename = os.path.basename(urlparse(url).path) or "downloaded_file"
        if not any(filename.endswith(ext) for ext in [".mp4", ".mp3", ".mkv", ".webm", ".avi", ".mov", ".wmv", ".flv", ".m4a", ".wav", ".aac", ".ogg"]):
            filename += ".mp4"  # Добавляем расширение по умолчанию
        
        filepath = os.path.join(tempdir, filename)
        
        # Скачиваем файл
        with requests.get(url, stream=True, timeout=300) as r:
            r.raise_for_status()
            with open(filepath, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
        
        return filepath

    async def add_download(self, callback_query: types.CallbackQuery, url: str, mode: str):
        """Добавить загрузку в очередь"""
        await self.queue.put((callback_query, url, mode))
        return True

    async def _send_cached_file(self, callback_query: types.CallbackQuery, file_path: str, mode: str):
        """Отправка файла из кэша"""
        try:
            target_chat_id = callback_query.message.chat.id
            await callback_query.message.answer("Найдено в кэше, отправляю...")
            if mode == "audio":
                await bot.send_chat_action(target_chat_id, action=ChatAction.UPLOAD_DOCUMENT)
                audio = FSInputFile(file_path)
                await bot.send_audio(
                    target_chat_id,
                    audio,
                    caption=f"{os.path.basename(file_path)}"
                )
            else:
                await bot.send_chat_action(target_chat_id, action=ChatAction.UPLOAD_VIDEO)
                video = FSInputFile(file_path)
                await bot.send_video(
                    target_chat_id,
                    video,
                    caption=f"{os.path.basename(file_path)}"
                )
        except Exception as e:
            logger.error(f"Ошибка при отправке кэшированного файла: {e}")
            # Если кэшированный файл поврежден, удаляем его из кэша
            cache_manager.remove_from_cache(file_path)
            # И пробуем загрузить заново
            await self.add_download(callback_query, callback_query.message.text, mode)

    async def _send_file(self, callback_query: types.CallbackQuery, url: str, filepath: str, mode: str, status_msg_id: int):
        """Отправка файла после загрузки с указанием источника и ссылки"""
        try:
            target_chat_id = callback_query.message.chat.id
            # Генерируем уникальный короткий ID для этой ссылки
            retry_id = str(uuid.uuid4())[:8]
            # Сохраняем соответствие ID -> URL
            RETRY_LINKS[retry_id] = (url, time.time())
            # Кнопка "Повторить загрузку" с коротким ID вместо полного URL
            retry_kb = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🔄 Повторить загрузку", callback_data=f"retry:{mode}:{retry_id}")]
            ])
            await bot.edit_message_text(
                chat_id=target_chat_id,
                message_id=status_msg_id,
                text="Отправляю файл..."
            )
            stat = os.stat(filepath)
            size_mb = stat.st_size / (1024 * 1024)
            # Определяем источник видео
            source = "Неизвестно"
            url_low = url.lower()
            if "youtube.com" in url_low or "youtu.be" in url_low:
                source = "YouTube"
            elif "tiktok.com" in url_low:
                source = "TikTok"
            elif "instagram.com" in url_low:
                source = "Instagram"
            elif "facebook.com" in url_low:
                source = "Facebook"
            elif "twitter.com" in url_low or "x.com" in url_low:
                source = "Twitter/X"
            elif "vk.com" in url_low or "m.vkvideo.ru" in url_low:
                source = "VK"
            elif "reddit.com" in url_low:
                source = "Reddit"
            elif "pinterest.com" in url_low or "pin.it" in url_low:
                source = "Pinterest"
            elif "dailymotion.com" in url_low:
                source = "Dailymotion"
            elif "vimeo.com" in url_low:
                source = "Vimeo"
            elif "soundcloud.com" in url_low:
                source = "SoundCloud"
            elif DIRECT_FILE_RE.search(url):
                source = "Прямая ссылка"

            if size_mb > 48:
                await bot.edit_message_text(
                    chat_id=target_chat_id,
                    message_id=status_msg_id,
                    text=f"Файл большой ({size_mb:.1f} MB). Загружаю на облачный сервис..."
                )
                link = await upload_to_multiple_services(filepath)
                if link:
                    await bot.edit_message_text(
                        chat_id=target_chat_id,
                        message_id=status_msg_id,
                        text=(
                            f"Файл превышает лимит Telegram ({size_mb:.1f} MB).\n"
                            f"Ссылка: {link}\n"
                            f"📌 Источник: {source}\n"
                            f"🔗 Оригинальная ссылка: {url}"
                        ),
                        reply_markup=retry_kb,
                        disable_web_page_preview=True
                    )
                else:
                    await bot.edit_message_text(
                        chat_id=target_chat_id,
                        message_id=status_msg_id,
                        text=(
                            f"Не удалось загрузить файл ни на один сервис.\n"
                            f"Попробуйте позже или используйте другой источник.\n"
                            f"📌 Источник: {source}\n"
                            f"🔗 Оригинальная ссылка: {url}"
                        ),
                        reply_markup=retry_kb,
                        disable_web_page_preview=True
                    )
            else:
                caption = f"📌 Источник: {source}\n🔗 {url}"
                if mode == "audio":
                    await bot.send_chat_action(target_chat_id, action=ChatAction.UPLOAD_DOCUMENT)
                    audio = FSInputFile(filepath)
                    await bot.send_audio(
                        target_chat_id,
                        audio,
                        caption=caption
                    )
                else:
                    await bot.send_chat_action(target_chat_id, action=ChatAction.UPLOAD_VIDEO)
                    video = FSInputFile(filepath)
                    await bot.send_video(
                        target_chat_id,
                        video,
                        caption=caption
                    )
                await bot.edit_message_text(
                    chat_id=target_chat_id,
                    message_id=status_msg_id,
                    text=(
                        f"✅ Готово — отправлено ({size_mb:.1f} MB).\n"
                        f"📌 Источник: {source}\n"
                        f"🔗 Оригинальная ссылка: {url}"
                    ),
                    reply_markup=retry_kb,
                    disable_web_page_preview=True
                )
        except Exception as e:
            logger.exception("Ошибка при отправке файла")
            await bot.edit_message_text(
                chat_id=target_chat_id,
                message_id=status_msg_id,
                text=f"Ошибка при отправке: {str(e)}"
            )

    async def _handle_download_error(self, callback_query: types.CallbackQuery, error: Exception, url: str, status_msg_id: int):
        """Улучшенная обработка ошибок загрузки"""
        target_chat_id = callback_query.message.chat.id
        user_id = callback_query.from_user.id

        # Определяем тип ошибки
        error_type = error_manager.get_error_type(error, url)
        logger.warning(f"Ошибка типа {error_type} для пользователя {user_id}: {str(error)}")

        # Формируем сообщение
        lang = user_settings.get_settings(user_id)["language"]
        error_message = error_manager.format_error_message(error_type, lang, url)

        # Создаем клавиатуру с действиями
        action_kb = None
        if error_type in [DownloadErrorType.NETWORK_ERROR, DownloadErrorType.RATE_LIMITED, DownloadErrorType.URL_NOT_FOUND]:
            action_kb = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🔄 Повторить загрузку", callback_data=f"retry:auto:{url}")]
            ])

        # Отправляем сообщение
        try:
            await bot.edit_message_text(
                chat_id=target_chat_id,
                message_id=status_msg_id,
                text=error_message,
                reply_markup=action_kb,
                parse_mode="HTML"
            )
        except Exception as e:
            logger.error(f"Не удалось отправить сообщение об ошибке: {e}")
            # Попробуем отправить без разметки
            try:
                await bot.edit_message_text(
                    chat_id=target_chat_id,
                    message_id=status_msg_id,
                    text=f"Ошибка при загрузке: {str(error)[:1000]}",
                    reply_markup=action_kb
                )
            except Exception as e2:
                logger.error(f"Критическая ошибка при отправке сообщения: {e2}")

# ===== МЕНЕДЖЕР КЭША =====
class CacheManager:
    def __init__(self, cache_dir="downloads", db_path="cache.db"):
        self.cache_dir = cache_dir
        self.db_path = db_path
        os.makedirs(cache_dir, exist_ok=True)
        self._init_db()
        # Запускаем фоновую задачу для автоочистки
        asyncio.create_task(self._auto_cleanup_task())

    def _init_db(self):
        """Инициализация SQLite базы данных для кэша"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS cache (
            url TEXT PRIMARY KEY,
            file_path TEXT NOT NULL,
            file_type TEXT NOT NULL,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
        )
        """)
        conn.commit()
        conn.close()

    def get_cached_file(self, url: str, file_type: str) -> Optional[str]:
        """Получить путь к кэшированному файлу"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT file_path FROM cache WHERE url = ? AND file_type = ?", (url, file_type))
        result = cursor.fetchone()
        conn.close()
        if result and os.path.exists(result[0]):
            return result[0]
        return None

    def add_to_cache(self, url: str, file_path: str, file_type: str) -> bool:
        """Добавить файл в кэш"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        try:
            # Убедимся, что файл существует перед добавлением в кэш
            if not os.path.exists(file_path):
                return False
            # Создаем копию файла в директории кэша
            filename = os.path.basename(file_path)
            cache_path = os.path.join(self.cache_dir, filename)
            # Если файл уже в кэше, просто обновляем запись
            if os.path.abspath(file_path) == os.path.abspath(cache_path):
                pass
            else:
                # Если файл уже существует в кэше, удаляем старую версию
                if os.path.exists(cache_path):
                    os.remove(cache_path)
                shutil.copy2(file_path, cache_path)
            cursor.execute(
                "INSERT OR REPLACE INTO cache (url, file_path, file_type) VALUES (?, ?, ?)",
                (url, cache_path, file_type)
            )
            conn.commit()
            return True
        except Exception as e:
            logger.error(f"Ошибка добавления в кэш: {e}")
            return False
        finally:
            conn.close()

    def remove_from_cache(self, file_path: str) -> bool:
        """Удалить файл из кэша"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        try:
            cursor.execute("DELETE FROM cache WHERE file_path = ?", (file_path,))
            conn.commit()
            # Удаляем физический файл, если он существует
            if os.path.exists(file_path):
                os.remove(file_path)
            return True
        except Exception as e:
            logger.error(f"Ошибка удаления из кэша: {e}")
            return False
        finally:
            conn.close()

    def cleanup_old_files_by_hours(self, hours=6) -> int:
        """Удаление файлов старше указанного количества часов (было дней)"""
        cutoff = datetime.now() - timedelta(hours=hours)
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("DELETE FROM cache WHERE timestamp < ?", (cutoff,))
        deleted = cursor.rowcount
        conn.commit()
        conn.close()
        # Также проверяем физические файлы
        if deleted > 0:
            self._cleanup_orphaned_files()
        return deleted

    def _cleanup_orphaned_files(self):
        """Удаление файлов, которые есть на диске, но отсутствуют в базе"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT file_path FROM cache")
        cached_files = set(row[0] for row in cursor.fetchall())
        conn.close()
        # Проверяем каждый файл в папке кэша
        for filename in os.listdir(self.cache_dir):
            file_path = os.path.join(self.cache_dir, filename)
            if os.path.isfile(file_path) and file_path not in cached_files:
                try:
                    os.remove(file_path)
                    logger.info(f"Удален орфанный файл кэша: {file_path}")
                except Exception as e:
                    logger.error(f"Не удалось удалить орфанный файл {file_path}: {e}")

    def get_cache_size(self) -> int:
        """Получить общий размер кэша в байтах"""
        total_size = 0
        for filename in os.listdir(self.cache_dir):
            file_path = os.path.join(self.cache_dir, filename)
            if os.path.isfile(file_path):
                total_size += os.path.getsize(file_path)
        return total_size

    def cleanup_by_size(self, target_size: int) -> int:
        """Очистка кэша до указанного размера, удаляя самые старые файлы"""
        # Получаем список файлов с сортировкой по дате
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT url, file_path, timestamp FROM cache ORDER BY timestamp ASC")
        files = cursor.fetchall()
        conn.close()
        current_size = self.get_cache_size()
        if current_size <= target_size:
            return 0
        # Удаляем файлы, начиная с самых старых
        deleted_count = 0
        for url, file_path, timestamp in files:
            if current_size <= target_size:
                break
            try:
                size = os.path.getsize(file_path)
                os.remove(file_path)
                # Удаляем запись из базы
                conn = sqlite3.connect(self.db_path)
                cursor = conn.cursor()
                cursor.execute("DELETE FROM cache WHERE url = ?", (url,))
                conn.commit()
                conn.close()
                current_size -= size
                deleted_count += 1
                logger.info(f"Очищен кэш: {file_path} ({size/(1024*1024):.2f} MB)")
            except Exception as e:
                logger.error(f"Не удалось удалить файл кэша {file_path}: {e}")
        return deleted_count

    async def _auto_cleanup_task(self):
        """Фоновая задача для регулярной очистки кэша"""
        while True:
            try:
                # Очищаем файлы старше 6 часов (было 7 дней)
                deleted = self.cleanup_old_files_by_hours(hours=6)
                if deleted > 0:
                    logger.info(f"Автоочистка кэша: удалено {deleted} старых записей")
                # Проверяем общий размер кэша
                cache_size = self.get_cache_size()
                max_cache_size = 10 * 1024 * 1024 * 1024  # 10 GB
                if cache_size > max_cache_size:
                    # Оставляем 8 GB
                    target_size = 8 * 1024 * 1024 * 1024
                    deleted_count = self.cleanup_by_size(target_size)
                    logger.info(f"Автоочистка кэша: удалено {deleted_count} файлов для уменьшения размера кэша")
            except Exception as e:
                logger.error(f"Ошибка в задаче автоочистки кэша: {e}")
            # Проверяем каждые 24 часа
            await asyncio.sleep(24 * 3600)

# ===== МЕНЕДЖЕР ИСТОРИИ ЗАГРУЗОК =====
class HistoryManager:
    def __init__(self, db_path="history.db"):
        self.db_path = db_path
        self._init_db()

    def _init_db(self):
        """Инициализация базы данных для истории загрузок"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS history (
            user_id INTEGER NOT NULL,
            url TEXT NOT NULL,
            file_type TEXT NOT NULL,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (user_id, url, file_type)
        )
        """)
        conn.commit()
        conn.close()

    def add_to_history(self, user_id: int, url: str, file_type: str) -> bool:
        """Добавить запись в историю"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        try:
            cursor.execute(
                "INSERT OR REPLACE INTO history (user_id, url, file_type) VALUES (?, ?, ?)",
                (user_id, url, file_type)
            )
            conn.commit()
            return True
        except Exception as e:
            logger.error(f"Ошибка добавления в историю: {e}")
            return False
        finally:
            conn.close()

    def get_history(self, user_id: int, limit=10) -> List[Tuple[str, str, str]]:
        """Получить историю загрузок пользователя"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        try:
            cursor.execute(
                "SELECT url, file_type, timestamp FROM history WHERE user_id = ? ORDER BY timestamp DESC LIMIT ?",
                (user_id, limit)
            )
            return cursor.fetchall()
        except Exception as e:
            logger.error(f"Ошибка получения истории: {e}")
            return []
        finally:
            conn.close()

    def clear_history(self, user_id: int) -> bool:
        """Очистить историю пользователя"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        try:
            cursor.execute(
                "DELETE FROM history WHERE user_id = ?",
                (user_id,)
            )
            conn.commit()
            return True
        except Exception as e:
            logger.error(f"Ошибка очистки истории: {e}")
            return False
        finally:
            conn.close()

# ===== Глобальные переменные =====
# Глобальный словарь для хранения временных ссылок для кнопки "Повторить загрузку"
RETRY_LINKS = {}
RETRY_LINKS_EXPIRY = 3600  # Время жизни записей в секундах (1 час)

async def cleanup_retry_links():
    """Очистка старых записей из RETRY_LINKS"""
    while True:
        try:
            now = time.time()
            # Копируем ключи, так как мы будем изменять словарь
            for retry_id, (url, timestamp) in list(RETRY_LINKS.items()):
                if now - timestamp > RETRY_LINKS_EXPIRY:
                    del RETRY_LINKS[retry_id]
        except Exception as e:
            logger.error(f"Ошибка при очистке RETRY_LINKS: {e}")
        # Проверяем каждые 10 минут
        await asyncio.sleep(600)

# ---- helper functions ----
def find_first_url(text: str) -> Optional[str]:
    if not text:
        return None
    m = URL_RE.search(text)
    return m.group(0) if m else None

def strip_tracking_params(url: str) -> str:
    try:
        p = urlparse(url)
        return urlunparse((p.scheme, p.netloc, p.path, "", "", ""))
    except Exception:
        return url

def resolve_redirects(url: str, timeout: int = 10) -> str:
    """Следуем редиректам — сначала HEAD, затем GET (если нужно)."""
    try:
        headers = {"User-Agent": "Mozilla/5.0"}
        r = requests.head(url, allow_redirects=True, timeout=timeout, headers=headers)
        if r.url:
            return r.url
    except Exception:
        pass
    try:
        r = requests.get(url, allow_redirects=True, timeout=timeout, headers={"User-Agent": "Mozilla/5.0"})
        return r.url
    except Exception:
        return url

def extract_jsonld(html: str) -> Optional[dict]:
    try:
        for m in re.finditer(
            r'<script[^>]*type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',
            html, re.DOTALL | re.IGNORECASE
        ):
            text = m.group(1).strip()
            try:
                return json.loads(text)
            except Exception:
                try:
                    return json.loads(text.replace('\n', ''))
                except Exception:
                    continue
    except Exception:
        pass
    return None

def extract_sigi_state(html: str) -> Optional[dict]:
    try:
        m = re.search(r'<script[^>]*id=["\']SIGI_STATE["\'][^>]*>(.*?)</script>', html, re.DOTALL | re.IGNORECASE)
        if not m:
            return None
        text = m.group(1).strip()
        text = re.sub(r'^\s*(?:window\.)?SIGI_STATE\s*=\s*', '', text)
        text = text.rstrip(';\n ')
        try:
            return json.loads(text)
        except Exception:
            mm = re.search(r'"ItemModule"\s*:\s*({.*?})\s*,', text, re.DOTALL)
            if mm:
                try:
                    return {"ItemModule": json.loads(mm.group(1))}
                except Exception:
                    return None
    except Exception:
        pass
    return None

def extract_tiktok_video_from_html(html: str) -> Optional[str]:
    # 1) JSON-LD
    try:
        ld = extract_jsonld(html)
        def pick_from_ld(obj) -> Optional[str]:
            if not isinstance(obj, dict):
                return None
            cu = obj.get("contentUrl")
            if cu:
                return strip_tracking_params(cu)
            u = obj.get("url")
            if u and "/video/" in u:
                return strip_tracking_params(u)
            return None
        if isinstance(ld, dict):
            got = pick_from_ld(ld)
            if got:
                return got
        elif isinstance(ld, list):
            for el in ld:
                got = pick_from_ld(el) if isinstance(el, dict) else None
                if got:
                    return got
    except Exception:
        pass
    # 2) SIGI_STATE
    try:
        sigi = extract_sigi_state(html)
        if sigi:
            item_module = sigi.get("ItemModule") or {}
            if isinstance(item_module, dict):
                for k, v in item_module.items():
                    vid = None
                    user = None
                    if isinstance(v, dict):
                        vid = v.get("id") or v.get("itemInfos", {}).get("id") or k
                        user = (
                            v.get("author") or
                            v.get("authorInfo") or
                            v.get("itemInfos", {}).get("author")
                        )
                    if vid:
                        if isinstance(user, dict):
                            user = user.get("uniqueId") or user.get("nickname")
                        if user:
                            return f"https://www.tiktok.com/@{user}/video/{vid}"
                        return f"https://www.tiktok.com/video/{vid}"
    except Exception:
        pass
    # 3) og:url
    m = re.search(r'<meta[^>]+property=["\']og:url["\'][^>]+content=["\']([^"\']+)["\']', html, re.IGNORECASE)
    if m:
        u = m.group(1)
        if "/video/" in u:
            return strip_tracking_params(u)
    # 4) itemId
    m = re.search(r'itemId["\']?\s*[:=]\s*["\']?(\d{6,})["\']?', html)
    if m:
        vid = m.group(1)
        u = re.search(r'"uniqueId"\s*:\s*"([^"]+)"', html)
        if u:
            return f"https://www.tiktok.com/@{u.group(1)}/video/{vid}"
        return f"https://www.tiktok.com/video/{vid}"
    # 5) fallback по /@user/video/<id>
    m = re.search(r'/@([^/]+)/video/(\d{6,})', html)
    if m:
        user, vid = m.group(1), m.group(2)
        return f"https://www.tiktok.com/@{user}/video/{vid}"
    return None

async def normalize_tiktok_url_async(url: str, session: aiohttp.ClientSession) -> Optional[str]:
    """
    Асинхронная нормализация TikTok URL без блокировок.
    """
    try:
        url_low = url.lower()
        headers = {"User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15"}

        # Если это короткая ссылка — разрешаем редирект
        if any(d in url_low for d in SHORTENER_DOMAINS):
            try:
                async with session.head(url, allow_redirects=True, timeout=aiohttp.ClientTimeout(total=10), headers=headers) as resp:
                    final = str(resp.url)
            except Exception:
                async with session.get(url, allow_redirects=True, timeout=aiohttp.ClientTimeout(total=12), headers=headers) as resp:
                    final = str(resp.url)
            final_clean = strip_tracking_params(final)
            if "/video/" in final_clean:
                return final_clean

            # Пытаемся распарсить HTML
            async with session.get(final, headers=headers, timeout=aiohttp.ClientTimeout(total=12)) as resp:
                if resp.status == 200:
                    html = await resp.text()
                    ex = extract_tiktok_video_from_html(html)
                    if ex:
                        return ex

        # Уже /video/ — просто чистим
        if "/video/" in url_low:
            return strip_tracking_params(url)

        # Профиль/хэштег — парсим HTML
        if any(p in url_low for p in ("/@", "/tag/", "/hashtag/", "/music/", "/explore", "/search")):
            async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=12)) as resp:
                if resp.status == 200:
                    html = await resp.text()
                    ex = extract_tiktok_video_from_html(html)
                    if ex:
                        return ex

        # Последняя попытка
        try:
            async with session.head(url, allow_redirects=True, timeout=aiohttp.ClientTimeout(total=10), headers=headers) as resp:
                final = str(resp.url)
        except Exception:
            async with session.get(url, allow_redirects=True, timeout=aiohttp.ClientTimeout(total=10), headers=headers) as resp:
                final = str(resp.url)
        final_clean = strip_tracking_params(final)
        if "/video/" in final_clean:
            return final_clean

    except Exception:
        logger.exception("normalize_tiktok_url_async error for %s", url)
    return None

def normalize_twitter_url(url: str) -> Optional[str]:
    """Нормализация URL Twitter/X"""
    try:
        url_low = url.lower()
        if "x.com" in url_low or "twitter.com" in url_low:
            # Разрешаем редиректы
            final = resolve_redirects(url)
            # Убираем трекинг-параметры
            clean = strip_tracking_params(final)
            # Проверяем, что это ссылка на статус
            if re.search(r'(?:twitter\.com|x\.com)/[^/]+/status/\d+', clean, re.IGNORECASE):
                return clean
    except Exception:
        logger.exception("normalize_twitter_url error for %s", url)
    return None

def normalize_reddit_url(url: str) -> Optional[str]:
    """Нормализация URL Reddit — извлекает прямую ссылку на видео"""
    try:
        url_low = url.lower()
        if "reddit.com" in url_low:
            # Разрешаем редиректы
            final = resolve_redirects(url)
            # Проверяем, что это ссылка на пост
            if not re.search(r'reddit\.com/(?:r/[^/]+/comments/|comments/)[\w]+/[\w_-]+/[\w]+', final, re.IGNORECASE):
                return None
            # Загружаем HTML страницы
            headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
            r = requests.get(final, headers=headers, timeout=12)
            if r.status_code != 200:
                return None
            html = r.text
            # Ищем JSON в HTML (Reddit использует JSON для хранения данных поста)
            # Ищем window.___r = или подобное
            match = re.search(r'window\.___r\s*=\s*({.*?});', html, re.DOTALL)
            if not match:
                return None
            try:
                data = json.loads(match.group(1))
                # Ищем видео в данных
                # Структура может меняться, но обычно видео находится в:
                # data.props.pageProps.postInfo.post
                post = data.get("props", {}).get("pageProps", {}).get("postInfo", {}).get("post", {})
                if not post:
                    return None
                # Ищем видео
                video_url = None
                media = post.get("media", {})
                if media.get("type") == "video":
                    video_url = media.get("content", {}).get("url")
                # Альтернативный способ: через secure_media
                if not video_url:
                    secure_media = post.get("secure_media", {})
                    if secure_media.get("type") == "video":
                        video_url = secure_media.get("content", {}).get("url")
                # Еще один способ: через crosspost_parent_list
                if not video_url:
                    crosspost = post.get("crosspost_parent_list", [])
                    if crosspost and len(crosspost) > 0:
                        media = crosspost[0].get("media", {})
                        if media.get("type") == "video":
                            video_url = media.get("content", {}).get("url")
                if video_url:
                    return video_url
            except Exception:
                logger.exception("Failed to parse Reddit JSON")
                return None
    except Exception:
        logger.exception("normalize_reddit_url error for %s", url)
    return None

async def download_instagram_video_async(url: str, out_dir: str, mode: str = "video", quality: str = "best", session: aiohttp.ClientSession = None) -> str:
    """
    Асинхронная загрузка видео с Instagram.
    """
    close_session = False
    if session is None:
        session = aiohttp.ClientSession(
            headers={
                "User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15",
                "Accept-Language": "en-US,en;q=0.9",
                "Referer": "https://www.instagram.com/",
                "X-Requested-With": "XMLHttpRequest",
                "X-IG-App-ID": "936619743392459",
            }
        )
        close_session = True

    try:
        for attempt in range(3):
            try:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=30)) as resp:
                    if resp.status != 200:
                        raise Exception(f"Не удалось загрузить страницу Instagram: {resp.status}")
                    html_content = await resp.text()

                # Ищем JSON (те же паттерны)
                json_data = None
                json_patterns = [
                    r'window\.__additionalDataLoaded\([^,]+,\s*({.+?})\);',
                    r'<script type="application/json"[^>]*>(.+?)</script>',
                    r'window\.__initialDataLoaded\([^,]+,\s*({.+?})\);',
                    r'window\.__sharedData\s*=\s*({.+?});',
                    r'window\._sharedData\s*=\s*({.+?});',
                    r'window\.__graphql__\s*=\s*({.+?});',
                ]
                for pattern in json_patterns:
                    matches = re.findall(pattern, html_content, re.DOTALL)
                    for match in matches:
                        try:
                            if isinstance(match, tuple):
                                match = match[0]
                            json_data = json.loads(match)
                            break
                        except json.JSONDecodeError:
                            continue
                    if json_data:
                        break

                if not json_data:
                    raise Exception("Не удалось найти данные поста")

                def find_video_url(data, depth=0):
                    if depth > 10:
                        return None
                    if isinstance(data, dict):
                        for key in ['video_url', 'videoUrl', 'contentUrl', 'url', 'src', 'video_versions']:
                            if key in data:
                                value = data[key]
                                if isinstance(value, str) and value.startswith('http') and any(ext in value for ext in ['.mp4', '.mov']):
                                    return value
                                elif isinstance(value, list) and key == 'video_versions':
                                    best = None
                                    for v in value:
                                        if isinstance(v, dict) and 'url' in v:
                                            if quality == "best":
                                                if not best or v.get('width', 0) > best.get('width', 0):
                                                    best = v
                                            else:
                                                target_h = int(quality.replace('p', ''))
                                                if v.get('height') == target_h:
                                                    return v['url']
                                    if best:
                                        return best['url']
                        for v in data.values():
                            res = find_video_url(v, depth + 1)
                            if res:
                                return res
                    elif isinstance(data, list):
                        for item in data:
                            res = find_video_url(item, depth + 1)
                            if res:
                                return res
                    return None

                video_url = find_video_url(json_data)
                if not video_url:
                    og_match = re.search(r'<meta[^>]+property="og:video"[^>]+content="([^"]+)"', html_content)
                    if og_match:
                        video_url = og_match.group(1)
                    else:
                        raise Exception("Видео не найдено")

                timestamp = int(time.time())
                filename = f"instagram_{timestamp}"
                filepath = os.path.join(out_dir, filename + (".mp3" if mode == "audio" else ".mp4"))

                # Скачиваем видео асинхронно
                async with session.get(video_url, timeout=aiohttp.ClientTimeout(total=300)) as resp:
                    resp.raise_for_status()
                    total_size = int(resp.headers.get('content-length', 0))
                    downloaded = 0
                    with open(filepath, 'wb') as f:
                        async for chunk in resp.content.iter_chunked(8192):
                            f.write(chunk)
                            downloaded += len(chunk)
                            if total_size > 0 and downloaded % (total_size // 20) == 0:
                                percent = downloaded / total_size * 100
                                logger.info(f"Instagram: скачано {percent:.1f}%")

                # Конвертация в аудио (если нужно) — остаётся синхронной (ffmpeg не имеет async-обёртки)
                if mode == "audio":
                    audio_path = filepath.replace(".mp4", ".mp3")
                    try:
                        await asyncio.create_subprocess_exec(
                            "ffmpeg", "-i", filepath, "-vn", "-acodec", "libmp3lame", "-q:a", "2", "-y", audio_path,
                            stdout=asyncio.subprocess.DEVNULL,
                            stderr=asyncio.subprocess.DEVNULL
                        )
                        os.remove(filepath)
                        filepath = audio_path
                    except Exception as e:
                        logger.warning(f"Не удалось конвертировать аудио: {e}")
                        if not os.path.exists(filepath):
                            raise

                return filepath

            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                logger.warning(f"Сетевая ошибка Instagram (попытка {attempt + 1}/3): {e}")
                if attempt < 2:
                    await asyncio.sleep(3)
                    continue
                else:
                    raise
            except Exception as e:
                logger.error(f"Ошибка Instagram (попытка {attempt + 1}/3): {e}")
                if attempt < 2:
                    await asyncio.sleep(3)
                    continue
                else:
                    raise

    finally:
        if close_session:
            await session.close()

    raise Exception("Не удалось скачать Instagram после 3 попыток")

def is_youtube_video(url: str) -> bool:
    return bool(YOUTUBE_VIDEO_RE.search(url or ""))

def is_supported_by_platform(url: str) -> bool:
    u = (url or "").lower()
    if "tiktok.com" in u or "vm.tiktok.com" in u or "m.tiktok.com" in u:
        return bool(TIKTOK_ANY_RE.search(u))
    if "youtube.com" in u or "youtu.be" in u:
        return is_youtube_video(url)
    if "instagram.com" in u:
        return bool(INSTAGRAM_RE.search(u))
    if "facebook.com" in u:
        return bool(FACEBOOK_RE.search(u))
    if "twitter.com" in u or "x.com" in u:
        return bool(TWITTER_RE.search(u))
    if "vk.com" in u or "m.vkvideo.ru" in u:
        return bool(VK_RE.search(u))
    if "reddit.com" in u:
        return bool(REDDIT_RE.search(u))
    if "pinterest.com" in u or "pin.it" in u:
        return bool(PINTEREST_RE.search(u))
    if "dailymotion.com" in u:
        return bool(DAILYMOTION_RE.search(u))
    if "vimeo.com" in u:
        return bool(VIMEO_RE.search(u))
    if "soundcloud.com" in u:
        return bool(SOUNDCLOUD_RE.search(u))
    # Проверяем прямые ссылки на файлы
    if DIRECT_FILE_RE.search(url):
        return True
    return False

def make_actions_kb(pending_msg_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="Аудио (mp3)", callback_data=f"dl:audio:{pending_msg_id}"),
            InlineKeyboardButton(text="Видео (mp4)", callback_data=f"dl:video:{pending_msg_id}")
        ],
        [
            InlineKeyboardButton(text="Отмена", callback_data=f"dl:cancel:{pending_msg_id}")
        ]
    ])

def upload_to_transfersh(path: str) -> Optional[str]:
    filename = os.path.basename(path)
    url = f"https://transfer.sh/{filename}"
    try:
        with open(path, "rb") as fp:
            r = requests.put(url, data=fp, timeout=120)
            if r.status_code in (200, 201):
                return r.text.strip()
    except Exception:
        logger.exception("transfer.sh upload failed")
    return None

async def upload_to_fileio(session: aiohttp.ClientSession, filepath: str) -> Optional[str]:
    """Загружает файл на file.io (время жизни по умолчанию — 14 дней)"""
    url = "https://file.io/?expires=14d"
    try:
        with open(filepath, "rb") as f:
            data = aiohttp.FormData()
            data.add_field('file', f, filename=os.path.basename(filepath))
            async with session.post(url, data=data, timeout=aiohttp.ClientTimeout(total=120)) as resp:
                if resp.status == 200:
                    json_resp = await resp.json()
                    if json_resp.get("success"):
                        return json_resp.get("link")
    except Exception as e:
        logger.warning(f"file.io upload failed: {e}")
    return None

async def upload_to_anonfiles(session: aiohttp.ClientSession, filepath: str) -> Optional[str]:
    """Загружает файл на anonfiles.com"""
    url = "https://api.anonfiles.com/upload"
    try:
        with open(filepath, "rb") as f:
            data = aiohttp.FormData()
            data.add_field('file', f, filename=os.path.basename(filepath))
            async with session.post(url, data=data, timeout=aiohttp.ClientTimeout(total=120)) as resp:
                if resp.status == 200:
                    json_resp = await resp.json()
                    if json_resp.get("status"):
                        return json_resp["data"]["file"]["url"]["short"]
    except Exception as e:
        logger.warning(f"anonfiles.com upload failed: {e}")
    return None

async def upload_to_gofile(session: aiohttp.ClientSession, filepath: str) -> Optional[str]:
    """Загружает файл на gofile.io"""
    try:
        # Сначала получаем сервер
        async with session.get("https://api.gofile.io/servers", timeout=aiohttp.ClientTimeout(total=10)) as resp:
            if resp.status != 200:
                return None
            server_data = await resp.json()
            if not server_data.get("status") == "ok":
                return None
            server = server_data["data"]["servers"][0]["name"]
        # Загружаем файл
        upload_url = f"https://{server}.gofile.io/contents/uploadfile"
        with open(filepath, "rb") as f:
            data = aiohttp.FormData()
            data.add_field('file', f, filename=os.path.basename(filepath))
            async with session.post(upload_url, data=data, timeout=aiohttp.ClientTimeout(total=120)) as resp:
                if resp.status == 200:
                    json_resp = await resp.json()
                    if json_resp.get("status") == "ok":
                        return json_resp["data"]["downloadPage"]
    except Exception as e:
        logger.warning(f"gofile.io upload failed: {e}")
    return None

async def upload_to_multiple_services(filepath: str) -> Optional[str]:
    """Пытается загрузить файл на несколько сервисов с fallback'ом"""
    async with aiohttp.ClientSession() as session:
        # Порядок приоритета: transfer.sh → file.io → anonfiles → gofile
        for uploader in [
            lambda s: asyncio.to_thread(upload_to_transfersh, filepath),  # остаётся синхронной
            lambda s: upload_to_fileio(s, filepath),
            lambda s: upload_to_anonfiles(s, filepath),
            lambda s: upload_to_gofile(s, filepath)
        ]:
            try:
                link = await uploader(session)
                if link:
                    logger.info(f"Файл успешно загружен: {link}")
                    return link
            except Exception as e:
                logger.warning(f"Загрузчик завершился с ошибкой: {e}")
                continue
    logger.error("Все сервисы загрузки недоступны")
    return None

# ---- yt-dlp download ----
def ytdl_download(url: str, out_dir: str, mode: str, progress_hook=None) -> str:
    """
    Прямая загрузка через yt-dlp. Поддерживает прогресс-хук.
    """
    opts = YTDL_BASE_OPTS.copy()
    opts["outtmpl"] = os.path.join(out_dir, "%(id)s.%(ext)s")
    if mode == "audio":
        opts.update({
            "format": "bestaudio/best",
            "postprocessors": [{
                "key": "FFmpegExtractAudio",
                "preferredcodec": "mp3",
                "preferredquality": "192",
            }],
        })
    else:
        opts.update({
            "format": "bestvideo+bestaudio/best",
            "merge_output_format": "mp4",
        })
    if progress_hook:
        opts["progress_hooks"] = [progress_hook]
    with YoutubeDL(opts) as ytdl:
        info = ytdl.extract_info(url, download=True)
        if mode == "audio":
            filename = os.path.join(out_dir, f"{info.get('id')}.mp3")
            if not os.path.exists(filename):
                filename = ytdl.prepare_filename(info)
                filename = os.path.splitext(filename)[0] + ".mp3"
        else:
            filename = ytdl.prepare_filename(info)
            if not filename.lower().endswith(".mp4"):
                filename = os.path.splitext(filename)[0] + ".mp4"
        if not os.path.exists(filename):
            raise FileNotFoundError(f"Не найден файл: {filename}")
        return filename

def make_progress_hook(loop: asyncio.AbstractEventLoop, chat_id: int, status_message_id: int, task_id: int):
    """
    Потокобезопасный прогресс-хук для yt-dlp с улучшенным визуальным прогресс-баром и интерактивными элементами
    """
    last_update = 0.0
    total_size = 0
    start_time = time.time()

    async def _edit(text: str, reply_markup=None):
        try:
            await bot.edit_message_text(
                chat_id=chat_id,
                message_id=status_message_id,
                text=text,
                reply_markup=reply_markup,
                parse_mode="HTML"
            )
        except Exception as e:
            logger.debug(f"Failed to edit progress message: {e}")
            pass

    def hook(d: dict):
        nonlocal last_update, total_size
        try:
            status = d.get("status")
            now = time.time()
            # Кнопки управления
            control_kb = InlineKeyboardMarkup(inline_keyboard=[
                [
                    InlineKeyboardButton(text="⏸️ Приостановить", callback_data=f"progress:pause:{task_id}"),
                    InlineKeyboardButton(text="⏹️ Отменить", callback_data=f"progress:cancel:{task_id}")
                ]
            ])
            if status == "downloading":
                total = d.get("total_bytes") or d.get("total_bytes_estimate") or 0
                downloaded = d.get("downloaded_bytes", 0)
                speed = d.get("speed", 0)
                if total > 0:
                    total_size = total
                    percent = min(100, max(0, downloaded / total * 100))
                    # Более детальный прогресс-бар
                    filled = int(percent / 5)
                    empty = 20 - filled
                    progress_bar = "▰" * filled + "▱" * empty
                    # Расчет оставшегося времени
                    elapsed = now - start_time
                    if speed > 0:
                        remaining = (total - downloaded) / speed
                        mins, secs = divmod(int(remaining), 60)
                        time_str = f"Осталось: {mins}м {secs}с"
                    else:
                        time_str = "Оценка времени недоступна"
                    # Скорость с усреднением
                    if elapsed > 0:
                        avg_speed = downloaded / elapsed
                        speed_str = f"Текущая: {speed/1024:.1f} KB/s | Средняя: {avg_speed/1024:.1f} KB/s"
                    else:
                        speed_str = f"Скорость: {speed/1024:.1f} KB/s"
                    text = (
                        f"⏳ <b>Загрузка видео</b>\n"
                        f"{progress_bar} <code>{percent:.1f}%</code>\n"
                        f"Скачано: <code>{downloaded//1024} KB</code> из <code>{total//1024} KB</code>\n"
                        f"{speed_str}\n"
                        f"{time_str}"
                    )
                else:
                    text = f"📥 Скачано: {downloaded//1024} KB"
                # Обновляем каждые 2 секунды или при значительном изменении прогресса
                if now - last_update > 2.0 or (percent % 5 == 0 and percent > 0):
                    last_update = now
                    asyncio.run_coroutine_threadsafe(_edit(text, control_kb), loop)
            elif status == "processing":
                text = (
                    "🎬 <b>Обработка видео</b>\n"
                    "Выполняется конвертация и объединение потоков...\n"
                    "Этот этап может занять некоторое время в зависимости от длины видео."
                )
                asyncio.run_coroutine_threadsafe(_edit(text, control_kb), loop)
            elif status == "finished":
                text = "✅ <b>Загрузка завершена!</b>\nПодготовка файла к отправке..."
                asyncio.run_coroutine_threadsafe(_edit(text), loop)
        except Exception as e:
            logger.debug(f"Progress hook error: {str(e)}", exc_info=True)
    return hook

# ---- handlers ----
async def cmd_start(message: types.Message):
    user_id = message.from_user.id
    # Убедимся, что пользователь добавлен в базу
    settings = user_settings.get_settings(user_id)
    
    # Создаем интерактивную клавиатуру с основными действиями
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="🎬 Скачать видео", callback_data="start_download"),
            InlineKeyboardButton(text="🎵 Скачать аудио", callback_data="format:audio")
        ],
        [
            InlineKeyboardButton(text="📜 История загрузок", callback_data="history:view")
        ],
        [
            InlineKeyboardButton(text="👥 Добавить в группу", url="https://t.me/Lain_ExBot?startgroup=true")
        ]
    ])
    
    # Формируем информативное приветственное сообщение
    welcome_text = (
        f"👋 <b>Привет, {message.from_user.first_name}!</b>\n\n"
        "🎉 <b>Добро пожаловать в мое логово скачивании видео!</b>\n\n"
        "📥 <b>Я могу скачать для тебя:</b>\n"
        "• Видео с YouTube, TikTok, Instagram, Facebook, Twitter/X\n"
        "• Контент с VK, Reddit, Pinterest, Dailymotion\n"
        "• Видео с Vimeo, SoundCloud и прямых ссылок\n"
        "• Аудио в формате MP3 из любого видео\n\n"
        "🚀 <b>Как использовать:</b>\n"
        "1. Просто отправь мне ссылку на видео\n"
        "2. Выбери формат (видео/аудио)\n"
        "3. Получи готовый файл!\n\n"
        "👤 <b>Автор:</b> @frastiel\n"
        "📢 <b>Новости:</b> @ExLainInfo\n\n"
        "💎 <i>Нажми на кнопки ниже чтобы начать!</i>"
    )
    
    try:
        # Пытаемся отправить сообщение с фото
        await bot.send_photo(
            chat_id=message.chat.id,
            photo="https://2chan.gt.tc/629e2f97fc046da3b7de90adae06c394.jpg",
            caption=welcome_text,
            reply_markup=keyboard,
            parse_mode="HTML"
        )
    except Exception:
        # Если не удалось отправить с фото, отправляем текстовое сообщение
        await message.reply(
            welcome_text,
            reply_markup=keyboard,
            parse_mode="HTML",
            disable_web_page_preview=True
        )
    
    # Отправляем дополнительное сообщение с примерами
    examples_text = (
        "🌐 <b>Примеры поддерживаемых ссылок:</b>\n"
        "• <code>https://www.youtube.com/watch?v=...</code>\n"
        "• <code>https://vm.tiktok.com/...</code>\n"
        "• <code>https://www.instagram.com/reel/...</code>\n"
        "• <code>https://x.com/user/status/...</code>\n"
        "• <code>https://vk.com/video-...</code>\n\n"
        "📝 <i>Просто скопируй и отправь мне любую ссылку!</i>"
    )
    
    await message.answer(
        examples_text,
        parse_mode="HTML",
        disable_web_page_preview=True
    )

def has_enough_disk_space(path: str, required_mb: int = 500) -> bool:
    """Проверяет, достаточно ли свободного места на диске"""
    try:
        total, used, free = shutil.disk_usage(path)
        free_mb = free // (1024 * 1024)
        return free_mb >= required_mb
    except Exception:
        return True  # На случай ошибки, не блокируем загрузку

async def handle_text(message: types.Message):
    user_id = message.from_user.id
    # Добавляем пользователя в базу при первом взаимодействии
    user_settings.get_settings(user_id)
    text = (message.text or "").strip()
    url = find_first_url(text)
    # Если ссылка не найдена, но это личный чат - сообщаем об ошибке
    if not url:
        if message.chat.type == "private":
            await message.reply("Не нашёл ссылку в сообщении. Пришлите ссылку на видео.")
        return

    normalized = url
    ulow = url.lower()

    if any(dom in ulow for dom in ("tiktok.com", "vm.tiktok.com", "m.tiktok.com")):
        try:
            async with aiohttp.ClientSession() as session:
                norm = await normalize_tiktok_url_async(url, session)
            if norm:
                normalized = norm
        except Exception:
            logger.exception("Normalization failed for %s", url)
    elif any(dom in ulow for dom in ("twitter.com", "x.com")):
        try:
            norm = normalize_twitter_url(url)
            if norm:
                normalized = norm
        except Exception:
            logger.exception("Normalization failed for %s", url)
    elif "reddit.com" in ulow:
        try:
            norm = normalize_reddit_url(url)
            if norm:
                normalized = norm
        except Exception:
            logger.exception("Normalization failed for %s", url)
    elif "pinterest.com" in ulow or "pin.it" in ulow:
        try:
            final = resolve_redirects(url)
            clean = strip_tracking_params(final)
            if "/pin/" in clean:
                normalized = clean
        except Exception:
            logger.exception("Normalization failed for %s", url)
    elif DIRECT_FILE_RE.search(url):
        # Если это прямая ссылка на файл, показываем уведомление
        await message.answer(
            "📥 Обнаружена прямая ссылка на файл. Начинаю загрузку...\n"
            "Это может занять некоторое время в зависимости от размера файла."
        )

    if not is_supported_by_platform(normalized):
        # В личном чате сообщаем об ошибке
        if message.chat.type == "private":
            await message.reply(
                "Поддерживаются только прямые ссылки на видео с поддерживаемых платформ.\n"
                "Платформы: YouTube, TikTok, Instagram, Facebook, Twitter/X, VK, Reddit, Pinterest, Dailymotion, Vimeo, SoundCloud.\n"
                "Также поддерживаются прямые ссылки на файлы (mp4, mp3 и др.).\n"
                "Если прислали профиль/хештег/страницу — отправьте прямую ссылку на видео."
            )
        return

    kb_msg = await message.answer("Выберите формат для скачивания:", reply_markup=make_actions_kb(0))
    PENDING_LINKS[kb_msg.message_id] = normalized
    await bot.edit_message_reply_markup(
        chat_id=kb_msg.chat.id,
        message_id=kb_msg.message_id,
        reply_markup=make_actions_kb(kb_msg.message_id)
    )

# --- колбэк ---
async def cb_download(callback: types.CallbackQuery):
    data = callback.data or ""
    parts = data.split(":")
    if len(parts) != 3:
        await callback.answer("Некорректные данные.", show_alert=True)
        return
    _, what, msg_id_str = parts
    try:
        msg_id = int(msg_id_str)
    except ValueError:
        await callback.answer("Ошибка данных.", show_alert=True)
        return
    original_url = PENDING_LINKS.get(msg_id)
    if not original_url:
        await callback.answer("Ссылка устарела или не найдена. Отправьте ссылку снова.", show_alert=True)
        return
    if what == "cancel":
        PENDING_LINKS.pop(msg_id, None)
        try:
            await callback.message.edit_text("Отменено.")
        except Exception:
            pass
        await callback.answer()
        return
    await callback.answer()
    # Добавляем загрузку в менеджер
    user_id = callback.from_user.id
    mode = "audio" if what == "audio" else "video"
    await download_manager.add_download(callback, original_url, mode)
    # Удаляем из PENDING_LINKS, чтобы не дублировать
    PENDING_LINKS.pop(msg_id, None)

async def cmd_history(message: types.Message):
    """Показать историю загрузок пользователя"""
    user_id = message.from_user.id
    history = history_manager.get_history(user_id)
    if not history:
        await message.reply("Ваша история загрузок пуста.")
        return
    text = "📜 Ваша история загрузок:\n"
    for i, (url, file_type, timestamp) in enumerate(history, 1):
        # Форматируем дату
        try:
            dt = datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S.%f") if '.' in timestamp else datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S")
            date_str = dt.strftime("%d.%m.%Y %H:%M")
        except:
            date_str = timestamp
        text += f"{i}. {date_str}\n"
        text += f"🔗 {url}\n"
        text += f"🎬 {'Видео' if file_type == 'video' else 'Аудио'}\n"
    # Кнопка для очистки истории
    clear_kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🧹 Очистить историю", callback_data="history:clear")]
    ])
    await message.reply(text, reply_markup=clear_kb)

async def cb_history(callback: types.CallbackQuery):
    """Обработчик колбэков для истории"""
    data = callback.data
    user_id = callback.from_user.id
    if data == "history:clear":
        if history_manager.clear_history(user_id):
            await callback.message.edit_text("✅ История загрузок очищена.")
        else:
            await callback.answer("❌ Не удалось очистить историю.")
    elif data == "history:view":
        history = history_manager.get_history(user_id)
        if not history:
            await callback.message.edit_text("Ваша история загрузок пуста.")
            return
        text = "📜 Ваша история загрузок:\n"
        for i, (url, file_type, timestamp) in enumerate(history, 1):
            # Форматируем дату
            try:
                dt = datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S.%f") if '.' in timestamp else datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S")
                date_str = dt.strftime("%d.%m.%Y %H:%M")
            except:
                date_str = timestamp
            text += f"{i}. {date_str}\n"
            text += f"🔗 {url}\n"
            text += f"🎬 {'Видео' if file_type == 'video' else 'Аудио'}\n"
        # Кнопка для очистки истории
        clear_kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🧹 Очистить историю", callback_data="history:clear")]
        ])
        await callback.message.edit_text(text, reply_markup=clear_kb)
    elif data == "start_download":
        await callback.message.edit_text("Пришлите ссылку на видео, которое хотите скачать.")

# Команда для рассылки новостей (только для администратора)
async def cmd_addnews(message: types.Message):
    """Admin-only command: /addnews <text>
    Usage examples:
    /addnews Текст новости
    /addnews button=Label|https://example.com Текст новости
    /addnews button1=Label1|URL1,button2=Label2|URL2 Текст новости
    /addnews https://example.com Текст новости (will create a button from first URL)
    You can also reply to a message with /addnews to forward that message as news.
    Supports media files (photo, video, document) when replying to media messages.
    """
    ADMIN_ID = 6143311340  # Замените на ваш ID администратора
    user_id = message.from_user.id
    if user_id != ADMIN_ID:
        await message.reply("❌ Только администратор может использовать эту команду.")
        return

    # Try to get args after command. If empty, and this is a reply, take replied message text.
    raw = (message.text or "").strip()
    parts = raw.split(None, 1)
    # Remove command mention if present (e.g., /addnews@Bot)
    if parts:
        parts[0] = re.sub(r'@\w+$', '', parts[0])
    news_text = ""
    buttons = []
    
    # Check if we're replying to a message with media
    replied_message = message.reply_to_message
    media_file = None
    media_type = None
    
    if replied_message:
        # Get media from replied message if available
        if replied_message.photo:
            media_file = replied_message.photo[-1].file_id
            media_type = "photo"
        elif replied_message.video:
            media_file = replied_message.video.file_id
            media_type = "video"
        elif replied_message.document:
            media_file = replied_message.document.file_id
            media_type = "document"
        elif replied_message.audio:
            media_file = replied_message.audio.file_id
            media_type = "audio"
        
        # Use caption if available, otherwise use text
        if replied_message.caption:
            news_text = replied_message.caption
        elif replied_message.text:
            news_text = replied_message.text
    
    # If not replying or no text from reply, use command arguments
    if not news_text and len(parts) >= 2 and parts[1].strip():
        news_text = parts[1].strip()
    
    if not news_text and not media_file:
        await message.reply(
            "Использование: /addnews Текст новости.\n"
            "Пример: /addnews Бота обновлен! Новые функции: ...\n"
            "Дополнительно можно добавить кнопки: /addnews button1=Label1|URL1,button2=Label2|URL2 Текст\n"
            "Или ответьте на сообщение с медиа-файлом, чтобы отправить его в рассылке."
        )
        return

    # Parse buttons from text (multiple buttons supported: button1=Label1|URL1,button2=Label2|URL2)
    button_pattern = r'(?:^|,)\s*button(?:\d+)?\s*[:=]\s*([^,]+)'
    button_matches = re.findall(button_pattern, news_text, re.IGNORECASE)
    
    for btn_match in button_matches:
        btn_match = btn_match.strip()
        if '|' in btn_match:
            button_label, button_url = btn_match.split('|', 1)
            button_label = button_label.strip()
            button_url = button_url.strip()
            
            # Ensure URL has scheme
            if not re.match(r'^https?://', button_url, flags=re.IGNORECASE):
                button_url = 'https://' + button_url
            
            buttons.append(InlineKeyboardButton(text=button_label, url=button_url))
        
        # Remove button patterns from news text
        news_text = re.sub(button_pattern, '', news_text, flags=re.IGNORECASE)
    
    # If no explicit buttons, try to extract first URL from news_text to create a default button
    if not buttons:
        murl = re.search(r'https?://[^\s<>"()]+', news_text, flags=re.IGNORECASE)
        if murl:
            button_url = murl.group(0)
            buttons.append(InlineKeyboardButton(text="🔗 Перейти", url=button_url))
    
    # Clean up news text (remove extra commas and whitespace)
    news_text = re.sub(r'^\s*,\s*|\s*,\s*$', '', news_text)  # Remove leading/trailing commas
    news_text = re.sub(r'\s*,\s*', ', ', news_text)  # Normalize commas
    news_text = news_text.strip()
    
    # Prepare reply markup if buttons present
    reply_kb = None
    if buttons:
        # Split buttons into rows of 2 buttons each
        button_rows = [buttons[i:i+2] for i in range(0, len(buttons), 2)]
        reply_kb = InlineKeyboardMarkup(inline_keyboard=button_rows)
    
    # Prepare final message (if empty after stripping, put a placeholder)
    if not news_text and not media_file:
        news_text = "📣 Новость"
    
    # Add news header if we have text
    if news_text:
        formatted_text = f"📣 <b>Новость от бота</b>\n\n{news_text}"
    else:
        formatted_text = "📣 <b>Новость от бота</b>"
    
    # Broadcast
    recipients = user_settings.get_all_user_ids()
    if not recipients:
        await message.reply("Нет подписчиков для рассылки.")
        return

    sent = 0
    failed = 0
    total = len(recipients)
    progress_msg = await message.reply(f"📤 Начинаю рассылку новостей (0/{total})...")
    
    # Send sequentially with small delay to avoid flood
    for i, uid in enumerate(recipients):
        try:
            if media_file:
                # Send media with caption
                if media_type == "photo":
                    await bot.send_photo(
                        uid,
                        photo=media_file,
                        caption=formatted_text,
                        parse_mode="HTML",
                        reply_markup=reply_kb
                    )
                elif media_type == "video":
                    await bot.send_video(
                        uid,
                        video=media_file,
                        caption=formatted_text,
                        parse_mode="HTML",
                        reply_markup=reply_kb
                    )
                elif media_type == "document":
                    await bot.send_document(
                        uid,
                        document=media_file,
                        caption=formatted_text,
                        parse_mode="HTML",
                        reply_markup=reply_kb
                    )
                elif media_type == "audio":
                    await bot.send_audio(
                        uid,
                        audio=media_file,
                        caption=formatted_text,
                        parse_mode="HTML",
                        reply_markup=reply_kb
                    )
            else:
                # Send text message
                await bot.send_message(
                    uid,
                    formatted_text,
                    parse_mode="HTML",
                    reply_markup=reply_kb,
                    disable_web_page_preview=True
                )
            
            sent += 1
            
            # Update progress every 10 messages or every 5 seconds
            if i % 10 == 0 or i == total - 1:
                try:
                    await progress_msg.edit_text(f"📤 Рассылка новостей: {i+1}/{total} отправлено")
                except:
                    pass
            
            await asyncio.sleep(0.1)  # Задержка между отправкой сообщений
        except Exception as e:
            logger.error(f"Не удалось отправить сообщение пользователю {uid}: {e}")
            failed += 1
    
    # Send final report
    report_text = f"✅ Рассылка завершена\n\nОтправлено: {sent}\nНе удалось: {failed}"
    
    if failed > 0:
        report_text += "\n\n❌ Некоторые сообщения не были доставлены. Это может быть связано с тем, что пользователи заблокировали бота или удалили свои аккаунты."
    
    await progress_msg.edit_text(report_text)
    await message.reply(report_text)

# Обработчик для управления загрузкой (пауза/отмена)
async def cb_progress_control(callback: types.CallbackQuery):
    """Обработчик кнопок управления загрузкой"""
    data = callback.data
    parts = data.split(":")
    if len(parts) < 3:
        await callback.answer("Некорректные данные.", show_alert=True)
        return
    _, action, task_id_str = parts
    try:
        task_id = int(task_id_str)
    except ValueError:
        await callback.answer("Ошибка данных.", show_alert=True)
        return
    # Проверяем, существует ли задача
    if task_id not in ACTIVE_DOWNLOADS:
        await callback.answer("Задача не найдена или уже завершена.", show_alert=True)
        return
    download_info = ACTIVE_DOWNLOADS[task_id]
    user_id = callback.from_user.id
    # Проверяем, что пользователь владеет этой загрузкой
    if download_info["user_id"] != user_id:
        await callback.answer("Это не ваша загрузка.", show_alert=True)
        return
    if action == "pause":
        # В текущей реализации yt-dlp не поддерживает приостановку
        # Но мы можем отменить загрузку и сохранить прогресс для возобновления
        await callback.answer("Приостановка загрузки не поддерживается. Загрузка будет отменена.")
        # Отменяем текущую загрузку
        if "status_msg_id" in download_info:
            try:
                await bot.edit_message_text(
                    chat_id=callback.message.chat.id,
                    message_id=download_info["status_msg_id"],
                    text="Загрузка отменена по вашему запросу."
                )
            except Exception:
                pass
        # Здесь можно добавить логику для сохранения прогресса и возможности возобновления
        # Но это требует более сложной реализации с поддержкой resumable downloads
        # Удаляем из активных загрузок
        if task_id in ACTIVE_DOWNLOADS:
            del ACTIVE_DOWNLOADS[task_id]
    elif action == "cancel":
        await callback.answer("Загрузка отменена.")
        # Обновляем сообщение
        if "status_msg_id" in download_info:
            try:
                await bot.edit_message_text(
                    chat_id=callback.message.chat.id,
                    message_id=download_info["status_msg_id"],
                    text="Загрузка отменена по вашему запросу."
                )
            except Exception:
                pass
        # Удаляем из активных загрузок
        if task_id in ACTIVE_DOWNLOADS:
            del ACTIVE_DOWNLOADS[task_id]

# Обработчик кнопки "Повторить загрузку"
async def cb_retry(callback: types.CallbackQuery):
    """Обработчик кнопки 'Повторить загрузку'"""
    data = callback.data
    parts = data.split(":")
    if len(parts) < 3:
        await callback.answer("Некорректные данные.", show_alert=True)
        return
    _, mode, retry_id = parts[0], parts[1], parts[2]
    # Получаем URL по уникальному ID
    if retry_id not in RETRY_LINKS:
        await callback.answer("Ссылка устарела. Пожалуйста, отправьте ссылку заново.", show_alert=True)
        return
    url, _ = RETRY_LINKS[retry_id]
    # Добавляем загрузку в менеджер
    await callback.answer("Начинаем повторную загрузку...")
    await download_manager.add_download(callback, url, mode)
    # Удаляем использованный ID из словаря
    if retry_id in RETRY_LINKS:
        del RETRY_LINKS[retry_id]

# ===== ВЕБ-СЕРВЕР ДЛЯ HEALTH CHECK =====
async def health_check(request):
    """Endpoint для проверки работоспособности сервиса"""
    return web.json_response({"status": "ok", "bot": "running"})

async def start_web_server():
    """Запуск веб-сервера для health check"""
    app = web.Application()
    app.router.add_get('/health', health_check)
    runner = web.AppRunner(app)
    await runner.setup()
    # Render использует порт 10000 по умолчанию
    site = web.TCPSite(runner, '0.0.0.0', 10000)
    await site.start()
    logger.info("✅ Веб-сервер для health check запущен на порту 10000")

# ---- lifecycle ----
async def on_startup():
    logger.info("Start polling")
    # Запускаем задачу для очистки RETRY_LINKS
    asyncio.create_task(cleanup_retry_links())
    # Запускаем веб-сервер для health check
    asyncio.create_task(start_web_server())

async def on_shutdown():
    logger.info("Shutting down...")
    await bot.session.close()

async def main():
    # Создаем экземпляры менеджеров
    global download_manager, cache_manager, history_manager
    download_manager = DownloadManager(max_concurrent=3)
    cache_manager = CacheManager()
    history_manager = HistoryManager()

    # Получаем имя бота
    bot_info = await bot.get_me()
    bot_username = bot_info.username.lower()

    # Создаем фильтр
    group_filter = GroupFilter(bot_username)

    # Регистрируем обработчики
    dp.message.register(cmd_start, Command(commands=["start", "help"]), group_filter)
    dp.message.register(cmd_history, Command(commands=["history"]), group_filter)
    dp.message.register(cmd_addnews, Command(commands=["addnews"]), group_filter)
    dp.message.register(handle_text, F.text, group_filter)

    # Колбэки работают всегда (после того, как пользователь начал взаимодействие)
    dp.callback_query.register(cb_download, F.data.startswith("dl:"))
    dp.callback_query.register(cb_history, F.data.startswith("history:"))
    dp.callback_query.register(cb_retry, F.data.startswith("retry:"))
    dp.callback_query.register(cb_progress_control, F.data.startswith("progress:"))

    # Запускаем polling
    try:
        await dp.start_polling(bot, on_startup=on_startup, on_shutdown=on_shutdown)
    finally:
        await bot.session.close()

if __name__ == "__main__":
    asyncio.run(main())

