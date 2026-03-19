print ("ФАЙЛ ЗАПУЩЕН")
import asyncio
import logging
import os
import sqlite3
from contextlib import closing
from datetime import datetime
from typing import Optional

from aiogram import Bot, Dispatcher, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.exceptions import TelegramBadRequest
from aiogram.filters import Command, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message


# =========================================================
# CONFIG
# =========================================================
BOT_TOKEN = os.getenv("BOT_TOKEN", "8637251486:AAHwqEEN9hwzQS0EUAXy9IPgILMisGpc8K4")
ADMIN_IDS = {
    int(x.strip())
    for x in os.getenv("ADMIN_IDS", "8034491282").split(",")
    if x.strip()
}
WITHDRAWALS_CHAT_ID = int(os.getenv("WITHDRAWALS_CHAT_ID", "-5113722562"))
BOT_USERNAME = os.getenv("BOT_USERNAME", "your_bot_username")
REFERRAL_REWARD = float(os.getenv("REFERRAL_REWARD", "0.07"))
MIN_WITHDRAW = float(os.getenv("MIN_WITHDRAW", "2.0"))
DB_PATH = os.getenv("DB_PATH", "referral_bot.db")

if BOT_TOKEN == "PASTE_BOT_TOKEN_HERE":
    raise RuntimeError("Укажи BOT_TOKEN в переменных окружения")

logging.basicConfig(level=logging.INFO)


# =========================================================
# DATABASE
# =========================================================
class Database:
    def __init__(self, path: str):
        self.path = path
        self._init_db()

    def _connect(self):
        conn = sqlite3.connect(self.path)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_db(self):
        with closing(self._connect()) as conn, conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS users (
                    user_id INTEGER PRIMARY KEY,
                    username TEXT,
                    full_name TEXT,
                    balance REAL NOT NULL DEFAULT 0,
                    hold_balance REAL NOT NULL DEFAULT 0,
                    referred_by INTEGER,
                    referrals_count INTEGER NOT NULL DEFAULT 0,
                    created_at TEXT NOT NULL,
                    is_blocked INTEGER NOT NULL DEFAULT 0
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS referrals (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    inviter_id INTEGER NOT NULL,
                    invited_id INTEGER NOT NULL UNIQUE,
                    reward REAL NOT NULL,
                    created_at TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS sponsor_tasks (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    title TEXT NOT NULL,
                    description TEXT,
                    join_url TEXT NOT NULL,
                    channel_id TEXT NOT NULL,
                    reward REAL NOT NULL,
                    is_active INTEGER NOT NULL DEFAULT 1,
                    created_at TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS user_task_completions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    task_id INTEGER NOT NULL,
                    reward REAL NOT NULL,
                    completed_at TEXT NOT NULL,
                    UNIQUE(user_id, task_id)
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS withdrawals (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    amount REAL NOT NULL,
                    method TEXT NOT NULL,
                    requisites TEXT NOT NULL,
                    status TEXT NOT NULL,
                    admin_id INTEGER,
                    admin_note TEXT,
                    created_at TEXT NOT NULL,
                    processed_at TEXT,
                    channel_message_id INTEGER
                )
                """
            )

    def add_or_get_user(self, user_id: int, username: Optional[str], full_name: str):
        now = datetime.utcnow().isoformat()
        with closing(self._connect()) as conn, conn:
            row = conn.execute(
                "SELECT * FROM users WHERE user_id = ?", (user_id,)
            ).fetchone()
            if row:
                conn.execute(
                    "UPDATE users SET username = ?, full_name = ? WHERE user_id = ?",
                    (username, full_name, user_id),
                )
                return dict(row), False

            conn.execute(
                """
                INSERT INTO users (user_id, username, full_name, created_at)
                VALUES (?, ?, ?, ?)
                """,
                (user_id, username, full_name, now),
            )
            row = conn.execute(
                "SELECT * FROM users WHERE user_id = ?", (user_id,)
            ).fetchone()
            return dict(row), True

    def get_user(self, user_id: int):
        with closing(self._connect()) as conn:
            row = conn.execute(
                "SELECT * FROM users WHERE user_id = ?", (user_id,)
            ).fetchone()
            return dict(row) if row else None

    def bind_referral(self, invited_id: int, inviter_id: int) -> bool:
        if invited_id == inviter_id:
            return False

        now = datetime.utcnow().isoformat()
        with closing(self._connect()) as conn, conn:
            invited = conn.execute(
                "SELECT * FROM users WHERE user_id = ?", (invited_id,)
            ).fetchone()
            inviter = conn.execute(
                "SELECT * FROM users WHERE user_id = ?", (inviter_id,)
            ).fetchone()

            if not invited or not inviter:
                return False
            if invited["referred_by"] is not None:
                return False

            existing = conn.execute(
                "SELECT 1 FROM referrals WHERE invited_id = ?", (invited_id,)
            ).fetchone()
            if existing:
                return False

            conn.execute(
                "UPDATE users SET referred_by = ? WHERE user_id = ?",
                (inviter_id, invited_id),
            )
            conn.execute(
                "UPDATE users SET balance = balance + ?, referrals_count = referrals_count + 1 WHERE user_id = ?",
                (REFERRAL_REWARD, inviter_id),
            )
            conn.execute(
                """
                INSERT INTO referrals (inviter_id, invited_id, reward, created_at)
                VALUES (?, ?, ?, ?)
                """,
                (inviter_id, invited_id, REFERRAL_REWARD, now),
            )
            return True

    def get_stats(self, user_id: int):
        with closing(self._connect()) as conn:
            user = conn.execute(
                "SELECT * FROM users WHERE user_id = ?", (user_id,)
            ).fetchone()
            if not user:
                return None
            completed_tasks = conn.execute(
                "SELECT COUNT(*) AS cnt FROM user_task_completions WHERE user_id = ?",
                (user_id,),
            ).fetchone()["cnt"]
            approved_withdrawals = conn.execute(
                "SELECT COALESCE(SUM(amount), 0) AS total FROM withdrawals WHERE user_id = ? AND status = 'approved'",
                (user_id,),
            ).fetchone()["total"]
            return {
                "balance": user["balance"],
                "hold_balance": user["hold_balance"],
                "referrals_count": user["referrals_count"],
                "completed_tasks": completed_tasks,
                "approved_withdrawals": approved_withdrawals,
            }

    def get_active_tasks(self):
        with closing(self._connect()) as conn:
            rows = conn.execute(
                "SELECT * FROM sponsor_tasks WHERE is_active = 1 ORDER BY id DESC"
            ).fetchall()
            return [dict(r) for r in rows]

    def get_task(self, task_id: int):
        with closing(self._connect()) as conn:
            row = conn.execute(
                "SELECT * FROM sponsor_tasks WHERE id = ?", (task_id,)
            ).fetchone()
            return dict(row) if row else None

    def create_task(self, title: str, description: str, join_url: str, channel_id: str, reward: float):
        now = datetime.utcnow().isoformat()
        with closing(self._connect()) as conn, conn:
            cur = conn.execute(
                """
                INSERT INTO sponsor_tasks (title, description, join_url, channel_id, reward, created_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (title, description, join_url, channel_id, reward, now),
            )
            return cur.lastrowid

    def deactivate_task(self, task_id: int) -> bool:
        with closing(self._connect()) as conn, conn:
            cur = conn.execute(
                "UPDATE sponsor_tasks SET is_active = 0 WHERE id = ? AND is_active = 1",
                (task_id,),
            )
            return cur.rowcount > 0

    def has_completed_task(self, user_id: int, task_id: int) -> bool:
        with closing(self._connect()) as conn:
            row = conn.execute(
                "SELECT 1 FROM user_task_completions WHERE user_id = ? AND task_id = ?",
                (user_id, task_id),
            ).fetchone()
            return bool(row)

    def complete_task(self, user_id: int, task_id: int, reward: float) -> bool:
        now = datetime.utcnow().isoformat()
        with closing(self._connect()) as conn, conn:
            exists = conn.execute(
                "SELECT 1 FROM user_task_completions WHERE user_id = ? AND task_id = ?",
                (user_id, task_id),
            ).fetchone()
            if exists:
                return False

            conn.execute(
                """
                INSERT INTO user_task_completions (user_id, task_id, reward, completed_at)
                VALUES (?, ?, ?, ?)
                """,
                (user_id, task_id, reward, now),
            )
            conn.execute(
                "UPDATE users SET balance = balance + ? WHERE user_id = ?",
                (reward, user_id),
            )
            return True

    def create_withdrawal(self, user_id: int, amount: float, method: str, requisites: str) -> int:
        now = datetime.utcnow().isoformat()
        with closing(self._connect()) as conn, conn:
            user = conn.execute(
                "SELECT balance FROM users WHERE user_id = ?", (user_id,)
            ).fetchone()
            if not user or user["balance"] < amount:
                raise ValueError("Недостаточно средств")
            if amount < MIN_WITHDRAW:
                raise ValueError("Сумма меньше минимального вывода")

            conn.execute(
                "UPDATE users SET balance = balance - ?, hold_balance = hold_balance + ? WHERE user_id = ?",
                (amount, amount, user_id),
            )
            cur = conn.execute(
                """
                INSERT INTO withdrawals (user_id, amount, method, requisites, status, created_at)
                VALUES (?, ?, ?, ?, 'pending', ?)
                """,
                (user_id, amount, method, requisites, now),
            )
            return cur.lastrowid

    def set_withdrawal_channel_message(self, withdrawal_id: int, message_id: int):
        with closing(self._connect()) as conn, conn:
            conn.execute(
                "UPDATE withdrawals SET channel_message_id = ? WHERE id = ?",
                (message_id, withdrawal_id),
            )

    def get_withdrawal(self, withdrawal_id: int):
        with closing(self._connect()) as conn:
            row = conn.execute(
                "SELECT * FROM withdrawals WHERE id = ?", (withdrawal_id,)
            ).fetchone()
            return dict(row) if row else None

    def process_withdrawal(self, withdrawal_id: int, admin_id: int, approve: bool, admin_note: str = ""):
        now = datetime.utcnow().isoformat()
        with closing(self._connect()) as conn, conn:
            wd = conn.execute(
                "SELECT * FROM withdrawals WHERE id = ?", (withdrawal_id,)
            ).fetchone()
            if not wd:
                raise ValueError("Заявка не найдена")
            if wd["status"] != "pending":
                raise ValueError("Заявка уже обработана")

            if approve:
                conn.execute(
                    "UPDATE users SET hold_balance = hold_balance - ? WHERE user_id = ?",
                    (wd["amount"], wd["user_id"]),
                )
                conn.execute(
                    """
                    UPDATE withdrawals
                    SET status = 'approved', admin_id = ?, admin_note = ?, processed_at = ?
                    WHERE id = ?
                    """,
                    (admin_id, admin_note, now, withdrawal_id),
                )
            else:
                conn.execute(
                    "UPDATE users SET hold_balance = hold_balance - ?, balance = balance + ? WHERE user_id = ?",
                    (wd["amount"], wd["amount"], wd["user_id"]),
                )
                conn.execute(
                    """
                    UPDATE withdrawals
                    SET status = 'rejected', admin_id = ?, admin_note = ?, processed_at = ?
                    WHERE id = ?
                    """,
                    (admin_id, admin_note, now, withdrawal_id),
                )

    def get_top_users(self, limit: int = 10):
        with closing(self._connect()) as conn:
            rows = conn.execute(
                "SELECT user_id, username, full_name, referrals_count, balance FROM users ORDER BY referrals_count DESC, balance DESC LIMIT ?",
                (limit,),
            ).fetchall()
            return [dict(r) for r in rows]


db = Database(DB_PATH)


# =========================================================
# KEYBOARDS
# =========================================================
def main_menu():
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="👥 Рефералы", callback_data="menu_ref")],
            [InlineKeyboardButton(text="💼 Баланс", callback_data="menu_balance")],
            [InlineKeyboardButton(text="🎯 Задания", callback_data="menu_tasks")],
            [InlineKeyboardButton(text="💸 Вывод", callback_data="menu_withdraw")],
            [InlineKeyboardButton(text="🏆 Топ", callback_data="menu_top")],
        ]
    )


def back_menu():
    return InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="⬅️ В меню", callback_data="back_main")]]
    )


def withdrawal_moderation_kb(withdrawal_id: int):
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="✅ Одобрить", callback_data=f"wd_approve:{withdrawal_id}"
                ),
                InlineKeyboardButton(
                    text="❌ Отклонить", callback_data=f"wd_reject:{withdrawal_id}"
                ),
            ]
        ]
    )


def task_card_kb(task_id: int, join_url: str):
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🔗 Подписаться", url=join_url)],
            [InlineKeyboardButton(text="✅ Проверить", callback_data=f"check_task:{task_id}")],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data="menu_tasks")],
        ]
    )


# =========================================================
# STATES
# =========================================================
class WithdrawStates(StatesGroup):
    waiting_method = State()
    waiting_requisites = State()
    waiting_amount = State()


class CreateTaskStates(StatesGroup):
    waiting_payload = State()


# =========================================================
# HELPERS
# =========================================================
def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS


def money(value: float) -> str:
    return f"{value:.2f}$"


def user_link(user_id: int, full_name: str) -> str:
    safe = full_name.replace("<", "").replace(">", "")
    return f'<a href="tg://user?id={user_id}">{safe}</a>'


async def safe_edit(target, text: str, reply_markup=None):
    try:
        await target.edit_text(text, reply_markup=reply_markup)
    except TelegramBadRequest:
        await target.answer(text, reply_markup=reply_markup)


async def check_subscription(bot: Bot, user_id: int, channel_id: str) -> bool:
    try:
        member = await bot.get_chat_member(chat_id=channel_id, user_id=user_id)
        return member.status in {"member", "administrator", "creator"}
    except Exception:
        return False


async def render_main(message: Message | CallbackQuery):
    text = (
        "<b>Главное меню</b>\n\n"
        "Добро пожаловать в реферального бота.\n"
        f"• Награда за реферала: <b>{money(REFERRAL_REWARD)}</b>\n"
        f"• Минимальный вывод: <b>{money(MIN_WITHDRAW)}</b>\n\n"
        "Выбери раздел ниже."
    )
    if isinstance(message, CallbackQuery):
        await safe_edit(message.message, text, main_menu())
        await message.answer()
    else:
        await message.answer(text, reply_markup=main_menu())


# =========================================================
# ROUTER
# =========================================================
router = Router()


@router.message(CommandStart())
async def start_handler(message: Message):
    args = message.text.split(maxsplit=1)
    ref_arg = None
    if len(args) > 1:
        ref_arg = args[1].strip()

    _, created = db.add_or_get_user(
        user_id=message.from_user.id,
        username=message.from_user.username,
        full_name=message.from_user.full_name,
    )

    referral_text = ""
    if ref_arg and ref_arg.startswith("ref_"):
        try:
            inviter_id = int(ref_arg.replace("ref_", ""))
            bound = db.bind_referral(message.from_user.id, inviter_id)
            if bound:
                referral_text = (
                    f"\n🎉 Ты зарегистрировался по реферальной ссылке. "
                    f"Пригласивший получил {money(REFERRAL_REWARD)}."
                )
                try:
                    await message.bot.send_message(
                        inviter_id,
                        (
                            f"🎉 У тебя новый реферал: {user_link(message.from_user.id, message.from_user.full_name)}\n"
                            f"Начислено: <b>{money(REFERRAL_REWARD)}</b>"
                        ),
                    )
                except Exception:
                    pass
        except ValueError:
            pass

    welcome = (
        "<b>Бот запущен</b>\n\n"
        f"За каждого приглашённого пользователя начисляется <b>{money(REFERRAL_REWARD)}</b>."
        f"\nМинимальный вывод: <b>{money(MIN_WITHDRAW)}</b>."
        f"{referral_text}"
    )
    if created:
        welcome += "\n\nТы зарегистрирован в системе."

    await message.answer(welcome, reply_markup=main_menu())


@router.message(Command("menu"))
async def menu_cmd(message: Message):
    db.add_or_get_user(message.from_user.id, message.from_user.username, message.from_user.full_name)
    await render_main(message)


@router.callback_query(F.data == "back_main")
async def back_main(call: CallbackQuery):
    await render_main(call)


@router.callback_query(F.data == "menu_balance")
async def menu_balance(call: CallbackQuery):
    stats = db.get_stats(call.from_user.id)
    text = (
        "<b>Твой баланс</b>\n\n"

        
@router.message()
async def test(message: Message):
    await message.answer("Я жив")
