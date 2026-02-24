# bot.py — ПОЛНАЯ ВЕРСИЯ С РАСХОДАМИ (ТОЛЬКО ОТОБРАЖЕНИЕ, БЕЗ ВЫЧИТАНИЯ ИЗ ВЫРУЧКИ)
import asyncio
import json
import re
import requests
from datetime import datetime, timedelta
from typing import Optional

from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command, StateFilter
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, ReplyKeyboardMarkup, KeyboardButton
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.context import FSMContext
from aiogram.fsm.storage.memory import MemoryStorage

from config import TOKEN, OWNERS, ALLOWED_CHATS, EXCLUDED_FROM_REPORTS
try:
    from config import CRYPTO_WALLET, CRYPTO_CHAT
except ImportError:
    CRYPTO_WALLET = ""
    CRYPTO_CHAT = 0

try:
    from config import CRYPTO_TOPIC
except ImportError:
    CRYPTO_TOPIC = None

try:
    from config import OPERATORS, GOOGLE_SHEET_ID, GOOGLE_SHEET_NAME, GOOGLE_CREDS_FILE
except ImportError:
    OPERATORS = {}
    GOOGLE_SHEET_ID = ""
    GOOGLE_SHEET_NAME = "xGeorgia"
    GOOGLE_CREDS_FILE = "google_creds.json"

storage = MemoryStorage()
dp = Dispatcher(storage=storage)
bot = Bot(token=TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))

DATA_FILE = "data.json"
HISTORY_FILE = "history.json"
SETTINGS_FILE = "settings.json"
data = {"chats": {}}
history = {"shifts": []}  # архив смен для отчётов
settings = {}  # ручные курсы и проценты

# Fallback курсы
FALLBACK_LARI_TO_USD = 0.37
FALLBACK_EURO_TO_USD = 1.05
FALLBACK_AMD_TO_USD = 0.0025

current_lari_to_usd = FALLBACK_LARI_TO_USD
current_euro_to_usd = FALLBACK_EURO_TO_USD
current_amd_to_usd = FALLBACK_AMD_TO_USD


def update_exchange_rates():
    global current_lari_to_usd, current_euro_to_usd, current_amd_to_usd
    # Ручные курсы из settings имеют приоритет
    manual = settings.get("rates", {})
    if manual:
        if manual.get("lari"):
            current_lari_to_usd = manual["lari"]
        if manual.get("euro"):
            current_euro_to_usd = manual["euro"]
        if manual.get("amd"):
            current_amd_to_usd = manual["amd"]
        return
    try:
        response = requests.get("https://api.exchangerate.host/latest?base=USD", timeout=5)
        if response.status_code == 200:
            rates = response.json()["rates"]
            current_lari_to_usd = 1 / rates.get("GEL", 1 / FALLBACK_LARI_TO_USD)
            current_euro_to_usd = rates.get("EUR", FALLBACK_EURO_TO_USD)
            current_amd_to_usd = 1 / rates.get("AMD", 1 / FALLBACK_AMD_TO_USD)
    except:
        pass


# ==================== ПРОЦЕНТЫ ЗП ПО ИМЕНАМ ====================
SALARY_PERCENT = {
    "Саша": 0.12,
    "Света": 0.12,
}

DEFAULT_PERCENT = 0.10


# ==================== ПРОЦЕНТЫ ЗП АДМИНОВ ====================
ADMIN_SALARY_PERCENT = {
    "Иван": 0.05,
    "Марта": 0.03,
    "Софа": 0.015,
}

DEFAULT_ADMIN_PERCENT = 0.03


def get_admin_salary_percent(name: str) -> float:
    """Берёт процент админа из settings, если есть, иначе из ADMIN_SALARY_PERCENT."""
    custom = settings.get("admin_salary_percent", {})
    if name in custom:
        return custom[name]
    return ADMIN_SALARY_PERCENT.get(name, settings.get("default_admin_percent", DEFAULT_ADMIN_PERCENT))


def get_salary_percent(name: str) -> float:
    """Берёт процент из settings, если есть, иначе из SALARY_PERCENT / DEFAULT_PERCENT."""
    custom = settings.get("salary_percent", {})
    if name in custom:
        return custom[name]
    return SALARY_PERCENT.get(name, settings.get("default_percent", DEFAULT_PERCENT))


# ==================== СМЕНА 09:00 → 08:59 + НАЗВАНИЕ ЧАТА ====================
async def get_shift_info(chat_id: int) -> tuple[str, str]:
    now = datetime.now()
    if now.hour < 9:
        shift_start = now - timedelta(days=1)
    else:
        shift_start = now
    date_str = shift_start.strftime("%d.%m.%Y")

    chat_title = "Салон"
    try:
        chat = await bot.get_chat(chat_id)
        chat_title = (chat.title or chat.first_name or "Салон").strip()
    except:
        pass
    return date_str, chat_title or "Салон"


async def ensure_chat(chat_id: int):
    s = str(chat_id)
    current_date, chat_title = await get_shift_info(chat_id)

    if s not in data["chats"]:
        data["chats"][s] = {
            "bookings": [],
            "expenses": [],
            "board_msg": None,
            "date": current_date,
            "chat_title": chat_title,
            "next_id": 1,
        }
    else:
        if "expenses" not in data["chats"][s]:
            data["chats"][s]["expenses"] = []

        if data["chats"][s]["date"] != current_date:
            # Архивируем старую смену перед сбросом
            archive_shift(s)
            data["chats"][s]["bookings"] = []
            data["chats"][s]["expenses"] = []
            data["chats"][s]["date"] = current_date
            data["chats"][s]["next_id"] = 1
        data["chats"][s]["chat_title"] = chat_title


# ==================== STORAGE ====================
def load_data():
    global data
    try:
        with open(DATA_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
    except FileNotFoundError:
        data = {"chats": {}}


def save_data():
    with open(DATA_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def load_history():
    global history
    try:
        with open(HISTORY_FILE, "r", encoding="utf-8") as f:
            history = json.load(f)
    except FileNotFoundError:
        history = {"shifts": []}


def save_history():
    with open(HISTORY_FILE, "w", encoding="utf-8") as f:
        json.dump(history, f, ensure_ascii=False, indent=2)


def load_settings():
    global settings
    try:
        with open(SETTINGS_FILE, "r", encoding="utf-8") as f:
            settings = json.load(f)
    except FileNotFoundError:
        settings = {}


def save_settings():
    with open(SETTINGS_FILE, "w", encoding="utf-8") as f:
        json.dump(settings, f, ensure_ascii=False, indent=2)


load_data()
load_history()
load_settings()


class EditState(StatesGroup):
    waiting_for_new_text = State()


class ReportState(StatesGroup):
    waiting_for_period = State()
    waiting_for_operator = State()


class ExpenseState(StatesGroup):
    waiting_for_date = State()
    waiting_for_input = State()
    waiting_for_period_view = State()


class SettingsState(StatesGroup):
    waiting_for_rate = State()
    waiting_for_percent = State()
    waiting_for_admin_percent = State()


class AnketaState(StatesGroup):
    waiting_for_phone = State()


class OperatorSalaryState(StatesGroup):
    waiting_for_period = State()


# ==================== ГЛОБАЛЬНЫЕ РАСХОДЫ (expenses.json) ====================
EXPENSES_FILE = "expenses.json"
global_expenses = []  # [{chat_id, chat_title, date, type, amount, currency, amount_usd, comment, created_at, id}]


def load_expenses():
    global global_expenses
    try:
        with open(EXPENSES_FILE, "r", encoding="utf-8") as f:
            global_expenses = json.load(f)
    except FileNotFoundError:
        global_expenses = []


def save_expenses():
    with open(EXPENSES_FILE, "w", encoding="utf-8") as f:
        json.dump(global_expenses, f, ensure_ascii=False, indent=2)


def next_expense_id() -> int:
    if not global_expenses:
        return 1
    return max(e.get("id", 0) for e in global_expenses) + 1


def get_expenses_for_period(date_from, date_to, chat_id=None):
    result = []
    for e in global_expenses:
        d = parse_date_str(e.get("date", ""))
        if not d:
            continue
        if date_from <= d <= date_to:
            if chat_id is None or str(e.get("chat_id", "")) == str(chat_id):
                result.append(e)
    return result


def expense_to_usd(amount, currency):
    currency = currency.lower()
    if any(x in currency for x in ["лари", "лар", "lari", "gel"]):
        return amount * current_lari_to_usd
    elif any(x in currency for x in ["доллар", "dollar", "usd", "$"]):
        return amount
    elif any(x in currency for x in ["евро", "euro", "€", "eur"]):
        return amount * current_euro_to_usd
    elif any(x in currency for x in ["драм", "dram", "amd", "֏"]):
        return amount * current_amd_to_usd
    return amount


load_expenses()


def parse_duration(text: str) -> tuple[int, str]:
    text = (text or "").lower().strip()
    hours = minutes = 0
    h = re.search(r"(\d+)\s*(ч|час)", text)
    if h: hours = int(h.group(1))
    m = re.search(r"(\d+)\s*(мин|минут|м)", text)
    if m: minutes = int(m.group(1))
    seconds = hours * 3600 + minutes * 60
    pretty = []
    if hours: pretty.append(f"{hours}ч")
    if minutes: pretty.append(f"{minutes}мин" if hours else f"{minutes} мин")
    return seconds, " ".join(pretty) or "30 мин"


def find_booking_index(chat_str: str, bid: int) -> Optional[int]:
    for i, b in enumerate(data["chats"][chat_str]["bookings"]):
        if b.get("id") == bid:
            return i
    return None


def time_key(time_str: str) -> int:
    hh, mm = map(int, time_str.split(':'))
    minutes = hh * 60 + mm
    if hh < 9:
        minutes += 24 * 60
    return minutes


def personal_kb(bid: int, done: bool = False, cancelled: bool = False, deleted: bool = False):
    row1 = []
    row2 = []
    if not deleted:
        row1.append(InlineKeyboardButton(text="Пришёл", callback_data=f"done:{bid}"))
        if not cancelled:
            row1.append(InlineKeyboardButton(text="Не пришёл", callback_data=f"cancel:{bid}"))
        row2.append(InlineKeyboardButton(text="Отменить бронь", callback_data=f"delete:{bid}"))
        row2.append(InlineKeyboardButton(text="Редактировать", callback_data=f"edit:{bid}"))
    kb = [row1] if row1 else []
    if row2: kb.append(row2)
    return InlineKeyboardMarkup(inline_keyboard=kb) if kb else None


async def refresh_board(chat_id: int):
    chat_str = str(chat_id)
    await ensure_chat(chat_id)
    chat_data = data["chats"][chat_str]
    bookings = sorted(chat_data["bookings"], key=lambda x: time_key(x["time"]))

    header = f"<b>Брони на {chat_data['date']} — {chat_data['chat_title']} (смена)</b>\n"
    lines = [header]
    if not bookings:
        lines.append("<i>Пока нет броней</i>")
    else:
        for i, b in enumerate(bookings, 1):
            text = f"{i}. {b['time']} — {b['info']} ({b['duration']})"
            if b.get("done"):
                text += " Пришёл"
            if b.get("deleted"):
                text = f"<s>{text} Отменено</s>"
            elif b.get("cancelled"):
                text = f"<s>{text} Не пришёл</s>"
            lines.append(text)

    full_text = "\n".join(lines)
    msg_id = chat_data.get("board_msg")

    # Удаляем старое сообщение-доску и постим новое внизу
    if msg_id:
        try:
            await bot.delete_message(chat_id, msg_id)
        except:
            pass

    msg = await bot.send_message(chat_id, full_text, parse_mode=ParseMode.HTML)
    chat_data["board_msg"] = msg.message_id
    save_data()


# ==================== ТАЙМЕР ====================
async def booking_timer(chat_id: int, bid: int):
    chat_str = str(chat_id)
    idx = find_booking_index(chat_str, bid)
    if idx is None: return
    b = data["chats"][chat_str]["bookings"][idx]
    if b.get("deleted") or b.get("cancelled"): return

    mins = max(1, b.get("duration_sec", 1800) // 60)
    try:
        start_msg = await bot.send_message(chat_id, f"Таймер запущен\n{b['time']} — {b['info']} — {mins} мин", parse_mode=ParseMode.HTML)
        await asyncio.sleep(b.get("duration_sec", 1800))
        await bot.send_message(chat_id, f"Время вышло!\n{b['time']} — {b['info']}", parse_mode=ParseMode.HTML)
        await asyncio.sleep(25)
        await bot.delete_message(chat_id, start_msg.message_id)
    except:
        pass


# ==================== ДОБАВЛЕНИЕ БРОНИ ====================
@dp.message(F.text.regexp(r"^\d{1,2}:\d{2}"), ~StateFilter(EditState.waiting_for_new_text))
async def add_booking(m: types.Message, state: FSMContext):
    if m.chat.id not in ALLOWED_CHATS: return
    text = m.text.strip()
    if len(text.split()) < 2: return

    time_part = text.split(maxsplit=1)[0]
    rest = text[len(time_part):].strip()
    sec, pretty = parse_duration(rest)
    info = re.sub(r"\d+\s*(ч|час|мин|минут|м)\b.*$", "", rest, flags=re.I).strip() or "Без имени"

    chat_str = str(m.chat.id)
    await ensure_chat(m.chat.id)
    bid = data["chats"][chat_str]["next_id"]
    data["chats"][chat_str]["next_id"] += 1

    booking = {
        "id": bid, "time": time_part, "info": info, "duration": pretty, "duration_sec": sec,
        "author_id": m.from_user.id, "done": False, "cancelled": False, "deleted": False,
        "reply_msg_id": None,
        "original_text": text.strip(),
    }
    data["chats"][chat_str]["bookings"].append(booking)
    save_data()

    sorted_b = sorted(data["chats"][chat_str]["bookings"], key=lambda x: time_key(x["time"]))
    pos = next((i + 1 for i, b in enumerate(sorted_b) if b["id"] == bid), 0)

    reply = await m.reply(f"Добавлено!\n{pos}. {time_part} — {info} ({pretty})", reply_markup=personal_kb(bid))
    booking["reply_msg_id"] = reply.message_id
    save_data()
    await refresh_board(m.chat.id)


# ==================== ДЕЙСТВИЯ ====================
@dp.callback_query(F.data.startswith(("done:", "cancel:", "delete:")))
async def actions(c: types.CallbackQuery):
    await c.answer()
    action, payload = c.data.split(":", 1)
    bid = int(payload)
    chat_id = c.message.chat.id
    chat_str = str(chat_id)
    await ensure_chat(chat_id)
    idx = find_booking_index(chat_str, bid)
    if idx is None:
        await c.answer("Бронь не найдена.", show_alert=True)
        return
    b = data["chats"][chat_str]["bookings"][idx]
    if c.from_user.id not in (b["author_id"], *OWNERS):
        await c.answer("Это не твоя бронь!", show_alert=True)
        return

    if action == "done":
        if not b.get("done"):
            b["done"] = True
            b["cancelled"] = False
            asyncio.create_task(booking_timer(chat_id, bid))
            await c.answer("Клиент пришёл — таймер запущен!", show_alert=True)
    elif action == "cancel":
        b["cancelled"] = True
        b["done"] = False
    elif action == "delete":
        b["deleted"] = True

    save_data()
    await refresh_board(chat_id)
    if b.get("reply_msg_id"):
        try:
            await bot.edit_message_reply_markup(chat_id, b["reply_msg_id"], reply_markup=personal_kb(bid, b.get("done"), b.get("cancelled"), b.get("deleted")))
        except:
            b["reply_msg_id"] = None
            save_data()


# ==================== РЕДАКТИРОВАНИЕ ====================
@dp.callback_query(F.data.startswith("edit:"))
async def start_edit(c: types.CallbackQuery, state: FSMContext):
    await c.answer()
    bid = int(c.data.split(":", 1)[1])
    chat_str = str(c.message.chat.id)
    await ensure_chat(c.message.chat.id)
    idx = find_booking_index(chat_str, bid)
    if idx is None:
        await c.answer("Бронь не найдена.", show_alert=True)
        return
    b = data["chats"][chat_str]["bookings"][idx]
    if c.from_user.id not in (b["author_id"], *OWNERS):
        await c.answer("Это не твоя бронь! Редактировать нельзя.", show_alert=True)
        return

    await state.update_data(edit_bid=bid, reply_msg_id=b.get("reply_msg_id"))
    await state.set_state(EditState.waiting_for_new_text)

    cancel_kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Отменить редактирование", callback_data="cancel_edit")]
    ])

    await c.message.reply(
        "<b>Редактируй бронь:</b>\n\n"
        f"<b>Текущая:</b> <code>{b['time']} {b['info']} {b['duration']}</code>\n\n"
        "<b>Пиши в формате:</b>\n"
        "<code>18:30 Анна 1ч 30мин</code>\n"
        "<code>15:00 Иван 300 лари</code>\n\n"
        "<i>или нажми кнопку ниже</i>",
        parse_mode=ParseMode.HTML,
        reply_markup=cancel_kb
    )


@dp.callback_query(F.data == "cancel_edit")
async def cancel_edit_callback(c: types.CallbackQuery, state: FSMContext):
    await state.clear()
    await c.answer("Редактирование отменено", show_alert=True)
    await c.message.edit_text("Редактирование отменено.")


@dp.message(StateFilter(EditState.waiting_for_new_text))
async def apply_edit(m: types.Message, state: FSMContext):
    user_data = await state.get_data()
    bid = user_data.get("edit_bid")
    chat_str = str(m.chat.id)
    await ensure_chat(m.chat.id)
    idx = find_booking_index(chat_str, bid)
    if idx is None:
        await m.reply("Бронь уже удалена.")
        await state.clear()
        return
    b = data["chats"][chat_str]["bookings"][idx]
    if m.from_user.id not in (b["author_id"], *OWNERS):
        await m.reply("Это не твоя бронь!")
        await state.clear()
        return

    new_text = m.text.strip()
    if not re.match(r"^\d{1,2}:\d{2}", new_text):
        await m.reply("Начни с времени: 17:30 ...")
        return

    time_part = new_text.split(maxsplit=1)[0]
    rest = new_text[len(time_part):].strip()
    sec, pretty = parse_duration(rest)
    info = re.sub(r"\d+\s*(ч|час|мин|минут|м)\b.*$", "", rest, flags=re.I).strip() or "Без имени"

    b.update({"time": time_part, "info": info, "duration": pretty, "duration_sec": sec,
              "original_text": new_text,
              "done": False, "cancelled": False, "deleted": False})
    save_data()

    sorted_b = sorted(data["chats"][chat_str]["bookings"], key=lambda x: time_key(x["time"]))
    pos = next((i + 1 for i, bb in enumerate(sorted_b) if bb["id"] == bid), 0)
    reply_text = f"Обновлено!\n{pos}. {time_part} — {info} ({pretty})"

    if b.get("reply_msg_id"):
        try:
            await bot.edit_message_text(chat_id=m.chat.id, message_id=b["reply_msg_id"],
                                        text=reply_text, reply_markup=personal_kb(bid))
        except:
            r = await m.reply(reply_text, reply_markup=personal_kb(bid))
            b["reply_msg_id"] = r.message_id
    else:
        r = await m.reply(reply_text, reply_markup=personal_kb(bid))
        b["reply_msg_id"] = r.message_id

    save_data()
    await m.reply("Бронь обновлена!")

    try:
        await m.reply_to_message.delete()
    except:
        pass

    await refresh_board(m.chat.id)
    await state.clear()


# ==================== /cancel ====================
@dp.message(Command("cancel"))
async def cmd_cancel(m: types.Message, state: FSMContext):
    current_state = await state.get_state()
    if current_state:
        await state.clear()
        await m.reply("Редактирование отменено.")
    else:
        await m.reply("Нечего отменять.")


# ==================== КОМАНДА /expense — ТОЛЬКО ДЛЯ ВЛАДЕЛЬЦЕВ ====================
@dp.message(Command("expense"))
async def cmd_expense(m: types.Message):
    if m.from_user.id not in OWNERS:
        await m.reply("Ты не владелец")
        return

    text = m.text[len("/expense"):].strip()
    if not text:
        await m.reply("Использование: /expense <тип> <сумма> [комментарий]\nПример: /expense квартира 500")
        return

    parts = text.split(maxsplit=2)
    if len(parts) < 2:
        await m.reply("Укажи тип и сумму\nПример: /expense билет 200")
        return

    exp_type = parts[0].capitalize()
    try:
        amount = float(parts[1])
    except ValueError:
        await m.reply("Сумма должна быть числом")
        return

    comment = parts[2] if len(parts) > 2 else ""

    chat_str = str(m.chat.id)
    await ensure_chat(m.chat.id)

    expense = {
        "type": exp_type,
        "amount": amount,
        "comment": comment,
        "author_id": m.from_user.id
    }
    data["chats"][chat_str]["expenses"].append(expense)
    save_data()

    await m.reply(f"Добавлен расход: {exp_type} {amount:.2f} USD\n{comment if comment else ''}")


# ==================== ОБЩАЯ ФУНКЦИЯ ДЛЯ ГЕНЕРАЦИИ СООБЩЕНИЯ ====================
async def generate_summary_text(chat_str: str) -> str:
    update_exchange_rates()

    chat_data = data["chats"][chat_str]
    bookings = sorted(chat_data["bookings"], key=lambda x: time_key(x["time"]))

    header = f"<b>Брони на {chat_data['date']} — {chat_data['chat_title']} (смена)</b>\n\n"
    lines = []
    if not bookings:
        lines.append("<i>Пока нет броней</i>")
    else:
        for i, b in enumerate(bookings, 1):
            text = f"{i}. {b['time']} — {b['info']} ({b['duration']})"
            if b.get("done"):
                text += " Пришёл"
            if b.get("deleted"):
                text = f"<s>{text} Отменено</s>"
            elif b.get("cancelled"):
                text = f"<s>{text} Не пришёл</s>"
            lines.append(text)

    board_text = "\n".join(lines)

    totals = {"лари": 0, "доллар": 0, "евро": 0, "крипта": 0, "драм": 0}
    came = [b for b in bookings if b.get("done")]

    total_usd = 0
    for b in came:
        text = (b.get("original_text") or b["info"] or "").lower()
        matches = re.findall(r"(\d+)\s*(лари|лaри|лар|lari|доллар[аов]?|dollar|usd|\$|евро|euro|€|крипта|crypto|usdt|btc|eth|драм|драмм|драмов|драма|dram|amd|֏)", text)
        for amount_str, currency in matches:
            amt = int(amount_str)
            currency = currency.lower()
            if any(x in currency for x in ["лари", "лар", "lari"]):
                totals["лари"] += amt
                total_usd += amt * current_lari_to_usd
            elif any(x in currency for x in ["доллар", "dollar", "usd", "$"]):
                totals["доллар"] += amt
                total_usd += amt
            elif any(x in currency for x in ["евро", "euro", "€"]):
                totals["евро"] += amt
                total_usd += amt * current_euro_to_usd
            elif any(x in currency for x in ["крипта", "crypto", "usdt", "btc", "eth"]):
                totals["крипта"] += amt
                total_usd += amt
            elif any(x in currency for x in ["драм", "драмм", "драмов", "драма", "dram", "amd", "֏"]):
                totals["драм"] += amt
                total_usd += amt * current_amd_to_usd

    result = "\n\n<b>Общие итоги смены:</b>\n"
    has_money = False
    if totals["лари"]:
        half = totals["лари"] / 2
        result += f"Лари: {totals['лари']} (на двоих: {half:.0f})\n"
        has_money = True
    if totals["доллар"]:
        half = totals["доллар"] / 2
        result += f"Доллары: {totals['доллар']} (на двоих: {half:.2f})\n"
        has_money = True
    if totals["евро"]:
        half = totals["евро"] / 2
        result += f"Евро: {totals['евро']} (на двоих: {half:.2f})\n"
        has_money = True
    if totals["крипта"]:
        half = totals["крипта"] / 2
        result += f"Крипта: {totals['крипта']} (на двоих: {half:.0f})\n"
        has_money = True
    if totals["драм"]:
        half = totals["драм"] / 2
        result += f"Драмы: {totals['драм']} (на двоих: {half:.0f})\n"
        has_money = True

    result += f"\nОбщая выручка: {total_usd:.2f} USD"
    result += f"\nНа двоих: {total_usd / 2:.2f} USD"

    # Расходы — просто отображаем, НЕ вычитаем
    expenses = chat_data.get("expenses", [])
    if expenses:
        result += "\n\n<b>Расходы за смену:</b>\n"
        total_expenses = 0
        for e in expenses:
            line = f"{e['type']}: {e['amount']:.2f} USD"
            if e['comment']:
                line += f" ({e['comment']})"
            result += line + "\n"
            total_expenses += e['amount']
        result += f"Итого расходов: {total_expenses:.2f} USD"

    # ЗП операторам (от полной суммы, без вычета расходов)
    operator_money = {}
    for b in came:
        text = (b.get("original_text") or b["info"] or "").lower()
        matches = re.findall(r"(\d+)\s*(лари|лaри|лар|lari|доллар[аов]?|dollar|usd|\$|евро|euro|€|крипта|crypto|usdt|btc|eth|драм|драмм|драмов|драма|dram|amd|֏)", text)
        amount_usd = 0
        for amount_str, currency in matches:
            amt = int(amount_str)
            currency = currency.lower()
            if any(x in currency for x in ["лари", "лар", "lari"]):
                amount_usd += amt * current_lari_to_usd
            elif any(x in currency for x in ["доллар", "dollar", "usd", "$"]):
                amount_usd += amt
            elif any(x in currency for x in ["евро", "euro", "€"]):
                amount_usd += amt * current_euro_to_usd
            elif any(x in currency for x in ["крипта", "crypto", "usdt", "btc", "eth"]):
                amount_usd += amt
            elif any(x in currency for x in ["драм", "драмм", "драмов", "драма", "dram", "amd", "֏"]):
                amount_usd += amt * current_amd_to_usd

        info_words = b["info"].strip().split()
        name = info_words[0] if info_words else "Неизвестно"

        if name not in operator_money:
            operator_money[name] = 0
        operator_money[name] += amount_usd

    if operator_money:
        result += "\n\n<b>ЗП операторам (от полной суммы):</b>\n"
        for name, usd in operator_money.items():
            percent = get_salary_percent(name)
            salary = usd * percent
            result += f"{name}: {salary:.2f} USD ({int(percent*100)}%)\n"

    # ЗП админов (от полной суммы всех операторов)
    total_all_usd = sum(operator_money.values())
    if total_all_usd > 0:
        admin_pct = settings.get("admin_salary_percent", {})
        all_admins = set(list(ADMIN_SALARY_PERCENT.keys()) + list(admin_pct.keys()))
        if all_admins:
            result += "\n<b>ЗП админам (от полной суммы):</b>\n"
            for aname in sorted(all_admins):
                apct = get_admin_salary_percent(aname)
                asal = total_all_usd * apct
                result += f"{aname}: {asal:.2f} USD ({apct*100:.1f}%)\n"

    full_message = header + board_text + result
    return full_message


# ==================== /summary ====================
@dp.message(Command("summary"))
async def cmd_summary(m: types.Message):
    if m.from_user.id not in OWNERS:
        await m.reply("Ты не владелец")
        return

    chat_str = str(m.chat.id)
    await ensure_chat(m.chat.id)

    full_message = await generate_summary_text(chat_str)

    for owner_id in OWNERS:
        try:
            await bot.send_message(owner_id, full_message, parse_mode=ParseMode.HTML)
        except Exception as e:
            print(f"Не удалось отправить саммари владельцу {owner_id}: {e}")

    await m.reply("Проверь личку!")


# ==================== АВТО ИТОГИ В 08:59 ====================
def archive_shift(chat_str: str):
    """Сохраняет текущую смену в history.json перед сбросом."""
    chat_data = data["chats"].get(chat_str)
    if not chat_data:
        return
    bookings = chat_data.get("bookings", [])
    # Сохраняем только если были реальные брони
    if not bookings:
        return
    shift_record = {
        "chat_id": chat_str,
        "date": chat_data.get("date", ""),
        "chat_title": chat_data.get("chat_title", ""),
        "bookings": list(bookings),
        "expenses": list(chat_data.get("expenses", [])),
        "archived_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }
    history["shifts"].append(shift_record)
    save_history()


async def send_summary_for_all_chats():
    for chat_id in ALLOWED_CHATS:
        try:
            chat_str = str(chat_id)
            if chat_str not in data["chats"]:
                continue

            # Архивируем смену перед отправкой итогов
            archive_shift(chat_str)

            full_message = await generate_summary_text(chat_str)

            for owner_id in OWNERS:
                await bot.send_message(owner_id, full_message, parse_mode=ParseMode.HTML)
        except Exception as e:
            print(f"Ошибка при отправке итогов для чата {chat_id}: {e}")

    # Очистка старых записей раз в день
    cleanup_old_history()


# ==================== КОМАНДЫ ====================
@dp.message(Command("new_shift"))
async def cmd_new_shift(m: types.Message):
    if m.from_user.id not in OWNERS:
        await m.reply("Ты не владелец")
        return
    await ensure_chat(m.chat.id)
    chat_str = str(m.chat.id)
    # Архивируем перед сбросом
    archive_shift(chat_str)
    current_date, chat_title = await get_shift_info(m.chat.id)
    data["chats"][chat_str]["bookings"] = []
    data["chats"][chat_str]["expenses"] = []
    data["chats"][chat_str]["date"] = current_date
    data["chats"][chat_str]["next_id"] = 1
    data["chats"][chat_str]["chat_title"] = chat_title
    save_data()
    await m.reply("Смена сброшена вручную!")
    await refresh_board(m.chat.id)


@dp.message(Command("daily"))
async def cmd_daily(m: types.Message):
    if m.chat.id in ALLOWED_CHATS:
        await refresh_board(m.chat.id)


async def daily_job():
    for cid in ALLOWED_CHATS:
        try:
            await refresh_board(cid)
        except:
            pass


# ==================== ОТЧЁТЫ В ЛИЧКЕ (ТОЛЬКО OWNERS) ====================
DAY_NAMES = ["пн", "вт", "ср", "чт", "пт", "сб", "вс"]

MAX_MSG_LEN = 4000


async def safe_send(target, text: str, edit: bool = False):
    """Отправляет длинный текст, разбивая на части по 4000 символов.
    target — Message объект. edit=True — первую часть edit, остальные send."""
    if len(text) <= MAX_MSG_LEN:
        if edit:
            await target.edit_text(text, parse_mode=ParseMode.HTML)
        else:
            await target.answer(text, parse_mode=ParseMode.HTML)
        return

    # Разбиваем по строкам, чтобы не резать теги
    lines = text.split("\n")
    parts = []
    current = ""
    for line in lines:
        if len(current) + len(line) + 1 > MAX_MSG_LEN:
            if current:
                parts.append(current)
            current = line
        else:
            current = current + "\n" + line if current else line
    if current:
        parts.append(current)

    for i, part in enumerate(parts):
        if i == 0 and edit:
            await target.edit_text(part, parse_mode=ParseMode.HTML)
        elif i == 0:
            await target.answer(part, parse_mode=ParseMode.HTML)
        else:
            if edit:
                await target.answer(part, parse_mode=ParseMode.HTML)
            else:
                await target.answer(part, parse_mode=ParseMode.HTML)


def parse_date_str(s: str) -> Optional[datetime]:
    """Парсит дату в формате ДД.ММ.ГГГГ или ДД.ММ"""
    for fmt in ("%d.%m.%Y", "%d.%m"):
        try:
            d = datetime.strptime(s.strip(), fmt)
            if fmt == "%d.%m":
                d = d.replace(year=datetime.now().year)
            return d
        except ValueError:
            continue
    return None


def get_shifts_for_period(date_from: datetime, date_to: datetime) -> list:
    """Возвращает архивные смены за период + текущие смены если попадают."""
    excluded = set(str(c) for c in EXCLUDED_FROM_REPORTS)
    result = []
    for shift in history.get("shifts", []):
        if shift.get("chat_id", "") in excluded:
            continue
        d = parse_date_str(shift.get("date", ""))
        if d and date_from <= d <= date_to:
            result.append(shift)
    # Добавляем текущие смены из data, если их дата попадает в период
    for chat_str, chat_data in data.get("chats", {}).items():
        if chat_str in excluded:
            continue
        d = parse_date_str(chat_data.get("date", ""))
        if d and date_from <= d <= date_to:
            if chat_data.get("bookings"):
                result.append({
                    "chat_id": chat_str,
                    "date": chat_data["date"],
                    "chat_title": chat_data.get("chat_title", ""),
                    "bookings": chat_data["bookings"],
                    "expenses": chat_data.get("expenses", []),
                })
    return result


def extract_booking_usd(booking: dict) -> tuple[float, dict]:
    """Извлекает сумму в USD из брони. Возвращает (usd, {валюта: сумма})."""
    text = (booking.get("original_text") or booking.get("info") or "").lower()
    matches = re.findall(
        r"(\d[\d\s]*)\s*(лари|лaри|лар|lari|доллар[аов]?|dollar|usd|\$|евро|euro|€|крипта|crypto|usdt|btc|eth|драм|драмм|драмов|драма|dram|amd|֏)",
        text
    )
    total_usd = 0.0
    currencies = {}
    for amount_str, currency in matches:
        amt = int(amount_str.replace(" ", ""))
        currency = currency.lower()
        if any(x in currency for x in ["лари", "лар", "lari"]):
            currencies["лари"] = currencies.get("лари", 0) + amt
            total_usd += amt * current_lari_to_usd
        elif any(x in currency for x in ["доллар", "dollar", "usd", "$"]):
            currencies["$"] = currencies.get("$", 0) + amt
            total_usd += amt
        elif any(x in currency for x in ["евро", "euro", "€"]):
            currencies["евро"] = currencies.get("евро", 0) + amt
            total_usd += amt * current_euro_to_usd
        elif any(x in currency for x in ["крипта", "crypto", "usdt", "btc", "eth"]):
            currencies["крипта"] = currencies.get("крипта", 0) + amt
            total_usd += amt
        elif any(x in currency for x in ["драм", "драмм", "драмов", "драма", "dram", "amd", "֏"]):
            currencies["драм"] = currencies.get("драм", 0) + amt
            total_usd += amt * current_amd_to_usd
    return total_usd, currencies


def extract_operator_name(booking: dict) -> str:
    info_words = booking.get("info", "").strip().split()
    return info_words[0] if info_words else "Неизвестно"


def extract_girl_name(chat_title: str) -> str:
    """Извлекает имя девочки из названия чата, например '💞Ария💞Тбилиси 28.01' -> 'Ария'"""
    m = re.search(r"💞([^💞]+)💞", chat_title)
    if m:
        return m.group(1).strip()
    # fallback — первое слово
    return chat_title.split()[0] if chat_title else "Неизвестно"


def generate_period_report(date_from: datetime, date_to: datetime) -> str:
    """Генерирует общий отчёт за период: подробно по чатам + итоговая сводка."""
    update_exchange_rates()
    shifts = get_shifts_for_period(date_from, date_to)

    period_str = f"{date_from.strftime('%d.%m')} — {date_to.strftime('%d.%m.%Y')}"

    # Группируем смены по chat_title -> по дате
    # {chat_title: {date_str: [bookings]}}
    by_chat = {}
    for shift in shifts:
        title = shift.get("chat_title", "Неизвестно")
        date_str = shift.get("date", "")
        if title not in by_chat:
            by_chat[title] = {}
        if date_str not in by_chat[title]:
            by_chat[title][date_str] = []
        came = [b for b in shift.get("bookings", []) if b.get("done") and not b.get("deleted")]
        by_chat[title][date_str].extend(came)

    # Итоговые аккумуляторы
    girl_totals = {}   # {chat_title: {"usd": X, "лари": X, ...}}
    operator_totals = {}  # {имя: usd}

    # ===== ПОДРОБНЫЙ ОТЧЁТ ПО ЧАТАМ =====
    text = f"<b>Отчёт за период: {period_str}</b>\n"
    text += "━" * 30 + "\n\n"

    for title in sorted(by_chat.keys()):
        dates = by_chat[title]
        text += f"<b>{title}</b>\n"

        chat_usd = 0
        chat_currencies = {"лари": 0, "$": 0, "евро": 0, "крипта": 0, "драм": 0}

        for date_str in sorted(dates.keys(), key=lambda x: parse_date_str(x) or datetime.min):
            bookings = dates[date_str]
            if not bookings:
                continue
            d = parse_date_str(date_str)
            day_name = DAY_NAMES[d.weekday()].upper() if d else ""
            text += f"  <b>{date_str} ({day_name}):</b>\n"

            for b in sorted(bookings, key=lambda x: time_key(x.get("time", "00:00"))):
                usd, currencies = extract_booking_usd(b)
                op_name = extract_operator_name(b)
                chat_usd += usd
                for cur, amt in currencies.items():
                    chat_currencies[cur] = chat_currencies.get(cur, 0) + amt

                # ЗП оператора
                if op_name not in operator_totals:
                    operator_totals[op_name] = 0
                operator_totals[op_name] += usd

                info = b.get("info", "")
                duration = b.get("duration", "")
                usd_note = f" ({usd:.0f}$)" if usd > 0 else ""
                text += f"    {b.get('time', '')} — {info} ({duration}){usd_note}\n"

        # Итог по чату
        parts = []
        if chat_currencies.get("лари"): parts.append(f"{chat_currencies['лари']} лари")
        if chat_currencies.get("$"): parts.append(f"{chat_currencies['$']}$")
        if chat_currencies.get("евро"): parts.append(f"{chat_currencies['евро']} евро")
        if chat_currencies.get("крипта"): parts.append(f"{chat_currencies['крипта']} крипта")
        if chat_currencies.get("драм"): parts.append(f"{chat_currencies['драм']} драм")
        cur_str = " + ".join(parts) if parts else "0"
        text += f"  <b>Итог: {cur_str} ≈ {chat_usd:.0f}$ (на двоих: {chat_usd/2:.0f}$)</b>\n\n"

        # Сохраняем для сводки
        girl_totals[title] = {"usd": chat_usd}
        girl_totals[title].update(chat_currencies)

    # ===== ИТОГОВАЯ СВОДКА =====
    text += "━" * 30 + "\n"
    text += "<b>СВОДКА ПО КАССАМ:</b>\n"
    grand_total = 0
    if girl_totals:
        for title, tots in sorted(girl_totals.items()):
            parts = []
            if tots.get("лари"): parts.append(f"{tots['лари']} лари")
            if tots.get("$"): parts.append(f"{tots['$']}$")
            if tots.get("евро"): parts.append(f"{tots['евро']} евро")
            if tots.get("крипта"): parts.append(f"{tots['крипта']} крипта")
            if tots.get("драм"): parts.append(f"{tots['драм']} драм")
            cur_str = " + ".join(parts) if parts else ""
            usd_total = tots["usd"]
            grand_total += usd_total
            half = usd_total / 2
            text += f"  <b>{title}:</b> {cur_str} ≈ {usd_total:.0f}$ (на двоих: {half:.0f}$)\n"
        text += f"\n  <b>ИТОГО:</b> {grand_total:.0f}$ (на двоих: {grand_total/2:.0f}$)\n"
    else:
        text += "  Нет данных\n"

    # ЗП операторов
    text += "\n<b>ЗП операторов:</b>\n"
    if operator_totals:
        total_salary = 0
        for name, usd in sorted(operator_totals.items()):
            percent = get_salary_percent(name)
            salary = usd * percent
            total_salary += salary
            text += f"  {name}: {salary:.2f}$ ({int(percent*100)}%) — от кассы {usd:.0f}$\n"
        text += f"\n  <b>Итого ЗП операторов:</b> {total_salary:.2f}$\n"
    else:
        text += "  Нет данных\n"

    # ЗП админов
    text += "\n<b>ЗП админов:</b>\n"
    if grand_total > 0:
        admin_pct = settings.get("admin_salary_percent", {})
        all_admins = set(list(ADMIN_SALARY_PERCENT.keys()) + list(admin_pct.keys()))
        total_admin_salary = 0
        for aname in sorted(all_admins):
            apct = get_admin_salary_percent(aname)
            asal = grand_total * apct
            total_admin_salary += asal
            text += f"  {aname}: {asal:.2f}$ ({apct*100:.1f}%) — от общей кассы {grand_total:.0f}$\n"
        text += f"\n  <b>Итого ЗП админов:</b> {total_admin_salary:.2f}$\n"
    else:
        text += "  Нет данных\n"

    # Расходы из expenses.json за период
    period_expenses = get_expenses_for_period(date_from, date_to)
    if period_expenses:
        text += "\n" + "━" * 30 + "\n"
        text += "<b>Расходы за период:</b>\n"
        # Группируем по чату
        exp_by_chat = {}
        total_exp = 0
        for e in period_expenses:
            title = e.get("chat_title", "Неизвестно")
            if title not in exp_by_chat:
                exp_by_chat[title] = []
            exp_by_chat[title].append(e)
            total_exp += e.get("amount_usd", 0)

        for title in sorted(exp_by_chat.keys()):
            text += f"  <b>{title}:</b>\n"
            for e in sorted(exp_by_chat[title], key=lambda x: parse_date_str(x.get("date", "")) or datetime.min):
                cur_d = e.get("currency", "$")
                if cur_d == "$": cur_d = "USD"
                line = f"    {e['date']} — {e['type']}: {e['amount']:.0f} {cur_d} ≈ {e['amount_usd']:.0f}$"
                if e.get("comment"):
                    line += f" ({e['comment']})"
                text += line + "\n"
        text += f"\n  <b>Итого расходов:</b> {total_exp:.0f}$\n"

    return text


def generate_girl_report(date_from: datetime, date_to: datetime, chat_id_filter: str) -> str:
    """Отчёт по кассе девочки (чату) в формате: день -> список броней -> итоги в gel."""
    update_exchange_rates()

    # gel за 1 доллар (обратный курс)
    usd_to_gel = 1 / current_lari_to_usd if current_lari_to_usd > 0 else 2.70
    euro_to_gel = current_euro_to_usd * usd_to_gel

    shifts = get_shifts_for_period(date_from, date_to)
    # Фильтруем по нужному чату
    shifts = [s for s in shifts if str(s.get("chat_id", "")) == str(chat_id_filter)]

    if not shifts:
        return "Нет данных за этот период."

    chat_title = shifts[0].get("chat_title", "Неизвестно") if shifts else "Неизвестно"
    girl_name = extract_girl_name(chat_title)
    period_str = f"{date_from.strftime('%d.%m')} — {date_to.strftime('%d.%m.%Y')}"

    # Группируем по дате
    by_date = {}
    for shift in shifts:
        date_str = shift.get("date", "")
        if date_str not in by_date:
            by_date[date_str] = []
        came = [b for b in shift.get("bookings", []) if b.get("done") and not b.get("deleted")]
        by_date[date_str].extend(came)

    # Генерируем все дни
    text = f"<b>{girl_name}</b>\n"
    text += f"<i>{chat_title}</i>\n"
    text += f"<i>{period_str}</i>\n\n"

    totals = {"лари": 0, "$": 0, "евро": 0, "крипта": 0, "драм": 0}

    current = date_from
    while current <= date_to:
        d_str = current.strftime("%d.%m")
        d_full = current.strftime("%d.%m.%Y")

        # Ищем смену за эту дату
        bookings = by_date.get(d_full, [])

        text += f"<b>{d_str}</b>\n"
        if not bookings:
            text += "0\n"
        else:
            for b in sorted(bookings, key=lambda x: time_key(x.get("time", "00:00"))):
                _, currencies = extract_booking_usd(b)
                op_name = extract_operator_name(b)
                duration = b.get("duration", "")

                # Форматируем сумму
                parts = []
                for cur_name, amt in currencies.items():
                    if amt > 0:
                        if cur_name == "лари":
                            parts.append(f"{amt}")
                        elif cur_name == "$":
                            parts.append(f"{amt}$")
                        elif cur_name == "евро":
                            parts.append(f"{amt}€")
                        elif cur_name == "крипта":
                            parts.append(f"{amt} USDT")
                        elif cur_name == "драм":
                            parts.append(f"{amt} драм")
                amount_str = " + ".join(parts) if parts else "0"

                text += f"  {amount_str}/2 {op_name}\n"

                for cur_name, amt in currencies.items():
                    totals[cur_name] = totals.get(cur_name, 0) + amt

        current += timedelta(days=1)

    # ===== РАСЧЁТ =====
    text += "\n<b>Расчёт:</b>\n"

    total_gel = 0

    # Лари
    if totals["лари"] > 0:
        half = totals["лари"] / 2
        text += f"\n<b>Лари:</b> {totals['лари']:.0f} gel\n"
        text += f"  {totals['лари']:.0f} / 2 = {half:.0f} gel\n"
        total_gel += half

    # USD
    if totals["$"] > 0:
        half = totals["$"] / 2
        gel_val = half * usd_to_gel
        text += f"\n<b>USD:</b> {totals['$']:.0f}$\n"
        text += f"  {totals['$']:.0f} / 2 = {half:.0f}$\n"
        text += f"  {half:.0f} × {usd_to_gel:.2f} = {gel_val:.2f} gel\n"
        total_gel += gel_val

    # Евро
    if totals["евро"] > 0:
        half = totals["евро"] / 2
        usd_val = half * current_euro_to_usd
        gel_val = usd_val * usd_to_gel
        text += f"\n<b>Евро:</b> {totals['евро']:.0f}€\n"
        text += f"  {totals['евро']:.0f} / 2 = {half:.0f}€\n"
        text += f"  {half:.0f} × {current_euro_to_usd:.2f} = {usd_val:.2f}$\n"
        text += f"  {usd_val:.2f} × {usd_to_gel:.2f} = {gel_val:.2f} gel\n"
        total_gel += gel_val

    # Крипта
    crypto_gel = 0
    if totals["крипта"] > 0:
        half = totals["крипта"] / 2
        gel_val = half * usd_to_gel
        text += f"\n<b>Крипта:</b> {totals['крипта']:.0f} USDT\n"
        text += f"  {totals['крипта']:.0f} / 2 = {half:.0f} USDT\n"
        text += f"  {half:.0f} × {usd_to_gel:.2f} = {gel_val:.2f} gel\n"
        crypto_gel = gel_val

    # Драм
    if totals["драм"] > 0:
        half_usd = totals["драм"] * current_amd_to_usd / 2
        gel_val = half_usd * usd_to_gel
        text += f"\n<b>Драм:</b> {totals['драм']:.0f} драм\n"
        text += f"  ≈ {totals['драм'] * current_amd_to_usd:.2f}$ / 2 = {half_usd:.2f}$\n"
        text += f"  {half_usd:.2f} × {usd_to_gel:.2f} = {gel_val:.2f} gel\n"
        total_gel += gel_val

    cash_gel = total_gel
    text += f"\n<b>Итого наличка:</b> {cash_gel:.2f} gel\n"

    if crypto_gel > 0:
        text += f"\n<b>Крипта (вся наша):</b>\n"
        text += f"  {totals['крипта']:.0f} / 2 = {totals['крипта']/2:.0f} USDT\n"
        text += f"  {totals['крипта']/2:.0f} × {usd_to_gel:.2f} = {crypto_gel:.2f} gel\n"
        cash_after_crypto = cash_gel - crypto_gel
        text += f"\n<b>Наличка за вычетом крипты:</b>\n"
        text += f"  {cash_gel:.2f} − {crypto_gel:.2f} = {cash_after_crypto:.2f} gel\n"
    else:
        cash_after_crypto = cash_gel

    # Расходы из expenses.json
    expenses = get_expenses_for_period(date_from, date_to, chat_id_filter)
    rent_gel = 0        # Квартира — девочка должна нам
    deduct_gel = 0      # Такси и прочее — вычитаем из кассы
    photo_gel = 0       # Фотосессия — только статистика

    if expenses:
        text += "\n<b>Расходы:</b>\n"
        for e in sorted(expenses, key=lambda x: parse_date_str(x.get("date", "")) or datetime.min):
            exp_usd = e.get("amount_usd", 0)
            exp_gel = exp_usd * usd_to_gel
            cur_d = e.get("currency", "$")
            exp_type = e.get("type", "").lower()

            if "кварт" in exp_type:
                text += f"  {e['date']} {e['type']}: {e['amount']:.0f} {cur_d} ≈ {exp_gel:.0f} gel (долг девочки)\n"
                rent_gel += exp_gel
            elif "фото" in exp_type:
                text += f"  {e['date']} {e['type']}: {e['amount']:.0f} {cur_d} ≈ {exp_gel:.0f} gel (наш расход, не в кассе)\n"
                photo_gel += exp_gel
            else:
                text += f"  {e['date']} {e['type']}: {e['amount']:.0f} {cur_d} ≈ {exp_gel:.0f} gel\n"
                deduct_gel += exp_gel

        if rent_gel > 0:
            text += f"  <b>Квартира (долг):</b> +{rent_gel:.0f} gel\n"
        if deduct_gel > 0:
            text += f"  <b>Расходы (вычет):</b> −{deduct_gel:.0f} gel\n"
        if photo_gel > 0:
            text += f"  <b>Фотосессии (справочно):</b> {photo_gel:.0f} gel\n"

    # Финальный расчёт
    text += "\n<b>Финальный расчёт:</b>\n"
    final = cash_after_crypto + rent_gel - deduct_gel
    parts_calc = [f"{cash_after_crypto:.2f}"]
    if rent_gel > 0:
        parts_calc.append(f"+ {rent_gel:.0f} (квартира)")
    if deduct_gel > 0:
        parts_calc.append(f"− {deduct_gel:.0f} (расходы)")
    text += f"  {' '.join(parts_calc)} = <b>{final:.2f} gel</b>\n"

    return text


def generate_operator_report(date_from: datetime, date_to: datetime, op_name: str) -> str:
    """Генерирует детальный отчёт по одному оператору (как в примере по Саше)."""
    update_exchange_rates()
    shifts = get_shifts_for_period(date_from, date_to)

    period_str = f"{date_from.strftime('%d.%m')} — {date_to.strftime('%d.%m.%Y')}"

    # Группируем по дням
    days = {}  # {date_str: [(booking, chat_title, shift_date_obj)]}
    for shift in shifts:
        chat_title = shift.get("chat_title", "Неизвестно")
        shift_date = parse_date_str(shift.get("date", ""))
        if not shift_date:
            continue
        came = [b for b in shift.get("bookings", []) if b.get("done") and not b.get("deleted")]
        for b in came:
            name = extract_operator_name(b)
            if name.lower() == op_name.lower():
                date_key = shift.get("date", "")
                if date_key not in days:
                    days[date_key] = []
                days[date_key].append((b, chat_title, shift_date))

    text = f"<b>Отчёт по оператору: {op_name}</b>\n"
    text += f"<b>Период: {period_str}</b>\n\n"

    chat_totals = {}  # {chat_title: {"usd": X, currencies...}}
    total_usd = 0

    # Генерируем все дни периода для показа "НИКОГО"
    current = date_from
    all_dates = []
    while current <= date_to:
        all_dates.append(current)
        current += timedelta(days=1)

    days_by_date = {}
    for date_str, entries in days.items():
        d = parse_date_str(date_str)
        if d:
            days_by_date[d.strftime("%d.%m.%Y")] = entries

    for d in all_dates:
        day_name = DAY_NAMES[d.weekday()].upper()
        d_str = d.strftime("%d.%m.%Y")
        entries = days_by_date.get(d_str, [])

        if not entries:
            text += f"<b>{day_name}</b> НИКОГО\n"
        else:
            text += f"<b>{day_name}</b>\n"
            entries_sorted = sorted(entries, key=lambda x: time_key(x[0].get("time", "00:00")))
            for b, chat_title, _ in entries_sorted:
                usd, currencies = extract_booking_usd(b)
                total_usd += usd

                # Касса по чату
                if chat_title not in chat_totals:
                    chat_totals[chat_title] = {"usd": 0, "лари": 0, "$": 0, "евро": 0, "крипта": 0, "драм": 0}
                chat_totals[chat_title]["usd"] += usd
                for cur, amt in currencies.items():
                    chat_totals[chat_title][cur] = chat_totals[chat_title].get(cur, 0) + amt

                info = b.get("info", "")
                duration = b.get("duration", "")
                usd_note = f" ({usd:.0f}$)" if usd > 0 else ""
                text += f"  {b.get('time', '')} {info} ({duration}) — {chat_title}{usd_note}\n"

    text += "\n<b>Итоги по чатам:</b>\n"
    for title, tots in sorted(chat_totals.items()):
        parts = []
        if tots.get("лари"): parts.append(f"{tots['лари']} лари")
        if tots.get("$"): parts.append(f"{tots['$']}$")
        if tots.get("крипта"): parts.append(f"{tots['крипта']} крипта")
        if tots.get("драм"): parts.append(f"{tots['драм']:.0f} драм")
        if tots.get("евро"): parts.append(f"{tots['евро']} евро")
        cur_str = " + ".join(parts) if parts else ""
        text += f"  {title}: {cur_str} ≈ {tots['usd']:.0f}$\n"

    percent = get_salary_percent(op_name)
    salary = total_usd * percent
    text += f"\n<b>ИТОГО касса:</b> {total_usd:.0f}$\n"
    text += f"<b>ЗП {op_name} ({int(percent*100)}%):</b> {salary:.2f}$\n"

    return text


def get_all_operators(date_from: datetime, date_to: datetime) -> list[str]:
    """Возвращает список уникальных операторов за период."""
    shifts = get_shifts_for_period(date_from, date_to)
    operators = set()
    for shift in shifts:
        came = [b for b in shift.get("bookings", []) if b.get("done") and not b.get("deleted")]
        for b in came:
            operators.add(extract_operator_name(b))
    return sorted(operators)


def generate_operator_stats(date_from: datetime, date_to: datetime) -> str:
    """Генерирует статистику по операторам: всего броней, пришёл, отменено, не пришёл."""
    update_exchange_rates()
    shifts = get_shifts_for_period(date_from, date_to)

    period_str = f"{date_from.strftime('%d.%m')} — {date_to.strftime('%d.%m.%Y')}"

    # {оператор: {"total": X, "came": X, "cancelled": X, "no_show": X, "usd": X}}
    stats = {}

    for shift in shifts:
        for b in shift.get("bookings", []):
            op_name = extract_operator_name(b)
            if op_name not in stats:
                stats[op_name] = {"total": 0, "came": 0, "cancelled": 0, "no_show": 0, "usd": 0}

            stats[op_name]["total"] += 1

            if b.get("deleted"):
                stats[op_name]["cancelled"] += 1
            elif b.get("cancelled"):
                stats[op_name]["no_show"] += 1
            elif b.get("done"):
                stats[op_name]["came"] += 1
                usd, _ = extract_booking_usd(b)
                stats[op_name]["usd"] += usd

    text = f"<b>Статистика операторов за {period_str}</b>\n"
    text += "━" * 30 + "\n\n"

    if not stats:
        text += "Нет данных за этот период.\n"
        return text

    for name in sorted(stats.keys()):
        s = stats[name]
        total = s["total"]
        came = s["came"]
        cancelled = s["cancelled"]
        no_show = s["no_show"]
        # Процент успешных от общего
        success_pct = (came / total * 100) if total > 0 else 0

        text += f"<b>{name}</b>\n"
        text += f"  Всего броней: {total}\n"
        text += f"  Пришёл: {came}\n"
        text += f"  Отменено: {cancelled}\n"
        text += f"  Не пришёл: {no_show}\n"
        text += f"  Конверсия: {success_pct:.0f}%\n"
        text += f"  Касса (пришедшие): {s['usd']:.0f}$\n\n"

    return text


# ----------- Постоянная клавиатура в ЛС -----------
REPORT_BUTTON_TEXT = "Отчёты"
EXPENSE_BUTTON_TEXT = "Расходы"
SETTINGS_BUTTON_TEXT = "Настройки"
MY_SALARY_BUTTON_TEXT = "Моя ЗП"

owner_kb = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text=REPORT_BUTTON_TEXT), KeyboardButton(text=EXPENSE_BUTTON_TEXT)],
        [KeyboardButton(text=SETTINGS_BUTTON_TEXT)],
    ],
    resize_keyboard=True,
)

operator_kb = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text=MY_SALARY_BUTTON_TEXT)],
    ],
    resize_keyboard=True,
)


def get_operator_name_by_tg_id(tg_id: int) -> str:
    """Возвращает имя оператора по его Telegram ID."""
    for name, op_id in OPERATORS.items():
        if op_id == tg_id:
            return name
    return ""


@dp.message(Command("start"))
async def cmd_start_private(m: types.Message):
    if m.chat.type != "private":
        return
    if m.from_user.id in OWNERS:
        await m.answer("Привет! Кнопка отчётов — внизу", reply_markup=owner_kb)
    elif get_operator_name_by_tg_id(m.from_user.id):
        await m.answer("Привет! Ты можешь посмотреть свою ЗП", reply_markup=operator_kb)
    # Остальные — молчим


# ----------- Обработка кнопки «Моя ЗП» для операторов -----------
@dp.message(F.text == MY_SALARY_BUTTON_TEXT, F.chat.type == "private")
async def handle_my_salary_button(m: types.Message, state: FSMContext):
    op_name = get_operator_name_by_tg_id(m.from_user.id)
    if not op_name:
        return

    await state.clear()
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Эта неделя", callback_data="mysalary:this_week")],
        [InlineKeyboardButton(text="Прошлая неделя", callback_data="mysalary:last_week")],
        [InlineKeyboardButton(text="Свой период", callback_data="mysalary:custom")],
    ])
    await m.answer(f"<b>{op_name}, выбери период:</b>", reply_markup=kb, parse_mode=ParseMode.HTML)


def generate_my_salary_report(date_from: datetime, date_to: datetime, op_name: str) -> str:
    """Отчёт ЗП для оператора — используем полный отчёт по оператору."""
    return generate_operator_report(date_from, date_to, op_name)


@dp.callback_query(F.data.startswith("mysalary:"))
async def my_salary_callbacks(c: types.CallbackQuery, state: FSMContext):
    op_name = get_operator_name_by_tg_id(c.from_user.id)
    if not op_name:
        await c.answer("Нет доступа", show_alert=True)
        return

    action = c.data.split(":", 1)[1]

    if action == "this_week":
        now = datetime.now()
        monday = now - timedelta(days=now.weekday())
        date_from = monday.replace(hour=0, minute=0, second=0, microsecond=0)
        date_to = now
        try:
            report = generate_my_salary_report(date_from, date_to, op_name)
            await safe_send(c.message, report)
        except Exception as e:
            await bot.send_message(c.from_user.id, f"Ошибка: {e}")

    elif action == "last_week":
        now = datetime.now()
        monday = now - timedelta(days=now.weekday() + 7)
        sunday = monday + timedelta(days=6)
        date_from = monday.replace(hour=0, minute=0, second=0, microsecond=0)
        date_to = sunday.replace(hour=23, minute=59, second=59)
        try:
            report = generate_my_salary_report(date_from, date_to, op_name)
            await safe_send(c.message, report)
        except Exception as e:
            await bot.send_message(c.from_user.id, f"Ошибка: {e}")

    elif action == "custom":
        await state.set_state(OperatorSalaryState.waiting_for_period)
        await c.message.edit_text(
            "Введи период в формате:\n<code>01.02-15.02</code>",
            parse_mode=ParseMode.HTML
        )

    await c.answer()


@dp.message(StateFilter(OperatorSalaryState.waiting_for_period))
async def handle_operator_salary_period(m: types.Message, state: FSMContext):
    if m.chat.type != "private":
        return
    op_name = get_operator_name_by_tg_id(m.from_user.id)
    if not op_name:
        return

    text = m.text.strip()
    match = re.match(r"(\d{1,2}\.\d{1,2})(?:\.(\d{2,4}))?\s*[-–]\s*(\d{1,2}\.\d{1,2})(?:\.(\d{2,4}))?", text)
    if not match:
        await m.reply("Формат: <code>01.02-15.02</code>", parse_mode=ParseMode.HTML)
        return

    year = datetime.now().year
    try:
        d1 = match.group(1)
        y1 = match.group(2) or str(year)
        if len(y1) == 2:
            y1 = "20" + y1
        date_from = datetime.strptime(f"{d1}.{y1}", "%d.%m.%Y")

        d2 = match.group(3)
        y2 = match.group(4) or str(year)
        if len(y2) == 2:
            y2 = "20" + y2
        date_to = datetime.strptime(f"{d2}.{y2}", "%d.%m.%Y").replace(hour=23, minute=59, second=59)
    except:
        await m.reply("Неверная дата. Формат: <code>01.02-15.02</code>", parse_mode=ParseMode.HTML)
        return

    report = generate_my_salary_report(date_from, date_to, op_name)
    try:
        await safe_send(m, report)
    except Exception as e:
        await m.reply(f"Ошибка: {e}")
    await state.clear()


# ----------- Обработка нажатия текстовой кнопки «Отчёты» -----------
@dp.message(F.text == REPORT_BUTTON_TEXT, F.chat.type == "private")
async def handle_report_button(m: types.Message, state: FSMContext):
    if m.from_user.id not in OWNERS:
        return

    await state.clear()
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Эта неделя (Пн–Вс)", callback_data="rep:this_week")],
        [InlineKeyboardButton(text="Прошлая неделя", callback_data="rep:last_week")],
        [InlineKeyboardButton(text="Ввести период", callback_data="rep:custom")],
        [InlineKeyboardButton(text="По оператору", callback_data="rep:operator")],
        [InlineKeyboardButton(text="Статистика операторов", callback_data="rep:stats")],
        [InlineKeyboardButton(text="Касса девочки", callback_data="rep:girl")],
    ])
    await m.answer("<b>Отчёты</b>\n\nВыбери тип отчёта:", reply_markup=kb)


# ----------- /report — главное меню в ЛС (оставляем как fallback) -----------
@dp.message(Command("report"))
async def cmd_report(m: types.Message, state: FSMContext):
    if m.from_user.id not in OWNERS:
        return
    # Работает только в ЛС
    if m.chat.type != "private":
        await m.reply("Напиши мне /report в личку!")
        return

    await state.clear()
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Эта неделя (Пн–Вс)", callback_data="rep:this_week")],
        [InlineKeyboardButton(text="Прошлая неделя", callback_data="rep:last_week")],
        [InlineKeyboardButton(text="Ввести период", callback_data="rep:custom")],
        [InlineKeyboardButton(text="По оператору", callback_data="rep:operator")],
        [InlineKeyboardButton(text="Статистика операторов", callback_data="rep:stats")],
        [InlineKeyboardButton(text="Касса девочки", callback_data="rep:girl")],
    ])
    await m.answer("<b>Отчёты</b>\n\nВыбери тип отчёта:", reply_markup=kb)


# ----------- Callback: выбор типа отчёта -----------
@dp.callback_query(F.data.startswith("rep:"))
async def report_callbacks(c: types.CallbackQuery, state: FSMContext):
    if c.from_user.id not in OWNERS:
        await c.answer("Нет доступа", show_alert=True)
        return

    action = c.data.split(":", 1)[1]
    now = datetime.now()

    if action == "this_week":
        # Пн текущей недели — Вс текущей недели
        monday = now - timedelta(days=now.weekday())
        sunday = monday + timedelta(days=6)
        date_from = monday.replace(hour=0, minute=0, second=0, microsecond=0)
        date_to = sunday.replace(hour=23, minute=59, second=59)
        report = generate_period_report(date_from, date_to)
        await safe_send(c.message, report, edit=True)

    elif action == "last_week":
        monday = now - timedelta(days=now.weekday() + 7)
        sunday = monday + timedelta(days=6)
        date_from = monday.replace(hour=0, minute=0, second=0, microsecond=0)
        date_to = sunday.replace(hour=23, minute=59, second=59)
        report = generate_period_report(date_from, date_to)
        await safe_send(c.message, report, edit=True)

    elif action == "custom":
        await state.set_state(ReportState.waiting_for_period)
        await c.message.edit_text(
            "Введи период в формате:\n<code>01.02-08.02</code>\nили <code>01.02.2026-08.02.2026</code>",
            parse_mode=ParseMode.HTML
        )

    elif action == "operator":
        # Показываем период сначала — берём текущую неделю по умолчанию, потом дадим выбрать оператора
        await state.set_state(ReportState.waiting_for_period)
        await state.update_data(mode="operator")
        await c.message.edit_text(
            "Сначала введи период:\n<code>01.02-08.02</code>\n\nПотом выберешь оператора.",
            parse_mode=ParseMode.HTML
        )

    elif action == "stats":
        await state.set_state(ReportState.waiting_for_period)
        await state.update_data(mode="stats")
        await c.message.edit_text(
            "Введи период для статистики:\n<code>01.02-08.02</code>",
            parse_mode=ParseMode.HTML
        )

    elif action == "girl":
        # Показываем список чатов
        excluded = set(str(ch) for ch in EXCLUDED_FROM_REPORTS)
        buttons = []
        for ch_id in ALLOWED_CHATS:
            if str(ch_id) in excluded:
                continue
            try:
                chat = await bot.get_chat(ch_id)
                title = (chat.title or chat.first_name or str(ch_id)).strip()
            except:
                title = str(ch_id)
            buttons.append([InlineKeyboardButton(text=title, callback_data=f"girlchat:{ch_id}")])
        kb = InlineKeyboardMarkup(inline_keyboard=buttons)
        await c.message.edit_text("Выбери чат:", reply_markup=kb)

    await c.answer()


# ----------- Касса девочки: выбор чата → период -----------
@dp.callback_query(F.data.startswith("girlchat:"))
async def girl_chat_selected(c: types.CallbackQuery, state: FSMContext):
    if c.from_user.id not in OWNERS:
        await c.answer("Нет доступа", show_alert=True)
        return

    chat_id = c.data.split(":", 1)[1]
    await state.update_data(mode="girl", girl_chat_id=chat_id)
    await state.set_state(ReportState.waiting_for_period)
    await c.message.edit_text(
        "Введи период:\n<code>27.01-12.02</code>",
        parse_mode=ParseMode.HTML
    )
    await c.answer()


# ----------- Ввод периода вручную -----------
@dp.message(StateFilter(ReportState.waiting_for_period))
async def handle_period_input(m: types.Message, state: FSMContext):
    if m.from_user.id not in OWNERS:
        return
    if m.chat.type != "private":
        return

    text = m.text.strip().replace(" ", "")
    parts = re.split(r"[-–—]", text, maxsplit=1)
    if len(parts) != 2:
        await m.reply("Неверный формат. Пиши так: <code>01.02-08.02</code>", parse_mode=ParseMode.HTML)
        return

    date_from = parse_date_str(parts[0])
    date_to = parse_date_str(parts[1])
    if not date_from or not date_to:
        await m.reply("Не могу разобрать даты. Пиши так: <code>01.02-08.02</code>", parse_mode=ParseMode.HTML)
        return

    date_from = date_from.replace(hour=0, minute=0, second=0)
    date_to = date_to.replace(hour=23, minute=59, second=59)

    user_data = await state.get_data()
    mode = user_data.get("mode", "")

    if mode == "operator":
        # Показываем список операторов за период кнопками
        operators = get_all_operators(date_from, date_to)
        if not operators:
            await m.reply("За этот период нет данных по операторам.")
            await state.clear()
            return

        await state.update_data(
            date_from=date_from.strftime("%d.%m.%Y"),
            date_to=date_to.strftime("%d.%m.%Y"),
        )
        await state.set_state(ReportState.waiting_for_operator)

        buttons = []
        row = []
        for op in operators:
            row.append(InlineKeyboardButton(text=op, callback_data=f"op:{op}"))
            if len(row) == 2:
                buttons.append(row)
                row = []
        if row:
            buttons.append(row)
        # Кнопка "Все операторы"
        buttons.append([InlineKeyboardButton(text="Все операторы (общий)", callback_data="op:__ALL__")])

        kb = InlineKeyboardMarkup(inline_keyboard=buttons)
        await m.reply("Выбери оператора:", reply_markup=kb)
    elif mode == "stats":
        report = generate_operator_stats(date_from, date_to)
        await safe_send(m, report)
        await state.clear()
    elif mode == "girl":
        girl_chat_id = user_data.get("girl_chat_id", "")
        report = generate_girl_report(date_from, date_to, girl_chat_id)
        await safe_send(m, report)
        await state.clear()
    else:
        report = generate_period_report(date_from, date_to)
        await safe_send(m, report)
        await state.clear()


# ----------- Выбор оператора -----------
@dp.callback_query(F.data.startswith("op:"), StateFilter(ReportState.waiting_for_operator))
async def handle_operator_select(c: types.CallbackQuery, state: FSMContext):
    if c.from_user.id not in OWNERS:
        await c.answer("Нет доступа", show_alert=True)
        return

    op_name = c.data.split(":", 1)[1]
    user_data = await state.get_data()
    date_from = parse_date_str(user_data.get("date_from", ""))
    date_to = parse_date_str(user_data.get("date_to", ""))

    if not date_from or not date_to:
        await c.message.edit_text("Ошибка периода. Начни сначала: /report")
        await state.clear()
        await c.answer()
        return

    date_from = date_from.replace(hour=0, minute=0, second=0)
    date_to = date_to.replace(hour=23, minute=59, second=59)

    if op_name == "__ALL__":
        report = generate_period_report(date_from, date_to)
    else:
        report = generate_operator_report(date_from, date_to, op_name)

    await safe_send(c.message, report, edit=True)

    await state.clear()
    await c.answer()


# ==================== РАСХОДЫ В ЛИЧКЕ ====================

async def get_chat_list_kb():
    """Генерирует кнопки со списком рабочих чатов (кроме исключённых)."""
    excluded = set(str(c) for c in EXCLUDED_FROM_REPORTS)
    buttons = []
    for chat_id in ALLOWED_CHATS:
        if str(chat_id) in excluded:
            continue
        try:
            chat = await bot.get_chat(chat_id)
            title = (chat.title or chat.first_name or str(chat_id)).strip()
        except:
            title = str(chat_id)
        buttons.append([InlineKeyboardButton(text=title, callback_data=f"expchat:{chat_id}")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


@dp.message(F.text == EXPENSE_BUTTON_TEXT, F.chat.type == "private")
async def handle_expense_button(m: types.Message, state: FSMContext):
    if m.from_user.id not in OWNERS:
        return
    await state.clear()
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Добавить расход", callback_data="exp:add")],
        [InlineKeyboardButton(text="Список расходов", callback_data="exp:list")],
    ])
    await m.answer("<b>Расходы</b>\n\nВыбери действие:", reply_markup=kb)


@dp.callback_query(F.data.startswith("exp:"))
async def expense_menu_callbacks(c: types.CallbackQuery, state: FSMContext):
    if c.from_user.id not in OWNERS:
        await c.answer("Нет доступа", show_alert=True)
        return

    action = c.data.split(":", 1)[1]

    if action == "add":
        kb = await get_chat_list_kb()
        await c.message.edit_text("Выбери чат для добавления расхода:", reply_markup=kb)
        await state.update_data(exp_action="add")

    elif action == "list":
        kb = await get_chat_list_kb()
        await c.message.edit_text("Выбери чат для просмотра расходов:", reply_markup=kb)
        await state.update_data(exp_action="list")

    await c.answer()


@dp.callback_query(F.data.startswith("expchat:"))
async def expense_chat_selected(c: types.CallbackQuery, state: FSMContext):
    if c.from_user.id not in OWNERS:
        await c.answer("Нет доступа", show_alert=True)
        return

    chat_id = c.data.split(":", 1)[1]
    user_data = await state.get_data()
    exp_action = user_data.get("exp_action", "")

    # Получаем название чата
    try:
        chat = await bot.get_chat(int(chat_id))
        chat_title = (chat.title or chat.first_name or chat_id).strip()
    except:
        chat_title = chat_id

    await state.update_data(exp_chat_id=chat_id, exp_chat_title=chat_title)

    if exp_action == "add":
        await state.set_state(ExpenseState.waiting_for_date)
        await c.message.edit_text(
            f"<b>Чат:</b> {chat_title}\n\n"
            "Введи дату расхода:\n"
            "<code>05.02</code> или <code>05.02.2026</code>\n\n"
            "Или напиши <code>сегодня</code>",
            parse_mode=ParseMode.HTML
        )

    elif exp_action == "list":
        await state.set_state(ExpenseState.waiting_for_period_view)
        await c.message.edit_text(
            f"<b>Чат:</b> {chat_title}\n\n"
            "Введи период для просмотра расходов:\n"
            "<code>01.02-08.02</code>",
            parse_mode=ParseMode.HTML
        )

    await c.answer()


# ----------- Ввод даты расхода -----------
@dp.message(StateFilter(ExpenseState.waiting_for_date))
async def expense_date_input(m: types.Message, state: FSMContext):
    if m.from_user.id not in OWNERS or m.chat.type != "private":
        return

    text = m.text.strip().lower()
    if text == "сегодня":
        date = datetime.now()
    else:
        date = parse_date_str(m.text.strip())

    if not date:
        await m.reply("Не могу разобрать дату. Пиши: <code>05.02</code> или <code>сегодня</code>", parse_mode=ParseMode.HTML)
        return

    date_str = date.strftime("%d.%m.%Y")
    user_data = await state.get_data()
    chat_title = user_data.get("exp_chat_title", "")
    await state.update_data(exp_date=date_str)
    await state.set_state(ExpenseState.waiting_for_input)

    await m.reply(
        f"<b>Чат:</b> {chat_title}\n"
        f"<b>Дата:</b> {date_str}\n\n"
        "Введи расход в формате:\n"
        "<code>квартира 500 лари</code>\n"
        "<code>такси 30$</code>\n"
        "<code>фотосессия 200 доллар комментарий</code>\n\n"
        "Поддерживаемые валюты: лари, $, доллар, евро, драм",
        parse_mode=ParseMode.HTML
    )


# ----------- Ввод расхода -----------
@dp.message(StateFilter(ExpenseState.waiting_for_input))
async def expense_input(m: types.Message, state: FSMContext):
    if m.from_user.id not in OWNERS or m.chat.type != "private":
        return

    text = m.text.strip()
    # Парсим: тип сумма валюта [комментарий]
    match = re.match(
        r"^(\S+)\s+(\d+(?:\.\d+)?)\s*(лари|лар|lari|gel|доллар[аов]?|dollar|usd|\$|евро|euro|€|eur|драм|драмов|dram|amd|֏)?\s*(.*)?$",
        text, re.I
    )

    if not match:
        await m.reply(
            "Не могу разобрать. Формат:\n<code>квартира 500 лари</code>\n<code>такси 30$</code>",
            parse_mode=ParseMode.HTML
        )
        return

    exp_type = match.group(1).capitalize()
    amount = float(match.group(2))
    currency = (match.group(3) or "$").strip()
    comment = (match.group(4) or "").strip()

    update_exchange_rates()
    amount_usd = expense_to_usd(amount, currency)

    user_data = await state.get_data()
    chat_id = user_data.get("exp_chat_id", "")
    chat_title = user_data.get("exp_chat_title", "")
    date_str = user_data.get("exp_date", "")

    expense = {
        "id": next_expense_id(),
        "chat_id": chat_id,
        "chat_title": chat_title,
        "date": date_str,
        "type": exp_type,
        "amount": amount,
        "currency": currency,
        "amount_usd": round(amount_usd, 2),
        "comment": comment,
        "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }
    global_expenses.append(expense)
    save_expenses()

    cur_display = currency if currency != "$" else "USD"
    msg = (
        f"Расход добавлен:\n"
        f"<b>{chat_title}</b> — {date_str}\n"
        f"{exp_type}: {amount:.0f} {cur_display} ≈ {amount_usd:.0f}$"
    )
    if comment:
        msg += f"\n({comment})"

    await m.reply(msg, parse_mode=ParseMode.HTML)
    await state.clear()


# ----------- Просмотр расходов за период -----------
@dp.message(StateFilter(ExpenseState.waiting_for_period_view))
async def expense_period_view(m: types.Message, state: FSMContext):
    if m.from_user.id not in OWNERS or m.chat.type != "private":
        return

    text = m.text.strip().replace(" ", "")
    parts = re.split(r"[-–—]", text, maxsplit=1)
    if len(parts) != 2:
        await m.reply("Формат: <code>01.02-08.02</code>", parse_mode=ParseMode.HTML)
        return

    date_from = parse_date_str(parts[0])
    date_to = parse_date_str(parts[1])
    if not date_from or not date_to:
        await m.reply("Не могу разобрать даты.", parse_mode=ParseMode.HTML)
        return

    date_from = date_from.replace(hour=0, minute=0, second=0)
    date_to = date_to.replace(hour=23, minute=59, second=59)

    user_data = await state.get_data()
    chat_id = user_data.get("exp_chat_id", "")
    chat_title = user_data.get("exp_chat_title", "")

    expenses = get_expenses_for_period(date_from, date_to, chat_id)

    if not expenses:
        await m.reply(f"Нет расходов по <b>{chat_title}</b> за этот период.", parse_mode=ParseMode.HTML)
        await state.clear()
        return

    period_str = f"{date_from.strftime('%d.%m')} — {date_to.strftime('%d.%m.%Y')}"
    text_msg = f"<b>Расходы: {chat_title}</b>\n<b>Период: {period_str}</b>\n\n"

    total_usd = 0
    buttons = []
    for e in sorted(expenses, key=lambda x: parse_date_str(x.get("date", "")) or datetime.min):
        cur_display = e.get("currency", "$")
        if cur_display == "$":
            cur_display = "USD"
        line = f"  {e['date']} — {e['type']}: {e['amount']:.0f} {cur_display} ≈ {e['amount_usd']:.0f}$"
        if e.get("comment"):
            line += f" ({e['comment']})"
        text_msg += line + "\n"
        total_usd += e.get("amount_usd", 0)
        buttons.append([InlineKeyboardButton(
            text=f"Удалить: {e['date']} {e['type']} {e['amount']:.0f}",
            callback_data=f"expdel:{e['id']}"
        )])

    text_msg += f"\n<b>Итого расходов:</b> {total_usd:.0f}$"

    kb = InlineKeyboardMarkup(inline_keyboard=buttons) if buttons else None
    await m.reply(text_msg, parse_mode=ParseMode.HTML, reply_markup=kb)
    await state.clear()


# ----------- Удаление расхода -----------
@dp.callback_query(F.data.startswith("expdel:"))
async def expense_delete(c: types.CallbackQuery):
    if c.from_user.id not in OWNERS:
        await c.answer("Нет доступа", show_alert=True)
        return

    exp_id = int(c.data.split(":", 1)[1])
    found = None
    for i, e in enumerate(global_expenses):
        if e.get("id") == exp_id:
            found = i
            break

    if found is not None:
        removed = global_expenses.pop(found)
        save_expenses()
        await c.answer(f"Удалён: {removed['type']} {removed['amount']:.0f}", show_alert=True)
        # Обновляем сообщение — убираем удалённую кнопку
        try:
            # Пересобираем клавиатуру без удалённой кнопки
            old_kb = c.message.reply_markup
            if old_kb:
                new_buttons = []
                for row in old_kb.inline_keyboard:
                    new_row = [btn for btn in row if btn.callback_data != f"expdel:{exp_id}"]
                    if new_row:
                        new_buttons.append(new_row)
                new_kb = InlineKeyboardMarkup(inline_keyboard=new_buttons) if new_buttons else None
                await c.message.edit_reply_markup(reply_markup=new_kb)
        except:
            pass
    else:
        await c.answer("Расход не найден", show_alert=True)


# ==================== НАСТРОЙКИ В ЛИЧКЕ ====================

@dp.message(F.text == SETTINGS_BUTTON_TEXT, F.chat.type == "private")
async def handle_settings_button(m: types.Message, state: FSMContext):
    if m.from_user.id not in OWNERS:
        return
    await state.clear()

    # Текущие курсы
    manual = settings.get("rates", {})
    rates_info = "авто (API)" if not manual else f"лари={manual.get('lari','авто')}, евро={manual.get('euro','авто')}, драм={manual.get('amd','авто')}"

    # Текущие проценты
    custom_pct = settings.get("salary_percent", {})
    pct_lines = []
    all_names = set(list(SALARY_PERCENT.keys()) + list(custom_pct.keys()))
    for name in sorted(all_names):
        pct = get_salary_percent(name)
        pct_lines.append(f"  {name}: {int(pct*100)}%")
    pct_info = "\n".join(pct_lines) if pct_lines else "  По умолчанию 10%"

    # Проценты админов
    admin_pct = settings.get("admin_salary_percent", {})
    admin_lines = []
    all_admin_names = set(list(ADMIN_SALARY_PERCENT.keys()) + list(admin_pct.keys()))
    for name in sorted(all_admin_names):
        pct = get_admin_salary_percent(name)
        admin_lines.append(f"  {name}: {pct*100:.1f}%")
    admin_pct_info = "\n".join(admin_lines) if admin_lines else "  Нет админов"

    text = (
        f"<b>Настройки</b>\n\n"
        f"<b>Курсы валют:</b> {rates_info}\n"
        f"<b>Текущие:</b> 1 лари = {current_lari_to_usd:.4f}$, 1 драм = {current_amd_to_usd:.5f}$\n\n"
        f"<b>Проценты ЗП операторов:</b>\n{pct_info}\n"
        f"  По умолчанию: {int(settings.get('default_percent', DEFAULT_PERCENT)*100)}%\n\n"
        f"<b>Проценты ЗП админов:</b>\n{admin_pct_info}"
    )

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Установить курсы валют", callback_data="set:rates")],
        [InlineKeyboardButton(text="Сбросить курсы (авто)", callback_data="set:rates_reset")],
        [InlineKeyboardButton(text="Настроить % оператора", callback_data="set:percent")],
        [InlineKeyboardButton(text="Настроить % админа", callback_data="set:admin_percent")],
    ])
    await m.answer(text, reply_markup=kb)


@dp.callback_query(F.data.startswith("set:"))
async def settings_callbacks(c: types.CallbackQuery, state: FSMContext):
    if c.from_user.id not in OWNERS:
        await c.answer("Нет доступа", show_alert=True)
        return

    action = c.data.split(":", 1)[1]

    if action == "rates":
        await state.set_state(SettingsState.waiting_for_rate)
        await c.message.edit_text(
            "Введи курсы к доллару в формате:\n"
            "<code>лари 0.37</code> или <code>драм 0.0025</code> или <code>евро 1.05</code>\n\n"
            "Можно несколько через запятую:\n"
            "<code>лари 0.37, драм 0.0025</code>",
            parse_mode=ParseMode.HTML
        )

    elif action == "rates_reset":
        settings.pop("rates", None)
        save_settings()
        update_exchange_rates()
        await c.message.edit_text("Курсы сброшены на автоматические (API).")

    elif action == "percent":
        await state.set_state(SettingsState.waiting_for_percent)
        await c.message.edit_text(
            "Введи процент оператора в формате:\n"
            "<code>Саша 12</code> или <code>Лера 10</code>\n\n"
            "Для изменения процента по умолчанию:\n"
            "<code>по_умолчанию 10</code>",
            parse_mode=ParseMode.HTML
        )

    elif action == "admin_percent":
        await state.set_state(SettingsState.waiting_for_admin_percent)
        await c.message.edit_text(
            "Введи процент админа в формате:\n"
            "<code>Иван 5</code> или <code>Марта 3</code>\n\n"
            "Для дробных процентов:\n"
            "<code>Софа 1.5</code>",
            parse_mode=ParseMode.HTML
        )

    await c.answer()


@dp.message(StateFilter(SettingsState.waiting_for_rate))
async def handle_rate_input(m: types.Message, state: FSMContext):
    if m.from_user.id not in OWNERS or m.chat.type != "private":
        return

    if "rates" not in settings:
        settings["rates"] = {}

    parts = m.text.strip().split(",")
    results = []
    for part in parts:
        match = re.match(r"(лари|лар|евро|euro|драм|dram|amd)\s+([\d.]+)", part.strip(), re.I)
        if match:
            cur = match.group(1).lower()
            val = float(match.group(2))
            if any(x in cur for x in ["лари", "лар"]):
                settings["rates"]["lari"] = val
                results.append(f"лари = {val}$")
            elif any(x in cur for x in ["евро", "euro"]):
                settings["rates"]["euro"] = val
                results.append(f"евро = {val}$")
            elif any(x in cur for x in ["драм", "dram", "amd"]):
                settings["rates"]["amd"] = val
                results.append(f"драм = {val}$")

    if results:
        save_settings()
        update_exchange_rates()
        await m.reply(f"Курсы обновлены:\n" + "\n".join(results))
    else:
        await m.reply("Не удалось распознать. Формат: <code>лари 0.37</code>", parse_mode=ParseMode.HTML)

    await state.clear()


@dp.message(StateFilter(SettingsState.waiting_for_percent))
async def handle_percent_input(m: types.Message, state: FSMContext):
    if m.from_user.id not in OWNERS or m.chat.type != "private":
        return

    text = m.text.strip()
    match = re.match(r"^(по_умолчанию|\S+)\s+(\d+)$", text, re.I)
    if not match:
        await m.reply("Формат: <code>Саша 12</code> или <code>по_умолчанию 10</code>", parse_mode=ParseMode.HTML)
        return

    name = match.group(1)
    pct = int(match.group(2)) / 100

    if name.lower() == "по_умолчанию":
        settings["default_percent"] = pct
        save_settings()
        await m.reply(f"Процент по умолчанию: {int(pct*100)}%")
    else:
        if "salary_percent" not in settings:
            settings["salary_percent"] = {}
        settings["salary_percent"][name] = pct
        save_settings()
        await m.reply(f"Процент {name}: {int(pct*100)}%")

    await state.clear()


@dp.message(StateFilter(SettingsState.waiting_for_admin_percent))
async def handle_admin_percent_input(m: types.Message, state: FSMContext):
    if m.from_user.id not in OWNERS or m.chat.type != "private":
        return

    text = m.text.strip()
    match = re.match(r"^(\S+)\s+(\d+(?:\.\d+)?)$", text, re.I)
    if not match:
        await m.reply("Формат: <code>Иван 5</code> или <code>Софа 1.5</code>", parse_mode=ParseMode.HTML)
        return

    name = match.group(1)
    pct = float(match.group(2)) / 100

    if "admin_salary_percent" not in settings:
        settings["admin_salary_percent"] = {}
    settings["admin_salary_percent"][name] = pct
    save_settings()
    await m.reply(f"Процент админа {name}: {pct*100:.1f}%")

    await state.clear()


# ----------- /crypto — проверка баланса и последних поступлений -----------
@dp.message(Command("crypto"))
async def cmd_crypto(m: types.Message):
    if not CRYPTO_WALLET:
        await m.reply("Крипто-мониторинг не настроен.")
        return

    # Доступно в чате КРИПТА (или группе с топиком КРИПТА) и в ЛС владельцам
    is_crypto_chat = (m.chat.id == CRYPTO_CHAT)
    is_crypto_topic = (CRYPTO_TOPIC and m.message_thread_id == CRYPTO_TOPIC)
    if not is_crypto_chat and not is_crypto_topic and m.from_user.id not in OWNERS:
        return

    balance = get_usdt_balance(CRYPTO_WALLET)
    transfers = get_recent_usdt_transfers(CRYPTO_WALLET, limit=5)

    wallet_short = f"{CRYPTO_WALLET[:6]}...{CRYPTO_WALLET[-4:]}"
    text = f"<b>Крипто-кошелёк</b>\n"
    text += f"Адрес: <code>{CRYPTO_WALLET}</code>\n"
    text += f"<b>Баланс: {balance:.2f} USDT</b>\n\n"

    if transfers:
        text += "<b>Последние поступления:</b>\n"
        for tx in transfers[:5]:
            amount = float(tx.get("value", 0)) / 1_000_000
            from_addr = tx.get("from", "—")
            timestamp = tx.get("block_timestamp", 0)
            tx_time = datetime.fromtimestamp(timestamp / 1000).strftime("%d.%m %H:%M") if timestamp else "—"
            from_short = f"{from_addr[:6]}...{from_addr[-4:]}" if len(from_addr) > 10 else from_addr
            text += f"  {tx_time} — {amount:.2f} USDT от {from_short}\n"
    else:
        text += "Поступлений пока нет.\n"

    await m.reply(text, parse_mode=ParseMode.HTML)


# ----------- /anketa — ручной запуск ротации (владельцы) -----------
@dp.message(Command("anketa"))
async def cmd_anketa(m: types.Message):
    if m.from_user.id not in OWNERS:
        return
    if m.chat.type != "private":
        await m.reply("Напиши в личку!")
        return
    if not OPERATORS or not GOOGLE_SHEET_ID:
        await m.reply("Ротация анкет не настроена.")
        return

    a_state = load_anketa_state()
    today = datetime.now().strftime("%d.%m.%Y")
    if a_state.get("last_date") == today:
        await m.reply(f"Анкеты уже распределены сегодня ({today}). Чтобы перераспределить — удали файл anketa_state.json и повтори.")
        return

    await m.reply("Запускаю ротацию анкет...")
    await distribute_anketas()
    await m.reply("Готово!")


# ----------- /save_current — ручное сохранение текущих смен в архив -----------
@dp.message(Command("save_current"))
async def cmd_save_current(m: types.Message):
    """Ручная команда: сохранить текущие смены в архив (для первого запуска)."""
    if m.from_user.id not in OWNERS:
        return
    if m.chat.type != "private":
        await m.reply("Напиши в личку!")
        return

    count = 0
    for chat_str, chat_data in data.get("chats", {}).items():
        if chat_data.get("bookings"):
            archive_shift(chat_str)
            count += 1
    await m.reply(f"Сохранено {count} смен в архив.")


def cleanup_old_history():
    """Удаляет записи из history.json старше 90 дней."""
    cutoff = datetime.now() - timedelta(days=90)
    before = len(history.get("shifts", []))
    history["shifts"] = [
        s for s in history.get("shifts", [])
        if (parse_date_str(s.get("date", "")) or datetime.min) >= cutoff
    ]
    after = len(history["shifts"])
    if before != after:
        save_history()
        print(f"Очистка history: удалено {before - after} старых записей")


# ==================== РОТАЦИЯ АНКЕТ (Google Sheets) ====================
ANKETA_FILE = "anketa_state.json"


def load_anketa_state() -> dict:
    try:
        with open(ANKETA_FILE, "r") as f:
            return json.load(f)
    except:
        return {"last_date": "", "offset": 0}


def save_anketa_state(state_data: dict):
    with open(ANKETA_FILE, "w", encoding="utf-8") as f:
        json.dump(state_data, f, ensure_ascii=False, indent=2)


def get_google_sheet():
    """Подключается к Google Sheets и возвращает лист."""
    try:
        import gspread
        from google.oauth2.service_account import Credentials
    except ImportError:
        print("gspread или google-auth не установлены! pip install gspread google-auth")
        return None

    try:
        scopes = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ]
        creds = Credentials.from_service_account_file(GOOGLE_CREDS_FILE, scopes=scopes)
        gc = gspread.authorize(creds)
        spreadsheet = gc.open_by_key(GOOGLE_SHEET_ID)
        sheet = spreadsheet.worksheet(GOOGLE_SHEET_NAME)
        return sheet
    except Exception as e:
        print(f"Ошибка подключения к Google Sheets: {e}")
        return None


def get_active_anketas(sheet) -> list:
    """Читает таблицу, возвращает активные анкеты (столбец D не пуст и не Off)."""
    rows = sheet.get_all_values()
    anketas = []
    for i, row in enumerate(rows[1:], start=2):  # пропускаем заголовок, нумерация строк с 2
        if len(row) < 6:
            continue
        col_d = row[3].strip() if len(row) > 3 else ""
        if not col_d or col_d.lower() == "off":
            continue
        anketas.append({
            "row": i,
            "site": row[0].strip(),       # A — сайт
            "login": row[1].strip(),       # B — логин
            "password": row[2].strip(),    # C — пароль
            "date": col_d,                 # D — дата
            "operator": row[4].strip() if len(row) > 4 else "",  # E — оператор
            "deva": row[5].strip() if len(row) > 5 else "",      # F — дева
            "phone": row[6].strip() if len(row) > 6 else "",     # G — номер
        })
    return anketas


async def distribute_anketas():
    """Ротация анкет: распределяет активные анкеты среди операторов по кругу."""
    if not OPERATORS or not GOOGLE_SHEET_ID:
        return

    sheet = get_google_sheet()
    if not sheet:
        return

    anketas = get_active_anketas(sheet)
    if not anketas:
        print("Нет активных анкет.")
        return

    # Загружаем состояние ротации
    a_state = load_anketa_state()
    today = datetime.now().strftime("%d.%m.%Y")

    # Если сегодня уже распределяли — пропускаем
    if a_state.get("last_date") == today:
        print("Анкеты уже распределены сегодня.")
        return

    # Увеличиваем offset
    offset = a_state.get("offset", 0) + 1
    a_state["last_date"] = today
    a_state["offset"] = offset
    save_anketa_state(a_state)

    # Список активных операторов
    op_list = list(OPERATORS.items())  # [(имя, tg_id), ...]
    num_ops = len(op_list)
    num_anketas = len(anketas)

    if num_ops == 0:
        return

    # Распределяем по кругу
    for i, (op_name, op_tg_id) in enumerate(op_list):
        anketa_idx = (i + offset) % num_anketas
        anketa = anketas[anketa_idx]

        # Отправляем оператору в ЛС
        msg = (
            f"<b>Твоя анкета на сегодня:</b>\n\n"
            f"<b>Дева:</b> {anketa['deva']}\n"
            f"<b>Логин:</b> <code>{anketa['login']}</code>\n"
            f"<b>Пароль:</b> <code>{anketa['password']}</code>\n\n"
            f"Отправь номер телефона, который поставишь на эту анкету:"
        )

        try:
            from aiogram.fsm.storage.memory import MemoryStorage
            # Записываем в таблицу имя оператора (столбец E)
            sheet.update_cell(anketa["row"], 5, op_name)

            await bot.send_message(op_tg_id, msg, parse_mode=ParseMode.HTML)

            # Сохраняем привязку оператор -> строка, чтобы записать номер позже
            anketa_assignments[op_tg_id] = {
                "row": anketa["row"],
                "anketa": anketa,
                "date": today,
            }
            print(f"Анкета {anketa['login']} → {op_name}")
        except Exception as e:
            print(f"Ошибка отправки анкеты {op_name}: {e}")

    # Уведомляем владельцев
    summary = f"<b>Анкеты распределены на {today}:</b>\n\n"
    for i, (op_name, op_tg_id) in enumerate(op_list):
        anketa_idx = (i + offset) % num_anketas
        anketa = anketas[anketa_idx]
        summary += f"{op_name} → {anketa['deva']} ({anketa['login']})\n"

    for owner_id in OWNERS:
        try:
            await bot.send_message(owner_id, summary, parse_mode=ParseMode.HTML)
        except:
            pass


# Хранилище привязок оператор -> строка в таблице (для записи номера)
anketa_assignments = {}


# ----------- Оператор отправляет номер (catch-all для ЛС) -----------
@dp.message(F.chat.type == "private")
async def handle_operator_phone(m: types.Message, state: FSMContext):
    """Если оператор написал номер после получения анкеты."""
    tg_id = m.from_user.id

    # Не перехватываем, если владелец в процессе ввода (FSM)
    current_state = await state.get_state()
    if current_state:
        return

    # Проверяем — есть ли привязка
    assignment = anketa_assignments.get(tg_id)
    if not assignment:
        return  # Не наш кейс — пропускаем

    # Проверяем что сообщение похоже на номер (цифры, +, пробелы)
    phone = m.text.strip() if m.text else ""
    if not re.match(r"^[\d\s\+\-\(\)]{7,20}$", phone):
        return  # Не похоже на номер — пропускаем

    # Записываем номер в Google Sheets (столбец G)
    try:
        sheet = get_google_sheet()
        if sheet:
            sheet.update_cell(assignment["row"], 7, phone)
            await m.reply(f"Номер <b>{phone}</b> записан. Удачной смены!", parse_mode=ParseMode.HTML)
            # Удаляем привязку — номер записан
            del anketa_assignments[tg_id]
        else:
            await m.reply("Ошибка подключения к таблице. Отправь номер ещё раз.")
    except Exception as e:
        await m.reply(f"Ошибка записи: {e}")


# ==================== КРИПТО-МОНИТОРИНГ USDT TRC-20 ====================
TRONGRID_API = "https://api.trongrid.io"
USDT_CONTRACT = "TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t"  # USDT TRC-20 contract

last_seen_tx = None  # Последний обработанный txID


def get_usdt_balance(wallet: str) -> float:
    """Получает баланс USDT на кошельке."""
    try:
        # Метод 1: через trc20 балансы аккаунта
        url = f"{TRONGRID_API}/v1/accounts/{wallet}"
        resp = requests.get(url, timeout=10)
        if resp.status_code == 200:
            data = resp.json().get("data", [])
            if data:
                trc20 = data[0].get("trc20", [])
                for token in trc20:
                    if USDT_CONTRACT in token:
                        return float(token[USDT_CONTRACT]) / 1_000_000
    except:
        pass
    return 0.0


def get_recent_usdt_transfers(wallet: str, limit: int = 5) -> list:
    """Получает последние входящие USDT транзакции."""
    try:
        url = f"{TRONGRID_API}/v1/accounts/{wallet}/transactions/trc20"
        params = {
            "only_to": "true",
            "contract_address": USDT_CONTRACT,
            "limit": limit,
        }
        resp = requests.get(url, params=params, timeout=10)
        if resp.status_code == 200:
            return resp.json().get("data", [])
    except:
        pass
    return []


async def check_crypto_payments():
    """Проверяет новые входящие USDT транзакции и отправляет уведомления."""
    global last_seen_tx
    if not CRYPTO_WALLET or not CRYPTO_CHAT:
        return

    try:
        transfers = get_recent_usdt_transfers(CRYPTO_WALLET, limit=3)
        if not transfers:
            return

        # При первом запуске запоминаем последнюю транзакцию
        if last_seen_tx is None:
            last_seen_tx = transfers[0].get("transaction_id", "")
            return

        # Проверяем новые транзакции
        new_txs = []
        for tx in transfers:
            tx_id = tx.get("transaction_id", "")
            if tx_id == last_seen_tx:
                break
            new_txs.append(tx)

        if not new_txs:
            return

        # Обновляем last_seen
        last_seen_tx = transfers[0].get("transaction_id", "")

        # Получаем баланс
        balance = get_usdt_balance(CRYPTO_WALLET)

        # Отправляем уведомления
        for tx in reversed(new_txs):
            amount = float(tx.get("value", 0)) / 1_000_000
            from_addr = tx.get("from", "неизвестно")
            tx_id = tx.get("transaction_id", "")
            timestamp = tx.get("block_timestamp", 0)
            tx_time = datetime.fromtimestamp(timestamp / 1000).strftime("%d.%m.%Y %H:%M") if timestamp else "—"

            from_short = f"{from_addr[:6]}...{from_addr[-4:]}" if len(from_addr) > 10 else from_addr

            # Короткое сообщение для чата КРИПТА
            msg_chat = (
                f"<b>Поступление USDT</b>\n\n"
                f"<b>Сумма:</b> {amount:.2f} USDT\n"
                f"<b>Время:</b> {tx_time}\n"
                f"<b>От:</b> {from_short}\n\n"
                f"<b>Баланс кошелька:</b> {balance:.2f} USDT"
            )

            # Полное сообщение для владельцев
            msg_owner = (
                f"<b>Поступление USDT</b>\n\n"
                f"<b>Сумма:</b> {amount:.2f} USDT\n"
                f"<b>Время:</b> {tx_time}\n"
                f"<b>От:</b> <code>{from_addr}</code>\n"
                f"<b>Хеш:</b> <code>{tx_id}</code>\n\n"
                f"<b>Баланс кошелька:</b> {balance:.2f} USDT"
            )

            try:
                await bot.send_message(CRYPTO_CHAT, msg_chat, parse_mode=ParseMode.HTML,
                                       message_thread_id=CRYPTO_TOPIC)
            except Exception as e:
                print(f"Ошибка отправки крипто-уведомления в чат: {e}")

            # Дублируем владельцам в ЛС
            for owner_id in OWNERS:
                try:
                    await bot.send_message(owner_id, msg_owner, parse_mode=ParseMode.HTML)
                except:
                    pass

    except Exception as e:
        print(f"Ошибка проверки крипто: {e}")


async def crypto_monitor_loop():
    """Цикл проверки крипто-платежей раз в 60 секунд."""
    if not CRYPTO_WALLET or not CRYPTO_CHAT:
        return
    while True:
        await check_crypto_payments()
        await asyncio.sleep(60)


async def scheduler():
    """Свой scheduler без aioschedule — проверяет время каждые 30 сек."""
    triggered_today = set()  # Какие задачи уже выполнены сегодня

    while True:
        now = datetime.now()
        today = now.strftime("%Y-%m-%d")
        hhmm = now.strftime("%H:%M")

        # Сброс триггеров в полночь
        if not any(today in t for t in triggered_today):
            triggered_today.clear()

        key = f"{today}_{hhmm}"

        if hhmm == "09:00" and f"{today}_daily" not in triggered_today:
            triggered_today.add(f"{today}_daily")
            asyncio.create_task(daily_job())

        if hhmm == "14:00" and f"{today}_anketa" not in triggered_today:
            if OPERATORS and GOOGLE_SHEET_ID:
                triggered_today.add(f"{today}_anketa")
                asyncio.create_task(distribute_anketas())

        await asyncio.sleep(30)


async def main():
    load_data()
    load_history()
    load_expenses()
    load_settings()
    await daily_job()
    asyncio.create_task(scheduler())
    if CRYPTO_WALLET and CRYPTO_CHAT:
        asyncio.create_task(crypto_monitor_loop())
        print(f"Крипто-мониторинг запущен: {CRYPTO_WALLET[:8]}...")
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
