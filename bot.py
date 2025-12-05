# bot.py — ФИНАЛЬНАЯ ВЕРСИЯ + УДАЛЕНИЕ ВСПОМОГАТЕЛЬНОГО СООБЩЕНИЯ ПОСЛЕ РЕДАКТИРОВАНИЯ
import asyncio
import aioschedule
import json
import re
from datetime import datetime, timedelta
from typing import Optional

from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command, StateFilter
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.context import FSMContext
from aiogram.fsm.storage.memory import MemoryStorage

from config import TOKEN, OWNER_ID, ALLOWED_CHATS

storage = MemoryStorage()
dp = Dispatcher(storage=storage)
bot = Bot(token=TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))

DATA_FILE = "data.json"
data = {"chats": {}}


# ==================== СМЕНА 09:00 → 08:00 + НАЗВАНИЕ ЧАТА ====================
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
            "board_msg": None,
            "date": current_date,
            "chat_title": chat_title,
            "next_id": 1,
        }
    else:
        if data["chats"][s]["date"] != current_date:
            data["chats"][s]["bookings"] = []
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


load_data()


class EditState(StatesGroup):
    waiting_for_new_text = State()


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


# УМНАЯ СОРТИРОВКА ВРЕМЕНИ (чтобы 01:00 шёл после 23:59)
def time_key(time_str: str) -> int:
    hh, mm = map(int, time_str.split(':'))
    minutes = hh * 60 + mm
    if hh < 5:  # 00:00–04:59 — это "следующий день"
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
            if b.get("done"): text += " Пришёл"
            if b.get("deleted"): text = f"<s>{text} Отменено</s>"
            elif b.get("cancelled"): text = f"<s>{text} Не пришёл</s>"
            lines.append(text)

    full_text = "\n".join(lines)
    msg_id = chat_data.get("board_msg")

    if msg_id:
        try:
            await bot.edit_message_text(full_text, chat_id, msg_id, parse_mode=ParseMode.HTML)
        except:
            msg = await bot.send_message(chat_id, full_text, parse_mode=ParseMode.HTML)
            chat_data["board_msg"] = msg.message_id
    else:
        msg = await bot.send_message(chat_id, full_text, parse_mode=ParseMode.HTML)
        chat_data["board_msg"] = msg.message_id
    save_data()


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
    }
    data["chats"][chat_str]["bookings"].append(booking)
    save_data()

    sorted_b = sorted(data["chats"][chat_str]["bookings"], key=lambda x: time_key(x["time"]))
    pos = next((i + 1 for i, b in enumerate(sorted_b) if b["id"] == bid), 0)

    reply = await m.reply(f"Добавлено!\n{pos}. {time_part} — {info} ({pretty})", reply_markup=personal_kb(bid))
    booking["reply_msg_id"] = reply.message_id
    save_data()
    await refresh_board(m.chat.id)


# ==================== ТАЙМЕР (как был) ====================
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
    if c.from_user.id not in (b["author_id"], OWNER_ID):
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
    if c.from_user.id not in (b["author_id"], OWNER_ID):
        await c.answer("Это не твоя бронь! Редактировать нельзя.", show_alert=True)
        return

    await state.update_data(edit_bid=bid, reply_msg_id=b.get("reply_msg_id"))
    await state.set_state(EditState.waiting_for_new_text)

    await c.message.reply(
        "<b>Редактируй бронь:</b>\n\n"
        f"<b>Текущая:</b> <code>{b['time']} {b['info']} {b['duration']}</code>\n\n"
        "<b>Пиши в формате:</b>\n"
        "<code>18:30 Анна 1ч 30мин</code>\n"
        "<code>15:00 Иван 300 лари</code>\n\n"
        "<i>/cancel — отменить редактирование</i>",
        parse_mode=ParseMode.HTML
    )


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
    if m.from_user.id not in (b["author_id"], OWNER_ID):
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

    # ← НОВАЯ ФИЧА: УДАЛЯЕМ СООБЩЕНИЕ "Редактируй бронь: ..."
    try:
        await m.reply_to_message.delete()
    except:
        pass

    await refresh_board(m.chat.id)
    await state.clear()


# ==================== КОМАНДЫ ====================
@dp.message(Command("new_shift"))
async def cmd_new_shift(m: types.Message):
    if m.from_user.id != OWNER_ID:
        await m.reply("Ты не босс")
        return
    await ensure_chat(m.chat.id)
    chat_str = str(m.chat.id)
    current_date, chat_title = await get_shift_info(m.chat.id)
    data["chats"][chat_str]["bookings"] = []
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


@dp.message(Command("cancel"))
async def cmd_cancel(m: types.Message, state: FSMContext):
    if await state.get_state():
        await state.clear()
        await m.reply("Отменено.")
    else:
        await m.reply("Нечего отменять.")


# ==================== ПЛАНИРОВЩИК ====================
async def daily_job():
    for cid in ALLOWED_CHATS:
        try:
            await refresh_board(cid)
        except:
            pass


async def scheduler():
    aioschedule.every().day.at("09:00").do(lambda: asyncio.create_task(daily_job()))
    while True:
        await aioschedule.run_pending()
        await asyncio.sleep(1)


async def main():
    load_data()
    await daily_job()
    asyncio.create_task(scheduler())
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())