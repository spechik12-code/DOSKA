# bot.py — ПОЛНАЯ ВЕРСИЯ С РАСХОДАМИ (ТОЛЬКО ОТОБРАЖЕНИЕ, БЕЗ ВЫЧИТАНИЯ ИЗ ВЫРУЧКИ)
import asyncio
import aioschedule
import json
import re
import requests
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

from config import TOKEN, OWNERS, ALLOWED_CHATS

storage = MemoryStorage()
dp = Dispatcher(storage=storage)
bot = Bot(token=TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))

DATA_FILE = "data.json"
data = {"chats": {}}

# Fallback курсы
FALLBACK_LARI_TO_USD = 0.37
FALLBACK_EURO_TO_USD = 1.05
FALLBACK_AMD_TO_USD = 0.0025  # Армянский драм

current_lari_to_usd = FALLBACK_LARI_TO_USD
current_euro_to_usd = FALLBACK_EURO_TO_USD
current_amd_to_usd = FALLBACK_AMD_TO_USD  # Армянский драм


def update_exchange_rates():
    global current_lari_to_usd, current_euro_to_usd, current_amd_to_usd
    try:
        response = requests.get("https://api.exchangerate.host/latest?base=USD", timeout=5)
        if response.status_code == 200:
            rates = response.json()["rates"]
            current_lari_to_usd = 1 / rates.get("GEL", 1 / FALLBACK_LARI_TO_USD)
            current_euro_to_usd = rates.get("EUR", FALLBACK_EURO_TO_USD)
            current_amd_to_usd = 1 / rates.get("AMD", 1 / FALLBACK_AMD_TO_USD)  # Армянский драм
    except:
        pass


# ==================== ПРОЦЕНТЫ ЗП ПО ИМЕНАМ ====================
SALARY_PERCENT = {
    "Саша": 0.12,
    "Света": 0.12,
}

DEFAULT_PERCENT = 0.10


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

    booking = {
        "time": time_part,
        "info": info,
        "duration": pretty,
        "duration_sec": sec,
        "original_text": text,
        "id": data["chats"][chat_str]["next_id"],
        "msg_id": m.message_id
    }
    data["chats"][chat_str]["next_id"] += 1
    data["chats"][chat_str]["bookings"].append(booking)
    save_data()

    await refresh_board(m.chat.id)

    kb = personal_kb(booking["id"])
    await m.reply(
        f"Бронь добавлена:\n{booking['time']} — {booking['info']} ({booking['duration']})",
        reply_markup=kb
    )


# ==================== CALLBACK КНОПКИ ====================
@dp.callback_query(F.data.startswith("done:"))
async def cb_done(cb: types.CallbackQuery):
    if cb.message.chat.id not in ALLOWED_CHATS:
        return await cb.answer("Недоступно")

    bid = int(cb.data.split(":")[1])
    chat_str = str(cb.message.chat.id)
    idx = find_booking_index(chat_str, bid)
    if idx is None:
        return await cb.answer("Бронь не найдена")

    b = data["chats"][chat_str]["bookings"][idx]
    if b.get("deleted"):
        return await cb.answer("Бронь удалена")

    b["done"] = True
    b["cancelled"] = False
    save_data()

    kb = personal_kb(bid, done=True, cancelled=False)
    try:
        await cb.message.edit_reply_markup(reply_markup=kb)
    except:
        pass

    await refresh_board(cb.message.chat.id)
    asyncio.create_task(booking_timer(cb.message.chat.id, bid))
    await cb.answer("Клиент пришёл, таймер запущен!")


@dp.callback_query(F.data.startswith("cancel:"))
async def cb_cancel(cb: types.CallbackQuery):
    if cb.message.chat.id not in ALLOWED_CHATS:
        return await cb.answer("Недоступно")

    bid = int(cb.data.split(":")[1])
    chat_str = str(cb.message.chat.id)
    idx = find_booking_index(chat_str, bid)
    if idx is None:
        return await cb.answer("Бронь не найдена")

    b = data["chats"][chat_str]["bookings"][idx]
    if b.get("deleted"):
        return await cb.answer("Бронь удалена")

    b["cancelled"] = True
    b["done"] = False
    save_data()

    kb = personal_kb(bid, done=False, cancelled=True)
    try:
        await cb.message.edit_reply_markup(reply_markup=kb)
    except:
        pass

    await refresh_board(cb.message.chat.id)
    await cb.answer("Отмечено: клиент не пришёл")


@dp.callback_query(F.data.startswith("delete:"))
async def cb_delete(cb: types.CallbackQuery):
    if cb.message.chat.id not in ALLOWED_CHATS:
        return await cb.answer("Недоступно")

    bid = int(cb.data.split(":")[1])
    chat_str = str(cb.message.chat.id)
    idx = find_booking_index(chat_str, bid)
    if idx is None:
        return await cb.answer("Бронь не найдена")

    b = data["chats"][chat_str]["bookings"][idx]
    b["deleted"] = True
    b["done"] = False
    b["cancelled"] = False
    save_data()

    kb = personal_kb(bid, deleted=True)
    try:
        await cb.message.edit_reply_markup(reply_markup=kb)
    except:
        pass

    await refresh_board(cb.message.chat.id)
    await cb.answer("Бронь удалена")


@dp.callback_query(F.data.startswith("edit:"))
async def cb_edit(cb: types.CallbackQuery, state: FSMContext):
    if cb.message.chat.id not in ALLOWED_CHATS:
        return await cb.answer("Недоступно")

    bid = int(cb.data.split(":")[1])
    chat_str = str(cb.message.chat.id)
    idx = find_booking_index(chat_str, bid)
    if idx is None:
        return await cb.answer("Бронь не найдена")

    b = data["chats"][chat_str]["bookings"][idx]
    if b.get("deleted"):
        return await cb.answer("Бронь удалена")

    await state.set_state(EditState.waiting_for_new_text)
    await state.update_data(booking_id=bid)
    await cb.message.answer("Отправь новый текст брони в формате:\nВремя Имя [продолжительность]")
    await cb.answer()


@dp.message(StateFilter(EditState.waiting_for_new_text))
async def process_edit(m: types.Message, state: FSMContext):
    if m.chat.id not in ALLOWED_CHATS:
        return

    data_state = await state.get_data()
    bid = data_state.get("booking_id")
    if bid is None:
        await state.clear()
        return

    chat_str = str(m.chat.id)
    idx = find_booking_index(chat_str, bid)
    if idx is None:
        await m.reply("Бронь не найдена")
        await state.clear()
        return

    text = m.text.strip()
    if not re.match(r"^\d{1,2}:\d{2}", text):
        await m.reply("Неверный формат. Попробуй ещё раз или отправь /cancel")
        return

    time_part = text.split(maxsplit=1)[0]
    rest = text[len(time_part):].strip()
    sec, pretty = parse_duration(rest)
    info = re.sub(r"\d+\s*(ч|час|мин|минут|м)\b.*$", "", rest, flags=re.I).strip() or "Без имени"

    b = data["chats"][chat_str]["bookings"][idx]
    b["time"] = time_part
    b["info"] = info
    b["duration"] = pretty
    b["duration_sec"] = sec
    b["original_text"] = text
    save_data()

    await refresh_board(m.chat.id)
    await m.reply(f"Бронь обновлена:\n{b['time']} — {b['info']} ({b['duration']})")
    await state.clear()


# ==================== ДОБАВЛЕНИЕ РАСХОДА ====================
@dp.message(Command("expense"))
async def cmd_expense(m: types.Message):
    if m.from_user.id not in OWNERS:
        await m.reply("Ты не владелец")
        return

    text = m.text[len("/expense"):].strip()
    if not text:
        await m.reply("Формат: /expense тип сумма [комментарий]\nПример: /expense Расходники 50 оплата за аренду")
        return

    parts = text.split(maxsplit=2)
    if len(parts) < 2:
        await m.reply("Не хватает данных. Нужно минимум тип и сумма")
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
            percent = SALARY_PERCENT.get(name, DEFAULT_PERCENT)
            salary = usd * percent
            result += f"{name}: {salary:.2f} USD ({int(percent*100)}%)\n"

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
async def send_summary_for_all_chats():
    for chat_id in ALLOWED_CHATS:
        try:
            chat_str = str(chat_id)
            if chat_str not in data["chats"]:
                continue

            full_message = await generate_summary_text(chat_str)

            for owner_id in OWNERS:
                await bot.send_message(owner_id, full_message, parse_mode=ParseMode.HTML)
        except Exception as e:
            print(f"Ошибка при отправке итогов для чата {chat_id}: {e}")


# ==================== КОМАНДЫ ====================
@dp.message(Command("new_shift"))
async def cmd_new_shift(m: types.Message):
    if m.from_user.id not in OWNERS:
        await m.reply("Ты не владелец")
        return
    await ensure_chat(m.chat.id)
    chat_str = str(m.chat.id)
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


async def scheduler():
    aioschedule.every().day.at("09:00").do(daily_job)
    aioschedule.every().day.at("08:59").do(send_summary_for_all_chats)
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
