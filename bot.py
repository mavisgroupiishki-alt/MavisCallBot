"""
Бот отчётов по пропущенным звонкам.
Работает через GitHub Actions (без сервера).

Два режима:
  python bot.py poll    — проверяет новые сообщения, отвечает на "с личного"
  python bot.py report  — генерирует и отправляет утренний отчёт

Данные хранятся в state.json (кешируется между запусками GitHub Actions).
"""

import os
import sys
import re
import json
import requests
from datetime import datetime, timedelta
from collections import defaultdict
from pathlib import Path

# ── Настройки (из GitHub Secrets → env) ──────────────────────
BOT_TOKEN = "8771665068:AAEFWQUF0Wrojh6HUhWwhzRG2Ae01csSm-Q"
GROUP_CHAT_ID = os.environ.get("GROUP_CHAT_ID", "")
REPORT_CHAT_ID = "1112419667"
BITRIX_WEBHOOK = os.environ.get(
    "BITRIX_WEBHOOK",
    "https://mavisgroup.bitrix24.by/rest/2110/crxrgmh6653tjopg/"
)

TG_API = f"https://api.telegram.org/bot{BOT_TOKEN}"
STATE_FILE = Path("state.json")


# ══════════════════════════════════════════════════════════════
#  СОСТОЯНИЕ (хранится в JSON между запусками)
# ══════════════════════════════════════════════════════════════

def load_state() -> dict:
    if STATE_FILE.exists():
        with open(STATE_FILE) as f:
            return json.load(f)
    return {"last_update_id": 0, "calls": {}}


def save_state(state: dict):
    with open(STATE_FILE, "w") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)


# ══════════════════════════════════════════════════════════════
#  TELEGRAM API
# ══════════════════════════════════════════════════════════════

def tg(method: str, **params):
    """Вызов Telegram Bot API."""
    r = requests.post(f"{TG_API}/{method}", json=params, timeout=30)
    return r.json()


def get_updates(offset: int = 0) -> list:
    """Получить новые сообщения."""
    data = tg("getUpdates", offset=offset, timeout=5)
    return data.get("result", [])


def send_message(chat_id, text: str, reply_to: int = None):
    """Отправить сообщение в чат."""
    params = {"chat_id": chat_id, "text": text}
    if reply_to:
        params["reply_to_message_id"] = reply_to
    result = tg("sendMessage", **params)
    print(f"[TG SEND] chat_id={chat_id} ok={result.get('ok')} error={result.get('description','')}")
    return result


# ══════════════════════════════════════════════════════════════
#  BITRIX24 API
# ══════════════════════════════════════════════════════════════

def bitrix(method: str, params: dict = None) -> dict | None:
    url = BITRIX_WEBHOOK.rstrip("/") + "/" + method
    for attempt in range(3):
        try:
            r = requests.post(url, json=params or {}, timeout=120)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            if attempt < 2:
                print(f"[BITRIX RETRY] {method} attempt {attempt+1}: {e}")
                import time; time.sleep(2)
            else:
                print(f"[BITRIX ERROR] {method}: {e}")
    return None


def bitrix_all(method: str, params: dict = None) -> list:
    """Постраничная выгрузка из Bitrix24."""
    params = dict(params or {})
    result = []
    start = 0
    while True:
        params["start"] = start
        url = BITRIX_WEBHOOK.rstrip("/") + "/" + method
        try:
            r = requests.post(url, json=params, timeout=120)
            r.raise_for_status()
            data = r.json()
        except Exception as e:
            print(f"[BITRIX ERROR] {method}: {e}")
            break
        if not data or "result" not in data:
            break
        items = data["result"]
        if isinstance(items, list):
            result.extend(items)
        elif isinstance(items, dict):
            for v in items.values():
                if isinstance(v, list):
                    result.extend(v)
                    break
        if len(result) >= data.get("total", 0) or not items:
            break
        start += 50
    return result


def normalize_phone(phone: str) -> str:
    digits = re.sub(r"\D", "", phone)
    if len(digits) == 11 and digits.startswith("8"):
        digits = "7" + digits[1:]
    return digits


def get_bitrix_calls(date_str: str) -> list:
    """Все звонки из Bitrix24 за дату (формат: YYYY-MM-DD)."""
    d = datetime.strptime(date_str, "%Y-%m-%d")
    d_next = d + timedelta(days=1)
    params = {
        "FILTER": {
            ">CALL_START_DATE": d.strftime("%Y-%m-%dT00:00:00"),
            "<CALL_START_DATE": d_next.strftime("%Y-%m-%dT00:00:00"),
        },
        "SORT": "CALL_START_DATE",
        "ORDER": "ASC",
    }
    return bitrix_all("voximplant.statistic.get", params)


# ══════════════════════════════════════════════════════════════
#  ПАРСИНГ СООБЩЕНИЙ
# ══════════════════════════════════════════════════════════════

def parse_missed(text: str) -> dict | None:
    """Парсит сообщение Zruchna-bot о пропущенном."""
    if "Пропущенный вызов" not in text:
        return None

    def extract(pattern):
        m = re.search(pattern, text)
        return m.group(1).strip() if m else ""

    call_time = ""
    m = re.search(r"Время звонка:\s*([\d\-_:]+)", text)
    if m:
        call_time = m.group(1).replace("_", " ")

    m = re.search(r"(https?://\S+)", text)
    deal_url = m.group(1).strip() if m else ""

    return {
        "manager": extract(r"Ответственный менеджер:\s*(.+)"),
        "company": extract(r"Название компании:\s*(.+)"),
        "contact": extract(r"Имя контакта:\s*(.+)"),
        "phone": extract(r"Номер телефона:\s*(\+?[\d\s]+)"),
        "call_time": call_time,
        "deal_url": deal_url,
        "personal": False,
        "personal_by": "",
        "personal_time": "",
    }


def is_personal(text: str) -> bool:
    lower = text.lower().strip()
    patterns = [
        r"с\s+личного",
        r"звонил[аи]?\s+с\s+личного",
        r"перезвонил[аи]?\s+с\s+личного",
    ]
    return any(re.search(p, lower) for p in patterns)


# ══════════════════════════════════════════════════════════════
#  POLL — проверка новых сообщений (каждые 10 мин)
# ══════════════════════════════════════════════════════════════

def poll():
    state = load_state()
    offset = state["last_update_id"] + 1 if state["last_update_id"] else 0
    updates = get_updates(offset)

    if not updates:
        print("Нет новых сообщений.")
        save_state(state)
        return

    print(f"Получено {len(updates)} обновлений.")

    for upd in updates:
        state["last_update_id"] = upd["update_id"]
        msg = upd.get("message")
        if not msg or not msg.get("text"):
            continue

        text = msg["text"]
        msg_id = msg["message_id"]
        chat_id = msg["chat"]["id"]

        # 1. Пропущенный вызов от Zruchna-bot
        data = parse_missed(text)
        if data:
            key = str(msg_id)
            if key not in state["calls"]:
                state["calls"][key] = data
                print(f"  + Пропущенный: {data['phone']} → {data['manager']}")
            continue

        # 2. Ответ "с личного"
        reply = msg.get("reply_to_message")
        if reply and is_personal(text):
            reply_key = str(reply["message_id"])
            if reply_key in state["calls"]:
                call = state["calls"][reply_key]
                if not call["personal"]:
                    who = ""
                    user = msg.get("from", {})
                    first = user.get("first_name", "")
                    last = user.get("last_name", "")
                    who = f"{first} {last}".strip() or user.get("username", "")

                    call["personal"] = True
                    call["personal_by"] = who
                    call["personal_time"] = datetime.now().strftime(
                        "%Y-%m-%d %H:%M:%S"
                    )

                    send_message(
                        chat_id,
                        f"✅ Принято: {who} перезвонил(а) с личного — "
                        f"{call['company']} ({call['phone']})",
                        reply_to=msg_id,
                    )
                    print(f"  ✅ С личного: {call['phone']} — {who}")
            else:
                print(f"  ⚠ Reply на msg_id={reply['message_id']} — не найдено в базе")

    save_state(state)
    print(f"Состояние сохранено. Записей: {len(state['calls'])}")


# ══════════════════════════════════════════════════════════════
#  REPORT — утренний отчёт
# ══════════════════════════════════════════════════════════════

def _fmt(seconds: float) -> str:
    s = int(seconds)
    h, rem = divmod(s, 3600)
    m, sec = divmod(rem, 60)
    return f"{h}:{m:02d}:{sec:02d}"


def _short(name: str) -> str:
    parts = name.strip().split()
    return f"{parts[0]} {parts[1][0]}." if len(parts) >= 2 else name


def report(target_date: str = None):
    """
    Генерирует и отправляет отчёт.
    target_date: 'YYYY-MM-DD' или None (вчера).
    """
    if target_date is None:
        target_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

    target_dt = datetime.strptime(target_date, "%Y-%m-%d").date()
    display_date = target_dt.strftime("%d.%m.%Y")

    state = load_state()

    # ── Собираем пропущенные за нужную дату ──────────────────
    day_calls = []
    for key, c in state["calls"].items():
        if not c.get("call_time"):
            continue
        try:
            ct = datetime.strptime(c["call_time"][:10], "%Y-%m-%d").date()
        except ValueError:
            continue
        if ct == target_dt:
            day_calls.append(c)

    # Сортируем по времени
    day_calls.sort(key=lambda x: x.get("call_time", ""))

    total = len(day_calls)
    if total == 0:
        send_message(
            GROUP_CHAT_ID,
            f"📊 Пропущенные — {display_date}\n\nПропущенных: 0 ✅\nОтличная работа! 🎉",
        )
        return

    # ── Запрашиваем данные из Bitrix24 ───────────────────────
    bitrix_calls = []
    try:
        bitrix_calls = get_bitrix_calls(target_date)
        print(f"Bitrix24: {len(bitrix_calls)} звонков за {target_date}")
    except Exception as e:
        print(f"Bitrix24 ошибка: {e}")

    # ── Классифицируем каждый пропущенный ────────────────────
    results = []  # список dict с доп. полями: crm_callback, crm_time, client_back, client_time

    for c in day_calls:
        r = {**c, "crm_callback": False, "crm_time": None,
             "client_back": False, "client_time": None}

        mc_phone = normalize_phone(c["phone"])
        mc_time_str = c.get("call_time", "")
        try:
            mc_time = datetime.strptime(mc_time_str, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            results.append(r)
            continue

        # Ищем в Bitrix24 звонки на/от этого номера после пропущенного
        for bc in bitrix_calls:
            bc_phone = normalize_phone(bc.get("PHONE_NUMBER", ""))
            if bc_phone != mc_phone:
                continue

            raw_time = bc.get("CALL_START_DATE", "")
            try:
                bc_time = datetime.strptime(raw_time[:19], "%Y-%m-%dT%H:%M:%S")
            except ValueError:
                try:
                    bc_time = datetime.strptime(raw_time[:19], "%Y-%m-%d %H:%M:%S")
                except ValueError:
                    continue

            if bc_time <= mc_time:
                continue

            call_type = int(bc.get("CALL_TYPE", 0))
            duration = int(bc.get("CALL_DURATION", 0))

            # Исходящий (менеджер перезвонил)
            if call_type == 1 and duration > 0 and not r["crm_callback"]:
                r["crm_callback"] = True
                r["crm_time"] = bc_time.strftime("%Y-%m-%d %H:%M:%S")
                break

            # Входящий (клиент перезвонил сам)
            if call_type in (2, 3) and duration > 0 and not r["client_back"]:
                r["client_back"] = True
                r["client_time"] = bc_time.strftime("%Y-%m-%d %H:%M:%S")

        results.append(r)

    # ── Подсчёты ─────────────────────────────────────────────
    crm = [r for r in results if r["crm_callback"]]
    personal = [r for r in results if r["personal"] and not r["crm_callback"]]
    client_back = [r for r in results
                   if r["client_back"] and not r["crm_callback"] and not r["personal"]]
    not_done = [r for r in results
                if not r["crm_callback"] and not r["personal"] and not r["client_back"]]

    total_mgr = len(crm) + len(personal)

    # Среднее время: менеджер перезвонил
    mgr_d = []
    for r in crm:
        try:
            t1 = datetime.strptime(r["call_time"], "%Y-%m-%d %H:%M:%S")
            t2 = datetime.strptime(r["crm_time"], "%Y-%m-%d %H:%M:%S")
            d = (t2 - t1).total_seconds()
            if d > 0: mgr_d.append(d)
        except (ValueError, TypeError):
            pass
    for r in personal:
        try:
            t1 = datetime.strptime(r["call_time"], "%Y-%m-%d %H:%M:%S")
            t2 = datetime.strptime(r["personal_time"], "%Y-%m-%d %H:%M:%S")
            d = (t2 - t1).total_seconds()
            if d > 0: mgr_d.append(d)
        except (ValueError, TypeError):
            pass
    avg_mgr = _fmt(sum(mgr_d) / len(mgr_d)) if mgr_d else "—"

    # Среднее время: клиент перезвонил сам
    cli_d = []
    for r in client_back:
        try:
            t1 = datetime.strptime(r["call_time"], "%Y-%m-%d %H:%M:%S")
            t2 = datetime.strptime(r["client_time"], "%Y-%m-%d %H:%M:%S")
            d = (t2 - t1).total_seconds()
            if d > 0: cli_d.append(d)
        except (ValueError, TypeError):
            pass
    avg_cli = _fmt(sum(cli_d) / len(cli_d)) if cli_d else "—"

    # По менеджерам
    ms = defaultdict(lambda: {"total": 0, "crm": 0, "personal": 0, "bad": 0})
    for r in results:
        s = ms[r["manager"] or "Не указан"]
        s["total"] += 1
        if r["crm_callback"]:
            s["crm"] += 1
        elif r["personal"]:
            s["personal"] += 1
        elif not r["client_back"]:
            s["bad"] += 1

    # ── Формируем текст ──────────────────────────────────────
    pct = round(total_mgr / total * 100) if total else 0
    L = []

    L.append(f"📊 Пропущенные — {display_date}")
    L.append("")
    L.append(
        f"📞 Всего: {total} | "
        f"Обработано: {total_mgr} ({pct}%) | "
        f"Не обработано: {len(not_done)}"
    )
    L.append(f"⏱ Менеджер перезвонил: {total_mgr} (в среднем за {avg_mgr})")
    L.append(f"⏱ Клиент перезвонил сам: {len(client_back)} (в среднем через {avg_cli})")
    L.append("")

    L.append("👥 По менеджерам:")
    L.append("─" * 30)
    for name, s in sorted(ms.items(), key=lambda x: -x[1]["total"]):
        done = s["crm"] + s["personal"]
        L.append(
            f"👤 {name} — {s['total']} пропущ.\n"
            f"   ✅ {done} ({s['crm']} CRM + {s['personal']} личн.) | ❌ {s['bad']}"
        )
    L.append("")

    if personal:
        L.append(f"📱 С личного ({len(personal)}):")
        L.append("─" * 30)
        for r in personal:
            L.append(f"{r['phone']} | {_short(r['manager'])} | {r['company']}")
        L.append("")

    if not_done:
        L.append(f"❌ Не обработано ({len(not_done)}):")
        L.append("─" * 30)
        for r in not_done:
            L.append(f"{r['phone']} | {_short(r['manager'])} | {r['company']}")

    text = "\n".join(L)
    send_message(REPORT_CHAT_ID, text)
    print(f"Отчёт отправлен ({len(text)} символов).")

    # ── Очистка старых записей (старше 7 дней) ───────────────
    cutoff = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
    old_keys = [
        k for k, v in state["calls"].items()
        if v.get("call_time", "")[:10] < cutoff
    ]
    for k in old_keys:
        del state["calls"][k]
    if old_keys:
        print(f"Очищено {len(old_keys)} старых записей.")
    save_state(state)


# ══════════════════════════════════════════════════════════════
#  BITRIX-REPORT — отчёт целиком из Bitrix24 (для прошлых дат)
# ══════════════════════════════════════════════════════════════

def bitrix_report(target_date: str = None):
    """
    Генерирует отчёт полностью из Bitrix24 API, без данных из чата.
    Полезно для прошлых дат, когда бот ещё не был в группе.
    """
    if target_date is None:
        target_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

    target_dt = datetime.strptime(target_date, "%Y-%m-%d").date()
    display_date = target_dt.strftime("%d.%m.%Y")

    # Забираем все звонки из Bitrix24
    all_calls = []
    try:
        all_calls = get_bitrix_calls(target_date)
        print(f"Bitrix24: {len(all_calls)} звонков за {target_date}")
    except Exception as e:
        print(f"Bitrix24 ошибка: {e}")
        send_message(REPORT_CHAT_ID, f"❌ Ошибка Bitrix24: {e}")
        return

    if not all_calls:
        send_message(
            REPORT_CHAT_ID,
            f"📊 Пропущенные — {display_date}\n\nПропущенных: 0 ✅\nОтличная работа! 🎉",
        )
        return

    # ── Находим пропущенные ─────────────────────────────────────
    # Пропущенный = входящий (type 2,3) где CALL_FAILED_CODE != 200
    # Код 200 = отвечен, всё остальное (304, 603, 486 и т.д.) = пропущен
    print("Загружаем пользователей...")
    users = fetch_all_users()
    
    # Собираем пропущенные
    missed_raw = []
    for c in all_calls:
        call_type = int(c.get("CALL_TYPE", 0))
        failed_code = str(c.get("CALL_FAILED_CODE", ""))
        
        # Входящий звонок
        if call_type not in (2, 3):
            continue
        
        # Пропущенный = код НЕ 200 (200 = успешно отвечен)
        if failed_code == "200":
            continue
            
        phone = normalize_phone(c.get("PHONE_NUMBER", ""))
        raw_time = c.get("CALL_START_DATE", "")
        try:
            call_time = datetime.strptime(raw_time[:19], "%Y-%m-%dT%H:%M:%S")
        except ValueError:
            try:
                call_time = datetime.strptime(raw_time[:19], "%Y-%m-%d %H:%M:%S")
            except ValueError:
                continue
        user_id = str(c.get("PORTAL_USER_ID", ""))
        missed_raw.append({
            "phone": phone,
            "call_time": call_time.strftime("%Y-%m-%d %H:%M:%S"),
            "user_id": user_id,
        })

    if not missed_raw:
        send_message(
            REPORT_CHAT_ID,
            f"📊 Пропущенные — {display_date}\n\nПропущенных: 0 ✅\nОтличная работа! 🎉",
        )
        return

    # Загружаем компании пакетно
    print("Загружаем компании...")
    phone_companies = fetch_companies_for_calls(missed_raw, all_calls)

    # Собираем итоговый список
    missed = []
    for m in missed_raw:
        missed.append({
            "phone": m["phone"],
            "call_time": m["call_time"],
            "manager": users.get(m["user_id"], "Не указан"),
            "company": phone_companies.get(m["phone"], ""),
            "crm_callback": False,
            "crm_time": None,
            "client_back": False,
            "client_time": None,
        })

    print(f"Пропущенных: {len(missed)}")

    # ── Ищем перезвоны ───────────────────────────────────────
    for mc in missed:
        mc_phone = mc["phone"]
        mc_time = datetime.strptime(mc["call_time"], "%Y-%m-%d %H:%M:%S")

        for bc in all_calls:
            bc_phone = normalize_phone(bc.get("PHONE_NUMBER", ""))
            if bc_phone != mc_phone:
                continue

            raw_time = bc.get("CALL_START_DATE", "")
            try:
                bc_time = datetime.strptime(raw_time[:19], "%Y-%m-%dT%H:%M:%S")
            except ValueError:
                try:
                    bc_time = datetime.strptime(raw_time[:19], "%Y-%m-%d %H:%M:%S")
                except ValueError:
                    continue

            if bc_time <= mc_time:
                continue

            call_type = int(bc.get("CALL_TYPE", 0))
            duration = int(bc.get("CALL_DURATION", 0))

            # Исходящий (менеджер перезвонил)
            if call_type == 1 and duration > 0 and not mc["crm_callback"]:
                mc["crm_callback"] = True
                mc["crm_time"] = bc_time.strftime("%Y-%m-%d %H:%M:%S")
                break

            # Входящий (клиент перезвонил сам)
            if call_type in (2, 3) and duration > 0 and not mc["client_back"]:
                mc["client_back"] = True
                mc["client_time"] = bc_time.strftime("%Y-%m-%d %H:%M:%S")

    # ── Подсчёты ─────────────────────────────────────────────
    # Обработано = менеджер перезвонил (CRM) ИЛИ клиент перезвонил сам и менеджер поднял
    # Не обработано = никто не перезвонил
    total = len(missed)
    crm = [r for r in missed if r["crm_callback"]]
    client_back = [r for r in missed if r["client_back"] and not r["crm_callback"]]
    not_done = [r for r in missed if not r["crm_callback"] and not r["client_back"]]
    total_processed = len(crm) + len(client_back)

    # Среднее время: менеджер
    mgr_d = []
    for r in crm:
        try:
            t1 = datetime.strptime(r["call_time"], "%Y-%m-%d %H:%M:%S")
            t2 = datetime.strptime(r["crm_time"], "%Y-%m-%d %H:%M:%S")
            d = (t2 - t1).total_seconds()
            if d > 0: mgr_d.append(d)
        except (ValueError, TypeError):
            pass
    avg_mgr = _fmt(sum(mgr_d) / len(mgr_d)) if mgr_d else "—"

    # Среднее время: клиент
    cli_d = []
    for r in client_back:
        try:
            t1 = datetime.strptime(r["call_time"], "%Y-%m-%d %H:%M:%S")
            t2 = datetime.strptime(r["client_time"], "%Y-%m-%d %H:%M:%S")
            d = (t2 - t1).total_seconds()
            if d > 0: cli_d.append(d)
        except (ValueError, TypeError):
            pass
    avg_cli = _fmt(sum(cli_d) / len(cli_d)) if cli_d else "—"

    # По менеджерам
    ms = defaultdict(lambda: {"total": 0, "crm": 0, "bad": 0})
    for r in missed:
        s = ms[r["manager"] or "Не указан"]
        s["total"] += 1
        if r["crm_callback"]:
            s["crm"] += 1
        elif not r["client_back"]:
            s["bad"] += 1

    # ── Текст ────────────────────────────────────────────────
    pct = round(total_processed / total * 100) if total else 0
    L = []
    L.append(f"📊 Пропущенные — {display_date} (из Bitrix24)")
    L.append("")
    L.append(f"📞 Всего: {total} | Обработано: {total_processed} ({pct}%) | Не обработано: {len(not_done)}")
    L.append(f"⏱ Менеджер перезвонил: {len(crm)} (в среднем за {avg_mgr})")
    L.append(f"⏱ Клиент перезвонил сам: {len(client_back)} (в среднем через {avg_cli})")
    L.append("")

    L.append("👥 По менеджерам:")
    L.append("─" * 30)
    for name, s in sorted(ms.items(), key=lambda x: -x[1]["total"]):
        L.append(
            f"👤 {name} — {s['total']} пропущ.\n"
            f"   ✅ {s['crm']} обработ. (CRM) | ❌ {s['bad']}"
        )
    L.append("")

    L.append("")

    if client_back:
        L.append(f"📞 Клиент перезвонил сам ({len(client_back)}):")
        L.append("─" * 30)
        for r in client_back:
            L.append(f"{r['phone']} | {_short(r['manager'])} | {r['company']}")
        L.append("")

    if not_done:
        L.append(f"❌ Не обработано ({len(not_done)}):")
        L.append("─" * 30)
        for r in not_done:
            L.append(f"{r['phone']} | {_short(r['manager'])} | {r['company']}")

    text = "\n".join(L)
    send_message(REPORT_CHAT_ID, text)
    print(f"Bitrix-отчёт отправлен ({len(text)} символов).")


# ══════════════════════════════════════════════════════════════
#  ПАКЕТНЫЕ ЗАПРОСЫ К BITRIX24 (оптимизация)
# ══════════════════════════════════════════════════════════════

def fetch_all_users() -> dict:
    """Загружает всех пользователей за один запрос. Возвращает {id: 'Имя Фамилия'}."""
    users = {}
    result = bitrix_all("user.get", {})
    for u in result:
        uid = str(u.get("ID", ""))
        name = f"{u.get('NAME', '')} {u.get('LAST_NAME', '')}".strip()
        users[uid] = name or "Не указан"
    print(f"  Загружено пользователей: {len(users)}")
    return users


def fetch_companies_for_calls(missed_calls: list, all_calls: list) -> dict:
    """
    Находит компании/контакты для пропущенных номеров.
    Использует CRM_ENTITY_TYPE/CRM_ENTITY_ID из данных звонков.
    """
    phone_to_company = {}

    # Собираем уникальные номера из пропущенных
    missed_phones = set(m.get("phone", "") for m in missed_calls if m.get("phone"))

    # Собираем CRM-привязки только для пропущенных номеров
    phone_entities = {}
    for c in all_calls:
        phone = normalize_phone(c.get("PHONE_NUMBER", ""))
        if phone not in missed_phones or phone in phone_entities:
            continue
        entity_type = c.get("CRM_ENTITY_TYPE", "")
        entity_id = c.get("CRM_ENTITY_ID", "")
        if entity_type and entity_id:
            phone_entities[phone] = (entity_type, entity_id)

    print(f"  CRM-привязок для пропущенных: {len(phone_entities)}")

    # Собираем уникальные ID
    contact_ids = set()
    lead_ids = set()
    for phone, (etype, eid) in phone_entities.items():
        if etype == "CONTACT":
            contact_ids.add(eid)
        elif etype == "LEAD":
            lead_ids.add(eid)

    # Пакетно загружаем контакты
    contacts = {}
    if contact_ids:
        for cid in contact_ids:
            c = bitrix("crm.contact.get", {"ID": cid})
            if c and c.get("result"):
                contacts[str(cid)] = c["result"]
        print(f"  Загружено контактов: {len(contacts)}")

    # Пакетно загружаем лиды
    leads = {}
    if lead_ids:
        for lid in lead_ids:
            l = bitrix("crm.lead.get", {"ID": lid})
            if l and l.get("result"):
                leads[str(lid)] = l["result"]
        print(f"  Загружено лидов: {len(leads)}")

    # Собираем уникальные company_id из контактов
    company_ids = set()
    for c in contacts.values():
        cid = c.get("COMPANY_ID")
        if cid:
            company_ids.add(str(cid))

    # Пакетно загружаем компании
    companies = {}
    if company_ids:
        for comp_id in company_ids:
            comp = bitrix("crm.company.get", {"ID": comp_id})
            if comp and comp.get("result"):
                companies[str(comp_id)] = comp["result"].get("TITLE", "")
        print(f"  Загружено компаний: {len(companies)}")

    # Собираем результат: phone -> название
    for phone, (etype, eid) in phone_entities.items():
        if etype == "CONTACT" and eid in contacts:
            c = contacts[eid]
            company_id = str(c.get("COMPANY_ID", ""))
            if company_id in companies:
                phone_to_company[phone] = companies[company_id]
            else:
                name = f"{c.get('NAME', '')} {c.get('LAST_NAME', '')}".strip()
                phone_to_company[phone] = name
        elif etype == "LEAD" and eid in leads:
            l = leads[eid]
            company = l.get("COMPANY_TITLE", "")
            if company:
                phone_to_company[phone] = company
            else:
                name = f"{l.get('NAME', '')} {l.get('LAST_NAME', '')}".strip()
                phone_to_company[phone] = name

    return phone_to_company


# ══════════════════════════════════════════════════════════════
#  ТОЧКА ВХОДА
# ══════════════════════════════════════════════════════════════

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Использование: python bot.py poll|report|bitrix-report YYYY-MM-DD")
        sys.exit(1)

    mode = sys.argv[1]

    if mode == "poll":
        poll()
    elif mode == "report":
        date_arg = sys.argv[2] if len(sys.argv) > 2 else None
        if date_arg and "." in date_arg:
            parts = date_arg.split(".")
            date_arg = f"{parts[2]}-{parts[1]}-{parts[0]}"
        report(date_arg)
    elif mode == "bitrix-report":
        date_arg = sys.argv[2] if len(sys.argv) > 2 else None
        if date_arg and "." in date_arg:
            parts = date_arg.split(".")
            date_arg = f"{parts[2]}-{parts[1]}-{parts[0]}"
        bitrix_report(date_arg)
    else:
        print(f"Неизвестный режим: {mode}")
        sys.exit(1)
