import os
import time
import json
import requests
import threading
import schedule
from datetime import datetime, timedelta
import telebot
import anthropic

# ============================================================
# CONFIGURATION
# ============================================================
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "8704107268:AAHa428Al9B1zxldaVVwbninGH4Skt1FBdE")
MY_CHAT_ID = int(os.environ.get("MY_CHAT_ID", "320613087"))
META_AD_ACCOUNT = os.environ.get("META_AD_ACCOUNT", "act_1004160296398671")
META_ACCESS_TOKEN = os.environ.get("META_ACCESS_TOKEN", "EAAWjRhvFnNoBQ9hlLu1idGbeZCa377ykh87Qxin6k6v1N6ZBHRQXVvnzVzJZB6RV06eQ6TGZC4ahIaJHdbxdO6Yl7yoMh63PmtrQZC8BZBP9ZCvwPTYozdXw0m6eU6zmAJEYvWEP0d22BSZBRjrfr2rhgAxPYnng6h19ZBgT8RPBDAgDz6ZBNjqgRVlH8BLAdQ")
ANTHROPIC_KEY = os.environ.get("ANTHROPIC_KEY", "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")

ISRAEL_UTC_OFFSET = 2  # UTC+2 winter, change to 3 for summer

bot = telebot.TeleBot(TELEGRAM_TOKEN)
claude = anthropic.Anthropic(api_key=ANTHROPIC_KEY)

# ============================================================
# HELPERS
# ============================================================
def get_israel_now():
    return datetime.utcnow() + timedelta(hours=ISRAEL_UTC_OFFSET)

def get_date_range(period):
    now = get_israel_now()
    today = now.date()
    if period == "today":
        return str(today), str(today)
    elif period == "yesterday":
        yest = today - timedelta(days=1)
        return str(yest), str(yest)
    elif period == "week":
        return str(today - timedelta(days=7)), str(today)
    elif period == "month":
        return str(today - timedelta(days=30)), str(today)
    else:
        return str(today), str(today)

# ============================================================
# META ADS API (with pagination)
# ============================================================
def get_all_campaigns(fields="name,status,effective_status"):
    all_campaigns = []
    url = f"https://graph.facebook.com/v21.0/{META_AD_ACCOUNT}/campaigns"
    params = {
        "fields": fields,
        "limit": 500,
        "access_token": META_ACCESS_TOKEN,
    }
    while url:
        resp = requests.get(url, params=params)
        data = resp.json()
        if "data" in data:
            all_campaigns.extend(data["data"])
        url = data.get("paging", {}).get("next", None)
        params = {}
    return all_campaigns

def get_account_insights(since, until):
    """Get ALL campaign insights for the period (no status filter)."""
    url = f"https://graph.facebook.com/v21.0/{META_AD_ACCOUNT}/insights"
    params = {
        "fields": "campaign_name,spend,impressions,clicks,ctr,actions,cost_per_action_type",
        "time_range": json.dumps({"since": since, "until": until}),
        "level": "campaign",
        "limit": 500,
        "access_token": META_ACCESS_TOKEN,
    }

    all_insights = []
    while True:
        resp = requests.get(url, params=params)
        data = resp.json()
        if "data" in data:
            all_insights.extend(data["data"])
        next_url = data.get("paging", {}).get("next", None)
        if next_url:
            url = next_url
            params = {}
        else:
            break
    return all_insights

def extract_leads(insight):
    for action in insight.get("actions", []):
        if action.get("action_type") in ("lead", "onsite_conversion.lead_grouped"):
            return int(action.get("value", 0))
    return 0

def extract_cpl(insight):
    for cost in insight.get("cost_per_action_type", []):
        if cost.get("action_type") in ("lead", "onsite_conversion.lead_grouped"):
            return float(cost.get("value", 0))
    return 0.0

def enrich_insights(insights):
    """Convert raw insights to clean list, only campaigns with spend > 0."""
    enriched = []
    for ins in insights:
        spend = float(ins.get("spend", 0))
        if spend == 0:
            continue
        enriched.append({
            "campaign_name": ins.get("campaign_name", "—"),
            "spend": round(spend, 2),
            "ctr": round(float(ins.get("ctr", 0)), 2),
            "leads": extract_leads(ins),
            "cpl": round(extract_cpl(ins), 2),
            "impressions": int(ins.get("impressions", 0)),
            "clicks": int(ins.get("clicks", 0)),
        })
    # Sort by spend descending
    enriched.sort(key=lambda x: x["spend"], reverse=True)
    return enriched

# ============================================================
# INTENT DETECTION via Claude
# ============================================================
INTENT_PROMPT = """Ты — парсер запросов. Определи из текста: период и тип запроса.

Ответь ТОЛЬКО валидным JSON без markdown:
{"period": "today", "show": "spend"}

Варианты period: today, yesterday, week, month
Варианты show:
- "spend" — кампании с расходом за период (по умолчанию)
- "all_campaigns" — список всех кампаний в кабинете (с их статусами)

Правила:
- "как дела", "статус", "сводка", "отчёт", "что крутится" → period=today, show=spend
- "вчера" → period=yesterday
- "неделя", "за неделю" → period=week
- "месяц", "за месяц" → period=month
- "все кампании", "сколько кампаний", "список кампаний" → show=all_campaigns
- Если период не указан → today
- Если непонятно → period=today, show=spend"""

def detect_intent(user_text):
    try:
        response = claude.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=100,
            system=INTENT_PROMPT,
            messages=[{"role": "user", "content": user_text}]
        )
        raw = response.content[0].text.strip()
        raw = raw.replace("```json", "").replace("```", "").strip()
        return json.loads(raw)
    except Exception as e:
        print(f"Intent detection error: {e}")
        return {"period": "today", "show": "spend"}

# ============================================================
# FETCH DATA
# ============================================================
def fetch_spend_data(period):
    """Fetch campaigns that had spend in the given period."""
    since, until = get_date_range(period)
    insights = get_account_insights(since, until)
    campaigns = enrich_insights(insights)

    # Totals
    total_spend = sum(c["spend"] for c in campaigns)
    total_leads = sum(c["leads"] for c in campaigns)
    avg_cpl = round(total_spend / total_leads, 2) if total_leads > 0 else 0

    return {
        "period": period,
        "since": since,
        "until": until,
        "campaigns": campaigns,
        "total_spend": round(total_spend, 2),
        "total_leads": total_leads,
        "avg_cpl": avg_cpl,
    }

def fetch_all_campaigns_list():
    """Fetch full list of campaigns with statuses."""
    camps = get_all_campaigns()
    active = [c.get("name", "—") for c in camps if c.get("effective_status") == "ACTIVE"]
    paused = len([c for c in camps if c.get("effective_status") == "PAUSED"])
    return {
        "total": len(camps),
        "active_names": active,
        "active_count": len(active),
        "paused_count": paused,
    }

# ============================================================
# GENERATE RESPONSE via Claude
# ============================================================
RESPONSE_PROMPT = """Ты — ассистент по рекламе Meta Ads для салона iStudio Beauty Centre (Ришон ле-Цион).

ПРАВИЛА:
1. Отвечай ТОЛЬКО на основе данных в JSON. НЕ придумывай.
2. Если campaigns пустой — скажи "за этот период расхода не было".
3. НЕ используй Markdown таблицы. Используй эмодзи.
4. КРАТКО: 2-5 строк для простых вопросов, список + итог для отчётов.
5. НЕ задавай вопросов в конце.

Формат:
🟢 Название — 💰 $XX | 👤 X лидов | CTR X.X% | CPL $X.XX   (есть лиды)
🔴 Название — 💰 $XX | 👤 0 лидов | CTR X.X%               (нет лидов)

Итог:
💵 Итого: $XXX | 🎯 Лидов: XX | 📉 CPL: $X.XX

Ориентиры лучших CPL: B-Flexy $3.67, КП+РФ $4.77, Карбон 25 ИВР $5.09"""

def generate_response(user_text, data):
    try:
        # If it's a campaign list request
        if "active_names" in data:
            text = f"📋 Всего кампаний: {data['total']}\n"
            text += f"🟢 Активных: {data['active_count']} | 🔴 На паузе: {data['paused_count']}\n\n"
            if data["active_names"]:
                text += "🟢 Активные:\n"
                for name in data["active_names"]:
                    text += f"  • {name}\n"
            else:
                text += "Сейчас нет активных кампаний."
            return text

        # Spend data request
        campaigns = data.get("campaigns", [])
        period_names = {"today": "сегодня", "yesterday": "вчера", "week": "неделю", "month": "месяц"}
        p_name = period_names.get(data.get("period", "today"), data.get("period", ""))

        if not campaigns:
            return f"📊 За {p_name} расхода не было — ни одна кампания не тратила."

        response = claude.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=1500,
            system=RESPONSE_PROMPT,
            messages=[{
                "role": "user",
                "content": f"Данные:\n{json.dumps(data, ensure_ascii=False)}\n\nЗапрос: {user_text}"
            }]
        )
        return response.content[0].text
    except Exception as e:
        return f"Ошибка: {e}"

# ============================================================
# SIMPLE FORMAT (for commands, no Claude needed)
# ============================================================
def format_report(data):
    """Format spend data without Claude — fast, deterministic."""
    campaigns = data.get("campaigns", [])
    period_names = {"today": "сегодня", "yesterday": "вчера", "week": "неделю", "month": "месяц"}
    p_name = period_names.get(data["period"], data["period"])
    since = data["since"]
    until = data["until"]

    if not campaigns:
        return f"📊 За {p_name} ({since}) расхода не было."

    report = f"📊 Сводка за {p_name} ({since}"
    if since != until:
        report += f" — {until}"
    report += ")\n"
    report += f"{'─' * 28}\n\n"

    for c in campaigns:
        if c["leads"] > 0:
            report += f"🟢 {c['campaign_name']}\n"
            report += f"   💰 ${c['spend']:.2f} | 👤 {c['leads']} лидов | CTR {c['ctr']:.2f}% | CPL ${c['cpl']:.2f}\n\n"
        else:
            report += f"🔴 {c['campaign_name']}\n"
            report += f"   💰 ${c['spend']:.2f} | 👤 0 лидов | CTR {c['ctr']:.2f}%\n\n"

    report += f"{'─' * 28}\n"
    report += f"💵 Итого: ${data['total_spend']:.2f} | 🎯 Лидов: {data['total_leads']}"
    if data["total_leads"] > 0:
        report += f" | 📉 CPL: ${data['avg_cpl']:.2f}"
    report += "\n"

    return report

# ============================================================
# MORNING AUTO-REPORT
# ============================================================
def send_morning_report():
    data = fetch_spend_data("yesterday")
    now = get_israel_now()

    report = f"🌅 Доброе утро!\n\n"
    report += f"📊 Сводка Meta Ads — Вчера ({data['since']})\n"
    report += f"{'─' * 28}\n\n"

    if not data["campaigns"]:
        report += "Вчера расхода не было.\n"
    else:
        for c in data["campaigns"]:
            if c["leads"] > 0:
                report += f"🟢 {c['campaign_name']}\n"
                report += f"   💰 ${c['spend']:.2f} | 👤 {c['leads']} лидов | CTR {c['ctr']:.2f}% | CPL ${c['cpl']:.2f}\n\n"
            else:
                report += f"🔴 {c['campaign_name']}\n"
                report += f"   💰 ${c['spend']:.2f} | 👤 0 лидов | CTR {c['ctr']:.2f}%\n\n"

        report += f"{'─' * 28}\n"
        report += f"💵 Итого: ${data['total_spend']:.2f} | 🎯 Лидов: {data['total_leads']}"
        if data["total_leads"] > 0:
            report += f" | 📉 CPL: ${data['avg_cpl']:.2f}"
        report += "\n"

    report += f"\n/week — за неделю | /month — за месяц"

    try:
        bot.send_message(MY_CHAT_ID, report)
    except Exception as e:
        print(f"Morning report error: {e}")

# ============================================================
# TELEGRAM HANDLERS
# ============================================================
@bot.message_handler(commands=["start"])
def cmd_start(message):
    if message.chat.id != MY_CHAT_ID:
        return
    bot.send_message(MY_CHAT_ID,
        "👋 Привет! Я твой Meta Ads аналитик.\n\n"
        "Просто спрашивай:\n"
        "• «Как дела?» — сводка за сегодня\n"
        "• «Что вчера?» — за вчера\n"
        "• «За неделю» / «За месяц»\n"
        "• «Все кампании» — полный список\n\n"
        "Команды: /today /yesterday /week /month /campaigns /alerts"
    )

@bot.message_handler(commands=["today"])
def cmd_today(message):
    if message.chat.id != MY_CHAT_ID:
        return
    bot.send_message(MY_CHAT_ID, "⏳ Собираю...")
    data = fetch_spend_data("today")
    bot.send_message(MY_CHAT_ID, format_report(data))

@bot.message_handler(commands=["yesterday"])
def cmd_yesterday(message):
    if message.chat.id != MY_CHAT_ID:
        return
    bot.send_message(MY_CHAT_ID, "⏳ Собираю...")
    data = fetch_spend_data("yesterday")
    bot.send_message(MY_CHAT_ID, format_report(data))

@bot.message_handler(commands=["week"])
def cmd_week(message):
    if message.chat.id != MY_CHAT_ID:
        return
    bot.send_message(MY_CHAT_ID, "⏳ Собираю...")
    data = fetch_spend_data("week")
    bot.send_message(MY_CHAT_ID, format_report(data))

@bot.message_handler(commands=["month"])
def cmd_month(message):
    if message.chat.id != MY_CHAT_ID:
        return
    bot.send_message(MY_CHAT_ID, "⏳ Собираю...")
    data = fetch_spend_data("month")
    bot.send_message(MY_CHAT_ID, format_report(data))

@bot.message_handler(commands=["campaigns"])
def cmd_campaigns(message):
    if message.chat.id != MY_CHAT_ID:
        return
    bot.send_message(MY_CHAT_ID, "⏳ Загружаю...")
    data = fetch_all_campaigns_list()
    bot.send_message(MY_CHAT_ID, generate_response("список всех кампаний", data))

@bot.message_handler(commands=["alerts"])
def cmd_alerts(message):
    if message.chat.id != MY_CHAT_ID:
        return
    data = fetch_spend_data("today")
    alerts = []
    for c in data["campaigns"]:
        if c["spend"] > 30 and c["leads"] == 0:
            alerts.append(f"🚨 {c['campaign_name']}: ${c['spend']:.2f}, 0 лидов!")
        if c["ctr"] < 1.0 and c["spend"] > 10:
            alerts.append(f"⚠️ {c['campaign_name']}: CTR {c['ctr']:.2f}%")
    bot.send_message(MY_CHAT_ID, "🔔 Алерты:\n\n" + "\n".join(alerts) if alerts else "✅ Алертов нет.")

@bot.message_handler(commands=["report"])
def cmd_report(message):
    if message.chat.id != MY_CHAT_ID:
        return
    send_morning_report()

# ============================================================
# FREE-TEXT → Intent → Data → Response
# ============================================================
@bot.message_handler(func=lambda m: m.chat.id == MY_CHAT_ID)
def handle_text(message):
    user_text = message.text.strip()
    bot.send_message(MY_CHAT_ID, "🤔 Думаю...")

    # Step 1: Parse intent
    intent = detect_intent(user_text)
    print(f"Intent: {intent}")

    # Step 2: Fetch data
    show = intent.get("show", "spend")
    if show == "all_campaigns":
        data = fetch_all_campaigns_list()
    else:
        period = intent.get("period", "today")
        data = fetch_spend_data(period)

    # Step 3: Generate response
    resp = generate_response(user_text, data)
    bot.send_message(MY_CHAT_ID, resp)

# ============================================================
# SCHEDULER
# ============================================================
def run_scheduler():
    utc_hour = 8 - ISRAEL_UTC_OFFSET
    schedule.every().day.at(f"{utc_hour:02d}:00").do(send_morning_report)
    while True:
        schedule.run_pending()
        time.sleep(30)

# ============================================================
# MAIN
# ============================================================
if __name__ == "__main__":
    print("🚀 Bot starting...")
    print(f"📅 Israel time: {get_israel_now().strftime('%Y-%m-%d %H:%M')}")

    scheduler_thread = threading.Thread(target=run_scheduler, daemon=True)
    scheduler_thread.start()
    print("⏰ Morning report scheduled at 08:00 Israel time")

    print("📱 Telegram bot polling started...")
    bot.infinity_polling()
