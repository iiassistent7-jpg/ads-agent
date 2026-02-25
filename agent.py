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
    elif period == "7days":
        return str(today - timedelta(days=7)), str(today)
    elif period == "14days":
        return str(today - timedelta(days=14)), str(today)
    elif period == "90days":
        return str(today - timedelta(days=90)), str(today)
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

def get_account_insights(since, until, filtering=None):
    url = f"https://graph.facebook.com/v21.0/{META_AD_ACCOUNT}/insights"
    params = {
        "fields": "campaign_name,spend,impressions,clicks,ctr,actions,cost_per_action_type",
        "time_range": json.dumps({"since": since, "until": until}),
        "level": "campaign",
        "limit": 500,
        "access_token": META_ACCESS_TOKEN,
    }
    if filtering:
        params["filtering"] = json.dumps(filtering)

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

# ============================================================
# INTENT DETECTION via Claude
# ============================================================
INTENT_PROMPT = """Ты — парсер запросов. Определи из текста пользователя: период и фильтр кампаний.

Ответь ТОЛЬКО валидным JSON без markdown:
{"period": "today", "filter": "active"}

Варианты period: today, yesterday, week, month
Варианты filter: active, paused, all

Правила:
- "сегодня", "как дела", "статус", "работаешь" → period=today
- "вчера", "за вчера" → period=yesterday
- "неделя", "за неделю", "7 дней" → period=week
- "месяц", "за месяц", "30 дней" → period=month
- "на паузе", "неактивные", "выключенные", "paused" → filter=paused
- "все кампании", "все компании", "полный", "сколько всего" → filter=all
- "активные", "работающие" → filter=active
- Если период не указан → today
- Если фильтр не указан → active"""

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
        return {"period": "today", "filter": "active"}

# ============================================================
# FETCH DATA BASED ON INTENT
# ============================================================
def fetch_data_for_intent(intent):
    period = intent.get("period", "today")
    filt = intent.get("filter", "active")
    since, until = get_date_range(period)

    if filt == "active":
        filtering = [{"field": "campaign.effective_status", "operator": "IN", "value": ["ACTIVE"]}]
    elif filt == "paused":
        filtering = [{"field": "campaign.effective_status", "operator": "IN", "value": ["PAUSED"]}]
    else:
        filtering = None

    insights = get_account_insights(since, until, filtering=filtering)

    enriched = []
    for ins in insights:
        spend = float(ins.get("spend", 0))
        if spend == 0:
            continue
        enriched.append({
            "campaign_name": ins.get("campaign_name", "—"),
            "spend": spend,
            "ctr": round(float(ins.get("ctr", 0)), 2),
            "leads": extract_leads(ins),
            "cpl": round(extract_cpl(ins), 2),
            "impressions": int(ins.get("impressions", 0)),
            "clicks": int(ins.get("clicks", 0)),
        })

    all_camps = get_all_campaigns()
    active_count = sum(1 for c in all_camps if c.get("effective_status") == "ACTIVE")
    paused_count = sum(1 for c in all_camps if c.get("effective_status") == "PAUSED")

    return {
        "period": period,
        "since": since,
        "until": until,
        "filter": filt,
        "campaigns": enriched,
        "total_in_account": len(all_camps),
        "active_count": active_count,
        "paused_count": paused_count,
    }

# ============================================================
# GENERATE RESPONSE via Claude
# ============================================================
RESPONSE_PROMPT = """Ты — ассистент по рекламе Meta Ads для салона красоты iStudio Beauty Centre (Ришон ле-Цион).

СТРОГИЕ ПРАВИЛА:
1. Отвечай ТОЛЬКО на основе данных в JSON. НИКОГДА не придумывай.
2. Если список campaigns пустой — скажи "нет данных" и укажи кол-во активных/на паузе. Всё.
3. НЕ используй Markdown таблицы. Формат — эмодзи.
4. КРАТКО. Простой вопрос = 2-5 строк. Отчёт = список + итог.
5. НЕ задавай вопросов в конце.
6. Показывай ТОЛЬКО кампании из данных.

Формат:
🟢 Название — 💰 $XX | 👤 X лидов | CTR X.X% | CPL $X.XX
🔴 Название — 💰 $XX | 👤 0 лидов | CTR X.X%

Итог:
💵 Итого: $XXX | 🎯 Лидов: XX | 📉 CPL: $X.XX

Ориентиры CPL: B-Flexy $3.67, КП+РФ $4.77, Карбон 25 ИВР $5.09"""

def generate_response(user_text, data):
    try:
        campaigns = data.get("campaigns", [])
        period_names = {"today": "сегодня", "yesterday": "вчера", "week": "неделю", "month": "месяц"}
        filter_names = {"active": "активным", "paused": "паузированным", "all": "всем"}
        p_name = period_names.get(data["period"], data["period"])
        f_name = filter_names.get(data["filter"], "")

        if not campaigns:
            return (
                f"📊 Нет данных по {f_name} кампаниям за {p_name}.\n\n"
                f"🟢 Активных: {data['active_count']} | 🔴 На паузе: {data['paused_count']}"
            )

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
# MORNING AUTO-REPORT (8:00 Israel)
# ============================================================
def send_morning_report():
    since, until = get_date_range("yesterday")
    filtering = [{"field": "campaign.effective_status", "operator": "IN", "value": ["ACTIVE"]}]
    insights = get_account_insights(since, until, filtering=filtering)

    now = get_israel_now()
    report = f"🌅 Доброе утро!\n\n📊 Сводка Meta Ads — Вчера\n{now.strftime('%d.%m.%Y %H:%M')}\n\n"

    total_spend = 0.0
    total_leads = 0
    has_data = False

    for ins in insights:
        spend = float(ins.get("spend", 0))
        if spend == 0:
            continue
        has_data = True
        name = ins.get("campaign_name", "—")
        ctr = float(ins.get("ctr", 0))
        leads = extract_leads(ins)
        cpl = extract_cpl(ins)
        total_spend += spend
        total_leads += leads

        if leads > 0:
            report += f"🟢 {name}\n   💰 ${spend:.2f} | 👤 {leads} лидов | CTR {ctr:.2f}% | CPL ${cpl:.2f}\n\n"
        else:
            report += f"🔴 {name}\n   💰 ${spend:.2f} | 👤 0 лидов | CTR {ctr:.2f}%\n\n"

    if not has_data:
        report += "Нет активных кампаний с расходом за вчера.\n"
    else:
        report += f"{'─' * 28}\n"
        report += f"💵 Итого: ${total_spend:.2f} | 🎯 Лидов: {total_leads}\n"
        if total_leads > 0:
            report += f"📉 Средний CPL: ${total_spend / total_leads:.2f}\n"

    all_camps = get_all_campaigns()
    active = sum(1 for c in all_camps if c.get("effective_status") == "ACTIVE")
    paused = sum(1 for c in all_camps if c.get("effective_status") == "PAUSED")
    report += f"\n🟢 Активных: {active} | 🔴 На паузе: {paused}\n"
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
        "• «Как дела сегодня?»\n"
        "• «Что было вчера?»\n"
        "• «Отчёт за неделю»\n"
        "• «За месяц»\n"
        "• «Что на паузе?»\n"
        "• «Все кампании за месяц»\n\n"
        "Команды: /today /yesterday /week /month /campaigns /alerts"
    )

@bot.message_handler(commands=["today"])
def cmd_today(message):
    if message.chat.id != MY_CHAT_ID:
        return
    bot.send_message(MY_CHAT_ID, "⏳ Собираю...")
    data = fetch_data_for_intent({"period": "today", "filter": "active"})
    bot.send_message(MY_CHAT_ID, generate_response("сводка за сегодня", data))

@bot.message_handler(commands=["yesterday"])
def cmd_yesterday(message):
    if message.chat.id != MY_CHAT_ID:
        return
    bot.send_message(MY_CHAT_ID, "⏳ Собираю...")
    data = fetch_data_for_intent({"period": "yesterday", "filter": "active"})
    bot.send_message(MY_CHAT_ID, generate_response("сводка за вчера", data))

@bot.message_handler(commands=["week"])
def cmd_week(message):
    if message.chat.id != MY_CHAT_ID:
        return
    bot.send_message(MY_CHAT_ID, "⏳ Собираю...")
    data = fetch_data_for_intent({"period": "week", "filter": "active"})
    bot.send_message(MY_CHAT_ID, generate_response("сводка за неделю", data))

@bot.message_handler(commands=["month"])
def cmd_month(message):
    if message.chat.id != MY_CHAT_ID:
        return
    bot.send_message(MY_CHAT_ID, "⏳ Собираю...")
    data = fetch_data_for_intent({"period": "month", "filter": "active"})
    bot.send_message(MY_CHAT_ID, generate_response("сводка за месяц", data))

@bot.message_handler(commands=["campaigns"])
def cmd_campaigns(message):
    if message.chat.id != MY_CHAT_ID:
        return
    bot.send_message(MY_CHAT_ID, "⏳ Загружаю...")
    camps = get_all_campaigns()
    active = [c for c in camps if c.get("effective_status") == "ACTIVE"]
    paused = [c for c in camps if c.get("effective_status") == "PAUSED"]

    text = f"📋 Всего: {len(camps)}\n🟢 Активных: {len(active)} | 🔴 На паузе: {len(paused)}\n\n"
    if active:
        text += "🟢 Активные:\n"
        for c in active:
            text += f"  • {c.get('name', '—')}\n"
    else:
        text += "Нет активных кампаний."
    bot.send_message(MY_CHAT_ID, text)

@bot.message_handler(commands=["alerts"])
def cmd_alerts(message):
    if message.chat.id != MY_CHAT_ID:
        return
    since, until = get_date_range("today")
    filtering = [{"field": "campaign.effective_status", "operator": "IN", "value": ["ACTIVE"]}]
    insights = get_account_insights(since, until, filtering=filtering)

    alerts = []
    for ins in insights:
        spend = float(ins.get("spend", 0))
        leads = extract_leads(ins)
        name = ins.get("campaign_name", "—")
        ctr = float(ins.get("ctr", 0))
        if spend > 30 and leads == 0:
            alerts.append(f"🚨 {name}: ${spend:.2f}, 0 лидов!")
        if ctr < 1.0 and spend > 10:
            alerts.append(f"⚠️ {name}: CTR {ctr:.2f}%")

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
    data = fetch_data_for_intent(intent)

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
