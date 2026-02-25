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

ISRAEL_UTC_OFFSET = 2

bot = telebot.TeleBot(TELEGRAM_TOKEN)
claude = anthropic.Anthropic(api_key=ANTHROPIC_KEY)

# ============================================================
# CLAUDE API WITH RETRY
# ============================================================
def call_claude(system_prompt, user_content, max_tokens=2000, retries=3):
    """Call Claude with retry. Returns None on failure (for fallback)."""
    for attempt in range(retries):
        try:
            response = claude.messages.create(
                model="claude-sonnet-4-20250514",
                max_tokens=max_tokens,
                system=system_prompt,
                messages=[{"role": "user", "content": user_content}]
            )
            return response.content[0].text
        except anthropic.APIStatusError as e:
            if e.status_code == 529 and attempt < retries - 1:
                wait = (attempt + 1) * 10
                print(f"Claude overloaded, retry in {wait}s ({attempt+1}/{retries})")
                time.sleep(wait)
                continue
            else:
                print(f"Claude error: {e.status_code} - {e.message}")
                return None
        except Exception as e:
            print(f"Claude exception: {e}")
            return None

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
        return str(today - timedelta(days=1)), str(today - timedelta(days=1))
    elif period == "week":
        return str(today - timedelta(days=6)), str(today)
    elif period == "month":
        return str(today - timedelta(days=29)), str(today)
    else:
        return str(today), str(today)

# ============================================================
# META ADS API
# ============================================================
def get_all_campaigns(fields="name,status,effective_status"):
    all_campaigns = []
    url = f"https://graph.facebook.com/v21.0/{META_AD_ACCOUNT}/campaigns"
    params = {"fields": fields, "limit": 500, "access_token": META_ACCESS_TOKEN}
    while url:
        resp = requests.get(url, params=params)
        data = resp.json()
        if "data" in data:
            all_campaigns.extend(data["data"])
        url = data.get("paging", {}).get("next", None)
        params = {}
    return all_campaigns

def get_account_insights(since, until):
    url = f"https://graph.facebook.com/v21.0/{META_AD_ACCOUNT}/insights"
    params = {
        "fields": "campaign_name,spend,impressions,clicks,ctr,cpc,cpm,actions,cost_per_action_type",
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

# ============================================================
# EXTRACT ACTIONS — with deduplication
# ============================================================
# Map raw Meta action types to display labels
# Multiple raw types can map to the same label — we take the MAX count
ACTION_TYPE_TO_LABEL = {
    "lead": "📋 Лиды (форма)",
    "onsite_conversion.lead_grouped": "📋 Лиды (форма)",
    "offsite_conversion.fb_pixel_lead": "📋 Лиды (пиксель)",
    "onsite_conversion.messaging_conversation_started_7d": "💬 Переписки",
    "messaging_conversation_started_7d": "💬 Переписки",
    "onsite_conversion.messaging_first_reply": "💬 Первый ответ",
    "messaging_first_reply": "💬 Первый ответ",
    "landing_page_view": "🌐 Просмотры лендинга",
    "link_click": "🔗 Клики по ссылке",
    "post_engagement": "❤️ Вовлечённость",
    "omni_purchase": "🛒 Покупки",
    "contact_total": "📞 Контакты",
}

def extract_all_actions(insight):
    """Extract actions with deduplication — same label = take max count."""
    actions = insight.get("actions", [])
    costs = insight.get("cost_per_action_type", [])

    # Build raw maps
    action_map = {}
    for a in actions:
        atype = a.get("action_type", "")
        if atype in ACTION_TYPE_TO_LABEL:
            action_map[atype] = int(a.get("value", 0))

    cost_map = {}
    for c in costs:
        ctype = c.get("action_type", "")
        if ctype in ACTION_TYPE_TO_LABEL:
            cost_map[ctype] = float(c.get("value", 0))

    # Deduplicate: group by label, take max count
    label_data = {}
    for atype, count in action_map.items():
        if count <= 0:
            continue
        label = ACTION_TYPE_TO_LABEL[atype]
        cost = cost_map.get(atype, 0)
        if label not in label_data or count > label_data[label]["count"]:
            label_data[label] = {"label": label, "count": count, "cost_per": round(cost, 2)}

    return list(label_data.values())

def enrich_insights(insights):
    enriched = []
    for ins in insights:
        spend = float(ins.get("spend", 0))
        if spend == 0:
            continue
        impressions = int(ins.get("impressions", 0))
        clicks = int(ins.get("clicks", 0))
        ctr = round(float(ins.get("ctr", 0)), 2)
        cpc = round(float(ins.get("cpc", 0)), 2) if ins.get("cpc") else (round(spend / clicks, 2) if clicks > 0 else 0)
        cpm = round(float(ins.get("cpm", 0)), 2) if ins.get("cpm") else (round(spend / impressions * 1000, 2) if impressions > 0 else 0)
        enriched.append({
            "campaign_name": ins.get("campaign_name", "—"),
            "spend": round(spend, 2), "impressions": impressions, "clicks": clicks,
            "ctr": ctr, "cpc": cpc, "cpm": cpm,
            "actions": extract_all_actions(ins),
        })
    enriched.sort(key=lambda x: x["spend"], reverse=True)
    return enriched

# ============================================================
# FORMAT REPORT (no Claude needed)
# ============================================================
def format_report(data):
    campaigns = data.get("campaigns", [])
    period_names = {"today": "сегодня", "yesterday": "вчера", "week": "неделю", "month": "месяц"}
    p_name = period_names.get(data["period"], data["period"])
    since, until = data["since"], data["until"]

    if not campaigns:
        return f"📊 За {p_name} ({since}) расхода не было."

    header = f"📊 Сводка за {p_name} ({since}"
    if since != until:
        header += f" — {until}"
    header += ")\n" + f"{'─' * 30}\n\n"

    body = ""
    total_spend = 0
    for c in campaigns:
        total_spend += c["spend"]
        emoji = "🟢" if c["actions"] else "🔴"
        body += f"{emoji} {c['campaign_name']}\n"
        body += f"   💰 ${c['spend']:.2f} | 👁 {c['impressions']:,} показов\n"
        body += f"   🖱 {c['clicks']} кликов | CTR {c['ctr']:.2f}% | CPC ${c['cpc']:.2f}\n"
        for a in c["actions"]:
            body += f"   {a['label']}: {a['count']}"
            if a["cost_per"] > 0:
                body += f" (${a['cost_per']:.2f}/шт)"
            body += "\n"
        body += "\n"

    footer = f"{'─' * 30}\n💵 Общий расход: ${total_spend:.2f}\n"
    totals = {}
    for c in campaigns:
        for a in c["actions"]:
            totals[a["label"]] = totals.get(a["label"], 0) + a["count"]
    if totals:
        footer += "🎯 Итого:\n"
        for label, count in totals.items():
            footer += f"   {label}: {count}\n"
    return header + body + footer

# ============================================================
# INTENT DETECTION (with fallback)
# ============================================================
INTENT_PROMPT = """Парсер запросов. Ответь ТОЛЬКО JSON без markdown:
{"period": "today", "show": "spend"}

period: today | yesterday | week | month
show: spend | all_campaigns

- "как дела", "статус", "сводка", "сейчас" → today, spend
- "вчера" → yesterday, spend
- "неделя" → week, spend
- "месяц" → month, spend
- "все кампании", "список", "сколько" → all_campaigns
- по умолчанию → today, spend"""

def detect_intent(user_text):
    """Detect intent via Claude. If Claude is down, use keyword matching."""
    raw = call_claude(INTENT_PROMPT, user_text, max_tokens=100, retries=2)

    if raw:
        try:
            clean = raw.replace("```json", "").replace("```", "").strip()
            return json.loads(clean)
        except:
            pass

    # Fallback: simple keyword matching when Claude is unavailable
    text = user_text.lower()
    period = "today"
    show = "spend"

    if any(w in text for w in ["вчера", "yesterday"]):
        period = "yesterday"
    elif any(w in text for w in ["недел", "week", "7 дней"]):
        period = "week"
    elif any(w in text for w in ["месяц", "month", "30 дней"]):
        period = "month"

    if any(w in text for w in ["все кампании", "все компании", "список", "сколько кампаний"]):
        show = "all_campaigns"

    return {"period": period, "show": show}

# ============================================================
# FETCH DATA
# ============================================================
def fetch_spend_data(period):
    since, until = get_date_range(period)
    insights = get_account_insights(since, until)
    campaigns = enrich_insights(insights)
    total_spend = sum(c["spend"] for c in campaigns)
    return {"period": period, "since": since, "until": until, "campaigns": campaigns, "total_spend": round(total_spend, 2)}

def fetch_all_campaigns_list():
    camps = get_all_campaigns()
    active = [c.get("name", "—") for c in camps if c.get("effective_status") == "ACTIVE"]
    paused = len([c for c in camps if c.get("effective_status") == "PAUSED"])
    return {"total": len(camps), "active_names": active, "active_count": len(active), "paused_count": paused}

# ============================================================
# CLAUDE RESPONSE (with fallback to format_report)
# ============================================================
RESPONSE_PROMPT = """Ты — аналитик Meta Ads для салона iStudio Beauty Centre (Ришон ле-Цион).

ПРАВИЛА:
1. Отвечай ТОЛЬКО на основе данных JSON. НЕ придумывай.
2. Если campaigns пуст — "расхода не было".
3. НЕ используй Markdown таблицы. Эмодзи-формат.
4. КРАТКО но ПОЛНО: все метрики.
5. НЕ задавай вопросов в конце.
6. Анализируй: что работает, что нет.

Формат:
🟢/🔴 Название
   💰 Расход | 👁 Показы | 🖱 Клики | CTR | CPC
   [actions с ценой]
   💡 Краткий вывод

Итог + рекомендация.
Ориентиры CPL: B-Flexy $3.67, КП+РФ $4.77, Карбон 25 ИВР $5.09"""

def generate_response(user_text, data):
    # Campaign list — no Claude needed
    if "active_names" in data:
        text = f"📋 Всего: {data['total']}\n🟢 Активных: {data['active_count']} | 🔴 На паузе: {data['paused_count']}\n\n"
        if data["active_names"]:
            for name in data["active_names"]:
                text += f"  🟢 {name}\n"
        else:
            text += "Нет активных кампаний."
        return text

    campaigns = data.get("campaigns", [])
    p_names = {"today": "сегодня", "yesterday": "вчера", "week": "неделю", "month": "месяц"}
    if not campaigns:
        return f"📊 За {p_names.get(data.get('period','today'),'')} расхода не было."

    # Try Claude for smart analysis
    claude_response = call_claude(
        RESPONSE_PROMPT,
        f"Данные:\n{json.dumps(data, ensure_ascii=False)}\n\nЗапрос: {user_text}",
        max_tokens=2000, retries=2
    )

    # If Claude works — use it; otherwise fallback to format_report
    if claude_response:
        return claude_response
    else:
        return format_report(data)

# ============================================================
# MORNING REPORT
# ============================================================
def send_morning_report():
    data = fetch_spend_data("yesterday")
    report = f"🌅 Доброе утро!\n\n" + format_report(data)
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
    if message.chat.id != MY_CHAT_ID: return
    bot.send_message(MY_CHAT_ID,
        "👋 Привет! Я твой Meta Ads аналитик.\n\n"
        "Просто спрашивай:\n"
        "• «Как дела?» — сегодня\n"
        "• «Что вчера?» — вчера\n"
        "• «За неделю» / «За месяц»\n\n"
        "/today /yesterday /week /month /campaigns /alerts"
    )

@bot.message_handler(commands=["today"])
def cmd_today(message):
    if message.chat.id != MY_CHAT_ID: return
    bot.send_message(MY_CHAT_ID, "⏳")
    bot.send_message(MY_CHAT_ID, format_report(fetch_spend_data("today")))

@bot.message_handler(commands=["yesterday"])
def cmd_yesterday(message):
    if message.chat.id != MY_CHAT_ID: return
    bot.send_message(MY_CHAT_ID, "⏳")
    bot.send_message(MY_CHAT_ID, format_report(fetch_spend_data("yesterday")))

@bot.message_handler(commands=["week"])
def cmd_week(message):
    if message.chat.id != MY_CHAT_ID: return
    bot.send_message(MY_CHAT_ID, "⏳")
    bot.send_message(MY_CHAT_ID, format_report(fetch_spend_data("week")))

@bot.message_handler(commands=["month"])
def cmd_month(message):
    if message.chat.id != MY_CHAT_ID: return
    bot.send_message(MY_CHAT_ID, "⏳")
    bot.send_message(MY_CHAT_ID, format_report(fetch_spend_data("month")))

@bot.message_handler(commands=["campaigns"])
def cmd_campaigns(message):
    if message.chat.id != MY_CHAT_ID: return
    bot.send_message(MY_CHAT_ID, "⏳")
    bot.send_message(MY_CHAT_ID, generate_response("список кампаний", fetch_all_campaigns_list()))

@bot.message_handler(commands=["alerts"])
def cmd_alerts(message):
    if message.chat.id != MY_CHAT_ID: return
    data = fetch_spend_data("today")
    alerts = []
    for c in data["campaigns"]:
        if c["spend"] > 30 and not c["actions"]:
            alerts.append(f"🚨 {c['campaign_name']}: ${c['spend']:.2f}, 0 результатов!")
        if c["ctr"] < 1.0 and c["spend"] > 10:
            alerts.append(f"⚠️ {c['campaign_name']}: CTR {c['ctr']:.2f}%")
    bot.send_message(MY_CHAT_ID, "🔔 Алерты:\n\n" + "\n".join(alerts) if alerts else "✅ Алертов нет.")

@bot.message_handler(commands=["report"])
def cmd_report(message):
    if message.chat.id != MY_CHAT_ID: return
    send_morning_report()

# ============================================================
# FREE-TEXT
# ============================================================
@bot.message_handler(func=lambda m: m.chat.id == MY_CHAT_ID)
def handle_text(message):
    user_text = message.text.strip()
    bot.send_message(MY_CHAT_ID, "🤔 Думаю...")
    intent = detect_intent(user_text)
    print(f"Intent: {intent}")

    if intent.get("show") == "all_campaigns":
        data = fetch_all_campaigns_list()
    else:
        data = fetch_spend_data(intent.get("period", "today"))

    bot.send_message(MY_CHAT_ID, generate_response(user_text, data))

# ============================================================
# SCHEDULER
# ============================================================
def run_scheduler():
    utc_hour = 8 - ISRAEL_UTC_OFFSET
    schedule.every().day.at(f"{utc_hour:02d}:00").do(send_morning_report)
    while True:
        schedule.run_pending()
        time.sleep(30)

if __name__ == "__main__":
    print("🚀 Bot starting...")
    print(f"📅 Israel time: {get_israel_now().strftime('%Y-%m-%d %H:%M')}")
    scheduler_thread = threading.Thread(target=run_scheduler, daemon=True)
    scheduler_thread.start()
    print("⏰ Morning report at 08:00")
    print("📱 Polling...")
    bot.infinity_polling()
