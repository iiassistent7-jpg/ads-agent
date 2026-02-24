"""
Meta Ads Proactive Agent v2
============================
Команды:
    /start    — справка
    /today    — отчёт за сегодня
    /yesterday — отчёт за вчера
    /week     — отчёт за 7 дней
    /month    — отчёт за 30 дней
    /alerts   — проверить алерты сейчас
    /report   — полный AI-анализ за неделю

Автоматически:
    08:00 каждый день  — утренняя сводка
    каждый час         — проверка алертов
    пн 09:00           — еженедельный отчёт
"""

import telebot
import requests
import anthropic
import schedule
import time
import threading
from datetime import datetime
import os

# ============================================================
TELEGRAM_TOKEN    = os.environ.get("TELEGRAM_TOKEN", "8704107268:AAHa428Al9B1zxldaVVwbninGH4Skt1FBdE")
MY_CHAT_ID        = int(os.environ.get("MY_CHAT_ID", "320613087"))
META_ACCESS_TOKEN = os.environ.get("META_ACCESS_TOKEN", "EAAWjRhvFnNoBQ9hlLu1idGbeZCa377ykh87Qxin6k6v1N6ZBHRQXVvnzVzJZB6RV06eQ6TGZC4ahIaJHdbxdO6Yl7yoMh63PmtrQZC8BZBP9ZCvwPTYozdXw0m6eU6zmAJEYvWEP0d22BSZBRjrfr2rhgAxPYnng6h19ZBgT8RPBDAgDz6ZBNjqgRVlH8BLAdQ")
META_AD_ACCOUNT   = os.environ.get("META_AD_ACCOUNT", "act_1004160296398671")
ANTHROPIC_KEY     = os.environ.get("ANTHROPIC_KEY", "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")
CPL_SPIKE_PERCENT = 50
# ============================================================

bot = telebot.TeleBot(TELEGRAM_TOKEN)
previous_cpl = {}


def send(text):
    try:
        if len(text) > 4000:
            for i in range(0, len(text), 4000):
                bot.send_message(MY_CHAT_ID, text[i:i+4000], parse_mode="Markdown")
        else:
            bot.send_message(MY_CHAT_ID, text, parse_mode="Markdown")
    except Exception as e:
        print(f"Ошибка отправки: {e}")


def get_campaigns(date_preset="last_7d"):
    url = f"https://graph.facebook.com/v19.0/{META_AD_ACCOUNT}/campaigns"
    params = {
        "access_token": META_ACCESS_TOKEN,
        "fields": (
            f"id,name,status,objective,"
            f"insights.date_preset({date_preset}){{"
            f"spend,impressions,clicks,ctr,cpc,cpm,"
            f"actions,cost_per_action_type,reach,frequency"
            f"}}"
        ),
        "limit": 50,
    }
    r = requests.get(url, params=params)
    data = r.json()
    if "error" in data:
        print(f"Meta API error: {data['error']}")
        return []
    return data.get("data", [])


def extract_leads(insights):
    result = {"leads": 0, "cpl": None}
    if not insights:
        return result
    for a in insights.get("actions", []):
        if a.get("action_type") in ("lead", "onsite_conversion.lead_grouped"):
            result["leads"] = int(a.get("value", 0))
    for c in insights.get("cost_per_action_type", []):
        if c.get("action_type") in ("lead", "onsite_conversion.lead_grouped"):
            result["cpl"] = round(float(c.get("value", 0)), 2)
    return result


def format_period_report(date_preset, period_name):
    campaigns = get_campaigns(date_preset)
    lines = [f"📊 *Сводка Meta Ads — {period_name}*\n_{datetime.now().strftime('%d.%m.%Y %H:%M')}_\n"]

    total_spend = 0
    total_leads = 0
    active = 0
    paused = 0
    campaigns_with_data = []

    for c in campaigns:
        ins = c.get("insights", {}).get("data", [{}])[0] if c.get("insights") else {}
        ld = extract_leads(ins)
        spend = float(ins.get("spend", 0))
        if c.get("status") == "ACTIVE":
            active += 1
        else:
            paused += 1
        if spend > 0:
            total_spend += spend
            total_leads += ld["leads"]
            campaigns_with_data.append((c, ins, ld, spend))

    campaigns_with_data.sort(key=lambda x: x[3], reverse=True)

    for c, ins, ld, spend in campaigns_with_data[:10]:
        status_icon = "🟢" if c.get("status") == "ACTIVE" else "🔴"
        cpl_str = f" | CPL ${ld['cpl']}" if ld['cpl'] else ""
        lines.append(
            f"{status_icon} *{c['name'][:30]}*\n"
            f"   💰 ${spend:.2f} | 👤 {ld['leads']} лидов | CTR {ins.get('ctr','0')}%{cpl_str}"
        )

    lines.append(f"\n{'─'*30}")
    lines.append(f"💵 *Итого:* ${total_spend:.2f} | 🎯 *Лидов:* {total_leads}")
    if total_leads > 0:
        lines.append(f"📈 *Средний CPL:* ${total_spend/total_leads:.2f}")
    lines.append(f"🟢 Активных: {active} | 🔴 На паузе: {paused}")
    if not campaigns_with_data:
        lines.append("\n_За этот период расходов не найдено_")

    return "\n".join(lines)


def analyze_with_claude(data_text, prompt):
    client = anthropic.Anthropic(api_key=ANTHROPIC_KEY)
    msg = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=1500,
        system="Ты — эксперт по Meta Ads. Отвечай кратко для Telegram. Используй эмодзи. Максимум 700 слов.",
        messages=[{"role": "user", "content": f"{prompt}\n\n{data_text}"}]
    )
    return msg.content[0].text


def morning_briefing():
    print(f"[{datetime.now().strftime('%H:%M')}] Утренняя сводка...")
    report = format_period_report("yesterday", "Вчера")
    send(f"☀️ *Доброе утро!*\n\n{report}\n\n_/week — за неделю | /month — за месяц_")


def check_alerts():
    global previous_cpl
    print(f"[{datetime.now().strftime('%H:%M')}] Проверка алертов...")
    try:
        campaigns = get_campaigns("today")
        alerts = []
        for c in campaigns:
            name = c['name'][:30]
            ins = c.get("insights", {}).get("data", [{}])[0] if c.get("insights") else {}
            ld = extract_leads(ins)
            if c.get("status") == "PAUSED" and name in previous_cpl:
                alerts.append(f"⛔️ *Остановлена:* {name}")
            if ld["cpl"] and name in previous_cpl and previous_cpl[name]:
                old = previous_cpl[name]
                if ld["cpl"] > old * (1 + CPL_SPIKE_PERCENT / 100):
                    pct = round((ld["cpl"] / old - 1) * 100)
                    alerts.append(f"📈 *CPL +{pct}%* в _{name}_\n   ${old} → ${ld['cpl']}")
            if c.get("status") == "ACTIVE":
                previous_cpl[name] = ld["cpl"]
        if alerts:
            send("🚨 *АЛЕРТ Meta Ads*\n\n" + "\n\n".join(alerts))
    except Exception as e:
        print(f"Ошибка алертов: {e}")


def weekly_report():
    print(f"[{datetime.now().strftime('%H:%M')}] Еженедельный отчёт...")
    try:
        campaigns = get_campaigns("last_7d")
        lines = []
        for c in campaigns:
            ins = c.get("insights", {}).get("data", [{}])[0] if c.get("insights") else {}
            ld = extract_leads(ins)
            if float(ins.get("spend", 0)) > 0:
                lines.append(f"{c['name']} | {c.get('status')} | ${ins.get('spend','0')} | CTR {ins.get('ctr','0')}% | {ld['leads']} лидов | CPL ${ld['cpl'] or 'н/д'}")
        if not lines:
            send("📋 *Еженедельный отчёт*\n\n_За неделю расходов не найдено_")
            return
        analysis = analyze_with_claude("\n".join(lines), "Еженедельный отчёт Meta Ads. Дай: 1) оценку недели, 2) топ-3 проблемы с цифрами, 3) план на следующую неделю — 3 конкретных действия.")
        send(f"📋 *Еженедельный отчёт*\n_{datetime.now().strftime('%d.%m.%Y')}_\n\n{analysis}")
    except Exception as e:
        send(f"❌ Ошибка: {e}")


schedule.every().day.at("08:00").do(morning_briefing)
schedule.every().hour.do(check_alerts)
schedule.every().monday.at("09:00").do(weekly_report)


def run_schedule():
    while True:
        schedule.run_pending()
        time.sleep(30)


@bot.message_handler(commands=["start", "help"])
def cmd_start(message):
    if message.from_user.id != MY_CHAT_ID:
        return
    send("👋 *Meta Ads Agent v2*\n\n📅 *Периоды:*\n/today — сегодня\n/yesterday — вчера\n/week — 7 дней\n/month — 30 дней\n\n🤖 *Анализ:*\n/report — AI-анализ + рекомендации\n/alerts — проверить алерты\n\n💬 Или задай вопрос текстом\n\n⏰ *Авто:* 08:00 сводка | каждый час алерты | пн 09:00 отчёт")


@bot.message_handler(commands=["today"])
def cmd_today(message):
    if message.from_user.id != MY_CHAT_ID:
        return
    bot.reply_to(message, "⏳ Загружаю за сегодня...")
    send(format_period_report("today", "Сегодня"))


@bot.message_handler(commands=["yesterday"])
def cmd_yesterday(message):
    if message.from_user.id != MY_CHAT_ID:
        return
    bot.reply_to(message, "⏳ Загружаю за вчера...")
    send(format_period_report("yesterday", "Вчера"))


@bot.message_handler(commands=["week"])
def cmd_week(message):
    if message.from_user.id != MY_CHAT_ID:
        return
    bot.reply_to(message, "⏳ Загружаю за 7 дней...")
    send(format_period_report("last_7d", "Последние 7 дней"))


@bot.message_handler(commands=["month"])
def cmd_month(message):
    if message.from_user.id != MY_CHAT_ID:
        return
    bot.reply_to(message, "⏳ Загружаю за 30 дней...")
    send(format_period_report("last_30d", "Последние 30 дней"))


@bot.message_handler(commands=["alerts"])
def cmd_alerts(message):
    if message.from_user.id != MY_CHAT_ID:
        return
    bot.reply_to(message, "⏳ Проверяю...")
    check_alerts()
    bot.send_message(MY_CHAT_ID, "✅ Готово")


@bot.message_handler(commands=["report"])
def cmd_report(message):
    if message.from_user.id != MY_CHAT_ID:
        return
    bot.reply_to(message, "⏳ Анализирую с Claude... (~30 сек)")
    weekly_report()


@bot.message_handler(func=lambda m: True)
def handle_question(message):
    if message.from_user.id != MY_CHAT_ID:
        return
    bot.reply_to(message, "⏳ Думаю... (~30 сек)")
    try:
        campaigns = get_campaigns("last_7d")
        lines = []
        for c in campaigns:
            ins = c.get("insights", {}).get("data", [{}])[0] if c.get("insights") else {}
            ld = extract_leads(ins)
            if float(ins.get("spend", 0)) > 0:
                lines.append(f"{c['name']} | {c.get('status')} | ${ins.get('spend','0')} | CTR {ins.get('ctr','0')}% | {ld['leads']} лидов | CPL ${ld['cpl'] or 'н/д'}")
        data = "\n".join(lines) if lines else "Данных за 7 дней нет"
        answer = analyze_with_claude(data, f"Вопрос: {message.text}")
        bot.reply_to(message, answer)
    except Exception as e:
        bot.reply_to(message, f"❌ Ошибка: {e}")


print("🤖 Meta Ads Agent v2 запущен!")
t = threading.Thread(target=run_schedule, daemon=True)
t.start()
send("🚀 *Агент v2 запущен!* Напиши /start для справки.")
bot.polling(none_stop=True, timeout=60)
