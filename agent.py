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
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "")
MY_CHAT_ID = int(os.environ.get("MY_CHAT_ID", "0"))
META_AD_ACCOUNT = os.environ.get("META_AD_ACCOUNT", "")
META_ACCESS_TOKEN = os.environ.get("META_ACCESS_TOKEN", "")
ANTHROPIC_KEY = os.environ.get("ANTHROPIC_KEY", "")

# amoCRM config
AMOCRM_DOMAIN = os.environ.get("AMOCRM_DOMAIN", "istudiomkac.amocrm.ru")
AMOCRM_TOKEN = os.environ.get("AMOCRM_TOKEN", "")

ISRAEL_UTC_OFFSET = 2

bot = telebot.TeleBot(TELEGRAM_TOKEN)
claude = anthropic.Anthropic(api_key=ANTHROPIC_KEY)

# ============================================================
# CLAUDE API WITH RETRY
# ============================================================
def call_claude(system_prompt, user_content, max_tokens=2000, retries=3):
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
    elif period == "all":
        return "2023-01-01", str(today)
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
    actions = insight.get("actions", [])
    costs = insight.get("cost_per_action_type", [])
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
# amoCRM API
# ============================================================
def amocrm_request(endpoint, params=None):
    """Make authenticated request to amoCRM API v4."""
    if not AMOCRM_TOKEN:
        print("amoCRM token not configured")
        return None
    url = f"https://{AMOCRM_DOMAIN}/api/v4/{endpoint}"
    headers = {"Authorization": f"Bearer {AMOCRM_TOKEN}"}
    try:
        resp = requests.get(url, headers=headers, params=params or {}, timeout=30)
        if resp.status_code == 204:
            return {"_embedded": {}}
        if resp.status_code == 200:
            return resp.json()
        else:
            print(f"amoCRM error {resp.status_code}: {resp.text[:200]}")
            return None
    except Exception as e:
        print(f"amoCRM request error: {e}")
        return None

def get_amocrm_pipelines():
    """Get all pipelines (funnels) with stages."""
    data = amocrm_request("leads/pipelines")
    if not data:
        return []
    pipelines = []
    for p in data.get("_embedded", {}).get("pipelines", []):
        stages = []
        for s in p.get("_embedded", {}).get("statuses", []):
            stages.append({
                "id": s.get("id"),
                "name": s.get("name"),
                "sort": s.get("sort", 0),
            })
        stages.sort(key=lambda x: x["sort"])
        pipelines.append({
            "id": p.get("id"),
            "name": p.get("name"),
            "stages": stages,
        })
    return pipelines

def get_all_amocrm_deals(max_pages=20):
    """Fetch all deals with pagination."""
    all_deals = []
    for page in range(1, max_pages + 1):
        params = {"limit": 250, "page": page, "with": "contacts"}
        data = amocrm_request("leads", params)
        if not data:
            break
        deals = data.get("_embedded", {}).get("leads", [])
        if not deals:
            break
        all_deals.extend(deals)
        if len(deals) < 250:
            break
        time.sleep(0.3)  # Rate limit
    return all_deals

def get_deal_tags(deal):
    """Extract tags from a deal."""
    tags = deal.get("_embedded", {}).get("tags", [])
    return [t.get("name", "") for t in tags]

def extract_fb_tag(tags):
    """Extract Facebook campaign ID from tags like 'fb14285258249'."""
    for tag in tags:
        if tag.startswith("fb") and len(tag) > 3:
            return tag.rstrip("!")
    return None

def parse_campaign_tag(tags):
    """Parse procedure/language/budget tag like 'Карбон ИВР 250'."""
    branches = {"Ришон", "Хайфа", "Тель-Авив"}
    for tag in tags:
        if tag.startswith("fb") or tag in branches:
            continue
        parts = tag.split()
        if len(parts) >= 2:
            return {
                "procedure": parts[0],
                "language": parts[1] if len(parts) >= 2 else "",
                "budget": parts[2] if len(parts) >= 3 else "",
                "raw": tag,
            }
    return None

def get_deal_branch(tags):
    """Extract branch from tags."""
    for tag in tags:
        if tag in ["Ришон", "Хайфа", "Тель-Авив"]:
            return tag
    return "Не указан"

# ============================================================
# amoCRM ANALYTICS
# ============================================================
def analyze_crm_data():
    """Full CRM analysis."""
    print("Fetching amoCRM data...")
    deals = get_all_amocrm_deals()
    pipelines = get_amocrm_pipelines()

    if not deals:
        return {"error": "Не удалось загрузить сделки из amoCRM"}

    # Build stage name map
    stage_map = {}
    for p in pipelines:
        for s in p["stages"]:
            stage_map[s["id"]] = s["name"]

    total_revenue = 0
    total_deals = len(deals)
    deals_with_revenue = 0
    by_source = {}
    by_campaign_tag = {}
    by_stage = {}
    by_branch = {}

    for deal in deals:
        price = deal.get("price", 0) or 0
        tags = get_deal_tags(deal)
        fb_tag = extract_fb_tag(tags)
        campaign_info = parse_campaign_tag(tags)
        branch = get_deal_branch(tags)
        stage_id = deal.get("status_id", 0)
        stage_name = stage_map.get(stage_id, f"Stage {stage_id}")

        total_revenue += price
        if price > 0:
            deals_with_revenue += 1

        by_stage[stage_name] = by_stage.get(stage_name, 0) + 1

        if branch not in by_branch:
            by_branch[branch] = {"deals": 0, "revenue": 0}
        by_branch[branch]["deals"] += 1
        by_branch[branch]["revenue"] += price

        if fb_tag:
            if fb_tag not in by_source:
                by_source[fb_tag] = {"deals": 0, "revenue": 0, "with_revenue": 0}
            by_source[fb_tag]["deals"] += 1
            by_source[fb_tag]["revenue"] += price
            if price > 0:
                by_source[fb_tag]["with_revenue"] += 1

        if campaign_info:
            tag_key = campaign_info["raw"]
            if tag_key not in by_campaign_tag:
                by_campaign_tag[tag_key] = {"deals": 0, "revenue": 0, "with_revenue": 0}
            by_campaign_tag[tag_key]["deals"] += 1
            by_campaign_tag[tag_key]["revenue"] += price
            if price > 0:
                by_campaign_tag[tag_key]["with_revenue"] += 1

    sorted_campaigns = sorted(by_campaign_tag.items(), key=lambda x: x[1]["revenue"], reverse=True)

    return {
        "total_deals": total_deals,
        "total_revenue": total_revenue,
        "deals_with_revenue": deals_with_revenue,
        "avg_deal": round(total_revenue / deals_with_revenue, 2) if deals_with_revenue > 0 else 0,
        "by_stage": by_stage,
        "by_branch": by_branch,
        "by_source": dict(list(sorted(by_source.items(), key=lambda x: x[1]["revenue"], reverse=True))[:20]),
        "by_campaign_tag": dict(sorted_campaigns[:15]),
        "pipelines": [{"name": p["name"], "stages": [s["name"] for s in p["stages"]]} for p in pipelines],
    }

def analyze_campaign_roi():
    """Match Meta Ads spend with amoCRM revenue."""
    since, until = get_date_range("all")
    insights = get_account_insights(since, until)
    meta_campaigns = enrich_insights(insights)

    crm = analyze_crm_data()
    if "error" in crm:
        return crm

    roi_data = []
    for mc in meta_campaigns:
        campaign_name = mc["campaign_name"]
        spend = mc["spend"]
        matched_revenue = 0
        matched_deals = 0
        matched_tag = None

        for tag, tag_data in crm.get("by_campaign_tag", {}).items():
            tag_words = tag.lower().split()
            name_lower = campaign_name.lower()
            if any(w in name_lower for w in tag_words if len(w) > 2):
                matched_revenue += tag_data["revenue"]
                matched_deals += tag_data["deals"]
                matched_tag = tag

        roi = round((matched_revenue - spend) / spend * 100, 1) if spend > 0 else 0

        if spend > 0:
            roi_data.append({
                "campaign": campaign_name,
                "spend": spend,
                "revenue": matched_revenue,
                "deals": matched_deals,
                "roi": roi,
                "matched_tag": matched_tag,
            })

    roi_data.sort(key=lambda x: x["roi"], reverse=True)

    return {
        "roi_campaigns": roi_data[:15],
        "total_spend": sum(r["spend"] for r in roi_data),
        "total_revenue": crm["total_revenue"],
        "total_deals": crm["total_deals"],
    }

def analyze_funnel():
    """Analyze funnel conversion rates."""
    crm = analyze_crm_data()
    if "error" in crm:
        return crm
    return {
        "total_deals": crm["total_deals"],
        "by_stage": crm["by_stage"],
        "pipelines": crm["pipelines"],
        "by_branch": crm["by_branch"],
    }

def analyze_ltv():
    """Analyze LTV by source and campaign."""
    crm = analyze_crm_data()
    if "error" in crm:
        return crm
    return {
        "total_deals": crm["total_deals"],
        "total_revenue": crm["total_revenue"],
        "avg_deal": crm["avg_deal"],
        "deals_with_revenue": crm["deals_with_revenue"],
        "top_campaigns": crm["by_campaign_tag"],
        "by_branch": crm["by_branch"],
        "by_source": crm["by_source"],
    }

# ============================================================
# FORMAT REPORTS (fallback, no Claude)
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

def format_crm_report(data):
    if "error" in data:
        return f"❌ {data['error']}"
    text = f"📊 amoCRM Сводка\n{'─' * 30}\n\n"
    text += f"📋 Всего сделок: {data['total_deals']}\n"
    text += f"💰 Общая выручка: ₪{data['total_revenue']:,.0f}\n"
    text += f"💵 Средний чек: ₪{data['avg_deal']:,.0f}\n"
    text += f"✅ Сделок с оплатой: {data['deals_with_revenue']}\n\n"
    if data.get("by_stage"):
        text += "📊 По этапам воронки:\n"
        for stage, count in data["by_stage"].items():
            text += f"   • {stage}: {count}\n"
        text += "\n"
    if data.get("by_campaign_tag"):
        text += "🏷 Топ кампании (по выручке):\n"
        for tag, info in list(data["by_campaign_tag"].items())[:10]:
            text += f"   • {tag}: {info['deals']} сделок, ₪{info['revenue']:,.0f}\n"
    return text

def format_roi_report(data):
    if "error" in data:
        return f"❌ {data['error']}"
    text = f"📊 ROI по кампаниям\n{'─' * 30}\n\n"
    text += f"💵 Расход Meta: ${data['total_spend']:,.2f}\n"
    text += f"💰 Выручка CRM: ₪{data['total_revenue']:,.0f}\n\n"
    for r in data.get("roi_campaigns", [])[:10]:
        emoji = "🟢" if r["roi"] > 100 else "🟡" if r["roi"] > 0 else "🔴"
        text += f"{emoji} {r['campaign']}\n"
        text += f"   💸 ${r['spend']:.2f} → ₪{r['revenue']:,.0f} | {r['deals']} сделок | ROI: {r['roi']}%\n\n"
    return text

def format_funnel_report(data):
    if "error" in data:
        return f"❌ {data['error']}"
    text = f"📊 Воронка продаж\n{'─' * 30}\n\n"
    text += f"📋 Всего: {data['total_deals']}\n\n"
    if data.get("by_stage"):
        total = data["total_deals"]
        for stage, count in data["by_stage"].items():
            pct = round(count / total * 100, 1) if total > 0 else 0
            bar = "█" * int(pct / 5) + "░" * (20 - int(pct / 5))
            text += f"   {stage}\n   {bar} {count} ({pct}%)\n"
    return text

def format_ltv_report(data):
    if "error" in data:
        return f"❌ {data['error']}"
    text = f"📊 LTV Анализ\n{'─' * 30}\n\n"
    text += f"📋 Сделок: {data['total_deals']} | С оплатой: {data['deals_with_revenue']}\n"
    text += f"💰 Выручка: ₪{data['total_revenue']:,.0f} | Средний: ₪{data['avg_deal']:,.0f}\n\n"
    if data.get("top_campaigns"):
        text += "🏷 Топ источники:\n"
        for tag, info in list(data["top_campaigns"].items())[:10]:
            avg = round(info["revenue"] / info["with_revenue"], 0) if info.get("with_revenue", 0) > 0 else 0
            text += f"   • {tag}: {info['deals']} сделок, ₪{info['revenue']:,.0f}"
            if avg > 0:
                text += f" (средн. ₪{avg:,.0f})"
            text += "\n"
    return text

# ============================================================
# INTENT DETECTION
# ============================================================
INTENT_PROMPT = """Парсер запросов рекламного бота. Ответь ТОЛЬКО JSON без markdown:
{"period": "today", "show": "spend"}

period: today | yesterday | week | month | all
show: spend | all_campaigns | crm | roi | ltv | funnel

- "как дела", "статус", "сводка" → today, spend
- "вчера" → yesterday, spend
- "неделя" → week, spend
- "месяц" → month, spend
- "все кампании", "список" → all_campaigns
- "crm", "срм", "клиенты", "сделки", "амо" → crm
- "roi", "рои", "окупаемость", "эффективн", "лучшие кампании", "топ" → roi
- "ltv", "лтв", "выручка", "доход", "платиновые" → ltv
- "воронка", "конверсия", "funnel", "потери" → funnel
- по умолчанию → today, spend"""

def detect_intent(user_text):
    raw = call_claude(INTENT_PROMPT, user_text, max_tokens=100, retries=2)
    if raw:
        try:
            clean = raw.replace("```json", "").replace("```", "").strip()
            return json.loads(clean)
        except:
            pass

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
    elif any(w in text for w in ["crm", "срм", "амо", "amocrm", "сделки", "клиенты"]):
        show = "crm"
    elif any(w in text for w in ["roi", "рои", "окупаемость", "эффективн", "лучш", "топ"]):
        show = "roi"
    elif any(w in text for w in ["ltv", "лтв", "выручка", "доход", "платинов"]):
        show = "ltv"
    elif any(w in text for w in ["воронка", "конверси", "funnel", "теряем", "потери"]):
        show = "funnel"

    return {"period": period, "show": show}

# ============================================================
# FETCH & RESPOND
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

CRM_RESPONSE_PROMPT = """Ты — аналитик CRM и рекламы для салона красоты iStudio Beauty Centre.

ПРАВИЛА:
1. ЖИВОЙ ТЕКСТ с инсайтами и рекомендациями. Не сухие таблицы.
2. Данные из JSON — НЕ придумывай цифры.
3. Эмодзи для навигации.
4. Конкретные советы: "Убей кампанию X" или "Увеличь бюджет на Y".
5. НЕ задавай вопросов.
6. Валюта расходов Meta — $, выручка amoCRM — ₪.

Стиль: умный маркетолог, который говорит прямо.
Пример: "Карбон ИВР — твоя золотая жила: ₪15,600 выручки при $3,200 расходе."
"""

def generate_response(user_text, data):
    if "active_names" in data:
        text = f"📋 Всего: {data['total']}\n🟢 Активных: {data['active_count']} | 🔴 На паузе: {data['paused_count']}\n\n"
        if data["active_names"]:
            for name in data["active_names"]:
                text += f"  🟢 {name}\n"
        else:
            text += "Нет активных кампаний."
        return text

    data_type = data.get("_type", "")
    if data_type in ("crm", "roi", "ltv", "funnel"):
        if "error" in data:
            return f"❌ {data['error']}"
        claude_response = call_claude(
            CRM_RESPONSE_PROMPT,
            f"Тип: {data_type}\nДанные:\n{json.dumps(data, ensure_ascii=False)}\n\nЗапрос: {user_text}",
            max_tokens=2000, retries=2
        )
        if claude_response:
            return claude_response
        fallback = {"crm": format_crm_report, "roi": format_roi_report, "ltv": format_ltv_report, "funnel": format_funnel_report}
        return fallback.get(data_type, format_crm_report)(data)

    campaigns = data.get("campaigns", [])
    p_names = {"today": "сегодня", "yesterday": "вчера", "week": "неделю", "month": "месяц"}
    if not campaigns:
        return f"📊 За {p_names.get(data.get('period','today'),'')} расхода не было."

    claude_response = call_claude(
        RESPONSE_PROMPT,
        f"Данные:\n{json.dumps(data, ensure_ascii=False)}\n\nЗапрос: {user_text}",
        max_tokens=2000, retries=2
    )
    return claude_response if claude_response else format_report(data)

# ============================================================
# MORNING & WEEKLY REPORTS
# ============================================================
def send_morning_report():
    data = fetch_spend_data("yesterday")
    report = f"🌅 Доброе утро!\n\n" + format_report(data)
    report += f"\n/week — неделя | /month — месяц | /crm — CRM"
    try:
        bot.send_message(MY_CHAT_ID, report)
    except Exception as e:
        print(f"Morning report error: {e}")

def send_weekly_crm_report():
    try:
        bot.send_message(MY_CHAT_ID, "📊 Еженедельный CRM отчёт...\n⏳")
        roi_data = analyze_campaign_roi()
        roi_data["_type"] = "roi"
        report = generate_response("еженедельный отчёт ROI по кампаниям", roi_data)
        bot.send_message(MY_CHAT_ID, report)
    except Exception as e:
        print(f"Weekly CRM report error: {e}")

# ============================================================
# TELEGRAM HANDLERS
# ============================================================
@bot.message_handler(commands=["start"])
def cmd_start(message):
    if message.chat.id != MY_CHAT_ID: return
    bot.send_message(MY_CHAT_ID,
        "👋 Привет! Я твой Meta Ads + CRM аналитик.\n\n"
        "📊 Meta Ads:\n"
        "• «Как дела?» — сегодня\n"
        "• «Что вчера?» — вчера\n"
        "• /week /month /campaigns /alerts\n\n"
        "📈 CRM Аналитика:\n"
        "• /crm — сводка amoCRM\n"
        "• /roi — ROI по кампаниям\n"
        "• /ltv — LTV и выручка\n"
        "• /funnel — воронка продаж\n\n"
        "Или спрашивай своими словами!"
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

@bot.message_handler(commands=["crm"])
def cmd_crm(message):
    if message.chat.id != MY_CHAT_ID: return
    bot.send_message(MY_CHAT_ID, "📊 Загружаю amoCRM...\n⏳")
    data = analyze_crm_data()
    data["_type"] = "crm"
    bot.send_message(MY_CHAT_ID, generate_response("сводка CRM", data))

@bot.message_handler(commands=["roi"])
def cmd_roi(message):
    if message.chat.id != MY_CHAT_ID: return
    bot.send_message(MY_CHAT_ID, "📊 Считаю ROI...\n⏳")
    data = analyze_campaign_roi()
    data["_type"] = "roi"
    bot.send_message(MY_CHAT_ID, generate_response("ROI по кампаниям", data))

@bot.message_handler(commands=["ltv"])
def cmd_ltv(message):
    if message.chat.id != MY_CHAT_ID: return
    bot.send_message(MY_CHAT_ID, "📊 Анализирую LTV...\n⏳")
    data = analyze_ltv()
    data["_type"] = "ltv"
    bot.send_message(MY_CHAT_ID, generate_response("LTV анализ", data))

@bot.message_handler(commands=["funnel"])
def cmd_funnel(message):
    if message.chat.id != MY_CHAT_ID: return
    bot.send_message(MY_CHAT_ID, "📊 Анализирую воронку...\n⏳")
    data = analyze_funnel()
    data["_type"] = "funnel"
    bot.send_message(MY_CHAT_ID, generate_response("воронка продаж", data))

# ============================================================
# FREE-TEXT
# ============================================================
@bot.message_handler(func=lambda m: m.chat.id == MY_CHAT_ID)
def handle_text(message):
    user_text = message.text.strip()
    bot.send_message(MY_CHAT_ID, "🤔 Думаю...")
    intent = detect_intent(user_text)
    print(f"Intent: {intent}")

    show = intent.get("show", "spend")
    if show == "all_campaigns":
        data = fetch_all_campaigns_list()
    elif show == "crm":
        data = analyze_crm_data()
        data["_type"] = "crm"
    elif show == "roi":
        data = analyze_campaign_roi()
        data["_type"] = "roi"
    elif show == "ltv":
        data = analyze_ltv()
        data["_type"] = "ltv"
    elif show == "funnel":
        data = analyze_funnel()
        data["_type"] = "funnel"
    else:
        data = fetch_spend_data(intent.get("period", "today"))

    bot.send_message(MY_CHAT_ID, generate_response(user_text, data))

# ============================================================
# SCHEDULER
# ============================================================
def run_scheduler():
    utc_hour = 8 - ISRAEL_UTC_OFFSET
    schedule.every().day.at(f"{utc_hour:02d}:00").do(send_morning_report)
    utc_hour_weekly = 9 - ISRAEL_UTC_OFFSET
    schedule.every().sunday.at(f"{utc_hour_weekly:02d}:00").do(send_weekly_crm_report)
    while True:
        schedule.run_pending()
        time.sleep(30)

if __name__ == "__main__":
    print("🚀 Bot starting...")
    print(f"📅 Israel time: {get_israel_now().strftime('%Y-%m-%d %H:%M')}")
    print(f"📊 amoCRM: {'✅ configured' if AMOCRM_TOKEN else '❌ no token'}")
    scheduler_thread = threading.Thread(target=run_scheduler, daemon=True)
    scheduler_thread.start()
    print("⏰ Daily 08:00 | Weekly CRM: Sunday 09:00")
    print("📱 Polling...")
    bot.infinity_polling()
