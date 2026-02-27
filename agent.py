import os
import time
import json
import requests
import threading
import schedule
import re
from datetime import datetime, timedelta
from collections import defaultdict
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
def call_claude(system_prompt, user_content, max_tokens=3000, retries=3):
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
    elif period == "3months":
        return str(today - timedelta(days=89)), str(today)
    elif period == "6months":
        return str(today - timedelta(days=179)), str(today)
    elif period == "year":
        return str(today - timedelta(days=364)), str(today)
    elif period == "all":
        return "2023-01-01", str(today)
    else:
        return str(today), str(today)

def parse_custom_period(text):
    """Parse custom date references like 'за январь', 'за последние 3 месяца'."""
    text_lower = text.lower()
    now = get_israel_now()
    today = now.date()

    months_ru = {
        "январ": 1, "феврал": 2, "март": 3, "апрел": 4,
        "ма": 5, "июн": 6, "июл": 7, "август": 8,
        "сентябр": 9, "октябр": 10, "ноябр": 11, "декабр": 12
    }
    for name, month_num in months_ru.items():
        if name in text_lower:
            year = today.year if month_num <= today.month else today.year - 1
            from calendar import monthrange
            last_day = monthrange(year, month_num)[1]
            return str(datetime(year, month_num, 1).date()), str(datetime(year, month_num, last_day).date())

    match = re.search(r'(\d+)\s*(месяц|мес)', text_lower)
    if match:
        months = int(match.group(1))
        return str(today - timedelta(days=months * 30)), str(today)

    match = re.search(r'(\d+)\s*(недел)', text_lower)
    if match:
        weeks = int(match.group(1))
        return str(today - timedelta(weeks=weeks)), str(today)

    match = re.search(r'(\d+)\s*(дн|день|дней)', text_lower)
    if match:
        days = int(match.group(1))
        return str(today - timedelta(days=days)), str(today)

    return None, None

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
        "fields": "campaign_name,campaign_id,spend,impressions,clicks,ctr,cpc,cpm,actions,cost_per_action_type",
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

def get_meta_leads(since, until):
    """Fetch leads from Meta Leads Center (forms)."""
    all_leads = []
    # First get all forms
    url = f"https://graph.facebook.com/v21.0/{META_AD_ACCOUNT}/leadgen_forms"
    params = {"fields": "id,name,status", "access_token": META_ACCESS_TOKEN}
    try:
        resp = requests.get(url, params=params)
        forms = resp.json().get("data", [])
    except Exception as e:
        print(f"Meta forms error: {e}")
        return []

    since_ts = int(datetime.strptime(since, "%Y-%m-%d").timestamp())
    until_ts = int(datetime.strptime(until, "%Y-%m-%d").timestamp()) + 86400

    for form in forms:
        form_id = form.get("id")
        form_name = form.get("name", "")
        leads_url = f"https://graph.facebook.com/v21.0/{form_id}/leads"
        leads_params = {
            "fields": "id,created_time,field_data,ad_id,ad_name,campaign_id,campaign_name",
            "filtering": json.dumps([{"field": "time_created", "operator": "GREATER_THAN", "value": since_ts}]),
            "limit": 500,
            "access_token": META_ACCESS_TOKEN,
        }
        try:
            while True:
                resp = requests.get(leads_url, params=leads_params)
                data = resp.json()
                for lead in data.get("data", []):
                    lead["form_name"] = form_name
                    lead_time = lead.get("created_time", "")
                    all_leads.append(lead)
                next_url = data.get("paging", {}).get("next")
                if next_url:
                    leads_url = next_url
                    leads_params = {}
                else:
                    break
        except Exception as e:
            print(f"Error fetching leads for form {form_name}: {e}")
            continue
        time.sleep(0.5)  # Rate limit

    return all_leads

# ============================================================
# EXTRACT ACTIONS — with deduplication
# ============================================================
ACTION_TYPE_TO_LABEL = {
    "lead": "📋 Лиды",
    "onsite_conversion.lead_grouped": "📋 Лиды",
    "offsite_conversion.fb_pixel_lead": "📋 Лиды (пиксель)",
    "onsite_conversion.messaging_conversation_started_7d": "💬 Переписки",
    "messaging_conversation_started_7d": "💬 Переписки",
    "onsite_conversion.messaging_first_reply": "💬 Первый ответ",
    "messaging_first_reply": "💬 Первый ответ",
    "landing_page_view": "🌐 Просмотры",
    "link_click": "🔗 Клики",
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
        total_leads = 0
        for a in extract_all_actions(ins):
            if "Лид" in a["label"]:
                total_leads += a["count"]
        enriched.append({
            "campaign_name": ins.get("campaign_name", "—"),
            "campaign_id": ins.get("campaign_id", ""),
            "spend": round(spend, 2), "impressions": impressions, "clicks": clicks,
            "ctr": ctr, "cpc": cpc, "cpm": cpm,
            "actions": extract_all_actions(ins),
            "total_leads": total_leads,
            "cost_per_lead": round(spend / total_leads, 2) if total_leads > 0 else 0,
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
                "is_closed": s.get("type", 0) in [0, 1],  # closed won/lost
            })
        stages.sort(key=lambda x: x["sort"])
        pipelines.append({
            "id": p.get("id"),
            "name": p.get("name"),
            "stages": stages,
        })
    return pipelines

def get_all_amocrm_deals(max_pages=30, date_filter=None):
    """Fetch all deals with pagination. Optional date filter."""
    all_deals = []
    for page in range(1, max_pages + 1):
        params = {"limit": 250, "page": page, "with": "contacts"}
        if date_filter:
            params["filter[created_at][from]"] = date_filter.get("from", 0)
            params["filter[created_at][to]"] = date_filter.get("to", 0)
        data = amocrm_request("leads", params)
        if not data:
            break
        deals = data.get("_embedded", {}).get("leads", [])
        if not deals:
            break
        all_deals.extend(deals)
        if len(deals) < 250:
            break
        time.sleep(0.3)
    return all_deals

def get_amocrm_contacts(contact_ids):
    """Fetch contacts by IDs (batch) with name and phone."""
    contacts = {}
    if not contact_ids:
        return contacts
    unique_ids = list(set(contact_ids))
    batch_size = 50
    for i in range(0, len(unique_ids), batch_size):
        batch = unique_ids[i:i + batch_size]
        filter_str = "&".join([f"filter[id][]={cid}" for cid in batch])
        data = amocrm_request(f"contacts?{filter_str}&with=leads")
        if data:
            for c in data.get("_embedded", {}).get("contacts", []):
                name = c.get("name", "Без имени")
                phone = ""
                email = ""
                for cf in c.get("custom_fields_values", []):
                    field_code = cf.get("field_code", "")
                    values = cf.get("values", [])
                    if field_code == "PHONE" and values:
                        phone = values[0].get("value", "")
                    elif field_code == "EMAIL" and values:
                        email = values[0].get("value", "")
                contacts[c["id"]] = {
                    "id": c["id"],
                    "name": name,
                    "phone": phone,
                    "email": email,
                }
        time.sleep(0.3)
    return contacts

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
# DEEP ANALYTICS
# ============================================================
def analyze_crm_data(since=None, until=None):
    """Full CRM analysis with optional date filter."""
    print("Fetching amoCRM data...")

    date_filter = None
    if since and until:
        date_filter = {
            "from": int(datetime.strptime(since, "%Y-%m-%d").timestamp()),
            "to": int(datetime.strptime(until, "%Y-%m-%d").timestamp()) + 86400,
        }

    deals = get_all_amocrm_deals(date_filter=date_filter)
    pipelines = get_amocrm_pipelines()

    if not deals:
        return {"error": "Не удалось загрузить сделки из amoCRM"}

    stage_map = {}
    closed_won_ids = set()
    closed_lost_ids = set()
    for p in pipelines:
        for s in p["stages"]:
            stage_map[s["id"]] = s["name"]
            name_lower = s["name"].lower()
            if any(w in name_lower for w in ["успешно", "реализовано", "закрыто и реализовано", "оплач", "выполнен"]):
                closed_won_ids.add(s["id"])
            elif any(w in name_lower for w in ["закрыто и не реализовано", "отказ", "проиграна", "не реализовано"]):
                closed_lost_ids.add(s["id"])

    total_revenue = 0
    total_deals = len(deals)
    deals_with_revenue = 0
    by_source = {}
    by_campaign_tag = {}
    by_stage = {}
    by_branch = {}
    won_deals = []
    lost_deals = 0
    all_deal_details = []

    for deal in deals:
        price = deal.get("price", 0) or 0
        tags = get_deal_tags(deal)
        fb_tag = extract_fb_tag(tags)
        campaign_info = parse_campaign_tag(tags)
        branch = get_deal_branch(tags)
        stage_id = deal.get("status_id", 0)
        stage_name = stage_map.get(stage_id, f"Этап {stage_id}")
        created_at = deal.get("created_at", 0)
        closed_at = deal.get("closed_at", 0)

        total_revenue += price
        if price > 0:
            deals_with_revenue += 1

        by_stage[stage_name] = by_stage.get(stage_name, 0) + 1

        if branch not in by_branch:
            by_branch[branch] = {"deals": 0, "revenue": 0, "won": 0, "lost": 0}
        by_branch[branch]["deals"] += 1
        by_branch[branch]["revenue"] += price

        if stage_id in closed_won_ids:
            by_branch[branch]["won"] += 1
            won_deals.append(deal)
        elif stage_id in closed_lost_ids:
            by_branch[branch]["lost"] += 1
            lost_deals += 1

        if fb_tag:
            if fb_tag not in by_source:
                by_source[fb_tag] = {"deals": 0, "revenue": 0, "with_revenue": 0, "won": 0, "lost": 0}
            by_source[fb_tag]["deals"] += 1
            by_source[fb_tag]["revenue"] += price
            if price > 0:
                by_source[fb_tag]["with_revenue"] += 1
            if stage_id in closed_won_ids:
                by_source[fb_tag]["won"] += 1
            elif stage_id in closed_lost_ids:
                by_source[fb_tag]["lost"] += 1

        if campaign_info:
            tag_key = campaign_info["raw"]
            if tag_key not in by_campaign_tag:
                by_campaign_tag[tag_key] = {"deals": 0, "revenue": 0, "with_revenue": 0, "won": 0, "lost": 0, "prices": []}
            by_campaign_tag[tag_key]["deals"] += 1
            by_campaign_tag[tag_key]["revenue"] += price
            if price > 0:
                by_campaign_tag[tag_key]["with_revenue"] += 1
                by_campaign_tag[tag_key]["prices"].append(price)
            if stage_id in closed_won_ids:
                by_campaign_tag[tag_key]["won"] += 1
            elif stage_id in closed_lost_ids:
                by_campaign_tag[tag_key]["lost"] += 1

        # Collect deal detail for LTV
        deal_info = {
            "id": deal.get("id"),
            "name": deal.get("name", ""),
            "price": price,
            "stage": stage_name,
            "fb_tag": fb_tag,
            "campaign_tag": campaign_info["raw"] if campaign_info else None,
            "branch": branch,
            "created_at": created_at,
            "closed_at": closed_at,
            "is_won": stage_id in closed_won_ids,
            "is_lost": stage_id in closed_lost_ids,
            "contact_ids": [c["id"] for c in deal.get("_embedded", {}).get("contacts", [])],
        }
        all_deal_details.append(deal_info)

    # Cleanup prices from campaign tags for JSON serialization
    for tag_key in by_campaign_tag:
        prices = by_campaign_tag[tag_key].pop("prices", [])
        if prices:
            by_campaign_tag[tag_key]["avg_deal"] = round(sum(prices) / len(prices), 0)
            by_campaign_tag[tag_key]["max_deal"] = max(prices)
            by_campaign_tag[tag_key]["min_deal"] = min(prices)
        else:
            by_campaign_tag[tag_key]["avg_deal"] = 0

    sorted_campaigns = sorted(by_campaign_tag.items(), key=lambda x: x[1]["revenue"], reverse=True)

    return {
        "total_deals": total_deals,
        "total_revenue": total_revenue,
        "deals_with_revenue": deals_with_revenue,
        "avg_deal": round(total_revenue / deals_with_revenue, 2) if deals_with_revenue > 0 else 0,
        "won_deals": len(won_deals),
        "lost_deals": lost_deals,
        "conversion_rate": round(len(won_deals) / total_deals * 100, 1) if total_deals > 0 else 0,
        "by_stage": by_stage,
        "by_branch": by_branch,
        "by_source": dict(list(sorted(by_source.items(), key=lambda x: x[1]["revenue"], reverse=True))[:20]),
        "by_campaign_tag": dict(sorted_campaigns[:15]),
        "pipelines": [{"name": p["name"], "stages": [s["name"] for s in p["stages"]]} for p in pipelines],
        "period": {"since": since, "until": until} if since else None,
        "_deal_details": all_deal_details,  # For LTV analysis
    }

def analyze_golden_clients(since=None, until=None):
    """Find golden clients — repeat buyers, high LTV, long retention. With names and phones."""
    crm = analyze_crm_data(since, until)
    if "error" in crm:
        return crm

    deal_details = crm.pop("_deal_details", [])

    # Group by contact
    contact_deals = defaultdict(list)
    for d in deal_details:
        for cid in d.get("contact_ids", []):
            contact_deals[cid].append(d)

    # Fetch contact details (names, phones) from amoCRM
    all_contact_ids = list(contact_deals.keys())
    print(f"Fetching {len(all_contact_ids)} contacts from amoCRM...")
    contact_info_map = get_amocrm_contacts(all_contact_ids)

    # Analyze each contact
    golden_clients = []
    repeat_clients = []
    one_time_clients = []

    for cid, deals in contact_deals.items():
        total_spent = sum(d["price"] for d in deals)
        deal_count = len(deals)
        won_count = sum(1 for d in deals if d["is_won"])
        campaigns = list(set(d["campaign_tag"] for d in deals if d["campaign_tag"]))
        fb_tags = list(set(d["fb_tag"] for d in deals if d["fb_tag"]))
        branches = list(set(d["branch"] for d in deals))
        deal_names = list(set(d["name"] for d in deals if d["name"]))

        # Calculate client lifetime
        dates = [d["created_at"] for d in deals if d["created_at"]]
        if len(dates) >= 2:
            lifetime_days = (max(dates) - min(dates)) / 86400
        else:
            lifetime_days = 0

        # Get contact name and phone
        cinfo = contact_info_map.get(cid, {})
        client_name = cinfo.get("name", "Без имени")
        client_phone = cinfo.get("phone", "")
        client_email = cinfo.get("email", "")

        # Determine client source
        if fb_tags:
            client_source = "Реклама Meta"
            client_source_detail = ", ".join(campaigns) if campaigns else fb_tags[0]
        elif campaigns:
            client_source = "Реклама Meta"
            client_source_detail = ", ".join(campaigns)
        else:
            client_source = "Рекомендация / сарафан"
            client_source_detail = "Без рекламного тега"

        client_info = {
            "contact_id": cid,
            "name": client_name,
            "phone": client_phone,
            "email": client_email,
            "source": client_source,
            "source_detail": client_source_detail,
            "total_spent": total_spent,
            "deal_count": deal_count,
            "won_count": won_count,
            "campaigns": campaigns[:5],
            "fb_tags": fb_tags[:5],
            "branches": branches,
            "procedures": deal_names[:5],
            "lifetime_days": round(lifetime_days),
            "first_deal": min(dates) if dates else 0,
            "last_deal": max(dates) if dates else 0,
            "avg_deal_value": round(total_spent / deal_count, 0) if deal_count > 0 else 0,
        }

        if deal_count >= 3 or total_spent >= 3000:
            golden_clients.append(client_info)
        elif deal_count >= 2:
            repeat_clients.append(client_info)
        else:
            one_time_clients.append(client_info)

    golden_clients.sort(key=lambda x: x["total_spent"], reverse=True)
    repeat_clients.sort(key=lambda x: x["total_spent"], reverse=True)

    # Which campaigns produce golden clients?
    campaign_quality = defaultdict(lambda: {"golden": 0, "repeat": 0, "one_time": 0, "total_ltv": 0})
    for c in golden_clients:
        for camp in c["campaigns"]:
            campaign_quality[camp]["golden"] += 1
            campaign_quality[camp]["total_ltv"] += c["total_spent"]
    for c in repeat_clients:
        for camp in c["campaigns"]:
            campaign_quality[camp]["repeat"] += 1
            campaign_quality[camp]["total_ltv"] += c["total_spent"]
    for c in one_time_clients:
        for camp in c["campaigns"]:
            campaign_quality[camp]["one_time"] += 1
            campaign_quality[camp]["total_ltv"] += c["total_spent"]

    # Convert to serializable dict and add avg LTV
    campaign_quality_clean = {}
    for camp, data in campaign_quality.items():
        total_clients = data["golden"] + data["repeat"] + data["one_time"]
        campaign_quality_clean[camp] = {
            "golden_clients": data["golden"],
            "repeat_clients": data["repeat"],
            "one_time_clients": data["one_time"],
            "total_clients": total_clients,
            "total_ltv": data["total_ltv"],
            "avg_ltv_per_client": round(data["total_ltv"] / total_clients, 0) if total_clients > 0 else 0,
            "quality_score": round((data["golden"] * 3 + data["repeat"] * 1.5) / max(total_clients, 1) * 100, 1),
        }

    sorted_quality = sorted(campaign_quality_clean.items(), key=lambda x: x[1]["quality_score"], reverse=True)

    # Source breakdown: Ads vs Referral
    all_clients = golden_clients + repeat_clients + one_time_clients
    from_ads = [c for c in all_clients if c["source"] == "Реклама Meta"]
    from_referral = [c for c in all_clients if c["source"] == "Рекомендация / сарафан"]

    golden_from_ads = [c for c in golden_clients if c["source"] == "Реклама Meta"]
    golden_from_referral = [c for c in golden_clients if c["source"] == "Рекомендация / сарафан"]

    source_breakdown = {
        "total_from_ads": len(from_ads),
        "total_from_referral": len(from_referral),
        "revenue_from_ads": sum(c["total_spent"] for c in from_ads),
        "revenue_from_referral": sum(c["total_spent"] for c in from_referral),
        "avg_ltv_ads": round(sum(c["total_spent"] for c in from_ads) / len(from_ads), 0) if from_ads else 0,
        "avg_ltv_referral": round(sum(c["total_spent"] for c in from_referral) / len(from_referral), 0) if from_referral else 0,
        "golden_from_ads": len(golden_from_ads),
        "golden_from_referral": len(golden_from_referral),
        "golden_revenue_ads": sum(c["total_spent"] for c in golden_from_ads),
        "golden_revenue_referral": sum(c["total_spent"] for c in golden_from_referral),
    }

    return {
        "golden_clients_count": len(golden_clients),
        "repeat_clients_count": len(repeat_clients),
        "one_time_clients_count": len(one_time_clients),
        "top_golden": golden_clients[:15],
        "top_repeat": repeat_clients[:10],
        "campaign_quality": dict(sorted_quality[:15]),
        "source_breakdown": source_breakdown,
        "total_golden_revenue": sum(c["total_spent"] for c in golden_clients),
        "total_repeat_revenue": sum(c["total_spent"] for c in repeat_clients),
        "total_onetime_revenue": sum(c["total_spent"] for c in one_time_clients),
        "total_clients": len(contact_deals),
        "period": {"since": since, "until": until} if since else None,
        "crm_summary": {
            "total_deals": crm["total_deals"],
            "total_revenue": crm["total_revenue"],
            "by_campaign_tag": crm["by_campaign_tag"],
        },
    }

def analyze_campaign_roi(since=None, until=None):
    """Match Meta Ads spend with amoCRM revenue."""
    if not since or not until:
        since, until = get_date_range("all")
    insights = get_account_insights(since, until)
    meta_campaigns = enrich_insights(insights)

    crm = analyze_crm_data(since, until)
    if "error" in crm:
        return crm
    crm.pop("_deal_details", None)

    # Try to get Meta leads count
    meta_leads = []
    try:
        meta_leads = get_meta_leads(since, until)
    except Exception as e:
        print(f"Meta leads fetch error: {e}")

    meta_leads_by_campaign = defaultdict(int)
    for lead in meta_leads:
        camp_name = lead.get("campaign_name", "")
        if camp_name:
            meta_leads_by_campaign[camp_name] += 1

    roi_data = []
    for mc in meta_campaigns:
        campaign_name = mc["campaign_name"]
        spend = mc["spend"]
        matched_revenue = 0
        matched_deals = 0
        matched_won = 0
        matched_lost = 0
        matched_tag = None

        for tag, tag_data in crm.get("by_campaign_tag", {}).items():
            tag_words = tag.lower().split()
            name_lower = campaign_name.lower()
            if any(w in name_lower for w in tag_words if len(w) > 2):
                matched_revenue += tag_data["revenue"]
                matched_deals += tag_data["deals"]
                matched_won += tag_data.get("won", 0)
                matched_lost += tag_data.get("lost", 0)
                matched_tag = tag

        roi = round((matched_revenue - spend) / spend * 100, 1) if spend > 0 else 0
        meta_leads_count = meta_leads_by_campaign.get(campaign_name, mc.get("total_leads", 0))
        cost_per_client = round(spend / matched_won, 2) if matched_won > 0 else 0

        if spend > 0:
            roi_data.append({
                "campaign": campaign_name,
                "spend": spend,
                "revenue": matched_revenue,
                "deals_in_crm": matched_deals,
                "won": matched_won,
                "lost": matched_lost,
                "leads_from_meta": meta_leads_count,
                "cost_per_lead": mc.get("cost_per_lead", 0),
                "cost_per_client": cost_per_client,
                "roi_percent": roi,
                "matched_tag": matched_tag,
            })

    roi_data.sort(key=lambda x: x["roi_percent"], reverse=True)

    total_spend = sum(r["spend"] for r in roi_data)
    total_revenue = crm["total_revenue"]

    return {
        "roi_campaigns": roi_data[:20],
        "total_spend": total_spend,
        "total_revenue": total_revenue,
        "total_deals": crm["total_deals"],
        "total_roi": round((total_revenue - total_spend) / total_spend * 100, 1) if total_spend > 0 else 0,
        "meta_leads_total": len(meta_leads),
        "period": {"since": since, "until": until},
    }

def analyze_funnel(since=None, until=None):
    """Analyze funnel conversion rates."""
    crm = analyze_crm_data(since, until)
    if "error" in crm:
        return crm
    crm.pop("_deal_details", None)
    return {
        "total_deals": crm["total_deals"],
        "won_deals": crm["won_deals"],
        "lost_deals": crm["lost_deals"],
        "conversion_rate": crm["conversion_rate"],
        "by_stage": crm["by_stage"],
        "pipelines": crm["pipelines"],
        "by_branch": crm["by_branch"],
        "period": {"since": since, "until": until} if since else None,
    }

def analyze_ltv(since=None, until=None):
    """Analyze LTV by source and campaign with golden client data."""
    crm = analyze_crm_data(since, until)
    if "error" in crm:
        return crm
    crm.pop("_deal_details", None)
    return {
        "total_deals": crm["total_deals"],
        "total_revenue": crm["total_revenue"],
        "avg_deal": crm["avg_deal"],
        "deals_with_revenue": crm["deals_with_revenue"],
        "won_deals": crm["won_deals"],
        "lost_deals": crm["lost_deals"],
        "conversion_rate": crm["conversion_rate"],
        "top_campaigns": crm["by_campaign_tag"],
        "by_branch": crm["by_branch"],
        "by_source": crm["by_source"],
        "period": {"since": since, "until": until} if since else None,
    }

def full_analytics(since=None, until=None):
    """Complete analytics: Meta + Leads + CRM combined."""
    if not since or not until:
        since, until = get_date_range("month")

    # Meta Ads data
    insights = get_account_insights(since, until)
    meta_campaigns = enrich_insights(insights)

    # Meta leads
    meta_leads = []
    try:
        meta_leads = get_meta_leads(since, until)
    except:
        pass

    # CRM data
    crm = analyze_crm_data(since, until)
    if "error" in crm:
        return crm
    crm.pop("_deal_details", None)

    total_meta_spend = sum(c["spend"] for c in meta_campaigns)
    total_meta_leads = sum(c.get("total_leads", 0) for c in meta_campaigns)

    return {
        "period": {"since": since, "until": until},
        "meta_ads": {
            "total_spend": total_meta_spend,
            "total_campaigns": len(meta_campaigns),
            "total_leads": total_meta_leads,
            "avg_cost_per_lead": round(total_meta_spend / total_meta_leads, 2) if total_meta_leads > 0 else 0,
            "top_campaigns": [
                {
                    "name": c["campaign_name"],
                    "spend": c["spend"],
                    "leads": c["total_leads"],
                    "cost_per_lead": c["cost_per_lead"],
                    "clicks": c["clicks"],
                    "ctr": c["ctr"],
                }
                for c in meta_campaigns[:10]
            ],
        },
        "leads_center": {
            "total_leads": len(meta_leads),
            "by_campaign": dict(sorted(
                {lead.get("campaign_name", "?"): 0 for lead in meta_leads}.items()
            )),
        },
        "crm": {
            "total_deals": crm["total_deals"],
            "total_revenue": crm["total_revenue"],
            "avg_deal": crm["avg_deal"],
            "won": crm["won_deals"],
            "lost": crm["lost_deals"],
            "conversion": crm["conversion_rate"],
            "by_campaign_tag": crm["by_campaign_tag"],
            "by_branch": crm["by_branch"],
        },
        "overall_roi": round((crm["total_revenue"] - total_meta_spend) / total_meta_spend * 100, 1) if total_meta_spend > 0 else 0,
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

def format_fallback(data, data_type):
    """Simple fallback formatting without Claude."""
    if "error" in data:
        return f"❌ {data['error']}"
    return f"📊 Данные получены ({data_type}). Обработка временно недоступна."

# ============================================================
# INTENT DETECTION
# ============================================================
INTENT_PROMPT = """Парсер запросов рекламного/CRM бота. Ответь ТОЛЬКО JSON без markdown:
{"period": "month", "show": "spend", "custom_dates": null}

period: today | yesterday | week | month | 3months | 6months | year | all | custom
show: spend | all_campaigns | crm | roi | ltv | funnel | golden | full_report
custom_dates: null или {"since": "2025-01-01", "until": "2025-01-31"} если пользователь указал конкретные даты/месяц

ВАЖНО — определяй правильно:
- "как дела", "статус", "сводка" → today, spend
- "вчера" → yesterday, spend
- "неделя" → week, spend
- "месяц" → month, spend
- "3 месяца", "квартал" → 3months
- "полгода" → 6months
- "год" → year, spend
- "за январь" → custom с датами января текущего/прошлого года
- "за последние N дней/недель/месяцев" → custom с подсчётом дат
- "все кампании", "список" → all_campaigns
- "crm", "срм", "клиенты", "сделки", "амо" → crm
- "roi", "рои", "окупаемость", "эффективн", "лучшие кампании", "топ" → roi
- "ltv", "лтв", "выручка", "доход" → ltv
- "воронка", "конверсия", "funnel", "потери" → funnel
- "золотые клиенты", "лучшие клиенты", "кто остался", "постоянные", "лояльные", "долгие", "VIP", "вип" → golden
- "полный отчёт", "вся аналитика", "общая картина", "дашборд" → full_report
- "какая кампания лучше", "куда вложить" → roi
- "откуда приходят лучшие" → golden
- по умолчанию → today, spend

Сегодня: """ + get_israel_now().strftime("%Y-%m-%d")

def detect_intent(user_text):
    raw = call_claude(INTENT_PROMPT, user_text, max_tokens=200, retries=2)
    if raw:
        try:
            clean = raw.replace("```json", "").replace("```", "").strip()
            return json.loads(clean)
        except:
            pass

    # Fallback parsing
    text = user_text.lower()
    period = "today"
    show = "spend"

    if any(w in text for w in ["вчера", "yesterday"]):
        period = "yesterday"
    elif any(w in text for w in ["недел", "week", "7 дней"]):
        period = "week"
    elif any(w in text for w in ["месяц", "month", "30 дней"]):
        period = "month"
    elif any(w in text for w in ["3 месяц", "квартал"]):
        period = "3months"
    elif any(w in text for w in ["полгод", "6 месяц"]):
        period = "6months"
    elif any(w in text for w in ["год", "year"]):
        period = "year"

    if any(w in text for w in ["все кампании", "все компании", "список"]):
        show = "all_campaigns"
    elif any(w in text for w in ["золот", "лучшие клиенты", "постоянн", "лояльн", "вип", "vip", "кто остал", "долги"]):
        show = "golden"
    elif any(w in text for w in ["полный отчёт", "полный отчет", "общая картина", "дашборд", "вся аналитика"]):
        show = "full_report"
    elif any(w in text for w in ["crm", "срм", "амо", "amocrm", "сделки", "клиенты"]):
        show = "crm"
    elif any(w in text for w in ["roi", "рои", "окупаемость", "эффективн", "лучш", "топ", "куда вложить"]):
        show = "roi"
    elif any(w in text for w in ["ltv", "лтв", "выручка", "доход"]):
        show = "ltv"
    elif any(w in text for w in ["воронка", "конверси", "funnel", "теряем", "потери"]):
        show = "funnel"

    return {"period": period, "show": show, "custom_dates": None}

# ============================================================
# RESPONSE GENERATION
# ============================================================
ANALYST_PROMPT = """Ты — личный бизнес-аналитик для салона красоты iStudio Beauty Centre (Ришон ле-Цион, Израиль).

ТВОЙ СТИЛЬ:
1. Говори ПРОСТО, как умный друг-маркетолог. Никаких английских аббревиатур без пояснения.
   - Вместо "ROI" говори "окупаемость" или "возврат вложений"
   - Вместо "LTV" говори "ценность клиента за всё время" или "сколько клиент принёс денег"
   - Вместо "CPL" говори "стоимость одного обращения"
   - Вместо "CTR" говори "процент кликов"
   - Вместо "конверсия" можно "процент закрытия в продажу"
2. ЖИВО и с характером. Ты не робот, а умный партнёр.
3. Используй эмодзи для навигации, но не перебарщивай.
4. Давай КОНКРЕТНЫЕ рекомендации с цифрами: "Увеличь бюджет на Карбон ИВР на 30%" а не "рассмотри возможность".
5. Выделяй ЗОЛОТО (что работает) и ПРОБЛЕМЫ (что сливает деньги).
6. НЕ используй Markdown таблицы — только текстовый формат.
7. Если данных мало — скажи честно что нужно больше данных.
8. Валюта расходов Meta — $, выручка amoCRM — ₪.
9. НЕ задавай вопросов в конце ответа.
10. Максимум 3000 символов — коротко но по делу.
11. НИКОГДА не используй **звёздочки**, __подчёркивания__, ## заголовки или другую Markdown-разметку. Только чистый текст и эмодзи. Пиши чистым текстом.
12. Когда в данных есть имена и телефоны клиентов — ОБЯЗАТЕЛЬНО показывай их. Формат: "Имя — телефон — сколько принёс — сколько визитов — откуда пришёл". Это важнейшая информация для владельца.
13. Преобразуй даты из timestamp в человеческий формат (например "15 января 2025").

ПРИМЕР ХОРОШЕГО ОТВЕТА для золотых клиентов:
"🏆 Твоя топ-20 золотых клиентов за полгода:

1. Мария Иванова — +972-50-123-4567
   Принесла ₪9,550 за 21 визит (средний чек ₪455)
   Пришла с кампании: Карбон ИВР
   С нами уже 340 дней

2. Анна Петрова — +972-54-987-6543
   Принесла ₪7,850 за 10 визитов (средний чек ₪785)
   Пришла с кампании: 3 зоны за 999
   С нами 280 дней

💡 Вывод: кампания Карбон ИВР приводит самых лояльных клиентов."

ОРИЕНТИРЫ:
- Хорошая стоимость обращения: $3-5
- Хороший процент закрытия: 15-25%
- Золотой клиент: 3+ визита или ₪3000+ выручки
- Средний чек iStudio: ₪350-500
"""

def generate_response(user_text, data, data_type="spend"):
    # Campaign list — no Claude needed
    if "active_names" in data:
        text = f"📋 Всего: {data['total']}\n🟢 Активных: {data['active_count']} | 🔴 На паузе: {data['paused_count']}\n\n"
        if data["active_names"]:
            for name in data["active_names"]:
                text += f"  🟢 {name}\n"
        else:
            text += "Нет активных кампаний."
        return text

    if "error" in data:
        return f"❌ {data['error']}"

    # Remove internal data before sending to Claude
    clean_data = {k: v for k, v in data.items() if not k.startswith("_")}

    type_labels = {
        "spend": "расходы Meta Ads",
        "crm": "данные CRM (amoCRM)",
        "roi": "окупаемость рекламы (расходы vs выручка)",
        "ltv": "ценность клиентов по источникам",
        "funnel": "воронка продаж",
        "golden": "золотые и постоянные клиенты",
        "full_report": "полная аналитика: Meta + CRM + лиды",
    }

    claude_response = call_claude(
        ANALYST_PROMPT,
        f"Тип данных: {type_labels.get(data_type, data_type)}\n\n"
        f"JSON данные:\n{json.dumps(clean_data, ensure_ascii=False, default=str)}\n\n"
        f"Запрос пользователя: {user_text}",
        max_tokens=3000, retries=2
    )

    if claude_response:
        return claude_response

    # Fallback
    if data_type == "spend":
        return format_report(data)
    return format_fallback(data, data_type)

# ============================================================
# MORNING & WEEKLY REPORTS
# ============================================================
def send_morning_report():
    data = fetch_spend_data("yesterday")
    report = f"🌅 Доброе утро!\n\n" + format_report(data)
    report += f"\n\n💡 Напиши 'покажи за неделю' или 'золотые клиенты' для глубокой аналитики"
    safe_send(MY_CHAT_ID, report)

def send_weekly_crm_report():
    try:
        safe_send(MY_CHAT_ID, "📊 Еженедельный отчёт...\n⏳ Собираю данные из Meta Ads и amoCRM")
        data = full_analytics()
        data["_type"] = "full_report"
        report = generate_response("еженедельный полный отчёт по рекламе и CRM", data, "full_report")
        safe_send(MY_CHAT_ID, report)
    except Exception as e:
        print(f"Weekly report error: {e}")
        safe_send(MY_CHAT_ID, f"❌ Ошибка еженедельного отчёта: {e}")

# ============================================================
# SAFE SEND (split long messages)
# ============================================================
def safe_send(chat_id, text, max_len=4000):
    """Send message, splitting if too long for Telegram."""
    if not text:
        text = "⚠️ Пустой ответ"
    if len(text) <= max_len:
        try:
            bot.send_message(chat_id, text)
        except Exception as e:
            print(f"Send error: {e}")
        return

    # Split by double newline or at max_len
    parts = []
    while text:
        if len(text) <= max_len:
            parts.append(text)
            break
        split_at = text.rfind("\n\n", 0, max_len)
        if split_at == -1:
            split_at = text.rfind("\n", 0, max_len)
        if split_at == -1:
            split_at = max_len
        parts.append(text[:split_at])
        text = text[split_at:].lstrip("\n")

    for part in parts:
        try:
            bot.send_message(chat_id, part)
            time.sleep(0.3)
        except Exception as e:
            print(f"Send error: {e}")

# ============================================================
# FETCH HELPERS
# ============================================================
def fetch_spend_data(period, since=None, until=None):
    if not since or not until:
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
# TELEGRAM HANDLERS
# ============================================================
@bot.message_handler(commands=["start"])
def cmd_start(message):
    if message.chat.id != MY_CHAT_ID:
        return
    safe_send(MY_CHAT_ID,
        "👋 Привет! Я твой личный аналитик рекламы и продаж.\n\n"
        "📊 Что я умею:\n\n"
        "💰 Реклама:\n"
        "• «Как дела?» — расходы за сегодня\n"
        "• «Покажи за неделю/месяц» — сводка\n"
        "• /campaigns — список кампаний\n"
        "• /alerts — проблемы и алерты\n\n"
        "📈 Продажи и клиенты:\n"
        "• /crm — сводка по продажам\n"
        "• /roi — окупаемость рекламы\n"
        "• /funnel — воронка продаж\n\n"
        "⭐ Глубокая аналитика:\n"
        "• /golden — золотые клиенты\n"
        "• /ltv — ценность клиентов\n"
        "• /full — полный отчёт\n\n"
        "📅 Можно за период:\n"
        "• «покажи за январь»\n"
        "• «ROI за последние 3 месяца»\n"
        "• «золотые клиенты за полгода»\n\n"
        "Или просто спрашивай своими словами! 💬"
    )

@bot.message_handler(commands=["today"])
def cmd_today(message):
    if message.chat.id != MY_CHAT_ID:
        return
    safe_send(MY_CHAT_ID, "⏳")
    data = fetch_spend_data("today")
    safe_send(MY_CHAT_ID, generate_response("расходы сегодня", data, "spend"))

@bot.message_handler(commands=["yesterday"])
def cmd_yesterday(message):
    if message.chat.id != MY_CHAT_ID:
        return
    safe_send(MY_CHAT_ID, "⏳")
    data = fetch_spend_data("yesterday")
    safe_send(MY_CHAT_ID, generate_response("расходы вчера", data, "spend"))

@bot.message_handler(commands=["week"])
def cmd_week(message):
    if message.chat.id != MY_CHAT_ID:
        return
    safe_send(MY_CHAT_ID, "⏳")
    data = fetch_spend_data("week")
    safe_send(MY_CHAT_ID, generate_response("расходы за неделю", data, "spend"))

@bot.message_handler(commands=["month"])
def cmd_month(message):
    if message.chat.id != MY_CHAT_ID:
        return
    safe_send(MY_CHAT_ID, "⏳")
    data = fetch_spend_data("month")
    safe_send(MY_CHAT_ID, generate_response("расходы за месяц", data, "spend"))

@bot.message_handler(commands=["campaigns"])
def cmd_campaigns(message):
    if message.chat.id != MY_CHAT_ID:
        return
    safe_send(MY_CHAT_ID, "⏳")
    safe_send(MY_CHAT_ID, generate_response("список кампаний", fetch_all_campaigns_list()))

@bot.message_handler(commands=["alerts"])
def cmd_alerts(message):
    if message.chat.id != MY_CHAT_ID:
        return
    data = fetch_spend_data("today")
    alerts = []
    for c in data["campaigns"]:
        if c["spend"] > 30 and not c["actions"]:
            alerts.append(f"🚨 {c['campaign_name']}: ${c['spend']:.2f} потрачено, 0 результатов!")
        if c["ctr"] < 1.0 and c["spend"] > 10:
            alerts.append(f"⚠️ {c['campaign_name']}: очень низкий процент кликов ({c['ctr']:.2f}%)")
    safe_send(MY_CHAT_ID, "🔔 Проблемы и алерты:\n\n" + "\n".join(alerts) if alerts else "✅ Всё в порядке, проблем нет.")

@bot.message_handler(commands=["report"])
def cmd_report(message):
    if message.chat.id != MY_CHAT_ID:
        return
    send_morning_report()

@bot.message_handler(commands=["crm"])
def cmd_crm(message):
    if message.chat.id != MY_CHAT_ID:
        return
    safe_send(MY_CHAT_ID, "📊 Загружаю данные из amoCRM...\n⏳")
    data = analyze_crm_data()
    data.pop("_deal_details", None)
    safe_send(MY_CHAT_ID, generate_response("сводка по продажам CRM", data, "crm"))

@bot.message_handler(commands=["roi"])
def cmd_roi(message):
    if message.chat.id != MY_CHAT_ID:
        return
    safe_send(MY_CHAT_ID, "📊 Считаю окупаемость рекламы...\n⏳")
    data = analyze_campaign_roi()
    safe_send(MY_CHAT_ID, generate_response("окупаемость рекламы", data, "roi"))

@bot.message_handler(commands=["ltv"])
def cmd_ltv(message):
    if message.chat.id != MY_CHAT_ID:
        return
    safe_send(MY_CHAT_ID, "📊 Анализирую ценность клиентов...\n⏳")
    data = analyze_ltv()
    safe_send(MY_CHAT_ID, generate_response("ценность клиентов по источникам", data, "ltv"))

@bot.message_handler(commands=["funnel"])
def cmd_funnel(message):
    if message.chat.id != MY_CHAT_ID:
        return
    safe_send(MY_CHAT_ID, "📊 Анализирую воронку...\n⏳")
    data = analyze_funnel()
    safe_send(MY_CHAT_ID, generate_response("воронка продаж", data, "funnel"))

@bot.message_handler(commands=["golden"])
def cmd_golden(message):
    if message.chat.id != MY_CHAT_ID:
        return
    safe_send(MY_CHAT_ID, "⭐ Ищу золотых клиентов...\n⏳ Это может занять минуту")
    data = analyze_golden_clients()
    safe_send(MY_CHAT_ID, generate_response("золотые и постоянные клиенты", data, "golden"))

@bot.message_handler(commands=["full"])
def cmd_full(message):
    if message.chat.id != MY_CHAT_ID:
        return
    safe_send(MY_CHAT_ID, "📊 Собираю полный отчёт: Meta Ads + Центр лидов + amoCRM...\n⏳")
    data = full_analytics()
    safe_send(MY_CHAT_ID, generate_response("полный отчёт по рекламе и продажам", data, "full_report"))

# ============================================================
# FREE-TEXT HANDLER
# ============================================================
@bot.message_handler(func=lambda m: m.chat.id == MY_CHAT_ID)
def handle_text(message):
    user_text = message.text.strip()
    safe_send(MY_CHAT_ID, "🤔 Анализирую...")
    intent = detect_intent(user_text)
    print(f"Intent: {intent}")

    show = intent.get("show", "spend")
    period = intent.get("period", "today")
    custom = intent.get("custom_dates")

    # Determine date range
    since, until = None, None
    if custom and isinstance(custom, dict):
        since = custom.get("since")
        until = custom.get("until")
    elif period == "custom":
        since, until = parse_custom_period(user_text)

    if not since or not until:
        since, until = get_date_range(period)

    try:
        if show == "all_campaigns":
            data = fetch_all_campaigns_list()
            safe_send(MY_CHAT_ID, generate_response(user_text, data))
        elif show == "crm":
            data = analyze_crm_data(since, until)
            data.pop("_deal_details", None)
            safe_send(MY_CHAT_ID, generate_response(user_text, data, "crm"))
        elif show == "roi":
            data = analyze_campaign_roi(since, until)
            safe_send(MY_CHAT_ID, generate_response(user_text, data, "roi"))
        elif show == "ltv":
            data = analyze_ltv(since, until)
            safe_send(MY_CHAT_ID, generate_response(user_text, data, "ltv"))
        elif show == "funnel":
            data = analyze_funnel(since, until)
            safe_send(MY_CHAT_ID, generate_response(user_text, data, "funnel"))
        elif show == "golden":
            safe_send(MY_CHAT_ID, "⭐ Ищу золотых клиентов...\n⏳")
            data = analyze_golden_clients(since, until)
            safe_send(MY_CHAT_ID, generate_response(user_text, data, "golden"))
        elif show == "full_report":
            safe_send(MY_CHAT_ID, "📊 Собираю полный отчёт...\n⏳")
            data = full_analytics(since, until)
            safe_send(MY_CHAT_ID, generate_response(user_text, data, "full_report"))
        else:
            data = fetch_spend_data(period, since, until)
            safe_send(MY_CHAT_ID, generate_response(user_text, data, "spend"))
    except Exception as e:
        print(f"Handler error: {e}")
        safe_send(MY_CHAT_ID, f"❌ Ошибка при обработке: {str(e)[:200]}")

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

# ============================================================
# MAIN
# ============================================================
if __name__ == "__main__":
    print("🚀 Bot starting...")
    print(f"📅 Israel time: {get_israel_now().strftime('%Y-%m-%d %H:%M')}")
    print(f"📊 amoCRM: {'✅ configured' if AMOCRM_TOKEN else '❌ no token'}")
    print(f"📊 Meta: {'✅ configured' if META_ACCESS_TOKEN else '❌ no token'}")

    # Clear any stuck webhooks/polling
    bot.delete_webhook(drop_pending_updates=True)
    time.sleep(1)

    scheduler_thread = threading.Thread(target=run_scheduler, daemon=True)
    scheduler_thread.start()
    print("⏰ Daily 08:00 | Weekly: Sunday 09:00")
    print("📱 Polling...")
    bot.infinity_polling(timeout=60, long_polling_timeout=60)
