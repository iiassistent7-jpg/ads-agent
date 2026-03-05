import os
import time
import json
import requests
import threading
import schedule
import re
import tempfile
from datetime import datetime, timedelta
from collections import defaultdict
import telebot
import anthropic
from openai import OpenAI

# ============================================================
# CONFIGURATION
# ============================================================
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "")
MY_CHAT_ID = int(os.environ.get("MY_CHAT_ID", "0"))
META_AD_ACCOUNT = os.environ.get("META_AD_ACCOUNT", "")
META_ACCESS_TOKEN = os.environ.get("META_ACCESS_TOKEN", "")
ANTHROPIC_KEY = os.environ.get("ANTHROPIC_KEY", "")
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "")

# amoCRM config
AMOCRM_DOMAIN = os.environ.get("AMOCRM_DOMAIN", "istudiomkac.amocrm.ru")
AMOCRM_TOKEN = os.environ.get("AMOCRM_TOKEN", "")

ISRAEL_UTC_OFFSET = 2

bot = telebot.TeleBot(TELEGRAM_TOKEN)
claude = anthropic.Anthropic(api_key=ANTHROPIC_KEY)
openai_client = OpenAI(api_key=OPENAI_API_KEY) if OPENAI_API_KEY else None

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
    from datetime import timezone
    return datetime.now(timezone.utc).replace(tzinfo=None) + timedelta(hours=ISRAEL_UTC_OFFSET)

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

def get_previous_period(since, until):
    """Calculate the previous period of the same length."""
    try:
        from_date = datetime.strptime(since, "%Y-%m-%d").date()
        to_date = datetime.strptime(until, "%Y-%m-%d").date()
        period_days = (to_date - from_date).days + 1
        prev_until = from_date - timedelta(days=1)
        prev_since = prev_until - timedelta(days=period_days - 1)
        return str(prev_since), str(prev_until)
    except Exception as e:
        print(f"get_previous_period error: {e}")
        return None, None

def calc_delta(current, previous, inverse=False):
    """
    Calculate % delta between current and previous values.
    inverse=True means that growth is BAD (e.g. CPL, CAC — lower is better).
    Returns dict: {pct, direction, color}
    """
    try:
        cur = float(current or 0)
        prv = float(previous or 0)
        if prv == 0:
            return None
        pct = round((cur - prv) / prv * 100, 1)
        if pct == 0:
            return {"pct": 0, "direction": "flat", "color": "#6b6b80", "symbol": "→"}
        
        growing = pct > 0
        # For inverse metrics (CPL, CAC): growth = bad, decline = good
        if inverse:
            good = not growing
        else:
            good = growing

        color = "#22c55e" if good else "#ef4444"
        symbol = "▲" if growing else "▼"
        return {
            "pct": abs(pct),
            "direction": "up" if growing else "down",
            "color": color,
            "symbol": symbol,
            "prev_value": prv,
        }
    except Exception as e:
        print(f"calc_delta error: {e}")
        return None

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
        time.sleep(0.5)

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
def amocrm_request(endpoint, params=None, method="GET"):
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
        elif resp.status_code == 403:
            print(f"amoCRM 403 scope error on [{endpoint[:80]}] — skipping")
            return None  # graceful skip, not an error
        else:
            print(f"amoCRM error {resp.status_code} on [{endpoint[:80]}]: {resp.text[:150]}")
            return None
    except Exception as e:
        print(f"amoCRM request error: {e}")
        return None

def get_amocrm_pipelines():
    data = amocrm_request("leads/pipelines")
    if not data:
        return []
    pipelines = []
    for p in (data.get("_embedded") or {}).get("pipelines") or []:
        stages = []
        for s in (p.get("_embedded") or {}).get("statuses") or []:
            stages.append({
                "id": s.get("id"),
                "name": s.get("name"),
                "sort": s.get("sort", 0),
                "is_closed": s.get("type", 0) in [0, 1],
            })
        stages.sort(key=lambda x: x["sort"])
        pipelines.append({
            "id": p.get("id"),
            "name": p.get("name"),
            "stages": stages,
        })
    return pipelines

def get_all_amocrm_deals(max_pages=30, date_filter=None):
    all_deals = []
    for page in range(1, max_pages + 1):
        params = {"limit": 250, "page": page, "with": "contacts"}
        if date_filter:
            params["filter[created_at][from]"] = date_filter.get("from", 0)
            params["filter[created_at][to]"] = date_filter.get("to", 0)
        data = amocrm_request("leads", params)
        if not data:
            break
        deals = (data.get("_embedded") or {}).get("leads") or []
        if not deals:
            break
        all_deals.extend(deals)
        if len(deals) < 250:
            break
        time.sleep(0.3)
    return all_deals

def get_amocrm_contacts(contact_ids):
    contacts = {}
    if not contact_ids:
        return contacts
    unique_ids = list(set(contact_ids))
    batch_size = 50
    for i in range(0, len(unique_ids), batch_size):
        batch = unique_ids[i:i + batch_size]
        filter_str = "&".join([f"filter[id][]={cid}" for cid in batch])
        data = amocrm_request(f"contacts?{filter_str}")
        if data:
            for c in (data.get("_embedded") or {}).get("contacts") or []:
                name = c.get("name", "Без имени")
                phone = ""
                email = ""
                for cf in (c.get("custom_fields_values") or []):
                    field_code = cf.get("field_code", "")
                    values = cf.get("values") or []
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

def _parse_contact_from_amocrm(c, query_fallback=""):
    """Extract full contact info from amoCRM contact object."""
    phones, email, custom_fields = [], "", {}
    for cf in (c.get("custom_fields_values") or []):
        code  = cf.get("field_code", "")
        fname = cf.get("field_name", "")
        vals  = cf.get("values") or []
        if not vals: continue
        if code == "PHONE":
            for v in vals:
                p = v.get("value","")
                if p: phones.append(p)
        elif code == "EMAIL":
            email = vals[0].get("value","")
        else:
            val = vals[0].get("value")
            key = fname or code
            if key and val is not None:
                custom_fields[key] = val
    lead_ids = [lnk["id"] for lnk in ((c.get("_embedded") or {}).get("leads") or [])]
    return {
        "id": c["id"],
        "name": c.get("name", "Без имени"),
        "phone": phones[0] if phones else query_fallback,
        "all_phones": phones,
        "email": email,
        "lead_ids": lead_ids,
        "custom_fields": custom_fields,
    }

def find_contact_by_name(name):
    """Search amoCRM contact by name. Returns list of matches."""
    data = amocrm_request(f"contacts?query={name}&with=leads")
    if not data:
        return []
    contacts = (data.get("_embedded") or {}).get("contacts") or []
    return [_parse_contact_from_amocrm(c) for c in contacts[:5]]

def find_contact_by_phone(phone):
    """Search amoCRM contact by phone. Returns full contact dict with all custom fields."""
    import re
    digits = re.sub(r"[^\d]", "", phone.strip())

    variants = set()
    variants.add(digits)
    if digits.startswith("972") and len(digits) == 12:
        variants.add("0" + digits[3:])
        variants.add("+" + digits)
        variants.add(digits[3:])
    elif digits.startswith("0") and len(digits) == 10:
        intl = "972" + digits[1:]
        variants.add("+" + intl)
        variants.add(intl)
        variants.add(digits[1:])
    elif len(digits) == 9:
        variants.add("0" + digits)
        variants.add("972" + digits)
        variants.add("+972" + digits)

    for q in variants:
        data = amocrm_request(f"contacts?query={q}&with=leads,customers")
        if data:
            contacts = (data.get("_embedded") or {}).get("contacts") or []
            if contacts:
                return _parse_contact_from_amocrm(contacts[0], phone)
        time.sleep(0.15)
    return None

def find_client(query):
    """
    Universal client search — auto-detects phone vs name.
    Returns (contact, candidates_list) where candidates_list is non-empty
    only if multiple name matches found.
    """
    import re
    # Detect if it looks like a phone number
    digits = re.sub(r"[^\d]", "", query)
    is_phone = len(digits) >= 7 and len(digits) <= 13

    if is_phone:
        contact = find_contact_by_phone(query)
        return contact, []
    else:
        candidates = find_contact_by_name(query)
        if len(candidates) == 1:
            return candidates[0], []
        elif len(candidates) > 1:
            return None, candidates  # ambiguous — show list
        else:
            return None, []


def get_deal_notes(deal_id):
    """Fetch all notes/events for a deal including WhatsApp & Instagram DM messages."""
    notes = []
    # Fetch both deal notes and contact notes (messages often attach to contact)
    data = amocrm_request(f"leads/{deal_id}/notes?limit=250")
    if not data:
        return notes

    from datetime import datetime as _dt

    # Note types in amoCRM integrations:
    # "common"            — manual manager note
    # "call_in/call_out"  — phone call
    # "service_message"   — system event (stage change etc.)
    # "amocrm_bot"        — chatbot message
    # "sms"               — SMS
    # Wazzup/WhatsApp integration stores as note_type "common" with direction field
    # OR as custom type "wazzup" / "whatsapp" / "instagram_direct"
    # Some integrations use params.income (true=incoming from client)

    TYPE_LABELS = {
        "common": "📝 Заметка",
        "call_in": "📞 Входящий звонок",
        "call_out": "📞 Исходящий звонок",
        "service_message": "⚙️ Система",
        "amocrm_bot": "🤖 Бот",
        "sms": "💬 SMS",
        "wazzup": "💬 WhatsApp",
        "whatsapp": "💬 WhatsApp",
        "instagram_direct": "📸 Instagram DM",
        "telegram": "✈️ Telegram",
    }

    for n in (data.get("_embedded") or {}).get("notes") or []:
        note_type = n.get("note_type", "")
        params = n.get("params") or {}
        created = n.get("created_at", 0)
        dt_str = _dt.fromtimestamp(created).strftime("%d.%m.%Y %H:%M") if created else ""

        # Extract text from all possible fields
        text = (
            params.get("text") or
            params.get("message") or
            params.get("body") or
            params.get("content") or
            ""
        )

        # Direction: who wrote — client or manager
        income = params.get("income")  # True = incoming (from client)
        if income is True:
            direction = "👤 Клиент"
        elif income is False:
            direction = "💼 Менеджер"
        else:
            direction = ""

        # Skip empty service messages and stage-change noise
        if not text and note_type == "service_message":
            continue

        if text or note_type in ("call_in", "call_out"):
            label = TYPE_LABELS.get(note_type, f"[{note_type}]")
            notes.append({
                "type": note_type,
                "label": label,
                "direction": direction,
                "text": text[:800] if text else "(звонок)",
                "date": dt_str,
                "raw": n,
            })

    # Sort chronologically
    notes.sort(key=lambda x: x["raw"].get("created_at", 0))
    # Remove raw field before returning
    for n in notes:
        n.pop("raw", None)

    return notes

def get_contact_notes(contact_id):
    """Fetch notes attached directly to the contact (WhatsApp/Instagram often here)."""
    notes = []
    data = amocrm_request(f"contacts/{contact_id}/notes?limit=250")
    if not data:
        return notes

    from datetime import datetime as _dt
    TYPE_LABELS = {
        "common": "📝 Заметка", "call_in": "📞 Входящий", "call_out": "📞 Исходящий",
        "service_message": "⚙️ Система", "amocrm_bot": "🤖 Бот", "sms": "💬 SMS",
        "wazzup": "💬 WhatsApp", "whatsapp": "💬 WhatsApp",
        "instagram_direct": "📸 Instagram DM", "telegram": "✈️ Telegram",
    }

    for n in (data.get("_embedded") or {}).get("notes") or []:
        note_type = n.get("note_type", "")
        params = n.get("params") or {}
        created = n.get("created_at", 0)
        dt_str = _dt.fromtimestamp(created).strftime("%d.%m.%Y %H:%M") if created else ""
        text = params.get("text") or params.get("message") or params.get("body") or params.get("content") or ""
        income = params.get("income")
        direction = "👤 Клиент" if income is True else ("💼 Менеджер" if income is False else "")
        if not text and note_type == "service_message":
            continue
        if text or note_type in ("call_in", "call_out"):
            label = TYPE_LABELS.get(note_type, f"[{note_type}]")
            notes.append({
                "type": note_type, "label": label, "direction": direction,
                "text": text[:800] if text else "(звонок)", "date": dt_str,
                "ts": created,
            })

    notes.sort(key=lambda x: x.get("ts", 0))
    for n in notes: n.pop("ts", None)
    return notes
    """Fetch full deal details including custom fields."""
    data = amocrm_request(f"leads/{deal_id}?with=contacts,tags")
    if not data:
        return {}
    cf = {}
    for field in (data.get("custom_fields_values") or []):
        fname = field.get("field_name", "")
        vals = field.get("values") or []
        if fname and vals:
            val = vals[0].get("value")
            if isinstance(val, int) and val > 1000000000:
                from datetime import datetime as _dt
                try: val = _dt.fromtimestamp(val).strftime("%d.%m.%Y %H:%M")
                except: pass
            cf[fname] = val
    tags = [t.get("name","") for t in (data.get("_embedded") or {}).get("tags") or []]
    return {
        "id": deal_id,
        "name": data.get("name",""),
        "price": data.get("price", 0),
        "status_id": data.get("status_id"),
        "pipeline_id": data.get("pipeline_id"),
        "created_at": data.get("created_at", 0),
        "closed_at": data.get("closed_at", 0),
        "custom_fields": cf,
        "tags": tags,
    }

def get_contact_conversations(contact_id, lead_ids=None):
    """
    Fetch chat messages from amoCRM via all possible Wazzup/integration endpoints.
    Wazzup stores messages as notes with specific note_types OR via /talks API.
    """
    from datetime import datetime as _dt
    messages = []

    # 1. Try /talks API (amoCRM native chats, newer API)
    talks_data = amocrm_request(f"talks?contact_id={contact_id}&limit=100")
    if talks_data:
        for talk in (talks_data.get("_embedded") or {}).get("talks") or []:
            talk_id = talk.get("id")
            # Fetch messages within each talk
            msgs_data = amocrm_request(f"talks/{talk_id}/messages?limit=200")
            if msgs_data:
                for m in (msgs_data.get("_embedded") or {}).get("messages") or []:
                    created = m.get("created_at", 0)
                    text = m.get("text") or m.get("content") or ""
                    author_type = m.get("author", {}).get("type", "")
                    direction = "👤 Клиент" if author_type == "contact" else "💼 Менеджер"
                    channel = talk.get("channel_type", "chat")
                    if text:
                        messages.append({
                            "date": _dt.fromtimestamp(created).strftime("%d.%m.%Y %H:%M") if created else "",
                            "direction": direction,
                            "channel": channel,
                            "text": text[:600],
                            "ts": created,
                        })

    # 2. Try contact notes — Wazzup often stores as note_type containing "wazzup"/"whatsapp"/"instagram"
    notes_data = amocrm_request(f"contacts/{contact_id}/notes?limit=500")
    if notes_data:
        for n in (notes_data.get("_embedded") or {}).get("notes") or []:
            nt = n.get("note_type", "")
            params = n.get("params") or {}
            created = n.get("created_at", 0)
            # Message content in various fields
            text = (params.get("text") or params.get("message") or
                    params.get("body") or params.get("content") or "")
            income = params.get("income")
            direction = "👤 Клиент" if income is True else ("💼 Менеджер" if income is False else "")

            # Channel detection
            nt_low = nt.lower()
            if "whatsapp" in nt_low or "wazzup" in nt_low:
                channel = "WhatsApp"
            elif "instagram" in nt_low:
                channel = "Instagram"
            elif "facebook" in nt_low or "messenger" in nt_low:
                channel = "Facebook"
            elif "telegram" in nt_low:
                channel = "Telegram"
            elif nt in ("common", "amocrm_bot"):
                channel = "Заметка"
            else:
                channel = nt or "chat"

            if text and created:
                messages.append({
                    "date": _dt.fromtimestamp(created).strftime("%d.%m.%Y %H:%M"),
                    "direction": direction,
                    "channel": channel,
                    "text": text[:600],
                    "ts": created,
                })

    # 3. Also check lead-level notes for each recent deal
    if lead_ids:
        for lid in lead_ids[:5]:  # only last 5 deals
            ln_data = amocrm_request(f"leads/{lid}/notes?limit=100")
            if ln_data:
                for n in (ln_data.get("_embedded") or {}).get("notes") or []:
                    nt = n.get("note_type", "")
                    params = n.get("params") or {}
                    created = n.get("created_at", 0)
                    text = (params.get("text") or params.get("message") or
                            params.get("body") or params.get("content") or "")
                    income = params.get("income")
                    direction = "👤 Клиент" if income is True else ("💼 Менеджер" if income is False else "")
                    nt_low = nt.lower()
                    if "whatsapp" in nt_low or "wazzup" in nt_low: channel = "WhatsApp"
                    elif "instagram" in nt_low: channel = "Instagram"
                    elif "facebook" in nt_low: channel = "Facebook"
                    elif nt in ("common",): channel = "Заметка"
                    else: channel = nt or "chat"
                    if text and created:
                        messages.append({
                            "date": _dt.fromtimestamp(created).strftime("%d.%m.%Y %H:%M"),
                            "direction": direction,
                            "channel": channel,
                            "text": text[:600],
                            "ts": created,
                        })

    # Deduplicate by (ts, text) and sort chronologically
    seen = set()
    unique = []
    for m in messages:
        key = (m.get("ts"), m.get("text","")[:50])
        if key not in seen:
            seen.add(key)
            unique.append(m)

    unique.sort(key=lambda x: x.get("ts", 0))
    for m in unique: m.pop("ts", None)

    print(f"Conversations for contact {contact_id}: {len(unique)} messages total")
    return unique


    """Fetch full deal details including custom fields."""
    data = amocrm_request(f"leads/{deal_id}?with=contacts,tags")
    if not data:
        return {}
    cf = {}
    for field in (data.get("custom_fields_values") or []):
        fname = field.get("field_name", "")
        vals = field.get("values") or []
        if fname and vals:
            val = vals[0].get("value")
            if isinstance(val, int) and val > 1000000000:
                from datetime import datetime as _dt
                try: val = _dt.fromtimestamp(val).strftime("%d.%m.%Y %H:%M")
                except: pass
            cf[fname] = val
    tags = [t.get("name","") for t in (data.get("_embedded") or {}).get("tags") or []]
    return {
        "id": deal_id,
        "name": data.get("name", ""),
        "price": data.get("price", 0),
        "status_id": data.get("status_id"),
        "pipeline_id": data.get("pipeline_id"),
        "created_at": data.get("created_at", 0),
        "closed_at": data.get("closed_at", 0),
        "custom_fields": cf,
        "tags": tags,
    }

def get_deal_full(deal_id):
    """Fetch full deal details including custom fields."""
    data = amocrm_request(f"leads/{deal_id}?with=contacts,tags")
    if not data:
        return {}
    cf = {}
    for field in (data.get("custom_fields_values") or []):
        fname = field.get("field_name", "")
        vals = field.get("values") or []
        if fname and vals:
            val = vals[0].get("value")
            if isinstance(val, int) and val > 1000000000:
                from datetime import datetime as _dt
                try: val = _dt.fromtimestamp(val).strftime("%d.%m.%Y %H:%M")
                except: pass
            cf[fname] = val
    tags = [t.get("name","") for t in (data.get("_embedded") or {}).get("tags") or []]
    return {
        "id": deal_id,
        "name": data.get("name", ""),
        "price": data.get("price", 0),
        "status_id": data.get("status_id"),
        "pipeline_id": data.get("pipeline_id"),
        "created_at": data.get("created_at", 0),
        "closed_at": data.get("closed_at", 0),
        "custom_fields": cf,
        "tags": tags,
    }

def analyze_client(query):
    """Full client profile by phone OR name."""
    contact, candidates = find_client(query)

    if candidates:
        # Multiple name matches — return list for user to pick
        lines = [f"🔍 Найдено несколько клиентов по запросу «{query}»:\n"]
        for i, c in enumerate(candidates, 1):
            phones = ", ".join(c.get("all_phones", [])) or "нет телефона"
            cf = c.get("custom_fields", {})
            status = cf.get("Статус пациента") or cf.get("Статус клиента") or ""
            visits = cf.get("Количество процедур") or ""
            lines.append(f"{i}. {c['name']} | {phones}"
                         + (f" | {status}" if status else "")
                         + (f" | {visits} процедур" if visits else ""))
        lines.append("\nУточни номер телефона для точного анализа.")
        return {"error": "\n".join(lines)}

    if not contact:
        return {"error": f"Клиент «{query}» не найден в amoCRM"}

    lead_ids = contact.get("lead_ids", [])

    card = contact.get("custom_fields", {})
    card_visits = card.get("Количество процедур") or card.get("Кол-во процедур") or card.get("Visits")
    card_status = card.get("Статус пациента") or card.get("Статус клиента") or card.get("Status")
    card_gender = card.get("Пол") or card.get("Gender")

    print(f"Client {contact['name']} has {len(lead_ids)} deals total")

    deals_summary = []
    for lid in lead_ids[:60]:
        d = get_deal_full(lid)
        if d:
            deals_summary.append(d)
        time.sleep(0.15)

    recent_ids = lead_ids[:15]
    deals_detailed = []
    for lid in recent_ids:
        d = next((x for x in deals_summary if x["id"] == lid), get_deal_full(lid))
        if d:
            notes = get_deal_notes(lid)
            d["notes"] = notes
            deals_detailed.append(d)

    total_spent  = sum(d.get("price", 0) for d in deals_summary if d.get("price") and d.get("status_id") in (52041937, 70503946))
    total_deals  = len(deals_summary)
    won_deals    = sum(1 for d in deals_summary if d.get("status_id") in (52041937, 70503946))

    contact_notes = get_contact_conversations(contact["id"], lead_ids=lead_ids[:5])

    return {
        "contact": contact,
        "card_visits": card_visits,
        "card_status": card_status,
        "card_gender": card_gender,
        "deals": deals_detailed,
        "all_deals_count": total_deals,
        "won_deals_count": won_deals,
        "contact_notes": contact_notes,
        "total_deals": total_deals,
        "total_spent": total_spent,
    }

# Keep old name as alias for backwards compat
def analyze_client_by_phone(phone):
    return analyze_client(phone)

def format_client_profile(data, stage_map=None):
    """Send client data to Claude for psychological profile + recommendations."""
    if "error" in data:
        return f"❌ {data['error']}"

    contact = data["contact"]
    deals = data["deals"]
    card_visits = data.get("card_visits")
    card_status = data.get("card_status")
    card_gender = data.get("card_gender")
    all_phones  = contact.get("all_phones", [contact.get("phone","")])

    # Card-level extra info
    card_info = ""
    if card_visits: card_info += f"Процедур (из карточки): {card_visits}\n"
    if card_status: card_info += f"Статус: {card_status}\n"
    if card_gender: card_info += f"Пол: {card_gender}\n"
    extra_cf = {k:v for k,v in contact.get("custom_fields",{}).items()
                if k not in ("Количество процедур","Кол-во процедур","Visits",
                             "Статус пациента","Статус клиента","Status","Пол","Gender")}
    for k,v in extra_cf.items():
        card_info += f"{k}: {v}\n"

    deals_text = ""
    for i, d in enumerate(deals, 1):
        cf_str = "\n".join(f"    {k}: {v}" for k, v in d.get("custom_fields", {}).items())

        # Format notes with direction labels
        notes = d.get("notes", [])
        if notes:
            notes_str = ""
            for n in notes:
                direction = f" [{n['direction']}]" if n.get("direction") else ""
                notes_str += f"    [{n['date']}] {n['label']}{direction}: {n['text']}\n"
        else:
            notes_str = "    — заметок нет"

        from datetime import datetime as _dt
        created_str = _dt.fromtimestamp(d["created_at"]).strftime("%d.%m.%Y") if d.get("created_at") else "?"

        deals_text += f"""
Сделка {i} (создана {created_str}):
  Цена: ₪{d.get('price', 0)}
  Теги: {', '.join(d.get('tags', [])) or '—'}
  Доп.поля:
{cf_str or '    —'}
  История переписки и заметки:
{notes_str}"""

    contact_notes = data.get("contact_notes", [])
    contact_chat = ""
    if contact_notes:
        # Group by channel for summary
        by_channel = {}
        for n in contact_notes:
            ch = n.get("channel", "chat")
            by_channel[ch] = by_channel.get(ch, 0) + 1

        ch_summary = ", ".join(f"{ch}: {cnt}" for ch, cnt in by_channel.items())
        contact_chat = f"\nПЕРЕПИСКА ({ch_summary}):\n"

        # Show last 40 messages max to keep prompt manageable
        for n in contact_notes[-40:]:
            direction = f" [{n['direction']}]" if n.get("direction") else ""
            channel = f"[{n['channel']}]" if n.get("channel") else ""
            contact_chat += f"  {n['date']} {channel}{direction}: {n['text']}\n"

    prompt = f"""ДАННЫЕ КЛИЕНТА iStudio:
Имя: {contact['name']}
Телефоны: {', '.join(all_phones)}
Всего сделок в CRM: {data['all_deals_count']} (из них выполнено: {data['won_deals_count']})
Общая выручка (только выполненные): ₪{data['total_spent']}

ДАННЫЕ ИЗ КАРТОЧКИ КОНТАКТА:
{card_info or '— не заполнено'}
{contact_chat}
ПОСЛЕДНИЕ СДЕЛКИ (последние {len(deals)} из {data['all_deals_count']}):
{deals_text}

---
Сделай глубокий анализ клиента по разделам. Опирайся ТОЛЬКО на реальные данные выше — не выдумывай.

1. ПРОФИЛЬ
Кто клиент: язык, процедуры, сколько раз приходил, сколько потратил.

2. ПОВЕДЕНИЕ В ВОРОНКЕ
Как быстро движется от заявки до записи? Нужно ли его "дожимать"? Сколько касаний было до закрытия?

3. ПСИХОЛОГИЧЕСКИЙ ПОРТРЕТ (по переписке и заметкам)
Что важно для этого клиента — цена, результат, доверие, скорость? Как он общается — коротко/подробно, активно/пассивно? Есть ли возражения, сомнения? Тональность переписки.

4. ИСТОРИЯ
Хронология: когда пришёл первый раз, что делал, динамика отношений.

5. РЕКОМЕНДАЦИИ МЕНЕДЖЕРУ
Конкретно: как закрыть следующую сделку с этим клиентом, что предложить, чего избегать, когда лучше написать.

ВАЖНЫЙ КОНТЕКСТ ПРО ЦЕНЫ В СДЕЛКАХ:
- Сделки с ценой ₪0 — это НЕ бесплатные визиты. Причины: повторная процедура по пакету (оплачена в первой сделке), или цена не проставлена менеджером.
- Реальное число визитов = поле "Количество процедур" в карточке контакта (если есть), НЕ количество сделок.
- Не пиши "клиент потратил ₪X" если видишь много нулей — лучше напиши "выручка по заполненным сделкам: ₪X, реально процедур: Y".

    try:
        response = call_claude(ANALYST_PROMPT, prompt, max_tokens=2000)
        # Count messages
        total_msgs = len(contact_notes)
        by_channel = {}
        for n in contact_notes:
            ch = n.get("channel", "?")
            by_channel[ch] = by_channel.get(ch, 0) + 1
        ch_str = ", ".join(f"{ch}: {cnt}" for ch, cnt in by_channel.items()) if by_channel else "0"

        visits_str = f" | Процедур: {card_visits}" if card_visits else f" | Выполнено: {data['won_deals_count']}"
        status_str = f" | {card_status}" if card_status else ""
        phones_str = " / ".join(all_phones[:2])
        header = (
            f"👤 {contact['name']}{status_str}\n"
            f"📱 {phones_str}\n"
            f"💰 Выручка: ₪{data['total_spent']}{visits_str}\n"
            f"📋 Всего сделок: {data['all_deals_count']}\n"
            f"💬 Сообщений: {ch_str}\n"
            f"{'—'*30}\n\n"
        )
        return header + response
    except Exception as e:
        return f"❌ Ошибка анализа: {e}"


    tags = (deal.get("_embedded") or {}).get("tags") or []
    return [t.get("name", "") for t in tags]

def extract_fb_tag(tags):
    for tag in tags:
        if tag.startswith("fb") and len(tag) > 3:
            return tag.rstrip("!")
    return None

def parse_campaign_tag(tags):
    branches = {"Ришон", "Хайфа", "Тель-Авив", "Ашдод", "Раат"}
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
    known_branches = ["Ришон", "Хайфа", "Тель-Авив", "Ашдод", "Раат"]
    for tag in tags:
        for b in known_branches:
            if b.lower() in tag.lower():
                return b
    return "Не указан"

def should_filter_branch(branch, since=None, until=None):
    if branch in ["Ашдод", "Раат"]:
        return False
    if not since or not until:
        return True
    try:
        from_date = datetime.strptime(since, "%Y-%m-%d").date()
        to_date = datetime.strptime(until, "%Y-%m-%d").date()
        period_days = (to_date - from_date).days
    except:
        return True
    if period_days > 365:
        return True
    if branch in ["Ришон", "Не указан"]:
        return True
    return False

# ============================================================
# DEEP ANALYTICS
# ============================================================
def analyze_crm_data(since=None, until=None):
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

    # === iStudio amoCRM stage IDs (verified via /debug) ===
    # Рабочая воронка (5896168):
    #   52041937 = "Пробная процедура выполнена" ← ВЫРУЧКА
    #   143      = "Закрыто и не реализовано"    ← ПОТЕРЯ
    # Постоянные клиенты (8703286):
    #   70503946 = "Процедура выполнена"          ← ВЫРУЧКА
    #   143      = "Закрыто и не реализовано"    ← ПОТЕРЯ
    # Архив (5891302) / Догрев (5896195) — не считаем
    KNOWN_WON_IDS  = {52041937, 70503946}
    KNOWN_LOST_IDS = {143}

    for p in pipelines:
        for s in p["stages"]:
            stage_map[s["id"]] = s["name"]
            sid = s["id"]
            name_lower = s["name"].lower()

            if sid in KNOWN_WON_IDS:
                closed_won_ids.add(sid)
                print(f"WON: '{s['name']}' id={sid}")
            elif sid in KNOWN_LOST_IDS:
                closed_lost_ids.add(sid)
                print(f"LOST: '{s['name']}' id={sid}")
            else:
                # Fallback for any future new stages
                if "не реализовано" in name_lower or "закрыто и не" in name_lower:
                    closed_lost_ids.add(sid)
                    print(f"LOST (name): '{s['name']}' id={sid}")
                elif name_lower in ("процедура выполнена", "пробная процедура выполнена",
                                    "успешно реализовано"):
                    closed_won_ids.add(sid)
                    print(f"WON (name): '{s['name']}' id={sid}")
                else:
                    print(f"OTHER: '{s['name']}' id={sid}")

    print(f"WON ids={closed_won_ids} | LOST ids={closed_lost_ids}")

    total_revenue = 0
    total_deals = 0
    filtered_out = 0
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

        if not should_filter_branch(branch, since, until):
            filtered_out += 1
            continue

        total_deals += 1
        # Revenue only from successfully closed deals
        if stage_id in closed_won_ids:
            total_revenue += price
            if price > 0:
                deals_with_revenue += 1

        by_stage[stage_name] = by_stage.get(stage_name, 0) + 1

        if branch not in by_branch:
            by_branch[branch] = {"deals": 0, "revenue": 0, "won": 0, "lost": 0}
        by_branch[branch]["deals"] += 1

        if stage_id in closed_won_ids:
            by_branch[branch]["won"] += 1
            by_branch[branch]["revenue"] += price
            won_deals.append(deal)
        elif stage_id in closed_lost_ids:
            by_branch[branch]["lost"] += 1
            lost_deals += 1

        if fb_tag:
            if fb_tag not in by_source:
                by_source[fb_tag] = {"deals": 0, "revenue": 0, "with_revenue": 0, "won": 0, "lost": 0}
            by_source[fb_tag]["deals"] += 1
            if stage_id in closed_won_ids:
                by_source[fb_tag]["won"] += 1
                by_source[fb_tag]["revenue"] += price
                if price > 0:
                    by_source[fb_tag]["with_revenue"] += 1
            elif stage_id in closed_lost_ids:
                by_source[fb_tag]["lost"] += 1

        if campaign_info:
            tag_key = campaign_info["raw"]
            if tag_key not in by_campaign_tag:
                by_campaign_tag[tag_key] = {"deals": 0, "revenue": 0, "with_revenue": 0, "won": 0, "lost": 0, "prices": [], "stages": {}}
            by_campaign_tag[tag_key]["deals"] += 1
            by_campaign_tag[tag_key]["stages"][stage_name] = by_campaign_tag[tag_key]["stages"].get(stage_name, 0) + 1
            if stage_id in closed_won_ids:
                by_campaign_tag[tag_key]["won"] += 1
                by_campaign_tag[tag_key]["revenue"] += price
                if price > 0:
                    by_campaign_tag[tag_key]["with_revenue"] += 1
                    by_campaign_tag[tag_key]["prices"].append(price)
            elif stage_id in closed_lost_ids:
                by_campaign_tag[tag_key]["lost"] += 1

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
            "contact_ids": [c["id"] for c in (deal.get("_embedded") or {}).get("contacts") or []],
        }
        all_deal_details.append(deal_info)

    for tag_key in by_campaign_tag:
        prices = by_campaign_tag[tag_key].pop("prices", [])
        if prices:
            by_campaign_tag[tag_key]["avg_deal"] = round(sum(prices) / len(prices), 0)
            by_campaign_tag[tag_key]["max_deal"] = max(prices)
            by_campaign_tag[tag_key]["min_deal"] = min(prices)
        else:
            by_campaign_tag[tag_key]["avg_deal"] = 0

    PIPELINE_WORKING = 5896168
    PIPELINE_PERMANENT = 8703286

    working_funnel = {"total": 0, "stages": {}, "revenue": 0, "won": 0, "lost": 0}
    permanent_funnel = {"total": 0, "stages": {}, "revenue": 0, "won": 0, "lost": 0}

    for deal in deals:
        tags = get_deal_tags(deal)
        branch = get_deal_branch(tags)
        if not should_filter_branch(branch, since, until):
            continue
        pid = deal.get("pipeline_id", 0)
        price = deal.get("price", 0) or 0
        stage_id = deal.get("status_id", 0)
        stage_name = stage_map.get(stage_id, f"Stage {stage_id}")

        if pid == PIPELINE_WORKING:
            working_funnel["total"] += 1
            working_funnel["stages"][stage_name] = working_funnel["stages"].get(stage_name, 0) + 1
            if stage_id in closed_won_ids:
                working_funnel["won"] += 1
                working_funnel["revenue"] += price
            elif stage_id in closed_lost_ids:
                working_funnel["lost"] += 1
        elif pid == PIPELINE_PERMANENT:
            permanent_funnel["total"] += 1
            permanent_funnel["stages"][stage_name] = permanent_funnel["stages"].get(stage_name, 0) + 1
            if stage_id in closed_won_ids:
                permanent_funnel["won"] += 1
                permanent_funnel["revenue"] += price
            elif stage_id in closed_lost_ids:
                permanent_funnel["lost"] += 1

    sorted_campaigns = sorted(by_campaign_tag.items(), key=lambda x: x[1]["revenue"], reverse=True)
    print(f"REVENUE DEBUG: total_deals={total_deals}, won={len(won_deals)}, total_revenue={total_revenue}, filtered_out={filtered_out}")

    return {
        "total_deals": total_deals,
        "filtered_out_deals": filtered_out,
        "branch_filter": "Только Ришон" if since and until and (datetime.strptime(until, "%Y-%m-%d").date() - datetime.strptime(since, "%Y-%m-%d").date()).days <= 365 else "Все кроме Ашдода",
        "total_revenue": total_revenue,
        "deals_with_revenue": deals_with_revenue,
        "avg_deal": round(total_revenue / deals_with_revenue, 2) if deals_with_revenue > 0 else 0,
        "won_deals": len(won_deals),
        "lost_deals": lost_deals,
        "conversion_rate": round(len(won_deals) / total_deals * 100, 1) if total_deals > 0 else 0,
        "by_stage": by_stage,
        "working_funnel": working_funnel,
        "permanent_funnel": permanent_funnel,
        "by_branch": by_branch,
        "by_source": dict(list(sorted(by_source.items(), key=lambda x: x[1]["revenue"], reverse=True))[:20]),
        "by_campaign_tag": dict(sorted_campaigns[:15]),
        "pipelines": [{"name": p["name"], "stages": [s["name"] for s in p["stages"]]} for p in pipelines],
        "period": {"since": since, "until": until} if since else None,
        "_deal_details": all_deal_details,
    }

def analyze_campaign_funnel(campaign_tag, since=None, until=None):
    """Detailed stage-by-stage funnel for a specific campaign tag."""
    crm = analyze_crm_data(since, until)
    if "error" in crm:
        return crm

    deal_details = crm.get("_deal_details", [])

    # iStudio funnel stage order (working pipeline)
    STAGE_ORDER = [
        "Неразобранное",
        "Заявка взята в работу",
        "Связаться с клиентом повторно",
        "Записан на пробную процедуру",
        "Запись подтверждена",
        "Пробная процедура выполнена",
        "Закрыто и не реализовано",
    ]

    # Find matching deals — fuzzy multi-word match
    tag_lower = campaign_tag.lower().strip()
    search_words = [w for w in tag_lower.split() if len(w) > 2]  # e.g. ["карбон", "ивр"]

    def deal_matches(d):
        ct = (d.get("campaign_tag") or "").lower()
        ft = (d.get("fb_tag") or "").lower()
        # Full string match
        if tag_lower in ct or tag_lower in ft:
            return True
        # All words present in campaign_tag
        if search_words and all(w in ct for w in search_words):
            return True
        # Any single keyword match (for short queries like "карбон")
        if len(search_words) == 1 and search_words[0] in ct:
            return True
        return False

    matched = [d for d in deal_details if deal_matches(d)]

    if not matched:
        # Show available tags to help user
        available = sorted(set(
            d.get("campaign_tag") for d in deal_details
            if d.get("campaign_tag")
        ))
        hint = "\n".join(f"• {t}" for t in available[:8]) if available else "нет тегированных сделок"
        return {"error": f"Кампания '{campaign_tag}' не найдена.\n\nДоступные теги за период:\n{hint}"}

    # Count by stage
    stage_counts = {}
    for d in matched:
        s = d.get("stage", "Неизвестно")
        stage_counts[s] = stage_counts.get(s, 0) + 1

    total = len(matched)
    won = sum(1 for d in matched if d.get("is_won"))
    lost = sum(1 for d in matched if d.get("is_lost"))
    in_progress = total - won - lost
    revenue = sum(d.get("price", 0) for d in matched if d.get("is_won"))

    # Sort stages in funnel order, then unknowns at end
    def stage_sort_key(s):
        try:
            return STAGE_ORDER.index(s)
        except ValueError:
            return 99

    sorted_stages = sorted(stage_counts.items(), key=lambda x: stage_sort_key(x[0]))

    return {
        "campaign_tag": campaign_tag,
        "total": total,
        "won": won,
        "lost": lost,
        "in_progress": in_progress,
        "revenue": revenue,
        "conversion": round(won / total * 100, 1) if total > 0 else 0,
        "avg_deal": round(revenue / won, 0) if won > 0 else 0,
        "stages": sorted_stages,
        "period": {"since": since, "until": until},
    }

def format_campaign_funnel(data):
    """Format campaign funnel as readable Telegram text."""
    if "error" in data:
        return f"❌ {data['error']}"

    tag = data["campaign_tag"]
    total = data["total"]
    won = data["won"]
    lost = data["lost"]
    in_prog = data["in_progress"]
    revenue = data["revenue"]
    conv = data["conversion"]

    lines = [
        f"📊 Воронка кампании: {tag}",
        f"Период: {data['period']['since']} — {data['period']['until']}",
        "",
        f"Всего лидов: {total}",
        f"✅ Дошли до процедуры: {won} ({conv}%)",
        f"❌ Отказались: {lost} ({round(lost/total*100,1) if total else 0}%)",
        f"⏳ В работе (не закрыты): {in_prog} ({round(in_prog/total*100,1) if total else 0}%)",
        f"💰 Выручка: ₪{revenue:,.0f}",
        "",
        "─── Где сейчас каждый лид ───",
    ]

    for stage, count in data["stages"]:
        pct = round(count / total * 100, 1) if total else 0
        bar = "█" * min(count, 20)
        # Emoji per stage type
        if "выполнен" in stage.lower():
            emoji = "✅"
        elif "не реализовано" in stage.lower() or "закрыто" in stage.lower():
            emoji = "❌"
        elif "записан" in stage.lower() or "подтвержден" in stage.lower():
            emoji = "📅"
        elif "работу" in stage.lower() or "неразобр" in stage.lower():
            emoji = "🔄"
        elif "повторно" in stage.lower():
            emoji = "📞"
        else:
            emoji = "•"
        lines.append(f"{emoji} {stage}: {count} ({pct}%)")

    # Highlight stuck leads
    stuck = [(s, c) for s, c in data["stages"]
             if "работу" in s.lower() or "повторно" in s.lower() or "неразобр" in s.lower()]
    if stuck:
        stuck_total = sum(c for _, c in stuck)
        lines += [
            "",
            f"⚠️ Зависших лидов: {stuck_total} — нужна обработка!",
        ]

    return "\n".join(lines)

def analyze_golden_clients(since=None, until=None):
    crm = analyze_crm_data(since, until)
    if "error" in crm:
        return crm

    deal_details = crm.pop("_deal_details", [])

    contact_deals = defaultdict(list)
    for d in deal_details:
        for cid in (d.get("contact_ids") or []):
            contact_deals[cid].append(d)

    all_contact_ids = list(contact_deals.keys())
    print(f"Fetching {len(all_contact_ids)} contacts from amoCRM...")
    contact_info_map = get_amocrm_contacts(all_contact_ids)

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

        dates = [d["created_at"] for d in deals if d["created_at"]]
        if len(dates) >= 2:
            lifetime_days = (max(dates) - min(dates)) / 86400
        else:
            lifetime_days = 0

        cinfo = contact_info_map.get(cid, {})
        client_name = cinfo.get("name", "Без имени")
        client_phone = cinfo.get("phone", "")
        client_email = cinfo.get("email", "")

        if fb_tags:
            client_source = "Реклама Meta"
            client_source_detail = ", ".join(campaigns) if campaigns else fb_tags[0]
        elif campaigns:
            client_source = "Реклама Meta"
            client_source_detail = ", ".join(campaigns)
        else:
            # No tags at all — could be: referral, Instagram DM, WhatsApp, Facebook DM, manually added
            # We cannot know for sure, so mark as "Источник неизвестен"
            client_source = "Источник неизвестен"
            client_source_detail = "Нет тега (инста/вацап/фб директ/сарафан/внесён вручную)"

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

    fb_tag_quality = defaultdict(lambda: {"golden": 0, "repeat": 0, "one_time": 0, "total_ltv": 0, "manual_tags": set()})
    for c in golden_clients:
        for ft in c["fb_tags"]:
            fb_tag_quality[ft]["golden"] += 1
            fb_tag_quality[ft]["total_ltv"] += c["total_spent"]
            fb_tag_quality[ft]["manual_tags"].update(c["campaigns"])
    for c in repeat_clients:
        for ft in c["fb_tags"]:
            fb_tag_quality[ft]["repeat"] += 1
            fb_tag_quality[ft]["total_ltv"] += c["total_spent"]
            fb_tag_quality[ft]["manual_tags"].update(c["campaigns"])
    for c in one_time_clients:
        for ft in c["fb_tags"]:
            fb_tag_quality[ft]["one_time"] += 1
            fb_tag_quality[ft]["total_ltv"] += c["total_spent"]
            fb_tag_quality[ft]["manual_tags"].update(c["campaigns"])

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

    fb_to_meta_name = {}
    try:
        all_meta_campaigns = get_all_campaigns("name,id")
        for mc in all_meta_campaigns:
            meta_id = mc.get("id", "")
            meta_name = mc.get("name", "")
            fb_key = f"fb{meta_id}"
            fb_to_meta_name[fb_key] = meta_name
    except:
        pass

    fb_tag_quality_clean = {}
    for ft, data in fb_tag_quality.items():
        total_clients = data["golden"] + data["repeat"] + data["one_time"]
        if total_clients < 1:
            continue
        meta_name = fb_to_meta_name.get(ft, "")
        fb_tag_quality_clean[ft] = {
            "fb_tag": ft,
            "meta_campaign_name": meta_name if meta_name else "Не найдена в Meta (возможно удалена)",
            "manual_tag_group": ", ".join(data["manual_tags"]) if data["manual_tags"] else "Без тега",
            "golden_clients": data["golden"],
            "repeat_clients": data["repeat"],
            "one_time_clients": data["one_time"],
            "total_clients": total_clients,
            "total_ltv": data["total_ltv"],
            "avg_ltv_per_client": round(data["total_ltv"] / total_clients, 0) if total_clients > 0 else 0,
            "quality_score": round((data["golden"] * 3 + data["repeat"] * 1.5) / max(total_clients, 1) * 100, 1),
        }

    sorted_fb_quality = sorted(fb_tag_quality_clean.items(), key=lambda x: x[1]["quality_score"], reverse=True)

    all_clients = golden_clients + repeat_clients + one_time_clients
    from_ads     = [c for c in all_clients if c["source"] == "Реклама Meta"]
    from_unknown = [c for c in all_clients if c["source"] == "Источник неизвестен"]
    # "Referral" is unknown — we keep the key for backwards compatibility but note it's unknown
    from_referral = from_unknown  # alias

    golden_from_ads     = [c for c in golden_clients if c["source"] == "Реклама Meta"]
    golden_from_unknown = [c for c in golden_clients if c["source"] == "Источник неизвестен"]

    source_breakdown = {
        "total_from_ads": len(from_ads),
        "total_from_unknown": len(from_unknown),
        "note_unknown": "Нет fb-тега: инста/вацап/фб директ/сарафан/внесён вручную — источник неизвестен",
        "revenue_from_ads": sum(c["total_spent"] for c in from_ads),
        "revenue_from_unknown": sum(c["total_spent"] for c in from_unknown),
        "avg_ltv_ads": round(sum(c["total_spent"] for c in from_ads) / len(from_ads), 0) if from_ads else 0,
        "avg_ltv_unknown": round(sum(c["total_spent"] for c in from_unknown) / len(from_unknown), 0) if from_unknown else 0,
        "golden_from_ads": len(golden_from_ads),
        "golden_from_unknown": len(golden_from_unknown),
        "golden_revenue_ads": sum(c["total_spent"] for c in golden_from_ads),
        "golden_revenue_unknown": sum(c["total_spent"] for c in golden_from_unknown),
        # Legacy keys kept for compatibility
        "total_from_referral": len(from_unknown),
        "revenue_from_referral": sum(c["total_spent"] for c in from_unknown),
        "golden_from_referral": len(golden_from_unknown),
        "golden_revenue_referral": sum(c["total_spent"] for c in golden_from_unknown),
    }

    return {
        "golden_clients_count": len(golden_clients),
        "repeat_clients_count": len(repeat_clients),
        "one_time_clients_count": len(one_time_clients),
        "top_golden": golden_clients[:15],
        "top_repeat": repeat_clients[:10],
        "campaign_quality": dict(sorted_quality[:15]),
        "fb_campaign_quality": dict(sorted_fb_quality[:20]),
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
            "working_funnel": crm.get("working_funnel", {}),
            "permanent_funnel": crm.get("permanent_funnel", {}),
        },
    }

def analyze_campaign_roi(since=None, until=None):
    if not since or not until:
        since, until = get_date_range("all")
    insights = get_account_insights(since, until)
    meta_campaigns = enrich_insights(insights)

    crm = analyze_crm_data(since, until)
    if "error" in crm:
        return crm
    crm.pop("_deal_details", None)

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
    if not since or not until:
        since, until = get_date_range("month")

    insights = get_account_insights(since, until)
    meta_campaigns = enrich_insights(insights)

    meta_leads = []
    try:
        meta_leads = get_meta_leads(since, until)
    except:
        pass

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
            "working_funnel": crm.get("working_funnel", {}),
            "permanent_funnel": crm.get("permanent_funnel", {}),
        },
        "overall_roi": round((crm["total_revenue"] - total_meta_spend) / total_meta_spend * 100, 1) if total_meta_spend > 0 else 0,
    }

# ============================================================
# COMPARISON DATA FOR DASHBOARD
# ============================================================
def fetch_comparison_data(since, until):
    """
    Fetch analytics for the PREVIOUS period (same length) to compute deltas.
    Returns a simplified dict with key metrics only.
    """
    prev_since, prev_until = get_previous_period(since, until)
    if not prev_since:
        return {}

    print(f"Fetching comparison data: {prev_since} — {prev_until}")
    try:
        prev_insights = get_account_insights(prev_since, prev_until)
        prev_meta = enrich_insights(prev_insights)
        prev_total_spend = sum(c["spend"] for c in prev_meta)
        prev_total_leads = sum(c.get("total_leads", 0) for c in prev_meta)
        prev_avg_cpl = round(prev_total_spend / prev_total_leads, 2) if prev_total_leads > 0 else 0
    except Exception as e:
        print(f"Comparison Meta error: {e}")
        prev_total_spend = 0
        prev_total_leads = 0
        prev_avg_cpl = 0

    try:
        prev_crm = analyze_crm_data(prev_since, prev_until)
        prev_crm.pop("_deal_details", None)
        if "error" in prev_crm:
            prev_crm = {}
    except Exception as e:
        print(f"Comparison CRM error: {e}")
        prev_crm = {}

    prev_total_deals = prev_crm.get("total_deals", 0)
    prev_total_revenue = prev_crm.get("total_revenue", 0)
    prev_avg_deal = prev_crm.get("avg_deal", 0)
    prev_won = prev_crm.get("won_deals", 0)

    wf = prev_crm.get("working_funnel", {})
    pf = prev_crm.get("permanent_funnel", {})
    prev_wf_total = wf.get("total", 0) if wf else 0
    prev_wf_won = wf.get("won", 0) if wf else 0
    prev_wf_rev = wf.get("revenue", 0) if wf else 0
    prev_pf_total = pf.get("total", 0) if pf else 0
    prev_pf_rev = pf.get("revenue", 0) if pf else 0

    prev_conv = round(prev_wf_won / prev_wf_total * 100, 1) if prev_wf_total > 0 else 0
    prev_ltv = round(prev_pf_rev / prev_pf_total, 0) if prev_pf_total > 0 else 0
    prev_cac = round((prev_total_spend * 3.2) / prev_wf_won, 0) if prev_wf_won > 0 and prev_total_spend > 0 else 0
    prev_romi = round((prev_wf_rev - prev_total_spend * 3.2) / (prev_total_spend * 3.2) * 100, 0) if prev_total_spend > 0 else 0

    return {
        "period": {"since": prev_since, "until": prev_until},
        # Meta
        "total_spend": prev_total_spend,
        "total_leads": prev_total_leads,
        "avg_cpl": prev_avg_cpl,
        # CRM
        "total_deals": prev_total_deals,
        "total_revenue": prev_total_revenue,
        "avg_deal": prev_avg_deal,
        "won_deals": prev_won,
        # Key indicators
        "conversion": prev_conv,
        "ltv": prev_ltv,
        "cac": prev_cac,
        "romi": prev_romi,
        # Funnels
        "wf_total": prev_wf_total,
        "wf_won": prev_wf_won,
        "wf_revenue": prev_wf_rev,
        "pf_total": prev_pf_total,
        "pf_won": pf.get("won", 0) if pf else 0,
        "pf_revenue": prev_pf_rev,
    }

def render_delta_html(current, previous, inverse=False, prefix="", suffix=""):
    """
    Returns an HTML snippet for a delta badge.
    inverse=True: growth is bad (red), decline is good (green) - e.g. CPL, CAC.
    """
    d = calc_delta(current, previous, inverse=inverse)
    if d is None:
        return ""
    color = d["color"]
    symbol = d["symbol"]
    pct = d["pct"]
    prev_fmt = f"{prefix}{d['prev_value']:,.0f}{suffix}" if "prev_value" in d else ""
    tooltip = f"Было: {prev_fmt}" if prev_fmt else ""
    return (
        f'<span class="delta" style="color:{color}" title="{tooltip}">'
        f'{symbol} {pct}%'
        f'</span>'
    )

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
    if "error" in data:
        return f"❌ {data['error']}"
    return f"📊 Данные получены ({data_type}). Обработка временно недоступна."

# ============================================================
# INTENT DETECTION
# ============================================================
INTENT_PROMPT = """Парсер запросов рекламного/CRM бота. Ответь ТОЛЬКО JSON без markdown:
{"period": "month", "show": "spend", "custom_dates": null}

period: today | yesterday | week | month | 3months | 6months | year | all | custom
show: spend | all_campaigns | crm | roi | ltv | funnel | golden | full_report | budget_advice | dead_campaigns | best_source | branch_compare | dashboard | campaign_funnel | client_profile
campaign_tag: (только для campaign_funnel) название кампании или fb-тег
phone: (только для client_profile) номер телефона клиента
custom_dates: null или {"since": "YYYY-MM-DD", "until": "YYYY-MM-DD"} — ОБЯЗАТЕЛЬНО вычисли даты если указан конкретный период

ПРАВИЛА ДЛЯ ПЕРИОДОВ:
- Если пользователь указал конкретное число месяцев/недель/дней — ВЫЧИСЛИ custom_dates от сегодня назад
- "за 8 месяцев" → period: "custom", custom_dates: {"since": "2025-06-27", "until": "2026-02-27"}
- "за последние 2 месяца" → period: "custom", custom_dates вычисли
- "за январь" → period: "custom", custom_dates: {"since": "2026-01-01", "until": "2026-01-31"}
- "с июля по декабрь" → period: "custom", custom_dates с нужными датами
- Стандартные периоды БЕЗ custom_dates: today, yesterday, week, month, 3months, 6months, year, all

ОПРЕДЕЛЯЙ ПО СМЫСЛУ:
Расходы и реклама:
- "как дела", "статус", "сводка" → today, spend
- "вчера" → yesterday, spend
- "неделя", "за 7 дней" → week, spend
- "месяц" → month, spend
- "3 месяца", "квартал" → 3months, spend
- "полгода" → 6months, spend
- "год" → year, spend
- "за январь" → custom с датами
- "все кампании", "список" → all_campaigns

CRM и продажи:
- "crm", "срм", "клиенты", "сделки", "амо", "продажи" → crm
- "воронка", "конверсия", "потери", "где теряем", "куда уходят" → funnel
- "воронка по [кампании]", "где лиды с [тег]", "куда делись лиды карбон", "путь лидов", "где зависли лиды" → campaign_funnel + извлеки название кампании в поле "campaign_tag"

Анализ клиента:
- "кто такой +972...", "анализ клиента [телефон]", "профиль клиента [номер]", "расскажи о клиенте [номер]", "что знаешь о [телефон]" → client_profile + извлеки номер телефона в поле "phone"

Окупаемость:
- "roi", "рои", "окупаемость", "что окупается", "что работает", "что приносит деньги" → roi
- "какая кампания лучше", "куда вложить", "что масштабировать" → roi
- "эффективность рекламы", "реклама работает?" → roi

Клиенты и ценность:
- "ltv", "лтв", "выручка", "доход", "средний чек" → ltv
- "золотые клиенты", "лучшие клиенты", "VIP", "вип", "постоянные", "лояльные" → golden
- "кто остался", "кто возвращается", "повторные" → golden
- "откуда лучшие клиенты", "какая реклама приносит лучших" → golden
- "рекомендации или реклама", "сарафан", "сарафанное радио" → golden

Расширенная аналитика:
- "полный отчёт", "вся аналитика", "общая картина" → full_report
- "дашборд", "dashboard", "картинка", "png", "визуал" → dashboard
- "куда вложить бюджет", "как распределить бюджет", "совет по бюджету" → budget_advice
- "мёртвые кампании", "что выключить", "что не работает", "сливают бюджет", "убрать" → dead_campaigns
- "лучший источник", "откуда клиенты", "какой канал лучше", "источники" → best_source
- "по филиалам", "ришон vs хайфа", "сравни филиалы", "какой филиал лучше" → branch_compare

По умолчанию → today, spend

Сегодня: """ + get_israel_now().strftime("%Y-%m-%d")

def detect_intent(user_text):
    raw = call_claude(INTENT_PROMPT, user_text, max_tokens=200, retries=2)
    if raw:
        try:
            clean = raw.replace("```json", "").replace("```", "").strip()
            return json.loads(clean)
        except:
            pass

    text = user_text.lower()
    period = "today"
    show = "spend"
    custom_dates = None

    import re as re_mod
    num_months = re_mod.search(r'(\d+)\s*месяц', text)
    num_weeks = re_mod.search(r'(\d+)\s*недел', text)
    num_days = re_mod.search(r'(\d+)\s*(дн|день|дней)', text)

    if num_months:
        period = "custom"
        n = int(num_months.group(1))
        today = get_israel_now().date()
        custom_dates = {"since": str(today - timedelta(days=n * 30)), "until": str(today)}
    elif num_weeks:
        period = "custom"
        n = int(num_weeks.group(1))
        today = get_israel_now().date()
        custom_dates = {"since": str(today - timedelta(weeks=n)), "until": str(today)}
    elif num_days:
        period = "custom"
        n = int(num_days.group(1))
        today = get_israel_now().date()
        custom_dates = {"since": str(today - timedelta(days=n)), "until": str(today)}
    elif any(w in text for w in ["вчера", "yesterday"]):
        period = "yesterday"
    elif any(w in text for w in ["недел", "week"]):
        period = "week"
    elif any(w in text for w in ["квартал"]):
        period = "3months"
    elif any(w in text for w in ["полгод"]):
        period = "6months"
    elif any(w in text for w in ["месяц", "month"]):
        period = "month"
    elif any(w in text for w in ["год", "year"]):
        period = "year"

    if any(w in text for w in ["все кампании", "все компании", "список"]):
        show = "all_campaigns"
    elif any(w in text for w in ["золот", "лучшие клиенты", "постоянн", "лояльн", "вип", "vip", "кто остал", "долги", "сарафан", "рекомендац", "откуда лучш"]):
        show = "golden"
    elif any(w in text for w in ["полный отчёт", "полный отчет", "общая картина", "вся аналитика"]):
        show = "full_report"
    elif any(w in text for w in ["мёртв", "мертв", "выключить", "не работа", "слива", "убрать", "убить"]):
        show = "dead_campaigns"
    elif any(w in text for w in ["бюджет", "распредел", "куда вложить", "масштабир"]):
        show = "budget_advice"
    elif any(w in text for w in ["источник", "канал", "откуда клиент"]):
        show = "best_source"
    elif any(w in text for w in ["филиал", "ришон", "хайфа", "сравни"]):
        show = "branch_compare"
    elif any(w in text for w in ["дашборд", "dashboard", "картинк", "png", "изображен", "визуал"]):
        show = "dashboard"
    elif any(w in text for w in ["crm", "срм", "амо", "amocrm", "сделки", "клиенты", "продаж"]):
        show = "crm"
    elif any(w in text for w in ["roi", "рои", "окупаемость", "эффективн", "лучш", "топ", "что работа", "что приносит"]):
        show = "roi"
    elif any(w in text for w in ["ltv", "лтв", "выручка", "доход", "средний чек"]):
        show = "ltv"
    elif any(w in text for w in ["воронка", "конверси", "funnel", "теряем", "потери", "куда уход"]):
        show = "funnel"

    return {"period": period, "show": show, "custom_dates": custom_dates}

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

КРИТИЧЕСКИ ВАЖНО — НАЗВАНИЕ КОМПАНИИ:
- Компания называется iStudio (строчная i, заглавная S)
- НИКОГДА не пиши "ИСТУДИО", "istudio", "Истудио" — только iStudio

КРИТИЧЕСКИ ВАЖНО — ИСТОЧНИКИ КЛИЕНТОВ:
- "Реклама Meta" = есть fb-тег → точно пришёл с рекламы Facebook/Instagram
- "Источник неизвестен" = нет тегов → может быть: сарафан, директ инстаграм, директ фейсбук, WhatsApp, внесён вручную — МЫ НЕ ЗНАЕМ
- НИКОГДА не называй клиентов без тега "сарафанными" или "по рекомендации" — это неверно
- В отчёте пиши: "Х клиентов с неизвестным источником (нет тега — инста/вацап/фб директ/сарафан/ручной ввод)"

КРИТИЧЕСКИ ВАЖНО — ФИЛИАЛЫ:
- Сейчас работает ТОЛЬКО Ришон ле-Цион
- Ашдод закрыт, Раат продан — их данные отфильтрованы
- "Не указан" в филиале = это тоже Ришон (у клиентов слетел тег при удалении Раата из CRM)
- НИКОГДА не показывай "Не указан" как отдельный филиал в отчёте — объединяй с Ришоном
- Если в данных by_branch есть "Не указан" и "Ришон" — суммируй их и показывай как один Ришон

КОНТЕКСТ ПО ТЕГАМ КАМПАНИЙ:
- fb-тег (например fb14285258249) — уникальный ID конкретной кампании в Facebook
- Ручной тег (например "Карбон ИВР 250+2") — общая группа кампаний
- За одним ручным тегом может стоять НЕСКОЛЬКО разных кампаний Facebook
- meta_campaign_name — реальное название кампании в Facebook Ads Manager

ОРИЕНТИРЫ:
- Хорошая стоимость обращения: $3-5
- Хороший процент закрытия: 15-25%
- Золотой клиент: 3+ визита или ₪3000+ выручки
- Средний чек iStudio: ₪350-500
"""

def generate_response(user_text, data, data_type="spend"):
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

    clean_data = {k: v for k, v in data.items() if not k.startswith("_")}

    type_labels = {
        "spend": "расходы Meta Ads",
        "crm": "данные CRM (amoCRM)",
        "roi": "окупаемость рекламы (расходы vs выручка)",
        "ltv": "ценность клиентов по источникам",
        "funnel": "воронка продаж",
        "golden": "золотые и постоянные клиенты с источниками (реклама vs рекомендации)",
        "full_report": "полная аналитика: Meta + CRM + лиды",
        "budget_advice": "анализ для рекомендаций по бюджету — куда вложить, что масштабировать, что выключить",
        "dead_campaigns": "мёртвые и неэффективные кампании — что выключить и почему",
        "best_source": "сравнение источников клиентов: реклама vs рекомендации vs другое",
        "branch_compare": "сравнение филиалов по выручке, клиентам и эффективности",
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
# SAFE SEND
# ============================================================
def safe_send(chat_id, text, max_len=4000):
    if not text:
        text = "⚠️ Пустой ответ"
    if len(text) <= max_len:
        try:
            bot.send_message(chat_id, text)
        except Exception as e:
            print(f"Send error: {e}")
        return

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

    if period == "today" and total_spend == 0:
        y_since, y_until = get_date_range("yesterday")
        y_insights = get_account_insights(y_since, y_until)
        y_campaigns = enrich_insights(y_insights)
        y_spend = sum(c["spend"] for c in y_campaigns)

        try:
            all_camps = get_all_campaigns("name,effective_status")
            active_names = [c.get("name", "—") for c in all_camps if c.get("effective_status") == "ACTIVE"]
            paused_count = len([c for c in all_camps if c.get("effective_status") == "PAUSED"])
        except:
            active_names = []
            paused_count = 0

        return {
            "period": period, "since": since, "until": until,
            "campaigns": campaigns, "total_spend": 0,
            "note": "Meta API ещё не обновил данные за сегодня (задержка до нескольких часов). Показываю вчерашние данные для контекста.",
            "yesterday_data": {
                "since": y_since, "until": y_until,
                "campaigns": y_campaigns, "total_spend": round(y_spend, 2),
            },
            "active_campaigns": active_names,
            "active_count": len(active_names),
            "paused_count": paused_count,
        }

    return {"period": period, "since": since, "until": until, "campaigns": campaigns, "total_spend": round(total_spend, 2)}

def fetch_all_campaigns_list():
    camps = get_all_campaigns()
    active = [c.get("name", "—") for c in camps if c.get("effective_status") == "ACTIVE"]
    paused = len([c for c in camps if c.get("effective_status") == "PAUSED"])
    return {"total": len(camps), "active_names": active, "active_count": len(active), "paused_count": paused}

# ============================================================
# DASHBOARD PNG GENERATOR (with period comparison)
# ============================================================
def generate_dashboard_png(data, period_label="Сегодня", prev_data=None):
    """
    Generate a premium dashboard PNG.
    prev_data: dict with previous period metrics for delta comparison.
    """
    meta = data.get("meta_ads", data)
    crm = data.get("crm", data)
    total_spend = meta.get("total_spend", data.get("total_spend", 0))
    total_leads = meta.get("total_leads", data.get("total_leads", 0))
    avg_cpl = meta.get("avg_cost_per_lead", 0)
    if avg_cpl == 0 and total_leads > 0 and total_spend > 0:
        avg_cpl = round(total_spend / total_leads, 2)
    total_deals = crm.get("total_deals", data.get("total_deals", 0))
    total_revenue = crm.get("total_revenue", data.get("total_revenue", 0))
    avg_deal = crm.get("avg_deal", data.get("avg_deal", 0))
    won_deals = crm.get("won", crm.get("won_deals", data.get("won_deals", 0)))
    lost_deals = crm.get("lost", crm.get("lost_deals", data.get("lost_deals", 0)))
    conversion = crm.get("conversion", crm.get("conversion_rate", data.get("conversion_rate", 0)))
    overall_roi = data.get("overall_roi", data.get("total_roi", 0))
    if overall_roi == 0 and total_spend > 0 and total_revenue > 0:
        overall_roi = round((total_revenue - total_spend * 3.6) / (total_spend * 3.6) * 100, 1)

    tc = meta.get("top_campaigns", [])
    if isinstance(tc, dict):
        tc = [{"name": k, "spend": v.get("spend", 0), "leads": v.get("deals", 0)} for k, v in tc.items()]
    if not tc and "campaigns" in data:
        tc = data["campaigns"][:5]
    bt = crm.get("by_campaign_tag", data.get("by_campaign_tag", {}))
    if not tc and bt:
        tc = [{"name": k, "spend": 0, "leads": v.get("deals", 0), "revenue": v.get("revenue", 0)} for k, v in list(bt.items())[:5]]

    camps_html = ""
    for i, c in enumerate(tc[:5]):
        nm = c.get("name", c.get("campaign_name", "?"))
        if len(nm) > 28: nm = nm[:26] + "…"
        sp = c.get("spend", 0)
        le = c.get("leads", c.get("total_leads", c.get("deals", 0)))
        cp = c.get("cost_per_lead", 0)
        rv = c.get("revenue", 0)
        if cp == 0 and sp > 0 and le > 0: cp = round(sp / le, 2)
        if cp > 0:
            cls = "good" if cp < 15 else ("avg" if cp < 22 else "bad")
            val = f"${cp:.2f}"
        elif rv > 0: cls, val = "good", f"₪{rv:,.0f}"
        else: cls, val = "avg", "—"
        camps_html += f'<div class="cr"><span class="c0">{i+1}</span><span class="c1">{nm}</span><span class="c2">{"$"+f"{sp:,.0f}" if sp>0 else "—"}</span><span class="c3">{le if le>0 else "—"}</span><span class="c4 {cls}">{val}</span></div>'
    if not camps_html:
        camps_html = '<div class="cr" style="justify-content:center;color:#6b6b80">Нет данных</div>'

    wf = data.get("crm", data).get("working_funnel", data.get("working_funnel", {}))
    pf = data.get("crm", data).get("permanent_funnel", data.get("permanent_funnel", {}))

    def build_funnel_html(funnel_data, color1, color2):
        if not funnel_data or funnel_data.get("total", 0) == 0:
            return '<div style="text-align:center;color:#6b6b80;padding:16px">Нет данных</div>'
        total = funnel_data["total"]
        won = funnel_data.get("won", 0)
        lost = funnel_data.get("lost", 0)
        in_progress = total - won - lost
        stages_list = [
            ("Всего заявок", total, 100),
            ("В работе", in_progress, max(15, in_progress/max(total,1)*100)),
            ("Отказ", lost, max(10, lost/max(total,1)*100)),
            ("Выполнено", won, max(8, won/max(total,1)*100)),
        ]
        html = ""
        for i, (label, val, w) in enumerate(stages_list):
            if val == 0 and label in ("В работе",):
                continue
            pct = ""
            cls = ""
            if i > 0 and stages_list[0][1] > 0:
                r = round(val / stages_list[0][1] * 100, 1)
                pct = f"{r}%"
                cls = "good" if r >= 40 else ("warn" if r >= 15 else "bad")
            if label == "Отказ":
                c1, c2 = "#ef4444", "#f87171"
            elif label == "Выполнено":
                c1, c2 = "#22c55e", "#4ade80"
            else:
                c1, c2 = color1, color2
            conn = '<div class="fc"></div>' if i > 0 else ""
            vis = ' style="visibility:hidden"' if i == 0 else ""
            html += f'{conn}<div class="fs"><div class="fv {cls}"{vis}>{pct or "—"}</div><div class="fw"><div class="fb" style="width:{w}%;background:linear-gradient(135deg,{c1},{c2})"><span class="ft">{val:,}</span><span class="fl">{label}</span></div></div></div>'
        rev = funnel_data.get("revenue", 0)
        conv = round(won/total*100, 1) if total > 0 else 0
        html += f'<div style="display:flex;justify-content:center;gap:32px;margin-top:20px;font-size:32px;font-weight:900">'
        html += f'<span style="color:#22c55e">Конверсия: {conv}%</span>'
        html += f'<span style="color:#f0c040">Выручка: ₪{rev:,.0f}</span>'
        html += f'</div>'
        return html

    working_funnel_html = build_funnel_html(wf, "#3b82f6", "#60a5fa")
    permanent_funnel_html = build_funnel_html(pf, "#a855f7", "#c084fc")

    if total_revenue > 0 and total_spend > 0:
        profit_ok = (total_revenue - total_spend * 3.6) > 0
        status_txt = "БИЗНЕС В ПЛЮСЕ" if profit_ok else "ТРЕБУЕТ ВНИМАНИЯ"
        status_col = "#22c55e" if profit_ok else "#ef4444"
    elif total_revenue > 0: profit_ok, status_txt, status_col = True, "ДАННЫЕ ПОЛУЧЕНЫ", "#22c55e"
    else: profit_ok, status_txt, status_col = True, "ДАННЫЕ ПОЛУЧЕНЫ", "#3b82f6"

    if total_deals > 0 and avg_deal > 0:
        lost_rev = max(0, (total_deals - won_deals - lost_deals) * avg_deal)
        lost_n = max(0, total_deals - won_deals - lost_deals)
    else: lost_rev, lost_n = 0, 0

    wf_won = wf.get("won", 0) if wf else 0
    cac_ils = round((total_spend * 3.6) / wf_won, 0) if wf_won > 0 and total_spend > 0 else 0
    pf_total = pf.get("total", 0) if pf else 0
    pf_rev = pf.get("revenue", 0) if pf else 0
    ltv_ils = round(pf_rev / pf_total, 0) if pf_total > 0 else 0

    now = get_israel_now()
    date_str = now.strftime("%d.%m.%Y %H:%M")
    pi = data.get("period", {})
    if isinstance(pi, dict) and pi.get("since"):
        period_label = f'{pi["since"]} — {pi["until"]}'

    wf_rev = wf.get("revenue", 0) if wf else 0
    spend_ils = total_spend * 3.2
    real_romi = round((wf_rev - spend_ils) / spend_ils * 100, 0) if spend_ils > 0 else 0
    roi_col = "green" if real_romi > 0 else "red"
    wf_total = wf.get("total", 0) if wf else 0
    real_conv = round(wf_won / wf_total * 100, 1) if wf_total > 0 else 0
    conv_col = "green" if real_conv >= 20 else ("gold" if real_conv >= 10 else "red")

    # ---- DELTA BADGES ----
    p = prev_data or {}

    def d_badge(cur, prv, inverse=False, is_pct=False):
        """Returns HTML string for a delta badge, or empty."""
        try:
            cur_f = float(cur or 0)
            prv_f = float(prv or 0)
            if prv_f == 0:
                return ""
            pct = round((cur_f - prv_f) / prv_f * 100, 1)
            if pct == 0:
                return '<span class="db-flat">→ 0%</span>'
            growing = pct > 0
            good = (not growing) if inverse else growing
            color = "#22c55e" if good else "#ef4444"
            arrow = "▲" if growing else "▼"
            return f'<span class="db-delta" style="color:{color}">{arrow} {abs(pct):.1f}%</span>'
        except:
            return ""

    # Previous period label for subtitle
    prev_period_label = ""
    if p.get("period"):
        pp = p["period"]
        prev_period_label = f'vs {pp.get("since", "")} — {pp.get("until", "")}'

    # Compute deltas for all key metrics
    delta_spend      = d_badge(total_spend,   p.get("total_spend"),   inverse=True)
    delta_leads      = d_badge(total_leads,   p.get("total_leads"))
    delta_cpl        = d_badge(avg_cpl,       p.get("avg_cpl"),       inverse=True)
    delta_conv_meta  = d_badge(conversion,    p.get("conversion"))
    delta_deals      = d_badge(total_deals,   p.get("total_deals"))
    delta_won        = d_badge(won_deals,     p.get("won_deals"))
    delta_revenue    = d_badge(total_revenue, p.get("total_revenue"))
    delta_avg_deal   = d_badge(avg_deal,      p.get("avg_deal"))
    delta_cac        = d_badge(cac_ils,       p.get("cac"),           inverse=True)
    delta_ltv        = d_badge(ltv_ils,       p.get("ltv"))
    delta_romi       = d_badge(real_romi,     p.get("romi"))
    delta_real_conv  = d_badge(real_conv,     p.get("conversion"))

    # Delta for funnel rows (wf vs prev wf)
    prev_wf_total   = p.get("wf_total", 0)
    prev_wf_won     = p.get("wf_won", 0)
    prev_wf_rev     = p.get("wf_revenue", 0)
    prev_pf_total   = p.get("pf_total", 0)
    prev_pf_rev     = p.get("pf_revenue", 0)

    delta_wf_total  = d_badge(wf_total,  prev_wf_total)
    delta_wf_won    = d_badge(wf_won,    prev_wf_won)
    delta_wf_rev    = d_badge(wf_rev,    prev_wf_rev)
    delta_pf_total  = d_badge(pf_total,  prev_pf_total)
    delta_pf_won    = d_badge(pf.get("won", 0) if pf else 0, p.get("pf_won", 0))
    delta_pf_rev    = d_badge(pf_rev,    prev_pf_rev)

    html = f'''<!DOCTYPE html><html><head><meta charset="UTF-8"><style>
@import url('https://fonts.googleapis.com/css2?family=Unbounded:wght@300;400;500;600;700;800;900&family=JetBrains+Mono:wght@400;500;600&display=swap');
*{{margin:0;padding:0;box-sizing:border-box}}
body{{background:#06060c;color:#e8e8f0;font-family:'JetBrains Mono',monospace;width:1280px;overflow:hidden}}
body::before{{content:'';position:fixed;top:-200px;left:-200px;width:600px;height:600px;background:radial-gradient(circle,rgba(240,192,64,.07)0%,transparent 65%);pointer-events:none}}
body::after{{content:'';position:fixed;bottom:-300px;right:-200px;width:800px;height:800px;background:radial-gradient(circle,rgba(59,130,246,.05)0%,transparent 60%);pointer-events:none}}
.gr{{position:fixed;inset:0;background-image:linear-gradient(rgba(255,255,255,.015)1px,transparent 1px),linear-gradient(90deg,rgba(255,255,255,.015)1px,transparent 1px);background-size:60px 60px;pointer-events:none}}
.db{{position:relative;z-index:2;max-width:1240px;margin:0 auto;padding:32px 20px 24px}}
.hd{{text-align:center;margin-bottom:36px;position:relative}}
.hd::before{{content:'';position:absolute;top:-60px;left:50%;transform:translateX(-50%);width:400px;height:200px;background:radial-gradient(ellipse,rgba(240,192,64,.12)0%,transparent 70%);filter:blur(30px);pointer-events:none}}
.lg{{font-family:'Unbounded',sans-serif;font-size:36px;font-weight:900;letter-spacing:-1.5px;background:linear-gradient(135deg,#f0c040,#ffd700,#f5d060,#b8922e);-webkit-background-clip:text;-webkit-text-fill-color:transparent}}
.hs{{font-size:10px;color:#6b6b80;letter-spacing:5px;text-transform:uppercase;margin:4px 0 8px}}
.badge{{display:inline-block;font-family:'Unbounded',sans-serif;font-size:32px;font-weight:800;letter-spacing:-1px;background:linear-gradient(135deg,#e0e0f0,#a0a0c0);-webkit-background-clip:text;-webkit-text-fill-color:transparent;margin-bottom:4px}}
.vs-label{{font-size:11px;color:#4a4a5a;letter-spacing:3px;text-transform:uppercase;margin-top:4px}}
.sb{{display:flex;align-items:center;justify-content:center;gap:10px;margin-top:18px}}
.sd{{width:9px;height:9px;border-radius:50%;background:{status_col};box-shadow:0 0 12px {status_col}80,0 0 30px {status_col}40}}
.stx{{font-family:'Unbounded',sans-serif;font-size:22px;font-weight:900;color:{status_col};letter-spacing:2px}}
.g4{{display:grid;grid-template-columns:repeat(4,1fr);gap:14px;margin-bottom:14px}}
.g2{{display:grid;grid-template-columns:1fr 1fr;gap:14px;margin-bottom:14px}}
.card{{background:rgba(18,18,28,.85);border:1px solid rgba(255,255,255,.06);border-radius:18px;padding:22px;position:relative;overflow:hidden;backdrop-filter:blur(20px);box-shadow:0 4px 24px rgba(0,0,0,.4),0 1px 0 rgba(255,255,255,.04)inset,0 -2px 8px rgba(0,0,0,.2)inset;transform:perspective(800px)rotateX(3deg)}}
.card::before{{content:'';position:absolute;top:0;left:0;right:0;height:50%;background:linear-gradient(180deg,rgba(255,255,255,.04),transparent);border-radius:18px 18px 0 0;pointer-events:none}}
.cl{{font-size:20px;color:#e0e0f0;font-weight:700;text-transform:uppercase;letter-spacing:2.5px;margin-bottom:10px}}
.cv-row{{display:flex;align-items:center;gap:12px;flex-wrap:nowrap}}
.cv{{font-family:'Unbounded',sans-serif;font-size:40px;font-weight:800;line-height:1;text-shadow:0 2px 8px rgba(0,0,0,.3)}}
/* Delta badges — inline right of value, 2x size */
.db-delta{{display:inline-flex;align-items:center;font-family:'JetBrains Mono',monospace;font-size:22px;font-weight:700;padding:5px 14px;border-radius:20px;background:rgba(255,255,255,.09);letter-spacing:0.3px;white-space:nowrap}}
.db-flat{{display:inline-flex;align-items:center;font-family:'JetBrains Mono',monospace;font-size:20px;font-weight:600;padding:5px 14px;border-radius:20px;background:rgba(255,255,255,.05);color:#4a4a5a;white-space:nowrap}}
.sec{{font-family:'Unbounded',sans-serif;font-size:20px;font-weight:700;color:#e8e8f0;letter-spacing:4px;text-transform:uppercase;margin:32px 0 16px;display:flex;align-items:center;justify-content:center;gap:12px}}
.fcard{{background:rgba(18,18,28,.85);border:1px solid rgba(255,255,255,.06);border-radius:18px;padding:28px 24px;margin-bottom:14px;backdrop-filter:blur(20px);box-shadow:0 8px 32px rgba(0,0,0,.5),0 1px 0 rgba(255,255,255,.04)inset;overflow:hidden}}
.fstats{{display:flex;gap:24px;justify-content:center;margin-bottom:16px;flex-wrap:wrap}}
.fstat{{text-align:center;background:rgba(255,255,255,.03);border:1px solid rgba(255,255,255,.05);border-radius:12px;padding:16px 28px}}
.fstat-l{{font-size:16px;color:#6b6b80;text-transform:uppercase;letter-spacing:2.5px;font-weight:700;margin-bottom:8px}}
.fstat-v{{font-family:'Unbounded',sans-serif;font-size:32px;font-weight:700;display:flex;align-items:center;gap:12px}}
.fn{{display:flex;flex-direction:column;align-items:center;gap:2px;max-width:620px;margin:0 auto}}
.fs{{display:flex;align-items:center;width:100%}}
.fw{{flex:1;display:flex;justify-content:center}}
.fb{{height:56px;border-radius:10px;display:flex;align-items:center;justify-content:center;position:relative;box-shadow:0 4px 16px rgba(0,0,0,.4),0 2px 0 rgba(255,255,255,.15)inset;transform:perspective(500px)rotateX(5deg)}}
.fb::after{{content:'';position:absolute;top:0;left:0;right:0;height:50%;background:linear-gradient(180deg,rgba(255,255,255,.18),transparent);border-radius:10px 10px 0 0;pointer-events:none}}
.ft{{font-family:'Unbounded',sans-serif;font-size:30px;font-weight:800;color:#fff;text-shadow:0 2px 4px rgba(0,0,0,.4);position:relative;z-index:1}}
.fl{{position:absolute;right:-200px;font-size:20px;color:#e0e0f0;font-weight:700;text-transform:uppercase;letter-spacing:1px;white-space:nowrap;width:130px;z-index:1}}
.fv{{position:absolute;left:-90px;font-size:20px;font-weight:600;white-space:nowrap;width:50px;text-align:right}}
.fv.good{{color:#22c55e}}.fv.warn{{color:#f97316}}.fv.bad{{color:#ef4444}}
.fc{{width:2px;height:6px;background:linear-gradient(180deg,rgba(255,255,255,.08),rgba(255,255,255,.02));margin:0 auto}}
.cr{{display:flex;justify-content:space-between;align-items:center;padding:12px 0;border-bottom:1px solid rgba(255,255,255,.04)}}.cr:last-child{{border-bottom:none}}
.c0{{font-size:10px;color:#6b6b80;width:24px;font-weight:600}}.c1{{font-size:18px;font-weight:600;flex:1;padding-right:12px}}.c2{{font-size:18px;font-weight:600;color:#6b6b80;width:70px;text-align:right}}.c3{{font-size:18px;font-weight:600;width:50px;text-align:right}}.c4{{font-size:18px;font-weight:600;width:80px;text-align:right}}
.c4.good{{color:#22c55e}}.c4.avg{{color:#f97316}}.c4.bad{{color:#ef4444}}
.profit{{background:linear-gradient(135deg,rgba(34,197,94,.1),rgba(240,192,64,.06));border:1px solid rgba(34,197,94,.2);border-radius:18px;padding:24px;text-align:center;box-shadow:0 4px 24px rgba(34,197,94,.1)}}
.profit-l{{font-size:24px;color:#22c55e;font-weight:800;text-transform:uppercase;letter-spacing:4px;margin-bottom:8px}}
.profit-v{{font-family:'Unbounded',sans-serif;font-size:54px;font-weight:800;color:#22c55e;text-shadow:0 0 40px rgba(34,197,94,.3)}}
.profit-v em{{font-size:22px;font-weight:500;opacity:.7;font-style:normal}}
.lost{{background:linear-gradient(135deg,rgba(239,68,68,.08),rgba(239,68,68,.02));border:1px solid rgba(239,68,68,.15);border-radius:18px;padding:24px;text-align:center}}
.lost-l{{font-size:24px;color:#ef4444;font-weight:800;text-transform:uppercase;letter-spacing:4px;margin-bottom:8px}}
.lost-v{{font-family:'Unbounded',sans-serif;font-size:48px;font-weight:700;color:#ef4444}}
.lost-s{{font-size:18px;font-weight:600;color:#6b6b80;margin-top:8px}}
.pill{{flex:1;background:rgba(18,18,28,.85);border:1px solid rgba(255,255,255,.06);border-radius:14px;padding:16px;text-align:center;box-shadow:0 4px 16px rgba(0,0,0,.3)}}
.pill-l{{font-size:22px;color:#e0e0f0;font-weight:800;color:#6b6b80;text-transform:uppercase;letter-spacing:2.5px;margin-bottom:8px}}
.pill-v{{font-family:'Unbounded',sans-serif;font-size:42px;font-weight:700}}
.pill-v.gold{{color:#f0c040}}.pill-v.green{{color:#22c55e}}.pill-v.red{{color:#ef4444}}.pill-v.blue{{color:#3b82f6}}
.footer{{text-align:center;padding:24px 0 8px;font-size:9px;color:#3a3a50;letter-spacing:2px;text-transform:uppercase}}
</style></head><body><div class="gr"></div><div class="db">
<div class="hd">
  <div class="lg">iStudio</div>
  <div class="hs"></div>
  <div class="badge">{period_label}</div>
  {f'<div class="vs-label">{prev_period_label}</div>' if prev_period_label else ''}
  <div class="sb"><div class="sd"></div><div class="stx">{status_txt}</div></div>
</div>

<div class="sec">Meta Ads</div>
<div class="g4">
  <div class="card">
    <div class="cl">Расход</div>
    <div class="cv-row"><div class="cv">${total_spend:,.0f}</div>{delta_spend}</div>
  </div>
  <div class="card">
    <div class="cl">Лиды</div>
    <div class="cv-row"><div class="cv">{total_leads}</div>{delta_leads}</div>
  </div>
  <div class="card">
    <div class="cl">CPL</div>
    <div class="cv-row"><div class="cv">${avg_cpl:.2f}</div>{delta_cpl}</div>
  </div>
  <div class="card">
    <div class="cl">Конверсия</div>
    <div class="cv-row"><div class="cv">{conversion}%</div>{delta_conv_meta}</div>
  </div>
</div>

<div class="sec">Рабочая воронка (новые клиенты)</div>
<div class="fcard">
  {f"""<div class="fstats">
    <div class="fstat"><div class="fstat-l">Заявок</div><div class="fstat-v">{wf_total} {delta_wf_total}</div></div>
    <div class="fstat"><div class="fstat-l">Выполнено</div><div class="fstat-v" style="color:#22c55e">{wf_won} {delta_wf_won}</div></div>
    <div class="fstat"><div class="fstat-l">Выручка</div><div class="fstat-v" style="color:#f0c040">₪{wf_rev:,.0f} {delta_wf_rev}</div></div>
  </div>""" if wf_total > 0 else ""}
  <div class="fn">{working_funnel_html}</div>
</div>

<div class="sec">Постоянные клиенты</div>
<div class="fcard">
  {f"""<div class="fstats">
    <div class="fstat"><div class="fstat-l">Всего</div><div class="fstat-v">{pf_total} {delta_pf_total}</div></div>
    <div class="fstat"><div class="fstat-l">Выполнено</div><div class="fstat-v" style="color:#22c55e">{pf.get("won", 0) if pf else 0} {delta_pf_won}</div></div>
    <div class="fstat"><div class="fstat-l">Выручка</div><div class="fstat-v" style="color:#a855f7">₪{pf_rev:,.0f} {delta_pf_rev}</div></div>
  </div>""" if pf_total > 0 else ""}
  <div class="fn">{permanent_funnel_html}</div>
</div>

<div class="sec">amoCRM</div>
<div class="g4">
  <div class="card">
    <div class="cl">Сделок</div>
    <div class="cv-row"><div class="cv">{total_deals}</div>{delta_deals}</div>
  </div>
  <div class="card">
    <div class="cl">Продаж</div>
    <div class="cv-row"><div class="cv" style="color:#22c55e">{won_deals}</div>{delta_won}</div>
  </div>
  <div class="card">
    <div class="cl">Выручка</div>
    <div class="cv-row"><div class="cv" style="color:#22c55e">₪{total_revenue:,.0f}</div>{delta_revenue}</div>
  </div>
  <div class="card">
    <div class="cl">Ср. чек</div>
    <div class="cv-row"><div class="cv">₪{avg_deal:,.0f}</div>{delta_avg_deal}</div>
  </div>
</div>

<div class="sec">Кампании</div>
<div class="card">
  <div class="cr" style="color:#6b6b80;font-size:13px;text-transform:uppercase;letter-spacing:1.5px;font-weight:700;border-bottom:1px solid rgba(255,255,255,.06)!important;padding-bottom:8px!important">
    <span class="c0">#</span><span class="c1">Кампания</span><span class="c2">Расход</span><span class="c3">Лиды</span><span class="c4">CPL</span>
  </div>
  {camps_html}
</div>

<div class="g2" style="margin-top:14px">
  <div class="profit">
    <div class="profit-l">{"Прибыль" if profit_ok else "Выручка"}</div>
    <div class="profit-v"><em>₪</em>{abs(total_revenue - total_spend * 3.2):,.0f}</div>
  </div>
  <div class="lost">
    <div class="lost-l">Упущенная выручка</div>
    <div class="lost-v">₪{lost_rev:,.0f}</div>
    <div class="lost-s">{lost_n} сделок без результата</div>
  </div>
</div>

<div class="sec">Ключевые показатели</div>
<div class="g4">
  <div class="pill">
    <div class="pill-l">CAC</div>
    <div class="cv-row" style="justify-content:center">
      <div class="pill-v gold">{"₪"+f"{cac_ils:.0f}" if cac_ils>0 else "—"}</div>{delta_cac}
    </div>
  </div>
  <div class="pill">
    <div class="pill-l">LTV</div>
    <div class="cv-row" style="justify-content:center">
      <div class="pill-v blue">{"₪"+f"{ltv_ils:.0f}" if ltv_ils>0 else "—"}</div>{delta_ltv}
    </div>
  </div>
  <div class="pill">
    <div class="pill-l">ROMI</div>
    <div class="cv-row" style="justify-content:center">
      <div class="pill-v {roi_col}">{real_romi:.0f}%</div>{delta_romi}
    </div>
  </div>
  <div class="pill">
    <div class="pill-l">Конверсия</div>
    <div class="cv-row" style="justify-content:center">
      <div class="pill-v {conv_col}">{real_conv}%</div>{delta_real_conv}
    </div>
  </div>
</div>

<div class="footer">iStudio Performance Dashboard · Бот Рекламщик · {date_str}</div>
</div></body></html>'''

    html_path = tempfile.mktemp(suffix=".html", prefix="dash_")
    png_path = tempfile.mktemp(suffix=".png", prefix="dashboard_")
    with open(html_path, "w", encoding="utf-8") as f:
        f.write(html)
    try:
        from playwright.sync_api import sync_playwright
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True, args=['--no-sandbox', '--disable-setuid-sandbox', '--disable-dev-shm-usage', '--disable-gpu'])
            page = browser.new_page(viewport={"width": 1280, "height": 800}, device_scale_factor=2)
            page.goto(f"file://{html_path}", wait_until="networkidle")
            page.wait_for_timeout(1500)
            height = page.evaluate("document.documentElement.scrollHeight")
            page.set_viewport_size({"width": 1280, "height": height})
            page.wait_for_timeout(300)
            page.screenshot(path=png_path, full_page=True, type="png")
            browser.close()
    finally:
        try: os.unlink(html_path)
        except: pass
    return png_path

# ============================================================
# CAMPAIGN DASHBOARD
# ============================================================
def generate_campaign_dashboard_png(campaign_data, meta_data, period_label, prev_campaign_data=None):
    """Generate a per-campaign funnel + meta stats dashboard as PNG."""
    from datetime import datetime as _dt

    tag       = campaign_data.get("campaign_tag", "Кампания")
    total     = campaign_data.get("total", 0)
    won       = campaign_data.get("won", 0)
    lost      = campaign_data.get("lost", 0)
    in_prog   = campaign_data.get("in_progress", 0)
    revenue   = campaign_data.get("revenue", 0)
    conv      = campaign_data.get("conversion", 0)
    avg_deal  = campaign_data.get("avg_deal", 0)
    stages    = campaign_data.get("stages", [])
    date_str  = _dt.now().strftime("%d.%m.%Y %H:%M")

    # Meta stats for this campaign
    spend     = meta_data.get("spend", 0)
    leads     = meta_data.get("leads", 0)
    cpl       = round(spend / leads, 2) if leads > 0 else 0
    cpl_str   = f"${cpl:.2f}" if cpl > 0 else "—"
    romi      = round((revenue - spend * 3.6) / (spend * 3.6) * 100, 0) if spend > 0 else 0
    romi_col  = "#22c55e" if romi >= 0 else "#ef4444"

    # Delta badges vs prev period
    p = prev_campaign_data or {}
    def d_badge(cur, prv, inverse=False):
        if not prv or prv == 0 or cur is None: return ""
        try:
            pct = (cur - prv) / abs(prv) * 100
        except: return ""
        if abs(pct) < 0.5: return '<span class="db-flat">→ 0%</span>'
        up = pct > 0
        color = ("#ef4444" if up else "#22c55e") if inverse else ("#22c55e" if up else "#ef4444")
        arrow = "▲" if up else "▼"
        return f'<span class="db-delta" style="color:{color}">{arrow} {abs(pct):.1f}%</span>'

    d_total   = d_badge(total,   p.get("total"))
    d_won     = d_badge(won,     p.get("won"))
    d_lost    = d_badge(lost,    p.get("lost"), inverse=True)
    d_conv    = d_badge(conv,    p.get("conversion"))
    d_rev     = d_badge(revenue, p.get("revenue"))
    d_spend   = d_badge(spend,   p.get("spend"), inverse=True)
    d_leads   = d_badge(leads,   p.get("leads"))
    d_cpl     = d_badge(cpl,     p.get("cpl"), inverse=True)

    prev_label = ""
    if p.get("since") and p.get("until"):
        prev_label = f'<div class="vs-label">VS {p["since"]} — {p["until"]}</div>'

    # Build stages funnel bars
    STAGE_ORDER = [
        "Неразобранное", "Заявка взята в работу", "Связаться с клиентом повторно",
        "Записан на пробную процедуру", "Запись подтверждена",
        "Пробная процедура выполнена", "Закрыто и не реализовано",
    ]
    STAGE_COLORS = {
        "Пробная процедура выполнена": ("#22c55e", "#4ade80"),
        "Закрыто и не реализовано":   ("#ef4444", "#f87171"),
        "Записан на пробную процедуру": ("#3b82f6", "#60a5fa"),
        "Запись подтверждена":          ("#3b82f6", "#93c5fd"),
        "Заявка взята в работу":        ("#f59e0b", "#fbbf24"),
        "Связаться с клиентом повторно":("#f97316", "#fb923c"),
        "Неразобранное":                ("#6b7280", "#9ca3af"),
    }

    stages_html = ""
    for stage_name, count in stages:
        if count == 0: continue
        pct_of_total = round(count / total * 100, 1) if total > 0 else 0
        bar_w = max(8, pct_of_total)
        c1, c2 = STAGE_COLORS.get(stage_name, ("#6366f1", "#818cf8"))
        is_stuck = any(w in stage_name.lower() for w in ["работу", "повторно", "неразобр"])
        stuck_badge = '<span class="stuck-badge">⚠ ждут</span>' if is_stuck else ""
        stages_html += f'''
        <div class="stage-row">
          <div class="stage-label">{stage_name}{stuck_badge}</div>
          <div class="stage-bar-wrap">
            <div class="stage-bar" style="width:{bar_w}%;background:linear-gradient(90deg,{c1},{c2})">
              <span class="stage-count">{count}</span>
            </div>
          </div>
          <div class="stage-pct">{pct_of_total}%</div>
        </div>'''

    stuck_total = sum(c for s, c in stages if any(w in s.lower() for w in ["работу", "повторно", "неразобр"]))
    stuck_block = f'<div class="stuck-alert">⚠️ {stuck_total} лидов зависли и ждут обработки</div>' if stuck_total > 0 else ""

    html = f'''<!DOCTYPE html><html><head><meta charset="utf-8">
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=Unbounded:wght@400;700;900&family=JetBrains+Mono:wght@500;700&family=Inter:wght@400;500;600&display=swap" rel="stylesheet">
<style>
*{{margin:0;padding:0;box-sizing:border-box}}
body{{background:#0a0a14;color:#e8e8f0;font-family:'Inter',sans-serif;padding:40px;min-width:900px}}
.wrap{{max-width:900px;margin:0 auto}}

/* Header */
.hdr{{text-align:center;margin-bottom:36px}}
.logo{{font-family:'Unbounded',sans-serif;font-size:36px;font-weight:900;letter-spacing:-1.5px;background:linear-gradient(135deg,#f0c040,#ffd700,#b8922e);-webkit-background-clip:text;-webkit-text-fill-color:transparent}}
.campaign-name{{font-family:'Unbounded',sans-serif;font-size:26px;font-weight:700;color:#e8e8f0;margin:8px 0 4px;letter-spacing:-0.5px}}
.period-lbl{{font-family:'Unbounded',sans-serif;font-size:22px;font-weight:800;background:linear-gradient(135deg,#e0e0f0,#a0a0c0);-webkit-background-clip:text;-webkit-text-fill-color:transparent}}
.vs-label{{font-size:11px;color:#4a4a5a;letter-spacing:3px;text-transform:uppercase;margin-top:4px}}

/* Section title */
.sec{{font-size:11px;letter-spacing:4px;text-transform:uppercase;color:#6b6b80;text-align:center;margin:32px 0 14px;font-weight:600}}

/* Meta top row */
.meta-row{{display:grid;grid-template-columns:repeat(4,1fr);gap:14px;margin-bottom:8px}}
.mcard{{background:rgba(255,255,255,.04);border:1px solid rgba(255,255,255,.07);border-radius:16px;padding:18px 16px}}
.mcard .cl{{font-size:10px;letter-spacing:3px;text-transform:uppercase;color:#6b6b80;margin-bottom:8px;font-weight:600}}
.cv-row{{display:flex;align-items:center;gap:10px;flex-wrap:nowrap}}
.cv{{font-family:'Unbounded',sans-serif;font-size:34px;font-weight:800;line-height:1}}
.db-delta{{display:inline-flex;align-items:center;font-family:'JetBrains Mono',monospace;font-size:18px;font-weight:700;padding:3px 9px;border-radius:14px;background:rgba(255,255,255,.08);white-space:nowrap}}
.db-flat{{display:inline-flex;align-items:center;font-family:'JetBrains Mono',monospace;font-size:16px;padding:3px 9px;border-radius:14px;background:rgba(255,255,255,.04);color:#4a4a5a;white-space:nowrap}}

/* CRM summary row */
.crm-row{{display:grid;grid-template-columns:repeat(4,1fr);gap:14px;margin-bottom:8px}}

/* Funnel stages */
.funnel-card{{background:rgba(255,255,255,.03);border:1px solid rgba(255,255,255,.06);border-radius:20px;padding:28px 32px}}
.stage-row{{display:flex;align-items:center;gap:14px;margin-bottom:12px}}
.stage-label{{font-size:13px;color:#b0b0c0;width:230px;flex-shrink:0;display:flex;align-items:center;gap:8px}}
.stage-bar-wrap{{flex:1;background:rgba(255,255,255,.05);border-radius:8px;height:36px;overflow:hidden}}
.stage-bar{{height:100%;border-radius:8px;display:flex;align-items:center;padding-left:12px;min-width:36px;transition:width .3s}}
.stage-count{{font-family:'Unbounded',sans-serif;font-size:16px;font-weight:700;color:#fff}}
.stage-pct{{font-family:'JetBrains Mono',monospace;font-size:14px;color:#6b6b80;width:48px;text-align:right}}
.stuck-badge{{font-size:10px;background:rgba(251,191,36,.15);color:#fbbf24;border-radius:8px;padding:2px 6px;letter-spacing:0.5px}}
.stuck-alert{{background:rgba(251,191,36,.08);border:1px solid rgba(251,191,36,.2);border-radius:12px;padding:12px 20px;text-align:center;font-size:14px;color:#fbbf24;margin-top:16px}}

/* Summary row */
.summary-row{{display:grid;grid-template-columns:1fr 1fr 1fr;gap:14px;margin-top:8px}}
.sum-card{{background:rgba(255,255,255,.04);border:1px solid rgba(255,255,255,.07);border-radius:16px;padding:20px;text-align:center}}
.sum-label{{font-size:10px;letter-spacing:3px;text-transform:uppercase;color:#6b6b80;margin-bottom:10px;font-weight:600}}
.sum-val{{font-family:'Unbounded',sans-serif;font-size:38px;font-weight:800}}

/* ROMI */
.romi-card{{background:rgba(255,255,255,.04);border:1px solid rgba(255,255,255,.07);border-radius:16px;padding:20px;text-align:center;margin-top:14px}}
.romi-label{{font-size:10px;letter-spacing:3px;text-transform:uppercase;color:#6b6b80;margin-bottom:10px}}
.romi-val{{font-family:'Unbounded',sans-serif;font-size:54px;font-weight:900}}

.footer{{text-align:center;font-size:11px;color:#2a2a3a;margin-top:32px;letter-spacing:2px}}
</style></head><body><div class="wrap">

<div class="hdr">
  <div class="logo">iStudio</div>
  <div class="campaign-name">{tag}</div>
  <div class="period-lbl">{period_label}</div>
  {prev_label}
</div>

<div class="sec">Meta Ads — кампания</div>
<div class="meta-row">
  <div class="mcard"><div class="cl">Расход</div><div class="cv-row"><div class="cv">${spend:,.0f}</div>{d_spend}</div></div>
  <div class="mcard"><div class="cl">Лиды</div><div class="cv-row"><div class="cv">{leads}</div>{d_leads}</div></div>
  <div class="mcard"><div class="cl">CPL</div><div class="cv-row"><div class="cv">{cpl_str}</div>{d_cpl}</div></div>
  <div class="mcard"><div class="cl">ROMI</div><div class="cv-row"><div class="cv" style="color:{romi_col}">{romi:.0f}%</div></div></div>
</div>

<div class="sec">Воронка CRM — путь лидов</div>
<div class="funnel-card">
  {stages_html}
  {stuck_block}
</div>

<div class="sec">Итоги</div>
<div class="crm-row">
  <div class="mcard"><div class="cl">Всего лидов</div><div class="cv-row"><div class="cv">{total}</div>{d_total}</div></div>
  <div class="mcard"><div class="cl">Выполнено ✅</div><div class="cv-row"><div class="cv" style="color:#22c55e">{won}</div>{d_won}</div></div>
  <div class="mcard"><div class="cl">Отказ ❌</div><div class="cv-row"><div class="cv" style="color:#ef4444">{lost}</div>{d_lost}</div></div>
  <div class="mcard"><div class="cl">В работе ⏳</div><div class="cv-row"><div class="cv" style="color:#f59e0b">{in_prog}</div></div></div>
</div>

<div class="summary-row">
  <div class="sum-card"><div class="sum-label">Конверсия</div><div class="cv-row" style="justify-content:center"><div class="sum-val" style="color:{'#22c55e' if conv>=20 else '#f59e0b' if conv>=10 else '#ef4444'}">{conv}%</div>{d_conv}</div></div>
  <div class="sum-card"><div class="sum-label">Выручка</div><div class="cv-row" style="justify-content:center"><div class="sum-val" style="color:#22c55e">₪{revenue:,.0f}</div>{d_rev}</div></div>
  <div class="sum-card"><div class="sum-label">Средний чек</div><div class="sum-val" style="color:#f0c040">₪{avg_deal:,.0f}</div></div>
</div>

<div class="footer">iStudio Campaign Dashboard · {tag} · {date_str}</div>
</div></body></html>'''

    html_path = tempfile.mktemp(suffix=".html", prefix="camp_dash_")
    png_path  = tempfile.mktemp(suffix=".png",  prefix="campaign_")
    with open(html_path, "w", encoding="utf-8") as f:
        f.write(html)
    try:
        from playwright.sync_api import sync_playwright
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True, args=['--no-sandbox','--disable-setuid-sandbox','--disable-dev-shm-usage','--disable-gpu'])
            page = browser.new_page(viewport={"width": 980, "height": 800}, device_scale_factor=2)
            page.goto(f"file://{html_path}", wait_until="networkidle")
            page.wait_for_timeout(1500)
            height = page.evaluate("document.documentElement.scrollHeight")
            page.set_viewport_size({"width": 980, "height": height})
            page.wait_for_timeout(300)
            page.screenshot(path=png_path, full_page=True, type="png")
            browser.close()
    finally:
        try: os.unlink(html_path)
        except: pass
    return png_path

# ============================================================
# TELEGRAM HANDLERS
# ============================================================
@bot.message_handler(commands=["start"])
def cmd_start(message):
    if message.chat.id != MY_CHAT_ID:
        return
    safe_send(MY_CHAT_ID,
        "👋 Привет! Я твой личный аналитик рекламы и продаж.\n\n"
        "💰 Реклама:\n"
        "• «Как дела?» — расходы сегодня\n"
        "• «Покажи за неделю/месяц/квартал» — сводка\n"
        "• /campaigns — список кампаний\n"
        "• /alerts — проблемы\n\n"
        "📈 Продажи:\n"
        "• /crm — сводка продаж\n"
        "• /roi — что окупается\n"
        "• /funnel — где теряем клиентов\n\n"
        "⭐ Глубокая аналитика:\n"
        "• /golden — золотые клиенты (кто, откуда, сколько принёс)\n"
        "• /ltv — ценность клиентов\n"
        "• /full — полный отчёт\n\n"
        "🧠 Умные вопросы (просто спроси):\n"
        "• «Какая кампания приносит лучших клиентов?»\n"
        "• «Что выключить? Что сливает бюджет?»\n"
        "• «Куда вложить бюджет?»\n"
        "• «Рекомендации или реклама — что лучше?»\n"
        "• «Сравни филиалы»\n"
        "• «Откуда приходят постоянные клиенты?»\n"
        "• «Покажи золотых клиентов за 3 месяца»\n\n"
        "📅 Любой период: «за январь», «за квартал», «за полгода»"
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

@bot.message_handler(commands=["dashboard"])
def cmd_dashboard(message):
    if message.chat.id != MY_CHAT_ID:
        return
    safe_send(MY_CHAT_ID, "📊 Генерирую дашборд с динамикой...\n⏳ Загружаю оба периода параллельно")
    try:
        since, until = get_date_range("month")
        results = {}

        def _fetch_current():
            try:
                results["data"] = full_analytics(since, until)
            except Exception as e:
                results["data"] = {"error": str(e)}

        def _fetch_prev():
            try:
                results["prev"] = fetch_comparison_data(since, until)
            except Exception as e:
                results["prev"] = {}

        t1 = threading.Thread(target=_fetch_current)
        t2 = threading.Thread(target=_fetch_prev)
        t1.start()
        t2.start()
        t1.join(timeout=180)
        t2.join(timeout=180)

        data = results.get("data", {})
        prev_data = results.get("prev", {})

        if "error" in data:
            safe_send(MY_CHAT_ID, f"❌ Ошибка загрузки данных: {data['error'][:200]}")
            return

        png_path = generate_dashboard_png(data, "Последние 30 дней", prev_data=prev_data)
        with open(png_path, 'rb') as photo:
            bot.send_photo(MY_CHAT_ID, photo, caption="📊 iStudio Performance Dashboard")
        os.unlink(png_path)
    except Exception as e:
        print(f"Dashboard error: {e}")
        safe_send(MY_CHAT_ID, f"❌ Ошибка генерации дашборда: {str(e)[:200]}")

@bot.message_handler(commands=["debug"])
def cmd_debug(message):
    if message.chat.id != MY_CHAT_ID:
        return
    safe_send(MY_CHAT_ID, "🔍 Диагностика amoCRM...\n⏳")

    report = "🔧 ДИАГНОСТИКА amoCRM\n\n"

    pipelines = get_amocrm_pipelines()
    report += "📋 ВОРОНКИ:\n"
    for p in pipelines:
        report += f"\n  Воронка: {p['name']} (ID: {p['id']})\n"
        report += f"  Этапы: {', '.join(s['name'] for s in p['stages'])}\n"

    report += "\n\n📊 ПОСЛЕДНИЕ 20 СДЕЛОК (полные поля):\n"
    data = amocrm_request("leads", {"limit": 20, "order[created_at]": "desc", "with": "contacts"})
    if data:
        deals = (data.get("_embedded") or {}).get("leads") or []
        all_tags_found = set()
        all_pipelines_found = set()

        for i, deal in enumerate(deals[:20]):
            tags = get_deal_tags(deal)
            all_tags_found.update(tags)
            pipeline_id = deal.get("pipeline_id", 0)
            all_pipelines_found.add(pipeline_id)

            custom_fields = {}
            for cf in (deal.get("custom_fields_values") or []):
                field_name = cf.get("field_name", cf.get("field_id", "?"))
                values = cf.get("values") or []
                val = values[0].get("value", "") if values else ""
                custom_fields[field_name] = val

            source_info = deal.get("_embedded", {}) or {}
            source = source_info.get("source", {}) if isinstance(source_info, dict) else {}

            report += f"\n  --- Сделка #{i+1} ---\n"
            report += f"  Имя: {deal.get('name', '?')}\n"
            report += f"  Цена: {deal.get('price', 0)}\n"
            report += f"  Pipeline ID: {pipeline_id}\n"
            report += f"  Status ID: {deal.get('status_id', 0)}\n"
            report += f"  Теги: {tags if tags else 'НЕТ'}\n"
            if custom_fields:
                report += f"  Доп.поля: {json.dumps(custom_fields, ensure_ascii=False)}\n"

        report += f"\n\n📈 СВОДКА:\n"
        report += f"  Все найденные теги: {sorted(all_tags_found) if all_tags_found else 'НЕТ ТЕГОВ'}\n"
        report += f"  Pipeline IDs: {sorted(all_pipelines_found)}\n"

    safe_send(MY_CHAT_ID, report)

# ============================================================
# VOICE MESSAGE HANDLER
# ============================================================
def transcribe_voice(message):
    if not openai_client:
        return None
    try:
        file_info = bot.get_file(message.voice.file_id)
        file_url = f"https://api.telegram.org/file/bot{TELEGRAM_TOKEN}/{file_info.file_path}"
        response = requests.get(file_url)
        if response.status_code != 200:
            return None

        with tempfile.NamedTemporaryFile(suffix=".ogg", delete=False) as tmp:
            tmp.write(response.content)
            tmp_path = tmp.name

        with open(tmp_path, "rb") as audio_file:
            transcript = openai_client.audio.transcriptions.create(
                model="whisper-1",
                file=audio_file,
                language="ru",
            )

        os.unlink(tmp_path)
        return transcript.text
    except Exception as e:
        print(f"Voice transcription error: {e}")
        try:
            os.unlink(tmp_path)
        except:
            pass
        return None

def _handle_show(show, since, until, user_text, period=None, intent=None):
    """Shared handler logic for both text and voice."""
    if show == "campaign_funnel":
        campaign_tag = (intent or {}).get("campaign_tag", "")
        if not campaign_tag:
            for word in ["карбон", "bbl", "эпил", "вект", "зима", "лето", "пилинг", "ббл"]:
                if word in user_text.lower():
                    campaign_tag = word
                    break
        if not campaign_tag:
            safe_send(MY_CHAT_ID, "❓ Укажи название кампании, например:\n'дашборд карбон ИВР'\n'воронка по BBL'")
            return

        # Default to month if period is today (campaign data needs longer window)
        eff_since, eff_until = since, until
        if period in ("today", None) and not (intent or {}).get("custom_dates"):
            eff_since, eff_until = get_date_range("month")

        safe_send(MY_CHAT_ID, f"📊 Строю дашборд кампании: {campaign_tag}...\n⏳")

        # Fetch current campaign funnel data
        camp_data = analyze_campaign_funnel(campaign_tag, eff_since, eff_until)
        if "error" in camp_data:
            safe_send(MY_CHAT_ID, f"❌ {camp_data['error']}")
            return

        # Fetch Meta spend data for this campaign tag
        meta_camp = {}
        try:
            crm_raw = analyze_crm_data(eff_since, eff_until)
            bt = crm_raw.get("by_campaign_tag", {})
            for k, v in bt.items():
                if campaign_tag.lower() in k.lower():
                    meta_camp = {"spend": v.get("spend", 0), "leads": v.get("deals", 0), "cpl": v.get("avg_deal", 0)}
                    break
            # Try Meta data
            roi = analyze_campaign_roi(eff_since, eff_until)
            if roi and "campaigns" in roi:
                for c in roi["campaigns"]:
                    if campaign_tag.lower() in c.get("name", "").lower():
                        meta_camp = {"spend": c.get("spend", 0), "leads": c.get("leads", 0)}
                        break
        except Exception as e:
            print(f"Meta fetch for campaign dashboard: {e}")

        # Fetch previous period for deltas
        prev_camp = {}
        try:
            ps, pu = get_previous_period(eff_since, eff_until)
            prev_raw = analyze_campaign_funnel(campaign_tag, ps, pu)
            if "error" not in prev_raw:
                prev_camp = prev_raw
                prev_camp["since"] = ps
                prev_camp["until"] = pu
                prev_roi = analyze_campaign_roi(ps, pu)
                if prev_roi and "campaigns" in prev_roi:
                    for c in prev_roi["campaigns"]:
                        if campaign_tag.lower() in c.get("name", "").lower():
                            prev_camp["spend"] = c.get("spend", 0)
                            prev_camp["leads"] = c.get("leads", 0)
                            prev_camp["cpl"] = round(c.get("spend",0)/c.get("leads",1), 2) if c.get("leads") else 0
                            break
        except Exception as e:
            print(f"Prev period fetch for campaign dashboard: {e}")

        period_names = {"today":"Сегодня","yesterday":"Вчера","week":"Неделя","month":"Месяц","3months":"3 месяца","6months":"Полгода","year":"Год"}
        plabel = period_names.get(period, f"{eff_since} — {eff_until}") if period not in ("today", None) else f"{eff_since} — {eff_until}"

        png_path = generate_campaign_dashboard_png(camp_data, meta_camp, plabel, prev_camp or None)
        with open(png_path, 'rb') as photo:
            bot.send_photo(MY_CHAT_ID, photo, caption=f"📊 {campaign_tag} · {plabel}")
        os.unlink(png_path)

        # Also send text summary
        safe_send(MY_CHAT_ID, format_campaign_funnel(camp_data))
        return
    elif show == "client_profile":
        query = (intent or {}).get("phone", "") or (intent or {}).get("client_name", "")
        if not query:
            import re
            # Try phone first
            m = re.search(r"[\+\d][\d\s\-]{7,}", user_text)
            if m:
                query = re.sub(r"[\s\-]", "", m.group())
            else:
                # Try name — extract after keywords
                for kw in ["клиент", "кто такой", "кто такая", "профиль", "расскажи о", "что знаешь о", "найди"]:
                    if kw in user_text.lower():
                        idx = user_text.lower().index(kw) + len(kw)
                        name_part = user_text[idx:].strip().split()[0:2]
                        query = " ".join(name_part)
                        break
        if not query:
            safe_send(MY_CHAT_ID, "❓ Укажи телефон или имя клиента, например:\n'профиль Ирена'\n'кто такой +972501234567'\n'найди Алёна Gold'")
            return
        safe_send(MY_CHAT_ID, f"🔍 Ищу клиента «{query}» в amoCRM...\n⏳")
        data = analyze_client(query)
        safe_send(MY_CHAT_ID, format_client_profile(data))
        data = fetch_all_campaigns_list()
        safe_send(MY_CHAT_ID, generate_response(user_text, data))
    elif show == "crm":
        data = analyze_crm_data(since, until)
        data.pop("_deal_details", None)
        safe_send(MY_CHAT_ID, generate_response(user_text, data, "crm"))
    elif show == "roi":
        data = analyze_campaign_roi(since, until)
        if data and data.get("meta_total_spend", 0) == 0:
            try:
                golden = analyze_golden_clients(since, until)
                data["crm_campaign_quality"] = golden.get("campaign_quality", {})
                data["crm_source_breakdown"] = golden.get("source_breakdown", {})
            except:
                pass
        safe_send(MY_CHAT_ID, generate_response(user_text, data, "roi"))
    elif show == "ltv":
        data = analyze_ltv(since, until)
        safe_send(MY_CHAT_ID, generate_response(user_text, data, "ltv"))
    elif show == "funnel":
        data = analyze_funnel(since, until)
        safe_send(MY_CHAT_ID, generate_response(user_text, data, "funnel"))
    elif show == "golden":
        safe_send(MY_CHAT_ID, "⭐ Ищу золотых клиентов и определяю источники...\n⏳")
        data = analyze_golden_clients(since, until)
        safe_send(MY_CHAT_ID, generate_response(user_text, data, "golden"))
    elif show == "full_report":
        safe_send(MY_CHAT_ID, "📊 Собираю полный отчёт...\n⏳")
        data = full_analytics(since, until)
        safe_send(MY_CHAT_ID, generate_response(user_text, data, "full_report"))
    elif show in ("budget_advice", "dead_campaigns"):
        safe_send(MY_CHAT_ID, "💰 Анализирую данные...\n⏳")
        golden_data = analyze_golden_clients(since, until)
        meta_data = {}
        try:
            roi = analyze_campaign_roi(since, until)
            if roi and "error" not in roi:
                meta_data = roi
        except:
            pass
        combined = {"crm_analysis": golden_data, "meta_analysis": meta_data}
        safe_send(MY_CHAT_ID, generate_response(user_text, combined, show))
    elif show == "best_source":
        safe_send(MY_CHAT_ID, "🔍 Сравниваю источники клиентов...\n⏳")
        data = analyze_golden_clients(since, until)
        safe_send(MY_CHAT_ID, generate_response(user_text, data, "best_source"))
    elif show == "branch_compare":
        safe_send(MY_CHAT_ID, "🏢 Сравниваю филиалы...\n⏳")
        data = analyze_crm_data(since, until)
        data.pop("_deal_details", None)
        safe_send(MY_CHAT_ID, generate_response(user_text, data, "branch_compare"))
    elif show == "dashboard":
        safe_send(MY_CHAT_ID, "📊 Генерирую дашборд с динамикой...\n⏳ Загружаю оба периода параллельно")
        period_names = {
            "today": "Сегодня", "yesterday": "Вчера", "week": "Неделя",
            "month": "Месяц", "3months": "3 месяца", "6months": "Полгода",
            "year": "Год", "all": "Всё время"
        }
        plabel = period_names.get(period, f"{since} — {until}") if period else f"{since} — {until}"

        # Fetch current and previous period IN PARALLEL
        results = {}
        def _fetch_current():
            try:
                results["data"] = full_analytics(since, until)
            except Exception as e:
                print(f"Dashboard current fetch error: {e}")
                results["data"] = {"error": str(e)}

        def _fetch_prev():
            try:
                results["prev"] = fetch_comparison_data(since, until)
            except Exception as e:
                print(f"Dashboard prev fetch error: {e}")
                results["prev"] = {}

        t1 = threading.Thread(target=_fetch_current)
        t2 = threading.Thread(target=_fetch_prev)
        t1.start()
        t2.start()
        t1.join(timeout=180)
        t2.join(timeout=180)

        data = results.get("data", {})
        prev_data = results.get("prev", {})

        if "error" in data:
            safe_send(MY_CHAT_ID, f"❌ Ошибка загрузки данных: {data['error'][:200]}")
            return

        png_path = generate_dashboard_png(data, plabel, prev_data=prev_data)
        with open(png_path, 'rb') as photo:
            bot.send_photo(MY_CHAT_ID, photo, caption=f"📊 iStudio Dashboard · {plabel}")
        os.unlink(png_path)
        summary = generate_response(user_text, data, "full_report")
        safe_send(MY_CHAT_ID, summary)
    else:
        data = fetch_spend_data(period or "today", since, until)
        safe_send(MY_CHAT_ID, generate_response(user_text, data, "spend"))

@bot.message_handler(content_types=["voice", "video_note"])
def handle_voice(message):
    if message.chat.id != MY_CHAT_ID:
        return
    safe_send(MY_CHAT_ID, "🎙 Слушаю...")

    text = transcribe_voice(message)
    if not text:
        safe_send(MY_CHAT_ID, "❌ Не удалось распознать голосовое. Попробуй текстом или перезапиши.")
        return

    safe_send(MY_CHAT_ID, f"✅ Понял: «{text}»")

    intent = detect_intent(text)
    print(f"Voice intent: {intent}")

    show = intent.get("show", "spend")
    period = intent.get("period", "today")
    custom = intent.get("custom_dates")

    since, until = None, None
    if custom and isinstance(custom, dict):
        since = custom.get("since")
        until = custom.get("until")
    elif period == "custom":
        since, until = parse_custom_period(text)

    if not since or not until:
        since, until = get_date_range(period)

    try:
        _handle_show(show, since, until, text, period, intent=intent)
    except Exception as e:
        print(f"Voice handler error: {e}")
        safe_send(MY_CHAT_ID, f"❌ Ошибка: {str(e)[:200]}")

@bot.message_handler(func=lambda m: m.chat.id == MY_CHAT_ID)
def handle_text(message):
    user_text = message.text.strip()
    safe_send(MY_CHAT_ID, "🤔 Анализирую...")
    intent = detect_intent(user_text)
    print(f"Intent: {intent}")

    show = intent.get("show", "spend")
    period = intent.get("period", "today")
    custom = intent.get("custom_dates")

    since, until = None, None
    if custom and isinstance(custom, dict):
        since = custom.get("since")
        until = custom.get("until")
    elif period == "custom":
        since, until = parse_custom_period(user_text)

    if not since or not until:
        since, until = get_date_range(period)

    try:
        _handle_show(show, since, until, user_text, period, intent=intent)
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
    print(f"🎙 Voice: {'✅ OpenAI Whisper' if OPENAI_API_KEY else '❌ no key'}")

    bot.delete_webhook(drop_pending_updates=True)
    time.sleep(1)

    scheduler_thread = threading.Thread(target=run_scheduler, daemon=True)
    scheduler_thread.start()
    print("⏰ Daily 08:00 | Weekly: Sunday 09:00")
    print("📱 Polling...")
    bot.infinity_polling(timeout=60, long_polling_timeout=60)
