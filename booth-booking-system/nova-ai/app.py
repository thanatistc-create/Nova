import os
# Load .env file
_env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
if os.path.exists(_env_path):
    with open(_env_path) as _ef:
        for _line in _ef:
            _line = _line.strip()
            if _line and not _line.startswith("#") and "=" in _line:
                _k, _v = _line.split("=", 1)
                if os.environ.get(_k.strip()) is None:
                    os.environ[_k.strip()] = _v.strip()
import re
import json
import unicodedata
import traceback
import io
import base64

from flask import Flask, request, jsonify
try:
    from supabase import create_client
    SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
    SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY", "")
    supabase_client = create_client(SUPABASE_URL, SUPABASE_KEY) if SUPABASE_URL and SUPABASE_KEY else None
except Exception:
    supabase_client = None

app = Flask(__name__)

SECRET = os.environ.get("NOVA_SECRET", "")
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY", "")

try:
    import google.generativeai as genai
    if GEMINI_API_KEY:
        genai.configure(api_key=GEMINI_API_KEY)
    GEMINI_AVAILABLE = True
except ImportError:
    GEMINI_AVAILABLE = False
    print("[nova] Warning: google-generativeai not installed", flush=True)

try:
    import pandas as pd
    PANDAS_AVAILABLE = True
except ImportError:
    PANDAS_AVAILABLE = False
    print("[nova] Warning: pandas not installed", flush=True)

# ========================
# Keywords (Unicode escaped - Thai chars)
# ========================
# ชื่อร้าน, ร้านค้า, shop, store, brand, vendor
SHOP_KEYWORDS  = ["\u0e0a\u0e37\u0e48\u0e2d\u0e23\u0e49\u0e32\u0e19", "\u0e23\u0e49\u0e32\u0e19\u0e04\u0e49\u0e32", "shop", "store", "brand", "vendor"]
# บูธ, booth, เลขบูธ, no., หมายเลข
BOOTH_KEYWORDS = ["\u0e1a\u0e39\u0e18", "booth", "\u0e40\u0e25\u0e02\u0e1a\u0e39\u0e18", "no.", "\u0e2b\u0e21\u0e32\u0e22\u0e40\u0e25\u0e02"]
# หมายเหตุ, note, remark
NOTE_KEYWORDS  = ["\u0e2b\u0e21\u0e32\u0e22\u0e40\u0e2b\u0e15\u0e38", "note", "remark"]
# ขนาด, size, type, ประเภท
SIZE_KEYWORDS  = ["\u0e02\u0e19\u0e32\u0e14", "size", "type", "\u0e1b\u0e23\u0e30\u0e40\u0e20\u0e17"]
TABLE_CHAIR_KEYWORDS = ["\u0e42\u0e15\u0e4a\u0e30", "\u0e40\u0e01\u0e49\u0e32\u0e2d\u0e35\u0e49", "table", "chair"]
POWER_KEYWORDS = ["\u0e44\u0e1f", "power", "electric", "amp"]

# Thai month abbreviation → month number (1-12)
# ม.ค.=1, ก.พ.=2, มี.ค.=3, เม.ย.=4, พ.ค.=5, มิ.ย.=6
# ก.ค.=7, ส.ค.=8, ก.ย.=9, ต.ค.=10, พ.ย.=11, ธ.ค.=12
THAI_MON = {
    "\u0e21.\u0e04.": 1,  "\u0e21\u0e04":   1,
    "\u0e01.\u0e1e.": 2,  "\u0e01\u0e1e":   2,  "\u0e01.\u0e1e": 2,
    "\u0e21\u0e35.\u0e04.": 3, "\u0e21\u0e35\u0e04": 3,
    "\u0e40\u0e21.\u0e22.": 4, "\u0e40\u0e21\u0e22": 4, "\u0e40\u0e21.\u0e22": 4,
    "\u0e1e.\u0e04.": 5,  "\u0e1e\u0e04":   5,  "\u0e1e.\u0e04": 5, "\u0e1e\u0e04.": 5,
    "\u0e21\u0e34.\u0e22.": 6, "\u0e21\u0e34\u0e22": 6,
    "\u0e01.\u0e04.": 7,  "\u0e01\u0e04":   7,
    "\u0e2a.\u0e04.": 8,  "\u0e2a\u0e04":   8,
    "\u0e01.\u0e22.": 9,  "\u0e01\u0e22":   9,
    "\u0e15.\u0e04.": 10, "\u0e15\u0e04":  10,
    "\u0e1e.\u0e22.": 11, "\u0e1e\u0e22":  11,
    "\u0e18.\u0e04.": 12, "\u0e18\u0e04":  12,
}


def auth_ok(req):
    return req.headers.get("X-Nova-Secret", "") == SECRET or not SECRET


def find_header_row(df_raw):
    """Scan first 6 rows to find the one containing column-header keywords."""
    shop_kw  = [unicodedata.normalize("NFC", k.lower()) for k in SHOP_KEYWORDS]
    booth_kw = [unicodedata.normalize("NFC", k.lower()) for k in BOOTH_KEYWORDS]
    for i in range(min(6, len(df_raw))):
        row_vals = [unicodedata.normalize("NFC", str(v)).lower()
                    for v in df_raw.iloc[i] if pd.notna(v)]
        has_shop  = any(any(kw in v for kw in shop_kw)  for v in row_vals)
        has_booth = any(any(kw in v for kw in booth_kw) for v in row_vals)
        if has_shop or has_booth:
            return i
    return 0


def detect_col(df, keywords, skip_unnamed=True):
    """Return first column name that contains any keyword (NFC-normalised)."""
    for col in df.columns:
        col_str = unicodedata.normalize("NFC", str(col))
        if skip_unnamed and col_str.startswith("Unnamed:"):
            continue
        col_lower = col_str.lower()
        for kw in keywords:
            if unicodedata.normalize("NFC", kw.lower()) in col_lower:
                return col
    return None


def extract_date_range(text):
    if not text:
        return None
    text = unicodedata.normalize("NFC", str(text))
    month_num = None
    for abbr, mn in THAI_MON.items():
        if abbr in text:
            month_num = mn
            break
    if month_num is None:
        return None
    m = re.search(r"(\d{1,2})\s*[-\u2013]\s*(\d{1,2})", text)
    if m:
        return (int(m.group(1)), int(m.group(2)), month_num)
    m = re.search(r"(\d{1,2})", text)
    if m:
        d = int(m.group(1))
        return (d, d, month_num)
    return None


def match_by_date(text, projs):
    dr = extract_date_range(text)
    if not dr:
        return None
    start_d, end_d, month_num = dr
    print(f"[nova] excel date-match: text={text!r} => days={start_d}-{end_d} month={month_num}", flush=True)
    for p in projs:
        s_date = p.get("startDate") or ""
        e_date = p.get("endDate") or ""
        if not s_date or not e_date:
            continue
        try:
            parts_s = s_date.split("-")
            parts_e = e_date.split("-")
            s_m     = int(parts_s[1])
            e_m     = int(parts_e[1])
            s_d_val = int(parts_s[2])
            e_d_val = int(parts_e[2])
            if month_num < s_m or month_num > e_m:
                continue
            proj_start = s_d_val if month_num == s_m else 1
            proj_end   = e_d_val if month_num == e_m else 31
            if start_d <= proj_end and end_d >= proj_start:
                print(f"[nova] excel date-match: matched {text!r} -> {p['name']!r}", flush=True)
                return p["name"]
        except Exception as ex:
            print(f"[nova] date-match error: {ex}", flush=True)
    print(f"[nova] excel date-match: no match for {text!r}", flush=True)
    return None


@app.route("/nova_process_excel", methods=["POST"])
def nova_process_excel():
    if not auth_ok(request):
        return jsonify({"error": "Unauthorized"}), 401
    if not PANDAS_AVAILABLE:
        return jsonify({"error": "pandas not installed"}), 500
    file = request.files.get("file")
    if not file:
        return jsonify({"error": "No file provided"}), 400
    project_names_raw = request.form.get("project_names", "[]")
    try:
        known_projects = json.loads(project_names_raw)
    except Exception:
        known_projects = []
    print(f"[nova] excel:start file={file.filename!r} known_projects={len(known_projects)}", flush=True)
    buf = file.read()
    try:
        xl = pd.ExcelFile(io.BytesIO(buf))
    except Exception as e:
        return jsonify({"error": f"Cannot open Excel: {e}"}), 400
    all_rows = []
    sheet_project_map = {}
    sheets_for_gemini = []
    for sheet_name in xl.sheet_names:
        proj = match_by_date(sheet_name, known_projects)
        if proj:
            sheet_project_map[sheet_name] = proj
            continue
        try:
            df_raw = pd.read_excel(io.BytesIO(buf), sheet_name=sheet_name, header=None, nrows=3)
            if len(df_raw) > 0:
                title_text = " ".join(str(v) for v in df_raw.iloc[0] if pd.notna(v))
                proj = match_by_date(title_text, known_projects)
                if proj:
                    sheet_project_map[sheet_name] = proj
                    continue
        except Exception:
            pass
        sheets_for_gemini.append(sheet_name)
    if sheets_for_gemini and known_projects and GEMINI_AVAILABLE and GEMINI_API_KEY:
        proj_names = [p["name"] if isinstance(p, dict) else str(p) for p in known_projects]
        prompt = (
            "Match each sheet name to the most relevant project name.\n"
            "Return a JSON object mapping sheet name -> project name. Use null if no match.\n"
            "Sheet names:\n" +
            "\n".join(f"  - {s}" for s in sheets_for_gemini) +
            "\nProject names:\n" +
            "\n".join(f"  - {p}" for p in proj_names)
        )
        try:
            model = genai.GenerativeModel("gemini-2.5-flash")
            resp  = model.generate_content(prompt, generation_config={"response_mime_type": "application/json"})
            gemini_map = json.loads(resp.text)
            for s in sheets_for_gemini:
                v = gemini_map.get(s)
                if v:
                    sheet_project_map[s] = v
                    print(f"[nova] excel gemini-match: {s!r} -> {v!r}", flush=True)
        except Exception as e:
            print(f"[nova] excel gemini batch error: {e}", flush=True)
    for sheet_name in xl.sheet_names:
        project_name = sheet_project_map.get(sheet_name, sheet_name)
        try:
            df_raw = pd.read_excel(io.BytesIO(buf), sheet_name=sheet_name, header=None)
            if df_raw.empty:
                continue
            header_row_idx = find_header_row(df_raw)
            df = pd.read_excel(io.BytesIO(buf), sheet_name=sheet_name, header=header_row_idx)
            df.columns = [unicodedata.normalize("NFC", str(c)) for c in df.columns]
            shop_col  = detect_col(df, SHOP_KEYWORDS)
            booth_col = detect_col(df, BOOTH_KEYWORDS)
            note_col  = detect_col(df, NOTE_KEYWORDS)
            size_col  = detect_col(df, SIZE_KEYWORDS)
            table_chair_col = detect_col(df, TABLE_CHAIR_KEYWORDS)
            power_col = detect_col(df, POWER_KEYWORDS)
            print(f"[nova] excel sheet={sheet_name!r} project={project_name!r} shop={shop_col!r} booth={booth_col!r} table_chair={table_chair_col!r} power={power_col!r} rows={len(df)}", flush=True)
            if not shop_col and not booth_col:
                print(f"[nova] excel sheet={sheet_name!r} skipped: no shop/booth cols", flush=True)
                continue
            for _, row in df.iterrows():
                shop_val  = str(row[shop_col]).strip()  if shop_col  and pd.notna(row.get(shop_col))  else ""
                booth_val = str(row[booth_col]).strip() if booth_col and pd.notna(row.get(booth_col)) else ""
                note_val  = str(row[note_col]).strip()  if note_col  and pd.notna(row.get(note_col))  else ""
                size_val  = str(row[size_col]).strip()  if size_col  and pd.notna(row.get(size_col))  else ""
                if not shop_val or shop_val.lower() in ("nan", "none", ""):
                    continue
                if not booth_val or booth_val.lower() in ("nan", "none", ""):
                    continue
                try:
                    bv = float(booth_val)
                    if bv == int(bv):
                        booth_val = str(int(bv))
                except Exception:
                    pass
                tc_price = ""
                if table_chair_col and pd.notna(row.get(table_chair_col)):
                    try: tc_price = float(row[table_chair_col])
                    except: tc_price = ""
                pw_price = ""
                if power_col and pd.notna(row.get(power_col)):
                    try: pw_price = float(row[power_col])
                    except: pw_price = ""
                all_rows.append({"projectName": project_name, "shopName": shop_val, "boothCode": booth_val, "note": note_val, "boothSize": size_val, "tableChairPrice": tc_price, "powerPrice": pw_price})
        except Exception as e:
            print(f"[nova] excel sheet={sheet_name!r} error: {e}", flush=True)
            traceback.print_exc()
    print(f"[nova] excel:done total={len(all_rows)}", flush=True)
    return jsonify({"status": "ok", "rows": all_rows})

@app.route("/nova_generate_summary", methods=["POST"])
def nova_generate_summary():
    if not auth_ok(request):
        return jsonify({"error": "Unauthorized"}), 401
    data = request.get_json(force=True, silent=True) or {}
    projects = data.get("projects", [])
    date_label = data.get("dateLabel", "")
    period = data.get("period", "")
    lines = []
    lines.append(f"\U0001f4ca\u00a0Booking Digest \u2014 {date_label}")
    if period:
        lines.append(f"({period})")
    lines.append("")
    for proj in projects:
        name = proj.get("projectName", "")
        total_booths = proj.get("totalBooths", 0)
        booked_list = proj.get("bookedBooths", [])
        days_left = proj.get("daysUntilStart")
        booked_set = set(str(b) for b in booked_list)
        booked_count = len(booked_set)
        lines.append(f"\U0001f3ea {name}")
        if days_left is not None:
            if days_left > 0:
                lines.append(f"   \u23f0 \u0e40\u0e23\u0e34\u0e48\u0e21\u0e07\u0e32\u0e19\u0e2d\u0e35\u0e01 {days_left} \u0e27\u0e31\u0e19")
            elif days_left == 0:
                lines.append(f"   \u23f0 \u0e40\u0e23\u0e34\u0e48\u0e21\u0e07\u0e32\u0e19\u0e27\u0e31\u0e19\u0e19\u0e35\u0e49!")
            else:
                lines.append(f"   \u23f0 \u0e07\u0e32\u0e19\u0e14\u0e33\u0e40\u0e19\u0e34\u0e19\u0e01\u0e32\u0e23\u0e2d\u0e22\u0e39\u0e48")
        if total_booths > 0:
            lines.append(f"   \U0001f4cb \u0e1a\u0e39\u0e18\u0e17\u0e35\u0e48\u0e08\u0e2d\u0e07: {booked_count}/{total_booths}")
        else:
            lines.append(f"   \U0001f4cb \u0e1a\u0e39\u0e18\u0e17\u0e35\u0e48\u0e08\u0e2d\u0e07: {booked_count}")
        if total_booths > 0:
            grid_parts = []
            for i in range(1, total_booths + 1):
                code = str(i)
                grid_parts.append(f"\u2705{code}" if code in booked_set else f"\u2b1c{code}")
            for chunk_start in range(0, len(grid_parts), 10):
                lines.append("   " + " ".join(grid_parts[chunk_start:chunk_start + 10]))
        elif booked_list:
            sorted_booths = sorted(booked_set, key=lambda x: (len(x), x))
            lines.append("   " + " ".join(f"\u2705{b}" for b in sorted_booths))
        lines.append("")
    summary_text = "\n".join(lines).rstrip()
    print(f"[nova] summary:done projects={len(projects)}", flush=True)
    return jsonify({"summary": summary_text})


@app.route("/nova_process_line_message", methods=["POST"])
def nova_process_line_message():
    if not auth_ok(request):
        return jsonify({"error": "Unauthorized"}), 401
    if not GEMINI_AVAILABLE or not GEMINI_API_KEY:
        return jsonify({"error": "Gemini not configured"}), 503
    data = request.get_json(force=True, silent=True) or {}
    message_type = data.get("type", "text")
    text = data.get("text", "")
    image_b64 = data.get("image", "")
    group_id = data.get("group_id", "")
    print(f"[nova] line:start type={message_type!r} group={group_id!r}", flush=True)

    # Query active projects for this group
    active_projects = []
    if supabase_client and group_id:
        try:
            from datetime import date
            today = date.today().isoformat()
            res = supabase_client.table("line_project_pricing") \
                .select("project_name") \
                .or_(f"group_id.eq.{group_id},group_id.eq.direct") \
                .gte("event_end_date", today) \
                .execute()
            active_projects = [r["project_name"] for r in (res.data or []) if r.get("project_name")]
            print(f"[nova] active_projects={active_projects}", flush=True)
        except Exception as e:
            print(f"[nova] supabase error: {e}", flush=True)

    try:
        model = genai.GenerativeModel("gemini-2.5-flash")
        parts = []
        if image_b64:
            img_bytes = base64.b64decode(image_b64)
            parts.append({"mime_type": "image/jpeg", "data": img_bytes})

        project_hint = ""
        if active_projects:
            project_hint = f"\nACTIVE PROJECTS in this group: {', '.join(active_projects)}\nIMPORTANT: Only classify as \'booking\' if projectName matches one of these active projects. If no match → classify as \'unknown\'."
        else:
            project_hint = "\nNo active projects found for this group. If you cannot confidently identify project, shop, and booth → classify as \'unknown\'."

        prompt = (
            "Classify this LINE message for a Thai booth booking bot.\n"
            "Return JSON only. Fields:\n"
            "- classification: \"booking\" | \"expense\" | \"income_slip\" | \"cancellation\" | \"tax_invoice\" | \"general_chat\" | \"unknown\"\n"
            "- projectName: string or null (must match active projects if booking)\n"
            "- shopName: string or null\n"
            "- phone: string or null\n"
            "- boothCode: string or null\n"
            "- amount: number or null\n"
            "- vendorName: string or null\n"
            "- expenseType: string or null\n"
            "\nRULES:\n"
            "- general_chat: screenshots of chats, conversations, LINE messages\n"
            "- income_slip: bank transfer slip where customer pays booth fee\n"
            "- expense: organizer pays vendor/supplier\n"
            "- booking: clear booth reservation with shop name + booth number\n"
            "- unknown: anything unclear or not matching above\n"
        ) + project_hint
        if text:
            prompt += f"\nMessage text:\n{text}"
        parts.append(prompt)
        resp = model.generate_content(parts, generation_config={"response_mime_type": "application/json"})
        result = json.loads(resp.text)
        print(f"[nova] line:done classification={result.get('classification')!r}", flush=True)
        return jsonify(result)
    except Exception as e:
        print(f"[nova] line:error {e}", flush=True)
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500


@app.route("/nova_read_floor_plan", methods=["POST"])
def nova_read_floor_plan():
    if not auth_ok(request):
        return jsonify({"error": "Unauthorized"}), 401
    if not GEMINI_AVAILABLE or not GEMINI_API_KEY:
        return jsonify({"error": "Gemini not configured"}), 503
    data = request.get_json(force=True, silent=True) or {}
    image_b64 = data.get("image", "")
    if not image_b64:
        return jsonify({"error": "No image provided"}), 400
    try:
        model = genai.GenerativeModel("gemini-2.5-flash")
        img_bytes = base64.b64decode(image_b64)
        prompt = (
            "This is a booth/event floor plan image. Extract the following information:\n"
            "1. Total number of booths (count all booth spaces shown)\n"
            "2. Event/project name if visible\n"
            "3. Event start date if visible\n"
            "4. Event end date if visible\n\n"
            "Return JSON only with these fields:\n"
            "- is_floor_plan: true/false\n"
            "- total_booths: number or null\n"
            "- project_name: string or null\n"
            "- start_date: string YYYY-MM-DD or null\n"
            "- end_date: string YYYY-MM-DD or null\n"
            "- notes: string (brief description of what you see)\n"
        )
        resp = model.generate_content(
            [{"mime_type": "image/jpeg", "data": img_bytes}, prompt],
            generation_config={"response_mime_type": "application/json"}
        )
        result = json.loads(resp.text)
        print(f"[nova] floor_plan:done is_floor_plan={result.get('is_floor_plan')} booths={result.get('total_booths')}", flush=True)
        return jsonify({"status": "ok", **result})
    except Exception as e:
        print(f"[nova] floor_plan:error {e}", flush=True)
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500



@app.route("/nova_generate_digest", methods=["POST"])
def nova_generate_digest():
    if not auth_ok(request):
        return jsonify({"error": "Unauthorized"}), 401
    data = request.get_json(force=True, silent=True) or {}
    group_id = data.get("group_id", "")
    date_str = data.get("date", "") or data.get("date_str", "")
    hour = data.get("hour", 9)

    if not supabase_client or not group_id:
        return jsonify({"error": "Missing group_id or supabase"}), 400

    try:
        from datetime import date, datetime, timezone, timedelta
        today = date_str or date.today().isoformat()
        BKK = timezone(timedelta(hours=7))
        THAI_MONTHS = ["ม.ค.","ก.พ.","มี.ค.","เม.ย.","พ.ค.","มิ.ย.","ก.ค.","ส.ค.","ก.ย.","ต.ค.","พ.ย.","ธ.ค."]

        def fmt_thai(d):
            if not d: return "?"
            dt = datetime.fromisoformat(d[:10])
            return f"{dt.day} {THAI_MONTHS[dt.month-1]} {dt.year+543}"

        def days_until(d):
            if not d: return None
            return (datetime.fromisoformat(d[:10]).date() - datetime.fromisoformat(today).date()).days

        # today range UTC for Bangkok
        y,m,dy = int(today[:4]),int(today[5:7]),int(today[8:10])
        start_utc = datetime(y,m,dy,0,0,0,tzinfo=BKK).astimezone(timezone.utc)
        end_utc = start_utc + timedelta(days=1)

        # Get active projects
        proj_res = supabase_client.table("line_project_pricing") \
            .select("project_name,event_start_date,event_end_date,total_booths") \
            .or_(f"group_id.eq.{group_id},group_id.eq.direct") \
            .gte("event_end_date", today) \
            .execute()
        projects = proj_res.data or []

        if not projects:
            return jsonify({"messages": [], "projects": 0})

        def _proj_match(stored, pricing):
            """Accept if either name contains the other (handles abbreviated stored names)."""
            if not stored: return False
            s = stored.lower().strip()
            p = pricing.lower().strip()
            return s in p or p in s

        messages = []
        header = f"สรุปยอดจองพื้นที่ (อัปเดต: {today} {str(hour).zfill(2)}:00)"

        for proj in projects:
            pname = proj["project_name"]
            pkey = pname.lower().strip()
            # Use first word as broad DB filter; Python-side _proj_match handles abbreviated names
            pname_first_word = pname.split()[0] if pname.split() else pname[:10]
            start = proj.get("event_start_date")
            end = proj.get("event_end_date")
            total = proj.get("total_booths")

            # Date label
            date_label = ""
            if start and end:
                date_label = f" ({fmt_thai(start)} - {fmt_thai(end)})"
            d = days_until(start)
            if d is not None:
                if d > 0: date_label += f" (งานนี้ใกล้เริ่มในอีก {d} วัน)"
                elif d == 0: date_label += " (งานเริ่มวันนี้!)"
                else: date_label += " (งานเริ่มแล้ว)"

            # Today bookings — use created_at; broad first-word filter + Python exact match
            today_res = supabase_client.table("line_booking_records") \
                .select("booth_code,shop_name,project_name") \
                .or_(f"group_id.eq.{group_id},group_id.eq.direct") \
                .eq("booking_status", "booked") \
                .ilike("project_name", f"%{pname_first_word}%") \
                .gte("created_at", start_utc.strftime("%Y-%m-%dT%H:%M:%SZ")) \
                .lt("created_at", end_utc.strftime("%Y-%m-%dT%H:%M:%SZ")) \
                .execute()
            today_bookings = [b for b in (today_res.data or []) if _proj_match(b.get("project_name",""), pname)]
            # Dedup today_bookings by booth_code
            seen_today = {}
            for b in today_bookings:
                bc = b.get("booth_code")
                if bc is not None: seen_today[str(bc)] = b
            today_bookings = list(seen_today.values())

            # All bookings — order by created_at DESC so dedup keeps newest record per booth
            start_date = proj.get("event_start_date", "")
            cutoff_dt = (datetime.fromisoformat(start_date) - timedelta(days=180)).strftime("%Y-%m-%d") if start_date else "2000-01-01"
            all_q = supabase_client.table("line_booking_records") \
                .select("booth_code,shop_name,table_free_qty,table_extra_qty,chair_free_qty,chair_extra_qty,power_label,power_amp,event_start_date,project_name") \
                .or_(f"group_id.eq.{group_id},group_id.eq.direct") \
                .eq("booking_status", "booked") \
                .ilike("project_name", f"%{pname_first_word}%") \
                .gte("created_at", cutoff_dt)
            if start_date:
                all_q = all_q.or_(f"event_start_date.eq.{start_date},event_start_date.is.null")
            all_res = all_q.order("created_at", desc=True).execute()
            all_bookings_raw = [b for b in (all_res.data or []) if _proj_match(b.get("project_name",""), pname)]
            # Deduplicate by booth_code (keep newest = first after DESC order), filter out booths beyond total capacity
            seen = {}
            for b in all_bookings_raw:
                bc = b.get("booth_code")
                if bc is None: continue
                # Normalize: strip non-numeric prefix e.g. "BOOTHNO.29" → "29"
                bc_str = str(bc).strip()
                m = re.search(r'(\d+)$', bc_str)
                if m: bc_str = m.group(1)
                try:
                    if total and int(bc_str) > int(total): continue
                except: continue  # skip non-numeric booth codes (old events)
                if bc_str not in seen:
                    b = dict(b); b["booth_code"] = bc_str
                    seen[bc_str] = b
            all_bookings = list(seen.values())
            all_bookings.sort(key=lambda b: (int(b["booth_code"]) if str(b.get("booth_code","")).isdigit() else 9999))
            booked = len(all_bookings)
            print(f"[DEBUG] {pname}: raw={len(all_bookings_raw)} dedup={booked} total={total}", flush=True)
            total_label = f"{booked}/{total}" if total else str(booked)

            lines = [header, "", f"{pname}{date_label}"]
            lines.append("[อัปเดตจองใหม่วันนี้]")
            if today_bookings:
                for b in today_bookings[:20]:
                    lines.append(f"• บูธ {b.get('booth_code','-')} | ร้าน {b.get('shop_name','-')}")
            else:
                lines.append("ไม่มีการจองใหม่วันนี้")

            lines.append(f"[สรุปพื้นที่ทั้งหมด ({total_label} บูธ)] (✅ = จองแล้ว, ⬜ = ว่าง)")

            booth_map = {str(b.get("booth_code","")).strip(): b for b in all_bookings}
            total_n = int(total) if total else None
            if total_n:
                for i in range(1, total_n+1):
                    bc = str(i)
                    b = booth_map.get(bc)
                    if b:
                        t = (b.get("table_free_qty") or 0) + (b.get("table_extra_qty") or 0)
                        c = (b.get("chair_free_qty") or 0) + (b.get("chair_extra_qty") or 0)
                        pw = b.get("power_label") or (f"{b.get('power_amp')}A" if b.get("power_amp") else "-")
                        lines.append(f"✅ บูธ {bc} | {b.get('shop_name','-')} | โต๊ะ {t} | เก้าอี้ {c} | ไฟ {pw}")
                    else:
                        lines.append(f"⬜ บูธ {bc} | - ว่าง -")
            else:
                for b in all_bookings:
                    bc = str(b.get("booth_code","-"))
                    t = (b.get("table_free_qty") or 0) + (b.get("table_extra_qty") or 0)
                    c = (b.get("chair_free_qty") or 0) + (b.get("chair_extra_qty") or 0)
                    pw = b.get("power_label") or "-"
                    lines.append(f"✅ บูธ {bc} | {b.get('shop_name','-')} | โต๊ะ {t} | เก้าอี้ {c} | ไฟ {pw}")

            total_t = sum((b.get("table_free_qty") or 0)+(b.get("table_extra_qty") or 0) for b in all_bookings)
            total_c = sum((b.get("chair_free_qty") or 0)+(b.get("chair_extra_qty") or 0) for b in all_bookings)
            lines.append("─────────────────────")
            lines.append(f"Inventory รวมทั้งงาน: โต๊ะทั้งหมด: {total_t} ตัว | เก้าอี้ทั้งหมด: {total_c} ตัว")

            msg = "\n".join(lines)
            # Split if > 4900 chars
            if len(msg) > 4900:
                chunks = []
                chunk = []
                cur_len = 0
                for line in lines:
                    if cur_len + len(line) > 4800 and chunk:
                        chunks.append("\n".join(chunk))
                        chunk = [line]
                        cur_len = len(line)
                    else:
                        chunk.append(line)
                        cur_len += len(line) + 1
                if chunk:
                    chunks.append("\n".join(chunk))
                messages.extend(chunks)
            else:
                messages.append(msg)

        print(f"[nova] digest:done group={group_id} projects={len(projects)} messages={len(messages)}", flush=True)
        return jsonify({"messages": messages, "projects": len(projects)})
    except Exception as e:
        import traceback as tb
        tb.print_exc()
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    print(f"[nova] Starting on port {port}", flush=True)
    app.run(host="0.0.0.0", port=port, debug=False)
