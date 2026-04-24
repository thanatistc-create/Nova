import crypto from "node:crypto";
import fs from "node:fs";
import http from "node:http";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { createClient } from "@supabase/supabase-js";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const ROOT_DIR = path.resolve(__dirname, "..");

function loadEnvFile(filepath) {
  if (!fs.existsSync(filepath)) return;
  const raw = fs.readFileSync(filepath, "utf8");
  for (const line of raw.split(/\r?\n/)) {
    const trimmed = line.trim();
    if (!trimmed || trimmed.startsWith("#")) continue;
    const eqIndex = trimmed.indexOf("=");
    if (eqIndex < 1) continue;
    const key = trimmed.slice(0, eqIndex).trim();
    const value = trimmed.slice(eqIndex + 1).trim();
    if (!process.env[key]) {
      process.env[key] = value.replace(/^["']|["']$/g, "");
    }
  }
}

loadEnvFile(path.join(ROOT_DIR, ".env"));
loadEnvFile(path.join(__dirname, ".env"));

const PORT = Number(process.env.LINE_BOT_PORT ?? process.env.PORT ?? 8787);
const LINE_CHANNEL_SECRET = process.env.LINE_CHANNEL_SECRET ?? "";
const LINE_CHANNEL_ACCESS_TOKEN = process.env.LINE_CHANNEL_ACCESS_TOKEN ?? "";
const SUPABASE_URL = process.env.SUPABASE_URL ?? process.env.VITE_SUPABASE_URL ?? "";
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY ?? "";
const LINE_PUBLIC_BASE_URL = (process.env.LINE_PUBLIC_BASE_URL ?? "").trim().replace(/\/+$/, "");
const LINE_EXPORT_TOKEN = (process.env.LINE_EXPORT_TOKEN ?? "").trim();
const LINE_EXPENSE_GROUP_IDS = (process.env.LINE_EXPENSE_GROUP_IDS ?? "")
  .split(",").map(s => s.trim()).filter(Boolean);
const LINE_TIMEZONE = process.env.LINE_TIMEZONE ?? "Asia/Bangkok";
const LINE_OCR_ENABLED = String(process.env.LINE_OCR_ENABLED ?? "true").toLowerCase() !== "false";
const LINE_OCR_LANG = process.env.LINE_OCR_LANG ?? "eng+tha";
const LINE_FETCH_TIMEOUT_MS = normalizeTimeoutMs(process.env.LINE_FETCH_TIMEOUT_MS, 15000);
const LINE_OCR_TIMEOUT_MS = normalizeTimeoutMs(process.env.LINE_OCR_TIMEOUT_MS, 12000);
const LINE_AI_TIMEOUT_MS = normalizeTimeoutMs(process.env.LINE_AI_TIMEOUT_MS, 20000);
const OPENAI_API_KEY = (process.env.OPENAI_API_KEY ?? "").trim();
const GEMINI_API_KEY = (process.env.GEMINI_API_KEY ?? "").trim();
const LINE_AI_PROVIDER = (process.env.LINE_AI_PROVIDER ?? (GEMINI_API_KEY ? "gemini" : OPENAI_API_KEY ? "openai" : "")).trim().toLowerCase();
const LINE_AI_IMAGE_MODEL = (process.env.LINE_AI_IMAGE_MODEL ?? (LINE_AI_PROVIDER === "gemini" ? "gemini-2.5-flash" : "gpt-5-mini")).trim();
const LINE_AI_IMAGE_FALLBACK_ENABLED =
  String(process.env.LINE_AI_IMAGE_FALLBACK_ENABLED ?? "true").toLowerCase() !== "false" &&
  ((LINE_AI_PROVIDER === "gemini" && Boolean(GEMINI_API_KEY)) ||
    (LINE_AI_PROVIDER !== "gemini" && Boolean(OPENAI_API_KEY)));
const LINE_IMAGE_GROUP_REPLY_MODE = (process.env.LINE_IMAGE_GROUP_REPLY_MODE ?? "immediate").trim().toLowerCase();
const LINE_IMAGE_SUMMARY_ENABLED =
  String(process.env.LINE_IMAGE_SUMMARY_ENABLED ?? (LINE_IMAGE_GROUP_REPLY_MODE === "digest" ? "true" : "false")).toLowerCase() !== "false";
const LINE_IMAGE_SUMMARY_HOURS = parseDigestHours(process.env.LINE_IMAGE_SUMMARY_HOURS ?? "9,17");
const LINE_IMAGE_SUMMARY_MAX_ITEMS = normalizeDigestMaxItems(process.env.LINE_IMAGE_SUMMARY_MAX_ITEMS, 8);
const IMAGE_DIGEST_STATE_FILE = path.join(__dirname, ".line-image-digest.json");
const LINE_AI_TEXT_FALLBACK_ENABLED =
  String(process.env.LINE_AI_TEXT_FALLBACK_ENABLED ?? "true").toLowerCase() !== "false" &&
  ((LINE_AI_PROVIDER === "gemini" && Boolean(GEMINI_API_KEY)) ||
    (LINE_AI_PROVIDER !== "gemini" && Boolean(OPENAI_API_KEY)));
const LINE_AI_TEXT_MODEL = (process.env.LINE_AI_TEXT_MODEL ?? LINE_AI_IMAGE_MODEL).trim();
const _novaApiUrl = (process.env.NOVA_API_URL ?? "").trim();
const _novaBaseUrl = (process.env.NOVA_BASE_URL ?? "").trim().replace(/\/$/, "");
const NOVA_API_URL = _novaApiUrl || (_novaBaseUrl ? `${_novaBaseUrl}/nova_process_line_message` : "");
const NOVA_SECRET_KEY = (process.env.NOVA_SECRET_KEY ?? "").trim();
const NOVA_ENABLED = Boolean(NOVA_API_URL);
const NOVA_BASE_URL = NOVA_API_URL ? NOVA_API_URL.replace(/\/nova_process_line_message.*$/, "").replace(/\/$/, "") : null;

if (!LINE_CHANNEL_SECRET || !LINE_CHANNEL_ACCESS_TOKEN) {
  console.error("Missing LINE_CHANNEL_SECRET or LINE_CHANNEL_ACCESS_TOKEN");
  process.exit(1);
}

if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
  console.error("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY");
  process.exit(1);
}

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, {
  auth: { autoRefreshToken: false, persistSession: false },
});
const PENDING_REPLACE_TTL_MS = 15 * 60 * 1000;
const BOOKING_CONFLICT_LOOKBACK_DAYS = 60;
const pendingReplacementByActor = new Map();
const pendingExpenseByActor = new Map();
const pendingProjectSelectionByActor = new Map();
const EXPENSE_MIGRATION_HINT = "Expense table not found. Run supabase/line_bot_expense_migration.sql in Supabase SQL Editor first.";
const SALES_MIGRATION_HINT = "Sales schema not ready. Run supabase/line_bot_sales_migration.sql in Supabase SQL Editor first.";
const DEFAULT_EXPENSE_PROJECT_NAME = "ไม่ระบุโปรเจกต์";

function normalizeTimeoutMs(value, fallback) {
  const parsed = Number(value);
  if (!Number.isFinite(parsed) || parsed <= 0) return fallback;
  return Math.floor(parsed);
}

function normalizeDigestMaxItems(value, fallback) {
  const parsed = Number(value);
  if (!Number.isFinite(parsed) || parsed <= 0) return fallback;
  return Math.min(20, Math.floor(parsed));
}

function parseDigestHours(value) {
  const parsed = Array.from(
    new Set(
      String(value ?? "")
        .split(/[^0-9]+/)
        .map((part) => Number(part))
        .filter((hour) => Number.isInteger(hour) && hour >= 0 && hour <= 23),
    ),
  ).sort((a, b) => a - b);
  return parsed.length ? parsed : [9, 17];
}

function getTimePartsInTz(date, timeZone = LINE_TIMEZONE) {
  const parts = new Intl.DateTimeFormat("en-CA", {
    timeZone,
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    hourCycle: "h23",
  }).formatToParts(date);
  const year = parts.find((part) => part.type === "year")?.value;
  const month = parts.find((part) => part.type === "month")?.value;
  const day = parts.find((part) => part.type === "day")?.value;
  const hour = Number(parts.find((part) => part.type === "hour")?.value ?? NaN);
  const minute = Number(parts.find((part) => part.type === "minute")?.value ?? NaN);
  return {
    dateStr: year && month && day ? `${year}-${month}-${day}` : "",
    hour: Number.isInteger(hour) ? hour : 0,
    minute: Number.isInteger(minute) ? minute : 0,
  };
}
function elapsedMs(startedAt) {
  return Math.max(0, Date.now() - startedAt);
}

async function runWithTimeout(label, timeoutMs, task) {
  let timeoutId = null;
  try {
    return await Promise.race([
      Promise.resolve().then(task),
      new Promise((_, reject) => {
        timeoutId = setTimeout(() => {
          reject(new Error(`${label} timed out after ${timeoutMs}ms`));
        }, timeoutMs);
      }),
    ]);
  } finally {
    if (timeoutId) clearTimeout(timeoutId);
  }
}

function isMissingExpenseTableError(error) {
  const raw = `${error?.message ?? ""} ${error?.hint ?? ""}`;
  return error?.code === "PGRST205" && /line_expense_records/i.test(raw);
}

function isMissingSalesSchemaError(error) {
  const raw = `${error?.message ?? ""} ${error?.hint ?? ""}`;
  return /line_project_pricing|booth_price/i.test(raw);
}

function jsonResponse(res, statusCode, payload) {
  const body = JSON.stringify(payload);
  res.writeHead(statusCode, {
    "Content-Type": "application/json; charset=utf-8",
    "Content-Length": Buffer.byteLength(body),
  });
  res.end(body);
}

function textResponse(res, statusCode, body, headers = {}) {
  const payload = String(body ?? "");
  res.writeHead(statusCode, {
    "Content-Type": "text/plain; charset=utf-8",
    "Content-Length": Buffer.byteLength(payload),
    ...headers,
  });
  res.end(payload);
}

function verifySignature(rawBody, signature) {
  if (!signature) return false;
  const digest = crypto
    .createHmac("sha256", LINE_CHANNEL_SECRET)
    .update(rawBody)
    .digest("base64");
  const digestBuf = Buffer.from(digest, "utf8");
  const signatureBuf = Buffer.from(signature, "utf8");
  if (digestBuf.length !== signatureBuf.length) return false;
  return crypto.timingSafeEqual(digestBuf, signatureBuf);
}

async function replyMessage(replyToken, messages) {
  if (!replyToken || !messages?.length) return;
  const payload = {
    replyToken,
    messages: messages.map((m) =>
      typeof m === "string" ? { type: "text", text: m } : m,
    ),
  };

  const response = await fetch("https://api.line.me/v2/bot/message/reply", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Authorization: `Bearer ${LINE_CHANNEL_ACCESS_TOKEN}`,
    },
    body: JSON.stringify(payload),
  });

  if (!response.ok) {
    const text = await response.text();
    console.error("LINE reply failed:", response.status, text);
  }
}

async function pushMessage(target, messages) {
  if (!target || !messages?.length) return false;
  const payload = {
    to: target,
    messages: messages.map((m) => (typeof m === "string" ? { type: "text", text: m } : m)),
  };
  const response = await fetch("https://api.line.me/v2/bot/message/push", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Authorization: `Bearer ${LINE_CHANNEL_ACCESS_TOKEN}`,
    },
    body: JSON.stringify(payload),
  });
  if (!response.ok) {
    const text = await response.text();
    console.error("LINE push failed:", response.status, text);
    return false;
  }
  return true;
}

function getImageDigestPushTarget(source) {
  return source?.groupId ?? source?.roomId ?? "";
}

function shouldSuppressImmediateImageReply(event) {
  const sourceType = event?.source?.type ?? "";
  return LINE_IMAGE_GROUP_REPLY_MODE === "digest" && (sourceType === "group" || sourceType === "room");
}

function readImageDigestState() {
  try {
    if (!fs.existsSync(IMAGE_DIGEST_STATE_FILE)) return { events: [], sentSlots: {} };
    const parsed = JSON.parse(fs.readFileSync(IMAGE_DIGEST_STATE_FILE, "utf8"));
    return {
      events: Array.isArray(parsed?.events) ? parsed.events : [],
      sentSlots: parsed?.sentSlots && typeof parsed.sentSlots === "object" ? parsed.sentSlots : {},
    };
  } catch (error) {
    console.error("Failed to read image digest state:", error);
    return { events: [], sentSlots: {} };
  }
}

function pruneImageDigestState(state) {
  const now = Date.now();
  const eventCutoff = now - 14 * 24 * 60 * 60 * 1000;
  const slotCutoff = now - 7 * 24 * 60 * 60 * 1000;
  const events = (state?.events ?? []).filter((item) => {
    const timestamp = Date.parse(item?.occurredAt ?? "");
    if (!Number.isFinite(timestamp)) return false;
    return timestamp >= eventCutoff || !item?.sentAt;
  });
  const sentSlots = {};
  for (const [key, value] of Object.entries(state?.sentSlots ?? {})) {
    const timestamp = Date.parse(value ?? "");
    if (Number.isFinite(timestamp) && timestamp >= slotCutoff) sentSlots[key] = value;
  }
  return { events, sentSlots };
}

function writeImageDigestState(state) {
  try {
    const next = pruneImageDigestState(state);
    fs.mkdirSync(path.dirname(IMAGE_DIGEST_STATE_FILE), { recursive: true });
    fs.writeFileSync(IMAGE_DIGEST_STATE_FILE, JSON.stringify(next, null, 2), "utf8");
    return true;
  } catch (error) {
    console.error("Failed to write image digest state:", error);
    return false;
  }
}

function queueImageDigestEvent(eventPayload) {
  if ((!LINE_IMAGE_SUMMARY_ENABLED && !LINE_AI_TEXT_FALLBACK_ENABLED) || !eventPayload?.pushTarget) return;
  const state = readImageDigestState();
  state.events.push({
    id: eventPayload.id ?? crypto.randomUUID(),
    occurredAt: new Date().toISOString(),
    ...eventPayload,
  });
  writeImageDigestState(state);
}

function buildImageDigestEvent(event, payload = {}) {
  const source = event?.source ?? {};
  const pushTarget = getImageDigestPushTarget(source);
  if (!pushTarget) return null;
  return {
    pushTarget,
    groupId: getGroupIdFromSource(source),
    sourceType: source?.type ?? "",
    messageId: event?.message?.id ?? null,
    category: payload.category ?? "failure",
    status: payload.status ?? "needs_review",
    sourceTag: normalizeSpaces(payload.sourceTag).slice(0, 40),
    reason: normalizeSpaces(payload.reason).slice(0, 80),
    projectName: normalizeSpaces(payload.projectName).slice(0, 160),
    shopName: normalizeSpaces(payload.shopName).slice(0, 160),
    boothCode: normalizeSpaces(payload.boothCode).slice(0, 40),
    vendorName: normalizeSpaces(payload.vendorName).slice(0, 160),
    amount: Number.isFinite(Number(payload.amount)) ? Number(payload.amount) : null,
    currency: normalizeSpaces(payload.currency ?? "THB").toUpperCase() || "THB",
    detail: normalizeSpaces(payload.detail).slice(0, 240),
  };
}

function formatImageDigestFailureReason(reason) {
  switch (reason) {
    case "fetch_failed": return "could not fetch image";
    case "ocr_failed": return "OCR failed";
    case "unclassified": return "could not classify image";
    case "save_failed": return "read image but could not save";
    case "needs_confirmation": return "duplicate booth needs review";
    case "image_reading_disabled": return "image reading disabled";
    case "pipeline_error": return "unexpected pipeline error";
    case "ai_parsed": return "⚠️ AI parsed - กรุณาตรวจสอบ";
    case "flowaccount_link": return "⚠️ Flowaccount - กรุณาตรวจสอบ";
    default: return normalizeSpaces(reason) || "needs manual review";
  }
}

function formatImageDigestEventLine(item) {
  if (item?.status === "saved" && item?.category === "expense") {
    return `- expense | ${item.projectName || DEFAULT_EXPENSE_PROJECT_NAME} | ${item.amount === null ? "-" : formatAmount(item.amount, item.currency || "THB")} | ${item.vendorName || "-"}`;
  }
  if (item?.status === "saved" && item?.category === "booking") {
    return `- booking | ${item.projectName || "-"} | ${item.shopName || "-"} | booth ${item.boothCode || "-"}`;
  }
  const parts = [formatImageDigestFailureReason(item?.reason)];
  if (item?.projectName) parts.push(item.projectName);
  if (item?.shopName) parts.push(item.shopName);
  if (item?.boothCode) parts.push(`booth ${item.boothCode}`);
  if (item?.vendorName) parts.push(item.vendorName);
  if (item?.amount !== null && item?.amount !== undefined) parts.push(formatAmount(item.amount, item.currency || "THB"));
  if (item?.detail) parts.push(item.detail);
  return `- review | ${parts.join(" | ")}`;
}

async function buildBookingDigestMessage(slot, groupId, reviewItems, projectFilter = "") {
  const todayStr = slot.dateStr;
  const todayRange = getDailyRangeUtc(todayStr);

  // Thai month abbreviations
  const THAI_MONTHS = ["ม.ค.","ก.พ.","มี.ค.","เม.ย.","พ.ค.","มิ.ย.","ก.ค.","ส.ค.","ก.ย.","ต.ค.","พ.ย.","ธ.ค."];
  const formatDateThai = (dateStr) => {
    if (!dateStr) return "?";
    const d = new Date(dateStr);
    return `${d.getUTCDate()} ${THAI_MONTHS[d.getUTCMonth()]} ${d.getUTCFullYear() + 543}`;
  };
  const daysUntilDate = (dateStr) => {
    if (!dateStr) return null;
    return Math.ceil((new Date(dateStr) - new Date(todayStr)) / 86400000);
  };

  // Build pricing map: lowercase key → { canonicalName, startDate, endDate, totalBooths }
  // Using lowercase keys so "URBAN Craft" and "Urban Craft" map to the same entry
  const pricingMap = new Map(); // lowerKey → { canonicalName, startDate, endDate, totalBooths }
  const pricingActiveKeys = new Set(); // lowercase keys with end_date >= today
  const pricingKnownKeys = new Set();  // all lowercase keys in pricing table
  {
    let pricingQuery = supabase
      .from("line_project_pricing")
      .select("project_name, event_start_date, event_end_date, total_booths");
    if (groupId) pricingQuery = pricingQuery.or(`group_id.eq.${groupId},group_id.eq.direct`);
    const { data } = await pricingQuery;
    for (const row of data ?? []) {
      if (!row.project_name) continue;
      const key = row.project_name.toLowerCase().trim();
      pricingKnownKeys.add(key);
      pricingMap.set(key, {
        canonicalName: row.project_name,
        startDate: row.event_start_date ?? null,
        endDate: row.event_end_date ?? null,
        totalBooths: row.total_booths ?? null,
      });
      if (row.event_end_date >= todayStr) pricingActiveKeys.add(key);
    }
  }

  // Get all distinct project names from bookings, grouped by lowercase key
  // bookingCanonicalMap: lowerKey → best canonical name seen in bookings
  const bookingCanonicalMap = new Map();
  {
    let bookingNamesQuery = supabase
      .from("line_booking_records")
      .select("project_name")
      .eq("booking_status", "booked")
      .not("project_name", "is", null);
    if (groupId) bookingNamesQuery = bookingNamesQuery.or(`group_id.eq.${groupId},group_id.eq.direct`);
    const { data } = await bookingNamesQuery;
    for (const row of data ?? []) {
      if (!row.project_name) continue;
      const key = row.project_name.toLowerCase().trim();
      // Prefer pricing canonical name; otherwise keep first seen
      if (!bookingCanonicalMap.has(key)) bookingCanonicalMap.set(key, row.project_name);
    }
  }

  const todayBookingKeys = new Set();
  if (todayRange) {
    let todayBookingNamesQuery = supabase
      .from("line_booking_records")
      .select("project_name")
      .eq("booking_status", "booked")
      .not("project_name", "is", null)
      .gte("booked_at", todayRange.startIso)
      .lt("booked_at", todayRange.endIso);
    if (groupId) todayBookingNamesQuery = todayBookingNamesQuery.or(`group_id.eq.${groupId},group_id.eq.direct`);
    const { data } = await todayBookingNamesQuery;
    for (const row of data ?? []) {
      if (!row.project_name) continue;
      todayBookingKeys.add(row.project_name.toLowerCase().trim());
    }
  }

  // Build active project list: only projects with future end_date in pricing
  // Keep today's booked projects visible even if their pricing row has not been created yet.
  const activeKeySet = new Set([...pricingActiveKeys, ...todayBookingKeys]);
  // Resolve canonical display names: pricing wins over booking raw name
  const resolveCanonical = (key) => pricingMap.get(key)?.canonicalName ?? bookingCanonicalMap.get(key) ?? key;
  let activeProjects = [...activeKeySet].sort().map((key) => ({ key, name: resolveCanonical(key) }));
  if (!activeProjects.length && groupId) {
    activeProjects = [...pricingActiveKeys].sort().map((key) => ({ key, name: resolveCanonical(key) }));
  }
  if (projectFilter) {
    const f = projectFilter.toLowerCase().trim();
    activeProjects = activeProjects.filter((p) => p.key.includes(f) || p.name.toLowerCase().includes(f));
  }

  const lines = [`สรุปยอดจองพื้นที่ (อัปเดต: ${todayStr} ${String(slot.hour).padStart(2, "0")}:00)`];
  const projectsData = []; // collected for Nova summary endpoint

  for (const { key: projectKey, name: projectName } of activeProjects) {
    const pricing = pricingMap.get(projectKey) ?? {};
    const { startDate, endDate, totalBooths } = pricing;

    // Project header line
    let dateRange = "";
    if (startDate && endDate) dateRange = ` (${formatDateThai(startDate)} - ${formatDateThai(endDate)})`;
    else if (endDate) dateRange = ` (ถึง ${formatDateThai(endDate)})`;
    let daysLabel = "";
    if (startDate) {
      const d = daysUntilDate(startDate);
      if (d > 0) daysLabel = ` (งานนี้ใกล้เริ่มในอีก ${d} วัน)`;
      else if (d === 0) daysLabel = ` (งานเริ่มวันนี้!)`;
      else daysLabel = ` (งานเริ่มแล้ว)`;
    }

    // Today's new bookings — query case-insensitively by fetching all variants under this key
    let todayBookings = [];
    if (todayRange) {
      let q = supabase
        .from("line_booking_records")
        .select("booth_code, shop_name, project_name")
        .eq("booking_status", "booked")
        .ilike("project_name", projectKey.replace(/%/g, "\\%"))
        .gte("booked_at", todayRange.startIso)
        .lt("booked_at", todayRange.endIso)
        .order("booth_code", { ascending: true });
      if (groupId) q = q.or(`group_id.eq.${groupId},group_id.eq.direct`);
      const { data } = await q;
      todayBookings = (data ?? []).filter((r) => (r.project_name ?? "").toLowerCase().trim() === projectKey);
    }

    // All active bookings — case-insensitive group
    let allBookings = [];
    {
      let q = supabase
        .from("line_booking_records")
        .select("booth_code, shop_name, table_free_qty, table_extra_qty, chair_free_qty, chair_extra_qty, power_amp, power_label, project_name")
        .eq("booking_status", "booked")
        .ilike("project_name", projectKey.replace(/%/g, "\\%"))
        .order("booth_code", { ascending: true });
      if (groupId) q = q.or(`group_id.eq.${groupId},group_id.eq.direct`);
      const { data } = await q;
      allBookings = (data ?? []).filter((r) => (r.project_name ?? "").toLowerCase().trim() === projectKey);
    }

    const bookedCount = allBookings.length;
    const totalLabel = totalBooths ? `${bookedCount}/${totalBooths}` : String(bookedCount);

    // Collect structured data for Nova summary
    projectsData.push({
      name: projectName,
      startDate: startDate ?? null,
      endDate: endDate ?? null,
      totalBooths: totalBooths ?? null,
      todayBookings: todayBookings.map((b) => ({
        boothCode: normalizeBoothCode(b.booth_code) || "-",
        shopName: b.shop_name || "-",
      })),
      allBookings: allBookings.map((b) => ({
        boothCode: normalizeBoothCode(b.booth_code) || "-",
        shopName: b.shop_name || "-",
        tableQty: Number(b.table_free_qty ?? 0) + Number(b.table_extra_qty ?? 0),
        chairQty: Number(b.chair_free_qty ?? 0) + Number(b.chair_extra_qty ?? 0),
        power: b.power_label || (b.power_amp ? `${b.power_amp}A` : null) || "-",
      })),
    });

    lines.push("", `${projectName}${dateRange}${daysLabel}`);

    // Today section
    lines.push(`[อัปเดตจองใหม่วันนี้]`);
    if (todayBookings.length) {
      for (const b of todayBookings.slice(0, 20))
        lines.push(`• บูธ ${normalizeBoothCode(b.booth_code) || "-"} | ร้าน ${b.shop_name || "-"}`);
      if (todayBookings.length > 20) lines.push(`  ... และอีก ${todayBookings.length - 20} รายการ`);
    } else {
      lines.push("ไม่มีการจองใหม่วันนี้");
    }

    // All booths section
    lines.push(`[สรุปพื้นที่ทั้งหมด (${totalLabel} บูธ)] (✅ = จองแล้ว, ⬜ = ว่าง)`);
    if (allBookings.length) {
      const boothMap = new Map();
      for (const b of allBookings) boothMap.set(normalizeBoothCode(b.booth_code) ?? "", b);

      const numTotal = totalBooths ? Number(totalBooths) : null;
      if (numTotal && !isNaN(numTotal)) {
        // Show full range 1..totalBooths
        for (let i = 1; i <= numTotal; i++) {
          const bc = String(i);
          const b = boothMap.get(bc);
          if (b) {
            const t = Number(b.table_free_qty ?? 0) + Number(b.table_extra_qty ?? 0);
            const c = Number(b.chair_free_qty ?? 0) + Number(b.chair_extra_qty ?? 0);
            const pwr = b.power_label || (b.power_amp ? `${b.power_amp}A` : "-");
            lines.push(`✅ บูธ ${i} | ${b.shop_name || "-"} | โต๊ะ ${t} | เก้าอี้ ${c} | ไฟ ${pwr}`);
          } else {
            lines.push(`⬜ บูธ ${i} | - ว่าง -`);
          }
        }
      } else {
        // No totalBooths — show booked only
        for (const b of allBookings.slice(0, 50)) {
          const t = Number(b.table_free_qty ?? 0) + Number(b.table_extra_qty ?? 0);
          const c = Number(b.chair_free_qty ?? 0) + Number(b.chair_extra_qty ?? 0);
          const pwr = b.power_label || (b.power_amp ? `${b.power_amp}A` : "-");
          lines.push(`✅ บูธ ${normalizeBoothCode(b.booth_code) || "-"} | ${b.shop_name || "-"} | โต๊ะ ${t} | เก้าอี้ ${c} | ไฟ ${pwr}`);
        }
        if (allBookings.length > 50) lines.push(`... และอีก ${allBookings.length - 50} บูธ`);
      }

      lines.push("─────────────────────");
      const totalT = allBookings.reduce((s, b) => s + Number(b.table_free_qty ?? 0) + Number(b.table_extra_qty ?? 0), 0);
      const totalC = allBookings.reduce((s, b) => s + Number(b.chair_free_qty ?? 0) + Number(b.chair_extra_qty ?? 0), 0);
      const hasPower = allBookings.some((b) => b.power_amp || b.power_label);
      lines.push(`Inventory รวมทั้งงาน: โต๊ะทั้งหมด: ${totalT} ตัว | เก้าอี้ทั้งหมด: ${totalC} ตัว | กำลังไฟ(Amp): ${hasPower ? "ตรวจสอบแล้ว" : "-"}`);
    } else {
      lines.push("ยังไม่มีการจอง");
    }
  }

  if (!activeProjects.length) {
    lines.push("", "ไม่มีโปรเจกต์ที่กำลังจะจัดงาน");
  }

  // AI-parsed review items (bookings only)
  const bookingReview = (reviewItems ?? []).filter((i) => i?.category === "booking");
  // Split into per-project messages (avoids LINE 5000-char limit)
  const headerLine = lines[0];
  const messages = [];
  if (activeProjects.length === 0) {
    messages.push(lines.join("\n").slice(0, 4500));
  } else {
    const projectSections = [];
    let current = [];
    for (let i = 1; i < lines.length; i++) {
      const line = lines[i];
      if (line === "" && i + 1 < lines.length) {
        if (current.length) projectSections.push(current);
        current = [];
      } else {
        current.push(line);
      }
    }
    if (current.length) projectSections.push(current);
    messages.push(headerLine);
    for (const section of projectSections) {
      messages.push(section.join("\n").slice(0, 4990));
    }
  }
  const bookingReview2 = (reviewItems ?? []).filter((i) => i?.category === "booking");
  if (bookingReview2.length) {
    const reviewLines = ["⚠️ รอตรวจสอบ (" + bookingReview2.length + " รายการ)"];
    for (const item of bookingReview2.slice(0, 10)) reviewLines.push(formatImageDigestEventLine(item));
    messages.push(reviewLines.join("\n").slice(0, 4500));
  }
  console.log("[digest] built msgs=" + messages.length);
  return messages.filter(Boolean);
}

function listDueImageDigestSlots(now = new Date()) {
  if (!LINE_IMAGE_SUMMARY_ENABLED) return [];
  const current = getTimePartsInTz(now);
  return LINE_IMAGE_SUMMARY_HOURS
    .filter((hour) => current.hour >= hour)
    .map((hour) => ({ dateStr: current.dateStr, hour, slotKey: `${current.dateStr}@${String(hour).padStart(2, "0")}` }));
}

function maskLineTarget(target) {
  const raw = String(target ?? "");
  if (raw.length <= 10) return raw || "-";
  return `${raw.slice(0, 6)}...${raw.slice(-4)}`;
}

async function discoverBookingDigestTargets(slot, todayRange) {
  const targets = new Set();
  const addTarget = (value) => {
    const target = normalizeSpaces(value);
    if (target && target !== "direct") targets.add(target);
  };

  if (todayRange) {
    const { data: todayBookings, error: todayError } = await supabase
      .from("line_booking_records")
      .select("group_id")
      .eq("booking_status", "booked")
      .not("group_id", "is", null)
      .gte("booked_at", todayRange.startIso)
      .lt("booked_at", todayRange.endIso);
    if (todayError) console.error("[digest] discover today bookings failed:", todayError);
    for (const row of todayBookings ?? []) addTarget(row.group_id);
    console.log(
      `[digest] discover slot=${slot.slotKey} source=today_bookings rows=${todayBookings?.length ?? 0} targets=${targets.size}`,
    );
  }

  const { data: activeProjects, error: activeError } = await supabase
    .from("line_project_pricing")
    .select("group_id")
    .not("group_id", "is", null)
    .gte("event_end_date", slot.dateStr);
  if (activeError) console.error("[digest] discover active projects failed:", activeError);
  for (const row of activeProjects ?? []) addTarget(row.group_id);
  console.log(
    `[digest] discover slot=${slot.slotKey} source=active_projects rows=${activeProjects?.length ?? 0} targets=${targets.size}`,
  );

  return targets;
}

async function flushImageDigestSlot(slot) {
  const state = readImageDigestState();
  if (state.sentSlots?.[slot.slotKey]) {
    console.log(`[digest] slot=${slot.slotKey} skipped=already_sent sentAt=${state.sentSlots[slot.slotKey]}`);
    return;
  }
  console.log(
    `[digest] slot=${slot.slotKey} start enabled=${LINE_IMAGE_SUMMARY_ENABLED} hours=${LINE_IMAGE_SUMMARY_HOURS.join(",")} stateEvents=${state.events?.length ?? 0}`,
  );

  // Collect events from today (Bangkok date) — both pending and already sent in earlier slots today
  const todayRange = getDailyRangeUtc(slot.dateStr);
  const todayStartMs = todayRange ? new Date(todayRange.startIso).getTime() : 0;
  const todayEndMs = todayRange ? new Date(todayRange.endIso).getTime() : Infinity;

  const todayEvents = (state.events ?? []).filter((item) => {
    if (!item?.pushTarget) return false;
    const ts = item.occurredAt ? new Date(item.occurredAt).getTime() : 0;
    return ts >= todayStartMs && ts < todayEndMs;
  });

  // Group by pushTarget (use all today's events to know which groups to report to)
  const grouped = new Map();
  for (const item of todayEvents) {
    const list = grouped.get(item.pushTarget) ?? [];
    list.push(item);
    grouped.set(item.pushTarget, list);
  }
  const queuedTargetCount = grouped.size;

  for (const target of await discoverBookingDigestTargets(slot, todayRange)) {
    if (!grouped.has(target)) grouped.set(target, []);
  }
  console.log(
    `[digest] slot=${slot.slotKey} targets queued=${queuedTargetCount} total=${grouped.size} todayEvents=${todayEvents.length}`,
  );

  if (!grouped.size) {
    state.sentSlots[slot.slotKey] = new Date().toISOString();
    writeImageDigestState(state);
    console.log(`[digest] slot=${slot.slotKey} finished=no_targets mark_sent=true`);
    return;
  }

  const pendingIds = new Set(
    (state.events ?? []).filter((i) => !i?.sentAt && i?.pushTarget).map((i) => i.id),
  );

  const sentIds = new Set();
  let anyPushed = false;
  for (const [target, items] of grouped.entries()) {
    items.sort((a, b) => String(a.occurredAt).localeCompare(String(b.occurredAt)));
    const groupId = items.find((i) => i?.groupId)?.groupId ?? target;
    const reviewItems = items.filter((i) => i?.status !== "saved");
    const msgs = await buildBookingDigestMessage(slot, groupId, reviewItems);
    console.log(
      `[digest] slot=${slot.slotKey} target=${maskLineTarget(target)} group=${maskLineTarget(groupId)} events=${items.length} review=${reviewItems.length} msgs=${msgs?.length ?? 0}`,
    );
    if (!msgs?.length) continue;
    let ok = true;
    for (let i = 0; i < msgs.length; i += 5) {
      const chunk = msgs.slice(i, i + 5);
      const pushed = await pushMessage(target, chunk);
      console.log(
        `[digest] slot=${slot.slotKey} target=${maskLineTarget(target)} push chunk=${Math.floor(i / 5) + 1}/${Math.ceil(msgs.length / 5)} messages=${chunk.length} ok=${pushed}`,
      );
      if (!pushed) { ok = false; break; }
    }
    if (!ok) continue;
    anyPushed = true;
    for (const item of items) {
      if (pendingIds.has(item.id)) sentIds.add(item.id);
    }
  }

  if (!anyPushed) {
    console.log(`[digest] slot=${slot.slotKey} finished=no_successful_push mark_sent=false`);
    return;
  }
  const sentAt = new Date().toISOString();
  if (sentIds.size) {
    state.events = (state.events ?? []).map((item) =>
      sentIds.has(item.id) ? { ...item, sentAt, sentSlotKey: slot.slotKey } : item,
    );
  }
  state.sentSlots[slot.slotKey] = sentAt;
  writeImageDigestState(state);
  console.log(`[digest] slot=${slot.slotKey} finished=sent targets=${grouped.size} sentEvents=${sentIds.size} sentAt=${sentAt}`);
}

let imageDigestSchedulerStarted = false;
function startImageDigestScheduler() {
  if (imageDigestSchedulerStarted || !LINE_IMAGE_SUMMARY_ENABLED) return;
  imageDigestSchedulerStarted = true;
  const run = async () => {
    try {
      for (const slot of listDueImageDigestSlots(new Date())) await flushImageDigestSlot(slot);
    } catch (error) {
      console.error("Image digest scheduler failed:", error);
    }
  };
  setInterval(run, 60 * 1000);
  setTimeout(run, 5000);
  console.log("Image digest scheduler enabled at hours: " + LINE_IMAGE_SUMMARY_HOURS.join(", "));
}

function startEventReminderScheduler() {
  let lastRunDate = "";
  const check = async () => {
    const todayStr = new Date().toISOString().slice(0, 10);
    if (todayStr === lastRunDate) return;
    const hour = new Date().getHours();
    if (hour < 6) return; // Run after 06:00 server time
    lastRunDate = todayStr;
    try { await runEventReminderScheduler(); }
    catch (err) { console.error("[event-reminder] scheduler error:", err?.message ?? err); }
  };
  setInterval(check, 5 * 60 * 1000); // check every 5 min
  setTimeout(check, 10000);
  console.log("[event-reminder] scheduler started");
}
async function upsertGroupDefaultProject(groupId, projectName) {
  return supabase.from("line_group_settings").upsert(
    {
      group_id: groupId,
      default_project_name: projectName,
      updated_at: new Date().toISOString(),
    },
    { onConflict: "group_id" },
  );
}

const BOOKING_EVENT_DATE_LABELS = [
  "\u0e27\u0e31\u0e19\u0e17\u0e35\u0e48\u0e08\u0e31\u0e14\u0e07\u0e32\u0e19",
  "\u0e27\u0e31\u0e19\u0e08\u0e31\u0e14\u0e07\u0e32\u0e19",
  "\u0e27\u0e31\u0e19\u0e17\u0e35\u0e48\u0e07\u0e32\u0e19",
  "event date",
  "date",
];

// Extract date ranges like 29/04, 8/05, 29/04-8/05 from a string
function extractDatesFromText(text) {
  const dates = [];
  const raw = String(text ?? "");
  // Thai month abbreviations → month number
  const THAI_MONTHS = {
    "ม.ค": 1, "มค": 1, "ก.พ": 2, "กพ": 2,
    "มี.ค": 3, "มีค": 3, "เม.ย": 4, "เมย": 4,
    "พ.ค": 5, "พค": 5, "มิ.ย": 6, "มิย": 6,
    "ก.ค": 7, "กค": 7, "ส.ค": 8, "สค": 8,
    "ก.ย": 9, "กย": 9, "ต.ค": 10, "ตค": 10,
    "พ.ย": 11, "พย": 11, "ธ.ค": 12, "ธค": 12,
  };
  // Try "N-M thaimonth" or "N thaimonth" patterns first (e.g. "1-5 เม.ย", "8 ก.ย")
  let foundThaiDates = false;
  for (const [abbr, month] of Object.entries(THAI_MONTHS)) {
    const esc = abbr.replace(/\./g, "\\.");
    const reRange = new RegExp(`(\\d{1,2})\\s*[-–]\\s*(\\d{1,2})\\s*${esc}`, "g");
    let m;
    while ((m = reRange.exec(raw)) !== null) {
      const d1 = parseInt(m[1], 10), d2 = parseInt(m[2], 10);
      if (d1 >= 1 && d1 <= 31) { dates.push({ day: d1, month }); foundThaiDates = true; }
      if (d2 >= 1 && d2 <= 31) { dates.push({ day: d2, month }); foundThaiDates = true; }
    }
    const reSingle = new RegExp(`(\\d{1,2})[\\s.]*${esc}`, "g");
    while ((m = reSingle.exec(raw)) !== null) {
      const d = parseInt(m[1], 10);
      if (d >= 1 && d <= 31) { dates.push({ day: d, month }); foundThaiDates = true; }
    }
  }
  // Fallback: numeric patterns like 29/04, 8/05 (only when no Thai months found)
  if (!foundThaiDates) {
    const re = /(\d{1,2})[\/\-](\d{1,2})(?:[\/\-](\d{2,4}))?/g;
    let m;
    while ((m = re.exec(raw)) !== null) {
      const day = parseInt(m[1], 10);
      const month = parseInt(m[2], 10);
      if (day >= 1 && day <= 31 && month >= 1 && month <= 12) {
        dates.push({ day, month });
      }
    }
  }
  return dates;
}

function toGregorianYear(value) {
  const year = Number(value);
  if (!Number.isInteger(year)) return null;
  if (year >= 2400 && year <= 2700) return year - 543;
  if (year >= 2000 && year <= 2100) return year;
  return null;
}

function formatIsoDate(year, month, day) {
  if (!Number.isInteger(year) || !Number.isInteger(month) || !Number.isInteger(day)) return "";
  const date = new Date(Date.UTC(year, month - 1, day));
  if (
    date.getUTCFullYear() !== year ||
    date.getUTCMonth() !== month - 1 ||
    date.getUTCDate() !== day
  ) {
    return "";
  }
  return `${year}-${String(month).padStart(2, "0")}-${String(day).padStart(2, "0")}`;
}

function extractEventYearFromText(text) {
  const raw = normalizeSpaces(toAsciiDigits(String(text ?? "")));
  if (!raw) return null;
  const matched = raw.match(/\b(25\d{2}|20\d{2})\b/);
  return matched ? toGregorianYear(matched[1]) : null;
}

function extractEventDateRange(text) {
  const raw = normalizeSpaces(toAsciiDigits(String(text ?? "")));
  if (!raw) return { eventStartDate: "", eventEndDate: "" };

  const explicitIsoDates = [];
  for (const match of raw.matchAll(/\b(20\d{2}|25\d{2})-(\d{1,2})-(\d{1,2})\b/g)) {
    const year = toGregorianYear(match[1]);
    const month = Number(match[2]);
    const day = Number(match[3]);
    const iso = formatIsoDate(year, month, day);
    if (iso) explicitIsoDates.push(iso);
  }
  if (explicitIsoDates.length) {
    return {
      eventStartDate: explicitIsoDates[0],
      eventEndDate: explicitIsoDates[explicitIsoDates.length - 1],
    };
  }

  const explicitSlashDates = [];
  for (const match of raw.matchAll(/\b(\d{1,2})[\/-](\d{1,2})[\/-](20\d{2}|25\d{2})\b/g)) {
    const day = Number(match[1]);
    const month = Number(match[2]);
    const year = toGregorianYear(match[3]);
    const iso = formatIsoDate(year, month, day);
    if (iso) explicitSlashDates.push(iso);
  }
  if (explicitSlashDates.length) {
    return {
      eventStartDate: explicitSlashDates[0],
      eventEndDate: explicitSlashDates[explicitSlashDates.length - 1],
    };
  }

  const dates = extractDatesFromText(raw);
  const year = extractEventYearFromText(raw);
  if (!dates.length || !year) return { eventStartDate: "", eventEndDate: "" };

  const start = dates[0];
  const end = dates[dates.length - 1];
  let endYear = year;
  if (end.month < start.month || (end.month === start.month && end.day < start.day)) {
    endYear += 1;
  }

  return {
    eventStartDate: formatIsoDate(year, start.month, start.day),
    eventEndDate: formatIsoDate(endYear, end.month, end.day),
  };
}

function dateOverlapsProject(sheetDates, proj) {
  if (!sheetDates.length) return false;
  const start = proj.event_start_date ? new Date(proj.event_start_date) : null;
  const end = proj.event_end_date ? new Date(proj.event_end_date) : null;
  if (!start && !end) return false;
  const year = (start ?? end).getFullYear();
  return sheetDates.some(({ day, month }) => {
    const d = new Date(year, month - 1, day);
    if (start && end) return d >= start && d <= end;
    if (start) return d >= start;
    if (end) return d <= end;
    return false;
  });
}

function tokenize(text) {
  return String(text ?? "").toLowerCase()
    .replace(/[^\u0E00-\u0E7Fa-z0-9\s]/g, " ")
    .split(/\s+/)
    .filter((t) => t.length >= 2);
}

const SHEET_ALIASES=[
  {thai:"เมกกะ",en:"mega"},
  {thai:"กาแฟ",en:"coffee"},
  {thai:"ไอคอน",en:"iconsiam"},
  {thai:"พารากอน",en:"paragon"},
  {thai:"สยาม",en:"siam"},
];
function scoreSheetVsProject(sheetName, proj) {
  let expandedSheet=sheetName.toLowerCase();
  for(const a of SHEET_ALIASES){if(expandedSheet.includes(a.thai))expandedSheet+=" "+a.en;}
  const sheetTokens = tokenize(expandedSheet);
  const projTokens = tokenize(proj.project_name ?? "");
  if (!sheetTokens.length || !projTokens.length) return 0;
  const matches = projTokens.filter((pt) =>
    sheetTokens.some((st) => st.includes(pt) || pt.includes(st))
  ).length;
  return matches / projTokens.length;
}

async function resolveProjectFromSheetName(sheetName, groupId, defaultProject) {
  if (!sheetName || !groupId) return sheetName || defaultProject || "";

  const { data: projects } = await supabase
    .from("line_project_pricing")
    .select("project_name, event_start_date, event_end_date")
    .eq("group_id", groupId);

  if (!projects?.length) return defaultProject || sheetName;

  const sheetDates = extractDatesFromText(sheetName);

  // Score each project: text similarity + date overlap bonus
  const scored = projects.map((proj) => {
    const textScore = scoreSheetVsProject(sheetName, proj);
    const dateBonus = dateOverlapsProject(sheetDates, proj) ? 0.5 : 0;
    return { proj, score: textScore + dateBonus };
  }).filter((x) => x.score > 0).sort((a, b) => b.score - a.score);

  if (scored.length === 0) return defaultProject || sheetName;
  // Use best match if score is reasonable (at least one meaningful token matches)
  if (scored[0].score >= 0.3) return scored[0].proj.project_name;
  return defaultProject || sheetName;
}

async function getGroupDefaultProject(groupId) {
  const { data, error } = await supabase
    .from("line_group_settings")
    .select("default_project_name")
    .eq("group_id", groupId)
    .maybeSingle();
  if (error) return null;
  return data?.default_project_name ?? null;
}

async function upsertProjectBoothPrice(groupId, projectName, boothPrice) {
  return supabase.from("line_project_pricing").upsert(
    {
      group_id: groupId,
      project_name: projectName,
      booth_price: boothPrice,
      updated_at: new Date().toISOString(),
    },
    { onConflict: "group_id,project_name" },
  );
}

async function resolveCanonicalProjectName(groupId, projectName) {
  if (!projectName) return projectName;
  const { data, error } = await supabase
    .from("line_project_pricing")
    .select("project_name")
    .eq("group_id", groupId);
  if (error || !data?.length) return projectName;
  const exact = data.find(r => r.project_name === projectName);
  if (exact) return exact.project_name;
  const lower = projectName.toLowerCase().trim();
  const ci = data.find(r => r.project_name.toLowerCase().trim() === lower);
  if (ci) return ci.project_name;
  return projectName;
}

async function getProjectBoothPrice(groupId, projectName) {
  const { data, error } = await supabase
    .from("line_project_pricing")
    .select("booth_price")
    .eq("group_id", groupId)
    .eq("project_name", projectName)
    .maybeSingle();

  if (error) return { price: null, error };

  const price = normalizeAmount(data?.booth_price);
  return { price, error: null };
}

async function getProjectEventDates(groupId, projectName) {
  const { data, error } = await supabase
    .from("line_project_pricing")
    .select("event_start_date, event_end_date")
    .eq("group_id", groupId)
    .eq("project_name", projectName)
    .maybeSingle();
  if (error) return { eventStartDate: null, eventEndDate: null, error };
  return {
    eventStartDate: data?.event_start_date ?? null,
    eventEndDate: data?.event_end_date ?? null,
    error: null,
  };
}

async function ensureProjectPricingFromBooking(parsed, source) {
  const groupId = source?.groupId ?? source?.roomId ?? null;
  const projectName = normalizeSpaces(parsed?.projectName);
  const eventStartDate = normalizeSpaces(parsed?.eventStartDate);
  const eventEndDate = normalizeSpaces(parsed?.eventEndDate) || eventStartDate;
  if (!groupId || !projectName || !/^\d{4}-\d{2}-\d{2}$/.test(eventStartDate)) {
    return { ok: true, skipped: true };
  }

  const todayStr = getTimePartsInTz(new Date()).dateStr;
  if (eventEndDate && eventEndDate < todayStr) return { ok: true, skipped: true };

  const normalizedBoothPrice = normalizeAmount(parsed?.boothPrice) ?? 0;
  const { data: existing, error: readError } = await supabase
    .from("line_project_pricing")
    .select("project_name, event_start_date, event_end_date, booth_price")
    .eq("group_id", groupId)
    .eq("project_name", projectName)
    .maybeSingle();

  if (readError) {
    console.error("[project-auto-create] read failed:", readError);
    return { ok: false, error: readError };
  }

  if (!existing) {
    const { error: insertError } = await supabase
      .from("line_project_pricing")
      .insert({
        group_id: groupId,
        project_name: projectName,
        booth_price: normalizedBoothPrice,
        event_start_date: eventStartDate,
        event_end_date: eventEndDate,
        updated_at: new Date().toISOString(),
      });
    if (insertError) {
      console.error("[project-auto-create] insert failed:", insertError);
      return { ok: false, error: insertError };
    }
    console.log(`[project-auto-create] created project="${projectName}" start=${eventStartDate} end=${eventEndDate}`);
    return { ok: true, created: true };
  }

  const updatePayload = {};
  if (!existing.event_start_date) updatePayload.event_start_date = eventStartDate;
  if (!existing.event_end_date) updatePayload.event_end_date = eventEndDate;
  if (existing.booth_price === null && normalizedBoothPrice > 0) {
    updatePayload.booth_price = normalizedBoothPrice;
  }
  if (!Object.keys(updatePayload).length) return { ok: true, skipped: true };

  updatePayload.updated_at = new Date().toISOString();
  const { error: updateError } = await supabase
    .from("line_project_pricing")
    .update(updatePayload)
    .eq("group_id", groupId)
    .eq("project_name", projectName);
  if (updateError) {
    console.error("[project-auto-create] update failed:", updateError);
    return { ok: false, error: updateError };
  }
  console.log(
    `[project-auto-create] updated project="${projectName}" start=${updatePayload.event_start_date ?? existing.event_start_date} end=${updatePayload.event_end_date ?? existing.event_end_date}`,
  );
  return { ok: true, updated: true };
}

const helpText = [
  "Available commands:",
  "/help",
  "/agent -> show agent capabilities",
  "/book project=<name> shop=<name> phone=<phone> booth=<code> type=<product> [price=<baht>]",
  `/expense amount=<baht> [project=<name>] [vendor=<name>] [type=<type>] [note=<text>] -> default project="${DEFAULT_EXPENSE_PROJECT_NAME}" if omitted`,
  "/list [project=<name>] -> show latest bookings",
  "/review -> รายการรอตรวจสอบ (needs_project / pending_replace)",
  "/review fix <id> project=<ชื่องาน> -> กำหนดงานให้รายการ needs_project",
  "/confirm-replace id=<id> -> ยืนยันแทนที่บูธซ้ำ (ใช้ได้จาก 1:1 chat)",
  "/ลิสงาน [ชื่องาน] -> ลิสร้านในงาน | /project-shop <name>",
  "/ลิสร้าน [ชื่อร้าน] -> ค้นหาร้านด้วยชื่อ | /shop <name>",
  "/summary [project=<name>] -> detailed summary + duplicate booth check",
  "/sales-summary [project=<name>] -> sold booths + price + total",
  "/set-price project=<name> price=<baht> -> default booth price",
  "/expense-summary [project=<name>] [YYYY-MM-DD] -> expense summary",
  "/install [project=<name>] -> installer view (tables/chairs/power)",
  "/cancel id=<short_id> or /cancel shop=<shop_name>",
  "/cancel-expense -> no longer needed; expenses save immediately",
  "/confirm-replace (or reply YES) -> replace duplicate booth booking",
  "/export <YYYY-MM-DD> -> daily CSV",
  "/export-install project=<name> -> installer CSV",
  "/export-expense [project=<name>] [YYYY-MM-DD] -> daily expense CSV",
  "Thai command aliases are also supported",
].join("\n");


function normalizeSpaces(value) {
  return (value ?? "").replace(/\s+/g, " ").trim();
}

function normalizeKey(value) {
  return String(value ?? "")
    .toLowerCase()
    .replace(/[\s\u200b]+/g, "")
    .replace(/[.*_`~\-]/g, "")
    .replace(/[()（）\[\]【】]/g, "")
    .replace(/[\\/]/g, "")
    .replace(/[.,]/g, "");
}

function normalizeBoothCode(value) {
  const raw = normalizeSpaces(toAsciiDigits(String(value ?? "")))
    .replace(/^(\d+)\.0+$/, "$1")
    .replace(/^(?:\u0e40\u0e25\u0e02)?(?:\u0e1a\u0e39\u0e18|\u0e1a\u0e39\u0e17|booth)\s*/i, "");
  if (!raw) return "";

  const matched = raw.match(/^[A-Za-z]*\d+[A-Za-z0-9-]*/);
  const code = matched?.[0] ?? raw.split(/\s+/)[0] ?? "";
  return code.replace(/\s+/g, "").toUpperCase();
}

function extractBoothPriceFromText(value) {
  const raw = normalizeSpaces(toAsciiDigits(String(value ?? "")));
  if (!raw) return null;

  const matched = raw.match(
    /(?:\u0e27\u0e31\u0e19\u0e25\u0e30|\u0e23\u0e32\u0e04\u0e32|\u0e04\u0e48\u0e32\u0e1a\u0e39\u0e18|\u0e04\u0e48\u0e32\u0e1a\u0e39\u0e17|\u0e04\u0e48\u0e32\u0e40\u0e0a\u0e48\u0e32|price)\s*[:=]?\s*([0-9][0-9,]*(?:\.\d{1,2})?)/i,
  );
  return matched ? normalizeAmount(matched[1]) : null;
}

function stripListPrefix(value) {
  return String(value ?? "")
    .replace(/^\s*[-*]\s*/, "")
    .replace(/^\s*\d+[.)]\s*/, "")
    .trim();
}

function normalizePhone(value) {
  const thaiToArabic = {
    "๐": "0",
    "๑": "1",
    "๒": "2",
    "๓": "3",
    "๔": "4",
    "๕": "5",
    "๖": "6",
    "๗": "7",
    "๘": "8",
    "๙": "9",
  };
  let s = normalizeSpaces(value)
    .split("")
    .map((ch) => thaiToArabic[ch] ?? ch)
    .join("");
  s = s.replace(/[^\d+]/g, "");
  if (s.startsWith("+66")) s = `0${s.slice(3)}`;
  return s;
}



function toAsciiDigits(value) {
  const thaiToArabic = {
    "๐": "0",
    "๑": "1",
    "๒": "2",
    "๓": "3",
    "๔": "4",
    "๕": "5",
    "๖": "6",
    "๗": "7",
    "๘": "8",
    "๙": "9",
  };

  return String(value ?? "")
    .split("")
    .map((ch) => thaiToArabic[ch] ?? ch)
    .join("");
}

function extractFirstInteger(value) {
  const raw = toAsciiDigits(value);
  if (!raw.trim()) return 0;

  const thaiNotTake = /ไม่\s*รับ/;
  const thaiTake = /รับ/;
  if (thaiNotTake.test(raw)) return 0;
  if (thaiTake.test(raw) && !/\d/.test(raw)) return 1;

  const matched = raw.match(/\d+/);
  if (!matched) return 0;

  const n = Number(matched[0]);
  if (!Number.isFinite(n) || n < 0) return 0;
  return Math.min(99, Math.floor(n));
}

function parsePowerAmp(value) {
  const label = normalizeSpaces(toAsciiDigits(value));
  if (!label) return { powerAmp: null, powerLabel: "" };

  const matched =
    label.match(/(\d{1,3})(?:\s*(?:a|amp|amps|แอมป์))/i) ??
    label.match(/(\d{1,3})/);
  const amp = matched ? Number(matched[1]) : null;

  return {
    powerAmp: Number.isFinite(amp) && amp > 0 ? Math.min(500, Math.floor(amp)) : null,
    powerLabel: label.slice(0, 120),
  };
}

function parseEquipmentFields(input = {}) {
  const tableFreeQty = extractFirstInteger(input.tableFree ?? input.tableFreeQty);
  const tableExtraQty = extractFirstInteger(input.tableExtra ?? input.tableExtraQty);
  const chairFreeQty = extractFirstInteger(input.chairFree ?? input.chairFreeQty);
  const chairExtraQty = extractFirstInteger(input.chairExtra ?? input.chairExtraQty);
  const power = parsePowerAmp(input.electricity ?? input.powerLabel ?? input.powerAmp);

  return {
    tableFreeQty,
    tableExtraQty,
    chairFreeQty,
    chairExtraQty,
    powerAmp: power.powerAmp,
    powerLabel: power.powerLabel,
  };
}

function extractPhones(value) {
  const raw = String(value ?? "");
  if (!raw.trim()) return [];

  // Strip name prefix before number: e.g. "ฝน 084-5198289" → "084-5198289"
  // Also strip trailing labels: e.g. "094-5125619 (คุณเต็ม)" → "094-5125619"
  const cleaned = raw
    .replace(/\([^)]*\)/g, " ")          // remove (คุณเต็ม) etc.
    .replace(/[\u0E00-\u0E7F]+\s*/g, " ") // remove Thai words (name prefixes)
    .replace(/[\/|;]/g, " ");

  const chunks = cleaned
    .split(/\s+/)
    .map((part) => normalizePhone(part))
    .filter(Boolean);

  const valid = [];
  for (const phone of chunks) {
    const digits = phone.replace(/\D/g, "");
    if (/^0\d{8,9}$/.test(digits)) valid.push(digits);
  }

  return Array.from(new Set(valid));
}

function findPhonesFromText(text) {
  const matches = String(text ?? "").match(/(?:\+66|0)[\d\-\s/]{8,20}/g) ?? [];
  const all = [];
  for (const part of matches) all.push(...extractPhones(part));
  return Array.from(new Set(all));
}

function pickPrimaryPhone(phones) {
  return Array.isArray(phones) && phones.length ? phones[0] : "";
}

function maskSensitiveId(value) {
  const digits = String(value ?? "").replace(/\D/g, "");
  if (!digits) return "";
  if (digits.length <= 4) return "*".repeat(digits.length);
  const head = digits.slice(0, 3);
  const tail = digits.slice(-4);
  return `${head}${"*".repeat(Math.max(0, digits.length - 7))}${tail}`;
}

function parseKvSegments(text) {
  // Strip command prefix and square brackets (e.g. [notify=3] → notify=3)
  const content = String(text ?? "").replace(/^\/\S+\s*/, "").replace(/[\[\]]/g, "");
  const map = {};
  // Lazy value capture: stops before next key= / key: pattern
  const regex = /([A-Za-zก-๙_]+)\s*[:=]\s*([^|,\n]+?)(?=\s+[A-Za-zก-๙_]+\s*[:=]|[|,\n]|$)/g;
  let match = regex.exec(content);
  while (match) {
    map[match[1].trim().toLowerCase()] = normalizeSpaces(match[2]);
    match = regex.exec(content);
  }
  return map;
}

function extractLabeledFields(text) {
  const lines = String(text ?? "").split(/\r?\n/);
  const fields = {};
  let lastLabel = "";

  for (const rawLine of lines) {
    const line = stripListPrefix(rawLine);
    if (!line) {
      lastLabel = "";
      continue;
    }

    const sepMatch = line.match(/^(.+?)\s*[:=：]\s*(.*)$/);
    if (sepMatch) {
      const label = normalizeSpaces(sepMatch[1]);
      const value = normalizeSpaces(sepMatch[2]);
      const key = normalizeKey(label);
      if (key) {
        fields[key] = value;
        lastLabel = key;
      }
      continue;
    }

    if (
      lastLabel &&
      !/^\*/.test(line) &&
      !/^(ข้อมูล|จำนวน|รายละเอียด|กำลังไฟ|วิธีตอบ|หมายเหตุ|ขอบคุณ)/.test(line)
    ) {
      fields[lastLabel] = fields[lastLabel]
        ? normalizeSpaces(`${fields[lastLabel]} ${line}`)
        : normalizeSpaces(line);
    }
  }

  return fields;
}

function pickValue(map, keys) {
  for (const key of keys) {
    if (map[key] !== undefined) return map[key];
  }
  return "";
}

function pickByLabel(fields, aliases) {
  for (const alias of aliases) {
    const exactKey = normalizeKey(alias);
    if (fields[exactKey]) return fields[exactKey];

    const fuzzy = Object.entries(fields).find(
      ([key, value]) => value && (key.startsWith(exactKey) || key.includes(exactKey)),
    );
    if (fuzzy) return fuzzy[1];
  }
  return "";
}

function findAmpLine(text) {
  const lines = String(text ?? "").split(/\r?\n/);
  for (const rawLine of lines) {
    const line = stripListPrefix(rawLine);
    if (!line) continue;
    if (/แอมป์|\bamp\b/i.test(line) && !line.includes(":")) {
      return normalizeSpaces(line);
    }
  }
  return "";
}

function findBoothFromText(text) {
  const m = String(text ?? "").match(
    /(?:เลข)?(?:บูธ|บูท|booth)\s*(?:ที่จะจอง)?\s*[:=]?\s*([A-Za-z0-9-]+)/i,
  );
  return m ? normalizeBoothCode(m[1]) : "";
}

function looksLikeBookingForm(text) {
  const raw = String(text ?? "");
  const markers = [
    /ชื่องาน\s*[:=：]/,
    /ชื่อร้าน\s*[:=：]/,
    /เบอร์โทร\s*[:=：]/,
    /เลขบูธ(?:ที่จะจอง)?\s*[:=：]/,
    /ข้อมูลผู้จอง/,
    /ข้อมูลทั่วไป/,
    /จองพื้นที่/,
    /ชื่อบริษัท\s*[:=：]/,
    /เลขทะเบียนนิติบุคคล\s*[:=：]/,
  ];
  let hit = 0;
  for (const marker of markers) {
    if (marker.test(raw)) hit += 1;
  }
  return hit >= 2;
}

function looksLikeBookingText(text) {
  const raw = String(text ?? "");
  if (!raw.trim()) return false;
  if (looksLikeBookingForm(raw)) return true;

  const kv = parseKvSegments(raw);
  const fields = extractLabeledFields(raw);
  const projectName = normalizeSpaces(
    pickByLabel(fields, ["ชื่องาน", "ชื่อโปรเจกต์", "ชื่อโปรเจค", "โปรเจกต์", "โปรเจค", "project"]) ||
      pickValue(kv, ["ชื่องาน", "โปรเจกต์", "โปรเจค", "project"]),
  );
  const companyName = normalizeSpaces(
    pickByLabel(fields, ["ชื่อบริษัท", "บริษัท", "นิติบุคคล", "company"]) ||
      pickValue(kv, ["ชื่อบริษัท", "บริษัท", "company"]),
  );
  const shopName = normalizeSpaces(
    pickByLabel(fields, ["ชื่อร้าน", "ร้าน", "shop", "shop name"]) ||
      pickValue(kv, ["ชื่อร้าน", "ร้าน", "shop", "shop_name"]),
  );
  const boothCode = normalizeBoothCode(
    pickByLabel(fields, ["เลขบูธที่จะจอง", "เลขบูธ", "บูธ", "บูท", "booth"]) ||
      pickValue(kv, ["เลขบูธ", "บูธ", "บูท", "booth", "booth_code"]) ||
      findBoothFromText(raw),
  );

  const labeledPhones = normalizeSpaces(
    pickByLabel(fields, ["เบอร์โทร", "เบอร์โทรศัพท์", "เบอร์", "โทร", "phone", "มือถือ"]) ||
      pickValue(kv, ["เบอร์", "โทร", "phone"]),
  );
  const phones = extractPhones(labeledPhones);
  if (!phones.length) phones.push(...findPhonesFromText(raw));
  const phone = pickPrimaryPhone(phones);

  // Full data: phone + identity + location
  if (phone && (projectName || companyName) && (shopName || boothCode)) return true;
  // Partial: shop/booth + booking context (project name or "จอง" keyword)
  if ((shopName || boothCode) && (projectName || companyName || /จอง/i.test(raw))) return true;

  return false;
}

function hasRequiredBookingFields(parsed) {
  return Boolean(parsed.projectName && parsed.shopName);
}

const PLACEHOLDER_SHOP_KEYS = new Set([
  "\u0e0a\u0e37\u0e48\u0e2d\u0e23\u0e49\u0e32\u0e19",
  "\u0e23\u0e49\u0e32\u0e19",
  "shop",
  "shopname",
  "shop_name",
].map(normalizeKey));

const PLACEHOLDER_BOOTH_KEYS = new Set([
  "\u0e1a\u0e39\u0e18",
  "\u0e1a\u0e39\u0e17",
  "\u0e40\u0e25\u0e02\u0e1a\u0e39\u0e18",
  "\u0e40\u0e25\u0e02\u0e1a\u0e39\u0e17",
  "booth",
  "boothcode",
  "booth_code",
].map(normalizeKey));

function isPlaceholderBookingRow(parsed) {
  const shopKey = normalizeKey(parsed?.shopName);
  const boothKey = normalizeKey(parsed?.boothCode);
  return PLACEHOLDER_SHOP_KEYS.has(shopKey) || PLACEHOLDER_BOOTH_KEYS.has(boothKey);
}

function normalizeAmount(value) {
  const raw = toAsciiDigits(String(value ?? "")).replace(/,/g, "").trim();
  if (!raw) return null;
  const matched = raw.match(/-?\d+(?:\.\d{1,2})?/);
  if (!matched) return null;

  const amount = Number(matched[0]);
  if (!Number.isFinite(amount) || amount <= 0) return null;
  return Math.round(amount * 100) / 100;
}

function extractAmountFromText(text) {
  const raw = toAsciiDigits(String(text ?? ""));
  if (!raw.trim()) return null;

  const patterns = [
    /(?:ยอด(?:เงิน)?|จำนวนเงิน|amount|paid|payment|จ่าย|โอน|ชำระ|ค่าใช้จ่าย)\s*[:=]?\s*(\d[\d,]*(?:\.\d{1,2})?)/i,
    /(?:฿|บาท|thb)\s*(\d[\d,]*(?:\.\d{1,2})?)/i,
    /(\d[\d,]*(?:\.\d{1,2})?)\s*(?:บาท|baht|thb)\b/i,
  ];

  for (const pattern of patterns) {
    const match = raw.match(pattern);
    if (!match) continue;
    const amount = normalizeAmount(match[1]);
    if (amount !== null) return amount;
  }

  const candidates = raw.match(/\d[\d,]*(?:\.\d{1,2})?/g) ?? [];
  for (const candidate of candidates) {
    const digitsOnly = candidate.replace(/\D/g, "");
    if (/^0\d{8,9}$/.test(digitsOnly)) continue;
    if (digitsOnly.length === 8 && /^\d{8}$/.test(digitsOnly)) continue;

    const amount = normalizeAmount(candidate);
    if (amount !== null && amount <= 100000000) return amount;
  }

  return null;
}

function looksLikeExpenseText(text) {
  const raw = String(text ?? "");
  if (!raw.trim()) return false;
  if (looksLikeBookingText(raw)) return false;

  const hasExpenseHint =
    /ค่าใช้จ่าย|supplier|ซัพพลายเออร์|invoice|receipt|bill|payment|paid|slip|สลิป|ค่าแรง|ค่าวัสดุ|ค่าขนส่ง|ค่าจ้าง|ค่าที่พัก|มัดจำ/i.test(raw) ||
    /(?:โอน|ชำระ|จ่าย)\s*(?:เงิน|แล้ว|ค่า|ให้|ไป)?/i.test(raw);
  const hasAmountContext = /ยอด(?:เงิน|รวม|สุทธิ)?|amount|total|บาท|฿|thb/i.test(raw);
  const amount = extractAmountFromText(raw);
  return Boolean(amount !== null && hasExpenseHint && hasAmountContext);
}

function mightBeUnrecognizedExpense(text) {
  const raw = String(text ?? "");
  if (!raw.trim() || looksLikeExpenseText(raw) || looksLikeBookingText(raw)) return false;
  const hasAmount = extractAmountFromText(raw) !== null;
  const hasMoneyHint = /บาท|฿|thb|โอน|ชำระ|จ่าย|slip|สลิป|payment|paid/i.test(raw);
  return hasAmount && hasMoneyHint;
}

function parseProjectAnswer(text) {
  const cleaned = normalizeSpaces(String(text ?? ""));
  if (!cleaned) return "";

  const matched = cleaned.match(/^(?:โปรเจกต์|โปรเจค|โปรเจคท์|project|งาน)\s*[:=]?\s*(.+)$/i);
  return normalizeSpaces(matched ? matched[1] : cleaned).slice(0, 160);
}

function parseExpenseCommand(text) {
  const kv = parseKvSegments(text);
  const fields = extractLabeledFields(text);

  const projectName = normalizeSpaces(
    pickValue(kv, ["โปรเจกต์", "โปรเจค", "project", "ชื่องาน"]) ||
      pickByLabel(fields, ["ชื่องาน", "ชื่อโปรเจกต์", "ชื่อโปรเจค", "โปรเจกต์", "โปรเจค", "project"]),
  );

  const amount =
    normalizeAmount(
      pickValue(kv, ["amount", "ยอด", "จำนวนเงิน", "เงิน", "จ่าย", "paid"]) ||
        pickByLabel(fields, ["ยอด", "ยอดเงิน", "จำนวนเงิน", "ชำระ", "total", "amount"]),
    ) ?? extractAmountFromText(text);

  const vendorName = normalizeSpaces(
    pickValue(kv, ["vendor", "supplier", "ร้าน", "shop", "ผู้ขาย", "บริษัท"]) ||
      pickByLabel(fields, ["ชื่อร้าน", "ร้าน", "ผู้ขาย", "supplier", "vendor", "บริษัท"]),
  ).slice(0, 160);

  const expenseType = normalizeSpaces(
    pickValue(kv, ["type", "category", "ประเภท", "รายการ", "หัวข้อ"]) ||
      pickByLabel(fields, ["ประเภท", "หมวดหมู่", "รายการ", "หัวข้อ"]),
  ).slice(0, 120);

  let note = normalizeSpaces(
    pickValue(kv, ["note", "หมายเหตุ", "รายละเอียด", "memo", "remark"]) ||
      pickByLabel(fields, ["หมายเหตุ", "รายละเอียด", "memo", "remark"]),
  );

  if (!note) {
    note = normalizeSpaces(String(text ?? "").replace(/^\/\S+\s*/, ""));
  }

  return {
    projectName,
    amount,
    currency: "THB",
    vendorName,
    expenseType,
    note: note.slice(0, 1800),
  };
}

function hasRequiredExpenseFields(parsed) {
  return Boolean(parsed && Number.isFinite(parsed.amount) && parsed.amount > 0);
}


async function parseBookingCommand(text, groupId) {
  const kv = parseKvSegments(text);
  const projectName =
    pickValue(kv, ["โปรเจกต์", "โปรเจค", "project"]) ||
    (await getGroupDefaultProject(groupId));
  const shopName = pickValue(kv, ["ร้าน", "shop", "shop_name"]);
  const phones = extractPhones(pickValue(kv, ["เบอร์", "โทร", "phone"]));
  const phone = pickPrimaryPhone(phones);
  const eventDateText = normalizeSpaces(
    pickValue(kv, ["วันที่จัดงาน", "วันจัดงาน", "วันที่งาน", "event_date", "event date", "date"]),
  );
  const boothCode = pickValue(kv, ["บูธ", "บูท", "booth", "booth_code"]);
  const productType = pickValue(kv, ["ประเภท", "สินค้า", "category"]);
  const note = pickValue(kv, ["หมายเหตุ", "note"]);
  const boothPrice = normalizeAmount(
    pickValue(kv, ["ราคา", "ค่าบูธ", "ค่าเช่า", "price", "booth_price"]),
  ) ?? extractBoothPriceFromText(boothCode);
  const { eventStartDate, eventEndDate } = extractEventDateRange(eventDateText);

  const equipment = parseEquipmentFields({
    tableFree: pickValue(kv, ["โต๊ะฟรี", "table_free", "table_free_qty"]),
    tableExtra: pickValue(kv, ["โต๊ะเพิ่ม", "table_extra", "table_extra_qty"]),
    chairFree: pickValue(kv, ["เก้าอี้ฟรี", "chair_free", "chair_free_qty"]),
    chairExtra: pickValue(kv, ["เก้าอี้เพิ่ม", "chair_extra", "chair_extra_qty"]),
    electricity: pickValue(kv, ["ไฟ", "แอมป์", "power", "power_amp"]),
  });

  return {
    projectName: normalizeSpaces(projectName),
    shopName: normalizeSpaces(shopName),
    phone,
    boothCode: normalizeBoothCode(boothCode),
    productType: normalizeSpaces(productType),
    note: normalizeSpaces(note),
    eventStartDate,
    eventEndDate,
    boothPrice,
    tableFreeQty: equipment.tableFreeQty,
    tableExtraQty: equipment.tableExtraQty,
    chairFreeQty: equipment.chairFreeQty,
    chairExtraQty: equipment.chairExtraQty,
    powerAmp: equipment.powerAmp,
    powerLabel: equipment.powerLabel,
  };
}



async function parseBookingFormText(text, groupId) {
  const fields = extractLabeledFields(text);

  const projectName =
    pickByLabel(fields, [
      "ชื่องาน",
      "ชื่อโปรเจกต์",
      "ชื่อโปรเจค",
      "โปรเจกต์",
      "โปรเจค",
      "project",
    ]) || (await getGroupDefaultProject(groupId));

  const shopName = pickByLabel(fields, ["ชื่อร้าน", "ร้าน", "shop", "shop name"]);

  const labeledPhones = pickByLabel(fields, [
    "เบอร์โทร",
    "เบอร์โทรศัพท์",
    "เบอร์",
    "โทร",
    "phone",
    "มือถือ",
  ]);
  const phones = extractPhones(labeledPhones);
  if (!phones.length) phones.push(...findPhonesFromText(text));
  const phone = pickPrimaryPhone(phones);

  let boothCode = normalizeSpaces(
    pickByLabel(fields, ["เลขบูธที่จะจอง", "เลขบูธ", "บูธ", "บูท", "booth"]),
  );
  if (!boothCode) boothCode = findBoothFromText(text);

  const productType = normalizeSpaces(
    pickByLabel(fields, ["ประเภทสินค้า", "ประเภท", "สินค้า", "category"]),
  );

  const contactName = normalizeSpaces(
    pickByLabel(fields, ["ชื่อผู้ติดต่อ", "ชื่อผู้จอง", "ผู้ติดต่อ"]),
  );
  const citizenId = maskSensitiveId(
    pickByLabel(fields, ["เลขสำเนาบัตรประชาชน", "เลขบัตรประชาชน", "บัตรประชาชน"]),
  );
  const address = normalizeSpaces(
    pickByLabel(fields, ["ที่อยู่ติดต่อ", "ที่อยู่", "ที่อยู่ปัจจุบัน"]),
  );

  const companyName = normalizeSpaces(
    pickByLabel(fields, ["ชื่อบริษัท", "บริษัท", "นิติบุคคล", "company"]),
  );
  const companyRegNo = normalizeSpaces(
    pickByLabel(fields, ["เลขทะเบียนนิติบุคคล", "ทะเบียนนิติบุคคล", "เลขทะเบียนบริษัท"]),
  ).replace(/[^\dA-Za-z]/g, "");
  const companyAddress = normalizeSpaces(
    pickByLabel(fields, ["ที่อยู่จดทะเบียน", "ที่อยู่บริษัท", "ที่ตั้งบริษัท"]),
  );

  const tableFree = normalizeSpaces(
    pickByLabel(fields, ["จำนวนโต๊ะฟรี", "จำนวนโต๊ะฟรี 1 ตัว รับหรือไม่"]),
  );
  const tableExtra = normalizeSpaces(
    pickByLabel(fields, ["จำนวนโต๊ะเพิ่ม", "จำนวนโต๊ะเพิ่ม 350 บาทตัว"]),
  );
  const chairFree = normalizeSpaces(
    pickByLabel(fields, ["จำนวนเก้าอี้ฟรี", "จำนวนเก้าอี้ฟรี 1 ตัว รับหรือไม่"]),
  );
  const chairExtra = normalizeSpaces(
    pickByLabel(fields, ["จำนวนเก้าอี้เพิ่ม", "จำนวนเก้าอี้เพิ่ม 80 บาทตัว"]),
  );
  const electricity = normalizeSpaces(
    pickByLabel(fields, ["กำลังไฟที่ใช้", "กำลังไฟ", "ไฟที่ใช้", "แอมป์"]) || findAmpLine(text),
  );
  const saleChannel = normalizeSpaces(
    pickByLabel(fields, ["ช่องทางการจำหน่าย", "ig", "instagram", "facebook", "line"]),
  );
  const additionalRemark = normalizeSpaces(pickByLabel(fields, ["หมายเหตุ", "note"]));
  const eventDateText = normalizeSpaces(pickByLabel(fields, BOOKING_EVENT_DATE_LABELS));
  const boothPriceRaw = normalizeSpaces(
    pickByLabel(fields, ["ราคาบูธ", "ราคา", "ค่าบูธ", "ค่าเช่า", "ค่าพื้นที่", "price", "booth price"]),
  );
  const boothPrice = normalizeAmount(boothPriceRaw) ?? extractBoothPriceFromText(boothCode);
  const { eventStartDate, eventEndDate } = extractEventDateRange(eventDateText);

  const equipment = parseEquipmentFields({
    tableFree,
    tableExtra,
    chairFree,
    chairExtra,
    electricity,
  });

  const secondaryPhones = phones.filter((p) => p !== phone);

  const noteParts = [];
  if (companyName) noteParts.push(`company=${companyName}`);
  if (companyRegNo) noteParts.push(`company_reg_no=${companyRegNo}`);
  if (companyAddress) noteParts.push(`company_address=${companyAddress}`);
  if (contactName) noteParts.push(`contact=${contactName}`);
  if (citizenId) noteParts.push(`citizen_id_masked=${citizenId}`);
  if (address) noteParts.push(`address=${address}`);
  if (secondaryPhones.length) noteParts.push(`secondary_phones=${secondaryPhones.join(",")}`);
  if (tableFree) noteParts.push(`table_free_raw=${tableFree}`);
  if (tableExtra) noteParts.push(`table_extra_raw=${tableExtra}`);
  if (chairFree) noteParts.push(`chair_free_raw=${chairFree}`);
  if (chairExtra) noteParts.push(`chair_extra_raw=${chairExtra}`);
  if (electricity) noteParts.push(`power_raw=${electricity}`);
  if (saleChannel) noteParts.push(`sales_channel=${saleChannel}`);
  if (boothPriceRaw) noteParts.push(`booth_price_raw=${boothPriceRaw}`);
  if (additionalRemark) noteParts.push(`remark=${additionalRemark}`);

  return {
    projectName: normalizeSpaces(projectName),
    shopName: normalizeSpaces(shopName),
    phone,
    boothCode: normalizeBoothCode(boothCode),
    productType,
    note: normalizeSpaces(noteParts.join(" | ")).slice(0, 1800),
    eventStartDate,
    eventEndDate,
    boothPrice,
    tableFreeQty: equipment.tableFreeQty,
    tableExtraQty: equipment.tableExtraQty,
    chairFreeQty: equipment.chairFreeQty,
    chairExtraQty: equipment.chairExtraQty,
    powerAmp: equipment.powerAmp,
    powerLabel: equipment.powerLabel,
  };
}


function parseProjectFilter(text) {
  const kv = parseKvSegments(text);
  return normalizeSpaces(
    pickValue(kv, ["โปรเจกต์", "โปรเจค", "project"]) ??
      text.replace(/^\/\S+/, ""),
  );
}

function shortId(id) {
  return id.slice(0, 8);
}

function getGroupIdFromSource(source) {
  return source?.groupId ?? source?.roomId ?? "direct";
}

function getActorKeyFromSource(source) {
  const groupId = getGroupIdFromSource(source);
  const actor = source?.userId ?? "unknown";
  return `${groupId}:${actor}`;
}

function setPendingReplacement(source, payload) {
  pendingReplacementByActor.set(getActorKeyFromSource(source), {
    ...payload,
    createdAt: Date.now(),
  });
}

function getPendingReplacement(source) {
  const key = getActorKeyFromSource(source);
  const pending = pendingReplacementByActor.get(key);
  if (!pending) return null;

  if (Date.now() - pending.createdAt > PENDING_REPLACE_TTL_MS) {
    pendingReplacementByActor.delete(key);
    return null;
  }

  return pending;
}

function clearPendingReplacement(source) {
  pendingReplacementByActor.delete(getActorKeyFromSource(source));
}

function setPendingExpense(source, payload) {
  pendingExpenseByActor.set(getActorKeyFromSource(source), {
    ...payload,
    createdAt: Date.now(),
  });
}

function getPendingExpense(source) {
  const key = getActorKeyFromSource(source);
  return pendingExpenseByActor.get(key) ?? null;
}

function clearPendingExpense(source) {
  pendingExpenseByActor.delete(getActorKeyFromSource(source));
}

function setPendingProjectSelection(source, payload) {
  pendingProjectSelectionByActor.set(getActorKeyFromSource(source), {
    ...payload,
    createdAt: Date.now(),
  });
}

function getPendingProjectSelection(source) {
  const key = getActorKeyFromSource(source);
  const pending = pendingProjectSelectionByActor.get(key);
  if (!pending) return null;
  if (Date.now() - pending.createdAt > PENDING_REPLACE_TTL_MS) {
    pendingProjectSelectionByActor.delete(key);
    return null;
  }
  return pending;
}

function clearPendingProjectSelection(source) {
  pendingProjectSelectionByActor.delete(getActorKeyFromSource(source));
}

async function findActiveBoothConflict(source, projectName, boothCode, options = {}) {
  const groupId = getGroupIdFromSource(source);
  const normalizedBooth = normalizeBoothCode(boothCode);
  if (!projectName || !normalizedBooth) return { data: null, error: null };

  let eventStartDate = normalizeSpaces(options?.eventStartDate);
  let eventEndDate = normalizeSpaces(options?.eventEndDate);
  if (!eventStartDate || !eventEndDate) {
    const { eventStartDate: dbStartDate, eventEndDate: dbEndDate, error: dateError } = await getProjectEventDates(
      groupId,
      projectName,
    );
    if (dateError) return { data: null, error: dateError };
    if (!eventStartDate) eventStartDate = dbStartDate ?? "";
    if (!eventEndDate) eventEndDate = dbEndDate ?? "";
  }

  const todayStr = getTimePartsInTz(new Date()).dateStr;
  if (eventEndDate && todayStr > eventEndDate) return { data: null, error: null };

  // Search both the actual group_id and "direct" to catch records saved from different contexts
  const groupIds = groupId === "direct" ? ["direct"] : [groupId, "direct"];

  let conflictQuery = supabase
    .from("line_booking_records")
    .select("id, project_name, shop_name, phone, booth_code, booked_at")
    .in("group_id", groupIds)
    .eq("project_name", projectName)
    .eq("booking_status", "booked")
    .order("booked_at", { ascending: false })
    .limit(500);

  if (/^\d{4}-\d{2}-\d{2}$/.test(eventStartDate)) {
    const windowStart = new Date(`${eventStartDate}T00:00:00.000Z`);
    windowStart.setUTCDate(windowStart.getUTCDate() - BOOKING_CONFLICT_LOOKBACK_DAYS);
    conflictQuery = conflictQuery.gte("booked_at", windowStart.toISOString());
  }

  const { data, error } = await conflictQuery;

  if (error) return { data: null, error };

  const conflict = (data ?? []).find(
    (row) => normalizeBoothCode(row.booth_code) === normalizedBooth,
  );

  return { data: conflict ?? null, error: null };
}

async function cancelRecordById(recordId, reason) {
  const { data: current, error: readError } = await supabase
    .from("line_booking_records")
    .select("id, note")
    .eq("id", recordId)
    .maybeSingle();

  if (readError) return { error: readError };

  const appendedNote = normalizeSpaces(
    [current?.note ?? "", reason ?? ""].filter(Boolean).join(" | "),
  ).slice(0, 1800);

  const { error } = await supabase
    .from("line_booking_records")
    .update({
      booking_status: "cancelled",
      cancelled_at: new Date().toISOString(),
      note: appendedNote || null,
    })
    .eq("id", recordId);

  return { error };
}


function normalizeParsedBooking(parsed) {
  const normalizedPhone = pickPrimaryPhone(
    extractPhones(parsed?.phone ?? normalizePhone(parsed?.phone ?? "")),
  );

  const equipment = parseEquipmentFields({
    tableFreeQty: parsed?.tableFreeQty,
    tableExtraQty: parsed?.tableExtraQty,
    chairFreeQty: parsed?.chairFreeQty,
    chairExtraQty: parsed?.chairExtraQty,
    powerAmp: parsed?.powerAmp,
    powerLabel: parsed?.powerLabel,
    electricity: parsed?.powerLabel,
  });

  return {
    projectName: normalizeSpaces(parsed?.projectName),
    shopName: normalizeSpaces(parsed?.shopName),
    phone: normalizedPhone || normalizePhone(parsed?.phone),
    boothCode: normalizeBoothCode(parsed?.boothCode),
    productType: normalizeSpaces(parsed?.productType),
    note: normalizeSpaces(parsed?.note).slice(0, 1800),
    boothPrice: normalizeAmount(parsed?.boothPrice),
    tableFreeQty: equipment.tableFreeQty,
    tableExtraQty: equipment.tableExtraQty,
    chairFreeQty: equipment.chairFreeQty,
    chairExtraQty: equipment.chairExtraQty,
    powerAmp: equipment.powerAmp,
    powerLabel: equipment.powerLabel,
  };
}


async function saveBookingWithAgentRules(parsed, source, messageId, options = {}) {
  const normalized = normalizeParsedBooking(parsed);
  const forceReplace = options.forceReplace === true;
  let replacedConflict = null;

  if (!hasRequiredBookingFields(normalized)) {
    return {
      ok: false,
      message: [
        "❌ ข้อมูลไม่ครบ — ต้องระบุ โปรเจกต์ และ ชื่อร้าน",
        "ตัวอย่าง:",
        "/จอง โปรเจกต์=ชื่องาน ร้าน=ชื่อร้าน บูธ=A01",
      ].join("\n"),
    };
  }

  if (isPlaceholderBookingRow(normalized)) {
    console.log(
      `[booking] rejected placeholder row: shop="${normalized.shopName}" booth="${normalized.boothCode}"`,
    );
    return { ok: false, silent: true, message: "" };
  }

  // Reject numeric-only shop names (Excel import artifact)
  if (/^\d+$/.test((normalized.shopName ?? "").trim())) {
    console.log(`[booking] rejected numeric shop_name: "${normalized.shopName}"`);
    return { ok: false, silent: true, message: "" };
  }

  // Normalize project name to canonical from line_project_pricing (case-insensitive match)
  if (normalized.projectName) {
    const canonical = await resolveCanonicalProjectName(
      getGroupIdFromSource(source),
      normalized.projectName
    );
    if (canonical !== normalized.projectName) {
      console.log(`[booking] project "${normalized.projectName}" → "${canonical}"`);
      normalized.projectName = canonical;
    }
  }

  const effectiveBooking = { ...normalized };

  // Reject booking if the event has already ended (skip for Excel import)
  if (effectiveBooking.projectName && !options.allowPastEvents) {
    const todayStr = getTimePartsInTz(new Date()).dateStr;
    if (parsed?.eventEndDate && todayStr > parsed.eventEndDate) {
      console.log(
        `[booking] skipped — parsed event ended ${parsed.eventEndDate} (project: ${effectiveBooking.projectName})`,
      );
      return { ok: false, silent: true, message: "" };
    }

    const { eventEndDate, error: dateError } = await getProjectEventDates(
      getGroupIdFromSource(source),
      effectiveBooking.projectName,
    );
    if (!dateError && eventEndDate) {
      if (todayStr > eventEndDate) {
        console.log(`[booking] skipped — event ended ${eventEndDate} (project: ${effectiveBooking.projectName})`);
        return { ok: false, silent: true, message: "" };
      }
    }
  }

  if (effectiveBooking.boothPrice === null && effectiveBooking.projectName) {
    const { price, error: priceError } = await getProjectBoothPrice(
      getGroupIdFromSource(source),
      effectiveBooking.projectName,
    );

    if (priceError && !isMissingSalesSchemaError(priceError)) {
      console.error(priceError);
      return { ok: false, message: "Failed to read booth price setting. Please try again." };
    }

    effectiveBooking.boothPrice = price;
  }

  if (effectiveBooking.boothCode) {
    const { data: conflict, error: conflictError } = await findActiveBoothConflict(
      source,
      effectiveBooking.projectName,
      effectiveBooking.boothCode,
      {
        eventStartDate: parsed?.eventStartDate,
        eventEndDate: parsed?.eventEndDate,
      },
    );

    if (conflictError) {
      console.error(conflictError);
      return { ok: false, message: "Failed to check duplicate booth. Please try again." };
    }

    if (conflict) {
      const sameShop = normalizeKey(conflict.shop_name) === normalizeKey(effectiveBooking.shopName);
      const samePhone = normalizePhone(conflict.phone) === normalizePhone(effectiveBooking.phone);

      if (sameShop && samePhone) {
        clearPendingReplacement(source);
        return {
          ok: true,
          message: `This booking already exists (#${shortId(conflict.id)})`,
        };
      }

      if (!forceReplace) {
        setPendingReplacement(source, {
          parsed: effectiveBooking,
          messageId,
          conflictId: conflict.id,
          conflictSummary: {
            id: conflict.id,
            shopName: conflict.shop_name,
            phone: conflict.phone,
            boothCode: conflict.booth_code,
            projectName: conflict.project_name,
          },
        });
        insertPendingReplaceRecord(effectiveBooking, source, messageId, conflict.id).catch(console.error);

        return {
          ok: false,
          needsConfirmation: true,
          message: [
            `Duplicate booth found: ${conflict.project_name} booth ${normalizeBoothCode(conflict.booth_code)}`,
            `Existing booking: ${conflict.shop_name} (${conflict.phone}) #${shortId(conflict.id)}`,
            "If previous booking was cancelled, reply /confirm-replace (or YES)",
          ].join("\n"),
        };
      }

      const reason = `auto_cancelled_for_replace_by=${effectiveBooking.shopName}`;
      const { error: cancelError } = await cancelRecordById(conflict.id, reason);
      if (cancelError) {
        console.error(cancelError);
        return { ok: false, message: "Failed to replace booth because old booking could not be cancelled." };
      }

      replacedConflict = conflict;
    }
  }

  const { data, error } = await insertBookingRecord(effectiveBooking, source, messageId);
  if (error) {
    console.error(error);
    return { ok: false, message: "Failed to save booking. Please try again." };
  }

  clearPendingReplacement(source);
  const projectSync = await ensureProjectPricingFromBooking(
    {
      ...parsed,
      projectName: effectiveBooking.projectName,
      boothPrice: effectiveBooking.boothPrice,
    },
    source,
  );
  if (!projectSync?.ok) {
    console.error("[project-auto-create] sync failed after booking save", projectSync?.error ?? "");
  }
  let message = formatSavedBookingMessage(data, effectiveBooking.note);
  if (replacedConflict) {
    message += `\nReplaced old booking #${shortId(replacedConflict.id)} (${replacedConflict.shop_name})`;
  }

  const salesSnapshot = await buildProjectSalesSnapshot(
    getGroupIdFromSource(source),
    effectiveBooking.projectName,
  );
  if (salesSnapshot) {
    message += `\n${salesSnapshot}`;
  }

  return { ok: true, message };
}

async function commandConfirmReplace(source) {
  const pending = getPendingReplacement(source);
  if (!pending) return "ไม่พบรายการรอยืนยัน ลองใช้ /confirm-replace id=<id> แทนครับ";

  const result = await saveBookingWithAgentRules(
    pending.parsed,
    source,
    pending.messageId,
    { forceReplace: true },
  );

  return result.silent ? null : result.message;
}

async function commandConfirmReplaceById(idPrefix, source) {
  const isDirectChat = !source?.groupId && !source?.roomId;

  let query = supabase
    .from("line_booking_records")
    .select("id, group_id, project_name, shop_name, phone, booth_code, product_type, note, " +
      "table_free_qty, table_extra_qty, chair_free_qty, chair_extra_qty, " +
      "power_amp, power_label, booth_price, source_user_id, source_message_id")
    .eq("booking_status", "pending_replace")
    .order("booked_at", { ascending: false })
    .limit(200);
  if (!isDirectChat) query = query.eq("group_id", source.groupId ?? source.roomId);

  const { data, error } = await query;
  if (error) { console.error(error); return "ดึงรายการไม่สำเร็จ"; }

  const record = (data ?? []).find((r) => r.id.toLowerCase().startsWith(idPrefix.toLowerCase()));
  if (!record) return `ไม่พบรายการ pending_replace id: ${idPrefix}`;

  const conflictMatch = (record.note ?? "").match(/^pending_replace:([0-9a-fA-F-]{36})/);
  if (!conflictMatch) return `ไม่พบ conflict ID ใน record #${shortId(record.id)}`;
  const conflictId = conflictMatch[1];

  const { error: cancelError } = await cancelRecordById(conflictId,
    `replaced by ${record.shop_name} via /confirm-replace id=${shortId(record.id)}`);
  if (cancelError) { console.error(cancelError); return "ยกเลิก booking เดิมไม่สำเร็จ"; }

  await supabase.from("line_booking_records").delete().eq("id", record.id);

  const syntheticSource = isDirectChat
    ? { groupId: record.group_id !== "direct" ? record.group_id : undefined, userId: source.userId }
    : source;

  const cleanNote = (record.note ?? "").replace(/^pending_replace:[0-9a-fA-F-]+\s*\|\s*/, "") || null;
  const parsed = {
    projectName: record.project_name,
    shopName: record.shop_name,
    phone: record.phone,
    boothCode: record.booth_code,
    productType: record.product_type,
    note: cleanNote,
    boothPrice: record.booth_price,
    tableFreeQty: record.table_free_qty,
    tableExtraQty: record.table_extra_qty,
    chairFreeQty: record.chair_free_qty,
    chairExtraQty: record.chair_extra_qty,
    powerAmp: record.power_amp,
    powerLabel: record.power_label,
  };

  const { data: saved, error: insertError } = await insertBookingRecord(parsed, syntheticSource, record.source_message_id);
  if (insertError) { console.error(insertError); return "ยกเลิก booking เดิมแล้ว แต่บันทึกใหม่ไม่สำเร็จ ลอง /จอง อีกรอบ"; }

  let msg = formatSavedBookingMessage(saved, cleanNote);
  msg += `\n(แทนที่ booking เดิม #${shortId(conflictId)})`;
  const snap = await buildProjectSalesSnapshot(getGroupIdFromSource(syntheticSource), parsed.projectName);
  if (snap) msg += `\n${snap}`;
  return msg;
}


async function insertBookingRecord(parsed, source, messageId) {
  const basePayload = {
    group_id: getGroupIdFromSource(source),
    project_name: parsed.projectName,
    shop_name: parsed.shopName,
    phone: parsed.phone || "ไม่ระบุ",
    booth_code: parsed.boothCode || null,
    product_type: parsed.productType || null,
    note: parsed.note || null,
    table_free_qty: parsed.tableFreeQty ?? 0,
    table_extra_qty: parsed.tableExtraQty ?? 0,
    chair_free_qty: parsed.chairFreeQty ?? 0,
    chair_extra_qty: parsed.chairExtraQty ?? 0,
    power_amp: parsed.powerAmp ?? null,
    power_label: parsed.powerLabel || null,
    booking_status: "booked",
    source_user_id: source.userId ?? null,
    source_message_id: messageId ?? null,
  };

  const withPriceResult = await supabase
    .from("line_booking_records")
    .insert({
      ...basePayload,
      booth_price: parsed.boothPrice ?? null,
    })
    .select("id, project_name, shop_name, phone, booth_code, booth_price, table_free_qty, table_extra_qty, chair_free_qty, chair_extra_qty, power_amp, power_label")
    .single();

  if (!withPriceResult.error || !isMissingSalesSchemaError(withPriceResult.error)) {
    return withPriceResult;
  }

  return supabase
    .from("line_booking_records")
    .insert(basePayload)
    .select("id, project_name, shop_name, phone, booth_code, table_free_qty, table_extra_qty, chair_free_qty, chair_extra_qty, power_amp, power_label")
    .single();
}

// ── Pending-review DB helpers ─────────────────────────────────────────────────

async function insertPendingProjectRecord(parsed, source, messageId) {
  const groupId = getGroupIdFromSource(source);
  const { error } = await supabase.from("line_booking_records").insert({
    group_id: groupId,
    project_name: parsed.projectName || null,
    shop_name: parsed.shopName,
    phone: parsed.phone || "ไม่ระบุ",
    booth_code: parsed.boothCode || null,
    product_type: parsed.productType || null,
    note: parsed.note || null,
    table_free_qty: parsed.tableFreeQty ?? 0,
    table_extra_qty: parsed.tableExtraQty ?? 0,
    chair_free_qty: parsed.chairFreeQty ?? 0,
    chair_extra_qty: parsed.chairExtraQty ?? 0,
    power_amp: parsed.powerAmp ?? null,
    power_label: parsed.powerLabel || null,
    booth_price: parsed.boothPrice ?? null,
    booking_status: "needs_project",
    source_user_id: source.userId ?? null,
    source_message_id: messageId ?? null,
  });
  if (error) console.error("[pending] insertPendingProjectRecord:", error?.message);
}

async function insertPendingReplaceRecord(parsed, source, messageId, conflictId) {
  const groupId = getGroupIdFromSource(source);
  const notePrefix = `pending_replace:${conflictId}`;
  const combinedNote = parsed.note
    ? `${notePrefix} | ${parsed.note}`.slice(0, 1800)
    : notePrefix;
  const { error } = await supabase.from("line_booking_records").insert({
    group_id: groupId,
    project_name: parsed.projectName || null,
    shop_name: parsed.shopName,
    phone: parsed.phone || "ไม่ระบุ",
    booth_code: parsed.boothCode || null,
    product_type: parsed.productType || null,
    note: combinedNote,
    table_free_qty: parsed.tableFreeQty ?? 0,
    table_extra_qty: parsed.tableExtraQty ?? 0,
    chair_free_qty: parsed.chairFreeQty ?? 0,
    chair_extra_qty: parsed.chairExtraQty ?? 0,
    power_amp: parsed.powerAmp ?? null,
    power_label: parsed.powerLabel || null,
    booth_price: parsed.boothPrice ?? null,
    booking_status: "pending_replace",
    source_user_id: source.userId ?? null,
    source_message_id: messageId ?? null,
  });
  if (error) console.error("[pending] insertPendingReplaceRecord:", error?.message);
}

async function deletePendingDbRecords(groupId, status) {
  const { error } = await supabase
    .from("line_booking_records")
    .delete()
    .eq("group_id", groupId)
    .eq("booking_status", status);
  if (error) console.error(`[pending] deletePendingDbRecords(${status}):`, error?.message);
}

function formatAmount(amount, currency = "THB") {
  const numeric = Number(amount ?? 0);
  if (!Number.isFinite(numeric)) return `${amount} ${currency}`;

  const formatted = numeric.toLocaleString("en-US", {
    minimumFractionDigits: Number.isInteger(numeric) ? 0 : 2,
    maximumFractionDigits: 2,
  });

  return `${formatted} ${currency}`;
}

function normalizeParsedExpense(parsed) {
  return {
    projectName: normalizeSpaces(parsed?.projectName).slice(0, 160),
    amount: normalizeAmount(parsed?.amount),
    currency: normalizeSpaces(parsed?.currency ?? "THB").toUpperCase() || "THB",
    vendorName: normalizeSpaces(parsed?.vendorName).slice(0, 160),
    expenseType: normalizeSpaces(parsed?.expenseType).slice(0, 120),
    note: normalizeSpaces(parsed?.note).slice(0, 1800),
  };
}

function buildExpenseProjectQuestion(parsed) {
  const details = [
    `ยอด: ${formatAmount(parsed.amount, parsed.currency)}`,
    parsed.vendorName ? `ผู้ขาย/ร้าน: ${parsed.vendorName}` : null,
  ].filter(Boolean);

  return [
    "รับข้อมูลค่าใช้จ่ายแล้ว",
    ...details,
    "ยอดนี้ของโปรเจกต์ไหน?",
    "ตอบได้เลย เช่น: โปรเจกต์ Siam coffee culture",
    "ถ้าจะยกเลิก พิมพ์ /cancel-expense",
  ].join("\n");
}

function formatSavedExpenseMessage(data) {
  const lines = [
    `Saved expense (#${shortId(data.id)})`,
    `Project: ${data.project_name}`,
    `Amount: ${formatAmount(data.amount, data.currency ?? "THB")}`,
    `Vendor: ${data.vendor_name ?? "-"}`,
  ];

  if (data.expense_type) lines.push(`Type: ${data.expense_type}`);
  if (data.note) lines.push("Note: metadata saved");
  return lines.join("\n");
}

async function insertExpenseRecord(parsed, source, messageId) {
  return supabase
    .from("line_expense_records")
    .insert({
      group_id: getGroupIdFromSource(source),
      project_name: parsed.projectName,
      amount: parsed.amount,
      currency: parsed.currency,
      vendor_name: parsed.vendorName || null,
      expense_type: parsed.expenseType || null,
      note: parsed.note || null,
      expense_status: "recorded",
      source_user_id: source.userId ?? null,
      source_message_id: messageId ?? null,
    })
    .select("id, project_name, amount, currency, vendor_name, expense_type, note")
    .single();
}

async function saveExpenseWithProjectPrompt(parsed, source, messageId) {
  const normalized = normalizeParsedExpense(parsed);

  if (!hasRequiredExpenseFields(normalized)) {
    return {
      ok: false,
      message: [
        "Missing expense amount",
        "Example: /expense amount=2500 vendor=ABC project=Siam coffee culture note=ค่าช่างติดตั้ง",
      ].join("\n"),
    };
  }

  if (!normalized.projectName) {
    normalized.projectName = DEFAULT_EXPENSE_PROJECT_NAME;
  }

  const { data, error } = await insertExpenseRecord(normalized, source, messageId);
  if (error) {
    console.error(error);
    if (isMissingExpenseTableError(error)) {
      return { ok: false, message: EXPENSE_MIGRATION_HINT };
    }
    return { ok: false, message: "Failed to save expense. Please try again." };
  }

  clearPendingExpense(source);
  return {
    ok: true,
    message: formatSavedExpenseMessage(data),
  };
}

async function commandConfirmPendingExpenseProject(text, source, messageId) {
  const pending = getPendingExpense(source);
  if (!pending) return null;

  const projectName = parseProjectAnswer(text);
  if (!projectName) {
    return "กรุณาระบุชื่อโปรเจกต์ เช่น โปรเจกต์ Siam coffee culture";
  }

  const result = await saveExpenseWithProjectPrompt(
    {
      ...pending.parsed,
      projectName,
    },
    source,
    pending.messageId ?? messageId,
  );

  return result.message;
}

async function commandExpense(text, source, messageId, options = {}) {
  const parsed = parseExpenseCommand(text);

  if (options.sourceTag) {
    parsed.note = normalizeSpaces(
      `${options.sourceTag}${parsed.note ? ` | ${parsed.note}` : ""}`,
    ).slice(0, 1800);
  }

  const result = await saveExpenseWithProjectPrompt(parsed, source, messageId);
  if (!result.ok) return result.message;

  if (options.sourceTag === "image_ocr") {
    return `${result.message}\n(source: image OCR)`;
  }

  return result.message;
}

async function commandCancelExpense(source) {
  clearPendingExpense(source);
  return "ตอนนี้ค่าใช้จ่ายจะบันทึกทันทีแล้ว ไม่ต้องรอยืนยันโปรเจกต์";
}

function formatSavedBookingMessage(data, note) {
  const projectName = String(data.project_name ?? "-").slice(0, 60);
  const booth = normalizeBoothCode(data.booth_code) || "-";
  const lines = [
    `Saved (#${shortId(data.id)})`,
    `งาน: ${projectName} | บูธ ${booth}`,
    `ร้าน: ${data.shop_name} | โทร: ${data.phone}`,
  ];
  if (data.booth_price) lines.push(`ราคาบูธ: ${formatAmount(data.booth_price, "THB")}`);
  return lines.join("\n");
}


function formatDateInTz(date, timeZone = LINE_TIMEZONE) {
  const parts = new Intl.DateTimeFormat("en-CA", {
    timeZone,
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  }).formatToParts(date);

  const y = parts.find((p) => p.type === "year")?.value;
  const m = parts.find((p) => p.type === "month")?.value;
  const d = parts.find((p) => p.type === "day")?.value;

  if (!y || !m || !d) return "";
  return `${y}-${m}-${d}`;
}

function getDailyRangeUtc(dateStr) {
  const m = String(dateStr ?? "").match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if (!m) return null;
  const year = Number(m[1]);
  const month = Number(m[2]);
  const day = Number(m[3]);

  if (month < 1 || month > 12 || day < 1 || day > 31) return null;

  // LINE_TIMEZONE defaults to Bangkok (+07:00); keep logic explicit for stable daily cut.
  const tzOffsetHours = LINE_TIMEZONE === "Asia/Bangkok" ? 7 : 0;
  const startUtc = new Date(Date.UTC(year, month - 1, day, -tzOffsetHours, 0, 0));
  const endUtc = new Date(startUtc.getTime() + 24 * 60 * 60 * 1000);
  return {
    startIso: startUtc.toISOString(),
    endIso: endUtc.toISOString(),
  };
}

function parseExportDateFromText(text) {
  const raw = String(text ?? "");
  const explicit = raw.match(/(\d{4}-\d{2}-\d{2})/);
  if (explicit) return explicit[1];
  return formatDateInTz(new Date());
}

function csvEscape(value) {
  const s = String(value ?? "");
  if (/[",\n]/.test(s)) return `"${s.replace(/"/g, '""')}"`;
  return s;
}

function withUtf8Bom(value) {
  const text = String(value ?? "");
  return text.startsWith("\uFEFF") ? text : `\uFEFF${text}`;
}

function buildDailyCsv(rows) {
  const headers = [
    "booked_at",
    "project_name",
    "shop_name",
    "phone",
    "booth_code",
    "product_type",
    "booth_price",
    "table_free_qty",
    "table_extra_qty",
    "chair_free_qty",
    "chair_extra_qty",
    "power_amp",
    "power_label",
    "booking_status",
    "note",
    "source_user_id",
  ];

  const lines = [headers.join(",")];
  for (const row of rows) {
    lines.push(
      [
        row.booked_at,
        row.project_name,
        row.shop_name,
        row.phone,
        row.booth_code,
        row.product_type,
        row.booth_price,
        row.table_free_qty,
        row.table_extra_qty,
        row.chair_free_qty,
        row.chair_extra_qty,
        row.power_amp,
        row.power_label,
        row.booking_status,
        row.note,
        row.source_user_id,
      ]
        .map(csvEscape)
        .join(","),
    );
  }
  return `${lines.join("\n")}\n`;
}

function buildExpenseCsv(rows) {
  const headers = [
    "paid_at",
    "project_name",
    "amount",
    "currency",
    "vendor_name",
    "expense_type",
    "expense_status",
    "note",
    "source_user_id",
  ];

  const lines = [headers.join(",")];
  for (const row of rows) {
    lines.push(
      [
        row.paid_at,
        row.project_name,
        row.amount,
        row.currency,
        row.vendor_name,
        row.expense_type,
        row.expense_status,
        row.note,
        row.source_user_id,
      ]
        .map(csvEscape)
        .join(","),
    );
  }

  return `${lines.join("\n")}\n`;
}

async function fetchDailyExpenses(groupId, dateStr, projectName = "") {
  const range = getDailyRangeUtc(dateStr);
  if (!range) return { error: new Error("Invalid date format") };

  let query = supabase
    .from("line_expense_records")
    .select("id, paid_at, project_name, amount, currency, vendor_name, expense_type, expense_status, note, source_user_id")
    .gte("paid_at", range.startIso)
    .lt("paid_at", range.endIso)
    .order("paid_at", { ascending: true });

  if (groupId) query = query.eq("group_id", groupId);
  if (projectName) query = query.eq("project_name", projectName);

  const { data, error } = await query;
  return { data: data ?? [], error };
}

async function fetchDailyBookings(groupId, dateStr) {
  const range = getDailyRangeUtc(dateStr);
  if (!range) return { error: new Error("Invalid date format") };

  let query = supabase
    .from("line_booking_records")
    .select(
      "booked_at, project_name, shop_name, phone, booth_code, product_type, booth_price, table_free_qty, table_extra_qty, chair_free_qty, chair_extra_qty, power_amp, power_label, booking_status, note, source_user_id",
    )
    .gte("booked_at", range.startIso)
    .lt("booked_at", range.endIso)
    .order("booked_at", { ascending: true });

  if (groupId) query = query.eq("group_id", groupId);

  const { data, error } = await query;
  if (!error) {
    return { data: data ?? [], error: null };
  }

  if (!isMissingSalesSchemaError(error)) {
    return { data: [], error };
  }

  let fallbackQuery = supabase
    .from("line_booking_records")
    .select(
      "booked_at, project_name, shop_name, phone, booth_code, product_type, table_free_qty, table_extra_qty, chair_free_qty, chair_extra_qty, power_amp, power_label, booking_status, note, source_user_id",
    )
    .gte("booked_at", range.startIso)
    .lt("booked_at", range.endIso)
    .order("booked_at", { ascending: true });

  if (groupId) fallbackQuery = fallbackQuery.eq("group_id", groupId);

  const fallback = await fallbackQuery;
  if (fallback.error) return { data: [], error: fallback.error };

  const rows = (fallback.data ?? []).map((row) => ({ ...row, booth_price: null }));
  return { data: rows, error: null };
}

function buildExportUrl(groupId, dateStr) {
  if (!LINE_PUBLIC_BASE_URL || !LINE_EXPORT_TOKEN) return "";
  const params = new URLSearchParams({
    group: groupId,
    date: dateStr,
    token: LINE_EXPORT_TOKEN,
  });
  return `${LINE_PUBLIC_BASE_URL}/exports/daily.csv?${params.toString()}`;
}

function buildInstallExportUrl(groupId, projectName) {
  if (!LINE_PUBLIC_BASE_URL || !LINE_EXPORT_TOKEN) return "";
  const params = new URLSearchParams({
    group: groupId,
    token: LINE_EXPORT_TOKEN,
  });
  if (projectName) params.set("project", projectName);
  return `${LINE_PUBLIC_BASE_URL}/exports/install.csv?${params.toString()}`;
}

function buildExpenseExportUrl(groupId, dateStr, projectName = "") {
  if (!LINE_PUBLIC_BASE_URL || !LINE_EXPORT_TOKEN) return "";
  const params = new URLSearchParams({
    group: groupId,
    date: dateStr,
    token: LINE_EXPORT_TOKEN,
  });
  if (projectName) params.set("project", projectName);
  return `${LINE_PUBLIC_BASE_URL}/exports/expense.csv?${params.toString()}`;
}

async function fetchProjectBookings(groupId, projectName) {
  let query = supabase
    .from("line_booking_records")
    .select(
      "id, booked_at, project_name, shop_name, phone, booth_code, product_type, booth_price, table_free_qty, table_extra_qty, chair_free_qty, chair_extra_qty, power_amp, power_label, booking_status, note, source_user_id",
    )
    .eq("booking_status", "booked")
    .order("booth_code", { ascending: true });

  if (groupId) query = query.eq("group_id", groupId);
  if (projectName) query = query.eq("project_name", projectName);

  const { data, error } = await query;
  if (error) return { data: [], error };
  return { data: data ?? [], error: null };
}

function buildMasterCsv(rows, days = 1) {
  const headers = [
    "ลำดับ",
    "ชื่อร้าน",
    "เบอร์โทร",
    "บูธ",
    "ราคา/วัน",
    "วัน",
    "ยอดรวม",
    "ค่าไฟ",
    "ยอดก่อนภาษี",
    "VAT 7%",
    "หัก WHT 3%",
    "ยอดสุทธิ",
    "กำลังไฟ (A)",
    "หมายเหตุ",
  ];

  const numDays = Math.max(1, Number(days) || 1);
  const lines = [headers.join(",")];
  let totalBefore = 0;
  let totalVat = 0;
  let totalWht = 0;
  let totalNet = 0;

  for (let i = 0; i < rows.length; i++) {
    const row = rows[i];
    const price = Number(row.booth_price ?? 0);
    const subtotal = price * numDays;
    const electricity = 0; // left for manual entry
    const beforeTax = subtotal + electricity;
    const vat = Math.round(beforeTax * 0.07 * 100) / 100;
    const wht = Math.round(beforeTax * 0.03 * 100) / 100;
    const net = Math.round((beforeTax + vat - wht) * 100) / 100;
    totalBefore += beforeTax;
    totalVat += vat;
    totalWht += wht;
    totalNet += net;

    lines.push(
      [
        i + 1,
        row.shop_name,
        row.phone,
        row.booth_code,
        price || "",
        numDays,
        subtotal || "",
        electricity || "",
        beforeTax || "",
        vat || "",
        wht || "",
        net || "",
        row.power_amp ?? "",
        row.note ?? "",
      ]
        .map(csvEscape)
        .join(","),
    );
  }

  // totals row
  lines.push(
    [
      "รวม",
      "", "", "", "", "", "", "",
      Math.round(totalBefore * 100) / 100,
      Math.round(totalVat * 100) / 100,
      Math.round(totalWht * 100) / 100,
      Math.round(totalNet * 100) / 100,
      "", "",
    ]
      .map(csvEscape)
      .join(","),
  );

  return `${lines.join("\n")}\n`;
}

function buildMasterExportUrl(groupId, projectName, days = 1) {
  if (!LINE_PUBLIC_BASE_URL || !LINE_EXPORT_TOKEN) return "";
  const params = new URLSearchParams({ group: groupId, token: LINE_EXPORT_TOKEN });
  if (projectName) params.set("project", projectName);
  if (days && days !== 1) params.set("days", String(days));
  return `${LINE_PUBLIC_BASE_URL}/exports/master.csv?${params.toString()}`;
}
function buildInstallCsv(rows) {
  const headers = [
    "project_name",
    "booth_code",
    "shop_name",
    "phone",
    "table_free_qty",
    "table_extra_qty",
    "table_total_qty",
    "chair_free_qty",
    "chair_extra_qty",
    "chair_total_qty",
    "power_amp",
    "power_label",
    "note",
  ];

  const lines = [headers.join(",")];
  for (const row of rows) {
    const tableTotal = Number(row.table_free_qty ?? 0) + Number(row.table_extra_qty ?? 0);
    const chairTotal = Number(row.chair_free_qty ?? 0) + Number(row.chair_extra_qty ?? 0);

    lines.push(
      [
        row.project_name,
        row.booth_code,
        row.shop_name,
        row.phone,
        row.table_free_qty,
        row.table_extra_qty,
        tableTotal,
        row.chair_free_qty,
        row.chair_extra_qty,
        chairTotal,
        row.power_amp,
        row.power_label,
        row.note,
      ]
        .map(csvEscape)
        .join(","),
    );
  }

  return `${lines.join("\n")}\n`;
}

async function fetchInstallBookings(groupId, projectName = "") {
  let query = supabase
    .from("line_booking_records")
    .select("project_name, shop_name, phone, booth_code, table_free_qty, table_extra_qty, chair_free_qty, chair_extra_qty, power_amp, power_label, note, booked_at")
    .eq("group_id", groupId)
    .eq("booking_status", "booked")
    .order("project_name", { ascending: true })
    .order("booth_code", { ascending: true })
    .order("shop_name", { ascending: true })
    .limit(2000);

  if (projectName) query = query.eq("project_name", projectName);

  const { data, error } = await query;
  return { data: data ?? [], error };
}



function commandAgentStatus() {
  return [
    "Agent mode is active.",
    "- Reads booking text/forms/images (OCR)",
    "- Detects duplicate booth in same project",
    "- Waits for confirmation before replacing duplicate booking",
    "- Tracks table/chair/power requirements",
    "- Can parse expense messages and ask project confirmation",
    "- Supports /summary, /sales-summary, /install, /cancel, /expense-summary, and CSV exports",
    "- Use external cron script for automatic backups",
  ].join("\n");
}

async function commandSetProject(text, source) {
  const projectName = normalizeSpaces(
    text.replace(/^\/(?:ผูกโปรเจกต์|bind-project|bind)\s*/i, ""),
  );
  if (!projectName) {
    return "Please provide project name, e.g. /bind-project Craft Corner March 2026";
  }
  const groupId = getGroupIdFromSource(source);
  const { error } = await upsertGroupDefaultProject(groupId, projectName);
  if (error) {
    console.error(error);
    return "Set default project failed. Please try again.";
  }
  return `Default project set to: ${projectName}`;
}

async function commandSetBoothPrice(text, source) {
  const groupId = getGroupIdFromSource(source);
  const kv = parseKvSegments(text);

  let projectName = normalizeSpaces(
    pickValue(kv, ["โปรเจกต์", "โปรเจค", "project"]),
  );

  if (!projectName) {
    projectName = await getGroupDefaultProject(groupId);
  }

  const price =
    normalizeAmount(
      pickValue(kv, ["ราคา", "price", "booth_price", "ค่าบูธ", "ค่าเช่า"]),
    ) ?? extractAmountFromText(text);

  if (!projectName) {
    return "Please provide project name: /set-price project=<name> price=<baht>";
  }

  if (!Number.isFinite(price) || price <= 0) {
    return "Please provide booth price: /set-price project=<name> price=<baht>";
  }

  const { error } = await upsertProjectBoothPrice(groupId, projectName, price);
  if (error) {
    console.error(error);
    if (isMissingSalesSchemaError(error)) return SALES_MIGRATION_HINT;
    return "Failed to set booth price. Please try again.";
  }

  return `Booth price set: ${projectName} = ${formatAmount(price, "THB")}`;
}

// ─── /set-event ───────────────────────────────────────────────────────────────
async function commandSetEvent(text, source) {
  const groupId = getGroupIdFromSource(source);
  const kv = parseKvSegments(text);

  let projectName = normalizeSpaces(pickValue(kv, ["project", "โปรเจกต์", "โปรเจค"]));
  if (!projectName) projectName = await getGroupDefaultProject(groupId);
  if (!projectName) return "ระบุชื่องาน: /set-event project=<ชื่อ> start=YYYY-MM-DD end=YYYY-MM-DD [notify=3]";

  const startRaw = pickValue(kv, ["start", "start_date", "วันเริ่ม"]);
  const endRaw   = pickValue(kv, ["end",   "end_date",   "วันจบ", "วันสุดท้าย"]);
  const notifyDays = parseInt(pickValue(kv, ["notify", "แจ้งก่อน", "days"]) ?? "3", 10);

  const dateRe = /^\d{4}-\d{2}-\d{2}$/;
  if (!startRaw || !dateRe.test(startRaw)) return "ระบุวันเริ่มงาน: start=YYYY-MM-DD";

  const eventEnd = (endRaw && dateRe.test(endRaw)) ? endRaw : startRaw;

  const eventPayload = {
    event_start_date: startRaw,
    event_end_date: eventEnd,
    notify_days_before: Number.isFinite(notifyDays) && notifyDays > 0 ? notifyDays : 3,
    notify_group_id: groupId,
  };

  // Try update first (avoids booth_price NOT NULL issue on insert)
  const { data: updated, error: updateError } = await supabase
    .from("line_project_pricing")
    .update(eventPayload)
    .eq("group_id", groupId)
    .eq("project_name", projectName)
    .select("project_name");

  if (updateError) { console.error(updateError); return "บันทึกวันงานไม่สำเร็จ กรุณาลองใหม่"; }

  // No existing row — insert new one with booth_price=0 as placeholder
  if (!updated?.length) {
    const { error: insertError } = await supabase
      .from("line_project_pricing")
      .insert({ group_id: groupId, project_name: projectName, booth_price: 0, ...eventPayload });
    if (insertError) { console.error(insertError); return "บันทึกวันงานไม่สำเร็จ กรุณาลองใหม่"; }
  }

  return [
    `📅 ตั้งวันงานแล้ว: ${projectName}`,
    `วันเริ่ม: ${startRaw}  วันสุดท้าย: ${eventEnd}`,
    `แจ้งเตือนก่อน: ${notifyDays} วัน`,
  ].join("\n");
}

// ─── Daily event-reminder scheduler ─────────────────────────────────────────
async function runEventReminderScheduler() {
  const todayStr = new Date().toISOString().slice(0, 10); // YYYY-MM-DD

  const { data, error } = await supabase
    .from("line_project_pricing")
    .select("group_id, project_name, event_start_date, event_end_date, notify_days_before, notify_group_id")
    .not("event_start_date", "is", null);

  if (error || !data?.length) return;

  for (const row of data) {
    const notifyDays = row.notify_days_before ?? 3;
    const targetGroupId = row.notify_group_id || row.group_id;
    if (!targetGroupId || !row.event_start_date) continue;

    // Calculate date that is notifyDays before start
    const eventDate = new Date(row.event_start_date);
    const notifyDate = new Date(eventDate);
    notifyDate.setDate(notifyDate.getDate() - notifyDays);
    const notifyDateStr = notifyDate.toISOString().slice(0, 10);

    if (notifyDateStr !== todayStr) continue;

    // Build summary for this project
    const fakeSource = { type: "group", groupId: targetGroupId };
    const { data: bookings, error: bErr } = await fetchInstallBookings(fakeSource, row.project_name);
    if (bErr || !bookings?.length) {
      await pushTextToGroup(targetGroupId, `📅 แจ้งเตือน: ${row.project_name} เริ่มในอีก ${notifyDays} วัน (${row.event_start_date})\nยังไม่มีข้อมูลการจอง`);
      continue;
    }

    let tableTotal = 0, chairTotal = 0;
    const lines = [
      `📅 แจ้งเตือนก่อนงาน ${notifyDays} วัน`,
      `งาน: ${row.project_name}`,
      `วันเริ่ม: ${row.event_start_date}${row.event_end_date !== row.event_start_date ? `  วันสุดท้าย: ${row.event_end_date}` : ""}`,
      `รวม ${bookings.length} บูธ`,
      "─".repeat(40),
      "บูธ | ร้าน | โต๊ะ | เก้าอี้ | ไฟ",
    ];

    const powerSummary = {};
    for (const b of bookings.slice(0, 80)) {
      const t = Number(b.table_free_qty ?? 0) + Number(b.table_extra_qty ?? 0);
      const c = Number(b.chair_free_qty ?? 0) + Number(b.chair_extra_qty ?? 0);
      tableTotal += t; chairTotal += c;
      const pwr = b.power_label || (b.power_amp ? `${b.power_amp}A` : "ฟรี");
      powerSummary[pwr] = (powerSummary[pwr] ?? 0) + 1;
      lines.push(`${normalizeBoothCode(b.booth_code) || "-"} | ${b.shop_name} | ${t}ตัว | ${c}ตัว | ${pwr}`);
    }
    if (bookings.length > 80) lines.push(`... และอีก ${bookings.length - 80} บูธ`);

    lines.push("─".repeat(40));
    lines.push(`รวมโต๊ะ: ${tableTotal} | รวมเก้าอี้: ${chairTotal}`);
    lines.push("สรุปไฟ: " + Object.entries(powerSummary).map(([l, n]) => `${l}×${n}`).join(", "));

    await pushTextToGroup(targetGroupId, lines.join("\n").slice(0, 4900));
  }
}

async function pushTextToGroup(groupId, text) {
  try {
    await lineClient.pushMessage({ to: groupId, messages: [{ type: "text", text }] });
  } catch (err) {
    console.error("[event-reminder] push failed:", err?.message ?? err);
  }
}

async function buildProjectSalesSnapshot(groupId, projectName) {
  if (!projectName) return "";

  const { data, error } = await supabase
    .from("line_booking_records")
    .select("booth_price")
    .eq("group_id", groupId)
    .eq("project_name", projectName)
    .eq("booking_status", "booked")
    .limit(5000);

  if (error) {
    if (!isMissingSalesSchemaError(error)) console.error(error);
    return "";
  }

  const rows = data ?? [];
  if (!rows.length) return "";

  const total = rows.reduce((sum, row) => {
    const price = normalizeAmount(row.booth_price);
    return price === null ? sum : sum + price;
  }, 0);

  return `Sales snapshot: ${projectName} | ${rows.length} booths | ${formatAmount(total, "THB")}`;
}
async function commandSalesSummary(text, source) {
  const groupId = getGroupIdFromSource(source);
  const projectFilter = parseProjectFilter(text);

  let query = supabase
    .from("line_booking_records")
    .select("project_name, booth_code, booth_price")
    .eq("group_id", groupId)
    .eq("booking_status", "booked")
    .order("project_name", { ascending: true })
    .limit(5000);

  if (projectFilter) query = query.eq("project_name", projectFilter);

  const { data, error } = await query;
  if (error) {
    console.error(error);
    if (isMissingSalesSchemaError(error)) return SALES_MIGRATION_HINT;
    return "Failed to generate sales summary";
  }

  const rows = data ?? [];
  if (!rows.length) {
    return projectFilter
      ? `No active bookings found in project: ${projectFilter}`
      : "No active bookings found";
  }

  const map = new Map();
  for (const row of rows) {
    const projectName = normalizeSpaces(row.project_name) || "(no project)";
    if (!map.has(projectName)) {
      map.set(projectName, {
        booths: 0,
        total: 0,
        prices: [],
        missingPrice: 0,
      });
    }

    const bucket = map.get(projectName);
    bucket.booths += 1;

    const price = normalizeAmount(row.booth_price);
    if (price === null) {
      bucket.missingPrice += 1;
    } else {
      bucket.prices.push(price);
      bucket.total += price;
    }
  }

  const lines = [
    projectFilter ? `Sales summary: ${projectFilter}` : "Sales summary: all projects",
    "Format: Project | Booths sold | Price | Sales total",
    "",
  ];

  let grandBooths = 0;
  let grandTotal = 0;

  const projects = Array.from(map.entries()).sort((a, b) => a[0].localeCompare(b[0]));
  for (let i = 0; i < projects.length; i += 1) {
    const [projectName, item] = projects[i];
    grandBooths += item.booths;
    grandTotal += item.total;

    let priceText = "Price not set";
    if (item.prices.length) {
      const minPrice = Math.min(...item.prices);
      const maxPrice = Math.max(...item.prices);
      priceText = minPrice === maxPrice
        ? formatAmount(minPrice, "THB")
        : `${formatAmount(minPrice, "THB")} - ${formatAmount(maxPrice, "THB")}`;
    }

    let line = `${i + 1}. ${projectName} | ${item.booths} booths | ${priceText} | ${formatAmount(item.total, "THB")}`;
    if (item.missingPrice > 0) {
      line += ` | Missing price: ${item.missingPrice}`;
    }
    lines.push(line);
  }

  lines.push("");
  lines.push(`Grand total: ${grandBooths} booths | ${formatAmount(grandTotal, "THB")}`);

  return lines.join("\n").slice(0, 4900);
}
async function commandBooking(text, source, messageId) {
  const groupId = getGroupIdFromSource(source);
  let parsed = await parseBookingCommand(text, groupId);

  if (!hasRequiredBookingFields(parsed) && looksLikeBookingForm(text)) {
    parsed = await parseBookingFormText(text, groupId);
  }

  // If still missing fields, show Thai help with active projects
  if (!hasRequiredBookingFields(parsed)) {
    let activeProjects = [];
    if (groupId) {
      const todayStr = getTimePartsInTz(new Date()).dateStr;
      const { data } = await supabase
        .from("line_project_pricing")
        .select("project_name")
        .eq("group_id", groupId)
        .gte("event_end_date", todayStr)
        .order("event_start_date", { ascending: true });
      activeProjects = (data ?? []).map((p) => p.project_name).filter(Boolean);
    }
    const projectHint = activeProjects.length
      ? `โปรเจกต์ที่เปิดอยู่: ${activeProjects.join(", ")}`
      : "";
    const exampleProject = activeProjects[0] || "ชื่องาน";
    return [
      "❌ ข้อมูลไม่ครบ — ต้องระบุ โปรเจกต์ และ ชื่อร้าน",
      projectHint,
      "",
      "ตัวอย่าง:",
      `/จอง โปรเจกต์=${exampleProject} ร้าน=ชื่อร้าน บูธ=A01`,
    ].filter(Boolean).join("\n");
  }

  const result = await saveBookingWithAgentRules(parsed, source, messageId);
  return result.silent ? null : result.message;
}

async function commandBookingFromTemplate(text, source, messageId) {
  const groupId = getGroupIdFromSource(source);
  const parsed = await parseBookingFormText(text, groupId);

  if (!hasRequiredBookingFields(parsed)) {
    return [
      "Unable to read complete form. Please provide at least:",
      "- project",
      "- shop",
    ].join("\n");
  }

  const result = await saveBookingWithAgentRules(parsed, source, messageId);
  return result.silent ? null : result.message;
}

async function tryAutoBookingText(text, source, messageId, lineEvent = null) {
  const looksLike = looksLikeBookingText(text);
  console.log(`[text] looksLikeBooking=${looksLike} chars=${text.length} preview=${text.slice(0, 60).replace(/\n/g, "↵")}`);
  if (!looksLike) return null;

  const groupId = getGroupIdFromSource(source);
  const parsedCandidates = [
    await parseBookingFormText(text, groupId),
    await parseBookingCommand(text, groupId),
  ];
  const parsed = parsedCandidates.find((candidate) => hasRequiredBookingFields(candidate));
  console.log(`[text] structuredParse project=${parsed?.projectName ?? "-"} shop=${parsed?.shopName ?? "-"} booth=${parsed?.boothCode ?? "-"}`);

  if (!parsed) {
    if (!LINE_AI_TEXT_FALLBACK_ENABLED) return null;
    const analysis = await callAIForTextParse(text, lineEvent);
    const classification = inferAiClassification(analysis, text);
    if (classification !== "booking") return null;
    const aiParsed = buildBookingFromAiAnalysis(analysis, text);
    // Resolve projectName from active projects if AI couldn't extract it
    if (!aiParsed.projectName) {
      const gid = getGroupIdFromSource(source);
      if (gid) {
        const todayStr = getTimePartsInTz(new Date()).dateStr;
        const { data: projData } = await supabase
          .from("line_project_pricing")
          .select("project_name")
          .eq("group_id", gid)
          .gte("event_end_date", todayStr)
          .order("event_start_date", { ascending: true });
        const activeProjects = (projData ?? []).map((p) => p.project_name).filter(Boolean);
        if (activeProjects.length === 1) {
          aiParsed.projectName = activeProjects[0];
        } else if (activeProjects.length > 1) {
          setPendingProjectSelection(source, { parsed: aiParsed, messageId });
          insertPendingProjectRecord(aiParsed, source, messageId).catch(console.error);
          const projectList = activeProjects.map((p, i) => `${i + 1}. ${p}`).join("\n");
          return `📋 ข้อมูลร้าน: ${aiParsed.shopName || "?"}\nกรุณาพิมพ์ชื่อโปรเจกต์ที่ต้องการจอง:\n${projectList}`;
        } else {
          const def = await getGroupDefaultProject(gid);
          if (def) aiParsed.projectName = def;
        }
      }
    }
    if (!hasRequiredBookingFields(aiParsed)) return null;
    const aiSaveResult = await saveBookingWithAgentRules(aiParsed, source, messageId);
    console.log(`[text] ai:saveResult ok=${aiSaveResult.ok} silent=${aiSaveResult.silent ?? false} project=${aiParsed.projectName ?? "-"} shop=${aiParsed.shopName ?? "-"}`);
    const pushTarget = getImageDigestPushTarget(source ?? {});
    if (aiSaveResult.ok && pushTarget) {
      queueImageDigestEvent({
        pushTarget,
        groupId: getGroupIdFromSource(source),
        sourceType: source?.type ?? "",
        messageId: messageId ?? null,
        category: "booking",
        status: "saved",
        sourceTag: "text",
        reason: "ai_parsed",
        projectName: aiParsed.projectName ?? "",
        shopName: aiParsed.shopName ?? "",
        boothCode: aiParsed.boothCode ?? "",
        vendorName: "",
        amount: null,
        currency: "THB",
        detail: "",
      });
      startImageDigestScheduler();
    }
    // In 1:1 chat reply directly since there is no group digest
    if (!source?.groupId && !source?.roomId) {
      return aiSaveResult.message ?? (aiSaveResult.ok ? `✅ บันทึกแล้ว: ${aiParsed.shopName ?? ""} บูธ ${aiParsed.boothCode ?? "-"} (${aiParsed.projectName ?? "-"})` : null);
    }
    return null; // silent — reported in digest
  }

  const result = await saveBookingWithAgentRules(parsed, source, messageId);
  console.log(`[text] saveResult ok=${result.ok} silent=${result.silent ?? false} needsConfirmation=${result.needsConfirmation ?? false}`);
  if (!result.ok) return result.needsConfirmation ? result.message : null;
  const bkPushTarget = getImageDigestPushTarget(source ?? {});
  const isDirectChat = !source?.groupId && !source?.roomId;
  if (bkPushTarget) {
    queueImageDigestEvent({
      pushTarget: bkPushTarget,
      groupId: getGroupIdFromSource(source),
      sourceType: source?.type ?? "",
      messageId: messageId ?? null,
      category: "booking",
      status: "saved",
      sourceTag: "text",
      reason: "auto_parsed",
      projectName: parsed.projectName ?? "",
      shopName: parsed.shopName ?? "",
      boothCode: parsed.boothCode ?? "",
      vendorName: "",
      amount: null,
      currency: "THB",
      detail: "",
    });
    startImageDigestScheduler();
  }
  // In 1:1 chat there is no group digest — reply directly
  if (isDirectChat) return result.message ?? `✅ บันทึกแล้ว: ${parsed.shopName ?? ""} บูธ ${parsed.boothCode ?? "-"} (${parsed.projectName ?? "-"})`;
  return null; // silent - reported in digest
}

async function commandList(text, source) {
  const groupId = getGroupIdFromSource(source);
  const projectFilter = parseProjectFilter(text);
  let query = supabase
    .from("line_booking_records")
    .select("id, project_name, shop_name, phone, booth_code, booking_status, booked_at")
    .eq("group_id", groupId)
    .eq("booking_status", "booked")
    .order("booked_at", { ascending: false })
    .limit(20);

  if (projectFilter) query = query.eq("project_name", projectFilter);
  const { data, error } = await query;
  if (error) {
    console.error(error);
    return "ดึงรายการไม่สำเร็จ";
  }
  if (!data?.length) {
    return projectFilter
      ? `ยังไม่มีรายการจองในโปรเจกต์ ${projectFilter}`
      : "ยังไม่มีรายการจอง";
  }

  const header = projectFilter
    ? `รายการจองล่าสุด: ${projectFilter}`
    : "รายการจองล่าสุด";
  const lines = data.map((row, index) => {
    const booth = row.booth_code ? ` บูธ:${row.booth_code}` : "";
    return `${index + 1}. #${shortId(row.id)} ${row.shop_name} (${row.phone})${booth}`;
  });
  return [header, ...lines].join("\n").slice(0, 4900);
}

// Helper: build and send chunked shop list output
function buildShopListChunks(header, rows, showProjectTag) {
  const chunks = [];
  let current = "";
  const append = (line) => {
    if (current.length + line.length + 1 > 4900) { chunks.push(current.trim()); current = line; }
    else current += (current ? "\n" : "") + line;
  };
  append(header);
  rows.forEach((row, i) => {
    const booth = normalizeBoothCode(row.booth_code) || "-";
    const phone = row.phone || "ไม่ระบุ";
    const tag = showProjectTag ? ` | ${row.project_name || "-"}` : "";
    append(`${i + 1}. ${row.shop_name} | บูธ ${booth} | ${phone}${tag}`);
  });
  if (current.trim()) chunks.push(current.trim());
  return chunks.length === 1 ? chunks[0] : chunks;
}

// /ลิสงาน [ชื่องาน] — ลิสร้านในงาน (กรองด้วย ilike บน project_name)
async function commandProjectShopList(text, source) {
  const groupId = source?.groupId ?? source?.roomId ?? null; // null = 1:1 chat → no groupId filter
  const filter = normalizeSpaces(
    text.replace(/^\/ลิสงาน\s*/u, "").replace(/^\/project-shops?\s*/i, "")
  );

  let query = supabase
    .from("line_booking_records")
    .select("project_name, shop_name, booth_code, phone")
    .eq("booking_status", "booked")
    .order("booth_code", { ascending: true })
    .limit(1000);
  if (groupId) query = query.eq("group_id", groupId);
  if (filter) query = query.ilike("project_name", `%${filter}%`);

  const { data, error } = await query;
  if (error) { console.error(error); return "ดึงรายการไม่สำเร็จ"; }
  if (!data?.length) return filter ? `ไม่พบงานที่ชื่อ "${filter}"` : "ยังไม่มีรายการจองเลย";

  // Deduplicate: same shop + same booth (across project name variants) → most complete record
  const completeness = (row) =>
    (row.phone && row.phone !== "ไม่ระบุ" ? 4 : 0) +
    (row.project_name ? 2 : 0) +
    (row.booth_code ? 1 : 0);
  const dedupMap2 = new Map();
  for (const row of data) {
    const key = `${normalizeKey(row.shop_name)}::${normalizeBoothCode(row.booth_code) || "-"}`;
    const existing = dedupMap2.get(key);
    if (!existing || completeness(row) > completeness(existing)) dedupMap2.set(key, row);
  }
  const dedupedData = [...dedupMap2.values()];

  // Group by project, sort booths
  const projectMap = new Map();
  for (const row of dedupedData) {
    const proj = row.project_name || "(ไม่ระบุงาน)";
    if (!projectMap.has(proj)) projectMap.set(proj, []);
    projectMap.get(proj).push(row);
  }
  for (const [, rows] of projectMap) {
    rows.sort((a, b) => (normalizeBoothCode(a.booth_code) || "~").localeCompare(normalizeBoothCode(b.booth_code) || "~"));
  }
  const sortedProjects = [...projectMap.keys()].sort((a, b) => a.localeCompare(b));

  const chunks = [];
  let current = "";
  const append = (line) => {
    if (current.length + line.length + 1 > 4900) { chunks.push(current.trim()); current = line; }
    else current += (current ? "\n" : "") + line;
  };

  const header = filter
    ? `📋 ลิสงาน: ${filter} (${data.length} ร้าน)`
    : `📋 ลิสงานทั้งหมด (${sortedProjects.length} งาน | ${data.length} ร้าน)`;
  append(header);

  for (const proj of sortedProjects) {
    const rows = projectMap.get(proj);
    append(`\n── ${proj} (${rows.length} ร้าน) ──`);
    rows.forEach((row, i) => {
      const booth = normalizeBoothCode(row.booth_code) || "-";
      const phone = row.phone || "ไม่ระบุ";
      append(`${i + 1}. ${row.shop_name} | บูธ ${booth} | ${phone}`);
    });
  }

  if (current.trim()) chunks.push(current.trim());
  return chunks.length === 1 ? chunks[0] : chunks;
}

// /review — แสดงรายการรอตรวจสอบ (needs_project / pending_replace)
async function commandReview(source) {
  const groupId = source?.groupId ?? source?.roomId ?? null;

  let query = supabase
    .from("line_booking_records")
    .select("id, group_id, project_name, shop_name, booth_code, booking_status, booked_at")
    .in("booking_status", ["needs_project", "pending_replace"])
    .order("booked_at", { ascending: false })
    .limit(30);
  if (groupId) query = query.eq("group_id", groupId);

  const { data, error } = await query;
  if (error) { console.error(error); return "ดึงรายการไม่สำเร็จ"; }
  if (!data?.length) return "ไม่มีรายการรอตรวจสอบ ✅";

  const lines = [`⚠️ รายการรอตรวจสอบ (${data.length} รายการ)`];
  for (const [i, row] of data.entries()) {
    const sid = shortId(row.id);
    const booth = normalizeBoothCode(row.booth_code) || "-";
    const proj = row.project_name || "(ยังไม่ระบุงาน)";
    const typeLabel = row.booking_status === "needs_project" ? "ยังไม่ระบุงาน" : "บูธซ้ำรอยืนยัน";
    lines.push(`${i + 1}. #${sid} [${typeLabel}] ${row.shop_name} | บูธ ${booth} | ${proj}`);
  }
  lines.push("");
  lines.push("แก้ไข:");
  lines.push("  /review fix <id> project=<ชื่องาน>  (กำหนดงานให้รายการ needs_project)");
  lines.push("  /confirm-replace id=<id>  (ยืนยันแทนที่บูธซ้ำ)");
  return lines.join("\n").slice(0, 4900);
}

// /review fix <id> project=<name>
async function commandReviewFix(text, source) {
  const idMatch = text.match(/fix\s+([0-9a-fA-F]{6,36})/i);
  if (!idMatch) return "วิธีใช้: /review fix <id> project=<ชื่องาน>";
  const idPrefix = idMatch[1].toLowerCase();

  const kv = parseKvSegments(text.replace(/^\/review\s+fix\s+\S+\s*/i, "/_ "));
  const projectName = normalizeSpaces(pickValue(kv, ["project", "project_name", "โปรเจกต์", "งาน"]));
  if (!projectName) return "วิธีใช้: /review fix <id> project=<ชื่องาน>";

  const isDirectChat = !source?.groupId && !source?.roomId;
  let query = supabase
    .from("line_booking_records")
    .select("id, group_id, project_name, shop_name, phone, booth_code, product_type, note, " +
      "table_free_qty, table_extra_qty, chair_free_qty, chair_extra_qty, " +
      "power_amp, power_label, booth_price, source_user_id, source_message_id")
    .eq("booking_status", "needs_project")
    .order("booked_at", { ascending: false })
    .limit(200);
  if (!isDirectChat) query = query.eq("group_id", source.groupId ?? source.roomId);

  const { data, error } = await query;
  if (error) { console.error(error); return "ดึงรายการไม่สำเร็จ"; }

  const record = (data ?? []).find((r) => r.id.toLowerCase().startsWith(idPrefix));
  if (!record) return `ไม่พบรายการ #${idPrefix}`;

  const resolvedProject = await resolveCanonicalProjectName(record.group_id, projectName) ?? projectName;

  const syntheticSource = isDirectChat
    ? { groupId: record.group_id !== "direct" ? record.group_id : undefined, userId: source.userId }
    : source;

  // Delete the pending record first then re-run through normal booking pipeline
  await supabase.from("line_booking_records").delete().eq("id", record.id);

  const parsed = {
    projectName: resolvedProject,
    shopName: record.shop_name,
    phone: record.phone,
    boothCode: record.booth_code,
    productType: record.product_type,
    note: record.note,
    boothPrice: record.booth_price,
    tableFreeQty: record.table_free_qty,
    tableExtraQty: record.table_extra_qty,
    chairFreeQty: record.chair_free_qty,
    chairExtraQty: record.chair_extra_qty,
    powerAmp: record.power_amp,
    powerLabel: record.power_label,
  };

  const result = await saveBookingWithAgentRules(parsed, syntheticSource, record.source_message_id);
  if (result.silent) return null;
  return result.message;
}

// /ลิสร้าน [ชื่อร้าน] — ค้นหาร้านด้วยชื่อ (ilike บน shop_name)
async function commandShopList(text, source) {
  const groupId = source?.groupId ?? source?.roomId ?? null; // null = 1:1 chat → no groupId filter
  const filter = normalizeSpaces(
    text.replace(/^\/ลิสร้าน\s*/u, "").replace(/^\/shops?\s*/i, "")
  );

  let query = supabase
    .from("line_booking_records")
    .select("project_name, shop_name, booth_code, phone")
    .eq("booking_status", "booked")
    .limit(1000);
  if (groupId) query = query.eq("group_id", groupId);
  if (filter) query = query.ilike("shop_name", `%${filter}%`);

  const { data, error } = await query;
  if (error) { console.error(error); return "ดึงรายการไม่สำเร็จ"; }
  if (!data?.length) return filter ? `ไม่พบร้านที่ชื่อ "${filter}"` : "ยังไม่มีรายการจองเลย";

  // Deduplicate: same shop + same booth → keep the most complete record
  const completeness = (row) =>
    (row.phone && row.phone !== "ไม่ระบุ" ? 4 : 0) +
    (row.project_name ? 2 : 0) +
    (row.booth_code ? 1 : 0);
  const dedupMap = new Map();
  for (const row of data) {
    const key = `${normalizeKey(row.shop_name)}::${normalizeBoothCode(row.booth_code) || "-"}`;
    const existing = dedupMap.get(key);
    if (!existing || completeness(row) > completeness(existing)) dedupMap.set(key, row);
  }
  const deduped = [...dedupMap.values()];

  // Sort by project then booth
  const sorted = [...deduped].sort((a, b) => {
    const pc = (a.project_name || "").localeCompare(b.project_name || "");
    return pc !== 0 ? pc : (normalizeBoothCode(a.booth_code) || "~").localeCompare(normalizeBoothCode(b.booth_code) || "~");
  });

  const header = filter
    ? `🔍 ค้นร้าน: "${filter}" (${sorted.length} ผล)`
    : `🔍 ลิสร้านทั้งหมด (${sorted.length} ร้าน)`;
  return buildShopListChunks(header, sorted, true);
}

async function commandSummary(text, source) {
  const groupId = getGroupIdFromSource(source);
  const projectFilter = parseProjectFilter(text);

  let query = supabase
    .from("line_booking_records")
    .select("id, project_name, shop_name, phone, booth_code, booking_status, booked_at")
    .eq("group_id", groupId)
    .order("booked_at", { ascending: false })
    .limit(1000);

  if (projectFilter) query = query.eq("project_name", projectFilter);
  const { data, error } = await query;
  if (error) {
    console.error(error);
    return "Failed to generate summary";
  }

  const rows = data ?? [];
  if (!rows.length) {
    return projectFilter
      ? `No records found for project: ${projectFilter}`
      : "No booking records found";
  }

  const active = rows.filter((row) => row.booking_status === "booked");
  const cancelled = rows.length - active.length;

  const sortedActive = [...active].sort((a, b) => {
    const projectCmp = String(a.project_name ?? "").localeCompare(String(b.project_name ?? ""));
    if (projectCmp !== 0) return projectCmp;

    const boothA = normalizeBoothCode(a.booth_code ?? "~");
    const boothB = normalizeBoothCode(b.booth_code ?? "~");
    const boothCmp = boothA.localeCompare(boothB);
    if (boothCmp !== 0) return boothCmp;

    return String(a.shop_name ?? "").localeCompare(String(b.shop_name ?? ""));
  });

  const lines = [];
  lines.push(projectFilter ? `Summary: ${projectFilter}` : "Summary: all projects");
  lines.push(`Active ${active.length} | Cancelled ${cancelled} | Total ${rows.length}`);
  lines.push("");
  lines.push("Active booking list (Project | Booth | Shop | Phone):");

  if (!sortedActive.length) {
    lines.push("- none");
  } else {
    const maxRows = 80;
    for (let i = 0; i < Math.min(maxRows, sortedActive.length); i += 1) {
      const row = sortedActive[i];
      lines.push(
        `${i + 1}. ${row.project_name} | ${normalizeBoothCode(row.booth_code) || "-"} | ${row.shop_name} | ${row.phone}`,
      );
    }
    if (sortedActive.length > maxRows) {
      lines.push(`... and ${sortedActive.length - maxRows} more active bookings`);
    }
  }

  const boothMap = new Map();
  for (const row of sortedActive) {
    const booth = normalizeBoothCode(row.booth_code);
    if (!booth) continue;
    const key = `${row.project_name}::${booth}`;
    if (!boothMap.has(key)) boothMap.set(key, []);
    boothMap.get(key).push(row);
  }

  const duplicateEntries = Array.from(boothMap.entries())
    .filter(([, items]) => items.length > 1)
    .map(([key, items]) => ({ key, items }));

  lines.push("");
  if (!duplicateEntries.length) {
    lines.push("Duplicate booth check: no duplicates in active bookings");
  } else {
    lines.push("Duplicate booth warning:");
    for (const dup of duplicateEntries) {
      const [projectName, booth] = dup.key.split("::");
      const shops = dup.items.map((item) => `${item.shop_name}(${item.phone})`).join(", ");
      lines.push(`- ${projectName} | booth ${booth} -> ${dup.items.length} bookings: ${shops}`);
    }
    lines.push("If old booking was cancelled, use /confirm-replace after sending new booking");
  }

  return lines.join("\n").slice(0, 4900);
}


async function commandExpenseSummary(text, source) {
  const groupId = getGroupIdFromSource(source);
  const projectFilter = parseProjectFilter(text);
  const dateStr = parseExportDateFromText(text);

  const { data, error } = await fetchDailyExpenses(groupId, dateStr, projectFilter);
  if (error) {
    console.error(error);
    if (isMissingExpenseTableError(error)) return EXPENSE_MIGRATION_HINT;
    return "Failed to generate expense summary";
  }

  const rows = data ?? [];
  if (!rows.length) {
    return projectFilter
      ? `No expense records found on ${dateStr} for project: ${projectFilter}`
      : `No expense records found on ${dateStr}`;
  }

  const total = rows.reduce((sum, row) => sum + Number(row.amount ?? 0), 0);
  const sorted = [...rows].sort((a, b) => {
    const timeA = new Date(a.paid_at ?? 0).getTime();
    const timeB = new Date(b.paid_at ?? 0).getTime();
    return timeB - timeA;
  });

  const lines = [
    projectFilter
      ? `Expense summary ${dateStr} | ${projectFilter}`
      : `Expense summary ${dateStr} | all projects`,
    `Rows: ${rows.length} | Total: ${formatAmount(total, "THB")}`,
    "",
    "Latest expenses (Project | Amount | Vendor):",
  ];

  for (let i = 0; i < Math.min(30, sorted.length); i += 1) {
    const row = sorted[i];
    lines.push(
      `${i + 1}. ${row.project_name} | ${formatAmount(row.amount, row.currency ?? "THB")} | ${row.vendor_name ?? "-"}`,
    );
  }

  if (sorted.length > 30) {
    lines.push(`... and ${sorted.length - 30} more rows`);
  }

  return lines.join("\n").slice(0, 4900);
}

async function commandExportExpenseCsv(text, source) {
  const groupId = getGroupIdFromSource(source);
  const dateStr = parseExportDateFromText(text);
  const projectFilter = parseProjectFilter(text);

  if (!LINE_PUBLIC_BASE_URL || !LINE_EXPORT_TOKEN) {
    return [
      "Export link is not enabled yet",
      "Please set LINE_PUBLIC_BASE_URL and LINE_EXPORT_TOKEN in .env",
    ].join("\n");
  }

  const { data, error } = await fetchDailyExpenses(groupId, dateStr, projectFilter);
  if (error) {
    console.error(error);
    if (isMissingExpenseTableError(error)) return EXPENSE_MIGRATION_HINT;
    return "Failed to generate expense export link";
  }

  const url = buildExpenseExportUrl(groupId, dateStr, projectFilter);
  const scope = projectFilter ? ` | Project: ${projectFilter}` : "";
  return [
    `Expense CSV: ${dateStr}${scope}`,
    `Rows: ${data.length}`,
    url,
  ].join("\n");
}

async function commandInstallList(text, source) {
  const groupId = getGroupIdFromSource(source);
  let projectFilter = parseProjectFilter(text);
  if (!projectFilter) {
    projectFilter = await getGroupDefaultProject(groupId);
  }

  if (!projectFilter) {
    return "Please provide project name: /install project=<name> (or set default with /bind-project)";
  }

  const { data, error } = await fetchInstallBookings(groupId, projectFilter);
  if (error) {
    console.error(error);
    return "Failed to build installer list";
  }

  if (!data.length) {
    return `No active bookings found for installer list in project: ${projectFilter}`;
  }

  const lines = [
    `📋 ใบสั่งงานช่าง: ${projectFilter}`,
    `รวม ${data.length} บูธ`,
    "บูธ | ร้าน | โทร | โต๊ะ | เก้าอี้ | ไฟ",
    "─".repeat(50),
  ];

  let tableTotal = 0;
  let chairTotal = 0;
  let powerSummary = {};
  for (let i = 0; i < Math.min(120, data.length); i += 1) {
    const row = data[i];
    const tableCount = Number(row.table_free_qty ?? 0) + Number(row.table_extra_qty ?? 0);
    const chairCount = Number(row.chair_free_qty ?? 0) + Number(row.chair_extra_qty ?? 0);
    tableTotal += tableCount;
    chairTotal += chairCount;

    // Power label: prefer stored label, else infer from amp
    let powerDisplay = row.power_label || (row.power_amp ? `${row.power_amp}A` : "ฟรี 5A");
    // Count power types for summary
    powerSummary[powerDisplay] = (powerSummary[powerDisplay] ?? 0) + 1;

    lines.push(
      `${normalizeBoothCode(row.booth_code) || "-"} | ${row.shop_name} | ${row.phone || "-"} | ${tableCount}ตัว | ${chairCount}ตัว | ${powerDisplay}`,
    );
  }

  if (data.length > 120) {
    lines.push(`... and ${data.length - 120} more rows`);
  }

  lines.push("─".repeat(50));
  lines.push(`รวมโต๊ะ: ${tableTotal} ตัว | รวมเก้าอี้: ${chairTotal} ตัว`);
  if (Object.keys(powerSummary).length) {
    lines.push("สรุปไฟ:");
    for (const [label, count] of Object.entries(powerSummary)) {
      lines.push(`  ${label}: ${count} บูธ`);
    }
  }
  return lines.join("\n").slice(0, 4900);
}

async function commandCancel(text, source) {
  const groupId = getGroupIdFromSource(source);

  const idMatch = text.match(/id\s*=\s*([0-9a-fA-F-]{6,36})/i) ?? text.match(/^\/\S+\s+([0-9a-fA-F-]{6,36})$/i);
  const idOrPrefix = idMatch?.[1]?.trim().toLowerCase();

  const kv = parseKvSegments(text);
  const projectFilter = normalizeSpaces(pickValue(kv, ["project"]));

  let shopName = normalizeSpaces(pickValue(kv, ["shop", "shop_name"]));
  if (!shopName && !idOrPrefix) {
    const tail = normalizeSpaces(text.replace(/^\/\S+\s*/, ""));
    if (tail && !/^id\s*=/i.test(tail)) {
      const eqMatch = tail.match(/^[^=]+\s*=\s*(.+)$/);
      shopName = normalizeSpaces(eqMatch ? eqMatch[1] : tail);
      if (/^project$/i.test(shopName)) shopName = "";
    }
  }

  let query = supabase
    .from("line_booking_records")
    .select("id, project_name, shop_name, phone, booth_code, booking_status, booked_at")
    .eq("group_id", groupId)
    .eq("booking_status", "booked")
    .order("booked_at", { ascending: false })
    .limit(500);

  if (projectFilter) query = query.eq("project_name", projectFilter);

  const { data, error } = await query;
  if (error) {
    console.error(error);
    return "Failed to find booking record";
  }

  const candidates = data ?? [];
  let target = null;

  if (idOrPrefix) {
    target = candidates.find((row) => row.id.toLowerCase().startsWith(idOrPrefix)) ?? null;
    if (!target) return `Booking id not found: ${idOrPrefix}`;
  } else {
    if (!shopName) {
      return "Please provide id or shop name: /cancel id=<id> or /cancel shop=<name>";
    }

    const normalizedShop = normalizeKey(shopName);
    const exact = candidates.filter((row) => normalizeKey(row.shop_name) === normalizedShop);
    const fuzzyPool = exact.length
      ? exact
      : candidates.filter((row) => normalizeKey(row.shop_name).includes(normalizedShop));

    if (!fuzzyPool.length) {
      return `Shop not found in active bookings: ${shopName}`;
    }

    if (fuzzyPool.length > 1) {
      const previews = fuzzyPool.slice(0, 5).map(
        (row) => `- #${shortId(row.id)} ${row.project_name} | ${normalizeBoothCode(row.booth_code) || "-"} | ${row.shop_name}`,
      );
      return [
        `Found multiple bookings for "${shopName}". Please cancel by id:`,
        ...previews,
      ].join("\n");
    }

    target = fuzzyPool[0];
  }

  const { error: updateError } = await supabase
    .from("line_booking_records")
    .update({
      booking_status: "cancelled",
      cancelled_at: new Date().toISOString(),
    })
    .eq("id", target.id);

  if (updateError) {
    console.error(updateError);
    return "Cancel booking failed";
  }

  clearPendingReplacement(source);
  return `Cancelled booking #${shortId(target.id)} (${target.shop_name})`;
}


async function commandExportCsv(text, source) {
  const groupId = getGroupIdFromSource(source);
  const dateStr = parseExportDateFromText(text);

  if (!LINE_PUBLIC_BASE_URL || !LINE_EXPORT_TOKEN) {
    return [
      "Export link is not enabled yet",
      "Please set LINE_PUBLIC_BASE_URL and LINE_EXPORT_TOKEN in .env",
    ].join("\n");
  }

  const { data, error } = await fetchDailyBookings(groupId, dateStr);
  if (error) {
    console.error(error);
    return "Failed to generate daily export link";
  }

  const url = buildExportUrl(groupId, dateStr);
  return [
    `Daily CSV: ${dateStr}`,
    `Rows: ${data.length}`,
    url,
  ].join("\n");
}

async function commandExportInstallCsv(text, source) {
  const groupId = getGroupIdFromSource(source);
  let projectFilter = parseProjectFilter(text);
  if (!projectFilter) {
    projectFilter = await getGroupDefaultProject(groupId);
  }

  if (!LINE_PUBLIC_BASE_URL || !LINE_EXPORT_TOKEN) {
    return [
      "Export link is not enabled yet",
      "Please set LINE_PUBLIC_BASE_URL and LINE_EXPORT_TOKEN in .env",
    ].join("\n");
  }

  if (!projectFilter) {
    return "Please provide project name: /export-install project=<name>";
  }

  const { data, error } = await fetchInstallBookings(groupId, projectFilter);
  if (error) {
    console.error(error);
    return "Failed to generate installer export link";
  }

  const url = buildInstallExportUrl(groupId, projectFilter);
  return [
    `Installer CSV: ${projectFilter}`,
    `Rows: ${data.length}`,
    url,
  ].join("\n");
}


async function commandExportMasterCsv(text, source) {
  const groupId = getGroupIdFromSource(source);
  let projectFilter = parseProjectFilter(text);
  if (!projectFilter) {
    projectFilter = await getGroupDefaultProject(groupId);
  }
  if (!LINE_PUBLIC_BASE_URL || !LINE_EXPORT_TOKEN) {
    return [
      "Export link is not enabled yet",
      "Please set LINE_PUBLIC_BASE_URL and LINE_EXPORT_TOKEN in .env",
    ].join("\n");
  }
  if (!projectFilter) {
    return "Please provide project name: /export-master project=<name> [days=<n>]";
  }
  const daysMatch = text.match(/\bdays\s*=\s*(\d+)/i);
  const days = daysMatch ? Number(daysMatch[1]) : 1;
  const { data, error } = await fetchProjectBookings(groupId, projectFilter);
  if (error) {
    console.error("[export-master] fetchProjectBookings error:", error);
    return "Failed to generate master export link";
  }
  const url = buildMasterExportUrl(groupId, projectFilter, days);
  return [
    `\uD83D\uDCCA Master CSV: ${projectFilter} | ${days} \u0E27\u0E31\u0E19`,
    `\u0E1A\u0E39\u0E18\u0E17\u0E35\u0E48 active: ${data.length} \u0E23\u0E32\u0E22\u0E01\u0E32\u0E23`,
    url,
  ].join("\n");
}
async function fetchLineMessageContent(messageId) {
  if (!messageId) return null;

  const startedAt = Date.now();
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), LINE_FETCH_TIMEOUT_MS);

  console.log(`[image] fetch-line-content:start message=${messageId}`);

  try {
    const response = await fetch(
      `https://api-data.line.me/v2/bot/message/${encodeURIComponent(messageId)}/content`,
      {
        method: "GET",
        headers: {
          Authorization: `Bearer ${LINE_CHANNEL_ACCESS_TOKEN}`,
        },
        signal: controller.signal,
      },
    );

    if (!response.ok) {
      const body = await response.text();
      console.error("Fetch LINE content failed:", messageId, response.status, body);
      return null;
    }

    const buffer = Buffer.from(await response.arrayBuffer());
    console.log(
      `[image] fetch-line-content:done message=${messageId} bytes=${buffer.length} mime=${detectImageMimeType(buffer)} elapsed_ms=${elapsedMs(startedAt)}`,
    );
    return buffer;
  } catch (error) {
    console.error("Fetch LINE content failed:", messageId, error);
    return null;
  } finally {
    clearTimeout(timeout);
  }
}

async function recognizeTextFromImage(buffer, options = {}) {
  if (!LINE_OCR_ENABLED) return "";

  const messageId = options.messageId ?? "-";
  const startedAt = Date.now();
  console.log(`[image] ocr:start message=${messageId} bytes=${buffer?.length ?? 0} lang=${LINE_OCR_LANG}`);

  try {
    const mod = await import("tesseract.js");
    const recognize =
      (typeof mod.recognize === "function" && mod.recognize) ||
      (mod.default && typeof mod.default.recognize === "function" ? mod.default.recognize : null);

    if (!recognize) {
      console.error("tesseract.js recognize() not found");
      return "";
    }

    const result = await runWithTimeout("OCR", LINE_OCR_TIMEOUT_MS, () =>
      recognize(buffer, LINE_OCR_LANG, {}),
    );
    const text = normalizeSpaces(result?.data?.text ?? "");
    console.log(`[image] ocr:done message=${messageId} chars=${text.length} elapsed_ms=${elapsedMs(startedAt)}`);
    return text;
  } catch (error) {
    console.error("OCR failed:", messageId, error);
    return "";
  }
}

function detectImageMimeType(buffer) {
  if (!buffer?.length) return "image/jpeg";
  if (buffer[0] === 0x89 && buffer[1] === 0x50 && buffer[2] === 0x4e && buffer[3] === 0x47) {
    return "image/png";
  }
  if (buffer[0] === 0xff && buffer[1] === 0xd8 && buffer[2] === 0xff) {
    return "image/jpeg";
  }
  if (buffer[0] === 0x47 && buffer[1] === 0x49 && buffer[2] === 0x46) {
    return "image/gif";
  }
  if (
    buffer.length >= 12 &&
    buffer[0] === 0x52 &&
    buffer[1] === 0x49 &&
    buffer[2] === 0x46 &&
    buffer[3] === 0x46 &&
    buffer[8] === 0x57 &&
    buffer[9] === 0x45 &&
    buffer[10] === 0x42 &&
    buffer[11] === 0x50
  ) {
    return "image/webp";
  }
  return "image/jpeg";
}

function extractOpenAIOutputText(payload) {
  if (typeof payload?.output_text === "string" && payload.output_text.trim()) {
    return payload.output_text.trim();
  }

  const parts = [];
  for (const item of payload?.output ?? []) {
    for (const content of item?.content ?? []) {
      const value = content?.text ?? content?.output_text ?? content?.value;
      if (typeof value === "string" && value.trim()) {
        parts.push(value.trim());
      }
    }
  }

  return parts.join("\n").trim();
}

function parseJsonObjectResponse(text) {
  const raw = String(text ?? "").trim();
  if (!raw) return null;

  const cleaned = raw
    .replace(/^```(?:json)?\s*/i, "")
    .replace(/\s*```$/, "")
    .trim();

  try {
    return JSON.parse(cleaned);
  } catch {
    const start = cleaned.indexOf("{");
    const end = cleaned.lastIndexOf("}");
    if (start < 0 || end <= start) return null;
    try {
      return JSON.parse(cleaned.slice(start, end + 1));
    } catch {
      return null;
    }
  }
}

function buildTextAiParsePrompt(text) {
  const preview = normalizeSpaces(text).slice(0, 3000) || "(empty)";
  return [
    "Classify this LINE chat message for a booth booking bot.",
    "Return JSON only.",
    "Never invent missing facts.",
    "Use null for unknown fields.",
    "classification must be one of: booking, expense, unknown.",
    "Choose booking only when the message clearly contains booth booking details (project + shop at minimum).",
    "Choose expense when the message looks like a payment notification or expense record with an amount.",
    "If you cannot confidently classify, use unknown.",
    "Message text:",
    preview,
  ].join("\n");
}

function buildImageAiAnalysisPrompt(ocrText) {
  const ocrPreview = normalizeSpaces(ocrText).slice(0, 3000) || "(no OCR text)";
  return [
    "Classify this LINE image for a Thai booth booking bot.",
    "Return JSON only. Never invent missing facts. Use null for unknown fields.",
    "classification must be exactly one of: booking, expense, create_event, unknown.",
    "",
    "RULE 1 — create_event: Use when the image is a FLOOR PLAN, BOOTH MAP, or EVENT POSTER showing an event name, event dates, and multiple numbered booth squares in a grid/layout. DO NOT classify a floor plan as booking.",
    "If create_event: fill event.projectName, event.startDate (YYYY-MM-DD), event.endDate (YYYY-MM-DD), event.totalBooths (count ALL numbered booth squares), event.venue, event.boothPrice.",
    "",
    "RULE 2 — booking: Use ONLY when image is a single shop booking FORM or CONFIRMATION with shopName + booth assignment. Must have shopName.",
    "",
    "RULE 3 — expense: Use when image is a transfer slip, receipt, or payment proof with an amount.",
    "",
    "RULE 4 — unknown: Use when none of the above apply.",
    "",
    "OCR text:",
    ocrPreview,
  ].join("\n");
}

// Gemini responseJsonSchema accepts only a subset of JSON Schema,
// so numeric-looking fields are returned as strings and normalized later.
const IMAGE_AI_ANALYSIS_SCHEMA = {
  type: "object",
  additionalProperties: false,
  required: ["classification"],
  properties: {
    classification: {
      type: "string",
      enum: ["booking", "expense", "create_event", "unknown"],
    },
    confidence: {
      type: ["number", "null"],
    },
    summary: {
      type: ["string", "null"],
    },
    booking: {
      type: "object",
      additionalProperties: false,
      properties: {
        projectName: { type: ["string", "null"] },
        shopName: { type: ["string", "null"] },
        phone: { type: ["string", "null"] },
        boothCode: { type: ["string", "null"] },
        productType: { type: ["string", "null"] },
        boothPrice: { type: ["string", "null"] },
        tableFreeQty: { type: ["string", "null"] },
        tableExtraQty: { type: ["string", "null"] },
        chairFreeQty: { type: ["string", "null"] },
        chairExtraQty: { type: ["string", "null"] },
        powerAmp: { type: ["string", "null"] },
        powerLabel: { type: ["string", "null"] },
        note: { type: ["string", "null"] },
      },
    },
    expense: {
      type: "object",
      additionalProperties: false,
      properties: {
        projectName: { type: ["string", "null"] },
        amount: { type: ["string", "null"] },
        vendorName: { type: ["string", "null"] },
        expenseType: { type: ["string", "null"] },
        note: { type: ["string", "null"] },
      },
    },
    event: {
      type: "object",
      additionalProperties: false,
      properties: {
        projectName: { type: ["string", "null"] },
        startDate: { type: ["string", "null"] },
        endDate: { type: ["string", "null"] },
        totalBooths: { type: ["string", "null"] },
        boothPrice: { type: ["string", "null"] },
        venue: { type: ["string", "null"] },
      },
    },
  },
};

async function analyzeImageWithOpenAI(buffer, ocrText = "") {
  if (!LINE_AI_IMAGE_FALLBACK_ENABLED || !OPENAI_API_KEY) return null;

  const startedAt = Date.now();
  const mimeType = detectImageMimeType(buffer);
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), LINE_AI_TIMEOUT_MS);

  console.log(
    `[image] ai:openai:start model=${LINE_AI_IMAGE_MODEL} bytes=${buffer?.length ?? 0} mime=${mimeType} ocr_chars=${ocrText.length}`,
  );

  try {
    const response = await fetch("https://api.openai.com/v1/responses", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${OPENAI_API_KEY}`,
      },
      body: JSON.stringify({
        model: LINE_AI_IMAGE_MODEL,
        max_output_tokens: 700,
        text: {
          format: {
            type: "json_object",
          },
        },
        input: [
          {
            role: "user",
            content: [
              {
                type: "input_text",
                text: buildImageAiAnalysisPrompt(ocrText),
              },
              {
                type: "input_image",
                image_url: `data:${mimeType};base64,${buffer.toString("base64")}`,
              },
            ],
          },
        ],
      }),
      signal: controller.signal,
    });

    if (!response.ok) {
      const body = await response.text();
      console.error("OpenAI image analysis failed:", response.status, body);
      return null;
    }

    const payload = await response.json();
    const parsed = parseJsonObjectResponse(extractOpenAIOutputText(payload));
    console.log(`[image] ai:openai:done elapsed_ms=${elapsedMs(startedAt)}`);
    return parsed;
  } catch (error) {
    console.error("OpenAI image analysis failed:", error);
    return null;
  } finally {
    clearTimeout(timeout);
  }
}

function extractGeminiOutputText(payload) {
  const parts = payload?.candidates?.[0]?.content?.parts ?? [];
  return parts
    .map((part) => (typeof part?.text === "string" ? part.text.trim() : ""))
    .filter(Boolean)
    .join("\n")
    .trim();
}

async function analyzeImageWithGemini(buffer, ocrText = "", mimeTypeOverride = null) {
  if (!LINE_AI_IMAGE_FALLBACK_ENABLED || !GEMINI_API_KEY) return null;

  const startedAt = Date.now();
  const mimeType = mimeTypeOverride ?? detectImageMimeType(buffer);
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), LINE_AI_TIMEOUT_MS);

  console.log(
    `[image] ai:gemini:start model=${LINE_AI_IMAGE_MODEL} bytes=${buffer?.length ?? 0} mime=${mimeType} ocr_chars=${ocrText.length}`,
  );

  try {
    const endpoint = `https://generativelanguage.googleapis.com/v1beta/models/${encodeURIComponent(LINE_AI_IMAGE_MODEL)}:generateContent`;
    const response = await fetch(endpoint, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "x-goog-api-key": GEMINI_API_KEY,
      },
      body: JSON.stringify({
        contents: [
          {
            parts: [
              {
                text: buildImageAiAnalysisPrompt(ocrText),
              },
              {
                inline_data: {
                  mime_type: mimeType,
                  data: buffer.toString("base64"),
                },
              },
            ],
          },
        ],
        generationConfig: {
          temperature: 0.1,
          responseMimeType: "application/json",
          responseJsonSchema: IMAGE_AI_ANALYSIS_SCHEMA,
        },
      }),
      signal: controller.signal,
    });

    if (!response.ok) {
      const body = await response.text();
      console.error("Gemini image analysis failed:", response.status, body);
      return null;
    }

    const payload = await response.json();
    const parsed = parseJsonObjectResponse(extractGeminiOutputText(payload));
    console.log(`[image] ai:gemini:done elapsed_ms=${elapsedMs(startedAt)}`);
    return parsed;
  } catch (error) {
    console.error("Gemini image analysis failed:", error);
    return null;
  } finally {
    clearTimeout(timeout);
  }
}

// ── Nova AI provider (optional, falls back to Gemini/OpenAI if unavailable) ──
async function analyzeImageWithNova(buffer, ocrText = "", lineEvent = null) {
  if (!NOVA_ENABLED) return null;
  const startedAt = Date.now();
  const mimeType = detectImageMimeType(buffer);
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), LINE_AI_TIMEOUT_MS);
  console.log(`[image] ai:nova:start bytes=${buffer?.length ?? 0} mime=${mimeType} ocr_chars=${ocrText.length}`);
  try {
    const payload = {
      secret: NOVA_SECRET_KEY || undefined,
      line_event: lineEvent ?? undefined,
      ocr_text: ocrText || null,
      image_base64: buffer.toString("base64"),
      image_mime_type: mimeType,
    };
    const response = await fetch(NOVA_API_URL, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
      signal: controller.signal,
    });
    if (!response.ok) {
      console.error(`[image] ai:nova:error status=${response.status}`);
      return null;
    }
    const data = await response.json();
    console.log(`[image] ai:nova:done elapsed_ms=${Date.now() - startedAt} status=${data?.status} action=${data?.action ?? "-"}`);
    if (data?.status !== "ok") return null;
    if (data?.action === "create_event") return { _raw: data, action: "create_event" };
    return adaptNovaResponse(data);
  } catch (err) {
    console.error(`[image] ai:nova:failed elapsed_ms=${Date.now() - startedAt}`, err?.message ?? err);
    return null;
  } finally {
    clearTimeout(timeout);
  }
}

async function callTextParseWithNova(text, lineEvent = null) {
  if (!NOVA_ENABLED) return null;
  const startedAt = Date.now();
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), LINE_AI_TIMEOUT_MS);
  console.log(`[text] ai:nova:start chars=${text.length}`);
  try {
    const payload = {
      secret: NOVA_SECRET_KEY || undefined,
      line_event: lineEvent ?? undefined,
      text,
    };
    const response = await fetch(NOVA_API_URL, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
      signal: controller.signal,
    });
    if (!response.ok) {
      console.error(`[text] ai:nova:error status=${response.status}`);
      return null;
    }
    const data = await response.json();
    console.log(`[text] ai:nova:done elapsed_ms=${Date.now() - startedAt} status=${data?.status} action=${data?.action ?? "-"}`);
    if (data?.status !== "ok") return null;
    // Pass create_event raw so handleTextMessage can process it
    if (data?.action === "create_event") return { _raw: data, action: "create_event" };
    return adaptNovaResponse(data);
  } catch (err) {
    console.error(`[text] ai:nova:failed elapsed_ms=${Date.now() - startedAt}`, err?.message ?? err);
    return null;
  } finally {
    clearTimeout(timeout);
  }
}

function adaptNovaResponse(data) {
  // Nova returns: { status, replyText, structuredData: { type, booking, expense }, action }
  // Also supports legacy format: { status, replyText, bookingData, expenseData }
  const sd = data?.structuredData;
  const bookingRaw = sd?.booking ?? data?.bookingData;
  const expenseRaw = sd?.expense ?? data?.expenseData;
  const type = sd?.type ?? (bookingRaw ? "booking" : expenseRaw ? "expense" : "unknown");

  if (type === "booking" && bookingRaw) {
    return {
      classification: "booking",
      confidence: null,
      summary: data.replyText ?? null,
      booking: {
        projectName: bookingRaw.projectName ?? null,
        shopName: bookingRaw.shopName ?? null,
        phone: bookingRaw.phone ?? null,
        boothCode: bookingRaw.boothCode ?? null,
        productType: bookingRaw.productType ?? null,
        boothPrice: bookingRaw.boothPrice != null ? String(bookingRaw.boothPrice) : null,
        tableFreeQty: bookingRaw.tableFreeQty != null ? String(bookingRaw.tableFreeQty) : null,
        tableExtraQty: bookingRaw.tableExtraQty != null ? String(bookingRaw.tableExtraQty) : null,
        chairFreeQty: bookingRaw.chairFreeQty != null ? String(bookingRaw.chairFreeQty) : null,
        chairExtraQty: bookingRaw.chairExtraQty != null ? String(bookingRaw.chairExtraQty) : null,
        powerAmp: bookingRaw.powerAmp != null ? String(bookingRaw.powerAmp) : null,
        powerLabel: bookingRaw.powerLabel ?? null,
        note: bookingRaw.note ?? null,
      },
    };
  }
  if (type === "expense" && expenseRaw) {
    return {
      classification: "expense",
      confidence: null,
      summary: data.replyText ?? null,
      expense: {
        projectName: expenseRaw.projectName ?? null,
        amount: expenseRaw.amount != null ? String(expenseRaw.amount) : null,
        vendorName: expenseRaw.vendorName ?? null,
        expenseType: expenseRaw.expenseType ?? null,
        note: expenseRaw.note ?? null,
      },
    };
  }
  return { classification: "unknown", confidence: null, summary: data.replyText ?? null };
}

async function analyzeImageWithAi(buffer, ocrText = "", lineEvent = null) {
  // Try Nova first if configured
  if (NOVA_ENABLED) {
    const novaResult = await analyzeImageWithNova(buffer, ocrText, lineEvent);
    if (novaResult) return novaResult;
    console.log("[image] ai:nova:fallback — switching to primary AI provider");
  }
  if (LINE_AI_PROVIDER === "gemini") {
    return analyzeImageWithGemini(buffer, ocrText);
  }
  return analyzeImageWithOpenAI(buffer, ocrText);
}

async function callTextParseWithOpenAI(text) {
  if (!LINE_AI_TEXT_FALLBACK_ENABLED || !OPENAI_API_KEY) return null;

  const startedAt = Date.now();
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), LINE_AI_TIMEOUT_MS);

  console.log(`[text] ai:openai:start model=${LINE_AI_TEXT_MODEL} chars=${text.length}`);

  try {
    const response = await fetch("https://api.openai.com/v1/responses", {
      method: "POST",
      headers: { "Content-Type": "application/json", Authorization: `Bearer ${OPENAI_API_KEY}` },
      body: JSON.stringify({
        model: LINE_AI_TEXT_MODEL,
        max_output_tokens: 700,
        text: { format: { type: "json_object" } },
        input: [{ role: "user", content: [{ type: "input_text", text: buildTextAiParsePrompt(text) }] }],
      }),
      signal: controller.signal,
    });

    if (!response.ok) {
      const body = await response.text();
      console.error("OpenAI text parse failed:", response.status, body);
      return null;
    }

    const payload = await response.json();
    const parsed = parseJsonObjectResponse(extractOpenAIOutputText(payload));
    console.log(`[text] ai:openai:done elapsed_ms=${elapsedMs(startedAt)}`);
    return parsed;
  } catch (error) {
    console.error("OpenAI text parse failed:", error);
    return null;
  } finally {
    clearTimeout(timeout);
  }
}

async function callTextParseWithGemini(text) {
  if (!LINE_AI_TEXT_FALLBACK_ENABLED || !GEMINI_API_KEY) return null;

  const startedAt = Date.now();
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), LINE_AI_TIMEOUT_MS);

  console.log(`[text] ai:gemini:start model=${LINE_AI_TEXT_MODEL} chars=${text.length}`);

  try {
    const endpoint = `https://generativelanguage.googleapis.com/v1beta/models/${encodeURIComponent(LINE_AI_TEXT_MODEL)}:generateContent`;
    const response = await fetch(endpoint, {
      method: "POST",
      headers: { "Content-Type": "application/json", "x-goog-api-key": GEMINI_API_KEY },
      body: JSON.stringify({
        contents: [{ parts: [{ text: buildTextAiParsePrompt(text) }] }],
        generationConfig: {
          temperature: 0.1,
          responseMimeType: "application/json",
          responseJsonSchema: IMAGE_AI_ANALYSIS_SCHEMA,
        },
      }),
      signal: controller.signal,
    });

    if (!response.ok) {
      const body = await response.text();
      console.error("Gemini text parse failed:", response.status, body);
      return null;
    }

    const payload = await response.json();
    const parsed = parseJsonObjectResponse(extractGeminiOutputText(payload));
    console.log(`[text] ai:gemini:done elapsed_ms=${elapsedMs(startedAt)}`);
    return parsed;
  } catch (error) {
    console.error("Gemini text parse failed:", error);
    return null;
  } finally {
    clearTimeout(timeout);
  }
}

async function callNovaForExcel(buffer, fileName, projectNames = []) {
  if (!NOVA_ENABLED || !NOVA_BASE_URL) return null;
  const boundary = `NovaBoundary${Date.now()}`;
  const CRLF = "\r\n";
  const secretPart = Buffer.from(
    `--${boundary}${CRLF}Content-Disposition: form-data; name="secret"${CRLF}${CRLF}${NOVA_SECRET_KEY}`
  );
  const projectsPart = Buffer.from(
    `${CRLF}--${boundary}${CRLF}Content-Disposition: form-data; name="project_names"${CRLF}${CRLF}${JSON.stringify(projectNames)}`
  );
  const filePart = Buffer.from(
    `${CRLF}--${boundary}${CRLF}` +
    `Content-Disposition: form-data; name="file"; filename="${fileName.replace(/"/g, "_")}"${CRLF}` +
    `Content-Type: application/octet-stream${CRLF}${CRLF}`
  );
  const closing = Buffer.from(`${CRLF}--${boundary}--${CRLF}`);
  const body = Buffer.concat([secretPart, projectsPart, filePart, buffer, closing]);
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 150_000);
  try {
    console.log(`[file] nova:excel:start file="${fileName}" bytes=${buffer.length}`);
    const res = await fetch(`${NOVA_BASE_URL}/nova_process_excel`, {
      method: "POST",
      headers: {
        "Content-Type": `multipart/form-data; boundary=${boundary}`,
        "Content-Length": String(body.length),
      },
      body,
      signal: controller.signal,
    });
    if (!res.ok) { console.warn(`[file] nova:excel HTTP ${res.status}`); return null; }
    const json = await res.json();
  if (json.status === "ok" && Array.isArray(json.rows)) {
      console.log(`[file] nova:excel:done rows=${json.rows.length}`);
      return json.rows;
    }
  } catch (err) {
    console.warn("[file] nova:excel error:", err?.message ?? err);
  } finally {
    clearTimeout(timeout);
  }
  return null;
}

async function callAIForTextParse(text, lineEvent = null) {
  // Try Nova first if configured
  if (NOVA_ENABLED) {
    const novaResult = await callTextParseWithNova(text, lineEvent);
    if (novaResult && novaResult.classification !== "unknown") return novaResult;
    console.log("[text] ai:nova:fallback — switching to primary AI provider");
  }
  if (LINE_AI_PROVIDER === "gemini") {
    return callTextParseWithGemini(text);
  }
  return callTextParseWithOpenAI(text);
}

function normalizeAiClassification(value) {
  const key = normalizeKey(value);
  if (key.includes("create_event") || key.includes("createevent")) return "create_event";
  if (key.includes("booking")) return "booking";
  if (key.includes("expense")) return "expense";
  return "unknown";
}

function inferAiClassification(analysis, ocrText = "") {
  const explicit = normalizeAiClassification(analysis?.classification);
  if (explicit !== "unknown") return explicit;
  // Nova create_event action
  if (analysis?.action === "create_event") return "create_event";

  const expenseAmount = normalizeAmount(analysis?.expense?.amount);
  if (expenseAmount !== null) return "expense";

  const booking = analysis?.booking ?? {};
  const phones = extractPhones(booking.phone ?? "");
  if (!phones.length && ocrText) phones.push(...findPhonesFromText(ocrText));
  const phone = pickPrimaryPhone(phones);
  if (normalizeSpaces(booking.shopName)) {
    return "booking";
  }

  return "unknown";
}

function buildBookingFromAiAnalysis(analysis, ocrText = "") {
  const booking = analysis?.booking ?? {};
  const equipment = parseEquipmentFields({
    tableFree: booking.tableFreeQty,
    tableExtra: booking.tableExtraQty,
    chairFree: booking.chairFreeQty,
    chairExtra: booking.chairExtraQty,
    electricity: booking.powerLabel ?? booking.powerAmp,
  });
  const phones = extractPhones(booking.phone ?? "");
  if (!phones.length && ocrText) phones.push(...findPhonesFromText(ocrText));

  const noteParts = ["source=image_ai"];
  if (analysis?.summary) noteParts.push(`ai_summary=${normalizeSpaces(analysis.summary).slice(0, 240)}`);
  if (booking.note) noteParts.push(`ai_note=${normalizeSpaces(booking.note).slice(0, 240)}`);
  if (ocrText) noteParts.push(`ocr_preview=${normalizeSpaces(ocrText).slice(0, 240)}`);

  return {
    projectName: normalizeSpaces(booking.projectName),
    shopName: normalizeSpaces(booking.shopName),
    phone: pickPrimaryPhone(phones),
    boothCode: normalizeBoothCode(booking.boothCode || findBoothFromText(ocrText)),
    productType: normalizeSpaces(booking.productType),
    boothPrice: normalizeAmount(booking.boothPrice),
    tableFreeQty: equipment.tableFreeQty,
    tableExtraQty: equipment.tableExtraQty,
    chairFreeQty: equipment.chairFreeQty,
    chairExtraQty: equipment.chairExtraQty,
    powerAmp: equipment.powerAmp,
    powerLabel: equipment.powerLabel,
    note: normalizeSpaces(noteParts.join(" | ")).slice(0, 1800),
  };
}

function buildExpenseFromAiAnalysis(analysis, ocrText = "") {
  const expense = analysis?.expense ?? {};
  const noteParts = ["source=image_ai"];
  if (analysis?.summary) noteParts.push(`ai_summary=${normalizeSpaces(analysis.summary).slice(0, 240)}`);
  if (expense.note) noteParts.push(`ai_note=${normalizeSpaces(expense.note).slice(0, 240)}`);
  if (ocrText) noteParts.push(`ocr_preview=${normalizeSpaces(ocrText).slice(0, 240)}`);

  return {
    projectName: normalizeSpaces(expense.projectName),
    amount: normalizeAmount(expense.amount),
    currency: "THB",
    vendorName: normalizeSpaces(expense.vendorName),
    expenseType: normalizeSpaces(expense.expenseType),
    note: normalizeSpaces(noteParts.join(" | ")).slice(0, 1800),
  };
}

async function commandBookingFromImage(event) {
  if (!LINE_OCR_ENABLED && !LINE_AI_IMAGE_FALLBACK_ENABLED) {
    return {
      replyText: "Image reading is disabled. Please send booking or expense as text.",
      digestEvent: buildImageDigestEvent(event, { category: "failure", status: "needs_review", reason: "image_reading_disabled" }),
    };
  }

  const startedAt = Date.now();
  const source = event.source ?? {};
  const groupId = getGroupIdFromSource(source);
  const messageId = event.message?.id ?? "-";
  const expenseAllowed = LINE_EXPENSE_GROUP_IDS.length === 0 || LINE_EXPENSE_GROUP_IDS.includes(groupId);
  console.log(`[image] pipeline:start message=${messageId} group=${groupId || "-"} expenseAllowed=${expenseAllowed}`);

  const imageBuffer = await fetchLineMessageContent(messageId);
  if (!imageBuffer) {
    console.log(`[image] pipeline:stop message=${messageId} reason=fetch_failed elapsed_ms=${elapsedMs(startedAt)}`);
    return {
      digestEvent: buildImageDigestEvent(event, { category: "failure", status: "needs_review", reason: "fetch_failed" }),
    };
  }

  const ocrText = LINE_OCR_ENABLED ? await recognizeTextFromImage(imageBuffer, { messageId }) : "";

  if (ocrText && looksLikeBookingForm(ocrText)) {
    const parsed = await parseBookingFormText(ocrText, groupId);
    if (hasRequiredBookingFields(parsed)) {
      parsed.note = normalizeSpaces(`source=image_ocr${parsed.note ? ` | ${parsed.note}` : ""}`).slice(0, 1800);
      const result = await saveBookingWithAgentRules(parsed, source, messageId);
      console.log(`[image] pipeline:ocr-booking message=${messageId} ok=${result.ok} elapsed_ms=${elapsedMs(startedAt)}`);
      if (result.silent) return null;
      if (!result.ok) {
        return {
          replyText: result.message,
          forceImmediateReply: Boolean(result.needsConfirmation),
          digestEvent: buildImageDigestEvent(event, {
            category: "booking", status: "needs_review", reason: result.needsConfirmation ? "needs_confirmation" : "save_failed",
            sourceTag: "image_ocr", projectName: parsed.projectName, shopName: parsed.shopName, boothCode: parsed.boothCode, detail: result.message,
          }),
        };
      }
      return {
        replyText: `${result.message}
(source: image OCR)`,
        digestEvent: buildImageDigestEvent(event, { category: "booking", status: "saved", sourceTag: "image_ocr", projectName: parsed.projectName, shopName: parsed.shopName, boothCode: parsed.boothCode }),
      };
    }
  }

  if (ocrText && looksLikeExpenseText(ocrText) && expenseAllowed) {
    console.log(`[image] pipeline:ocr-expense message=${messageId} elapsed_ms=${elapsedMs(startedAt)}`);
    const parsed = parseExpenseCommand(ocrText);
    parsed.note = normalizeSpaces(`image_ocr${parsed.note ? ` | ${parsed.note}` : ""}`).slice(0, 1800);
    const result = await saveExpenseWithProjectPrompt(parsed, source, messageId);
    if (!result.ok) {
      return {
        replyText: result.message,
        digestEvent: buildImageDigestEvent(event, { category: "expense", status: "needs_review", reason: "save_failed", sourceTag: "image_ocr", projectName: parsed.projectName, vendorName: parsed.vendorName, amount: parsed.amount, currency: parsed.currency, detail: result.message }),
      };
    }
    return {
      replyText: `${result.message}
(source: image OCR)`,
      digestEvent: buildImageDigestEvent(event, { category: "expense", status: "saved", sourceTag: "image_ocr", projectName: parsed.projectName || DEFAULT_EXPENSE_PROJECT_NAME, vendorName: parsed.vendorName, amount: parsed.amount, currency: parsed.currency }),
    };
  }

  const aiAnalysis = await analyzeImageWithAi(imageBuffer, ocrText, event);

  // Handle create_event action — from Nova (_raw.structuredData.event) or Gemini (analysis.event)
  const isCreateEvent = aiAnalysis?.action === "create_event" || inferAiClassification(aiAnalysis, ocrText) === "create_event";
  if (isCreateEvent) {
    const ev = aiAnalysis._raw?.structuredData?.event ?? aiAnalysis?.event ?? {};
    const gid = getGroupIdFromSource(source);
    if (ev.projectName && gid) {
      const totalBooths = (ev.totalBooths && Number(ev.totalBooths) > 0) ? Number(ev.totalBooths) : null;
      const venue = normalizeSpaces(ev.location ?? ev.venue ?? "") || null;
      const { error } = await supabase.from("line_project_pricing").upsert({
        group_id: gid,
        project_name: normalizeSpaces(ev.projectName),
        event_start_date: ev.startDate ?? null,
        event_end_date: ev.endDate ?? null,
        booth_price: ev.boothPrice ? Number(ev.boothPrice) : 0,
        total_booths: totalBooths,
        updated_at: new Date().toISOString(),
      }, { onConflict: "group_id,project_name" });
      if (!error) {
        const boothLine = totalBooths ? `\n🏪 ${totalBooths} บูธ` : "";
        const venueLine = venue ? `\n📍 ${venue}` : "";
        console.log(`[image] pipeline:create_event project="${ev.projectName}" start=${ev.startDate} end=${ev.endDate} total_booths=${totalBooths}`);
        return {
          replyText: `✅ บันทึกงาน: ${ev.projectName}\n📅 ${ev.startDate ?? "?"} → ${ev.endDate ?? "?"}${venueLine}${boothLine}`,
          forceImmediateReply: true,
        };
      }
    }
    return { replyText: null };
  }

  const classification = inferAiClassification(aiAnalysis, ocrText);
  console.log(`[image] pipeline:ai-classification message=${messageId} classification=${classification} elapsed_ms=${elapsedMs(startedAt)}`);

  if (classification === "booking") {
    const parsed = buildBookingFromAiAnalysis(aiAnalysis, ocrText);
    // If no projectName, resolve from active projects or default
    if (!parsed.projectName) {
      const groupId = getGroupIdFromSource(source);
      let activeProjects = [];
      if (groupId) {
        const todayStr = getTimePartsInTz(new Date()).dateStr;
        const { data } = await supabase
          .from("line_project_pricing")
          .select("project_name")
          .eq("group_id", groupId)
          .gte("event_end_date", todayStr)
          .order("event_start_date", { ascending: true });
        activeProjects = (data ?? []).map((p) => p.project_name).filter(Boolean);
      }
      if (activeProjects.length === 1) {
        parsed.projectName = activeProjects[0];
      } else if (activeProjects.length > 1) {
        setPendingProjectSelection(source, { parsed, messageId });
        insertPendingProjectRecord(parsed, source, messageId).catch(console.error);
        const projectList = activeProjects.map((p, i) => `${i + 1}. ${p}`).join("\n");
        return {
          replyText: `📋 ข้อมูลร้าน: ${parsed.shopName || "?"}${parsed.boothCode ? ` บูธ ${parsed.boothCode}` : ""}\nกรุณาพิมพ์ชื่อโปรเจกต์ที่ต้องการจอง:\n${projectList}`,
          forceImmediateReply: true,
          digestEvent: buildImageDigestEvent(event, { category: "booking", status: "needs_review", reason: "needs_project_selection", sourceTag: "image_ai", shopName: parsed.shopName, boothCode: parsed.boothCode }),
        };
      } else {
        // No active projects found — fall back to group default project
        const groupId2 = getGroupIdFromSource(source);
        if (groupId2) {
          const def = await getGroupDefaultProject(groupId2);
          if (def) parsed.projectName = def;
        }
      }
    }
    if (hasRequiredBookingFields(parsed)) {
      const result = await saveBookingWithAgentRules(parsed, source, messageId);
      console.log(`[image] pipeline:ai-booking message=${messageId} ok=${result.ok} elapsed_ms=${elapsedMs(startedAt)}`);
      if (result.silent) return null;
      if (!result.ok) {
        return {
          replyText: result.message,
          forceImmediateReply: Boolean(result.needsConfirmation),
          digestEvent: buildImageDigestEvent(event, { category: "booking", status: "needs_review", reason: result.needsConfirmation ? "needs_confirmation" : "save_failed", sourceTag: "image_ai", projectName: parsed.projectName, shopName: parsed.shopName, boothCode: parsed.boothCode, detail: result.message }),
        };
      }
      return {
        replyText: `${result.message}
(source: image AI)`,
        digestEvent: buildImageDigestEvent(event, { category: "booking", status: "saved", sourceTag: "image_ai", projectName: parsed.projectName, shopName: parsed.shopName, boothCode: parsed.boothCode }),
      };
    }
  }

  if (classification === "expense" && expenseAllowed) {
    const parsed = buildExpenseFromAiAnalysis(aiAnalysis, ocrText);
    if (hasRequiredExpenseFields(parsed)) {
      const result = await saveExpenseWithProjectPrompt(parsed, source, messageId);
      console.log(`[image] pipeline:ai-expense message=${messageId} ok=${result.ok} elapsed_ms=${elapsedMs(startedAt)}`);
      if (!result.ok) {
        return {
          replyText: result.message,
          digestEvent: buildImageDigestEvent(event, { category: "expense", status: "needs_review", reason: "save_failed", sourceTag: "image_ai", projectName: parsed.projectName, vendorName: parsed.vendorName, amount: parsed.amount, currency: parsed.currency, detail: result.message }),
        };
      }
      return {
        replyText: `${result.message}
(source: image AI)`,
        digestEvent: buildImageDigestEvent(event, { category: "expense", status: "saved", sourceTag: "image_ai", projectName: parsed.projectName || DEFAULT_EXPENSE_PROJECT_NAME, vendorName: parsed.vendorName, amount: parsed.amount, currency: parsed.currency }),
      };
    }
  }

  if (!ocrText && !LINE_AI_IMAGE_FALLBACK_ENABLED) {
    console.log(`[image] pipeline:stop message=${messageId} reason=ocr_failed elapsed_ms=${elapsedMs(startedAt)}`);
    return {
      digestEvent: buildImageDigestEvent(event, { category: "failure", status: "needs_review", reason: "ocr_failed" }),
    };
  }

  console.log(`[image] pipeline:stop message=${messageId} reason=unclassified elapsed_ms=${elapsedMs(startedAt)}`);
  return {
    digestEvent: buildImageDigestEvent(event, { category: "failure", status: "needs_review", reason: "unclassified", detail: `classification=${classification}` }),
  };
}
const FLOWACCOUNT_URL_REGEX =
  /https?:\/\/(?:app\.)?flowaccount\.com\/(?:share|invoice|document)\/[A-Za-z0-9_\-]+/i;

async function parseFlowaccountHtml(html) {
  try {
    // Extract amount — e.g. "1,234.00" or "1234.00" near "Total" / "รวมทั้งสิ้น"
    const amountMatch =
      html.match(/(?:Grand\s*Total|Total\s*Amount|รวมทั้งสิ้น)[^0-9]*([0-9]{1,3}(?:,[0-9]{3})*(?:\.[0-9]+)?)/i) ||
      html.match(/(?:Amount|จำนวนเงิน)[^0-9]*([0-9]{1,3}(?:,[0-9]{3})*(?:\.[0-9]+)?)/i);
    const amount = amountMatch
      ? parseFloat(amountMatch[1].replace(/,/g, ""))
      : null;

    // Extract vendor / seller name
    const vendorMatch =
      html.match(/(?:From|จาก|ผู้ขาย|บริษัท|Company)[:\s]*([^\n<]{2,80})/i) ||
      html.match(/<title>([^<]{2,80})<\/title>/i);
    const vendorName = vendorMatch
      ? vendorMatch[1].replace(/<[^>]+>/g, "").trim().slice(0, 120)
      : null;

    // Extract document number
    const docMatch = html.match(/(?:INV|EXP|REC|เลขที่)[\/\-\s#]*([A-Za-z0-9\-\/]+)/i);
    const docNumber = docMatch ? docMatch[1].trim().slice(0, 40) : null;

    // Extract date (yyyy-mm-dd or dd/mm/yyyy or Thai date patterns)
    const dateMatch =
      html.match(/(\d{4}-\d{2}-\d{2})/) ||
      html.match(/(\d{2}\/\d{2}\/\d{4})/);
    const docDate = dateMatch ? dateMatch[1] : null;

    return { amount, vendorName, docNumber, docDate };
  } catch {
    return { amount: null, vendorName: null, docNumber: null, docDate: null };
  }
}

async function fetchFlowaccountInvoice(url) {
  try {
    const ctrl = new AbortController();
    const timer = setTimeout(() => ctrl.abort(), 10000);
    const res = await fetch(url, {
      signal: ctrl.signal,
      headers: { "User-Agent": "Mozilla/5.0 (compatible; LineBot/1.0)" },
    });
    clearTimeout(timer);
    if (!res.ok) return null;
    const html = await res.text();
    return parseFlowaccountHtml(html);
  } catch (err) {
    console.warn("[flowaccount] fetch error:", err?.message ?? err);
    return null;
  }
}
async function handleTextMessage(event) {
  const rawText = String(event.message?.text ?? "").trim();
  if (!rawText) return null;
  console.log(`[text] received chars=${rawText.length} preview=${rawText.slice(0, 60).replace(/\n/g, "↵")}`);

  const source = event.source ?? {};
  const normalized = normalizeSpaces(rawText);
  const lowered = normalized.toLowerCase();

  const thaiHelp = "/ช่วยเหลือ";
  const thaiBind = "/ผูกโปรเจกต์";
  const thaiBook = "/จอง";
  const thaiList = "/รายการ";
  const thaiShopList = "/ลิสร้าน";
  const thaiProjectShopList = "/ลิสงาน";
  const thaiSummary = "/สรุป";
  const thaiSalesSummary = "/ยอดขาย";
  const thaiSetPrice = "/ตั้งราคาบูธ";
  const thaiExpense = "/จ่าย";
  const thaiExpenseSummary = "/สรุปค่าใช้จ่าย";
  const thaiInstall = "/ติดตั้ง";
  const thaiCancel = "/ยกเลิก";
  const thaiCancelExpense = "/ยกเลิกค่าใช้จ่าย";
  const thaiExport = "/ส่งออก";
  const thaiExportInstall = "/ส่งออกติดตั้ง";
  const thaiExportExpense = "/ส่งออกค่าใช้จ่าย";
  const thaiExportMaster = "/ส่งออก-master";
  const thaiConfirmReplace = "/ยืนยันแทนที่";
  const thaiYes = "ใช่";

  if (normalized === "/help" || normalized === thaiHelp) return helpText;
  if (/^\/agent(?:\s|$)/i.test(normalized)) return commandAgentStatus();

  if (normalized === "/groupid") {
    const gid = source.groupId ?? source.roomId ?? null;
    if (!gid) return "คำสั่งนี้ใช้ได้เฉพาะใน Group หรือ Room เท่านั้น";
    const inWhitelist = LINE_EXPENSE_GROUP_IDS.length === 0 || LINE_EXPENSE_GROUP_IDS.includes(gid);
    return `Group ID: ${gid}\nสถานะ expense: ${inWhitelist ? "✅ บันทึกได้" : "🚫 ไม่บันทึก (ไม่อยู่ใน whitelist)"}`;
  }

  if (/^\/confirm-replace(?:\s|$)/i.test(normalized) || /^\/ยืนยันแทนที่(?:\s|$)/.test(normalized)) {
    const idMatch = normalized.match(/id\s*=\s*([0-9a-fA-F-]{6,36})/i);
    if (idMatch) return commandConfirmReplaceById(idMatch[1].trim(), source);
    return commandConfirmReplace(source);
  }

  if ((lowered === "yes" || lowered === "y" || normalized === thaiYes) && getPendingReplacement(source)) {
    return commandConfirmReplace(source);
  }

  // Handle pending project selection (after image AI detected booking but no project name)
  const pendingProjSel = getPendingProjectSelection(source);
  if (pendingProjSel) {
    const typed = normalizeSpaces(rawText).trim();
    if (typed) {
      clearPendingProjectSelection(source);
      deletePendingDbRecords(getGroupIdFromSource(source), "needs_project").catch(console.error);
      const updatedParsed = { ...pendingProjSel.parsed, projectName: typed };
      const result = await saveBookingWithAgentRules(updatedParsed, source, pendingProjSel.messageId);
      if (result.silent) return null;
      return result.message;
    }
  }

  if (normalized.startsWith(thaiBind) || /^\/bind(?:-project)?(?:\s|$)/i.test(normalized)) {
    return commandSetProject(normalized, source);
  }

  if (normalized.startsWith(thaiSetPrice) || /^\/set-price(?:\s|$)/i.test(normalized)) {
    return commandSetBoothPrice(normalized, source);
  }

  if (/^\/set-event(?:\s|$)/i.test(normalized) || /^\/ตั้งวันงาน(?:\s|$)/.test(normalized)) {
    return commandSetEvent(normalized, source);
  }

  if (/^\/สรุป(?:\s|$)/.test(normalized) || /^\/report(?:\s|$)/i.test(normalized)) {
    const { dateStr, hour } = getTimePartsInTz(new Date());
    // In 1:1 chat there is no groupId — pass null to get all-group summary
    const groupId = source?.groupId ?? source?.roomId ?? null;
    const pushTarget = source?.groupId ?? source?.roomId ?? source?.userId;
    const projectFilter = normalized.replace(/^\/สรุป\s*/i, "").replace(/^\/report\s*/i, "").trim();
    const msgs = await buildBookingDigestMessage({ dateStr, hour }, groupId, [], projectFilter);
    if (msgs?.length) {
      await replyMessage(event.replyToken, msgs.slice(0, 5));
      for (let i = 5; i < msgs.length; i += 5) await pushMessage(pushTarget, msgs.slice(i, i + 5));
    } else {
      return projectFilter ? `ไม่พบโปรเจกต์ "${projectFilter}"` : "ยังไม่มีข้อมูลการจองวันนี้";
    }
    return null;
  }

  if (normalized.startsWith(thaiBook) || /^\/book(?:\s|$)/i.test(normalized)) {
    return commandBooking(rawText, source, event.message?.id);
  }

  if (normalized.startsWith(thaiList) || /^\/list(?:\s|$)/i.test(normalized)) {
    return commandList(normalized, source);
  }

  if (/^\/review(?:\s|$)/i.test(normalized)) {
    if (/^\/review\s+fix\s+/i.test(normalized)) return commandReviewFix(normalized, source);
    return commandReview(source);
  }

  const sendChunked = async (result) => {
    if (!Array.isArray(result)) return result;
    await replyMessage(event.replyToken, [{ type: "text", text: result[0] }]);
    const pushTarget = source?.groupId ?? source?.roomId ?? source?.userId;
    for (let i = 1; i < result.length; i++) await pushMessage(pushTarget, [{ type: "text", text: result[i] }]);
    return null;
  };

  if (normalized.startsWith(thaiProjectShopList) || /^\/project-shops?(?:\s|$)/i.test(normalized)) {
    return sendChunked(await commandProjectShopList(normalized, source));
  }

  if (normalized.startsWith(thaiShopList) || /^\/shops?(?:\s|$)/i.test(normalized)) {
    return sendChunked(await commandShopList(normalized, source));
  }

  if (normalized.startsWith(thaiSummary) || /^\/(summary|sum)(?:\s|$)/i.test(normalized)) {
    return commandSummary(normalized, source);
  }

  if (normalized.startsWith(thaiSalesSummary) || /^\/sales-summary(?:\s|$)/i.test(normalized)) {
    return commandSalesSummary(normalized, source);
  }

  if (normalized.startsWith(thaiExpenseSummary) || /^\/expense-summary(?:\s|$)/i.test(normalized)) {
    return commandExpenseSummary(normalized, source);
  }

  if (normalized.startsWith(thaiExpense) || /^\/(expense|pay)(?:\s|$)/i.test(normalized)) {
    return commandExpense(rawText, source, event.message?.id);
  }

  if (normalized.startsWith(thaiInstall) || /^\/install(?:\s|$)/i.test(normalized)) {
    return commandInstallList(normalized, source);
  }

  if (normalized.startsWith(thaiCancelExpense) || /^\/cancel-expense(?:\s|$)/i.test(normalized)) {
    return commandCancelExpense(source);
  }

  if (normalized.startsWith(thaiCancel) || /^\/cancel(?:\s|$)/i.test(normalized)) {
    return commandCancel(normalized, source);
  }

  if (normalized.startsWith(thaiExportInstall) || /^\/export-install(?:\s|$)/i.test(normalized)) {
    return commandExportInstallCsv(normalized, source);
  }

  if (normalized.startsWith(thaiExportExpense) || /^\/export-expense(?:\s|$)/i.test(normalized)) {
    return commandExportExpenseCsv(normalized, source);
  }

  if (normalized.startsWith(thaiExportMaster) || /^\/export-master(?:\s|$)/i.test(normalized)) {
    return commandExportMasterCsv(normalized, source);
  }
  if (normalized.startsWith(thaiExport) || /^\/export(?:\s|$)/i.test(normalized)) {
    return commandExportCsv(normalized, source);
  }

  if (!normalized.startsWith("/") && /^(ยกเลิก|cancel)$/i.test(normalized) && getPendingExpense(source)) {
    clearPendingExpense(source);
    return "ยกเลิกสถานะเก่าที่ค้างไว้แล้ว";
  }

  // Flowaccount invoice URL detection
  if (FLOWACCOUNT_URL_REGEX.test(rawText)) {
    const faUrl = rawText.match(FLOWACCOUNT_URL_REGEX)?.[0];
    if (faUrl) {
      const faData = await fetchFlowaccountInvoice(faUrl);
      if (faData && faData.amount) {
        const aiParsed = {
          amount: faData.amount,
          currency: "THB",
          vendorName: faData.vendorName || "Flowaccount",
          projectName: await getGroupDefaultProject(getGroupIdFromSource(source)) || "",
          expenseType: "invoice",
          note: normalizeSpaces(
            `source=flowaccount_link | doc=${faData.docNumber ?? ""} | date=${faData.docDate ?? ""} | url=${faUrl}`
          ).slice(0, 1800),
        };
        if (hasRequiredExpenseFields(aiParsed)) {
          const result = await saveExpenseWithProjectPrompt(aiParsed, source, event.message?.id);
          if (result.ok) {
            queueImageDigestEvent({
              pushTarget: source?.groupId ?? source?.roomId ?? "",
              groupId: getGroupIdFromSource(source),
              sourceType: source?.type ?? "",
              messageId: event.message?.id,
              category: "expense",
              status: "needs_review",
              reason: "flowaccount_link",
              sourceTag: "flowaccount",
              projectName: aiParsed.projectName,
              vendorName: aiParsed.vendorName,
              amount: aiParsed.amount,
              currency: "THB",
            });
          }
          return null;
        }
      }
    }
  }

  if (!normalized.startsWith("/") && NOVA_ENABLED) {
    const novaRaw = await callTextParseWithNova(rawText, event);
    if (novaRaw?.action === "create_event") {
      const ev = novaRaw._raw?.structuredData?.event ?? {};
      const gid = getGroupIdFromSource(source);
      if (ev.projectName && gid) {
        const totalBooths = (ev.totalBooths && Number(ev.totalBooths) > 0) ? Number(ev.totalBooths) : null;
        const venue = normalizeSpaces(ev.location ?? ev.venue ?? "") || null;
        const { error } = await supabase.from("line_project_pricing").upsert({
          group_id: gid,
          project_name: normalizeSpaces(ev.projectName),
          event_start_date: ev.startDate ?? null,
          event_end_date: ev.endDate ?? null,
          booth_price: ev.boothPrice ? Number(ev.boothPrice) : 0,
          total_booths: totalBooths,
          updated_at: new Date().toISOString(),
        }, { onConflict: "group_id,project_name" });
        if (error) {
          console.error("[nova] create_event error:", error);
          return "เกิดข้อผิดพลาดในการบันทึกงาน";
        }
        const boothLine = totalBooths ? `\n🏪 ${totalBooths} บูธ` : "";
        const venueLine = venue ? `\n📍 ${venue}` : "";
        console.log(`[nova] create_event project="${ev.projectName}" start=${ev.startDate} end=${ev.endDate} total_booths=${totalBooths}`);
        return `✅ บันทึกงาน: ${ev.projectName}\n📅 ${ev.startDate ?? "?"} → ${ev.endDate ?? "?"}${venueLine}${boothLine}`;
      }
    }
  }

  if (!normalized.startsWith("/")) {
    const bookingResponse = await tryAutoBookingText(rawText, source, event.message?.id, event);
    if (bookingResponse) return bookingResponse;
  }

  if (looksLikeExpenseText(rawText)) {
    const expParsed = parseExpenseCommand(rawText);
    const expResult = await saveExpenseWithProjectPrompt(expParsed, source, event.message?.id);
    const expPushTarget = getImageDigestPushTarget(source ?? {});
    if (!expResult.ok) return null; // silent - not saved
    if (expPushTarget) {
      queueImageDigestEvent({
        pushTarget: expPushTarget,
        groupId: getGroupIdFromSource(source),
        sourceType: source?.type ?? "",
        messageId: event.message?.id ?? null,
        category: "expense",
        status: "saved",
        sourceTag: "text",
        reason: "auto_parsed",
        projectName: expParsed.projectName ?? "",
        shopName: "",
        boothCode: "",
        vendorName: expParsed.vendorName ?? "",
        amount: expParsed.amount ?? null,
        currency: expParsed.currency ?? "THB",
        detail: expParsed.detail ?? "",
      });
      startImageDigestScheduler();
    }
    return null; // silent - reported in digest
  }

  if (LINE_AI_TEXT_FALLBACK_ENABLED && mightBeUnrecognizedExpense(rawText)) {
    const analysis = await callAIForTextParse(rawText);
    const classification = inferAiClassification(analysis, rawText);
    if (classification === "expense") {
      const aiExpense = buildExpenseFromAiAnalysis(analysis, rawText);
      if (aiExpense?.amount !== null && aiExpense?.amount !== undefined) {
        const pushTarget = getImageDigestPushTarget(source ?? {});
        if (pushTarget) {
          queueImageDigestEvent({
            pushTarget,
            groupId: getGroupIdFromSource(source),
            sourceType: source?.type ?? "",
            messageId: event.message?.id ?? null,
            category: "expense",
            status: "saved",
            sourceTag: "text",
            reason: "ai_parsed",
            projectName: aiExpense.projectName ?? "",
            shopName: "",
            boothCode: "",
            vendorName: aiExpense.vendorName ?? "",
            amount: aiExpense.amount,
            currency: aiExpense.currency ?? "THB",
            detail: aiExpense.detail ?? "",
          });
          startImageDigestScheduler();
        }
        await saveExpenseWithProjectPrompt(aiExpense, source, event.message?.id);
      }
    }
    return null; // silent — no immediate reply
  }

  return null;
}


async function handleExportCsvRequest(res, reqUrl, type = "daily") {
  if (!LINE_EXPORT_TOKEN) {
    return jsonResponse(res, 503, { error: "Export token not configured" });
  }

  const token = reqUrl.searchParams.get("token") ?? "";
  if (token !== LINE_EXPORT_TOKEN) {
    return jsonResponse(res, 401, { error: "Invalid token" });
  }

  const groupId = reqUrl.searchParams.get("group") ?? "";
  if (!groupId) {
    return jsonResponse(res, 400, { error: "Missing group parameter" });
  }

  if (type === "expense") {
    const dateStr = reqUrl.searchParams.get("date") ?? formatDateInTz(new Date());
    const projectName = normalizeSpaces(reqUrl.searchParams.get("project") ?? "");

    const { data, error } = await fetchDailyExpenses(groupId, dateStr, projectName);
    if (error) {
      console.error(error);
      if (isMissingExpenseTableError(error)) {
        return jsonResponse(res, 503, { error: EXPENSE_MIGRATION_HINT });
      }
      return jsonResponse(res, 500, { error: "Failed to fetch expense records" });
    }

    const csv = withUtf8Bom(buildExpenseCsv(data));
    const safeProject = projectName ? `-${projectName.replace(/[^A-Za-z0-9_-]+/g, "_")}` : "";
    const filename = `expense-${dateStr}${safeProject}.csv`;
    return textResponse(res, 200, csv, {
      "Content-Type": "text/csv; charset=utf-8",
      "Content-Disposition": `attachment; filename="${filename}"`,
    });
  }

  if (type === "master") {
    const projectName = normalizeSpaces(reqUrl.searchParams.get("project") ?? "");
    const groupId = normalizeSpaces(reqUrl.searchParams.get("group") ?? "");
    const days = Math.max(1, parseInt(reqUrl.searchParams.get("days") ?? "1", 10) || 1);
    const { data, error } = await fetchProjectBookings(groupId || null, projectName || null);
    if (error) {
      res.writeHead(500, { "Content-Type": "text/plain; charset=utf-8" });
      res.end("DB error");
      return;
    }
    const csv = buildMasterCsv(data, days);
    const safeProject = (projectName || "master").replace(/[^A-Za-z0-9\u0E00-\u0E7F_\-]/g, "_");
    res.writeHead(200, {
      "Content-Type": "text/csv; charset=utf-8",
      "Content-Disposition": `attachment; filename="${safeProject}_master.csv"`,
    });
    res.end(withUtf8Bom(csv));
    return;
  }
  if (type === "install") {
    const projectName = normalizeSpaces(reqUrl.searchParams.get("project") ?? "");
    if (!projectName) {
      return jsonResponse(res, 400, { error: "Missing project parameter" });
    }

    const { data, error } = await fetchInstallBookings(groupId, projectName);
    if (error) {
      console.error(error);
      return jsonResponse(res, 500, { error: "Failed to fetch installer bookings" });
    }

    const csv = withUtf8Bom(buildInstallCsv(data));
    const safeProject = projectName.replace(/[^A-Za-z0-9_-]+/g, "_");
    const filename = `install-${safeProject}.csv`;
    return textResponse(res, 200, csv, {
      "Content-Type": "text/csv; charset=utf-8",
      "Content-Disposition": `attachment; filename="${filename}"`,
    });
  }

  const dateStr = reqUrl.searchParams.get("date") ?? formatDateInTz(new Date());
  const { data, error } = await fetchDailyBookings(groupId, dateStr);
  if (error) {
    console.error(error);
    return jsonResponse(res, 500, { error: "Failed to fetch bookings" });
  }

  const csv = withUtf8Bom(buildDailyCsv(data));
  const filename = `booking-${dateStr}.csv`;
  return textResponse(res, 200, csv, {
    "Content-Type": "text/csv; charset=utf-8",
    "Content-Disposition": `attachment; filename="${filename}"`,
  });
}


async function handlePdfMessage(event) {
  const source = event.source;
  const messageId = event.message?.id;
  const fileName = event.message?.fileName ?? "";
  console.log(`[file] pdf:start message=${messageId} file=${fileName}`);

  const buffer = await fetchLineMessageContent(messageId);
  if (!buffer) {
    console.error("[file] pdf: failed to fetch content");
    return;
  }

  const pushTarget = getImageDigestPushTarget(source ?? {});
  const analysis = await analyzeImageWithGemini(buffer, "", "application/pdf");
  if (!analysis) {
    console.warn("[file] pdf: AI analysis returned null");
    return;
  }

  const classification = inferAiClassification(analysis, "");
  console.log(`[file] pdf:classification=${classification} message=${messageId}`);

  if (classification === "booking") {
    const parsed = buildBookingFromAiAnalysis(analysis, "");
    if (!parsed.projectName) {
      const gid = getGroupIdFromSource(source);
      const def = await getGroupDefaultProject(gid);
      if (def) parsed.projectName = def;
    }
    if (!hasRequiredBookingFields(parsed)) {
      console.warn("[file] pdf: missing required booking fields");
      return;
    }
    const result = await saveBookingWithAgentRules(parsed, source, messageId);
    console.log(`[file] pdf:saveResult ok=${result.ok} project=${parsed.projectName ?? "-"} shop=${parsed.shopName ?? "-"}`);
    if (pushTarget) {
      queueImageDigestEvent({
        pushTarget,
        groupId: getGroupIdFromSource(source),
        sourceType: source?.type ?? "",
        messageId: messageId ?? null,
        category: "booking",
        status: result.ok ? "saved" : "needs_review",
        sourceTag: "pdf",
        reason: "ai_parsed",
        projectName: parsed.projectName ?? "",
        shopName: parsed.shopName ?? "",
        boothCode: parsed.boothCode ?? "",
        vendorName: "",
        amount: null,
        currency: "THB",
        detail: "",
      });
      startImageDigestScheduler();
    }
  } else if (classification === "expense") {
    const parsed = buildExpenseFromAiAnalysis(analysis, "");
    if (!hasRequiredExpenseFields(parsed)) return;
    await saveExpenseWithProjectPrompt(parsed, source, messageId);
  }
}

function powerPriceToLabel(price) {
  const p = Number(price) || 0;
  const map = { 0: "5A (ฟรี)", 500: "5A 24ชม", 700: "10-15A", 1000: "20-30A", 1200: "10-15A 24ชม", 1500: "⚠️ ตรวจสอบ (30A 3เฟส หรือ 20-30A 24ชม)", 2000: "30A 3เฟส 24ชม" };
  return map[p] ?? (p > 0 ? String(p) + " บ." : "-");
}
async function handleFileMessage(event) {
  const source = event.source;
  const messageId = event.message?.id;
  const fileName = event.message?.fileName ?? "";

  // Route PDF to dedicated handler
  if (/\.pdf$/i.test(fileName)) {
    return handlePdfMessage(event);
  }

  const isXlsx =
    /\.xlsx?$/i.test(fileName) || event.message?.type === "file";

  if (!isXlsx) return;

  let buffer;
  try {
    buffer = await fetchLineMessageContent(messageId);
  } catch (err) {
    console.error("[file] fetchLineMessageContent error:", err?.message ?? err);
    return;
  }
  if (!buffer) return;

  const groupId = getGroupIdFromSource(source);

  // Fetch known projects (with dates) for this group to help Nova match sheet → project
  let knownProjects = [];
  if (groupId) {
    const todayStr = getTimePartsInTz(new Date()).dateStr;
    const { data: pData } = await supabase
      .from("line_project_pricing")
      .select("project_name, event_start_date, event_end_date")
      .eq("group_id", groupId)
      .gte("event_end_date", todayStr);
    knownProjects = (pData ?? [])
      .filter((p) => p.project_name)
      .map((p) => ({ name: p.project_name, startDate: p.event_start_date, endDate: p.event_end_date }));
  }

  // ── Try Nova first for intelligent AI parsing ──────────────────────────────
  // Also parse XLSX locally to get raw phone values (Nova may miss Thai-prefixed phones)
  let localPhoneMap = new Map(); // key: "sheetIndex:boothCode" or "sheetIndex:rowIndex"
  try {
    const xlsxMod = await import("xlsx");
    const XLSX = xlsxMod.default ?? xlsxMod;
    const wb = XLSX.read(buffer, { type: "buffer" });
    wb.SheetNames.forEach((sheetName, si) => {
      const rows = XLSX.utils.sheet_to_json(wb.Sheets[sheetName], { defval: "" });
      rows.forEach((row, ri) => {
        const pickRaw = (...keys) => String(keys.map((k) => row[k] ?? "").find((v) => v !== "") ?? "");
        const rawPhone = pickRaw("phone", "เบอร์โทร", "เบอร์", "Phone");
        const rawBooth = normalizeSpaces(String(pickRaw("booth_code", "บูธ", "เลขบูธ", "Booth", "BOOTH"))).replace(/^(\d+)\.0+$/, "$1");
        if (rawPhone) {
          if (rawBooth) localPhoneMap.set(`${si}:${rawBooth}`, rawPhone);
          localPhoneMap.set(`${si}:${ri}`, rawPhone);
        }
      });
    });
  } catch { /* xlsx not available or parse error — proceed without local phone map */ }

  const novaRows = await callNovaForExcel(buffer, fileName, knownProjects);
  let excelRows;
  if (novaRows) {
    excelRows = novaRows
      .map((r, ri) => {
        const tcPrice = Number(r.tableChairPrice) || 0;
        const tableFreeQty = Math.floor(tcPrice / 350);
        const chairFreeQty = Math.floor((tcPrice % 350) / 80);
        const pwLabel = powerPriceToLabel(Number(r.powerPrice) || 0);
        const boothCode = normalizeSpaces(String(r.boothCode ?? "")).replace(/^(\d+)\.0+$/, "$1");
        const novaPhone = normalizeSpaces(String(r.phone ?? ""));
        // If Nova returned empty phone, fall back to raw local XLSX value so extractPhones can parse it
        const sheetIdx = Number(r._sheetIndex ?? 0);
        const rawLocalPhone = localPhoneMap.get(`${sheetIdx}:${boothCode}`) || localPhoneMap.get(`${sheetIdx}:${ri}`) || "";
        const phone = novaPhone || rawLocalPhone;
        return {
          boothCode,
          shopName: normalizeSpaces(String(r.shopName ?? "")),
          projectName: normalizeSpaces(String(r.projectName ?? "")),
          phone,
          note: normalizeSpaces(String(r.note ?? "")),
          tableFreeQty,
          chairFreeQty,
          powerLabel: pwLabel !== "-" ? pwLabel : "",
        };
      })
      .filter((r) => r.shopName && r.projectName);
  } else {
    // ── Fallback: local XLSX parsing ──────────────────────────────────────────
    let xlsxMod;
    try {
      xlsxMod = await import("xlsx");
    } catch {
      console.warn("[file] xlsx module not available and Nova unavailable — cannot parse Excel");
      return;
    }
    const XLSX = xlsxMod.default ?? xlsxMod;
    let workbook;
    try {
      workbook = XLSX.read(buffer, { type: "buffer" });
    } catch (err) {
      console.warn("[file] XLSX.read error:", err?.message ?? err);
      return;
    }

    const defaultProject = await getGroupDefaultProject(groupId);
    const sheetProjectMap = {};
    for (const sheetName of workbook.SheetNames) {
      sheetProjectMap[sheetName] = await resolveProjectFromSheetName(sheetName, groupId, defaultProject);
      console.log(`[file] xlsx:sheet="${sheetName}" → project="${sheetProjectMap[sheetName]}"`);
    }

    excelRows = [];
    let firstSheetLogged = false;
    for (const sheetName of workbook.SheetNames) {
      const sheet = workbook.Sheets[sheetName];
      const rows = XLSX.utils.sheet_to_json(sheet, { defval: "" });
      if (!firstSheetLogged && rows.length > 0) {
        console.log(`[file] xlsx:columns sheet="${sheetName}" cols=${JSON.stringify(Object.keys(rows[0]))}`);
        console.log(`[file] xlsx:sample row[0]=${JSON.stringify(rows[0]).slice(0, 200)}`);
        firstSheetLogged = true;
      }
      let lastShopName = "";
      for (const row of rows) {
        const pick = (...keys) => normalizeSpaces(String(keys.map((k) => row[k] ?? "").find((v) => v !== "") ?? ""));
        const boothCode = pick("booth_code", "บูธ", "เลขบูธ", "Booth", "BOOTH");
        const rawShop = pick("shop_name", "ชื่อร้าน", "ร้าน", "Shop", "SHOP");
        if (rawShop) lastShopName = rawShop;
        const shopName = lastShopName;
        const projectName = sheetProjectMap[sheetName] || defaultProject || "";
        const phone = pick("phone", "เบอร์โทร", "เบอร์", "Phone");
        const note = pick("note", "หมายเหตุ", "Note");
        const isSummaryRow = /^(total|รวม|ผลรวม|สรุป|summary|sub\s*total|grand\s*total|ทั้งหมด)$/i.test(shopName);
        if (boothCode && shopName && !isSummaryRow) excelRows.push({ boothCode, shopName, projectName, phone, note });
      }
    }
  }

  if (excelRows.length === 0) {
    const groupReplyTarget = source?.groupId ?? source?.roomId ?? source?.userId;
    if (groupReplyTarget) await pushMessage(groupReplyTarget, ["ไม่พบข้อมูลในไฟล์ Excel (ตรวจสอบชื่อคอลัมน์: ชื่อร้าน หรือ shop_name)"]);
    return;
  }

  console.log(`[file] xlsx:import rows=${excelRows.length} file=${fileName}`);

  let saved = 0, duplicates = 0, errors = 0;
  for (const row of excelRows) {
    if (!row.projectName) { errors++; continue; }
    const result = await saveBookingWithAgentRules(row, source, messageId, { forceReplace: true });
    if (result.ok) saved++;
    else if (result.needsConfirmation) {
      duplicates++;
      if (row.tableFreeQty || row.chairFreeQty || row.powerLabel) {
        await supabase.from("line_booking_records")
          .update({ table_free_qty: row.tableFreeQty ?? 0, chair_free_qty: row.chairFreeQty ?? 0, power_label: row.powerLabel || null })
          .eq("project_name", row.projectName).eq("booth_code", row.boothCode).eq("booking_status", "booked");
      }
    }
    else errors++;
  }

  const groupReplyTarget = source?.groupId ?? source?.roomId ?? source?.userId;
  if (!groupReplyTarget) return;

  const replyText = [
    `📊 นำเข้า Excel: ${fileName}`,
    `✅ บันทึกแล้ว: ${saved} รายการ`,
    duplicates > 0 ? `⚠️ บูธซ้ำ (ข้าม): ${duplicates} รายการ` : null,
    errors > 0 ? `❌ ข้อมูลไม่ครบ (ข้าม): ${errors} รายการ` : null,
  ].filter(Boolean).join("\n");

  await pushMessage(groupReplyTarget, [replyText]);
}
async function processEvent(event) {
  try {
    if (event.type !== "message") return;

    if (event.message?.type === "image") {
      const result = await commandBookingFromImage(event);
      if (result?.digestEvent) queueImageDigestEvent(result.digestEvent);
      if (result?.replyText && event.replyToken && (result.forceImmediateReply || !shouldSuppressImmediateImageReply(event))) {
        await replyMessage(event.replyToken, [result.replyText]);
      }
      return;
    }

    if (event.message?.type === "file") {
      await handleFileMessage(event);
      return;
    }
    if (event.message?.type !== "text") return;
    const responseText = await handleTextMessage(event);
    if (!responseText) return;
    await replyMessage(event.replyToken, [responseText]);
  } catch (error) {
    console.error("Event handling error:", error);
    if (event.message?.type === "image" && shouldSuppressImmediateImageReply(event)) {
      queueImageDigestEvent(buildImageDigestEvent(event, { category: "failure", status: "needs_review", reason: "pipeline_error", detail: error?.message ?? String(error) }));
      return;
    }
    if (event.replyToken) {
      await replyMessage(event.replyToken, ["เกิดข้อผิดพลาดในระบบ กรุณาลองใหม่"]);
    }
  }
}
const server = http.createServer(async (req, res) => {
  const reqUrl = new URL(req.url ?? "/", `http://${req.headers.host ?? "localhost"}`);

  if (req.method === "GET" && reqUrl.pathname === "/health") {
    return jsonResponse(res, 200, { ok: true, service: "line-booking-bot" });
  }

  if (req.method === "GET" && reqUrl.pathname === "/exports/daily.csv") {
    return handleExportCsvRequest(res, reqUrl, "daily");
  }

  if (req.method === "GET" && reqUrl.pathname === "/exports/install.csv") {
    return handleExportCsvRequest(res, reqUrl, "install");
  }

  if (req.method === "GET" && reqUrl.pathname === "/exports/master.csv") {
    const token = reqUrl.searchParams.get("token");
    if (token !== LINE_EXPORT_TOKEN) {
      res.writeHead(403, { "Content-Type": "text/plain" });
      res.end("Forbidden");
      return;
    }
    await handleExportCsvRequest(res, reqUrl, "master");
    return;
  }
  if (req.method === "GET" && reqUrl.pathname === "/exports/expense.csv") {
    return handleExportCsvRequest(res, reqUrl, "expense");
  }

  if (req.method !== "POST" || reqUrl.pathname !== "/webhook/line") {
    return jsonResponse(res, 404, { error: "Not found" });
  }

  const chunks = [];
  req.on("data", (chunk) => chunks.push(chunk));
  req.on("error", (err) => {
    console.error(err);
    jsonResponse(res, 500, { error: "Read error" });
  });
  req.on("end", async () => {
    const rawBody = Buffer.concat(chunks).toString("utf8");
    const signature = req.headers["x-line-signature"];
    if (!verifySignature(rawBody, signature)) {
      return jsonResponse(res, 401, { error: "Invalid signature" });
    }

    let body;
    try {
      body = JSON.parse(rawBody);
    } catch {
      return jsonResponse(res, 400, { error: "Invalid JSON" });
    }

    const events = Array.isArray(body.events) ? body.events : [];
    await Promise.all(events.map((evt) => processEvent(evt)));
    return jsonResponse(res, 200, { ok: true });
  });
});

server.listen(PORT, () => {
  console.log(`LINE bot listening on :${PORT}`);
  console.log("POST /webhook/line");
  console.log("GET /exports/daily.csv");
  console.log("GET /exports/install.csv");
  console.log("GET /exports/expense.csv");
  console.log("GET /exports/master.csv");
  startImageDigestScheduler();
  startEventReminderScheduler();
});














