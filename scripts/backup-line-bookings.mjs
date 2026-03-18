import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { createClient } from "@supabase/supabase-js";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const ROOT_DIR = path.resolve(__dirname, "..", "..");
const LINE_BOT_DIR = path.resolve(__dirname, "..");

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
    if (process.env[key] === undefined) {
      process.env[key] = value.replace(/^["']|["']$/g, "");
    }
  }
}

function parseArgs(argv) {
  const args = {};
  for (let i = 2; i < argv.length; i += 1) {
    const part = argv[i];
    if (!part.startsWith("--")) continue;
    const key = part.slice(2);
    const value = argv[i + 1] && !argv[i + 1].startsWith("--") ? argv[i + 1] : "true";
    args[key] = value;
    if (value !== "true") i += 1;
  }
  return args;
}

function formatDateInTz(date, timeZone = "Asia/Bangkok") {
  const parts = new Intl.DateTimeFormat("en-CA", {
    timeZone,
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  }).formatToParts(date);

  const y = parts.find((p) => p.type === "year")?.value;
  const m = parts.find((p) => p.type === "month")?.value;
  const d = parts.find((p) => p.type === "day")?.value;
  return y && m && d ? `${y}-${m}-${d}` : "";
}

function getDailyRangeUtc(dateStr, timezone = "Asia/Bangkok") {
  const m = String(dateStr ?? "").match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if (!m) return null;

  const year = Number(m[1]);
  const month = Number(m[2]);
  const day = Number(m[3]);
  if (month < 1 || month > 12 || day < 1 || day > 31) return null;

  const tzOffsetHours = timezone === "Asia/Bangkok" ? 7 : 0;
  const startUtc = new Date(Date.UTC(year, month - 1, day, -tzOffsetHours, 0, 0));
  const endUtc = new Date(startUtc.getTime() + 24 * 60 * 60 * 1000);
  return { startIso: startUtc.toISOString(), endIso: endUtc.toISOString() };
}

function csvEscape(value) {
  const s = String(value ?? "");
  if (/[",\n]/.test(s)) return `"${s.replace(/"/g, '""')}"`;
  return s;
}

function isMissingSalesSchemaError(error) {
  const raw = `${error?.message ?? ""} ${error?.hint ?? ""}`;
  return /booth_price|line_project_pricing/i.test(raw);
}

function withUtf8Bom(value) {
  const text = String(value ?? "");
  return text.startsWith("\uFEFF") ? text : `\uFEFF${text}`;
}

function buildBookingCsv(rows) {
  const headers = [
    "id",
    "group_id",
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
    "booked_at",
    "cancelled_at",
    "note",
  ];

  const lines = [headers.join(",")];
  for (const row of rows) {
    lines.push(
      headers
        .map((h) => csvEscape(row[h]))
        .join(","),
    );
  }

  return `${lines.join("\n")}\n`;
}

function buildExpenseCsv(rows) {
  const headers = [
    "id",
    "group_id",
    "project_name",
    "amount",
    "currency",
    "vendor_name",
    "expense_type",
    "expense_status",
    "paid_at",
    "cancelled_at",
    "note",
    "source_user_id",
    "source_message_id",
  ];

  const lines = [headers.join(",")];
  for (const row of rows) {
    lines.push(
      headers
        .map((h) => csvEscape(row[h]))
        .join(","),
    );
  }

  return `${lines.join("\n")}\n`;
}

function pruneOldFiles(dirPath, keepDays) {
  if (!Number.isFinite(keepDays) || keepDays <= 0) return;
  const files = fs.readdirSync(dirPath, { withFileTypes: true });
  const cutoff = Date.now() - keepDays * 24 * 60 * 60 * 1000;

  for (const entry of files) {
    if (!entry.isFile()) continue;
    const fullPath = path.join(dirPath, entry.name);
    const stat = fs.statSync(fullPath);
    if (stat.mtimeMs < cutoff) {
      fs.unlinkSync(fullPath);
    }
  }
}

loadEnvFile(path.join(ROOT_DIR, ".env"));
loadEnvFile(path.join(LINE_BOT_DIR, ".env"));

const args = parseArgs(process.argv);
const timezone = process.env.LINE_TIMEZONE ?? "Asia/Bangkok";
const backupDate = args.date === "true" ? "" : (args.date ?? formatDateInTz(new Date(), timezone));
const groupId = args.group === "true" ? "" : (args.group ?? "");
const projectName = args.project === "true" ? "" : (args.project ?? "");
const allRows = args.all === "true";
const backupDir = path.resolve(args.out === "true" ? "./backups/line-booking" : (args.out ?? "./backups/line-booking"));
const keepDays = Number(args.keep_days ?? process.env.LINE_BACKUP_KEEP_DAYS ?? 45);

const SUPABASE_URL = process.env.SUPABASE_URL ?? process.env.VITE_SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
  console.error("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY");
  process.exit(1);
}

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, {
  auth: { autoRefreshToken: false, persistSession: false },
});

let bookingQuery = supabase
  .from("line_booking_records")
  .select("id, group_id, project_name, shop_name, phone, booth_code, product_type, booth_price, table_free_qty, table_extra_qty, chair_free_qty, chair_extra_qty, power_amp, power_label, booking_status, booked_at, cancelled_at, note")
  .order("booked_at", { ascending: true })
  .limit(100000);

let expenseQuery = supabase
  .from("line_expense_records")
  .select("id, group_id, project_name, amount, currency, vendor_name, expense_type, expense_status, paid_at, cancelled_at, note, source_user_id, source_message_id")
  .order("paid_at", { ascending: true })
  .limit(100000);

if (groupId) {
  bookingQuery = bookingQuery.eq("group_id", groupId);
  expenseQuery = expenseQuery.eq("group_id", groupId);
}

if (projectName) {
  bookingQuery = bookingQuery.eq("project_name", projectName);
  expenseQuery = expenseQuery.eq("project_name", projectName);
}

if (!allRows && backupDate) {
  const range = getDailyRangeUtc(backupDate, timezone);
  if (!range) {
    console.error("Invalid --date format. Use YYYY-MM-DD");
    process.exit(1);
  }
  bookingQuery = bookingQuery.gte("booked_at", range.startIso).lt("booked_at", range.endIso);
  expenseQuery = expenseQuery.gte("paid_at", range.startIso).lt("paid_at", range.endIso);
}

const [bookingResult, expenseResult] = await Promise.all([bookingQuery, expenseQuery]);

let bookingRows = [];
let salesWarning = "";
if (bookingResult.error) {
  if (!isMissingSalesSchemaError(bookingResult.error)) {
    console.error(bookingResult.error);
    process.exit(1);
  }

  salesWarning = "booth_price column not found. Run supabase/line_bot_sales_migration.sql to enable sales fields in backup.";

  let fallbackBookingQuery = supabase
    .from("line_booking_records")
    .select("id, group_id, project_name, shop_name, phone, booth_code, product_type, table_free_qty, table_extra_qty, chair_free_qty, chair_extra_qty, power_amp, power_label, booking_status, booked_at, cancelled_at, note")
    .order("booked_at", { ascending: true })
    .limit(100000);

  if (groupId) fallbackBookingQuery = fallbackBookingQuery.eq("group_id", groupId);
  if (projectName) fallbackBookingQuery = fallbackBookingQuery.eq("project_name", projectName);
  if (!allRows && backupDate) {
    const range = getDailyRangeUtc(backupDate, timezone);
    if (!range) {
      console.error("Invalid --date format. Use YYYY-MM-DD");
      process.exit(1);
    }
    fallbackBookingQuery = fallbackBookingQuery.gte("booked_at", range.startIso).lt("booked_at", range.endIso);
  }

  const fallback = await fallbackBookingQuery;
  if (fallback.error) {
    console.error(fallback.error);
    process.exit(1);
  }

  bookingRows = (fallback.data ?? []).map((row) => ({ ...row, booth_price: null }));
} else {
  bookingRows = bookingResult.data ?? [];
}

let expenseRows = [];
let expenseWarning = "";

if (expenseResult.error) {
  if (expenseResult.error.code === "PGRST205") {
    expenseWarning = "line_expense_records table not found. Run supabase/line_bot_expense_migration.sql to enable expense backup.";
  } else {
    console.error(expenseResult.error);
    process.exit(1);
  }
} else {
  expenseRows = expenseResult.data ?? [];
}

fs.mkdirSync(backupDir, { recursive: true });

const stamp = allRows ? "all" : backupDate;
const scope = [
  groupId ? `group-${groupId}` : "all-groups",
  projectName ? `project-${projectName.replace(/[^A-Za-z0-9_-]+/g, "_")}` : "all-projects",
].join("_");
const base = `${stamp}_${scope}`;

const jsonPath = path.join(backupDir, `${base}.json`);
const bookingCsvPath = path.join(backupDir, `${base}.csv`);
const expenseCsvPath = path.join(backupDir, `${base}_expenses.csv`);

fs.writeFileSync(
  jsonPath,
  JSON.stringify(
    {
      generated_at: new Date().toISOString(),
      timezone,
      date: allRows ? null : backupDate,
      group_id: groupId || null,
      project_name: projectName || null,
      rows: bookingRows,
      booking_rows: bookingRows,
      expense_rows: expenseRows,
    },
    null,
    2,
  ),
  "utf8",
);

fs.writeFileSync(bookingCsvPath, withUtf8Bom(buildBookingCsv(bookingRows)), "utf8");
fs.writeFileSync(expenseCsvPath, withUtf8Bom(buildExpenseCsv(expenseRows)), "utf8");

pruneOldFiles(backupDir, keepDays);

if (salesWarning) {
  console.warn(salesWarning);
}
if (expenseWarning) {
  console.warn(expenseWarning);
}
console.log(`Backup complete: bookings=${bookingRows.length}, expenses=${expenseRows.length}`);
console.log(`JSON: ${jsonPath}`);
console.log(`CSV : ${bookingCsvPath}`);
console.log(`CSV : ${expenseCsvPath}`);
