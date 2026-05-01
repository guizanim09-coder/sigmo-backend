const express = require("express");
const crypto = require("crypto");
const fs = require("fs");
const path = require("path");
const cors = require("cors");
const bcrypt = require("bcryptjs");
const multer = require("multer");
const Tesseract = require("tesseract.js");
const { PDFParse } = require("pdf-parse");
const helmet = require("helmet");
const rateLimit = require("express-rate-limit");
const jwt = require("jsonwebtoken");
const { Pool } = require("pg");
const BOT_SECRET = process.env.BOT_SECRET;

const app = express();

app.set("trust proxy", 1);

const PORT = process.env.PORT || 3000;
const DATABASE_URL = String(process.env.DATABASE_URL || "").trim();
const JWT_SECRET = String(process.env.JWT_SECRET || "").trim();
const ADMIN_EMAIL = String(process.env.ADMIN_EMAIL || "").trim().toLowerCase();
const ADMIN_PASSWORD = String(process.env.ADMIN_PASSWORD || "").trim();
const DENTPEG_PUBLIC_CHECKOUT_URL = String(
  process.env.DENTPEG_PUBLIC_CHECKOUT_URL || "https://api.dentpeg.com/checkout/sigmo"
).trim();
const DENTPEG_CHECKOUT_TIMEOUT_MS = Math.max(
  3000,
  Number(process.env.DENTPEG_CHECKOUT_TIMEOUT_MS || 15000)
);
const BACKUP_ENABLED =
  String(process.env.BACKUP_ENABLED || "true").trim().toLowerCase() !== "false";
const BACKUP_INTERVAL_HOURS = Number(process.env.BACKUP_INTERVAL_HOURS || 24);
const BACKUP_RETENTION_DAYS = Number(process.env.BACKUP_RETENTION_DAYS || 7);
const BACKUP_INITIAL_DELAY_MS = Number(process.env.BACKUP_INITIAL_DELAY_MS || 30000);
const BACKUP_DIR = String(process.env.BACKUP_DIR || "").trim();
const LIMITE_DEPOSITO_MIN = Number(process.env.LIMITE_DEPOSITO_MIN || 10);
const LIMITE_DEPOSITO_MAX = Number(process.env.LIMITE_DEPOSITO_MAX || 3000);
const LIMITE_SAQUE_PIX_MIN = Number(process.env.LIMITE_SAQUE_PIX_MIN || 100);
const LIMITE_SAQUE_PIX_MAX = Number(process.env.LIMITE_SAQUE_PIX_MAX || 5900);
const TAXA_SAQUE_PIX_PERCENTUAL = Number(
  process.env.TAXA_SAQUE_PIX_PERCENTUAL || 0.10
);
const COMPROVANTE_UPLOAD_WINDOW_MINUTES = Number(
  process.env.COMPROVANTE_UPLOAD_WINDOW_MINUTES || 60
);
const BONUS_BOAS_VINDAS_VALOR = Number(
  process.env.BONUS_BOAS_VINDAS_VALOR || 5
);
const PIX_SAQUE_DESBLOQUEIO_MIN = Number(
  process.env.PIX_SAQUE_DESBLOQUEIO_MIN || 100
);
const USER_MOBILE_TOKEN_TTL = String(
  process.env.USER_MOBILE_TOKEN_TTL || "30d"
).trim();
const SIGMO_TAP_CHARGE_TTL_SECONDS = Math.max(
  60,
  Number(process.env.SIGMO_TAP_CHARGE_TTL_SECONDS || 600)
);
const NFC_RECEIVE_SESSION_TTL_SECONDS = Math.max(
  15,
  Number(process.env.NFC_RECEIVE_SESSION_TTL_SECONDS || 30)
);
const NFC_PROTOCOL_VERSION = Math.max(
  1,
  Number(process.env.NFC_PROTOCOL_VERSION || 1)
);
const SIGMO_APP_TAP_RECEIVE_SCHEME = String(
  process.env.SIGMO_APP_TAP_RECEIVE_SCHEME || "sigmo://tap-receive"
).trim();
const SIGMO_APP_CARD_CLAIM_SCHEME = String(
  process.env.SIGMO_APP_CARD_CLAIM_SCHEME || "sigmo://card-claim"
).trim();
const STATUS_CONTA_ATIVA = "ativa";
const STATUS_CONTA_BANIDA = "banida";
const MOTIVO_BANIMENTO_FRAUDE_BONUS = "tentativa_fraude_bonus";

if (!DATABASE_URL) {
  console.error("DATABASE_URL não configurada.");
  process.exit(1);
}

if (!JWT_SECRET) {
  console.error("JWT_SECRET não configurada.");
  process.exit(1);
}

if (!BOT_SECRET) {
  console.error("BOT_SECRET não configurado.");
  process.exit(1);
}

const pool = new Pool({
  connectionString: DATABASE_URL,
  ssl: { rejectUnauthorized: false }
});

const BACKUPS_DIR = BACKUP_DIR
  ? path.resolve(BACKUP_DIR)
  : path.join(__dirname, "backups");

if (!fs.existsSync(BACKUPS_DIR)) {
  fs.mkdirSync(BACKUPS_DIR, { recursive: true });
}

const backupState = {
  running: false,
  timer: null,
  lastRunAt: null,
  lastSuccessAt: null,
  lastError: null,
  lastFile: null,
  lastDurationMs: null
};

function isPositiveNumber(value) {
  return Number.isFinite(value) && value > 0;
}

function sanitizeTimestamp(value) {
  return String(value || "")
    .replace(/[:.]/g, "-")
    .replace(/[^0-9TZ-]/g, "");
}

function getBackupFileName(date = new Date()) {
  return `sigmo-backup-${sanitizeTimestamp(date.toISOString())}.json`;
}

function getBackupFilePath(fileName) {
  return path.join(BACKUPS_DIR, fileName);
}

async function gerarPixDentpegPublico(valor) {
  const valorNumero = toMoney(valor);

  if (!Number.isFinite(valorNumero) || valorNumero <= 0) {
    throw new Error("Valor inválido para gerar PIX");
  }

  const amountInCents = Math.round(valorNumero * 100);
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), DENTPEG_CHECKOUT_TIMEOUT_MS);

  try {
    const response = await fetch(DENTPEG_PUBLIC_CHECKOUT_URL, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Accept: "application/json"
      },
      body: JSON.stringify({ amountInCents }),
      signal: controller.signal
    });

    const data = await response.json().catch(() => ({}));

    if (!response.ok) {
      throw new Error(data?.error || data?.message || "Falha ao gerar PIX na DentPeg");
    }

    const qrCopyPaste = String(data?.pix?.qrCopyPaste || "").trim();

    if (!qrCopyPaste) {
      throw new Error("DentPeg não retornou a chave PIX");
    }

    return {
      pixCode: qrCopyPaste,
      pixId: String(data?.pix?.id || "").trim() || null,
      qrImageUrl: String(data?.pix?.qrImageUrl || "").trim() || null,
      expiration: String(data?.pix?.expiration || "").trim() || null,
      reused: Boolean(data?.reused)
    };
  } catch (error) {
    if (error?.name === "AbortError") {
      throw new Error("Tempo esgotado ao gerar PIX na DentPeg");
    }

    throw error;
  } finally {
    clearTimeout(timeout);
  }
}

async function listBackupFiles() {
  const entries = await fs.promises.readdir(BACKUPS_DIR, { withFileTypes: true });
  const files = await Promise.all(
    entries
      .filter((entry) => entry.isFile() && entry.name.toLowerCase().endsWith(".json"))
      .map(async (entry) => {
        const filePath = getBackupFilePath(entry.name);
        const stats = await fs.promises.stat(filePath);

        return {
          fileName: entry.name,
          size: stats.size,
          createdAt: stats.birthtime ? stats.birthtime.toISOString() : null,
          updatedAt: stats.mtime ? stats.mtime.toISOString() : null
        };
      })
  );

  return files.sort((a, b) => {
    const timeA = new Date(a.updatedAt || 0).getTime();
    const timeB = new Date(b.updatedAt || 0).getTime();
    return timeB - timeA;
  });
}

async function cleanupOldBackups() {
  if (!isPositiveNumber(BACKUP_RETENTION_DAYS)) {
    return { removed: 0 };
  }

  const retentionMs = BACKUP_RETENTION_DAYS * 24 * 60 * 60 * 1000;
  const threshold = Date.now() - retentionMs;
  const files = await listBackupFiles();
  let removed = 0;

  for (const file of files) {
    const updatedAtMs = new Date(file.updatedAt || 0).getTime();
    if (!updatedAtMs || updatedAtMs >= threshold) {
      continue;
    }

    await fs.promises.unlink(getBackupFilePath(file.fileName));
    removed += 1;
  }

  return { removed };
}

async function createDatabaseBackup(trigger = "automatic") {
  if (backupState.running) {
    return {
      ok: false,
      skipped: true,
      error: "Backup já está em execução"
    };
  }

  backupState.running = true;
  backupState.lastRunAt = new Date().toISOString();
  backupState.lastError = null;

  const startedAt = Date.now();

  try {
    const client = await pool.connect();

    try {
      await client.query("BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ");

      const [
        usuarios,
        depositos,
        admins,
        financialTransactions,
        ledgerEntries,
        auditLogs
      ] = await Promise.all([
        client.query("SELECT * FROM usuarios ORDER BY criado_em ASC NULLS LAST, id ASC"),
        client.query("SELECT * FROM depositos ORDER BY criado_em ASC NULLS LAST, id ASC"),
        client.query("SELECT * FROM admins ORDER BY criado_em ASC NULLS LAST, id ASC"),
        client.query(
          "SELECT * FROM financial_transactions ORDER BY created_at ASC NULLS LAST, id ASC"
        ),
        client.query("SELECT * FROM ledger_entries ORDER BY created_at ASC NULLS LAST, id ASC"),
        client.query("SELECT * FROM audit_logs ORDER BY created_at ASC NULLS LAST, id ASC")
      ]);

      await client.query("COMMIT");

      const now = new Date();
      const fileName = getBackupFileName(now);
      const filePath = getBackupFilePath(fileName);
      const payload = {
        meta: {
          generatedAt: now.toISOString(),
          trigger,
          version: 2,
          tables: {
            usuarios: usuarios.rowCount,
            depositos: depositos.rowCount,
            admins: admins.rowCount,
            financial_transactions: financialTransactions.rowCount,
            ledger_entries: ledgerEntries.rowCount,
            audit_logs: auditLogs.rowCount
          }
        },
        data: {
          usuarios: usuarios.rows,
          depositos: depositos.rows,
          admins: admins.rows,
          financial_transactions: financialTransactions.rows,
          ledger_entries: ledgerEntries.rows,
          audit_logs: auditLogs.rows
        }
      };

      await fs.promises.writeFile(filePath, JSON.stringify(payload, null, 2), "utf8");
      const cleanup = await cleanupOldBackups();
      const durationMs = Date.now() - startedAt;

      backupState.lastSuccessAt = now.toISOString();
      backupState.lastFile = fileName;
      backupState.lastDurationMs = durationMs;

      console.log(
        `[backup] concluído (${trigger}) arquivo=${fileName} usuarios=${usuarios.rowCount} depositos=${depositos.rowCount} admins=${admins.rowCount} tx=${financialTransactions.rowCount} ledger=${ledgerEntries.rowCount} audit=${auditLogs.rowCount} removidos=${cleanup.removed}`
      );

      return {
        ok: true,
        fileName,
        generatedAt: backupState.lastSuccessAt,
        durationMs,
        removedOldBackups: cleanup.removed,
        counts: payload.meta.tables
      };
    } catch (error) {
      await client.query("ROLLBACK");
      throw error;
    } finally {
      client.release();
    }
  } catch (error) {
    backupState.lastError = error.message || "Erro ao gerar backup";
    console.error("[backup] erro:", error);
    return {
      ok: false,
      error: backupState.lastError
    };
  } finally {
    backupState.running = false;
  }
}

function scheduleNextBackup(delayMs) {
  if (!BACKUP_ENABLED || !isPositiveNumber(BACKUP_INTERVAL_HOURS)) {
    console.log("[backup] automático desativado.");
    return;
  }

  if (backupState.timer) {
    clearTimeout(backupState.timer);
  }

  const safeDelayMs = Math.max(1000, delayMs);

  backupState.timer = setTimeout(async () => {
    await createDatabaseBackup("automatic");
    scheduleNextBackup(BACKUP_INTERVAL_HOURS * 60 * 60 * 1000);
  }, safeDelayMs);
}

function startBackupScheduler() {
  if (!BACKUP_ENABLED) {
    console.log("[backup] BACKUP_ENABLED=false, rotina automática não iniciada.");
    return;
  }

  if (!isPositiveNumber(BACKUP_INTERVAL_HOURS)) {
    console.log("[backup] intervalo inválido, rotina automática não iniciada.");
    return;
  }

  console.log(
    `[backup] rotina automática iniciada. Intervalo=${BACKUP_INTERVAL_HOURS}h retenção=${BACKUP_RETENTION_DAYS}d`
  );
  scheduleNextBackup(BACKUP_INITIAL_DELAY_MS);
}

async function getBackupStatus() {
  const files = await listBackupFiles();

  return {
    enabled: BACKUP_ENABLED,
    running: backupState.running,
    intervalHours: BACKUP_INTERVAL_HOURS,
    retentionDays: BACKUP_RETENTION_DAYS,
    initialDelayMs: BACKUP_INITIAL_DELAY_MS,
    lastRunAt: backupState.lastRunAt,
    lastSuccessAt: backupState.lastSuccessAt,
    lastDurationMs: backupState.lastDurationMs,
    lastFile: backupState.lastFile,
    lastError: backupState.lastError,
    totalBackups: files.length,
    latestBackups: files.slice(0, 10)
  };
}

function db(now = new Date()) {
  return now.toISOString();
}

function addSeconds(date, seconds) {
  const base = date instanceof Date ? date : new Date(date || Date.now());
  return new Date(base.getTime() + Math.max(0, Number(seconds || 0)) * 1000);
}

function isTimestampExpired(value, now = new Date()) {
  if (!value) return false;
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return false;
  return date.getTime() <= now.getTime();
}

function getUserDisplayName(user) {
  const nome = String(user?.nome || "").trim();

  if (nome) return nome;

  const email = String(user?.email || "").trim();
  return email ? email.split("@")[0] : "Usuario";
}

function getRequestDeviceId(req) {
  return String(req.headers["x-sigmo-device-id"] || "")
    .trim()
    .slice(0, 120);
}

function normalizeTransactionPin(value) {
  return String(value || "").replace(/\D/g, "").trim();
}

function isValidTransactionPin(value) {
  return /^\d{4}$/.test(normalizeTransactionPin(value));
}

function normalizeEmail(email) {
  return String(email || "").trim().toLowerCase();
}

function toMoney(value) {
  const num = Number(value || 0);
  if (!Number.isFinite(num)) return 0;
  return Number(num.toFixed(2));
}

function isContaBanida(user) {
  return String(user?.statusConta || "")
    .trim()
    .toLowerCase() === STATUS_CONTA_BANIDA;
}

function getMensagemContaBanida() {
  return "Conta banida permanentemente por tentativa de fraude. Esta acao e irreversivel e o saldo ficou congelado.";
}

function buildContaBanidaPayload(user, code = "ACCOUNT_BANNED") {
  return {
    error: getMensagemContaBanida(),
    code,
    statusConta: STATUS_CONTA_BANIDA,
    contaBanida: true,
    contaBanidaEm: user?.contaBanidaEm || null,
    motivoBanimento: user?.motivoBanimento || MOTIVO_BANIMENTO_FRAUDE_BONUS,
    saldo: toMoney(user?.saldo)
  };
}

function buildPixUnlockPayload(valorRecebidoViaPix = 0) {
  return {
    error:
      "Para desbloquear a transferencia via Pix e necessario ter recebido ao menos R$100,00 via Pix, sem contabilizar valores recebidos por transferencia Sigmo para Sigmo.",
    code: "PIX_UNLOCK_REQUIRED",
    pixDesbloqueado: false,
    valorRecebidoViaPix: toMoney(valorRecebidoViaPix),
    valorMinimoDesbloqueioPix: PIX_SAQUE_DESBLOQUEIO_MIN
  };
}

function normalizarNome(s) {
  return String(s || "")
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[^a-z0-9\s]/g, "")
    .replace(/\s+/g, " ")
    .trim();
}

function normalizarTextoLivre(s, maxLen = 1200) {
  return String(s || "")
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/\s+/g, " ")
    .trim()
    .slice(0, maxLen);
}

function sanitizeBotIdentifier(value, { allowColon = false } = {}) {
  const limpo = String(value || "")
    .trim()
    .replace(allowColon ? /[^A-Za-z0-9:_-]/g : /[^A-Za-z0-9_-]/g, "");

  return limpo || null;
}

function normalizarDataHoraBot(value) {
  return normalizarDataHoraLocal(value);
}

function parseDataHoraLocal(value) {
  if (!value) return null;

  const s = String(value).trim().replace(/\s+/g, " ");
  if (!s) return null;

  const normalizarMesTextoBR = (mes) =>
    String(mes || "")
      .toLowerCase()
      .normalize("NFD")
      .replace(/[\u0300-\u036f]/g, "")
      .replace(/[^a-z]/g, "");

  const mapMesTextoBR = {
    jan: 1,
    janeiro: 1,
    fev: 2,
    fevereiro: 2,
    mar: 3,
    marco: 3,
    abril: 4,
    abr: 4,
    mai: 5,
    maio: 5,
    jun: 6,
    junho: 6,
    jul: 7,
    julho: 7,
    ago: 8,
    agosto: 8,
    set: 9,
    setembro: 9,
    out: 10,
    outubro: 10,
    nov: 11,
    novembro: 11,
    dez: 12,
    dezembro: 12
  };

  let match = s.match(
    /(\d{2})\/(\d{2})\/(\d{4})\s*(?:[^\d]{1,20})?\s*(\d{2}):(\d{2})(?::(\d{2}))?/
  );

  if (match) {
    const [, day, month, year, hour, minute, secondBruto] = match;
    return {
      year: Number(year),
      month: Number(month),
      day: Number(day),
      hour: Number(hour),
      minute: Number(minute),
      second: Number(secondBruto || "0")
    };
  }

  match = s.match(
    /(\d{1,2})[\/\-. ]([A-Za-zÀ-ÿ.]{3,15})[\/\-. ](\d{4})\s*(?:[^\d]{1,20})?\s*(\d{2}):(\d{2})(?::(\d{2}))?/i
  );

  if (match) {
    const [, day, monthText, year, hour, minute, secondBruto] = match;
    const month = mapMesTextoBR[normalizarMesTextoBR(monthText)];

    if (month) {
      return {
        year: Number(year),
        month,
        day: Number(day),
        hour: Number(hour),
        minute: Number(minute),
        second: Number(secondBruto || "0")
      };
    }
  }

  match = s.match(
    /(\d{4})-(\d{2})-(\d{2})[T\s](\d{2}):(\d{2})(?::(\d{2}))?/
  );

  if (match) {
    const [, year, month, day, hour, minute, secondBruto] = match;
    return {
      year: Number(year),
      month: Number(month),
      day: Number(day),
      hour: Number(hour),
      minute: Number(minute),
      second: Number(secondBruto || "0")
    };
  }

  return null;
}

function normalizarDataHoraLocal(value) {
  const partes = parseDataHoraLocal(value);
  if (!partes) return null;

  const pad = (numero) => String(numero).padStart(2, "0");

  return `${partes.year}-${pad(partes.month)}-${pad(partes.day)} ${pad(
    partes.hour
  )}:${pad(partes.minute)}:${pad(partes.second)}`;
}

function normalizarDataLocal(value) {
  const partes = parseDataHoraLocal(value);
  if (!partes) return null;

  const pad = (numero) => String(numero).padStart(2, "0");
  return `${partes.year}-${pad(partes.month)}-${pad(partes.day)}`;
}

function toEpochLocal(value) {
  const partes =
    value && typeof value === "object" && "year" in value
      ? value
      : parseDataHoraLocal(value);

  if (!partes) return NaN;

  return Date.UTC(
    partes.year,
    partes.month - 1,
    partes.day,
    partes.hour,
    partes.minute,
    partes.second || 0
  );
}

function extrairDatasHorasDoComprovante(texto) {
  const bruto = String(texto || "").replace(/\r/g, "\n");
  const resultados = [];
  const vistos = new Set();
  const regexes = [
    /(\d{2}\/\d{2}\/\d{4})\s*(?:[^\d]{1,20})?\s*(\d{2}:\d{2}(?::\d{2})?)/g,
    /(\d{1,2}[\/\-. ][A-Za-zÀ-ÿ.]{3,15}[\/\-. ]\d{4})\s*(?:[^\d]{1,20})?\s*(\d{2}:\d{2}(?::\d{2})?)/gi,
    /(\d{4}-\d{2}-\d{2})\s*(?:[^\d]{1,20})?\s*(\d{2}:\d{2}(?::\d{2})?)/g
  ];

  for (const regex of regexes) {
    for (const match of bruto.matchAll(regex)) {
      const normalizada = normalizarDataHoraLocal(`${match[1]} ${match[2]}`);
      if (!normalizada || vistos.has(normalizada)) continue;

      vistos.add(normalizada);
      resultados.push(normalizada);
    }
  }

  return resultados;
}

function extrairDatasDoComprovante(texto) {
  const bruto = String(texto || "").replace(/\r/g, "\n");
  const resultados = [];
  const vistos = new Set();
  const regexes = [
    /(\d{2}\/\d{2}\/\d{4})/g,
    /(\d{1,2}[\/\-. ][A-Za-zÀ-ÿ.]{3,15}[\/\-. ]\d{4})/gi,
    /(\d{4}-\d{2}-\d{2})/g
  ];

  for (const regex of regexes) {
    for (const match of bruto.matchAll(regex)) {
      const dataBruta = String(match[1] || "").trim();
      const normalizada = dataBruta.includes("/")
        ? normalizarDataLocal(`${dataBruta} 00:00:00`)
        : normalizarDataLocal(`${dataBruta} 00:00:00`);

      if (!normalizada || vistos.has(normalizada)) continue;

      vistos.add(normalizada);
      resultados.push(normalizada);
    }
  }

  return resultados;
}

function toEpochLegacy(value) {
  if (!value) return NaN;

  if (value instanceof Date) return value.getTime();
  if (typeof value === "number") return value;

  const s = String(value).trim();

  // já tem timezone (Z ou -03:00)
  if (/[zZ]|[+\-]\d{2}:\d{2}$/.test(s)) {
    return Date.parse(s);
  }

  // força UTC
  return Date.parse(s.replace(" ", "T") + "Z");
}

function toEpoch(value) {
  if (!value) return NaN;

  if (value instanceof Date) return value.getTime();
  if (typeof value === "number") return value;

  const normalizadoBot = normalizarDataHoraBot(value);
  if (normalizadoBot) {
    return toEpochLocal(normalizadoBot);
  }

  const s = String(value).trim();
  return Date.parse(s.includes("T") ? s : s.replace(" ", "T"));
}

function buildDentpegEventFingerprint({
  txid,
  idTransacao,
  cardKey,
  fallbackKey,
  valorLiquido,
  nomePagador,
  dataHora,
  raw
}) {
  const txidNormalizado = sanitizeBotIdentifier(txid);
  if (txidNormalizado) {
    return `txid:${txidNormalizado}`;
  }

  const idTransacaoNormalizado = sanitizeBotIdentifier(idTransacao);
  if (idTransacaoNormalizado) {
    return `id:${idTransacaoNormalizado}`;
  }

  const cardKeyNormalizado = sanitizeBotIdentifier(cardKey, { allowColon: true });
  if (cardKeyNormalizado) {
    return `card:${cardKeyNormalizado}`;
  }

  const dataHoraNormalizada =
    normalizarDataHoraLocal(dataHora) || normalizarDataLocal(dataHora) || "";
  const valorNormalizado = Number(toMoney(valorLiquido || 0)).toFixed(2);
  const nomeNormalizado = normalizarNome(nomePagador || "");
  const fallbackNormalizado = sanitizeBotIdentifier(fallbackKey) || "";
  const rawNormalizado = normalizarTextoLivre(raw || "");

  if (
    !dataHoraNormalizada &&
    !nomeNormalizado &&
    valorNormalizado === "0.00" &&
    !rawNormalizado &&
    fallbackNormalizado
  ) {
    return `fallback:${fallbackNormalizado}`;
  }

  const base = [
    dataHoraNormalizada,
    valorNormalizado,
    nomeNormalizado,
    rawNormalizado
  ].join("|");

  return `hash:${crypto.createHash("sha1").update(base).digest("hex")}`;
}

function buildDentpegDuplicateSearchParams({
  referenceKey,
  txid,
  idTransacao,
  cardKey,
  fallbackKey,
  eventFingerprint
}) {
  const normalized = {
    referenceKey: String(referenceKey || "").trim() || null,
    txid: sanitizeBotIdentifier(txid),
    idTransacao: sanitizeBotIdentifier(idTransacao),
    cardKey: sanitizeBotIdentifier(cardKey, { allowColon: true }),
    fallbackKey: sanitizeBotIdentifier(fallbackKey),
    eventFingerprint: String(eventFingerprint || "").trim() || null
  };

  const clauses = [];
  const params = [];

  const push = (sql, value) => {
    if (!value) return;
    params.push(value);
    clauses.push(sql.replace("?", `$${params.length}`));
  };

  push("reference_key = ?", normalized.referenceKey);
  push("metadata->>'txid' = ?", normalized.txid);
  push("metadata->>'idTransacao' = ?", normalized.idTransacao);
  push("metadata->>'cardKey' = ?", normalized.cardKey);
  push("metadata->>'fallbackKey' = ?", normalized.fallbackKey);
  push("metadata->>'eventFingerprint' = ?", normalized.eventFingerprint);

  return {
    normalized,
    clauses,
    params
  };
}

function buildDentpegEventFingerprintFromTransaction(tx) {
  if (!tx) return null;

  const metadata = tx.metadata || {};

  return buildDentpegEventFingerprint({
    txid: metadata.txid,
    idTransacao: metadata.idTransacao,
    cardKey: metadata.cardKey,
    fallbackKey: metadata.fallbackKey,
    valorLiquido: metadata.valorLiquidoBot,
    nomePagador: metadata.nomePagador,
    dataHora: metadata.dataHoraBot,
    raw: metadata.raw
  });
}

async function findExistingDentpegTransactionByEvent(
  client,
  {
    referenceKey,
    txid,
    idTransacao,
    cardKey,
    fallbackKey,
    eventFingerprint
  }
) {
  const search = buildDentpegDuplicateSearchParams({
    referenceKey,
    txid,
    idTransacao,
    cardKey,
    fallbackKey,
    eventFingerprint
  });

  if (search.clauses.length > 0) {
    const direct = await client.query(
      `
      SELECT *
      FROM financial_transactions
      WHERE source_type = 'dentpeg'
        AND (${search.clauses.join(" OR ")})
      ORDER BY created_at DESC NULLS LAST
      LIMIT 1
      `,
      search.params
    );

    if (direct.rows.length > 0) {
      return mapFinancialTransaction(direct.rows[0]);
    }
  }

  if (!search.normalized.eventFingerprint) {
    return null;
  }

  const recentes = await client.query(
    `
    SELECT *
    FROM financial_transactions
    WHERE source_type = 'dentpeg'
    ORDER BY created_at DESC NULLS LAST
    LIMIT 2000
    `
  );

  for (const row of recentes.rows) {
    const tx = mapFinancialTransaction(row);
    if (buildDentpegEventFingerprintFromTransaction(tx) === search.normalized.eventFingerprint) {
      return tx;
    }
  }

  return null;
}

function normalizarTextoPdfExtraido(texto) {
  return String(texto || "")
    .replace(/\u0000/g, "")
    .replace(/[^\x09\x0A\x0D\x20-\x7E\u00A0-\u024F\u20A0-\u20CF]/g, " ")
    .replace(/[^\S\r\n]+/g, " ")
    .replace(/[ \t]+\n/g, "\n")
    .replace(/\n{3,}/g, "\n\n")
    .trim();
}

function textoPdfPareceValido(texto) {
  const limpo = normalizarTextoPdfExtraido(texto);
  if (limpo.length < 12) return false;

  const total = limpo.length;
  const legiveis =
    (limpo.match(/[A-Za-zÀ-ÿ0-9\s.,:/\-@()$%#]/g) || []).length / total;
  const estranhos =
    (limpo.match(/[□�]/g) || []).length / total;

  return legiveis >= 0.75 && estranhos <= 0.02;
}

async function extrairTextoPdfComParser(caminho) {
  const parser = new PDFParse({ data: fs.readFileSync(caminho) });

  try {
    const result = await parser.getText();
    return normalizarTextoPdfExtraido(result?.text || "");
  } finally {
    await parser.destroy().catch(() => {});
  }
}

async function extrairTextoPdfViaOcr(caminho) {
  const parser = new PDFParse({ data: fs.readFileSync(caminho) });

  try {
    const screenshots = await parser.getScreenshot({
      first: 1,
      desiredWidth: 1800,
      imageDataUrl: false,
      imageBuffer: true
    });

    const paginas = Array.isArray(screenshots?.pages) ? screenshots.pages : [];
    const textos = [];

    for (const pagina of paginas) {
      const imagem = pagina?.data;
      if (!imagem) continue;

      const result = await Tesseract.recognize(imagem, "por+eng");
      const texto = limparTextoComprovante(result?.data?.text || "");
      if (texto) textos.push(texto);
    }

    return limparTextoComprovante(textos.join("\n\n"));
  } finally {
    await parser.destroy().catch(() => {});
  }
}

async function extrairTextoComprovante(caminho, mimetype = "") {
  try {
    const isPdf =
      String(mimetype || "").toLowerCase() === "application/pdf" ||
      path.extname(caminho).toLowerCase() === ".pdf";

    if (isPdf) {
      const textoPdf = await extrairTextoPdfComParser(caminho);
      if (textoPdfPareceValido(textoPdf)) {
        return limparTextoComprovante(textoPdf);
      }

      console.log("⚠️ Texto do PDF inválido; tentando OCR da página renderizada");

      const textoOcrPdf = await extrairTextoPdfViaOcr(caminho);
      if (textoPdfPareceValido(textoOcrPdf)) {
        return limparTextoComprovante(textoOcrPdf);
      }

      return limparTextoComprovante(textoOcrPdf || textoPdf || "");
    }

    const result = await Tesseract.recognize(caminho, "por+eng");
    return limparTextoComprovante(result.data.text || "");
  } catch (e) {
    console.log("❌ OCR erro:", e.message);
    return "";
  }
}

function limparTextoComprovante(texto) {
  return String(texto || "")
    .replace(/\u0000/g, "")
    .replace(/\r/g, "")
    .replace(/[ \t]+\n/g, "\n")
    .replace(/\n{3,}/g, "\n\n")
    .trim();
}

async function extrairTextoComprovanteLegacy(caminho) {
  try {
    const result = await Tesseract.recognize(caminho, "por+eng");
    return result.data.text || "";
  } catch (e) {
    console.log("❌ OCR erro:", e.message);
    return "";
  }
}



function calcularValorCreditadoDeposito(valorBruto) {
  const v = toMoney(valorBruto);

  if (!Number.isFinite(v) || v <= 0) return 0;

  return v;
}

function calcularDetalhesSaquePix(valorSolicitado, repassarTaxa = false) {
  const valor = toMoney(valorSolicitado);
  const repassar = Boolean(repassarTaxa);

  if (!Number.isFinite(valor) || valor <= 0) {
    return {
      valorSolicitado: 0,
      taxa: 0,
      valorLiquido: 0,
      valorDebitado: 0,
      repassarTaxa: repassar
    };
  }

  const taxa = toMoney(valor * TAXA_SAQUE_PIX_PERCENTUAL);
  const valorLiquido = repassar
    ? Math.max(0, toMoney(valor - taxa))
    : valor;
  const valorDebitado = repassar
    ? valor
    : toMoney(valor + taxa);

  return {
    valorSolicitado: valor,
    taxa,
    valorLiquido,
    valorDebitado,
    repassarTaxa: repassar
  };
}


function buildId(prefix) {
  return `${prefix}_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`;
}

function getRequestIp(req) {
  return (
    req.headers["x-forwarded-for"]?.toString().split(",")[0].trim() ||
    req.ip ||
    req.socket?.remoteAddress ||
    ""
  );
}

function calcularLiquidoDentpeg(valor) {
  const v = Number(valor);

  if (!Number.isFinite(v) || v <= 0) return 0;

  if (v <= 99) {
    return Number((v - 2).toFixed(2));
  }

  const taxaMax = v * 0.019 + 0.99;
  const taxaMin = v * 0.0079 + 0.99;

  return {
    min: Number((v - taxaMax).toFixed(2)),
    max: Number((v - taxaMin).toFixed(2))
  };
}

async function runInTransaction(workFn) {
  const client = await pool.connect();

  try {
    await client.query("BEGIN");
    const result = await workFn(client);
    await client.query("COMMIT");
    return result;
  } catch (error) {
    await client.query("ROLLBACK");
    throw error;
  } finally {
    client.release();
  }
}

async function ensureColumn(table, column, definition) {
  const check = await pool.query(
    `
    SELECT 1
    FROM information_schema.columns
    WHERE table_name = $1 AND column_name = $2
    LIMIT 1
    `,
    [table, column]
  );

  if (check.rows.length === 0) {
    await pool.query(`ALTER TABLE ${table} ADD COLUMN ${column} ${definition}`);
  }
}

async function ensureIndex(indexName, sql) {
  const check = await pool.query(
    `
    SELECT 1
    FROM pg_indexes
    WHERE schemaname = 'public' AND indexname = $1
    LIMIT 1
    `,
    [indexName]
  );

  if (check.rows.length === 0) {
    await pool.query(sql);
  }
}

async function initDB() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS usuarios (
      id TEXT PRIMARY KEY,
      nome TEXT,
      email TEXT UNIQUE NOT NULL,
      senha TEXT NOT NULL,
      saldo NUMERIC DEFAULT 0,
      criado_em TIMESTAMP,
      nome_atualizado_em TIMESTAMP,
      saldo_atualizado_em TIMESTAMP,
      senha_atualizada_em TIMESTAMP
    );
  `);

  await ensureColumn(
    "usuarios",
    "status_conta",
    `TEXT DEFAULT '${STATUS_CONTA_ATIVA}'`
  );
  await ensureColumn("usuarios", "conta_banida_em", "TIMESTAMP");
  await ensureColumn("usuarios", "motivo_banimento", "TEXT DEFAULT ''");
  await ensureColumn("usuarios", "bonus_boas_vindas", "NUMERIC DEFAULT 0");
  await ensureColumn("usuarios", "bonus_boas_vindas_concedido_em", "TIMESTAMP");
  await ensureColumn("usuarios", "pin_transacao_hash", "TEXT DEFAULT ''");
  await ensureColumn("usuarios", "pin_transacao_atualizado_em", "TIMESTAMP");

  await pool.query(`
    CREATE TABLE IF NOT EXISTS depositos (
      id TEXT PRIMARY KEY,
      user_id TEXT NOT NULL,
      valor NUMERIC DEFAULT 0,
      status TEXT DEFAULT 'pendente',
      tipo_transacao TEXT DEFAULT 'entrada',
      comprovante_url TEXT DEFAULT '',
      criado_em TIMESTAMP
    );
  `);

  await ensureColumn("depositos", "chave_pix", "TEXT DEFAULT ''");
  await ensureColumn("depositos", "tipo_chave", "TEXT DEFAULT ''");
  await ensureColumn("depositos", "descricao", "TEXT DEFAULT ''");
await ensureColumn("depositos", "comprovante_texto", "TEXT DEFAULT ''");
  await ensureColumn("depositos", "repassar_taxa", "BOOLEAN DEFAULT false");
  await ensureColumn("depositos", "taxa_pix", "NUMERIC DEFAULT 0");
  await ensureColumn("depositos", "valor_liquido_pix", "NUMERIC DEFAULT 0");
  await ensureColumn("depositos", "valor_debitado_pix", "NUMERIC DEFAULT 0");
  await ensureColumn("depositos", "aprovado_em", "TIMESTAMP");
  await ensureColumn("depositos", "recusado_em", "TIMESTAMP");
  await ensureColumn("depositos", "comprovante_enviado_em", "TIMESTAMP");

  await pool.query(`
    CREATE TABLE IF NOT EXISTS admins (
      id TEXT PRIMARY KEY,
      email TEXT UNIQUE NOT NULL,
      senha TEXT NOT NULL,
      nome TEXT DEFAULT '',
      role TEXT DEFAULT 'admin',
      ativo BOOLEAN DEFAULT true,
      criado_em TIMESTAMP,
      ultimo_login_em TIMESTAMP
    );
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS financial_transactions (
      id TEXT PRIMARY KEY,
      user_id TEXT NOT NULL,
      reference_key TEXT UNIQUE NOT NULL,
      source_type TEXT NOT NULL,
      source_id TEXT NOT NULL,
      operation_type TEXT NOT NULL,
      direction TEXT NOT NULL,
      amount NUMERIC NOT NULL DEFAULT 0,
      status TEXT NOT NULL DEFAULT 'completed',
      description TEXT DEFAULT '',
      metadata JSONB DEFAULT '{}'::jsonb,
      created_at TIMESTAMP,
      updated_at TIMESTAMP
    );
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS ledger_entries (
      id TEXT PRIMARY KEY,
      user_id TEXT NOT NULL,
      financial_transaction_id TEXT NOT NULL,
      entry_type TEXT NOT NULL,
      amount NUMERIC NOT NULL DEFAULT 0,
      balance_before NUMERIC NOT NULL DEFAULT 0,
      balance_after NUMERIC NOT NULL DEFAULT 0,
      description TEXT DEFAULT '',
      metadata JSONB DEFAULT '{}'::jsonb,
      created_at TIMESTAMP
    );
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS audit_logs (
      id TEXT PRIMARY KEY,
      admin_id TEXT,
      action TEXT NOT NULL,
      target_type TEXT NOT NULL,
      target_id TEXT NOT NULL,
      details JSONB DEFAULT '{}'::jsonb,
      ip_address TEXT DEFAULT '',
      created_at TIMESTAMP
    );
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS nfc_receive_sessions (
      id TEXT PRIMARY KEY,
      public_token TEXT UNIQUE NOT NULL,
      receiver_user_id TEXT NOT NULL,
      status TEXT NOT NULL DEFAULT 'pending',
      nonce TEXT NOT NULL,
      protocol_version INTEGER NOT NULL DEFAULT 1,
      expires_at TIMESTAMP NOT NULL,
      consumed_at TIMESTAMP,
      cancelled_at TIMESTAMP,
      payer_user_id TEXT DEFAULT '',
      amount NUMERIC DEFAULT 0,
      financial_transaction_id TEXT DEFAULT '',
      read_count INTEGER DEFAULT 0,
      last_read_at TIMESTAMP,
      metadata JSONB DEFAULT '{}'::jsonb,
      created_at TIMESTAMP,
      updated_at TIMESTAMP
    );
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS sigmo_tap_charges (
      id TEXT PRIMARY KEY,
      public_code TEXT UNIQUE NOT NULL,
      receiver_user_id TEXT NOT NULL,
      status TEXT NOT NULL DEFAULT 'pending',
      amount NUMERIC NOT NULL DEFAULT 0,
      description TEXT DEFAULT '',
      expires_at TIMESTAMP NOT NULL,
      nfc_session_id TEXT DEFAULT '',
      payer_user_id TEXT DEFAULT '',
      financial_transaction_id TEXT DEFAULT '',
      paid_at TIMESTAMP,
      cancelled_at TIMESTAMP,
      metadata JSONB DEFAULT '{}'::jsonb,
      created_at TIMESTAMP,
      updated_at TIMESTAMP
    );
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS sigmo_cards (
      id TEXT PRIMARY KEY,
      owner_user_id TEXT NOT NULL,
      holder_user_id TEXT NOT NULL,
      card_type TEXT NOT NULL DEFAULT 'primary',
      label TEXT DEFAULT '',
      status TEXT NOT NULL DEFAULT 'active',
      spending_limit NUMERIC NOT NULL DEFAULT 0,
      device_id TEXT DEFAULT '',
      claim_token TEXT DEFAULT '',
      bound_at TIMESTAMP,
      last_used_at TIMESTAMP,
      metadata JSONB DEFAULT '{}'::jsonb,
      created_at TIMESTAMP,
      updated_at TIMESTAMP
    );
  `);

  await ensureIndex(
    "idx_financial_transactions_user_id",
    "CREATE INDEX idx_financial_transactions_user_id ON financial_transactions (user_id)"
  );

  await ensureIndex(
    "idx_ledger_entries_user_id",
    "CREATE INDEX idx_ledger_entries_user_id ON ledger_entries (user_id)"
  );

  await ensureIndex(
    "idx_audit_logs_admin_id",
    "CREATE INDEX idx_audit_logs_admin_id ON audit_logs (admin_id)"
  );

  await ensureIndex(
    "idx_nfc_receive_sessions_receiver_user_id",
    "CREATE INDEX idx_nfc_receive_sessions_receiver_user_id ON nfc_receive_sessions (receiver_user_id)"
  );

  await ensureIndex(
    "idx_nfc_receive_sessions_status",
    "CREATE INDEX idx_nfc_receive_sessions_status ON nfc_receive_sessions (status)"
  );

  await ensureIndex(
    "idx_sigmo_tap_charges_receiver_user_id",
    "CREATE INDEX idx_sigmo_tap_charges_receiver_user_id ON sigmo_tap_charges (receiver_user_id)"
  );

  await ensureIndex(
    "idx_sigmo_tap_charges_status",
    "CREATE INDEX idx_sigmo_tap_charges_status ON sigmo_tap_charges (status)"
  );

  await ensureIndex(
    "idx_sigmo_cards_owner_user_id",
    "CREATE INDEX idx_sigmo_cards_owner_user_id ON sigmo_cards (owner_user_id)"
  );

  await ensureIndex(
    "idx_sigmo_cards_holder_user_id",
    "CREATE INDEX idx_sigmo_cards_holder_user_id ON sigmo_cards (holder_user_id)"
  );

  await ensureIndex(
    "idx_sigmo_cards_device_id",
    "CREATE INDEX idx_sigmo_cards_device_id ON sigmo_cards (device_id)"
  );

  await ensureAdmin();
  console.log("Banco pronto");
}

async function ensureAdmin() {
  if (!ADMIN_EMAIL || !ADMIN_PASSWORD) {
    console.log("ADMIN_EMAIL ou ADMIN_PASSWORD ausentes.");
    return;
  }

  const existing = await pool.query(
    "SELECT * FROM admins WHERE email = $1 LIMIT 1",
    [ADMIN_EMAIL]
  );

  const hash = await bcrypt.hash(ADMIN_PASSWORD, 10);

  if (existing.rows.length === 0) {
    await pool.query(
      `
      INSERT INTO admins (
        id, email, senha, nome, role, ativo, criado_em, ultimo_login_em
      )
      VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
      `,
      [
        buildId("admin"),
        ADMIN_EMAIL,
        hash,
        "Administrador",
        "admin",
        true,
        db(),
        null
      ]
    );
    console.log("Admin criado");
  } else {
    await pool.query(
      `
      UPDATE admins
      SET senha = $1,
          nome = $2,
          role = $3,
          ativo = $4
      WHERE email = $5
      `,
      [hash, "Administrador", "admin", true, ADMIN_EMAIL]
    );
    console.log("Admin sincronizado");
  }
}

app.use(helmet());
app.use(express.json());

app.use(
  rateLimit({
    windowMs: 60 * 1000,
    max: 120,
    standardHeaders: true,
    legacyHeaders: false
  })
);

const loginLimiter = rateLimit({
  windowMs: 60 * 1000,
  max: 8,
  standardHeaders: true,
  legacyHeaders: false,
  message: { error: "Muitas tentativas. Aguarde 1 minuto." }
});

const adminLoginLimiter = rateLimit({
  windowMs: 60 * 1000,
  max: 5,
  standardHeaders: true,
  legacyHeaders: false,
  message: { error: "Muitas tentativas admin. Aguarde 1 minuto." }
});

const allowedOrigins = [
  /^https:\/\/.*\.netlify\.app$/,
"https://sigmopay.com",
  "https://www.sigmopay.com",
  "http://localhost:3000",
  "http://127.0.0.1:3000",
  "http://localhost:5173",
  "http://127.0.0.1:5173"
];

app.use(
  cors({
    origin(origin, callback) {
      if (!origin) return callback(null, true);

      const permitido = allowedOrigins.some((item) => {
        if (item instanceof RegExp) return item.test(origin);
        return item === origin;
      });

      if (permitido) return callback(null, true);

      return callback(new Error("Origem não permitida pelo CORS"));
    },
    methods: ["GET", "POST", "OPTIONS"],
    allowedHeaders: ["Content-Type", "Authorization"]
  })
);

app.options("*", cors());

const UPLOADS_DIR = path.join(__dirname, "uploads");

if (!fs.existsSync(UPLOADS_DIR)) {
  fs.mkdirSync(UPLOADS_DIR, { recursive: true });
}

app.use("/uploads", express.static(UPLOADS_DIR));

const storage = multer.diskStorage({
  destination: (_, __, cb) => cb(null, UPLOADS_DIR),
  filename: (_, file, cb) => {
    const ext = path.extname(file.originalname || "");
    cb(null, `comp_${Date.now()}${ext}`);
  }
});

const upload = multer({
  storage,
  limits: { fileSize: 8 * 1024 * 1024 },
  fileFilter: (_, file, cb) => {
    const allowed = ["image/png", "image/jpeg", "image/jpg", "application/pdf"];
    if (!allowed.includes(file.mimetype)) {
      return cb(new Error("Formato inválido"));
    }
    cb(null, true);
  }
});

function signToken(admin) {
  return jwt.sign(
    {
      sub: admin.id,
      email: admin.email,
      role: admin.role || "admin",
      type: "admin"
    },
    JWT_SECRET,
    { expiresIn: "12h" }
  );
}

function signUserToken(user) {
  return jwt.sign(
    {
      sub: user.id,
      email: user.email,
      nome: getUserDisplayName(user),
      type: "user"
    },
    JWT_SECRET,
    { expiresIn: USER_MOBILE_TOKEN_TTL }
  );
}

function mapUser(row) {
  if (!row) return null;

  return {
    id: row.id,
    nome: row.nome || row.email?.split("@")[0] || "",
    email: row.email,
    senha: row.senha,
    saldo: toMoney(row.saldo),
    criadoEm: row.criado_em || null,
    nomeAtualizadoEm: row.nome_atualizado_em || null,
    saldoAtualizadoEm: row.saldo_atualizado_em || null,
    senhaAtualizadaEm: row.senha_atualizada_em || null,
    statusConta: row.status_conta || STATUS_CONTA_ATIVA,
    contaBanidaEm: row.conta_banida_em || null,
    motivoBanimento: row.motivo_banimento || "",
    bonusBoasVindas: toMoney(row.bonus_boas_vindas),
    bonusBoasVindasConcedidoEm: row.bonus_boas_vindas_concedido_em || null,
    pinTransacaoHash: row.pin_transacao_hash || "",
    pinTransacaoAtualizadoEm: row.pin_transacao_atualizado_em || null
  };
}

function buildUserPublicResponse(user, extras = {}) {
  return {
    id: user.id,
    nome: user.nome,
    email: user.email,
    saldo: toMoney(user.saldo),
    criadoEm: user.criadoEm || null,
    statusConta: user.statusConta || STATUS_CONTA_ATIVA,
    contaBanida: isContaBanida(user),
    contaBanidaEm: user.contaBanidaEm || null,
    motivoBanimento: user.motivoBanimento || "",
    bonusBoasVindas: toMoney(user.bonusBoasVindas),
    bonusBoasVindasConcedidoEm: user.bonusBoasVindasConcedidoEm || null,
    ...extras
  };
}

async function buildUserPublicResponseWithPix(user, client = pool, extras = {}) {
  const valorRecebidoViaPix = await getValorRecebidoViaPix(user.id, client);
  const deviceId = String(extras.deviceId || "").trim();
  const activeCard =
    Object.prototype.hasOwnProperty.call(extras, "activeCard")
      ? extras.activeCard
      : await buildUserActiveCardResponse(user, deviceId, client);
  const extraPayload = { ...extras };
  delete extraPayload.deviceId;
  delete extraPayload.activeCard;

  return buildUserPublicResponse(user, {
    pixDesbloqueado: valorRecebidoViaPix >= PIX_SAQUE_DESBLOQUEIO_MIN,
    valorRecebidoViaPix,
    valorMinimoDesbloqueioPix: PIX_SAQUE_DESBLOQUEIO_MIN,
    activeCard,
    ...extraPayload
  });
}

function buildUserMobileAuthResponse(user, token, extras = {}) {
  return {
    token,
    tokenType: "Bearer",
    tokenTtl: USER_MOBILE_TOKEN_TTL,
    user,
    ...extras
  };
}

function buildNfcReceiveSessionPayload(session) {
  return JSON.stringify({
    v: Number(session?.protocolVersion || NFC_PROTOCOL_VERSION),
    t: String(session?.publicToken || ""),
    n: String(session?.nonce || "")
  });
}

function parseNfcReceiveSessionPayload(payload) {
  if (typeof payload === "object" && payload) {
    return {
      version: Number(payload.v || payload.version || NFC_PROTOCOL_VERSION),
      publicToken: String(payload.t || payload.publicToken || "").trim(),
      nonce: String(payload.n || payload.nonce || "").trim()
    };
  }

  if (!String(payload || "").trim()) {
    return {
      version: NFC_PROTOCOL_VERSION,
      publicToken: "",
      nonce: ""
    };
  }

  try {
    const parsed = JSON.parse(String(payload));
    return parseNfcReceiveSessionPayload(parsed);
  } catch {
    return {
      version: NFC_PROTOCOL_VERSION,
      publicToken: "",
      nonce: ""
    };
  }
}

function buildNfcReceiveSessionResponse(session, receiver, extras = {}) {
  return {
    id: session.id,
    status: normalizeNfcSessionStatus(session.status),
    protocolVersion: Number(session.protocolVersion || NFC_PROTOCOL_VERSION),
    publicToken: session.publicToken,
    nonce: session.nonce,
    payload: buildNfcReceiveSessionPayload(session),
    expiresAt: session.expiresAt,
    consumedAt: session.consumedAt || null,
    cancelledAt: session.cancelledAt || null,
    amount: toMoney(session.amount),
    readCount: Number(session.readCount || 0),
    lastReadAt: session.lastReadAt || null,
    receiver: receiver
      ? {
          id: receiver.id,
          nome: getUserDisplayName(receiver),
          email: receiver.email
        }
      : null,
    ...extras
  };
}

function buildSigmoTapChargeAppLink(charge) {
  const chargeId = encodeURIComponent(String(charge?.id || "").trim());
  return `${SIGMO_APP_TAP_RECEIVE_SCHEME}?chargeId=${chargeId}`;
}

function buildSigmoTapChargeResponse(charge, receiver, extras = {}) {
  return {
    id: charge.id,
    publicCode: charge.publicCode,
    status: normalizeSigmoTapChargeStatus(charge.status),
    amount: toMoney(charge.amount),
    description: charge.description || "",
    expiresAt: charge.expiresAt || null,
    paidAt: charge.paidAt || null,
    cancelledAt: charge.cancelledAt || null,
    appLink: buildSigmoTapChargeAppLink(charge),
    receiver: receiver
      ? {
          id: receiver.id,
          nome: getUserDisplayName(receiver),
          email: receiver.email
        }
      : null,
    ...extras
  };
}

function normalizeSigmoCardType(cardType) {
  const normalized = String(cardType || "").trim().toLowerCase();
  return normalized === "additional" ? "additional" : "primary";
}

function normalizeSigmoCardStatus(status) {
  const normalized = String(status || "").trim().toLowerCase();
  return ["active", "blocked"].includes(normalized) ? normalized : "active";
}

function buildSigmoCardClaimAppLink(card) {
  const cardId = encodeURIComponent(String(card?.id || "").trim());
  const claimToken = encodeURIComponent(String(card?.claimToken || "").trim());
  return `${SIGMO_APP_CARD_CLAIM_SCHEME}?cardId=${cardId}&claimToken=${claimToken}`;
}

function buildSigmoCardResponse(card, owner, holder, extras = {}) {
  const spendingLimit = toMoney(card?.spendingLimit);
  const ownerBalance = toMoney(owner?.saldo);
  const availableToSpend = Math.max(0, Math.min(spendingLimit, ownerBalance));

  return {
    id: card.id,
    ownerUserId: card.ownerUserId,
    holderUserId: card.holderUserId,
    cardType: normalizeSigmoCardType(card.cardType),
    label: String(card.label || "").trim() || "Cartao Sigmo",
    status: normalizeSigmoCardStatus(card.status),
    spendingLimit,
    availableToSpend,
    deviceBound: Boolean(String(card.deviceId || "").trim()),
    boundAt: card.boundAt || null,
    lastUsedAt: card.lastUsedAt || null,
    appLink: buildSigmoCardClaimAppLink(card),
    owner: owner
      ? {
          id: owner.id,
          nome: getUserDisplayName(owner),
          email: owner.email
        }
      : null,
    holder: holder
      ? {
          id: holder.id,
          nome: getUserDisplayName(holder),
          email: holder.email
        }
      : null,
    ...extras
  };
}

function sendJsonError(res, statusCode, code, error, extras = {}) {
  return res.status(statusCode).json({
    code,
    error,
    ...extras
  });
}

function mapDeposito(row) {
  if (!row) return null;

  return {
    id: row.id,
    userId: row.user_id,
    valor: toMoney(row.valor),
    chavePix: row.chave_pix || "",
    tipoChave: row.tipo_chave || "",
    tipoTransacao: row.tipo_transacao || "entrada",
    status: row.status || "pendente",
    comprovanteTexto: row.comprovante_texto || "",
comprovanteUrl: row.comprovante_url || "",
    descricao: row.descricao || "",
    repassarTaxa: Boolean(row.repassar_taxa),
    taxaPix: toMoney(row.taxa_pix),
    valorLiquidoPix: toMoney(row.valor_liquido_pix),
    valorDebitadoPix: toMoney(row.valor_debitado_pix),
    criadoEm: row.criado_em || null,
    aprovadoEm: row.aprovado_em || null,
    recusadoEm: row.recusado_em || null,
    comprovanteEnviadoEm: row.comprovante_enviado_em || null
  };
}

function mapFinancialTransaction(row) {
  if (!row) return null;

  return {
    id: row.id,
    userId: row.user_id,
    referenceKey: row.reference_key,
    sourceType: row.source_type,
    sourceId: row.source_id,
    operationType: row.operation_type,
    direction: row.direction,
    amount: toMoney(row.amount),
    status: row.status,
    description: row.description || "",
    metadata: row.metadata || {},
    createdAt: row.created_at || null,
    updatedAt: row.updated_at || null
  };
}

function mapLedgerEntry(row) {
  if (!row) return null;

  return {
    id: row.id,
    userId: row.user_id,
    financialTransactionId: row.financial_transaction_id,
    entryType: row.entry_type,
    amount: toMoney(row.amount),
    balanceBefore: toMoney(row.balance_before),
    balanceAfter: toMoney(row.balance_after),
    description: row.description || "",
    metadata: row.metadata || {},
    createdAt: row.created_at || null
  };
}

function mapNfcReceiveSession(row) {
  if (!row) return null;

  return {
    id: row.id,
    publicToken: row.public_token,
    receiverUserId: row.receiver_user_id,
    status: row.status || "pending",
    nonce: row.nonce || "",
    protocolVersion: Number(row.protocol_version || NFC_PROTOCOL_VERSION),
    expiresAt: row.expires_at || null,
    consumedAt: row.consumed_at || null,
    cancelledAt: row.cancelled_at || null,
    payerUserId: row.payer_user_id || "",
    amount: toMoney(row.amount),
    financialTransactionId: row.financial_transaction_id || "",
    readCount: Number(row.read_count || 0),
    lastReadAt: row.last_read_at || null,
    metadata: row.metadata || {},
    createdAt: row.created_at || null,
    updatedAt: row.updated_at || null
  };
}

function normalizeNfcSessionStatus(status) {
  const normalized = String(status || "").trim().toLowerCase();
  if (["pending", "consumed", "cancelled", "expired"].includes(normalized)) {
    return normalized;
  }
  return "pending";
}

function mapSigmoTapCharge(row) {
  if (!row) return null;

  return {
    id: row.id,
    publicCode: row.public_code || "",
    receiverUserId: row.receiver_user_id || "",
    status: row.status || "pending",
    amount: toMoney(row.amount),
    description: row.description || "",
    expiresAt: row.expires_at || null,
    nfcSessionId: row.nfc_session_id || "",
    payerUserId: row.payer_user_id || "",
    financialTransactionId: row.financial_transaction_id || "",
    paidAt: row.paid_at || null,
    cancelledAt: row.cancelled_at || null,
    metadata: row.metadata || {},
    createdAt: row.created_at || null,
    updatedAt: row.updated_at || null
  };
}

function normalizeSigmoTapChargeStatus(status) {
  const normalized = String(status || "").trim().toLowerCase();
  if (["pending", "armed", "paid", "cancelled", "expired"].includes(normalized)) {
    return normalized;
  }
  return "pending";
}

async function getUserById(id, client = pool) {
  const result = await client.query(
    "SELECT * FROM usuarios WHERE id = $1 LIMIT 1",
    [id]
  );
  return mapUser(result.rows[0]);
}

async function getUserByIdForUpdate(id, client) {
  const result = await client.query(
    "SELECT * FROM usuarios WHERE id = $1 LIMIT 1 FOR UPDATE",
    [id]
  );
  return mapUser(result.rows[0]);
}

async function getUserByEmail(email, client = pool) {
  const result = await client.query(
    "SELECT * FROM usuarios WHERE email = $1 LIMIT 1",
    [normalizeEmail(email)]
  );
  return mapUser(result.rows[0]);
}

async function listUsers() {
  const result = await pool.query(
    "SELECT * FROM usuarios ORDER BY criado_em DESC NULLS LAST"
  );
  return result.rows.map(mapUser);
}

async function listUsersByIds(userIds, client = pool) {
  const ids = Array.from(
    new Set(
      (Array.isArray(userIds) ? userIds : [])
        .map((id) => String(id || "").trim())
        .filter(Boolean)
    )
  );

  if (!ids.length) {
    return [];
  }

  const result = await client.query(
    "SELECT * FROM usuarios WHERE id = ANY($1::text[])",
    [ids]
  );

  return result.rows.map(mapUser);
}

async function getNfcReceiveSessionById(id, client = pool) {
  const result = await client.query(
    "SELECT * FROM nfc_receive_sessions WHERE id = $1 LIMIT 1",
    [String(id || "").trim()]
  );
  return mapNfcReceiveSession(result.rows[0]);
}

async function getNfcReceiveSessionByIdForUpdate(id, client) {
  const result = await client.query(
    "SELECT * FROM nfc_receive_sessions WHERE id = $1 LIMIT 1 FOR UPDATE",
    [String(id || "").trim()]
  );
  return mapNfcReceiveSession(result.rows[0]);
}

async function getNfcReceiveSessionByPublicToken(publicToken, client = pool) {
  const result = await client.query(
    "SELECT * FROM nfc_receive_sessions WHERE public_token = $1 LIMIT 1",
    [String(publicToken || "").trim()]
  );
  return mapNfcReceiveSession(result.rows[0]);
}

async function getNfcReceiveSessionByPublicTokenForUpdate(publicToken, client) {
  const result = await client.query(
    "SELECT * FROM nfc_receive_sessions WHERE public_token = $1 LIMIT 1 FOR UPDATE",
    [String(publicToken || "").trim()]
  );
  return mapNfcReceiveSession(result.rows[0]);
}

async function saveNfcReceiveSession(session, client = pool) {
  const payload = {
    ...session,
    status: normalizeNfcSessionStatus(session?.status),
    protocolVersion: Number(session?.protocolVersion || NFC_PROTOCOL_VERSION),
    amount: toMoney(session?.amount),
    readCount: Math.max(0, Number(session?.readCount || 0)),
    metadata: session?.metadata || {}
  };

  await client.query(
    `
    INSERT INTO nfc_receive_sessions (
      id, public_token, receiver_user_id, status, nonce, protocol_version,
      expires_at, consumed_at, cancelled_at, payer_user_id, amount,
      financial_transaction_id, read_count, last_read_at, metadata,
      created_at, updated_at
    )
    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17)
    ON CONFLICT (id) DO UPDATE SET
      public_token = EXCLUDED.public_token,
      receiver_user_id = EXCLUDED.receiver_user_id,
      status = EXCLUDED.status,
      nonce = EXCLUDED.nonce,
      protocol_version = EXCLUDED.protocol_version,
      expires_at = EXCLUDED.expires_at,
      consumed_at = EXCLUDED.consumed_at,
      cancelled_at = EXCLUDED.cancelled_at,
      payer_user_id = EXCLUDED.payer_user_id,
      amount = EXCLUDED.amount,
      financial_transaction_id = EXCLUDED.financial_transaction_id,
      read_count = EXCLUDED.read_count,
      last_read_at = EXCLUDED.last_read_at,
      metadata = EXCLUDED.metadata,
      created_at = COALESCE(nfc_receive_sessions.created_at, EXCLUDED.created_at),
      updated_at = EXCLUDED.updated_at
    `,
    [
      payload.id,
      payload.publicToken,
      payload.receiverUserId,
      payload.status,
      payload.nonce,
      payload.protocolVersion,
      payload.expiresAt,
      payload.consumedAt || null,
      payload.cancelledAt || null,
      payload.payerUserId || "",
      payload.amount,
      payload.financialTransactionId || "",
      payload.readCount,
      payload.lastReadAt || null,
      JSON.stringify(payload.metadata || {}),
      payload.createdAt || db(),
      payload.updatedAt || db()
    ]
  );
}

async function getSigmoTapChargeById(id, client = pool) {
  const result = await client.query(
    "SELECT * FROM sigmo_tap_charges WHERE id = $1 LIMIT 1",
    [String(id || "").trim()]
  );
  return mapSigmoTapCharge(result.rows[0]);
}

async function getSigmoTapChargeByIdForUpdate(id, client) {
  const result = await client.query(
    "SELECT * FROM sigmo_tap_charges WHERE id = $1 LIMIT 1 FOR UPDATE",
    [String(id || "").trim()]
  );
  return mapSigmoTapCharge(result.rows[0]);
}

async function getSigmoTapChargeByPublicCode(publicCode, client = pool) {
  const result = await client.query(
    "SELECT * FROM sigmo_tap_charges WHERE public_code = $1 LIMIT 1",
    [String(publicCode || "").trim()]
  );
  return mapSigmoTapCharge(result.rows[0]);
}

async function saveSigmoTapCharge(charge, client = pool) {
  const payload = {
    ...charge,
    publicCode: String(charge?.publicCode || "").trim(),
    receiverUserId: String(charge?.receiverUserId || "").trim(),
    status: normalizeSigmoTapChargeStatus(charge?.status),
    amount: toMoney(charge?.amount),
    description: String(charge?.description || "").trim(),
    nfcSessionId: String(charge?.nfcSessionId || "").trim(),
    payerUserId: String(charge?.payerUserId || "").trim(),
    financialTransactionId: String(charge?.financialTransactionId || "").trim(),
    metadata: charge?.metadata || {}
  };

  await client.query(
    `
    INSERT INTO sigmo_tap_charges (
      id, public_code, receiver_user_id, status, amount, description,
      expires_at, nfc_session_id, payer_user_id, financial_transaction_id,
      paid_at, cancelled_at, metadata, created_at, updated_at
    )
    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)
    ON CONFLICT (id) DO UPDATE SET
      public_code = EXCLUDED.public_code,
      receiver_user_id = EXCLUDED.receiver_user_id,
      status = EXCLUDED.status,
      amount = EXCLUDED.amount,
      description = EXCLUDED.description,
      expires_at = EXCLUDED.expires_at,
      nfc_session_id = EXCLUDED.nfc_session_id,
      payer_user_id = EXCLUDED.payer_user_id,
      financial_transaction_id = EXCLUDED.financial_transaction_id,
      paid_at = EXCLUDED.paid_at,
      cancelled_at = EXCLUDED.cancelled_at,
      metadata = EXCLUDED.metadata,
      created_at = COALESCE(sigmo_tap_charges.created_at, EXCLUDED.created_at),
      updated_at = EXCLUDED.updated_at
    `,
    [
      payload.id,
      payload.publicCode,
      payload.receiverUserId,
      payload.status,
      payload.amount,
      payload.description,
      payload.expiresAt,
      payload.nfcSessionId,
      payload.payerUserId,
      payload.financialTransactionId,
      payload.paidAt || null,
      payload.cancelledAt || null,
      JSON.stringify(payload.metadata || {}),
      payload.createdAt || db(),
      payload.updatedAt || db()
    ]
  );
}

function buildSigmoCardClaimToken() {
  return crypto.randomBytes(12).toString("hex");
}

function mapSigmoCard(row) {
  if (!row) return null;

  return {
    id: row.id,
    ownerUserId: row.owner_user_id || "",
    holderUserId: row.holder_user_id || "",
    cardType: normalizeSigmoCardType(row.card_type),
    label: row.label || "",
    status: normalizeSigmoCardStatus(row.status),
    spendingLimit: toMoney(row.spending_limit),
    deviceId: row.device_id || "",
    claimToken: row.claim_token || "",
    boundAt: row.bound_at || null,
    lastUsedAt: row.last_used_at || null,
    metadata: row.metadata || {},
    createdAt: row.created_at || null,
    updatedAt: row.updated_at || null
  };
}

async function getSigmoCardById(id, client = pool) {
  const result = await client.query(
    "SELECT * FROM sigmo_cards WHERE id = $1 LIMIT 1",
    [String(id || "").trim()]
  );
  return mapSigmoCard(result.rows[0]);
}

async function getSigmoCardByIdForUpdate(id, client) {
  const result = await client.query(
    "SELECT * FROM sigmo_cards WHERE id = $1 LIMIT 1 FOR UPDATE",
    [String(id || "").trim()]
  );
  return mapSigmoCard(result.rows[0]);
}

async function getPrimarySigmoCardByOwner(ownerUserId, client = pool) {
  const result = await client.query(
    `
    SELECT *
    FROM sigmo_cards
    WHERE owner_user_id = $1
      AND card_type = 'primary'
    ORDER BY created_at ASC NULLS LAST
    LIMIT 1
    `,
    [String(ownerUserId || "").trim()]
  );
  return mapSigmoCard(result.rows[0]);
}

async function listSigmoCardsByOwner(ownerUserId, client = pool) {
  const result = await client.query(
    `
    SELECT *
    FROM sigmo_cards
    WHERE owner_user_id = $1
    ORDER BY
      CASE WHEN card_type = 'primary' THEN 0 ELSE 1 END,
      created_at ASC NULLS LAST,
      id ASC
    `,
    [String(ownerUserId || "").trim()]
  );
  return result.rows.map(mapSigmoCard);
}

async function getBoundSigmoCardByHolderAndDevice(holderUserId, deviceId, client = pool) {
  const holderId = String(holderUserId || "").trim();
  const normalizedDeviceId = String(deviceId || "").trim();

  if (!holderId || !normalizedDeviceId) {
    return null;
  }

  const result = await client.query(
    `
    SELECT *
    FROM sigmo_cards
    WHERE holder_user_id = $1
      AND device_id = $2
      AND status = 'active'
    ORDER BY created_at DESC NULLS LAST
    LIMIT 1
    `,
    [holderId, normalizedDeviceId]
  );

  return mapSigmoCard(result.rows[0]);
}

async function getSigmoCardsByHolder(holderUserId, client = pool) {
  const result = await client.query(
    `
    SELECT *
    FROM sigmo_cards
    WHERE holder_user_id = $1
    ORDER BY created_at DESC NULLS LAST
    `,
    [String(holderUserId || "").trim()]
  );
  return result.rows.map(mapSigmoCard);
}

async function saveSigmoCard(card, client = pool) {
  const payload = {
    ...card,
    ownerUserId: String(card?.ownerUserId || "").trim(),
    holderUserId: String(card?.holderUserId || "").trim(),
    cardType: normalizeSigmoCardType(card?.cardType),
    label: String(card?.label || "").trim(),
    status: normalizeSigmoCardStatus(card?.status),
    spendingLimit: Math.max(0, toMoney(card?.spendingLimit)),
    deviceId: String(card?.deviceId || "").trim(),
    claimToken: String(card?.claimToken || buildSigmoCardClaimToken()).trim(),
    metadata: card?.metadata || {}
  };

  await client.query(
    `
    INSERT INTO sigmo_cards (
      id, owner_user_id, holder_user_id, card_type, label, status,
      spending_limit, device_id, claim_token, bound_at, last_used_at,
      metadata, created_at, updated_at
    )
    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)
    ON CONFLICT (id) DO UPDATE SET
      owner_user_id = EXCLUDED.owner_user_id,
      holder_user_id = EXCLUDED.holder_user_id,
      card_type = EXCLUDED.card_type,
      label = EXCLUDED.label,
      status = EXCLUDED.status,
      spending_limit = EXCLUDED.spending_limit,
      device_id = EXCLUDED.device_id,
      claim_token = EXCLUDED.claim_token,
      bound_at = EXCLUDED.bound_at,
      last_used_at = EXCLUDED.last_used_at,
      metadata = EXCLUDED.metadata,
      created_at = COALESCE(sigmo_cards.created_at, EXCLUDED.created_at),
      updated_at = EXCLUDED.updated_at
    `,
    [
      payload.id,
      payload.ownerUserId,
      payload.holderUserId,
      payload.cardType,
      payload.label,
      payload.status,
      payload.spendingLimit,
      payload.deviceId,
      payload.claimToken,
      payload.boundAt || null,
      payload.lastUsedAt || null,
      JSON.stringify(payload.metadata || {}),
      payload.createdAt || db(),
      payload.updatedAt || db()
    ]
  );
}

async function ensurePrimarySigmoCard(user, client = pool) {
  if (!user?.id) return null;

  let card = await getPrimarySigmoCardByOwner(user.id, client);

  if (card) {
    return card;
  }

  card = {
    id: buildId("card"),
    ownerUserId: user.id,
    holderUserId: user.id,
    cardType: "primary",
    label: "Cartao principal",
    status: "active",
    spendingLimit: 0,
    deviceId: "",
    claimToken: buildSigmoCardClaimToken(),
    boundAt: null,
    lastUsedAt: null,
    metadata: {
      origin: "auto_primary"
    },
    createdAt: db(),
    updatedAt: db()
  };

  await saveSigmoCard(card, client);
  return card;
}

async function buildUserActiveCardResponse(user, deviceId, client = pool) {
  if (!user?.id || !String(deviceId || "").trim()) {
    return null;
  }

  const card = await getBoundSigmoCardByHolderAndDevice(user.id, deviceId, client);

  if (!card) {
    return null;
  }

  const owner = card.ownerUserId === user.id ? user : await getUserById(card.ownerUserId, client);
  const holder = card.holderUserId === user.id ? user : await getUserById(card.holderUserId, client);

  if (!owner || !holder) {
    return null;
  }

  return buildSigmoCardResponse(card, owner, holder);
}

async function cancelPendingNfcReceiveSessionsByReceiver(
  receiverUserId,
  client = pool,
  exceptSessionId = ""
) {
  const userId = String(receiverUserId || "").trim();
  const exceptId = String(exceptSessionId || "").trim();

  if (!userId) return;

  const params = [userId, db()];
  let sql = `
    UPDATE nfc_receive_sessions
    SET status = 'cancelled',
        cancelled_at = $2,
        updated_at = $2
    WHERE receiver_user_id = $1
      AND status = 'pending'
  `;

  if (exceptId) {
    params.push(exceptId);
    sql += ` AND id <> $3`;
  }

  await client.query(sql, params);
}

async function expireNfcReceiveSessionIfNeeded(session, client = pool) {
  if (!session || session.status !== "pending") {
    return session;
  }

  if (!isTimestampExpired(session.expiresAt)) {
    return session;
  }

  const expiredSession = {
    ...session,
    status: "expired",
    updatedAt: db()
  };

  await saveNfcReceiveSession(expiredSession, client);
  return expiredSession;
}

async function touchNfcReceiveSessionRead(session, client = pool) {
  if (!session?.id) return session;

  const updated = {
    ...session,
    readCount: Math.max(0, Number(session.readCount || 0)) + 1,
    lastReadAt: db(),
    updatedAt: db()
  };

  await saveNfcReceiveSession(updated, client);
  return updated;
}

async function syncSigmoTapChargeStatus(charge, client = pool) {
  if (!charge?.id) return charge;

  const currentStatus = normalizeSigmoTapChargeStatus(charge.status);

  if (currentStatus === "paid" || currentStatus === "cancelled" || currentStatus === "expired") {
    return charge;
  }

  let nextStatus = "pending";

  if (isTimestampExpired(charge.expiresAt)) {
    nextStatus = "expired";
  } else if (String(charge.nfcSessionId || "").trim()) {
    const session = await getNfcReceiveSessionById(charge.nfcSessionId, client);
    if (session && session.status === "pending" && !isTimestampExpired(session.expiresAt)) {
      nextStatus = "armed";
    }
  }

  if (nextStatus === currentStatus) {
    return charge;
  }

  const updated = {
    ...charge,
    status: nextStatus,
    updatedAt: db()
  };

  await saveSigmoTapCharge(updated, client);
  return updated;
}

async function saveUser(user, client = pool) {
  await client.query(
    `
    INSERT INTO usuarios (
      id, nome, email, senha, saldo, criado_em,
      nome_atualizado_em, saldo_atualizado_em, senha_atualizada_em,
      status_conta, conta_banida_em, motivo_banimento,
      bonus_boas_vindas, bonus_boas_vindas_concedido_em,
      pin_transacao_hash, pin_transacao_atualizado_em
    )
    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16)
    ON CONFLICT (id) DO UPDATE SET
      nome = EXCLUDED.nome,
      email = EXCLUDED.email,
      senha = EXCLUDED.senha,
      saldo = EXCLUDED.saldo,
      criado_em = COALESCE(usuarios.criado_em, EXCLUDED.criado_em),
      nome_atualizado_em = EXCLUDED.nome_atualizado_em,
      saldo_atualizado_em = EXCLUDED.saldo_atualizado_em,
      senha_atualizada_em = EXCLUDED.senha_atualizada_em,
      status_conta = EXCLUDED.status_conta,
      conta_banida_em = EXCLUDED.conta_banida_em,
      motivo_banimento = EXCLUDED.motivo_banimento,
      bonus_boas_vindas = EXCLUDED.bonus_boas_vindas,
      bonus_boas_vindas_concedido_em = EXCLUDED.bonus_boas_vindas_concedido_em,
      pin_transacao_hash = EXCLUDED.pin_transacao_hash,
      pin_transacao_atualizado_em = EXCLUDED.pin_transacao_atualizado_em
    `,
    [
      user.id,
      user.nome || "",
      normalizeEmail(user.email),
      user.senha,
      toMoney(user.saldo),
      user.criadoEm || db(),
      user.nomeAtualizadoEm || null,
      user.saldoAtualizadoEm || null,
      user.senhaAtualizadaEm || null,
      user.statusConta || STATUS_CONTA_ATIVA,
      user.contaBanidaEm || null,
      user.motivoBanimento || "",
      toMoney(user.bonusBoasVindas),
      user.bonusBoasVindasConcedidoEm || null,
      user.pinTransacaoHash || "",
      user.pinTransacaoAtualizadoEm || null
    ]
  );
}

async function getDepositoById(id, client = pool) {
  const result = await client.query(
    "SELECT * FROM depositos WHERE id = $1 LIMIT 1",
    [id]
  );
  return mapDeposito(result.rows[0]);
}

async function getDepositoByIdForUpdate(id, client) {
  const result = await client.query(
    "SELECT * FROM depositos WHERE id = $1 LIMIT 1 FOR UPDATE",
    [id]
  );
  return mapDeposito(result.rows[0]);
}

async function listDepositos() {
  const result = await pool.query(
    "SELECT * FROM depositos ORDER BY criado_em DESC NULLS LAST"
  );
  return result.rows.map(mapDeposito);
}

async function listDepositosByUser(userId) {
  const result = await pool.query(
    "SELECT * FROM depositos WHERE user_id = $1 ORDER BY criado_em DESC NULLS LAST",
    [userId]
  );
  return result.rows.map(mapDeposito);
}

async function saveDeposito(dep, client = pool) {
  await client.query(
    `
    INSERT INTO depositos (
      id, user_id, valor, chave_pix, tipo_chave, tipo_transacao, status,
      comprovante_url, comprovante_texto, descricao, repassar_taxa,
      taxa_pix, valor_liquido_pix, valor_debitado_pix, criado_em,
      aprovado_em, recusado_em, comprovante_enviado_em
    )
    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18)
    ON CONFLICT (id) DO UPDATE SET
      user_id = EXCLUDED.user_id,
      valor = EXCLUDED.valor,
      chave_pix = EXCLUDED.chave_pix,
      tipo_chave = EXCLUDED.tipo_chave,
      tipo_transacao = EXCLUDED.tipo_transacao,
      status = EXCLUDED.status,
      comprovante_url = EXCLUDED.comprovante_url,
      comprovante_texto = EXCLUDED.comprovante_texto, -- 🔥 FALTAVA ISSO
      descricao = EXCLUDED.descricao,
      repassar_taxa = EXCLUDED.repassar_taxa,
      taxa_pix = EXCLUDED.taxa_pix,
      valor_liquido_pix = EXCLUDED.valor_liquido_pix,
      valor_debitado_pix = EXCLUDED.valor_debitado_pix,
      criado_em = COALESCE(depositos.criado_em, EXCLUDED.criado_em),
      aprovado_em = EXCLUDED.aprovado_em,
      recusado_em = EXCLUDED.recusado_em,
      comprovante_enviado_em = EXCLUDED.comprovante_enviado_em
    `,
    [
      dep.id,
      dep.userId,
      toMoney(dep.valor),
      dep.chavePix || "",
      dep.tipoChave || "",
      dep.tipoTransacao || "entrada",
      dep.status || "pendente",
      dep.comprovanteUrl || "",
      dep.comprovanteTexto || "",
      dep.descricao || "",
      Boolean(dep.repassarTaxa),
      toMoney(dep.taxaPix),
      toMoney(dep.valorLiquidoPix),
      toMoney(dep.valorDebitadoPix),
      dep.criadoEm || db(),
      dep.aprovadoEm || null,
      dep.recusadoEm || null,
      dep.comprovanteEnviadoEm || null
    ]
  );
}

async function getAdminByEmail(email) {
  const result = await pool.query(
    "SELECT * FROM admins WHERE email = $1 LIMIT 1",
    [normalizeEmail(email)]
  );
  return result.rows[0] || null;
}

async function getAdminById(id) {
  const result = await pool.query(
    "SELECT * FROM admins WHERE id = $1 LIMIT 1",
    [id]
  );
  return result.rows[0] || null;
}

async function listFinancialTransactionsByUser(userId) {
  const result = await pool.query(
    `
    SELECT * FROM financial_transactions
    WHERE user_id = $1
    ORDER BY created_at DESC NULLS LAST, id DESC
    `,
    [userId]
  );
  return result.rows.map(mapFinancialTransaction);
}

async function listLedgerEntriesByUser(userId) {
  const result = await pool.query(
    `
    SELECT * FROM ledger_entries
    WHERE user_id = $1
    ORDER BY created_at DESC NULLS LAST, id DESC
    `,
    [userId]
  );
  return result.rows.map(mapLedgerEntry);
}

async function listCompletedFinancialTransactionsForUsers(userIds, client = pool) {
  const ids = Array.isArray(userIds)
    ? userIds.map((id) => String(id || "").trim()).filter(Boolean)
    : [];

  if (!ids.length) {
    return [];
  }

  const result = await client.query(
    `
    SELECT *
    FROM financial_transactions
    WHERE user_id = ANY($1::text[])
      AND status = 'completed'
    ORDER BY user_id ASC, created_at ASC NULLS LAST, id ASC
    `,
    [ids]
  );

  return result.rows.map(mapFinancialTransaction);
}

function normalizeBalanceSplit(amount, bonusAmount = 0, realAmount = 0) {
  const totalAmount = toMoney(amount);

  if (!Number.isFinite(totalAmount) || totalAmount <= 0) {
    return {
      bonusAmount: 0,
      realAmount: 0,
      hasExplicitSplit: false
    };
  }

  let bonus = Math.max(0, toMoney(bonusAmount));
  let real = Math.max(0, toMoney(realAmount));
  const explicit = bonus > 0 || real > 0;

  if (!explicit) {
    return {
      bonusAmount: 0,
      realAmount: totalAmount,
      hasExplicitSplit: false
    };
  }

  const splitTotal = toMoney(bonus + real);

  if (splitTotal <= 0) {
    return {
      bonusAmount: 0,
      realAmount: totalAmount,
      hasExplicitSplit: false
    };
  }

  if (splitTotal !== totalAmount) {
    const factor = totalAmount / splitTotal;
    bonus = toMoney(bonus * factor);
    real = toMoney(real * factor);
  }

  const diff = toMoney(totalAmount - bonus - real);

  if (diff !== 0) {
    real = toMoney(real + diff);
  }

  return {
    bonusAmount: bonus,
    realAmount: real,
    hasExplicitSplit: true
  };
}

function getBalanceSplitFromMetadata(metadata, amount) {
  if (!metadata || typeof metadata !== "object") return null;

  const split = normalizeBalanceSplit(
    amount,
    metadata.bonusAmount,
    metadata.realAmount
  );

  return split.hasExplicitSplit ? split : null;
}

function computeUserFinancialContext(transactions, currentBalance = 0) {
  let saldoBonusAtual = 0;
  let saldoRealAtual = 0;
  let valorRecebidoViaPix = 0;

  for (const tx of Array.isArray(transactions) ? transactions : []) {
    const amount = toMoney(tx?.amount);

    if (!Number.isFinite(amount) || amount <= 0) continue;

    if (
      tx.direction === "credit" &&
      tx.operationType === "deposit" &&
      (tx.sourceType === "dentpeg" || tx.sourceType === "deposito")
    ) {
      valorRecebidoViaPix = toMoney(valorRecebidoViaPix + amount);
    }

    if (tx.direction === "credit") {
      if (
        tx.sourceType === "welcome_bonus" ||
        tx.operationType === "welcome_bonus"
      ) {
        saldoBonusAtual = toMoney(saldoBonusAtual + amount);
        continue;
      }

      if (tx.sourceType === "transfer" && tx.operationType === "transfer_in") {
        const split = getBalanceSplitFromMetadata(tx.metadata, amount);

        if (split) {
          saldoBonusAtual = toMoney(saldoBonusAtual + split.bonusAmount);
          saldoRealAtual = toMoney(saldoRealAtual + split.realAmount);
          continue;
        }
      }

      saldoRealAtual = toMoney(saldoRealAtual + amount);
      continue;
    }

    if (tx.direction === "debit") {
      const split = getBalanceSplitFromMetadata(tx.metadata, amount);

      if (split) {
        saldoBonusAtual = toMoney(
          Math.max(0, saldoBonusAtual - split.bonusAmount)
        );
        saldoRealAtual = toMoney(
          Math.max(0, saldoRealAtual - split.realAmount)
        );
        continue;
      }

      const debitoBonus = Math.min(saldoBonusAtual, amount);
      const debitoReal = toMoney(amount - debitoBonus);

      saldoBonusAtual = toMoney(Math.max(0, saldoBonusAtual - debitoBonus));
      saldoRealAtual = toMoney(Math.max(0, saldoRealAtual - debitoReal));
    }
  }

  const saldoTotalAtual = toMoney(currentBalance);
  let totalCalculado = toMoney(saldoBonusAtual + saldoRealAtual);

  if (totalCalculado < saldoTotalAtual) {
    saldoRealAtual = toMoney(saldoRealAtual + (saldoTotalAtual - totalCalculado));
    totalCalculado = saldoTotalAtual;
  } else if (totalCalculado > saldoTotalAtual) {
    let excesso = toMoney(totalCalculado - saldoTotalAtual);

    if (saldoRealAtual >= excesso) {
      saldoRealAtual = toMoney(saldoRealAtual - excesso);
    } else {
      excesso = toMoney(excesso - saldoRealAtual);
      saldoRealAtual = 0;
      saldoBonusAtual = toMoney(Math.max(0, saldoBonusAtual - excesso));
    }
  }

  return {
    saldoTotalAtual,
    saldoBonusAtual: toMoney(Math.min(saldoTotalAtual, saldoBonusAtual)),
    saldoRealAtual: toMoney(Math.max(0, saldoTotalAtual - saldoBonusAtual)),
    bonusConcedido: 0,
    valorRecebidoViaPix,
    pixDesbloqueado: valorRecebidoViaPix >= PIX_SAQUE_DESBLOQUEIO_MIN
  };
}

function buildUserFinancialContextMap(users, transactions) {
  const txByUserId = new Map();

  for (const tx of Array.isArray(transactions) ? transactions : []) {
    const userId = String(tx?.userId || "").trim();
    if (!userId) continue;
    if (!txByUserId.has(userId)) txByUserId.set(userId, []);
    txByUserId.get(userId).push(tx);
  }

  const contextMap = new Map();

  for (const user of Array.isArray(users) ? users : []) {
    const txList = txByUserId.get(user.id) || [];
    const context = computeUserFinancialContext(txList, user.saldo);
    context.bonusConcedido = toMoney(user.bonusBoasVindas);
    contextMap.set(user.id, context);
  }

  return contextMap;
}

function buildDefaultUserFinancialContext(user) {
  return {
    saldoTotalAtual: toMoney(user?.saldo),
    saldoBonusAtual: 0,
    saldoRealAtual: toMoney(user?.saldo),
    bonusConcedido: toMoney(user?.bonusBoasVindas),
    valorRecebidoViaPix: 0,
    pixDesbloqueado: false
  };
}

function buildDefaultAdminFraudRiskContext() {
  return {
    riscoFraudeSaquePix: false,
    riscoFraudeSaquePixOrigemUserId: "",
    riscoFraudeSaquePixOrigemEmail: "",
    riscoFraudeSaquePixValorRecebido: 0,
    riscoFraudeSaquePixUltimoRecebimentoEm: null
  };
}

function buildDefaultAdminUserContext(user) {
  return {
    ...buildDefaultUserFinancialContext(user),
    ...buildDefaultAdminFraudRiskContext()
  };
}

async function getUsersFinancialContextMap(users, client = pool) {
  const lista = Array.isArray(users) ? users.filter(Boolean) : [];

  if (!lista.length) {
    return new Map();
  }

  const transactions = await listCompletedFinancialTransactionsForUsers(
    lista.map((user) => user.id),
    client
  );

  return buildUserFinancialContextMap(lista, transactions);
}

async function getUserFinancialContext(user, client = pool) {
  if (!user?.id) {
    return buildDefaultUserFinancialContext(user);
  }

  const contextMap = await getUsersFinancialContextMap([user], client);
  return contextMap.get(user.id) || buildDefaultUserFinancialContext(user);
}

function buildAdminUserResponse(user, context = null) {
  const financialContext = {
    ...buildDefaultAdminUserContext(user),
    ...(context || {})
  };

  return {
    id: user.id,
    nome: user.nome,
    email: user.email,
    saldo: toMoney(user.saldo),
    criadoEm: user.criadoEm || null,
    statusConta: user.statusConta || STATUS_CONTA_ATIVA,
    contaBanida: isContaBanida(user),
    contaBanidaEm: user.contaBanidaEm || null,
    motivoBanimento: user.motivoBanimento || "",
    bonusBoasVindas: toMoney(user.bonusBoasVindas),
    bonusBoasVindasConcedidoEm: user.bonusBoasVindasConcedidoEm || null,
    saldoBonusAtual: toMoney(financialContext.saldoBonusAtual),
    saldoRealAtual: toMoney(financialContext.saldoRealAtual),
    valorRecebidoViaPix: toMoney(financialContext.valorRecebidoViaPix),
    pixDesbloqueado: Boolean(financialContext.pixDesbloqueado),
    riscoFraudeSaquePix: Boolean(financialContext.riscoFraudeSaquePix),
    riscoFraudeSaquePixOrigemUserId:
      String(financialContext.riscoFraudeSaquePixOrigemUserId || "").trim(),
    riscoFraudeSaquePixOrigemEmail:
      String(financialContext.riscoFraudeSaquePixOrigemEmail || "").trim(),
    riscoFraudeSaquePixValorRecebido: toMoney(
      financialContext.riscoFraudeSaquePixValorRecebido
    ),
    riscoFraudeSaquePixUltimoRecebimentoEm:
      financialContext.riscoFraudeSaquePixUltimoRecebimentoEm || null
  };
}

async function getValorRecebidoViaPix(userId, client = pool) {
  const result = await client.query(
    `
    SELECT COALESCE(SUM(amount), 0) AS total
    FROM financial_transactions
    WHERE user_id = $1
      AND status = 'completed'
      AND direction = 'credit'
      AND operation_type = 'deposit'
      AND source_type IN ('dentpeg', 'deposito')
    `,
    [userId]
  );

  return toMoney(result.rows[0]?.total);
}

async function listTransferenciasRecebidasPorUsuario(userId, client = pool) {
  const result = await client.query(
    `
    SELECT
      COALESCE(metadata->>'fromUserId', '') AS from_user_id,
      COALESCE(metadata->>'fromEmail', '') AS from_email,
      COALESCE(SUM(amount), 0) AS total_amount,
      MAX(created_at) AS last_received_at
    FROM financial_transactions
    WHERE user_id = $1
      AND status = 'completed'
      AND source_type = 'transfer'
      AND operation_type = 'transfer_in'
      AND direction = 'credit'
    GROUP BY 1, 2
    ORDER BY MAX(created_at) DESC NULLS LAST
    `,
    [userId]
  );

  return result.rows.map((row) => ({
    fromUserId: row.from_user_id || "",
    fromEmail: row.from_email || "",
    totalAmount: toMoney(row.total_amount),
    lastReceivedAt: row.last_received_at || null
  }));
}

async function listTransferenciasRecebidasPorUsuarios(userIds, client = pool) {
  const ids = Array.from(
    new Set(
      (Array.isArray(userIds) ? userIds : [])
        .map((id) => String(id || "").trim())
        .filter(Boolean)
    )
  );

  if (!ids.length) {
    return [];
  }

  const result = await client.query(
    `
    SELECT
      user_id,
      COALESCE(metadata->>'fromUserId', '') AS from_user_id,
      COALESCE(metadata->>'fromEmail', '') AS from_email,
      COALESCE(SUM(amount), 0) AS total_amount,
      MAX(created_at) AS last_received_at
    FROM financial_transactions
    WHERE user_id = ANY($1::text[])
      AND status = 'completed'
      AND source_type = 'transfer'
      AND operation_type = 'transfer_in'
      AND direction = 'credit'
    GROUP BY 1, 2, 3
    ORDER BY user_id ASC, MAX(created_at) DESC NULLS LAST
    `,
    [ids]
  );

  return result.rows.map((row) => ({
    userId: row.user_id || "",
    fromUserId: row.from_user_id || "",
    fromEmail: row.from_email || "",
    totalAmount: toMoney(row.total_amount),
    lastReceivedAt: row.last_received_at || null
  }));
}

async function getResumoFinanceiroUsuario(userId, client = pool) {
  const result = await client.query(
    `
    SELECT
      COUNT(*) FILTER (WHERE status = 'completed') AS completed_count,
      COUNT(*) FILTER (
        WHERE status = 'completed'
          AND source_type = 'transfer'
          AND operation_type = 'transfer_in'
          AND direction = 'credit'
      ) AS transfer_in_count,
      COUNT(*) FILTER (
        WHERE status = 'completed'
          AND source_type = 'transfer'
          AND operation_type = 'transfer_out'
          AND direction = 'debit'
      ) AS transfer_out_count,
      COUNT(*) FILTER (
        WHERE status = 'completed'
          AND source_type NOT IN ('welcome_bonus', 'transfer')
      ) AS other_completed_ops,
      COALESCE(SUM(
        CASE
          WHEN status = 'completed'
            AND direction = 'credit'
            AND operation_type = 'deposit'
            AND source_type IN ('dentpeg', 'deposito')
          THEN amount
          ELSE 0
        END
      ), 0) AS qualifying_pix_total
    FROM financial_transactions
    WHERE user_id = $1
    `,
    [userId]
  );

  const row = result.rows[0] || {};

  return {
    completedCount: Number(row.completed_count || 0),
    transferInCount: Number(row.transfer_in_count || 0),
    transferOutCount: Number(row.transfer_out_count || 0),
    otherCompletedOps: Number(row.other_completed_ops || 0),
    qualifyingPixTotal: toMoney(row.qualifying_pix_total)
  };
}

async function getResumoFinanceiroUsuarios(userIds, client = pool) {
  const ids = Array.from(
    new Set(
      (Array.isArray(userIds) ? userIds : [])
        .map((id) => String(id || "").trim())
        .filter(Boolean)
    )
  );

  if (!ids.length) {
    return new Map();
  }

  const result = await client.query(
    `
    SELECT
      user_id,
      COUNT(*) FILTER (WHERE status = 'completed') AS completed_count,
      COUNT(*) FILTER (
        WHERE status = 'completed'
          AND source_type = 'transfer'
          AND operation_type = 'transfer_in'
          AND direction = 'credit'
      ) AS transfer_in_count,
      COUNT(*) FILTER (
        WHERE status = 'completed'
          AND source_type = 'transfer'
          AND operation_type = 'transfer_out'
          AND direction = 'debit'
      ) AS transfer_out_count,
      COUNT(*) FILTER (
        WHERE status = 'completed'
          AND source_type NOT IN ('welcome_bonus', 'transfer')
      ) AS other_completed_ops,
      COALESCE(SUM(
        CASE
          WHEN status = 'completed'
            AND direction = 'credit'
            AND operation_type = 'deposit'
            AND source_type IN ('dentpeg', 'deposito')
          THEN amount
          ELSE 0
        END
      ), 0) AS qualifying_pix_total
    FROM financial_transactions
    WHERE user_id = ANY($1::text[])
    GROUP BY user_id
    `,
    [ids]
  );

  const resumoMap = new Map();

  for (const row of result.rows) {
    resumoMap.set(row.user_id, {
      completedCount: Number(row.completed_count || 0),
      transferInCount: Number(row.transfer_in_count || 0),
      transferOutCount: Number(row.transfer_out_count || 0),
      otherCompletedOps: Number(row.other_completed_ops || 0),
      qualifyingPixTotal: toMoney(row.qualifying_pix_total)
    });
  }

  return resumoMap;
}

function isResumoContaOrigemFraudeBonus(user, resumo = null) {
  if (!user) return false;
  if (toMoney(user.bonusBoasVindas) <= 0) return false;
  if (toMoney(user.saldo) > 0) return false;

  const stats = resumo || {};

  if (toMoney(stats.qualifyingPixTotal) > 0) return false;
  if (Number(stats.otherCompletedOps || 0) > 0) return false;
  if (Number(stats.transferInCount || 0) > 0) return false;
  if (Number(stats.transferOutCount || 0) <= 0) return false;

  return Number(stats.completedCount || 0) <= Number(stats.transferOutCount || 0) + 1;
}

async function isContaOrigemFraudeBonus(userId, client = pool) {
  if (!userId) return false;

  const user = await getUserById(userId, client);

  if (!user) return false;
  if (toMoney(user.bonusBoasVindas) <= 0) return false;
  if (toMoney(user.saldo) > 0) return false;

  const resumo = await getResumoFinanceiroUsuario(userId, client);
  return isResumoContaOrigemFraudeBonus(user, resumo);
}

async function encontrarOrigemFraudeBonus(userId, client = pool) {
  const transferencias = await listTransferenciasRecebidasPorUsuario(userId, client);

  for (const transferencia of transferencias) {
    if (!transferencia.fromUserId) continue;

    const suspeita = await isContaOrigemFraudeBonus(
      transferencia.fromUserId,
      client
    );

    if (suspeita) {
      return transferencia;
    }
  }

  return null;
}

async function getUsersFraudRiskMap(users, client = pool) {
  const lista = Array.isArray(users) ? users.filter((user) => user?.id) : [];

  if (!lista.length) {
    return new Map();
  }

  const transferencias = await listTransferenciasRecebidasPorUsuarios(
    lista.map((user) => user.id),
    client
  );

  if (!transferencias.length) {
    return new Map();
  }

  const transferenciasByUserId = new Map();
  const sourceUserIds = new Set();

  for (const transferencia of transferencias) {
    if (!transferencia?.userId) continue;
    if (!transferenciasByUserId.has(transferencia.userId)) {
      transferenciasByUserId.set(transferencia.userId, []);
    }
    transferenciasByUserId.get(transferencia.userId).push(transferencia);
    if (transferencia.fromUserId) {
      sourceUserIds.add(transferencia.fromUserId);
    }
  }

  if (!sourceUserIds.size) {
    return new Map();
  }

  const [sourceUsers, sourceResumoMap] = await Promise.all([
    listUsersByIds(Array.from(sourceUserIds), client),
    getResumoFinanceiroUsuarios(Array.from(sourceUserIds), client)
  ]);

  const sourceUserMap = new Map(sourceUsers.map((user) => [user.id, user]));
  const riskMap = new Map();

  for (const [userId, listaTransferencias] of transferenciasByUserId.entries()) {
    for (const transferencia of listaTransferencias) {
      if (!transferencia.fromUserId) continue;

      const sourceUser = sourceUserMap.get(transferencia.fromUserId);
      const sourceResumo = sourceResumoMap.get(transferencia.fromUserId);

      if (!isResumoContaOrigemFraudeBonus(sourceUser, sourceResumo)) {
        continue;
      }

      riskMap.set(userId, {
        riscoFraudeSaquePix: true,
        riscoFraudeSaquePixOrigemUserId: transferencia.fromUserId || "",
        riscoFraudeSaquePixOrigemEmail: transferencia.fromEmail || "",
        riscoFraudeSaquePixValorRecebido: toMoney(transferencia.totalAmount),
        riscoFraudeSaquePixUltimoRecebimentoEm: transferencia.lastReceivedAt || null
      });
      break;
    }
  }

  return riskMap;
}

async function getUsersAdminContextMap(users, client = pool) {
  const lista = Array.isArray(users) ? users.filter(Boolean) : [];

  if (!lista.length) {
    return new Map();
  }

  const [financialContextMap, fraudRiskMap] = await Promise.all([
    getUsersFinancialContextMap(lista, client),
    getUsersFraudRiskMap(lista, client)
  ]);

  const contextMap = new Map();

  for (const user of lista) {
    contextMap.set(user.id, {
      ...buildDefaultAdminUserContext(user),
      ...(financialContextMap.get(user.id) || {}),
      ...(fraudRiskMap.get(user.id) || {})
    });
  }

  return contextMap;
}

async function getAdminUserContext(user, client = pool) {
  if (!user?.id) {
    return buildDefaultAdminUserContext(user);
  }

  const contextMap = await getUsersAdminContextMap([user], client);
  return contextMap.get(user.id) || buildDefaultAdminUserContext(user);
}

async function banirContaPorFraudeBonus(userId, client = pool) {
  const user =
    client === pool ? await getUserById(userId, client) : await getUserByIdForUpdate(userId, client);

  if (!user) {
    throw new Error("Usuario nao encontrado");
  }

  if (isContaBanida(user)) {
    return user;
  }

  user.statusConta = STATUS_CONTA_BANIDA;
  user.contaBanidaEm = db();
  user.motivoBanimento = MOTIVO_BANIMENTO_FRAUDE_BONUS;

  await saveUser(user, client);

  return user;
}

async function createAuditLog(
  client,
  {
    adminId = null,
    action,
    targetType,
    targetId,
    details = {},
    ipAddress = ""
  }
) {
  await client.query(
    `
    INSERT INTO audit_logs (
      id, admin_id, action, target_type, target_id, details, ip_address, created_at
    )
    VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
    `,
    [
      buildId("audit"),
      adminId,
      action,
      targetType,
      targetId,
      JSON.stringify(details || {}),
      ipAddress || "",
      db()
    ]
  );
}

async function createFinancialTransaction(
  client,
  {
    userId,
    referenceKey,
    sourceType,
    sourceId,
    operationType,
    direction,
    amount,
    status = "completed",
    description = "",
    metadata = {}
  }
) {
  const existing = await client.query(
    `
    SELECT * FROM financial_transactions
    WHERE reference_key = $1
    LIMIT 1
    `,
    [referenceKey]
  );

  if (existing.rows.length > 0) {
    return mapFinancialTransaction(existing.rows[0]);
  }

  const now = db();
  const result = await client.query(
    `
    INSERT INTO financial_transactions (
      id, user_id, reference_key, source_type, source_id,
      operation_type, direction, amount, status, description, metadata,
      created_at, updated_at
    )
    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)
    RETURNING *
    `,
    [
      buildId("ftx"),
      userId,
      referenceKey,
      sourceType,
      sourceId,
      operationType,
      direction,
      toMoney(amount),
      status,
      description,
      JSON.stringify(metadata || {}),
      now,
      now
    ]
  );

  return mapFinancialTransaction(result.rows[0]);
}

async function applyLedgerChange(
  client,
  {
    userId,
    financialTransactionId,
    entryType,
    amount,
    description = "",
    metadata = {}
  }
) {
  const user = await getUserByIdForUpdate(userId, client);

  if (!user) {
    throw new Error("Usuário não encontrado");
  }

  const value = toMoney(amount);

  if (!Number.isFinite(value) || value <= 0) {
    throw new Error("Valor inválido para ledger");
  }

  const balanceBefore = toMoney(user.saldo);
  let balanceAfter = balanceBefore;

  if (entryType === "credit") {
    balanceAfter = toMoney(balanceBefore + value);
  } else if (entryType === "debit") {
    if (balanceBefore < value) {
      throw new Error("Saldo insuficiente");
    }
    balanceAfter = toMoney(balanceBefore - value);
  } else {
    throw new Error("Tipo de lançamento inválido");
  }

  await client.query(
    `
    INSERT INTO ledger_entries (
      id, user_id, financial_transaction_id, entry_type, amount,
      balance_before, balance_after, description, metadata, created_at
    )
    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
    `,
    [
      buildId("ledger"),
      userId,
      financialTransactionId,
      entryType,
      value,
      balanceBefore,
      balanceAfter,
      description,
      JSON.stringify(metadata || {}),
      db()
    ]
  );

  user.saldo = balanceAfter;
  user.saldoAtualizadoEm = db();

  await saveUser(user, client);

  return user;
}

async function executeSigmoTransfer(
  client,
  {
    fromUserId,
    toUserId = "",
    toEmail = "",
    amount,
    channel = "app",
    metadata = {}
  }
) {
  const valorNum = toMoney(amount);

  if (!String(fromUserId || "").trim()) {
    throw new Error("Remetente nao encontrado");
  }

  if (!String(toUserId || "").trim() && !String(toEmail || "").trim()) {
    throw new Error("Usuario destino nao encontrado");
  }

  if (!Number.isFinite(valorNum) || valorNum <= 0) {
    throw new Error("Valor invalido");
  }

  const remetente = await getUserByIdForUpdate(fromUserId, client);
  const destino = String(toUserId || "").trim()
    ? await getUserById(String(toUserId || "").trim(), client)
    : await getUserByEmail(toEmail, client);

  if (!remetente) {
    throw new Error("Remetente nao encontrado");
  }

  if (!destino) {
    throw new Error("Usuario destino nao encontrado");
  }

  if (isContaBanida(remetente)) {
    const error = new Error(getMensagemContaBanida());
    error.statusCode = 403;
    error.payload = buildContaBanidaPayload(remetente);
    throw error;
  }

  if (isContaBanida(destino)) {
    const error = new Error("Conta destino indisponivel");
    error.statusCode = 403;
    error.payload = {
      error: "Conta destino indisponivel"
    };
    throw error;
  }

  if (remetente.id === destino.id) {
    throw new Error("Nao pode transferir para si mesmo");
  }

  if (toMoney(remetente.saldo) < valorNum) {
    throw new Error("Saldo insuficiente");
  }

  const remetenteContexto = await getUserFinancialContext(remetente, client);
  const bonusTransferido = Math.min(
    toMoney(remetenteContexto.saldoBonusAtual),
    valorNum
  );
  const realTransferido = toMoney(valorNum - bonusTransferido);
  const transferId = buildId("transfer");
  const now = db();
  const descricaoPrefixo = channel === "nfc" ? "Transferencia NFC" : "Transferencia";
  const metadataBase = {
    channel,
    bonusAmount: bonusTransferido,
    realAmount: realTransferido,
    ...metadata
  };

  const txSaida = await createFinancialTransaction(client, {
    userId: remetente.id,
    referenceKey: `transfer:${transferId}:debit`,
    sourceType: "transfer",
    sourceId: transferId,
    operationType: "transfer_out",
    direction: "debit",
    amount: valorNum,
    status: "completed",
    description: `${descricaoPrefixo} enviada para ${destino.email}`,
    metadata: {
      fromUserId: remetente.id,
      toUserId: destino.id,
      toEmail: destino.email,
      ...metadataBase
    }
  });

  const remetenteAtualizado = await applyLedgerChange(client, {
    userId: remetente.id,
    financialTransactionId: txSaida.id,
    entryType: "debit",
    amount: valorNum,
    description: `${descricaoPrefixo} enviada para ${destino.email}`,
    metadata: {
      transferId,
      counterpartUserId: destino.id,
      counterpartEmail: destino.email,
      ...metadataBase
    }
  });

  const txEntrada = await createFinancialTransaction(client, {
    userId: destino.id,
    referenceKey: `transfer:${transferId}:credit`,
    sourceType: "transfer",
    sourceId: transferId,
    operationType: "transfer_in",
    direction: "credit",
    amount: valorNum,
    status: "completed",
    description: `${descricaoPrefixo} recebida de ${remetente.email}`,
    metadata: {
      fromUserId: remetente.id,
      fromEmail: remetente.email,
      toUserId: destino.id,
      ...metadataBase
    }
  });

  const destinatarioAtualizado = await applyLedgerChange(client, {
    userId: destino.id,
    financialTransactionId: txEntrada.id,
    entryType: "credit",
    amount: valorNum,
    description: `${descricaoPrefixo} recebida de ${remetente.email}`,
    metadata: {
      transferId,
      counterpartUserId: remetente.id,
      counterpartEmail: remetente.email,
      ...metadataBase
    }
  });

  await saveDeposito(
    {
      id: buildId("dep"),
      userId: remetente.id,
      valor: valorNum,
      chavePix: "",
      tipoChave: "",
      tipoTransacao: "saida",
      status: "aprovado",
      comprovanteUrl: "",
      descricao: `${descricaoPrefixo} enviada para ${destino.email}`,
      criadoEm: now,
      aprovadoEm: now,
      recusadoEm: null,
      comprovanteEnviadoEm: null
    },
    client
  );

  await saveDeposito(
    {
      id: buildId("dep"),
      userId: destino.id,
      valor: valorNum,
      chavePix: "",
      tipoChave: "",
      tipoTransacao: "entrada",
      status: "aprovado",
      comprovanteUrl: "",
      descricao: `${descricaoPrefixo} recebida de ${remetente.email}`,
      criadoEm: now,
      aprovadoEm: now,
      recusadoEm: null,
      comprovanteEnviadoEm: null
    },
    client
  );

  return {
    transferId,
    remetente,
    destino,
    saldoAtualRemetente: toMoney(remetenteAtualizado.saldo),
    saldoAtualDestinatario: toMoney(destinatarioAtualizado.saldo),
    txSaida,
    txEntrada
  };
}

function authAdmin(req, res, next) {
  try {
    const auth = String(req.headers.authorization || "").trim();

    if (!auth.startsWith("Bearer ")) {
      return res.status(401).json({ error: "Não autorizado" });
    }

    const token = auth.slice(7).trim();
    const data = jwt.verify(token, JWT_SECRET);

    if (data.type !== "admin" || data.role !== "admin") {
      return res.status(401).json({ error: "Não autorizado" });
    }

    req.admin = data;
    next();
  } catch {
    return res.status(401).json({ error: "Não autorizado" });
  }
}

function authUser(req, res, next) {
  try {
    const auth = String(req.headers.authorization || "").trim();

    if (!auth.startsWith("Bearer ")) {
      return res.status(401).json({ error: "Nao autorizado" });
    }

    const token = auth.slice(7).trim();
    const data = jwt.verify(token, JWT_SECRET);

    if (data.type !== "user" || !String(data.sub || "").trim()) {
      return res.status(401).json({ error: "Nao autorizado" });
    }

    req.userAuth = data;
    req.deviceId = getRequestDeviceId(req);
    next();
  } catch {
    return res.status(401).json({ error: "Nao autorizado" });
  }
}

app.get("/", (req, res) => {
  res.json({ ok: true });
});

app.post("/admin/login", adminLoginLimiter, async (req, res) => {
  try {
    const { email, senha } = req.body;

    if (!email || !senha) {
      return res.status(400).json({ error: "Email e senha são obrigatórios" });
    }

    const admin = await getAdminByEmail(email);

    if (!admin || !admin.ativo) {
      return res.status(401).json({ error: "Login inválido" });
    }

    const ok = await bcrypt.compare(String(senha), String(admin.senha));

    if (!ok) {
      return res.status(401).json({ error: "Login inválido" });
    }

    await pool.query(
      "UPDATE admins SET ultimo_login_em = $1 WHERE id = $2",
      [db(), admin.id]
    );

    const token = signToken(admin);
    res.json({ token });
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: "Erro no login admin" });
  }
});

app.post("/register", async (req, res) => {
  try {
    const { email, senha } = req.body;

    if (!email || !senha) {
      return res.status(400).json({ error: "Email e senha são obrigatórios" });
    }

    const emailNorm = normalizeEmail(email);

    const exists = await pool.query(
      "SELECT id FROM usuarios WHERE email = $1 LIMIT 1",
      [emailNorm]
    );

    if (exists.rows.length > 0) {
      return res.status(400).json({ error: "Usuário já existe" });
    }

    const hash = await bcrypt.hash(String(senha), 10);
    const novoUsuario = await runInTransaction(async (client) => {
      const user = {
        id: buildId("user"),
        nome: emailNorm.split("@")[0],
        email: emailNorm,
        senha: hash,
        saldo: 0,
        criadoEm: db(),
        nomeAtualizadoEm: null,
        saldoAtualizadoEm: null,
        senhaAtualizadaEm: null,
        statusConta: STATUS_CONTA_ATIVA,
        contaBanidaEm: null,
        motivoBanimento: "",
        bonusBoasVindas: BONUS_BOAS_VINDAS_VALOR,
        bonusBoasVindasConcedidoEm: db()
      };

      await saveUser(user, client);

      const txBonus = await createFinancialTransaction(client, {
        userId: user.id,
        referenceKey: `welcome-bonus:${user.id}`,
        sourceType: "welcome_bonus",
        sourceId: user.id,
        operationType: "welcome_bonus",
        direction: "credit",
        amount: BONUS_BOAS_VINDAS_VALOR,
        status: "completed",
        description: "Saldo de boas-vindas Sigmo",
        metadata: {
          tipoBonus: "boas_vindas"
        }
      });

      const userAtualizado = await applyLedgerChange(client, {
        userId: user.id,
        financialTransactionId: txBonus.id,
        entryType: "credit",
        amount: BONUS_BOAS_VINDAS_VALOR,
        description: "Saldo de boas-vindas Sigmo",
        metadata: {
          tipoBonus: "boas_vindas"
        }
      });

      await saveDeposito(
        {
          id: buildId("dep"),
          userId: user.id,
          valor: BONUS_BOAS_VINDAS_VALOR,
          chavePix: "",
          tipoChave: "",
          tipoTransacao: "entrada",
          status: "aprovado",
          comprovanteUrl: "",
          descricao: "Saldo de boas-vindas Sigmo",
          repassarTaxa: false,
          taxaPix: 0,
          valorLiquidoPix: 0,
          valorDebitadoPix: 0,
          criadoEm: db(),
          aprovadoEm: db(),
          recusadoEm: null,
          comprovanteEnviadoEm: null
        },
        client
      );

      return userAtualizado;
    });

    res.status(201).json(
      buildUserPublicResponse(novoUsuario, {
        pixDesbloqueado: false,
        valorRecebidoViaPix: 0,
        valorMinimoDesbloqueioPix: PIX_SAQUE_DESBLOQUEIO_MIN,
        welcomeBonusGranted: true
      })
    );
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: "Erro ao cadastrar" });
  }
});

app.post("/login", loginLimiter, async (req, res) => {
  try {
    const { email, senha } = req.body;

    if (!email || !senha) {
      return res.status(400).json({ error: "Email e senha são obrigatórios" });
    }

    const user = await getUserByEmail(email);

    if (!user) {
      return res.status(401).json({ error: "Login inválido" });
    }

    const ok = await bcrypt.compare(String(senha), String(user.senha));

    if (!ok) {
      return res.status(401).json({ error: "Login inválido" });
    }

    res.json(await buildUserPublicResponseWithPix(user));
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: "Erro no login" });
  }
});

app.post("/mobile/login", loginLimiter, async (req, res) => {
  try {
    const { email, senha } = req.body;

    if (!email || !senha) {
      return sendJsonError(
        res,
        400,
        "AUTH_REQUIRED_FIELDS",
        "Email e senha sao obrigatorios"
      );
    }

    const user = await getUserByEmail(email);

    if (!user) {
      return sendJsonError(res, 401, "AUTH_INVALID", "Login invalido");
    }

    const ok = await bcrypt.compare(String(senha), String(user.senha));

    if (!ok) {
      return sendJsonError(res, 401, "AUTH_INVALID", "Login invalido");
    }

    const token = signUserToken(user);
    const userResponse = await buildUserPublicResponseWithPix(user, pool, {
      deviceId: getRequestDeviceId(req)
    });

    res.json(
      buildUserMobileAuthResponse(userResponse, token, {
        serverTime: db()
      })
    );
  } catch (error) {
    console.error(error);
    sendJsonError(res, 500, "AUTH_ERROR", "Erro no login mobile");
  }
});

app.get("/mobile/me", authUser, async (req, res) => {
  try {
    const user = await getUserById(req.userAuth.sub);

    if (!user) {
      return sendJsonError(res, 404, "USER_NOT_FOUND", "Usuario nao encontrado");
    }

    res.json({
      user: await buildUserPublicResponseWithPix(user, pool, {
        deviceId: req.deviceId
      }),
      serverTime: db()
    });
  } catch (error) {
    console.error(error);
    sendJsonError(res, 500, "USER_FETCH_ERROR", "Erro ao buscar usuario mobile");
  }
});

app.get("/sigmo-cards", async (req, res) => {
  try {
    const ownerUserId = String(req.query?.userId || "").trim();

    if (!ownerUserId) {
      return sendJsonError(res, 400, "CARD_OWNER_REQUIRED", "Usuario nao informado");
    }

    const cards = await runInTransaction(async (client) => {
      const owner = await getUserByIdForUpdate(ownerUserId, client);

      if (!owner) {
        return { statusCode: 404, code: "USER_NOT_FOUND", error: "Usuario nao encontrado" };
      }

      if (isContaBanida(owner)) {
        return { statusCode: 403, payload: buildContaBanidaPayload(owner) };
      }

      await ensurePrimarySigmoCard(owner, client);
      return listSigmoCardsByOwner(owner.id, client);
    });

    if (cards?.statusCode) {
      if (cards.payload) {
        return res.status(cards.statusCode).json(cards.payload);
      }
      return sendJsonError(res, cards.statusCode, cards.code, cards.error);
    }

    const relatedUsers = await listUsersByIds(
      cards.flatMap((card) => [card.ownerUserId, card.holderUserId])
    );
    const userMap = new Map(relatedUsers.map((item) => [item.id, item]));

    res.json(
      cards.map((card) =>
        buildSigmoCardResponse(
          card,
          userMap.get(card.ownerUserId),
          userMap.get(card.holderUserId)
        )
      )
    );
  } catch (error) {
    console.error(error);
    sendJsonError(res, 500, "CARD_LIST_ERROR", "Erro ao carregar cartoes");
  }
});

app.post("/sigmo-cards/primary", async (req, res) => {
  try {
    const ownerUserId = String(req.body?.userId || "").trim();
    const label = String(req.body?.label || "").trim();
    const spendingLimit = Math.max(0, toMoney(req.body?.spendingLimit));

    if (!ownerUserId) {
      return sendJsonError(res, 400, "CARD_OWNER_REQUIRED", "Usuario nao informado");
    }

    const result = await runInTransaction(async (client) => {
      const owner = await getUserByIdForUpdate(ownerUserId, client);

      if (!owner) {
        return { statusCode: 404, code: "USER_NOT_FOUND", error: "Usuario nao encontrado" };
      }

      if (isContaBanida(owner)) {
        return { statusCode: 403, payload: buildContaBanidaPayload(owner) };
      }

      let card = await ensurePrimarySigmoCard(owner, client);
      card = {
        ...card,
        label: label || card.label || "Cartao principal",
        spendingLimit,
        updatedAt: db()
      };

      await saveSigmoCard(card, client);

      return {
        owner,
        card
      };
    });

    if (result?.statusCode) {
      if (result.payload) {
        return res.status(result.statusCode).json(result.payload);
      }
      return sendJsonError(res, result.statusCode, result.code, result.error);
    }

    res.json(buildSigmoCardResponse(result.card, result.owner, result.owner));
  } catch (error) {
    console.error(error);
    sendJsonError(res, 500, "CARD_PRIMARY_ERROR", "Erro ao atualizar cartao principal");
  }
});

app.post("/sigmo-cards/additional", async (req, res) => {
  try {
    const ownerUserId = String(req.body?.userId || "").trim();
    const holderEmail = String(req.body?.holderEmail || "").trim();
    const label = String(req.body?.label || "").trim();
    const spendingLimit = Math.max(0, toMoney(req.body?.spendingLimit));

    if (!ownerUserId || !holderEmail) {
      return sendJsonError(
        res,
        400,
        "CARD_REQUIRED_FIELDS",
        "Usuario e email do portador sao obrigatorios"
      );
    }

    const result = await runInTransaction(async (client) => {
      const owner = await getUserByIdForUpdate(ownerUserId, client);
      const holder = await getUserByEmail(holderEmail, client);

      if (!owner) {
        return { statusCode: 404, code: "USER_NOT_FOUND", error: "Usuario nao encontrado" };
      }

      if (isContaBanida(owner)) {
        return { statusCode: 403, payload: buildContaBanidaPayload(owner) };
      }

      if (!holder) {
        return {
          statusCode: 404,
          code: "CARD_HOLDER_NOT_FOUND",
          error: "Usuario portador nao encontrado"
        };
      }

      if (isContaBanida(holder)) {
        return {
          statusCode: 403,
          code: "CARD_HOLDER_UNAVAILABLE",
          error: "Conta do portador indisponivel"
        };
      }

      if (holder.id === owner.id) {
        return {
          statusCode: 400,
          code: "CARD_PRIMARY_ALREADY_EXISTS",
          error: "Use o cartao principal para o proprio titular"
        };
      }

      const card = {
        id: buildId("card"),
        ownerUserId: owner.id,
        holderUserId: holder.id,
        cardType: "additional",
        label: label || `Cartao de ${getUserDisplayName(holder)}`,
        status: "active",
        spendingLimit,
        deviceId: "",
        claimToken: buildSigmoCardClaimToken(),
        boundAt: null,
        lastUsedAt: null,
        metadata: {
          createdBy: owner.id
        },
        createdAt: db(),
        updatedAt: db()
      };

      await saveSigmoCard(card, client);

      return { owner, holder, card };
    });

    if (result?.statusCode) {
      if (result.payload) {
        return res.status(result.statusCode).json(result.payload);
      }
      return sendJsonError(res, result.statusCode, result.code, result.error);
    }

    res.status(201).json(buildSigmoCardResponse(result.card, result.owner, result.holder));
  } catch (error) {
    console.error(error);
    sendJsonError(res, 500, "CARD_CREATE_ERROR", "Erro ao criar cartao adicional");
  }
});

app.post("/sigmo-cards/:id", async (req, res) => {
  try {
    const ownerUserId = String(req.body?.userId || "").trim();

    if (!ownerUserId) {
      return sendJsonError(res, 400, "CARD_OWNER_REQUIRED", "Usuario nao informado");
    }

    const result = await runInTransaction(async (client) => {
      const owner = await getUserByIdForUpdate(ownerUserId, client);
      let card = await getSigmoCardByIdForUpdate(req.params.id, client);

      if (!owner) {
        return { statusCode: 404, code: "USER_NOT_FOUND", error: "Usuario nao encontrado" };
      }

      if (isContaBanida(owner)) {
        return { statusCode: 403, payload: buildContaBanidaPayload(owner) };
      }

      if (!card || card.ownerUserId !== owner.id) {
        return { statusCode: 404, code: "CARD_NOT_FOUND", error: "Cartao nao encontrado" };
      }

      const nextLabel = String(req.body?.label || "").trim();
      const nextStatus = req.body?.status ? normalizeSigmoCardStatus(req.body.status) : card.status;
      const hasLimit =
        req.body?.spendingLimit !== undefined && req.body?.spendingLimit !== null && req.body?.spendingLimit !== "";

      card = {
        ...card,
        label: nextLabel || card.label,
        status: nextStatus,
        spendingLimit: hasLimit ? Math.max(0, toMoney(req.body?.spendingLimit)) : card.spendingLimit,
        updatedAt: db()
      };

      await saveSigmoCard(card, client);

      return {
        owner,
        holder: await getUserById(card.holderUserId, client),
        card
      };
    });

    if (result?.statusCode) {
      if (result.payload) {
        return res.status(result.statusCode).json(result.payload);
      }
      return sendJsonError(res, result.statusCode, result.code, result.error);
    }

    res.json(buildSigmoCardResponse(result.card, result.owner, result.holder));
  } catch (error) {
    console.error(error);
    sendJsonError(res, 500, "CARD_UPDATE_ERROR", "Erro ao atualizar cartao");
  }
});

app.post("/sigmo-cards/:id/reissue", async (req, res) => {
  try {
    const ownerUserId = String(req.body?.userId || "").trim();

    if (!ownerUserId) {
      return sendJsonError(res, 400, "CARD_OWNER_REQUIRED", "Usuario nao informado");
    }

    const result = await runInTransaction(async (client) => {
      const owner = await getUserByIdForUpdate(ownerUserId, client);
      let card = await getSigmoCardByIdForUpdate(req.params.id, client);

      if (!owner) {
        return { statusCode: 404, code: "USER_NOT_FOUND", error: "Usuario nao encontrado" };
      }

      if (isContaBanida(owner)) {
        return { statusCode: 403, payload: buildContaBanidaPayload(owner) };
      }

      if (!card || card.ownerUserId !== owner.id) {
        return { statusCode: 404, code: "CARD_NOT_FOUND", error: "Cartao nao encontrado" };
      }

      card = {
        ...card,
        deviceId: "",
        boundAt: null,
        claimToken: buildSigmoCardClaimToken(),
        updatedAt: db()
      };

      await saveSigmoCard(card, client);

      return {
        owner,
        holder: await getUserById(card.holderUserId, client),
        card
      };
    });

    if (result?.statusCode) {
      if (result.payload) {
        return res.status(result.statusCode).json(result.payload);
      }
      return sendJsonError(res, result.statusCode, result.code, result.error);
    }

    res.json(buildSigmoCardResponse(result.card, result.owner, result.holder));
  } catch (error) {
    console.error(error);
    sendJsonError(res, 500, "CARD_REISSUE_ERROR", "Erro ao liberar cartao para outro aparelho");
  }
});

app.get("/mobile/card", authUser, async (req, res) => {
  try {
    const user = await getUserById(req.userAuth.sub);

    if (!user) {
      return sendJsonError(res, 404, "USER_NOT_FOUND", "Usuario nao encontrado");
    }

    if (isContaBanida(user)) {
      return res.status(403).json(buildContaBanidaPayload(user));
    }

    res.json({
      card: await buildUserActiveCardResponse(user, req.deviceId)
    });
  } catch (error) {
    console.error(error);
    sendJsonError(res, 500, "MOBILE_CARD_ERROR", "Erro ao carregar cartao do aparelho");
  }
});

app.post("/mobile/cards/claim", authUser, async (req, res) => {
  try {
    const deviceId = String(req.deviceId || "").trim();
    const cardId = String(req.body?.cardId || "").trim();
    const claimToken = String(req.body?.claimToken || "").trim();

    if (!deviceId) {
      return sendJsonError(
        res,
        400,
        "DEVICE_ID_REQUIRED",
        "Este aparelho ainda nao foi identificado pela Sigmo"
      );
    }

    if (!cardId || !claimToken) {
      return sendJsonError(
        res,
        400,
        "CARD_CLAIM_REQUIRED",
        "Cartao e token de liberacao sao obrigatorios"
      );
    }

    const result = await runInTransaction(async (client) => {
      const holder = await getUserByIdForUpdate(req.userAuth.sub, client);
      let card = await getSigmoCardByIdForUpdate(cardId, client);

      if (!holder) {
        return { statusCode: 404, code: "USER_NOT_FOUND", error: "Usuario nao encontrado" };
      }

      if (isContaBanida(holder)) {
        return { statusCode: 403, payload: buildContaBanidaPayload(holder) };
      }

      if (!card) {
        return { statusCode: 404, code: "CARD_NOT_FOUND", error: "Cartao nao encontrado" };
      }

      if (card.holderUserId !== holder.id) {
        return {
          statusCode: 403,
          code: "CARD_HOLDER_MISMATCH",
          error: "Este cartao nao foi liberado para esta conta"
        };
      }

      if (card.claimToken !== claimToken) {
        return {
          statusCode: 403,
          code: "CARD_CLAIM_INVALID",
          error: "Link de liberacao invalido ou expirado"
        };
      }

      if (card.status !== "active") {
        return {
          statusCode: 409,
          code: "CARD_BLOCKED",
          error: "Este cartao esta bloqueado"
        };
      }

      if (card.deviceId && card.deviceId !== deviceId) {
        return {
          statusCode: 409,
          code: "CARD_ALREADY_BOUND",
          error: "Este cartao ja esta liberado em outro aparelho"
        };
      }

      const owner = await getUserById(card.ownerUserId, client);

      if (!owner) {
        return {
          statusCode: 404,
          code: "CARD_OWNER_NOT_FOUND",
          error: "Titular do cartao nao encontrado"
        };
      }

      if (isContaBanida(owner)) {
        return {
          statusCode: 403,
          code: "CARD_OWNER_UNAVAILABLE",
          error: "Titular do cartao indisponivel"
        };
      }

      const holderCards = await getSigmoCardsByHolder(holder.id, client);
      for (const holderCard of holderCards) {
        if (holderCard.id === card.id || holderCard.deviceId !== deviceId) continue;
        await saveSigmoCard(
          {
            ...holderCard,
            deviceId: "",
            boundAt: null,
            updatedAt: db()
          },
          client
        );
      }

      card = {
        ...card,
        deviceId,
        boundAt: db(),
        updatedAt: db()
      };

      await saveSigmoCard(card, client);

      return {
        holder,
        owner,
        card
      };
    });

    if (result?.statusCode) {
      if (result.payload) {
        return res.status(result.statusCode).json(result.payload);
      }
      return sendJsonError(res, result.statusCode, result.code, result.error);
    }

    res.json({
      card: buildSigmoCardResponse(result.card, result.owner, result.holder),
      user: await buildUserPublicResponseWithPix(result.holder, pool, {
        deviceId
      })
    });
  } catch (error) {
    console.error(error);
    sendJsonError(res, 500, "CARD_CLAIM_ERROR", "Erro ao liberar cartao neste aparelho");
  }
});

app.post("/sigmo-tap-charges", async (req, res) => {
  try {
    const receiverUserId = String(req.body?.userId || req.body?.receiverUserId || "").trim();
    const amount = toMoney(req.body?.amount);
    const description = String(req.body?.description || "").trim();

    if (!receiverUserId) {
      return sendJsonError(res, 400, "TAP_CHARGE_USER_REQUIRED", "Usuario nao informado");
    }

    if (!Number.isFinite(amount) || amount <= 0) {
      return sendJsonError(res, 400, "TAP_CHARGE_AMOUNT_INVALID", "Valor invalido");
    }

    const result = await runInTransaction(async (client) => {
      const receiver = await getUserByIdForUpdate(receiverUserId, client);

      if (!receiver) {
        return { statusCode: 404, code: "USER_NOT_FOUND", error: "Usuario nao encontrado" };
      }

      if (isContaBanida(receiver)) {
        return {
          statusCode: 403,
          payload: buildContaBanidaPayload(receiver)
        };
      }

      const now = new Date();
      const charge = {
        id: buildId("tapcharge"),
        publicCode: crypto.randomBytes(6).toString("hex").toUpperCase(),
        receiverUserId: receiver.id,
        status: "pending",
        amount,
        description,
        expiresAt: db(addSeconds(now, SIGMO_TAP_CHARGE_TTL_SECONDS)),
        nfcSessionId: "",
        payerUserId: "",
        financialTransactionId: "",
        paidAt: null,
        cancelledAt: null,
        metadata: {
          source: "web",
          receiverName: getUserDisplayName(receiver),
          receiverEmail: receiver.email
        },
        createdAt: db(now),
        updatedAt: db(now)
      };

      await saveSigmoTapCharge(charge, client);
      return { charge, receiver };
    });

    if (result?.payload || result?.statusCode) {
      return res
        .status(result.statusCode || 400)
        .json(result.payload || { code: result.code, error: result.error });
    }

    res.status(201).json(buildSigmoTapChargeResponse(result.charge, result.receiver));
  } catch (error) {
    console.error(error);
    sendJsonError(res, 500, "TAP_CHARGE_CREATE_ERROR", "Erro ao criar cobranca por aproximacao");
  }
});

app.get("/sigmo-tap-charges/:id", async (req, res) => {
  try {
    const userId = String(req.query?.userId || "").trim();
    const charge = await getSigmoTapChargeById(req.params.id);

    if (!charge) {
      return sendJsonError(res, 404, "TAP_CHARGE_NOT_FOUND", "Cobranca nao encontrada");
    }

    if (!userId || charge.receiverUserId !== userId) {
      return sendJsonError(res, 403, "TAP_CHARGE_FORBIDDEN", "Cobranca indisponivel");
    }

    const syncedCharge = await syncSigmoTapChargeStatus(charge);
    const receiver = await getUserById(syncedCharge.receiverUserId);
    res.json(buildSigmoTapChargeResponse(syncedCharge, receiver));
  } catch (error) {
    console.error(error);
    sendJsonError(res, 500, "TAP_CHARGE_FETCH_ERROR", "Erro ao consultar cobranca");
  }
});

app.post("/sigmo-tap-charges/:id/cancel", async (req, res) => {
  try {
    const userId = String(req.body?.userId || "").trim();

    if (!userId) {
      return sendJsonError(res, 400, "TAP_CHARGE_USER_REQUIRED", "Usuario nao informado");
    }

    const result = await runInTransaction(async (client) => {
      let charge = await getSigmoTapChargeByIdForUpdate(req.params.id, client);

      if (!charge) {
        return {
          statusCode: 404,
          code: "TAP_CHARGE_NOT_FOUND",
          error: "Cobranca nao encontrada"
        };
      }

      if (charge.receiverUserId !== userId) {
        return {
          statusCode: 403,
          code: "TAP_CHARGE_FORBIDDEN",
          error: "Cobranca indisponivel"
        };
      }

      charge = await syncSigmoTapChargeStatus(charge, client);

      if (charge.status === "paid") {
        return {
          statusCode: 409,
          code: "TAP_CHARGE_ALREADY_PAID",
          error: "Cobranca ja foi paga"
        };
      }

      charge = {
        ...charge,
        status: "cancelled",
        cancelledAt: db(),
        updatedAt: db()
      };

      await saveSigmoTapCharge(charge, client);

      if (charge.nfcSessionId) {
        const session = await getNfcReceiveSessionByIdForUpdate(charge.nfcSessionId, client);
        if (session && session.status === "pending") {
          await saveNfcReceiveSession(
            {
              ...session,
              status: "cancelled",
              cancelledAt: db(),
              updatedAt: db()
            },
            client
          );
        }
      }

      return { charge };
    });

    if (result?.statusCode) {
      return sendJsonError(res, result.statusCode, result.code, result.error);
    }

    const receiver = await getUserById(result.charge.receiverUserId);
    res.json(buildSigmoTapChargeResponse(result.charge, receiver));
  } catch (error) {
    console.error(error);
    sendJsonError(res, 500, "TAP_CHARGE_CANCEL_ERROR", "Erro ao cancelar cobranca");
  }
});

app.get("/mobile/tap-charges/:id", authUser, async (req, res) => {
  try {
    let charge = await getSigmoTapChargeById(req.params.id);

    if (!charge) {
      return sendJsonError(res, 404, "TAP_CHARGE_NOT_FOUND", "Cobranca nao encontrada");
    }

    if (charge.receiverUserId !== req.userAuth.sub) {
      return sendJsonError(res, 403, "TAP_CHARGE_FORBIDDEN", "Cobranca indisponivel");
    }

    charge = await syncSigmoTapChargeStatus(charge);
    const receiver = await getUserById(charge.receiverUserId);
    res.json(buildSigmoTapChargeResponse(charge, receiver));
  } catch (error) {
    console.error(error);
    sendJsonError(res, 500, "TAP_CHARGE_FETCH_ERROR", "Erro ao consultar cobranca");
  }
});

app.post("/mobile/tap-charges/:id/arm", authUser, async (req, res) => {
  try {
    const ttlSeconds = Math.min(
      120,
      Math.max(15, Number(req.body?.ttlSeconds || NFC_RECEIVE_SESSION_TTL_SECONDS))
    );

    const result = await runInTransaction(async (client) => {
      const receiver = await getUserByIdForUpdate(req.userAuth.sub, client);
      let charge = await getSigmoTapChargeByIdForUpdate(req.params.id, client);

      if (!receiver) {
        return { statusCode: 404, code: "USER_NOT_FOUND", error: "Usuario nao encontrado" };
      }

      if (!charge) {
        return {
          statusCode: 404,
          code: "TAP_CHARGE_NOT_FOUND",
          error: "Cobranca nao encontrada"
        };
      }

      if (charge.receiverUserId !== receiver.id) {
        return {
          statusCode: 403,
          code: "TAP_CHARGE_FORBIDDEN",
          error: "Cobranca indisponivel"
        };
      }

      if (isContaBanida(receiver)) {
        return {
          statusCode: 403,
          payload: buildContaBanidaPayload(receiver)
        };
      }

      charge = await syncSigmoTapChargeStatus(charge, client);

      if (charge.status === "paid") {
        return {
          statusCode: 409,
          code: "TAP_CHARGE_ALREADY_PAID",
          error: "Cobranca ja foi paga"
        };
      }

      if (charge.status === "cancelled" || charge.status === "expired") {
        return {
          statusCode: 409,
          code: "TAP_CHARGE_UNAVAILABLE",
          error: "Cobranca indisponivel"
        };
      }

      await cancelPendingNfcReceiveSessionsByReceiver(receiver.id, client);

      const now = new Date();
      const session = {
        id: buildId("nfcsess"),
        publicToken: crypto.randomBytes(16).toString("hex"),
        receiverUserId: receiver.id,
        status: "pending",
        nonce: crypto.randomBytes(8).toString("hex"),
        protocolVersion: NFC_PROTOCOL_VERSION,
        expiresAt: db(addSeconds(now, ttlSeconds)),
        consumedAt: null,
        cancelledAt: null,
        payerUserId: "",
        amount: charge.amount,
        financialTransactionId: "",
        readCount: 0,
        lastReadAt: null,
        metadata: {
          receiverName: getUserDisplayName(receiver),
          receiverEmail: receiver.email,
          channel: "nfc",
          chargeId: charge.id,
          chargePublicCode: charge.publicCode,
          fixedAmount: charge.amount,
          chargeDescription: charge.description || ""
        },
        createdAt: db(now),
        updatedAt: db(now)
      };

      await saveNfcReceiveSession(session, client);

      charge = {
        ...charge,
        status: "armed",
        nfcSessionId: session.id,
        updatedAt: db(now)
      };

      await saveSigmoTapCharge(charge, client);
      return { charge, session, receiver };
    });

    if (result?.payload || result?.statusCode) {
      return res
        .status(result.statusCode || 400)
        .json(result.payload || { code: result.code, error: result.error });
    }

    res.json({
      charge: buildSigmoTapChargeResponse(result.charge, result.receiver),
      session: buildNfcReceiveSessionResponse(result.session, result.receiver, {
        charge: buildSigmoTapChargeResponse(result.charge, result.receiver),
        fixedAmount: toMoney(result.charge.amount),
        requiresPin: false,
        requiresDeviceAuth: true,
        confirmationMode: "device_auth"
      })
    });
  } catch (error) {
    console.error(error);
    sendJsonError(res, 500, "TAP_CHARGE_ARM_ERROR", "Erro ao ativar cobranca por aproximacao");
  }
});

app.get("/usuario/:id", async (req, res) => {
  try {
    const user = await getUserById(req.params.id);

    if (!user) {
      return res.status(404).json({ error: "Usuário não encontrado" });
    }

    res.json(await buildUserPublicResponseWithPix(user));
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: "Erro ao buscar usuário" });
  }
});

app.post("/usuario/update-nome", async (req, res) => {
  try {
    const { userId, nome } = req.body;

    if (!userId || !nome) {
      return res.status(400).json({ error: "Dados inválidos" });
    }

    const user = await getUserById(userId);

    if (!user) {
      return res.status(404).json({ error: "Usuário não encontrado" });
    }

    if (isContaBanida(user)) {
      return res.status(403).json(buildContaBanidaPayload(user));
    }

    user.nome = String(nome).trim();
    user.nomeAtualizadoEm = db();

    await saveUser(user);

    res.json({ message: "Nome atualizado com sucesso" });
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: "Erro ao atualizar nome" });
  }
});

app.post("/usuario/delete", async (req, res) => {
  try {
    const { userId, email, senha } = req.body;

    if (!userId || !email || !senha) {
      return res.status(400).json({ error: "Dados obrigatórios" });
    }

    const user = await getUserById(userId);

    if (!user) {
      return res.status(404).json({ error: "Usuário não encontrado" });
    }

    if (isContaBanida(user)) {
      return res.status(403).json(buildContaBanidaPayload(user));
    }

    if (normalizeEmail(email) !== normalizeEmail(user.email)) {
      return res.status(401).json({ error: "Email inválido" });
    }

    const senhaValida = await bcrypt.compare(String(senha), String(user.senha));

    if (!senhaValida) {
      return res.status(401).json({ error: "Senha inválida" });
    }

    await runInTransaction(async (client) => {
      await client.query("DELETE FROM depositos WHERE user_id = $1", [userId]);
      await client.query("DELETE FROM financial_transactions WHERE user_id = $1", [userId]);
      await client.query("DELETE FROM ledger_entries WHERE user_id = $1", [userId]);
      await client.query("DELETE FROM audit_logs WHERE target_id = $1", [userId]);
      await client.query("DELETE FROM usuarios WHERE id = $1", [userId]);
    });

    res.json({ message: "Conta deletada com sucesso" });
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: "Erro ao deletar conta" });
  }
});

app.post("/deposito", async (req, res) => {
  try {
    const {
      userId,
      valor,
      chavePix,
      tipoChave,
      tipoTransacao,
      repassarTaxa
    } = req.body;

    if (!userId || valor === undefined || valor === null) {
      return res.status(400).json({ error: "userId e valor são obrigatórios" });
    }

    const valorNumero = toMoney(valor);
    const tipoTransacaoNormalizado =
      String(tipoTransacao || "entrada").trim().toLowerCase() === "saida"
        ? "saida"
        : "entrada";
    const isSaida = tipoTransacaoNormalizado === "saida";

    if (!Number.isFinite(valorNumero) || valorNumero <= 0) {
      return res.status(400).json({ error: "Valor inválido" });
    }

    if (isSaida) {
      if (valorNumero < LIMITE_SAQUE_PIX_MIN || valorNumero > LIMITE_SAQUE_PIX_MAX) {
        return res.status(400).json({
          error: `Saque via Pix disponível entre R$${LIMITE_SAQUE_PIX_MIN.toFixed(2)} e R$${LIMITE_SAQUE_PIX_MAX.toFixed(2)}`
        });
      }

      if (!String(chavePix || "").trim()) {
        return res.status(400).json({ error: "Chave Pix obrigatória para saque" });
      }
    } else if (
      valorNumero < LIMITE_DEPOSITO_MIN ||
      valorNumero > LIMITE_DEPOSITO_MAX
    ) {
      return res.status(400).json({
        error: `Depósito disponível entre R$${LIMITE_DEPOSITO_MIN.toFixed(2)} e R$${LIMITE_DEPOSITO_MAX.toFixed(2)}`
      });
    }

    const user = await getUserById(userId);

    if (!user) {
      return res.status(404).json({ error: "Usuário não encontrado" });
    }

    if (isContaBanida(user)) {
      return res.status(403).json(buildContaBanidaPayload(user));
    }

    if (isSaida) {
      const valorRecebidoViaPix = await getValorRecebidoViaPix(user.id);

      if (valorRecebidoViaPix < PIX_SAQUE_DESBLOQUEIO_MIN) {
        return res.status(403).json(buildPixUnlockPayload(valorRecebidoViaPix));
      }

      const origemFraude = await encontrarOrigemFraudeBonus(user.id);

      if (origemFraude) {
        const userBanido = await banirContaPorFraudeBonus(user.id);

        await createAuditLog(pool, {
          action: "ban_user_bonus_fraud",
          targetType: "usuario",
          targetId: userBanido.id,
          details: {
            userId: userBanido.id,
            origemFraudeUserId: origemFraude.fromUserId,
            origemFraudeEmail: origemFraude.fromEmail,
            valorRecebidoOrigemFraude: origemFraude.totalAmount,
            valorRecebidoViaPix
          },
          ipAddress: getRequestIp(req)
        });

        return res.status(403).json({
          ...buildContaBanidaPayload(userBanido, "ACCOUNT_BANNED_FRAUD"),
          fraudSourceUserId: origemFraude.fromUserId,
          fraudSourceEmail: origemFraude.fromEmail
        });
      }
    }

    const detalhesSaque = isSaida
      ? calcularDetalhesSaquePix(valorNumero, repassarTaxa)
      : null;

    if (detalhesSaque && toMoney(user.saldo) < detalhesSaque.valorDebitado) {
      return res.status(400).json({ error: "Saldo insuficiente" });
    }

    const pedido = {
      id: buildId("dep"),
      userId,
      valor: valorNumero,
      chavePix: chavePix || "",
      tipoChave: tipoChave || "",
      tipoTransacao: tipoTransacaoNormalizado,
      status: "pendente",
      comprovanteUrl: "",
      descricao: detalhesSaque
        ? `Saque Pix solicitado | Repassar taxa: ${detalhesSaque.repassarTaxa ? "sim" : "nao"} | Taxa: R$${detalhesSaque.taxa.toFixed(2)} | Valor liquido: R$${detalhesSaque.valorLiquido.toFixed(2)} | Valor debitado: R$${detalhesSaque.valorDebitado.toFixed(2)}`
        : "",
      repassarTaxa: detalhesSaque?.repassarTaxa || false,
      taxaPix: detalhesSaque?.taxa || 0,
      valorLiquidoPix: detalhesSaque?.valorLiquido || 0,
      valorDebitadoPix: detalhesSaque?.valorDebitado || 0,
      criadoEm: db(),
      aprovadoEm: null,
      recusadoEm: null,
      comprovanteEnviadoEm: null
    };

    await saveDeposito(pedido);

    res.status(201).json({
      ...pedido,
      repassarTaxa: detalhesSaque?.repassarTaxa || false,
      taxaPix: detalhesSaque?.taxa || 0,
      valorLiquidoPix: detalhesSaque?.valorLiquido || 0,
      valorDebitadoPix: detalhesSaque?.valorDebitado || 0
    });
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: "Erro ao criar pedido" });
  }
});

app.post("/deposito/pix-code", async (req, res) => {
  try {
    const { userId, valor } = req.body;

    if (!userId || valor === undefined || valor === null) {
      return res.status(400).json({ error: "userId e valor são obrigatórios" });
    }

    const valorNumero = toMoney(valor);

    if (!Number.isFinite(valorNumero) || valorNumero <= 0) {
      return res.status(400).json({ error: "Valor inválido" });
    }

    if (valorNumero < LIMITE_DEPOSITO_MIN || valorNumero > LIMITE_DEPOSITO_MAX) {
      return res.status(400).json({
        error: `Depósito disponível entre R$${LIMITE_DEPOSITO_MIN.toFixed(2)} e R$${LIMITE_DEPOSITO_MAX.toFixed(2)}`
      });
    }

    const user = await getUserById(userId);

    if (!user) {
      return res.status(404).json({ error: "Usuário não encontrado" });
    }

    if (isContaBanida(user)) {
      return res.status(403).json(buildContaBanidaPayload(user));
    }

    const pix = await gerarPixDentpegPublico(valorNumero);

    res.json({
      message: "Chave PIX gerada com sucesso",
      valor: valorNumero,
      ...pix
    });
  } catch (error) {
    console.error("Erro ao gerar chave PIX:", error);
    res.status(500).json({
      error: error.message || "Erro ao gerar chave PIX"
    });
  }
});

app.post("/deposito/:id/comprovante", upload.single("comprovante"), async (req, res) => {
  try {
    const pedido = await getDepositoById(req.params.id);

    if (!pedido) {
      return res.status(404).json({ error: "Pedido não encontrado" });
    }

    if (!req.file) {
      return res.status(400).json({ error: "Arquivo obrigatório" });
    }

    const user = await getUserById(pedido.userId);

    if (!user) {
      return res.status(404).json({ error: "Usuario nao encontrado" });
    }

    if (isContaBanida(user)) {
      return res.status(403).json(buildContaBanidaPayload(user));
    }

    pedido.comprovanteUrl = "/uploads/" + req.file.filename;

// 🔥 OCR REAL
    const caminho = path.join(UPLOADS_DIR, req.file.filename);
    const texto = await extrairTextoComprovante(caminho, req.file.mimetype);

    pedido.comprovanteTexto = texto;
    pedido.comprovanteEnviadoEm = db();

    await saveDeposito(pedido);

    res.json({
      message: "Comprovante enviado com sucesso",
      ocrExtraido: Boolean(texto),
      ocrAviso: texto
        ? null
        : "Nao foi possivel extrair texto suficiente do comprovante enviado",
      pedido
    });
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: "Erro upload" });
  }
});

app.get("/depositos/user/:id", async (req, res) => {
  try {
    const lista = await listDepositosByUser(req.params.id);
    res.json(lista);
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: "Erro ao buscar depósitos do usuário" });
  }
});

app.post("/transferir-sigmo", async (req, res) => {
  try {
    const { fromUserId, emailDestino, valor } = req.body;

    if (!fromUserId || !emailDestino || valor === undefined || valor === null) {
      return res.status(400).json({ error: "Dados obrigatórios" });
    }

    const valorNum = toMoney(valor);

    if (!Number.isFinite(valorNum) || valorNum <= 0) {
      return res.status(400).json({ error: "Valor inválido" });
    }

    const result = await runInTransaction(async (client) => {
      const remetente = await getUserByIdForUpdate(fromUserId, client);
      const destino = await getUserByEmail(emailDestino, client);

      if (!remetente) {
        throw new Error("Remetente não encontrado");
      }

      if (!destino) {
        throw new Error("Usuário destino não encontrado");
      }

      if (isContaBanida(remetente)) {
        const error = new Error(getMensagemContaBanida());
        error.statusCode = 403;
        error.payload = buildContaBanidaPayload(remetente);
        throw error;
      }

      if (isContaBanida(destino)) {
        const error = new Error("Conta destino indisponivel");
        error.statusCode = 403;
        error.payload = {
          error: "Conta destino indisponivel"
        };
        throw error;
      }

      if (remetente.id === destino.id) {
        throw new Error("Não pode transferir para si mesmo");
      }

      if (toMoney(remetente.saldo) < valorNum) {
        throw new Error("Saldo insuficiente");
      }

      const remetenteContexto = await getUserFinancialContext(remetente, client);
      const bonusTransferido = Math.min(
        toMoney(remetenteContexto.saldoBonusAtual),
        valorNum
      );
      const realTransferido = toMoney(valorNum - bonusTransferido);
      const transferId = buildId("transfer");
      const now = db();

      const txSaida = await createFinancialTransaction(client, {
        userId: remetente.id,
        referenceKey: `transfer:${transferId}:debit`,
        sourceType: "transfer",
        sourceId: transferId,
        operationType: "transfer_out",
        direction: "debit",
        amount: valorNum,
        status: "completed",
        description: `Transferência enviada para ${destino.email}`,
        metadata: {
          fromUserId: remetente.id,
          toUserId: destino.id,
          toEmail: destino.email,
          bonusAmount: bonusTransferido,
          realAmount: realTransferido
        }
      });

      const remetenteAtualizado = await applyLedgerChange(client, {
        userId: remetente.id,
        financialTransactionId: txSaida.id,
        entryType: "debit",
        amount: valorNum,
        description: `Transferência enviada para ${destino.email}`,
        metadata: {
          transferId,
          counterpartUserId: destino.id,
          counterpartEmail: destino.email,
          bonusAmount: bonusTransferido,
          realAmount: realTransferido
        }
      });

      const txEntrada = await createFinancialTransaction(client, {
        userId: destino.id,
        referenceKey: `transfer:${transferId}:credit`,
        sourceType: "transfer",
        sourceId: transferId,
        operationType: "transfer_in",
        direction: "credit",
        amount: valorNum,
        status: "completed",
        description: `Transferência recebida de ${remetente.email}`,
        metadata: {
          fromUserId: remetente.id,
          fromEmail: remetente.email,
          toUserId: destino.id,
          bonusAmount: bonusTransferido,
          realAmount: realTransferido
        }
      });

      await applyLedgerChange(client, {
        userId: destino.id,
        financialTransactionId: txEntrada.id,
        entryType: "credit",
        amount: valorNum,
        description: `Transferência recebida de ${remetente.email}`,
        metadata: {
          transferId,
          counterpartUserId: remetente.id,
          counterpartEmail: remetente.email,
          bonusAmount: bonusTransferido,
          realAmount: realTransferido
        }
      });

      await saveDeposito(
        {
          id: buildId("dep"),
          userId: remetente.id,
          valor: valorNum,
          chavePix: "",
          tipoChave: "",
          tipoTransacao: "saida",
          status: "aprovado",
          comprovanteUrl: "",
          descricao: `Transferência enviada para ${destino.email}`,
          criadoEm: now,
          aprovadoEm: now,
          recusadoEm: null,
          comprovanteEnviadoEm: null
        },
        client
      );

      await saveDeposito(
        {
          id: buildId("dep"),
          userId: destino.id,
          valor: valorNum,
          chavePix: "",
          tipoChave: "",
          tipoTransacao: "entrada",
          status: "aprovado",
          comprovanteUrl: "",
          descricao: `Transferência recebida de ${remetente.email}`,
          criadoEm: now,
          aprovadoEm: now,
          recusadoEm: null,
          comprovanteEnviadoEm: null
        },
        client
      );

      return {
        saldoAtual: toMoney(remetenteAtualizado.saldo)
      };
    });

    res.json({
      message: "Transferência realizada com sucesso",
      saldoAtual: result.saldoAtual
    });
  } catch (error) {
    console.error(error);
    res
      .status(error.statusCode || 400)
      .json(error.payload || { error: error.message || "Erro na transferência" });
  }
});

app.post("/nfc/receive-session", authUser, async (req, res) => {
  try {
    const ttlSeconds = Math.min(
      120,
      Math.max(15, Number(req.body?.ttlSeconds || NFC_RECEIVE_SESSION_TTL_SECONDS))
    );

    const result = await runInTransaction(async (client) => {
      const receiver = await getUserByIdForUpdate(req.userAuth.sub, client);

      if (!receiver) {
        return { error: "Usuario nao encontrado", statusCode: 404, code: "USER_NOT_FOUND" };
      }

      if (isContaBanida(receiver)) {
        return {
          error: getMensagemContaBanida(),
          statusCode: 403,
          payload: buildContaBanidaPayload(receiver)
        };
      }

      await cancelPendingNfcReceiveSessionsByReceiver(receiver.id, client);

      const now = new Date();
      const session = {
        id: buildId("nfcsess"),
        publicToken: crypto.randomBytes(16).toString("hex"),
        receiverUserId: receiver.id,
        status: "pending",
        nonce: crypto.randomBytes(8).toString("hex"),
        protocolVersion: NFC_PROTOCOL_VERSION,
        expiresAt: db(addSeconds(now, ttlSeconds)),
        consumedAt: null,
        cancelledAt: null,
        payerUserId: "",
        amount: 0,
        financialTransactionId: "",
        readCount: 0,
        lastReadAt: null,
        metadata: {
          receiverName: getUserDisplayName(receiver),
          receiverEmail: receiver.email,
          channel: "nfc"
        },
        createdAt: db(now),
        updatedAt: db(now)
      };

      await saveNfcReceiveSession(session, client);
      return { session, receiver };
    });

    if (result?.payload || result?.statusCode) {
      return res
        .status(result.statusCode || 400)
        .json(result.payload || { code: result.code, error: result.error });
    }

    res.status(201).json(
      buildNfcReceiveSessionResponse(result.session, result.receiver, {
        ttlSeconds
      })
    );
  } catch (error) {
    console.error(error);
    sendJsonError(
      res,
      500,
      "NFC_RECEIVE_SESSION_CREATE_ERROR",
      "Erro ao criar sessao NFC"
    );
  }
});

app.get("/nfc/receive-session/:id", authUser, async (req, res) => {
  try {
    const user = await getUserById(req.userAuth.sub);

    if (!user) {
      return sendJsonError(res, 404, "USER_NOT_FOUND", "Usuario nao encontrado");
    }

    let session = await getNfcReceiveSessionById(req.params.id);

    if (!session) {
      return sendJsonError(
        res,
        404,
        "NFC_RECEIVE_SESSION_NOT_FOUND",
        "Sessao NFC nao encontrada"
      );
    }

    if (session.receiverUserId !== user.id) {
      return sendJsonError(
        res,
        403,
        "NFC_RECEIVE_SESSION_FORBIDDEN",
        "Sessao NFC indisponivel"
      );
    }

    session = await expireNfcReceiveSessionIfNeeded(session);

    const payer =
      session.payerUserId && session.payerUserId !== user.id
        ? await getUserById(session.payerUserId)
        : null;
    const charge = session.metadata?.chargeId
      ? await syncSigmoTapChargeStatus(
          await getSigmoTapChargeById(session.metadata.chargeId)
        )
      : null;

    res.json(
      buildNfcReceiveSessionResponse(session, user, {
        payer: payer
          ? {
              id: payer.id,
              nome: getUserDisplayName(payer),
              email: payer.email
            }
          : session.metadata?.payerEmail
            ? {
                id: session.payerUserId || "",
                nome: session.metadata?.payerName || "",
                email: session.metadata?.payerEmail || ""
              }
            : null,
        financialTransactionId: session.financialTransactionId || "",
        fixedAmount: toMoney(charge?.amount || session.metadata?.fixedAmount || 0),
        requiresPin: false,
        requiresDeviceAuth: true,
        confirmationMode: "device_auth",
        charge: charge ? buildSigmoTapChargeResponse(charge, user) : null
      })
    );
  } catch (error) {
    console.error(error);
    sendJsonError(
      res,
      500,
      "NFC_RECEIVE_SESSION_FETCH_ERROR",
      "Erro ao consultar sessao NFC"
    );
  }
});

app.post("/nfc/receive-session/:id/cancel", authUser, async (req, res) => {
  try {
    const result = await runInTransaction(async (client) => {
      const user = await getUserById(req.userAuth.sub, client);

      if (!user) {
        return { statusCode: 404, code: "USER_NOT_FOUND", error: "Usuario nao encontrado" };
      }

      let session = await getNfcReceiveSessionByIdForUpdate(req.params.id, client);

      if (!session) {
        return {
          statusCode: 404,
          code: "NFC_RECEIVE_SESSION_NOT_FOUND",
          error: "Sessao NFC nao encontrada"
        };
      }

      if (session.receiverUserId !== user.id) {
        return {
          statusCode: 403,
          code: "NFC_RECEIVE_SESSION_FORBIDDEN",
          error: "Sessao NFC indisponivel"
        };
      }

      session = await expireNfcReceiveSessionIfNeeded(session, client);

      if (session.status === "pending") {
        session = {
          ...session,
          status: "cancelled",
          cancelledAt: db(),
          updatedAt: db()
        };
        await saveNfcReceiveSession(session, client);
      }

      if (session.metadata?.chargeId) {
        const charge = await getSigmoTapChargeByIdForUpdate(session.metadata.chargeId, client);
        if (charge && charge.status !== "paid" && charge.status !== "cancelled") {
          await saveSigmoTapCharge(
            {
              ...charge,
              status: "pending",
              updatedAt: db()
            },
            client
          );
        }
      }

      return { session, user };
    });

    if (result?.statusCode) {
      return sendJsonError(res, result.statusCode, result.code, result.error);
    }

    res.json(buildNfcReceiveSessionResponse(result.session, result.user));
  } catch (error) {
    console.error(error);
    sendJsonError(
      res,
      500,
      "NFC_RECEIVE_SESSION_CANCEL_ERROR",
      "Erro ao cancelar sessao NFC"
    );
  }
});

app.post("/nfc/session/resolve", authUser, async (req, res) => {
  try {
    const payload = parseNfcReceiveSessionPayload(req.body?.payload || req.body || {});

    if (!payload.publicToken || !payload.nonce) {
      return sendJsonError(
        res,
        400,
        "NFC_PAYLOAD_INVALID",
        "Payload NFC invalido"
      );
    }

    const result = await runInTransaction(async (client) => {
      let session = await getNfcReceiveSessionByPublicTokenForUpdate(
        payload.publicToken,
        client
      );

      if (!session) {
        return {
          statusCode: 404,
          code: "NFC_RECEIVE_SESSION_NOT_FOUND",
          error: "Sessao NFC nao encontrada"
        };
      }

      session = await expireNfcReceiveSessionIfNeeded(session, client);

      if (session.status === "expired") {
        return {
          statusCode: 410,
          code: "NFC_RECEIVE_SESSION_EXPIRED",
          error: "Sessao NFC expirada"
        };
      }

      if (session.status !== "pending") {
        return {
          statusCode: 409,
          code: "NFC_RECEIVE_SESSION_NOT_PENDING",
          error: "Sessao NFC indisponivel"
        };
      }

      if (session.nonce !== payload.nonce) {
        return {
          statusCode: 400,
          code: "NFC_PAYLOAD_INVALID",
          error: "Payload NFC invalido"
        };
      }

      if (session.receiverUserId === req.userAuth.sub) {
        return {
          statusCode: 400,
          code: "SELF_TRANSFER_NOT_ALLOWED",
          error: "Nao pode pagar para si mesmo"
        };
      }

      const receiver = await getUserById(session.receiverUserId, client);

      if (!receiver) {
        return {
          statusCode: 404,
          code: "NFC_RECEIVER_NOT_FOUND",
          error: "Recebedor nao encontrado"
        };
      }

      if (isContaBanida(receiver)) {
        return {
          statusCode: 403,
          code: "NFC_RECEIVER_UNAVAILABLE",
          error: "Conta destino indisponivel"
        };
      }

      let charge = null;

      if (session.metadata?.chargeId) {
        charge = await getSigmoTapChargeById(session.metadata.chargeId, client);

        if (!charge) {
          return {
            statusCode: 404,
            code: "TAP_CHARGE_NOT_FOUND",
            error: "Cobranca por aproximacao nao encontrada"
          };
        }

        charge = await syncSigmoTapChargeStatus(charge, client);

        if (charge.status === "paid" || charge.status === "cancelled" || charge.status === "expired") {
          return {
            statusCode: 409,
            code: "TAP_CHARGE_UNAVAILABLE",
            error: "Cobranca por aproximacao indisponivel"
          };
        }
      }

      session = await touchNfcReceiveSessionRead(session, client);
      return { session, receiver, charge };
    });

    if (result?.statusCode) {
      return sendJsonError(res, result.statusCode, result.code, result.error);
    }

    res.json(
      buildNfcReceiveSessionResponse(result.session, result.receiver, {
        canPay: true,
        fixedAmount: toMoney(result.charge?.amount || result.session.metadata?.fixedAmount || 0),
        requiresPin: false,
        requiresDeviceAuth: true,
        confirmationMode: "device_auth",
        charge: result.charge
          ? buildSigmoTapChargeResponse(result.charge, result.receiver)
          : null
      })
    );
  } catch (error) {
    console.error(error);
    sendJsonError(res, 500, "NFC_RESOLVE_ERROR", "Erro ao resolver sessao NFC");
  }
});

app.post("/nfc/pay", authUser, async (req, res) => {
  try {
    const payload = parseNfcReceiveSessionPayload(req.body?.payload || req.body || {});
    const authMethod = String(req.body?.authMethod || "device_auth").trim();
    const deviceId = String(req.deviceId || "").trim();

    if (!payload.publicToken || !payload.nonce) {
      return sendJsonError(
        res,
        400,
        "NFC_PAYLOAD_INVALID",
        "Payload NFC invalido"
      );
    }

    if (!deviceId) {
      return sendJsonError(
        res,
        400,
        "DEVICE_ID_REQUIRED",
        "Este aparelho ainda nao foi identificado pela Sigmo"
      );
    }

    const result = await runInTransaction(async (client) => {
      let session = await getNfcReceiveSessionByPublicTokenForUpdate(
        payload.publicToken,
        client
      );

      if (!session) {
        const error = new Error("Sessao NFC nao encontrada");
        error.statusCode = 404;
        error.payload = {
          code: "NFC_RECEIVE_SESSION_NOT_FOUND",
          error: "Sessao NFC nao encontrada"
        };
        throw error;
      }

      session = await expireNfcReceiveSessionIfNeeded(session, client);

      if (session.status === "expired") {
        const error = new Error("Sessao NFC expirada");
        error.statusCode = 410;
        error.payload = {
          code: "NFC_RECEIVE_SESSION_EXPIRED",
          error: "Sessao NFC expirada"
        };
        throw error;
      }

      if (session.status !== "pending") {
        const error = new Error("Sessao NFC indisponivel");
        error.statusCode = 409;
        error.payload = {
          code: "NFC_RECEIVE_SESSION_NOT_PENDING",
          error: "Sessao NFC indisponivel"
        };
        throw error;
      }

      if (session.nonce !== payload.nonce) {
        const error = new Error("Payload NFC invalido");
        error.statusCode = 400;
        error.payload = {
          code: "NFC_PAYLOAD_INVALID",
          error: "Payload NFC invalido"
        };
        throw error;
      }

      const payer = await getUserById(req.userAuth.sub, client);
      const receiver = await getUserById(session.receiverUserId, client);
      const activeCard = payer
        ? await getBoundSigmoCardByHolderAndDevice(payer.id, deviceId, client)
        : null;
      let charge = null;

      if (!payer) {
        const error = new Error("Usuario nao encontrado");
        error.statusCode = 404;
        error.payload = {
          code: "USER_NOT_FOUND",
          error: "Usuario nao encontrado"
        };
        throw error;
      }

      if (isContaBanida(payer)) {
        const error = new Error(getMensagemContaBanida());
        error.statusCode = 403;
        error.payload = buildContaBanidaPayload(payer);
        throw error;
      }

      if (!receiver) {
        const error = new Error("Recebedor nao encontrado");
        error.statusCode = 404;
        error.payload = {
          code: "NFC_RECEIVER_NOT_FOUND",
          error: "Recebedor nao encontrado"
        };
        throw error;
      }

      if (!activeCard) {
        const error = new Error("Nenhum cartao foi liberado neste aparelho");
        error.statusCode = 403;
        error.payload = {
          code: "CARD_NOT_RELEASED_FOR_DEVICE",
          error: "Nenhum cartao foi liberado neste aparelho"
        };
        throw error;
      }

      const fundingUser =
        activeCard.ownerUserId === payer.id
          ? payer
          : await getUserById(activeCard.ownerUserId, client);

      if (!fundingUser) {
        const error = new Error("Titular do cartao nao encontrado");
        error.statusCode = 404;
        error.payload = {
          code: "CARD_OWNER_NOT_FOUND",
          error: "Titular do cartao nao encontrado"
        };
        throw error;
      }

      if (isContaBanida(fundingUser)) {
        const error = new Error("Titular do cartao indisponivel");
        error.statusCode = 403;
        error.payload = {
          code: "CARD_OWNER_UNAVAILABLE",
          error: "Titular do cartao indisponivel"
        };
        throw error;
      }

      if (session.metadata?.chargeId) {
        charge = await getSigmoTapChargeByIdForUpdate(session.metadata.chargeId, client);

        if (!charge) {
          const error = new Error("Cobranca por aproximacao nao encontrada");
          error.statusCode = 404;
          error.payload = {
            code: "TAP_CHARGE_NOT_FOUND",
            error: "Cobranca por aproximacao nao encontrada"
          };
          throw error;
        }

        charge = await syncSigmoTapChargeStatus(charge, client);

        if (charge.status === "paid" || charge.status === "cancelled" || charge.status === "expired") {
          const error = new Error("Cobranca por aproximacao indisponivel");
          error.statusCode = 409;
          error.payload = {
            code: "TAP_CHARGE_UNAVAILABLE",
            error: "Cobranca por aproximacao indisponivel"
          };
          throw error;
        }
      }

      const valorNum = charge
        ? toMoney(charge.amount)
        : toMoney(req.body?.amount);

      if (!Number.isFinite(valorNum) || valorNum <= 0) {
        const error = new Error("Valor invalido");
        error.statusCode = 400;
        error.payload = {
          code: "NFC_AMOUNT_INVALID",
          error: "Valor invalido"
        };
        throw error;
      }

      const availableCardBalance = Math.max(
        0,
        Math.min(toMoney(activeCard.spendingLimit), toMoney(fundingUser.saldo))
      );

      if (availableCardBalance <= 0 || valorNum > availableCardBalance) {
        const error = new Error("O valor excede o limite liberado para este cartao");
        error.statusCode = 403;
        error.payload = {
          code: "CARD_LIMIT_EXCEEDED",
          error: "O valor excede o limite liberado para este cartao",
          cardLimit: toMoney(activeCard.spendingLimit),
          availableToSpend: availableCardBalance
        };
        throw error;
      }

      const transferencia = await executeSigmoTransfer(client, {
        fromUserId: fundingUser.id,
        toUserId: receiver.id,
        amount: valorNum,
        channel: "nfc",
        metadata: {
          nfcSessionId: session.id,
          nfcPublicToken: session.publicToken,
          tapChargeId: charge?.id || "",
          authMethod,
          sigmoCardId: activeCard.id,
          sigmoCardLabel: activeCard.label,
          cardOwnerUserId: fundingUser.id,
          cardHolderUserId: payer.id,
          cardType: activeCard.cardType,
          cardLimit: toMoney(activeCard.spendingLimit),
          deviceId
        }
      });

      const now = db();
      session = {
        ...session,
        status: "consumed",
        consumedAt: now,
        updatedAt: now,
        payerUserId: payer.id,
        amount: valorNum,
        financialTransactionId: transferencia.txSaida.id,
        metadata: {
          ...(session.metadata || {}),
          channel: "nfc",
          receiverName: getUserDisplayName(receiver),
          receiverEmail: receiver.email,
          payerName: getUserDisplayName(payer),
          payerEmail: payer.email,
          cardOwnerName: getUserDisplayName(fundingUser),
          cardOwnerEmail: fundingUser.email,
          sigmoCardId: activeCard.id,
          sigmoCardLabel: activeCard.label,
          transferId: transferencia.transferId,
          authMethod
        }
      };

      await saveNfcReceiveSession(session, client);

      const updatedCard = {
        ...activeCard,
        lastUsedAt: now,
        updatedAt: now
      };
      await saveSigmoCard(updatedCard, client);

      if (charge) {
        charge = {
          ...charge,
          status: "paid",
          payerUserId: payer.id,
          financialTransactionId: transferencia.txSaida.id,
          paidAt: now,
          updatedAt: now,
          metadata: {
            ...(charge.metadata || {}),
            payerName: getUserDisplayName(payer),
            payerEmail: payer.email,
            transferId: transferencia.transferId,
            authMethod
          }
        };

        await saveSigmoTapCharge(charge, client);
      }

      return {
        session,
        payer,
        fundingUser,
        receiver,
        transferencia,
        charge,
        card: updatedCard,
        userResponse: await buildUserPublicResponseWithPix(payer, client, {
          deviceId,
          activeCard: buildSigmoCardResponse(updatedCard, fundingUser, payer)
        })
      };
    });

    res.json({
      code: "NFC_PAYMENT_SUCCESS",
      message: "Pagamento por aproximacao realizado com sucesso",
      saldoAtual: result.transferencia.saldoAtualRemetente,
      user: result.userResponse,
      receiver: {
        id: result.receiver.id,
        nome: getUserDisplayName(result.receiver),
        email: result.receiver.email
      },
      card: buildSigmoCardResponse(result.card, result.fundingUser, result.payer),
      charge: result.charge
        ? buildSigmoTapChargeResponse(result.charge, result.receiver)
        : null,
      session: buildNfcReceiveSessionResponse(result.session, result.receiver, {
        payer: {
          id: result.payer.id,
          nome: getUserDisplayName(result.payer),
          email: result.payer.email
        },
        financialTransactionId: result.session.financialTransactionId || "",
        fixedAmount: toMoney(result.charge?.amount || result.session.metadata?.fixedAmount || 0),
        requiresPin: false,
        requiresDeviceAuth: true,
        confirmationMode: "device_auth",
        charge: result.charge
          ? buildSigmoTapChargeResponse(result.charge, result.receiver)
          : null
      })
    });
  } catch (error) {
    console.error(error);
    res
      .status(error.statusCode || 400)
      .json(error.payload || { code: "NFC_PAY_ERROR", error: error.message || "Erro no pagamento NFC" });
  }
});

app.get("/usuarios", authAdmin, async (req, res) => {
  try {
    const result = await listUsers();
    const contextMap = await getUsersAdminContextMap(result);
    res.json(result.map((u) => buildAdminUserResponse(u, contextMap.get(u.id))));
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: "Erro" });
  }
});

app.get("/depositos", authAdmin, async (req, res) => {
  try {
    const result = await listDepositos();
    res.json(result);
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: "Erro" });
  }
});

app.post("/aprovar", authAdmin, async (req, res) => {
  try {
    const { depositoId } = req.body;

    if (!depositoId) {
      return res.status(400).json({ error: "depositoId é obrigatório" });
    }

    const result = await runInTransaction(async (client) => {
      const pedido = await getDepositoByIdForUpdate(depositoId, client);

      if (!pedido) {
        throw new Error("Pedido não encontrado");
      }

      if (pedido.status === "aprovado") {
        throw new Error("Pedido já aprovado");
      }

      if (pedido.status === "recusado") {
        throw new Error("Pedido já recusado");
      }

      const usuario = await getUserByIdForUpdate(pedido.userId, client);

      if (!usuario) {
        throw new Error("Usuário não encontrado");
      }

      if (isContaBanida(usuario)) {
        throw new Error(getMensagemContaBanida());
      }

      if (pedido.tipoTransacao !== "saida" && !pedido.comprovanteUrl) {
        throw new Error("Sem comprovante");
      }

      const valorPedido = toMoney(pedido.valor);

      if (!Number.isFinite(valorPedido) || valorPedido <= 0) {
        throw new Error("Valor do pedido inválido");
      }

      const isSaida = pedido.tipoTransacao === "saida";
      const detalhesSaque = isSaida
        ? {
            repassarTaxa: Boolean(pedido.repassarTaxa),
            taxa: toMoney(pedido.taxaPix),
            valorLiquido:
              toMoney(pedido.valorLiquidoPix) > 0
                ? toMoney(pedido.valorLiquidoPix)
                : valorPedido,
            valorDebitado:
              toMoney(pedido.valorDebitadoPix) > 0
                ? toMoney(pedido.valorDebitadoPix)
                : valorPedido
          }
        : null;
      const valorFinal = isSaida
        ? detalhesSaque.valorDebitado
        : calcularValorCreditadoDeposito(valorPedido);
      const operationType = isSaida ? "withdrawal" : "deposit";
      const direction = isSaida ? "debit" : "credit";
      const description = isSaida
        ? "Saque aprovado pelo admin"
        : "Depósito aprovado pelo admin";

      const financialTx = await createFinancialTransaction(client, {
        userId: usuario.id,
        referenceKey: `deposito:${pedido.id}:approval`,
        sourceType: "deposito",
        sourceId: pedido.id,
        operationType,
        direction,
        amount: valorFinal,
        status: "completed",
        description,
        metadata: {
          pedidoId: pedido.id,
          tipoTransacao: pedido.tipoTransacao,
          adminId: req.admin.sub,
          repassarTaxa: detalhesSaque?.repassarTaxa || false,
          taxaPix: detalhesSaque?.taxa || 0,
          valorLiquidoPix: detalhesSaque?.valorLiquido || null
        }
      });

      const usuarioAtualizado = await applyLedgerChange(client, {
        userId: usuario.id,
        financialTransactionId: financialTx.id,
        entryType: isSaida ? "debit" : "credit",
        amount: valorFinal,
        description,
        metadata: {
          pedidoId: pedido.id,
          tipoTransacao: pedido.tipoTransacao,
          adminId: req.admin.sub,
          repassarTaxa: detalhesSaque?.repassarTaxa || false,
          taxaPix: detalhesSaque?.taxa || 0,
          valorLiquidoPix: detalhesSaque?.valorLiquido || null
        }
      });

      pedido.status = "aprovado";
      pedido.aprovadoEm = db();

      await saveDeposito(pedido, client);

      await createAuditLog(client, {
        adminId: req.admin.sub,
        action: "approve_order",
        targetType: "deposito",
        targetId: pedido.id,
        details: {
          userId: usuario.id,
          valor: valorFinal,
          tipoTransacao: pedido.tipoTransacao,
          repassarTaxa: detalhesSaque?.repassarTaxa || false,
          taxaPix: detalhesSaque?.taxa || 0,
          valorLiquidoPix: detalhesSaque?.valorLiquido || null,
          saldoFinal: toMoney(usuarioAtualizado.saldo)
        },
        ipAddress: getRequestIp(req)
      });

      return {
        pedido,
        saldoAtual: toMoney(usuarioAtualizado.saldo)
      };
    });

    res.json({
      message: "Pedido aprovado com sucesso",
      pedido: result.pedido,
      saldoAtual: result.saldoAtual
    });
  } catch (error) {
    console.error(error);
    res.status(400).json({ error: error.message || "Erro ao aprovar pedido" });
  }
});

app.post("/recusar", authAdmin, async (req, res) => {
  try {
    const { depositoId } = req.body;

    if (!depositoId) {
      return res.status(400).json({ error: "depositoId é obrigatório" });
    }

    const result = await runInTransaction(async (client) => {
      const pedido = await getDepositoByIdForUpdate(depositoId, client);

      if (!pedido) {
        throw new Error("Pedido não encontrado");
      }

      if (pedido.status === "aprovado") {
        throw new Error("Pedido já aprovado, não pode recusar");
      }

      if (pedido.status === "recusado") {
        throw new Error("Pedido já recusado");
      }

      pedido.status = "recusado";
      pedido.recusadoEm = db();

      await saveDeposito(pedido, client);

      await createAuditLog(client, {
        adminId: req.admin.sub,
        action: "reject_order",
        targetType: "deposito",
        targetId: pedido.id,
        details: {
          userId: pedido.userId,
          valor: toMoney(pedido.valor),
          tipoTransacao: pedido.tipoTransacao
        },
        ipAddress: getRequestIp(req)
      });

      return { pedido };
    });

    res.json({
      message: "Pedido recusado com sucesso",
      pedido: result.pedido
    });
  } catch (error) {
    console.error(error);
    res.status(400).json({ error: error.message || "Erro ao recusar pedido" });
  }
});

app.post("/admin/update-balance", authAdmin, async (req, res) => {
  try {
    const { userId, saldo } = req.body;

    if (!userId || saldo === undefined || saldo === null) {
      return res.status(400).json({ error: "userId e saldo são obrigatórios" });
    }

    const saldoNumero = toMoney(saldo);

    if (!Number.isFinite(saldoNumero) || saldoNumero < 0) {
      return res.status(400).json({ error: "Saldo inválido" });
    }

    const result = await runInTransaction(async (client) => {
      const usuario = await getUserByIdForUpdate(userId, client);

      if (!usuario) {
        throw new Error("Usuário não encontrado");
      }

      const saldoAtual = toMoney(usuario.saldo);
      const diferenca = toMoney(saldoNumero - saldoAtual);

      if (diferenca === 0) {
        return {
          user: usuario,
          changed: false
        };
      }

      const isCredit = diferenca > 0;
      const amount = Math.abs(diferenca);

      const financialTx = await createFinancialTransaction(client, {
        userId: usuario.id,
        referenceKey: `manual-balance:${usuario.id}:${Date.now()}`,
        sourceType: "admin_adjustment",
        sourceId: usuario.id,
        operationType: "manual_balance_adjustment",
        direction: isCredit ? "credit" : "debit",
        amount,
        status: "completed",
        description: `Ajuste manual de saldo por admin para ${saldoNumero.toFixed(2)}`,
        metadata: {
          oldBalance: saldoAtual,
          newBalance: saldoNumero,
          adminId: req.admin.sub
        }
      });

      const usuarioAtualizado = await applyLedgerChange(client, {
        userId: usuario.id,
        financialTransactionId: financialTx.id,
        entryType: isCredit ? "credit" : "debit",
        amount,
        description: `Ajuste manual de saldo por admin para ${saldoNumero.toFixed(2)}`,
        metadata: {
          oldBalance: saldoAtual,
          newBalance: saldoNumero,
          adminId: req.admin.sub
        }
      });

      if (toMoney(usuarioAtualizado.saldo) !== saldoNumero) {
        usuarioAtualizado.saldo = saldoNumero;
        usuarioAtualizado.saldoAtualizadoEm = db();
        await saveUser(usuarioAtualizado, client);
      }

      await createAuditLog(client, {
        adminId: req.admin.sub,
        action: "manual_balance_update",
        targetType: "usuario",
        targetId: usuario.id,
        details: {
          oldBalance: saldoAtual,
          newBalance: saldoNumero,
          difference: diferenca
        },
        ipAddress: getRequestIp(req)
      });

      return {
        user: usuarioAtualizado,
        changed: true
      };
    });

    res.json({
      message: result.changed
        ? "Saldo atualizado com sucesso"
        : "Saldo já estava com este valor",
      user: {
        id: result.user.id,
        email: result.user.email,
        saldo: toMoney(result.user.saldo)
      }
    });
  } catch (error) {
    console.error(error);
    res.status(400).json({ error: error.message || "Erro ao atualizar saldo" });
  }
});

app.post("/admin/reset-password", authAdmin, async (req, res) => {
  try {
    const { userId, novaSenha } = req.body;

    if (!userId || !novaSenha) {
      return res.status(400).json({ error: "userId e novaSenha são obrigatórios" });
    }

    const usuario = await getUserById(userId);

    if (!usuario) {
      return res.status(404).json({ error: "Usuário não encontrado" });
    }

    usuario.senha = await bcrypt.hash(String(novaSenha), 10);
    usuario.senhaAtualizadaEm = db();

    await saveUser(usuario);

    await runInTransaction(async (client) => {
      await createAuditLog(client, {
        adminId: req.admin.sub,
        action: "reset_user_password",
        targetType: "usuario",
        targetId: usuario.id,
        details: {
          email: usuario.email
        },
        ipAddress: getRequestIp(req)
      });
    });

    res.json({ message: "Senha redefinida com sucesso" });
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: "Erro ao redefinir senha" });
  }
});

app.get("/admin/backups/status", authAdmin, async (req, res) => {
  try {
    const status = await getBackupStatus();
    res.json(status);
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: "Erro ao buscar status dos backups" });
  }
});

app.get("/admin/backups", authAdmin, async (req, res) => {
  try {
    const backups = await listBackupFiles();
    res.json({ backups });
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: "Erro ao listar backups" });
  }
});

app.post("/admin/backups/run", authAdmin, async (req, res) => {
  try {
    const result = await createDatabaseBackup("manual");

    if (result.skipped) {
      return res.status(409).json({ error: result.error });
    }

    if (!result.ok) {
      return res.status(500).json({ error: result.error || "Erro ao executar backup" });
    }

    res.json({
      message: "Backup executado com sucesso",
      backup: result
    });
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: "Erro ao executar backup" });
  }
});

app.get("/admin/user/:id/ledger", authAdmin, async (req, res) => {
  try {
    const user = await getUserById(req.params.id);

    if (!user) {
      return res.status(404).json({ error: "Usuário não encontrado" });
    }

    const [transactions, ledger] = await Promise.all([
      listFinancialTransactionsByUser(user.id),
      listLedgerEntriesByUser(user.id)
    ]);
    const context = await getAdminUserContext(user);

    res.json({
      user: buildAdminUserResponse(user, context),
      transactions,
      ledger
    });
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: "Erro ao buscar ledger do usuário" });
  }
});

// =========================
// 🔥 COMPATIBILIDADE COM ADMIN.JS (MODAL USUÁRIO)
// =========================

// ALTERAR SALDO
app.post("/admin/alterar-saldo", authAdmin, async (req, res) => {
  try {
    const { userId, valor } = req.body;

    if (!userId || valor === undefined || valor === null) {
      return res.status(400).json({ error: "userId e valor são obrigatórios" });
    }

    req.body = { userId, saldo: valor };

    return app._router.handle(
      { ...req, url: "/admin/update-balance", method: "POST" },
      res
    );
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: "Erro ao alterar saldo" });
  }
});

// ALTERAR SENHA
app.post("/admin/alterar-senha", authAdmin, async (req, res) => {
  try {
    const { userId, senha } = req.body;

    if (!userId || !senha) {
      return res.status(400).json({ error: "userId e senha são obrigatórios" });
    }

    req.body = { userId, novaSenha: senha };

    return app._router.handle(
      { ...req, url: "/admin/reset-password", method: "POST" },
      res
    );
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: "Erro ao alterar senha" });
  }
});

// DELETAR USUÁRIO
app.post("/admin/deletar-usuario", authAdmin, async (req, res) => {
  try {
    const { userId } = req.body;

    if (!userId) {
      return res.status(400).json({ error: "userId obrigatório" });
    }

    await runInTransaction(async (client) => {
      const user = await getUserByIdForUpdate(userId, client);

      if (!user) {
        throw new Error("Usuário não encontrado");
      }

      await client.query("DELETE FROM usuarios WHERE id = $1", [userId]);

      await createAuditLog(client, {
        adminId: req.admin.sub,
        action: "delete_user",
        targetType: "usuario",
        targetId: userId,
        details: {
          email: user.email
        },
        ipAddress: getRequestIp(req)
      });
    });

    res.json({ message: "Usuário deletado com sucesso" });
  } catch (error) {
    console.error(error);
    res.status(400).json({
      error: error.message || "Erro ao deletar usuário"
    });
  }
});

// =========================
// 🔥 BOT AUTOMAÇÃO DENTPEG
// =========================

function authBot(req, res, next) {
  const token = req.headers["x-bot-token"];

  if (!token || token !== BOT_SECRET) {
    return res.status(401).json({ error: "não autorizado" });
  }

  next();
}

function bateNomeComprovante(textoComprovante, nomeExtrato) {
  if (!textoComprovante || !nomeExtrato) return false;

  if (
    textoComprovante.includes(nomeExtrato) ||
    nomeExtrato.includes(textoComprovante)
  ) {
    return true;
  }

  const ignorarTokens = new Set([
    "da",
    "de",
    "di",
    "do",
    "du",
    "das",
    "des",
    "dos",
    "e"
  ]);

  const tokens = nomeExtrato
    .split(" ")
    .map((token) => token.trim())
    .filter((token) => token.length >= 2 && !ignorarTokens.has(token));

  if (tokens.length === 0) {
    return false;
  }

  const primeiro = tokens[0];
  const ultimo = tokens[tokens.length - 1];
  const coincidencias = tokens.filter((token) => textoComprovante.includes(token));
  const tokensFortes = tokens.filter((token) => token.length >= 4);
  const coincidenciasFortes = tokensFortes.filter((token) => textoComprovante.includes(token));

  if (
    tokens.length >= 2 &&
    textoComprovante.includes(primeiro) &&
    textoComprovante.includes(ultimo)
  ) {
    return true;
  }

  if (coincidenciasFortes.length >= 2) {
    return true;
  }

  if (tokensFortes.length >= 3 && coincidenciasFortes.length >= 2 && coincidencias.length >= 3) {
    return true;
  }

  return coincidencias.length >= Math.min(3, tokens.length);
}

app.post("/deposito/confirmar-bot-legacy", authBot, async (req, res) => {
  try {

    const { txid, idTransacao, valorLiquido, nomePagador, dataHora } = req.body;

if (!txid && !idTransacao) {
  console.log("⚠️ Sem txid e sem idTransacao, ignorado");
  return res.json({ ok: false, ignorado: true });
}

    // 🔒 CHAVE ÚNICA DO DEPÓSITO (ANTI DUPLICAÇÃO)
    const fallbackKey = buildId("dentpeg_fallback");
   const referenceKey = txid
  ? `dentpeg:txid:${txid}`
  : idTransacao
    ? `dentpeg:id:${idTransacao}`
    : `dentpeg:fallback:${fallbackKey}`;

    // 🔒 BLOQUEIO DE DUPLICADOS
    const jaExiste = await pool.query(
      `SELECT 1 FROM financial_transactions WHERE reference_key = $1`,
      [referenceKey]
    );

    if (jaExiste.rowCount > 0) {
      console.log("⛔ DUPLICADO IGNORADO:", referenceKey);
      return res.json({ ok: true, duplicado: true });
    }

    // ✔ validação
    if (!valorLiquido || valorLiquido <= 0) {
      return res.status(400).json({ error: "Valor obrigatório" });
    }

    const resultado = await runInTransaction(async (client) => {

      const valorBot = toMoney(valorLiquido);

      if (!valorBot || valorBot <= 0) {
        throw new Error("Valor inválido do bot");
      }

      // 🔍 BUSCA TODOS PENDENTES
      const candidatos = await client.query(
        `
        SELECT * FROM depositos
        WHERE status = 'pendente'
        AND tipo_transacao = 'entrada'
        FOR UPDATE
        `
      );

      let depositoMatch = null;

      for (const row of candidatos.rows) {
        const dep = mapDeposito(row);

        const calc = calcularLiquidoDentpeg(dep.valor);

const nomeExtrato = normalizarNome(req.body.nomePagador);
const textoComprovante = normalizarNome(dep.comprovanteTexto);

// 🔒 valida nome
if (!nomeExtrato) continue;

// 🔒 valida OCR
if (!textoComprovante || textoComprovante.length < 5) {
  console.log("⚠️ OCR vazio ou inválido");
  continue;
}

// 🔒 exige nome + sobrenome
if (!nomeExtrato.includes(" ")) continue;

// 🔥 MATCH NOME
const bateNome = textoComprovante.includes(nomeExtrato);

if (!bateNome) {
  console.log("⛔ Nome não encontrado no comprovante:", nomeExtrato);
  continue;
}

// 🔥 MATCH VALOR
let bateValor = false;

if (typeof calc === "number") {
  bateValor = Math.abs(calc - valorBot) < 1.0;
} else {
  bateValor = valorBot >= calc.min && valorBot <= calc.max;
}

let bateTempo = false;

const dataBot = normalizarDataLocal(req.body.dataHora);
const dataPedido = normalizarDataLocal(dep.criadoEm);

if (dataBot && dataPedido) {
  console.log("🕒 DEBUG DATA:", {
    rawBot: req.body.dataHora,
    rawPedido: dep.criadoEm,
    dataBot,
    dataPedido
  });

  bateTempo = dataBot === dataPedido;
}

// 🔥 DEBUG FINAL (AGORA SIM CORRETO)
console.log("🔎 MATCH RESULT:", {
  nomeExtrato,
  textoComprovante,
  valorBot,
  valorPedido: dep.valor,
  dataBot: req.body.dataHora,
  dataPedido: dep.criadoEm,
  dataBotNormalizada: dataBot,
  dataPedidoNormalizada: dataPedido,
  bateNome,
  bateValor,
  bateTempo,
  final: bateNome && bateValor && bateTempo
});

// 🔥 RESULTADO FINAL
const bate = bateNome && bateValor && bateTempo;

if (bate) {
  depositoMatch = dep;
  break;
}
      }

      if (!depositoMatch) {
        throw new Error("Nenhum depósito compatível encontrado");
      }

      const usuario = await getUserByIdForUpdate(depositoMatch.userId, client);

// 🔥 NOVO (SIGMO)
const valorBruto = toMoney(depositoMatch.valor);
const valorFinal = calcularValorCreditadoDeposito(valorBruto);

      if (!usuario) {
        throw new Error("Usuário não encontrado");
      }

      // 💰 CRIA TRANSAÇÃO
      const tx = await createFinancialTransaction(client, {
        userId: usuario.id,
        referenceKey: referenceKey,
        sourceType: "dentpeg",
        sourceId: txid || idTransacao || buildId("dentpeg"),
        operationType: "deposit",
        direction: "credit",
        amount: valorFinal,
        description: "Depósito automático DentPeg",
        metadata: {
  txid: txid || null,
  idTransacao: idTransacao || null
}
      });

      // 💰 APLICA SALDO
      const usuarioAtualizado = await applyLedgerChange(client, {
        userId: usuario.id,
        financialTransactionId: tx.id,
        entryType: "credit",
        amount: valorFinal,
        description: "Depósito automático DentPeg",
        metadata: {
  txid: txid || null,
  idTransacao: idTransacao || null
}
      });

      // ✅ ATUALIZA DEPÓSITO
      depositoMatch.status = "aprovado";
      depositoMatch.aprovadoEm = db();
      depositoMatch.descricao = `Auto aprovado TXID ${txid}`;

      await saveDeposito(depositoMatch, client);

      // 📜 AUDITORIA
      await createAuditLog(client, {
        action: "auto_deposit",
        targetType: "deposito",
        targetId: depositoMatch.id,
        details: {
          txid,
          userId: usuario.id,
          valor: valorFinal
        },
        ipAddress: "bot"
      });

      return {
        duplicado: false,
        saldo: usuarioAtualizado.saldo
      };
    });

    if (resultado.duplicado) {
      return res.json({ message: "TXID já processado" });
    }

    res.json({
      message: "Depósito automático aprovado",
      saldo: resultado.saldo
    });

  } catch (error) {
    console.error("❌ ERRO BOT:", error.message);

    res.status(400).json({
      error: error.message || "Erro no depósito automático"
    });
  }
});

app.post("/deposito/confirmar-bot-path-legacy", authBot, async (req, res) => {
  try {
    const txid = String(req.body.txid || "").trim() || null;
    const idTransacao = String(req.body.idTransacao || "").trim() || null;
    const fallbackKey =
      String(req.body.fallbackKey || "")
        .trim()
        .replace(/[^A-Za-z0-9_-]/g, "") || null;
    const valorBot = toMoney(req.body.valorLiquido);
    const nomeExtrato = normalizarNome(req.body.nomePagador);
    const dataHoraBot = normalizarDataHoraLocal(req.body.dataHora);

    if (!txid && !idTransacao && !fallbackKey) {
      return res.status(400).json({ error: "Identificador da transacao obrigatorio" });
    }

    if (!valorBot || valorBot <= 0) {
      return res.status(400).json({ error: "Valor obrigatorio" });
    }

    if (!nomeExtrato || !nomeExtrato.includes(" ")) {
      return res.status(400).json({ error: "Nome do pagador invalido" });
    }

    if (!dataHoraBot) {
      return res.status(400).json({ error: "dataHora invalida" });
    }

    const referenceKey = txid
      ? `dentpeg:txid:${txid}`
      : idTransacao
        ? `dentpeg:id:${idTransacao}`
        : `dentpeg:fallback:${fallbackKey}`;

    const jaExiste = await pool.query(
      `SELECT 1 FROM financial_transactions WHERE reference_key = $1`,
      [referenceKey]
    );

    if (jaExiste.rowCount > 0) {
      console.log("⛔ DUPLICADO IGNORADO:", referenceKey);
      return res.json({ ok: true, duplicado: true });
    }

    const resultado = await runInTransaction(async (client) => {
      const duplicadoTx = await client.query(
        `SELECT id FROM financial_transactions WHERE reference_key = $1 LIMIT 1`,
        [referenceKey]
      );

      if (duplicadoTx.rowCount > 0) {
        return { duplicado: true, saldo: null, depositoId: null };
      }

      const candidatos = await client.query(
        `
        SELECT * FROM depositos
        WHERE status = 'pendente'
          AND tipo_transacao = 'entrada'
        FOR UPDATE
        `
      );

      let depositoMatch = null;
      let dataHoraComprovanteMatch = null;

      for (const row of candidatos.rows) {
        const dep = mapDeposito(row);
        const textoComprovante = normalizarNome(dep.comprovanteTexto);
        const datasComprovante = extrairDatasDoComprovante(dep.comprovanteTexto);
        const tComprovanteEnviado = Date.parse(
          String(dep.comprovanteEnviadoEm || "")
        );
        const idadeComprovanteMin = Number.isNaN(tComprovanteEnviado)
          ? null
          : (Date.now() - tComprovanteEnviado) / 60000;
        const comprovanteRecente =
          idadeComprovanteMin !== null &&
          idadeComprovanteMin >= 0 &&
          idadeComprovanteMin <= COMPROVANTE_UPLOAD_WINDOW_MINUTES;

        if (!textoComprovante || textoComprovante.length < 5) {
          console.log("⚠️ OCR vazio ou invalido para deposito", dep.id);
          continue;
        }

        if (!comprovanteRecente) {
          console.log("⚠️ Comprovante fora da janela valida", dep.id, {
            comprovanteEnviadoEm: dep.comprovanteEnviadoEm,
            idadeComprovanteMin
          });
          continue;
        }

        if (datasComprovante.length === 0) {
          console.log("⚠️ Data nao encontrada no comprovante", dep.id);
          continue;
        }

        const bateNome = bateNomeComprovante(textoComprovante, nomeExtrato);
        if (!bateNome) {
          continue;
        }

        const calc = calcularLiquidoDentpeg(dep.valor);
        const bateValor =
          typeof calc === "number"
            ? Math.abs(calc - valorBot) < 1
            : valorBot >= calc.min && valorBot <= calc.max;

        if (!bateValor) {
          continue;
        }

        const dataBot = normalizarDataLocal(dataHoraBot);
        const dataComprovanteMatch = dataBot && datasComprovante.includes(dataBot)
          ? dataBot
          : null;
        const bateData = Boolean(dataComprovanteMatch);

        console.log("🔎 MATCH RESULT:", {
          depositoId: dep.id,
          nomeExtrato,
          textoComprovante,
          valorBot,
          valorPedido: dep.valor,
          comprovanteEnviadoEm: dep.comprovanteEnviadoEm,
          idadeComprovanteMin,
          dataBot: dataHoraBot,
          dataBotNormalizada: dataBot,
          dataComprovanteMatch,
          datasComprovanteEncontradas: datasComprovante.slice(0, 5),
          bateNome,
          bateValor,
          bateData
        });

        if (!bateData) {
          continue;
        }

        depositoMatch = dep;
        dataHoraComprovanteMatch = dataComprovanteMatch;
        break;
      }

      if (!depositoMatch) {
        throw new Error("Nenhum depósito compatível encontrado");
      }

      const usuario = await getUserByIdForUpdate(depositoMatch.userId, client);
      if (!usuario) {
        throw new Error("Usuário não encontrado");
      }

      const identificadorBot = txid || idTransacao || fallbackKey;
      const valorFinal = calcularValorCreditadoDeposito(toMoney(depositoMatch.valor));
      const metadata = {
        txid,
        idTransacao,
        fallbackKey,
        dataHoraBot,
        dataHoraComprovanteMatch,
        nomePagador: nomeExtrato,
        valorLiquidoBot: valorBot,
        raw: req.body.raw || null
      };

      if (isContaBanida(usuario)) {
        throw new Error(getMensagemContaBanida());
      }

      const tx = await createFinancialTransaction(client, {
        userId: usuario.id,
        referenceKey,
        sourceType: "dentpeg",
        sourceId: identificadorBot || buildId("dentpeg"),
        operationType: "deposit",
        direction: "credit",
        amount: valorFinal,
        description: "Depósito automático DentPeg",
        metadata
      });

      const usuarioAtualizado = await applyLedgerChange(client, {
        userId: usuario.id,
        financialTransactionId: tx.id,
        entryType: "credit",
        amount: valorFinal,
        description: "Depósito automático DentPeg",
        metadata
      });

      depositoMatch.status = "aprovado";
      depositoMatch.aprovadoEm = db();
      depositoMatch.descricao = `Auto aprovado DentPeg ${identificadorBot}`;

      await saveDeposito(depositoMatch, client);

      await createAuditLog(client, {
        action: "auto_deposit",
        targetType: "deposito",
        targetId: depositoMatch.id,
        details: {
          txid,
          idTransacao,
          fallbackKey,
          userId: usuario.id,
          valor: valorFinal,
          dataHoraBot,
          dataHoraComprovanteMatch
        },
        ipAddress: "bot"
      });

      return {
        duplicado: false,
        saldo: usuarioAtualizado.saldo,
        depositoId: depositoMatch.id
      };
    });

    if (resultado.duplicado) {
      return res.json({ message: "Transacao ja processada", duplicado: true });
    }

    res.json({
      message: "Depósito automático aprovado",
      saldo: resultado.saldo,
      depositoId: resultado.depositoId
    });
  } catch (error) {
    console.error("❌ ERRO BOT:", error.message);

    res.status(400).json({
      error: error.message || "Erro no depósito automático"
    });
  }
});

app.post("/deposito/confirmar-bot", authBot, async (req, res) => {
  try {
    const txid = sanitizeBotIdentifier(req.body.txid);
    const idTransacao = sanitizeBotIdentifier(req.body.idTransacao);
    const cardKey = sanitizeBotIdentifier(req.body.cardKey, { allowColon: true });
    const fallbackKey = sanitizeBotIdentifier(req.body.fallbackKey);
    const valorBot = toMoney(req.body.valorLiquido);
    const nomeExtrato = normalizarNome(req.body.nomePagador);
    const dataHoraBot = normalizarDataHoraLocal(req.body.dataHora);
    const eventFingerprint = buildDentpegEventFingerprint({
      txid,
      idTransacao,
      cardKey,
      fallbackKey,
      valorLiquido: valorBot,
      nomePagador: nomeExtrato,
      dataHora: dataHoraBot,
      raw: req.body.raw || null
    });

    if (!txid && !idTransacao && !cardKey && !fallbackKey) {
      return res.status(400).json({ error: "Identificador da transacao obrigatorio" });
    }

    if (!valorBot || valorBot <= 0) {
      return res.status(400).json({ error: "Valor obrigatorio" });
    }

    if (!nomeExtrato || !nomeExtrato.includes(" ")) {
      return res.status(400).json({ error: "Nome do pagador invalido" });
    }

    if (!dataHoraBot) {
      return res.status(400).json({ error: "dataHora invalida" });
    }

    const referenceKey = txid
      ? `dentpeg:txid:${txid}`
      : idTransacao
        ? `dentpeg:id:${idTransacao}`
        : cardKey
          ? `dentpeg:card:${cardKey}`
          : `dentpeg:fallback:${fallbackKey}`;

    const txConsumida = await findExistingDentpegTransactionByEvent(pool, {
      referenceKey,
      txid,
      idTransacao,
      cardKey,
      fallbackKey,
      eventFingerprint
    });

    if (txConsumida) {
      console.log("⛔ CARD JA UTILIZADO IGNORADO:", {
        referenceKey,
        cardKey,
        eventFingerprint,
        financialTransactionId: txConsumida.id
      });
      return res.json({ ok: true, duplicado: true });
    }

    const resultado = await runInTransaction(async (client) => {
      const duplicadoTx = await findExistingDentpegTransactionByEvent(client, {
        referenceKey,
        txid,
        idTransacao,
        cardKey,
        fallbackKey,
        eventFingerprint
      });

      if (duplicadoTx) {
        return { duplicado: true, saldo: null, depositoId: null };
      }

      const candidatos = await client.query(
        `
        SELECT *
        FROM depositos
        WHERE status = 'pendente'
          AND tipo_transacao = 'entrada'
        ORDER BY comprovante_enviado_em DESC NULLS LAST, criado_em DESC NULLS LAST, id DESC
        FOR UPDATE
        `
      );

      let depositoMatch = null;
      let dataHoraComprovanteMatch = null;

      for (const row of candidatos.rows) {
        const dep = mapDeposito(row);
        const textoComprovante = normalizarNome(dep.comprovanteTexto);
        const datasComprovante = extrairDatasDoComprovante(dep.comprovanteTexto);
        const tComprovanteEnviado = Date.parse(String(dep.comprovanteEnviadoEm || ""));
        const idadeComprovanteMin = Number.isNaN(tComprovanteEnviado)
          ? null
          : (Date.now() - tComprovanteEnviado) / 60000;
        const comprovanteRecente =
          idadeComprovanteMin !== null &&
          idadeComprovanteMin >= 0 &&
          idadeComprovanteMin <= COMPROVANTE_UPLOAD_WINDOW_MINUTES;

        if (!textoComprovante || textoComprovante.length < 5) {
          console.log("⚠️ OCR vazio ou invalido para deposito", dep.id);
          continue;
        }

        if (!comprovanteRecente) {
          console.log("⚠️ Comprovante fora da janela valida", dep.id, {
            comprovanteEnviadoEm: dep.comprovanteEnviadoEm,
            idadeComprovanteMin
          });
          continue;
        }

        if (datasComprovante.length === 0) {
          console.log("⚠️ Data nao encontrada no comprovante", dep.id);
          continue;
        }

        const bateNome = bateNomeComprovante(textoComprovante, nomeExtrato);
        if (!bateNome) {
          continue;
        }

        const calc = calcularLiquidoDentpeg(dep.valor);
        const bateValor =
          typeof calc === "number"
            ? Math.abs(calc - valorBot) < 1
            : valorBot >= calc.min && valorBot <= calc.max;

        if (!bateValor) {
          continue;
        }

        const dataBot = normalizarDataLocal(dataHoraBot);
        const dataComprovanteMatch = dataBot && datasComprovante.includes(dataBot)
          ? dataBot
          : null;
        const bateData = Boolean(dataComprovanteMatch);

        console.log("🔎 MATCH RESULT:", {
          depositoId: dep.id,
          referenceKey,
          cardKey,
          eventFingerprint,
          nomeExtrato,
          textoComprovante,
          valorBot,
          valorPedido: dep.valor,
          comprovanteEnviadoEm: dep.comprovanteEnviadoEm,
          idadeComprovanteMin,
          dataBot: dataHoraBot,
          dataBotNormalizada: dataBot,
          dataComprovanteMatch,
          datasComprovanteEncontradas: datasComprovante.slice(0, 5),
          bateNome,
          bateValor,
          bateData
        });

        if (!bateData) {
          continue;
        }

        depositoMatch = dep;
        dataHoraComprovanteMatch = dataComprovanteMatch;
        break;
      }

      if (!depositoMatch) {
        throw new Error("Nenhum depósito compatível encontrado");
      }

      const usuario = await getUserByIdForUpdate(depositoMatch.userId, client);
      if (!usuario) {
        throw new Error("Usuário não encontrado");
      }

      if (isContaBanida(usuario)) {
        throw new Error(getMensagemContaBanida());
      }

      const identificadorBot = txid || idTransacao || cardKey || fallbackKey;
      const valorFinal = calcularValorCreditadoDeposito(toMoney(depositoMatch.valor));
      const metadata = {
        txid,
        idTransacao,
        cardKey,
        fallbackKey,
        eventFingerprint,
        dataHoraBot,
        dataHoraComprovanteMatch,
        nomePagador: nomeExtrato,
        valorLiquidoBot: valorBot,
        raw: req.body.raw || null
      };

      const tx = await createFinancialTransaction(client, {
        userId: usuario.id,
        referenceKey,
        sourceType: "dentpeg",
        sourceId: identificadorBot || buildId("dentpeg"),
        operationType: "deposit",
        direction: "credit",
        amount: valorFinal,
        description: "Depósito automático DentPeg",
        metadata
      });

      const usuarioAtualizado = await applyLedgerChange(client, {
        userId: usuario.id,
        financialTransactionId: tx.id,
        entryType: "credit",
        amount: valorFinal,
        description: "Depósito automático DentPeg",
        metadata
      });

      depositoMatch.status = "aprovado";
      depositoMatch.aprovadoEm = db();
      depositoMatch.descricao = `Auto aprovado DentPeg ${identificadorBot}`;

      await saveDeposito(depositoMatch, client);

      await createAuditLog(client, {
        action: "auto_deposit",
        targetType: "deposito",
        targetId: depositoMatch.id,
        details: {
          txid,
          idTransacao,
          cardKey,
          fallbackKey,
          eventFingerprint,
          userId: usuario.id,
          valor: valorFinal,
          dataHoraBot,
          dataHoraComprovanteMatch
        },
        ipAddress: "bot"
      });

      return {
        duplicado: false,
        saldo: usuarioAtualizado.saldo,
        depositoId: depositoMatch.id
      };
    });

    if (resultado.duplicado) {
      return res.json({ message: "Transacao ja processada", duplicado: true });
    }

    res.json({
      message: "Depósito automático aprovado",
      saldo: resultado.saldo,
      depositoId: resultado.depositoId
    });
  } catch (error) {
    console.error("❌ ERRO BOT:", error.message);

    res.status(400).json({
      error: error.message || "Erro no depósito automático"
    });
  }
});

initDB()
  .then(() => {
    startBackupScheduler();
    app.listen(PORT, () => {
      console.log(`Servidor rodando na porta ${PORT}`);
    });
  })
  .catch((error) => {
    console.error("Erro ao iniciar banco:", error);
    process.exit(1);
  });
