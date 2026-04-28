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
    bonusBoasVindasConcedidoEm: row.bonus_boas_vindas_concedido_em || null
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

  return buildUserPublicResponse(user, {
    pixDesbloqueado: valorRecebidoViaPix >= PIX_SAQUE_DESBLOQUEIO_MIN,
    valorRecebidoViaPix,
    valorMinimoDesbloqueioPix: PIX_SAQUE_DESBLOQUEIO_MIN,
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

async function saveUser(user, client = pool) {
  await client.query(
    `
    INSERT INTO usuarios (
      id, nome, email, senha, saldo, criado_em,
      nome_atualizado_em, saldo_atualizado_em, senha_atualizada_em,
      status_conta, conta_banida_em, motivo_banimento,
      bonus_boas_vindas, bonus_boas_vindas_concedido_em
    )
    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)
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
      bonus_boas_vindas_concedido_em = EXCLUDED.bonus_boas_vindas_concedido_em
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
      user.bonusBoasVindasConcedidoEm || null
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

async function isContaOrigemFraudeBonus(userId, client = pool) {
  if (!userId) return false;

  const user = await getUserById(userId, client);

  if (!user) return false;
  if (toMoney(user.bonusBoasVindas) <= 0) return false;
  if (toMoney(user.saldo) > 0) return false;

  const resumo = await getResumoFinanceiroUsuario(userId, client);

  if (resumo.qualifyingPixTotal > 0) return false;
  if (resumo.otherCompletedOps > 0) return false;
  if (resumo.transferInCount > 0) return false;
  if (resumo.transferOutCount <= 0) return false;

  return resumo.completedCount <= resumo.transferOutCount + 1;
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
          toEmail: destino.email
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
          counterpartEmail: destino.email
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
          toUserId: destino.id
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
          counterpartEmail: remetente.email
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

app.get("/usuarios", authAdmin, async (req, res) => {
  try {
    const result = await listUsers();
    res.json(
      result.map((u) => ({
        id: u.id,
        nome: u.nome,
        email: u.email,
        saldo: toMoney(u.saldo),
        criadoEm: u.criadoEm || null,
        statusConta: u.statusConta || STATUS_CONTA_ATIVA,
        contaBanida: isContaBanida(u),
        contaBanidaEm: u.contaBanidaEm || null,
        bonusBoasVindas: toMoney(u.bonusBoasVindas)
      }))
    );
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

    res.json({
      user: {
        id: user.id,
        email: user.email,
        saldo: toMoney(user.saldo)
      },
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
