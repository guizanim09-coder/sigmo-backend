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
const PUBLIC_WEB_BASE_URL = String(
  process.env.PUBLIC_WEB_BASE_URL || "https://sigmopay.com"
)
  .trim()
  .replace(/\/+$/, "");
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
const TAXA_RECARGA_CELULAR_PERCENTUAL = Number(
  process.env.TAXA_RECARGA_CELULAR_PERCENTUAL || 0.10
);
const RECARGA_CELULAR_VALORES_POR_OPERADORA = {
  tim: [20, 30, 40, 50, 60, 100],
  claro: [20, 25, 30, 35, 40, 50, 100],
  vivo: [20, 25, 30, 35, 40, 50, 100, 200, 300]
};
const LIMITE_RECARGA_CELULAR_MIN = Math.min(
  ...Object.values(RECARGA_CELULAR_VALORES_POR_OPERADORA).flat()
);
const LIMITE_RECARGA_CELULAR_MAX = Math.max(
  ...Object.values(RECARGA_CELULAR_VALORES_POR_OPERADORA).flat()
);
const COMPROVANTE_UPLOAD_WINDOW_MINUTES = Number(
  process.env.COMPROVANTE_UPLOAD_WINDOW_MINUTES || 60
);
const BONUS_INDICACAO_VALOR = Number(
  process.env.BONUS_INDICACAO_VALOR || process.env.BONUS_BOAS_VINDAS_VALOR || 5
);
const INDICACAO_PIX_QUALIFICACAO_MIN = Number(
  process.env.INDICACAO_PIX_QUALIFICACAO_MIN || 100
);
const PIX_SAQUE_DESBLOQUEIO_MIN = Number(
  process.env.PIX_SAQUE_DESBLOQUEIO_MIN || 100
);
const INVESTIMENTOS_PIX_DESBLOQUEIO_MIN = Number(
  process.env.INVESTIMENTOS_PIX_DESBLOQUEIO_MIN || 10000
);
const INVESTIMENTOS_CDI_ANUAL_REFERENCIA = Number(
  process.env.INVESTIMENTOS_CDI_ANUAL_REFERENCIA || 0.105
);
const INVESTIMENTOS_JUNIOR_MOVIMENTACAO_MENSAL_MIN = Number(
  process.env.INVESTIMENTOS_JUNIOR_MOVIMENTACAO_MENSAL_MIN || 1500
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
const BANNER_ROTATION_DEFAULT_MS = 7000;
const BANNER_ROTATION_MIN_MS = 2500;
const BANNER_ROTATION_MAX_MS = 30000;
const BANNER_DURATION_DEFAULT_MS = 7000;
const BANNER_SETTINGS_ID = "main";
const APP_RUNTIME_SETTINGS_ID = "main";
const APP_MAINTENANCE_MESSAGE_DEFAULT =
  "Estamos passando por atualizacoes, voltaremos em breve.";
const APP_MAINTENANCE_ETA_DEFAULT_MINUTES = 9;
const APP_MAINTENANCE_ETA_MIN_MINUTES = 1;
const APP_MAINTENANCE_ETA_MAX_MINUTES = 180;
const BANNER_IMAGE_MAX_BYTES = 10 * 1024 * 1024;
const STATUS_CONTA_ATIVA = "ativa";
const STATUS_CONTA_BLOQUEADA = "bloqueada";
const STATUS_CONTA_BANIDA = "banida";
const MOTIVO_BANIMENTO_FRAUDE_BONUS = "tentativa_fraude_bonus";
const MOTIVO_BANIMENTO_LABELS = Object.freeze({
  [MOTIVO_BANIMENTO_FRAUDE_BONUS]: "Tentativa de fraude com bonus"
});
const USER_NOTIFICATION_TYPE_LIMIT_REQUEST_PIX_KEY = "limit_request_pix_key";
const MOVEMENT_LIMIT_REQUEST_STATUS_PENDING = "pending";
const MOVEMENT_LIMIT_REQUEST_STATUS_RESPONDED = "responded";
const MOVEMENT_LIMIT_REQUEST_STATUS_CLOSED = "closed";
const INVESTMENT_RESERVE_STATUS_ACTIVE = "active";
const INVESTMENT_RESERVE_STATUS_PARTIAL = "partial";
const INVESTMENT_RESERVE_STATUS_CLOSED = "closed";
const SHOP_DEFAULT_MARKUP_PERCENTUAL = Number(
  process.env.SHOP_DEFAULT_MARKUP_PERCENTUAL || 110
);
const SHOP_SLUG_MAX_LENGTH = 72;
const SHOP_ORDER_STATUS_PENDING = "pendente";
const SHOP_ORDER_STATUS_APPROVED = "aprovado";
const SHOP_ORDER_STATUS_REFUSED = "recusado";
const SHOP_PRODUCT_SOURCE_DEFAULT = "kaiross";
const DATABASE_POOL_MAX = Math.max(5, Number(process.env.DATABASE_POOL_MAX || 20));
const DATABASE_POOL_IDLE_TIMEOUT_MS = Math.max(
  1000,
  Number(process.env.DATABASE_POOL_IDLE_TIMEOUT_MS || 30000)
);
const DATABASE_POOL_CONNECTION_TIMEOUT_MS = Math.max(
  1000,
  Number(process.env.DATABASE_POOL_CONNECTION_TIMEOUT_MS || 10000)
);
const GLOBAL_API_RATE_LIMIT_WINDOW_MS = Math.max(
  1000,
  Number(process.env.GLOBAL_API_RATE_LIMIT_WINDOW_MS || 60000)
);
const GLOBAL_API_RATE_LIMIT_MAX = Math.max(
  60,
  Number(process.env.GLOBAL_API_RATE_LIMIT_MAX || 120)
);
const PUBLIC_SHOP_CATALOG_RATE_LIMIT_WINDOW_MS = Math.max(
  1000,
  Number(process.env.PUBLIC_SHOP_CATALOG_RATE_LIMIT_WINDOW_MS || 60000)
);
const PUBLIC_SHOP_CATALOG_RATE_LIMIT_MAX = Math.max(
  120,
  Number(process.env.PUBLIC_SHOP_CATALOG_RATE_LIMIT_MAX || 6000)
);
const SHOP_ORDER_RATE_LIMIT_WINDOW_MS = Math.max(
  1000,
  Number(process.env.SHOP_ORDER_RATE_LIMIT_WINDOW_MS || 60000)
);
const SHOP_ORDER_RATE_LIMIT_MAX = Math.max(
  5,
  Number(process.env.SHOP_ORDER_RATE_LIMIT_MAX || 20)
);
const SHOP_PUBLIC_CATALOG_CACHE_TTL_MS = Math.max(
  5000,
  Number(process.env.SHOP_PUBLIC_CATALOG_CACHE_TTL_MS || 30000)
);
const SHOP_PUBLIC_CATALOG_STALE_WHILE_REVALIDATE_MS = Math.max(
  5000,
  Number(process.env.SHOP_PUBLIC_CATALOG_STALE_WHILE_REVALIDATE_MS || 120000)
);
const SHOP_PUBLIC_CATALOG_MAX_AGE_SECONDS = Math.max(
  1,
  Number(process.env.SHOP_PUBLIC_CATALOG_MAX_AGE_SECONDS || 30)
);
const SHOP_PUBLIC_CATALOG_STALE_WHILE_REVALIDATE_SECONDS = Math.max(
  SHOP_PUBLIC_CATALOG_MAX_AGE_SECONDS,
  Number(process.env.SHOP_PUBLIC_CATALOG_STALE_WHILE_REVALIDATE_SECONDS || 120)
);
const KAIROSS_BASE_URL = String(
  process.env.KAIROSS_BASE_URL || "https://app.kaiross.com.br"
)
  .trim()
  .replace(/\/+$/, "");
const KAIROSS_EMAIL = String(process.env.KAIROSS_EMAIL || "").trim().toLowerCase();
const KAIROSS_PASSWORD = String(process.env.KAIROSS_PASSWORD || "").trim();
const KAIROSS_TIMEOUT_MS = Math.max(
  3000,
  Number(process.env.KAIROSS_TIMEOUT_MS || 20000)
);
const SHOP_EMPTY_CATALOG_RECOVERY_MIN_INTERVAL_MS = Math.max(
  30000,
  Number(process.env.SHOP_EMPTY_CATALOG_RECOVERY_MIN_INTERVAL_MS || 300000)
);
const KAIROSS_LOGIN_PATH = "/api/auth/login";
const KAIROSS_PRODUCTS_PATH = "/api/produtos";
const KAIROSS_VITRINE_PATH = "/vitrine-de-produtos";
const INVESTMENT_PRODUCT_DEFINITIONS = {
  junior: {
    key: "junior",
    name: "JUNIOR",
    headline: "150% do CDI",
    cdiMultiplier: 1.5,
    minAmount: LIMITE_DEPOSITO_MIN,
    maxAmount: 10000,
    minDisplayCapacity: 10000,
    lockMonths: 0,
    minHoldDaysForProfit: 30,
    withdrawLock: false,
    allowPartialWithdraw: true,
    movementRequiredPerMonth: INVESTIMENTOS_JUNIOR_MOVIMENTACAO_MENSAL_MIN,
    description:
      "Liquidez diaria com rendimento liberado no resgate para valores mantidos por pelo menos 30 dias consecutivos."
  },
  pleno: {
    key: "pleno",
    name: "PLENO",
    headline: "200% do CDI",
    cdiMultiplier: 2,
    minAmount: LIMITE_DEPOSITO_MIN,
    maxAmount: 50000,
    minDisplayCapacity: 50000,
    lockMonths: 6,
    minHoldDaysForProfit: 0,
    withdrawLock: true,
    allowPartialWithdraw: false,
    movementRequiredPerMonth: 0,
    description:
      "Reserva com prazo fixo de 6 meses. O principal fica travado ate a data de liberacao."
  },
  senior: {
    key: "senior",
    name: "SENIOR",
    headline: "240% do CDI",
    cdiMultiplier: 2.4,
    minAmount: 100000,
    maxAmount: 500000,
    minDisplayCapacity: 500000,
    lockMonths: 12,
    minHoldDaysForProfit: 0,
    withdrawLock: true,
    allowPartialWithdraw: false,
    movementRequiredPerMonth: 0,
    description:
      "Reserva anual premium com aporte inicial de R$100.000,00 e travamento completo ate a liberacao."
  },
  executive: {
    key: "executive",
    name: "Executive",
    headline: "350% do CDI",
    cdiMultiplier: 3.5,
    minAmount: 1000000,
    maxAmount: null,
    minDisplayCapacity: 1000000,
    lockMonths: 18,
    minHoldDaysForProfit: 0,
    withdrawLock: true,
    allowPartialWithdraw: false,
    movementRequiredPerMonth: 0,
    description:
      "Estrutura de alta renda para aportes acima de R$1.000.000,00 com liberacao apos 18 meses."
  }
};
const RECARGA_CELULAR_OPERADORAS = [
  { id: "tim", label: "TIM", values: RECARGA_CELULAR_VALORES_POR_OPERADORA.tim },
  { id: "claro", label: "Claro", values: RECARGA_CELULAR_VALORES_POR_OPERADORA.claro },
  { id: "vivo", label: "Vivo", values: RECARGA_CELULAR_VALORES_POR_OPERADORA.vivo }
];
const RECARGA_CELULAR_OPERADORAS_IDS = new Set(
  RECARGA_CELULAR_OPERADORAS.map((item) => item.id)
);
const RECARGA_CELULAR_OPERADORAS_LABELS = RECARGA_CELULAR_OPERADORAS.reduce(
  (acc, item) => {
    acc[item.id] = item.label;
    return acc;
  },
  {}
);

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
  ssl: { rejectUnauthorized: false },
  max: DATABASE_POOL_MAX,
  idleTimeoutMillis: DATABASE_POOL_IDLE_TIMEOUT_MS,
  connectionTimeoutMillis: DATABASE_POOL_CONNECTION_TIMEOUT_MS,
  keepAlive: true
});

const shopPublicCatalogCache = {
  snapshot: null,
  refreshPromise: null,
  expiresAtMs: 0,
  staleUntilMs: 0,
  version: 0
};
const SHOP_CATALOG_SEED_FILE = path.join(__dirname, "shop_catalog_seed.json");
const shopCatalogRecoveryState = {
  activePromise: null,
  lastAttemptMs: 0,
  lastSuccessAtMs: 0,
  lastSuccessSource: "",
  lastFailureAtMs: 0,
  lastFailureMessage: ""
};

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
        recargasCelular,
        shopCategories,
        shopProducts,
        shopOrders,
        shopOrderItems,
        admins,
        financialTransactions,
        ledgerEntries,
        auditLogs
      ] = await Promise.all([
        client.query("SELECT * FROM usuarios ORDER BY criado_em ASC NULLS LAST, id ASC"),
        client.query("SELECT * FROM depositos ORDER BY criado_em ASC NULLS LAST, id ASC"),
        client.query(
          "SELECT * FROM topup_orders ORDER BY criado_em ASC NULLS LAST, id ASC"
        ),
        client.query(
          "SELECT * FROM shop_categories ORDER BY sort_order ASC, created_at ASC NULLS LAST, id ASC"
        ),
        client.query(
          "SELECT * FROM shop_products ORDER BY category_id ASC, name ASC, created_at ASC NULLS LAST, id ASC"
        ),
        client.query(
          "SELECT * FROM shop_orders ORDER BY created_at ASC NULLS LAST, id ASC"
        ),
        client.query(
          "SELECT * FROM shop_order_items ORDER BY created_at ASC NULLS LAST, id ASC"
        ),
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
          version: 4,
          tables: {
            usuarios: usuarios.rowCount,
            depositos: depositos.rowCount,
            topup_orders: recargasCelular.rowCount,
            shop_categories: shopCategories.rowCount,
            shop_products: shopProducts.rowCount,
            shop_orders: shopOrders.rowCount,
            shop_order_items: shopOrderItems.rowCount,
            admins: admins.rowCount,
            financial_transactions: financialTransactions.rowCount,
            ledger_entries: ledgerEntries.rowCount,
            audit_logs: auditLogs.rowCount
          }
        },
        data: {
          usuarios: usuarios.rows,
          depositos: depositos.rows,
          topup_orders: recargasCelular.rows,
          shop_categories: shopCategories.rows,
          shop_products: shopProducts.rows,
          shop_orders: shopOrders.rows,
          shop_order_items: shopOrderItems.rows,
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
        `[backup] concluído (${trigger}) arquivo=${fileName} usuarios=${usuarios.rowCount} depositos=${depositos.rowCount} shop_produtos=${shopProducts.rowCount} shop_pedidos=${shopOrders.rowCount} admins=${admins.rowCount} tx=${financialTransactions.rowCount} ledger=${ledgerEntries.rowCount} audit=${auditLogs.rowCount} removidos=${cleanup.removed}`
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

function normalizeDigits(value, maxLength = 32) {
  return String(value || "")
    .replace(/\D/g, "")
    .slice(0, Math.max(0, Number(maxLength || 0) || 0));
}

function normalizeEmail(email) {
  return String(email || "").trim().toLowerCase();
}

function normalizeReferralCode(value) {
  return String(value || "")
    .trim()
    .toUpperCase()
    .replace(/[^A-Z0-9]/g, "")
    .slice(0, 24);
}

function buildReferralCodeCandidate() {
  return `SIG${crypto
    .randomBytes(6)
    .toString("base64url")
    .replace(/[^A-Za-z0-9]/g, "")
    .toUpperCase()
    .slice(0, 9)}`;
}

function buildReferralLinkFromCode(referralCode) {
  const normalized = normalizeReferralCode(referralCode);
  if (!normalized) return "";
  return `${PUBLIC_WEB_BASE_URL}/login.html?ref=${encodeURIComponent(normalized)}`;
}

function toMoney(value) {
  const num = Number(value || 0);
  if (!Number.isFinite(num)) return 0;
  return Number(num.toFixed(2));
}

function normalizeShopText(value, maxLength = 500) {
  return String(value || "")
    .replace(/\s+/g, " ")
    .trim()
    .slice(0, Math.max(0, Number(maxLength || 0) || 0));
}

function scoreShopCatalogTextQuality(value) {
  const text = String(value || "");
  if (!text) return 0;

  const mojibakeMatches = text.match(/(?:Ã.|Â.|â.|Ê.|Ô.|Õ.|Ð.|�)/g) || [];
  const replacementMatches = text.match(/�/g) || [];
  const accentedMatches = text.match(/[À-ÿ]/g) || [];
  const alphaNumMatches = text.match(/[A-Za-z0-9]/g) || [];

  return (
    accentedMatches.length +
    alphaNumMatches.length * 0.05 -
    mojibakeMatches.length * 6 -
    replacementMatches.length * 8
  );
}

function repairShopCatalogText(value) {
  let current = String(value || "");
  if (!current) return "";

  let best = current;
  let bestScore = scoreShopCatalogTextQuality(best);
  const seen = new Set([current]);

  for (let attempt = 0; attempt < 2; attempt += 1) {
    let candidate = "";

    try {
      candidate = Buffer.from(current, "latin1").toString("utf8");
    } catch {
      break;
    }

    if (!candidate || seen.has(candidate)) {
      break;
    }

    seen.add(candidate);
    const candidateScore = scoreShopCatalogTextQuality(candidate);

    if (candidateScore <= bestScore) {
      break;
    }

    best = candidate;
    bestScore = candidateScore;
    current = candidate;
  }

  return best;
}

function normalizeShopCatalogText(value, maxLength = 500) {
  return normalizeShopText(repairShopCatalogText(value), maxLength);
}

function normalizeShopCatalogCategoryName(value, maxLength = 120) {
  return normalizeShopCatalogText(value, maxLength) || "Sem categoria";
}

function slugifyShopValue(value, fallback = "item", maxLength = SHOP_SLUG_MAX_LENGTH) {
  const safeMaxLength = Math.max(1, Number(maxLength || SHOP_SLUG_MAX_LENGTH) || SHOP_SLUG_MAX_LENGTH);
  const normalized = String(value || "")
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "")
    .slice(0, safeMaxLength);

  return normalized || fallback;
}

function buildShopUniqueSlug(value, uniqueKey, fallback = "item") {
  const normalizedUniqueKey = String(uniqueKey || "").trim();

  if (!normalizedUniqueKey) {
    return slugifyShopValue(value, fallback);
  }

  const hashSuffix = crypto.createHash("md5").update(normalizedUniqueKey).digest("hex").slice(0, 8);
  const baseMaxLength = Math.max(1, SHOP_SLUG_MAX_LENGTH - hashSuffix.length - 1);
  const baseSlug = slugifyShopValue(value, fallback, baseMaxLength);

  return `${baseSlug}-${hashSuffix}`;
}

function normalizeShopUrl(value) {
  const raw = String(value || "").trim();
  if (!raw) return "";

  try {
    const parsed = new URL(raw);
    if (!["http:", "https:"].includes(parsed.protocol)) {
      return "";
    }
    return parsed.toString();
  } catch {
    return "";
  }
}

function normalizeShopSource(value) {
  return (
    normalizeShopText(value, 48)
      .toLowerCase()
      .replace(/[^a-z0-9_-]/g, "") || SHOP_PRODUCT_SOURCE_DEFAULT
  );
}

function normalizeShopCategorySourceKey(value, source = SHOP_PRODUCT_SOURCE_DEFAULT) {
  const cleaned = normalizeShopText(value, 160)
    .toLowerCase()
    .replace(/[^a-z0-9:_-]/g, "");
  return cleaned || `${normalizeShopSource(source)}:categoria`;
}

function normalizeShopProductSourceKey(value, source = SHOP_PRODUCT_SOURCE_DEFAULT) {
  const cleaned = normalizeShopText(value, 220)
    .toLowerCase()
    .replace(/[^a-z0-9:/._-]/g, "");
  return cleaned || `${normalizeShopSource(source)}:produto`;
}

function normalizeShopMarkupPercent(value) {
  const numeric = Number(value);
  if (!Number.isFinite(numeric) || numeric < 0) {
    return toMoney(SHOP_DEFAULT_MARKUP_PERCENTUAL);
  }
  return toMoney(Math.min(10000, numeric));
}

function calculateShopSalePrice(supplierPrice, markupPercent = SHOP_DEFAULT_MARKUP_PERCENTUAL) {
  const cost = Math.max(0, toMoney(supplierPrice));
  const markup = normalizeShopMarkupPercent(markupPercent);
  return toMoney(cost * (1 + markup / 100));
}

function normalizeHttpBaseUrl(value, fallback = "") {
  const raw = String(value || "").trim().replace(/\/+$/, "");

  if (!raw) {
    return String(fallback || "").trim().replace(/\/+$/, "");
  }

  try {
    const parsed = new URL(raw);
    if (!["http:", "https:"].includes(parsed.protocol)) {
      return String(fallback || "").trim().replace(/\/+$/, "");
    }
    return parsed.toString().replace(/\/+$/, "");
  } catch {
    return String(fallback || "").trim().replace(/\/+$/, "");
  }
}

function splitSetCookieHeader(value) {
  const raw = String(value || "").trim();
  if (!raw) return [];

  return raw
    .split(/,(?=\s*[^=;,\s]+=[^;,]+)/g)
    .map((item) => String(item || "").trim())
    .filter(Boolean);
}

function extractCookieHeaderFromResponse(response) {
  const headers = response?.headers;
  if (!headers) return "";

  const setCookies =
    typeof headers.getSetCookie === "function"
      ? headers.getSetCookie()
      : splitSetCookieHeader(headers.get("set-cookie"));

  return setCookies
    .map((entry) => String(entry || "").split(";")[0].trim())
    .filter(Boolean)
    .join("; ");
}

async function fetchJsonWithTimeout(url, options = {}, timeoutMs = 15000) {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), Math.max(1000, Number(timeoutMs || 0) || 0));

  try {
    const response = await fetch(url, {
      ...options,
      headers: {
        Accept: "application/json",
        ...(options.headers || {})
      },
      signal: controller.signal
    });
    const data = await response.json().catch(() => ({}));

    return { response, data };
  } catch (error) {
    if (error?.name === "AbortError") {
      throw new Error("Tempo esgotado ao consultar provedor externo");
    }
    throw error;
  } finally {
    clearTimeout(timeout);
  }
}

function buildKairossProductShortDescription(product) {
  const descricao = normalizeShopCatalogText(product?.descricao, 320);
  if (descricao) return descricao;

  const pieces = [
    normalizeShopCatalogText(product?.marca, 60),
    normalizeShopCatalogText(product?.cor, 40),
    normalizeShopCatalogText(product?.tamanho, 40),
    normalizeShopCatalogText(product?.sku, 60)
      ? `SKU ${normalizeShopCatalogText(product?.sku, 60)}`
      : ""
  ].filter(Boolean);

  return normalizeShopCatalogText(
    pieces.join(" | ") || "Produto importado automaticamente da Kaiross.",
    320
  );
}

function buildKairossProductDescription(product) {
  const descricao = normalizeShopCatalogText(product?.descricao, 2400);
  if (descricao) return descricao;

  const pieces = [
    normalizeShopCatalogText(product?.marca, 80),
    normalizeShopCatalogText(product?.cor, 80),
    normalizeShopCatalogText(product?.tamanho, 80),
    normalizeShopCatalogText(product?.ncm, 40)
      ? `NCM ${normalizeShopCatalogText(product?.ncm, 40)}`
      : "",
    normalizeShopCatalogText(product?.sku, 80)
      ? `SKU ${normalizeShopCatalogText(product?.sku, 80)}`
      : ""
  ].filter(Boolean);

  return normalizeShopCatalogText(
    pieces.join(" | ") || "Produto importado automaticamente da Kaiross.",
    2400
  );
}

async function loginKaiross(options = {}) {
  const baseUrl = normalizeHttpBaseUrl(
    options.baseUrl || KAIROSS_BASE_URL,
    "https://app.kaiross.com.br"
  );
  const email = normalizeEmail(options.email || KAIROSS_EMAIL);
  const senha = String(options.senha || KAIROSS_PASSWORD || "").trim();

  if (!email || !senha) {
    throw new Error("Credenciais da Kaiross obrigatorias");
  }

  const { response, data } = await fetchJsonWithTimeout(
    `${baseUrl}${KAIROSS_LOGIN_PATH}`,
    {
      method: "POST",
      headers: {
        "Content-Type": "application/json"
      },
      body: JSON.stringify({ email, senha })
    },
    KAIROSS_TIMEOUT_MS
  );

  if (!response.ok) {
    throw new Error(data?.error || data?.message || "Falha ao autenticar na Kaiross");
  }

  const cookieHeader = extractCookieHeaderFromResponse(response);

  if (!cookieHeader) {
    throw new Error("A Kaiross nao retornou cookies de autenticacao");
  }

  return {
    baseUrl,
    cookieHeader,
    user: data?.user || null
  };
}

async function fetchKairossProducts(options = {}) {
  async function requestProducts(session) {
    const { response, data } = await fetchJsonWithTimeout(
      `${session.baseUrl}${KAIROSS_PRODUCTS_PATH}`,
      {
        method: "GET",
        headers: {
          Cookie: session.cookieHeader
        }
      },
      KAIROSS_TIMEOUT_MS
    );

    return { session, response, data };
  }

  function normalizeKairossProductsPayload(data) {
    const products = Array.isArray(data)
      ? data
      : Array.isArray(data?.produtos)
        ? data.produtos
        : Array.isArray(data?.items)
          ? data.items
          : Array.isArray(data?.data)
            ? data.data
            : null;

    if (!Array.isArray(products)) {
      throw new Error("A Kaiross nao retornou uma lista valida de produtos");
    }

    return products;
  }

  let result = await requestProducts(await loginKaiross(options));

  if (!result.response.ok && [401, 403].includes(Number(result.response.status || 0))) {
    result = await requestProducts(await loginKaiross(options));
  }

  if (!result.response.ok) {
    throw new Error(
      result.data?.error || result.data?.message || "Falha ao carregar produtos da Kaiross"
    );
  }

  return {
    ...result.session,
    products: normalizeKairossProductsPayload(result.data)
  };
}

function buildKairossShopImportPayload(products, options = {}) {
  const source = normalizeShopSource(options.source || SHOP_PRODUCT_SOURCE_DEFAULT);
  const markupPercent = normalizeShopMarkupPercent(
    options.markupPercent === undefined ? SHOP_DEFAULT_MARKUP_PERCENTUAL : options.markupPercent
  );
  const baseUrl = normalizeHttpBaseUrl(
    options.baseUrl || KAIROSS_BASE_URL,
    "https://app.kaiross.com.br"
  );
  const vitrineUrl = `${baseUrl}${KAIROSS_VITRINE_PATH}`;
  const categories = new Map();

  for (const rawProduct of Array.isArray(products) ? products : []) {
    const categoryName = normalizeShopCatalogCategoryName(rawProduct?.categoria, 120);

    if (!categories.has(categoryName)) {
      categories.set(categoryName, {
        name: categoryName,
        sourceKey: `${source}:category:${slugifyShopValue(categoryName, "categoria")}`,
        sortOrder: categories.size,
        products: []
      });
    }

    const supplierPrice = Math.max(
      0,
      toMoney(
        rawProduct?.precoSugerido ??
          rawProduct?.preco ??
          rawProduct?.valor ??
          rawProduct?.price
      )
    );
    const rawStockNumber = Number(rawProduct?.estoque);
    const hasStockNumber = Number.isFinite(rawStockNumber);
    const active =
      rawProduct?.ativo === true &&
      rawProduct?.pausadoPorEstoque !== true &&
      (!hasStockNumber || rawStockNumber > 0);
    const secondaryImages = Array.isArray(rawProduct?.imagensSecundariasUrls)
      ? rawProduct.imagensSecundariasUrls
      : [];
    const fallbackImage = secondaryImages.find((item) => normalizeShopUrl(item));
    const imageUrl = normalizeShopUrl(rawProduct?.imagemPrincipalUrl || fallbackImage || "");
    const externalId = normalizeShopCatalogText(
      rawProduct?.externalId || rawProduct?.id || rawProduct?.sku,
      120
    );
    const externalKey =
      externalId ||
      slugifyShopValue(
        normalizeShopCatalogText(
          rawProduct?.nome || rawProduct?.sku || rawProduct?.ean || "produto-kaiross",
          180
        ),
        "produto-kaiross"
      );

    categories.get(categoryName).products.push({
      sourceKey: `${source}:product:${externalKey}`,
      externalId,
      externalUrl: vitrineUrl,
      name: normalizeShopCatalogText(rawProduct?.nome, 180),
      shortDescription: buildKairossProductShortDescription(rawProduct),
      description: buildKairossProductDescription(rawProduct),
      imageUrl,
      supplierPrice,
      markupPercent,
      active,
      rawPayload: {
        ...rawProduct,
        importedFrom: "kaiross",
        importedAt: db(),
        vitrineUrl
      }
    });
  }

  return {
    source,
    markupPercent,
    categories: Array.from(categories.values())
  };
}

function normalizeShopOrderStatus(value) {
  const normalized = String(value || "").trim().toLowerCase();
  if (
    [
      SHOP_ORDER_STATUS_PENDING,
      SHOP_ORDER_STATUS_APPROVED,
      SHOP_ORDER_STATUS_REFUSED
    ].includes(normalized)
  ) {
    return normalized;
  }
  return SHOP_ORDER_STATUS_PENDING;
}

function normalizeShopQuantity(value) {
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) return 0;
  return Math.max(0, Math.floor(numeric));
}

function normalizeShopPostalCode(value) {
  return normalizeDigits(value, 8);
}

function normalizeShopPhone(value) {
  return normalizeDigits(value, 15);
}

function normalizeShopState(value) {
  return normalizeShopText(value, 2).toUpperCase();
}

function buildShopFormattedAddress(address = {}) {
  const parts = [
    normalizeShopText(address.shippingStreet || address.street, 120),
    normalizeShopText(address.shippingNumber || address.number, 20),
    normalizeShopText(address.shippingComplement || address.complement, 120),
    normalizeShopText(address.shippingNeighborhood || address.neighborhood, 120),
    normalizeShopText(address.shippingCity || address.city, 120),
    normalizeShopState(address.shippingState || address.state),
    normalizeShopPostalCode(address.shippingZip || address.zip)
  ].filter(Boolean);

  return parts.join(", ");
}

function montarDescricaoShopPedido(order = {}) {
  const total = toMoney(order.totalAmount);
  const quantity = Array.isArray(order.items)
    ? order.items.reduce((sum, item) => sum + normalizeShopQuantity(item?.quantity), 0)
    : 0;
  const quantityLabel = quantity > 0 ? `${quantity} item(ns)` : "pedido";
  return `Shop Sigmo | ${quantityLabel} | Total debitado: R$${total.toFixed(2)}`;
}

function normalizeStatusConta(value) {
  const normalized = String(value || "")
    .trim()
    .toLowerCase();

  if (
    normalized === STATUS_CONTA_ATIVA ||
    normalized === STATUS_CONTA_BLOQUEADA ||
    normalized === STATUS_CONTA_BANIDA
  ) {
    return normalized;
  }

  return STATUS_CONTA_ATIVA;
}

function normalizeAccountRestrictionReason(value, maxLen = 220) {
  return String(value || "")
    .trim()
    .replace(/\s+/g, " ")
    .slice(0, maxLen);
}

function isContaBloqueada(user) {
  return normalizeStatusConta(user?.statusConta) === STATUS_CONTA_BLOQUEADA;
}

function isContaPermanentementeBanida(user) {
  return normalizeStatusConta(user?.statusConta) === STATUS_CONTA_BANIDA;
}

function isContaBanida(user) {
  return isContaPermanentementeBanida(user) || isContaBloqueada(user);
}

function getMotivoBanimentoFormatado(value) {
  const normalized = String(value || "")
    .trim()
    .toLowerCase();

  if (!normalized) {
    return "";
  }

  return MOTIVO_BANIMENTO_LABELS[normalized] || normalizeAccountRestrictionReason(value);
}

function getMensagemContaBanida(user = null) {
  if (isContaBloqueada(user)) {
    const motivo = normalizeAccountRestrictionReason(user?.motivoBloqueio);
    const motivoTrecho = motivo ? ` Motivo: ${motivo}.` : "";
    const tipoBloqueio = user?.bloqueioTemporario
      ? "temporariamente bloqueada"
      : "bloqueada";

    return `Sua conta esta ${tipoBloqueio}.${motivoTrecho} Enquanto isso, as movimentacoes ficam indisponiveis.`;
  }

  if (isContaPermanentementeBanida(user)) {
    const motivo = getMotivoBanimentoFormatado(
      user?.motivoBanimento || MOTIVO_BANIMENTO_FRAUDE_BONUS
    );
    const motivoTrecho = motivo ? ` Motivo: ${motivo}.` : "";

    return `Sua conta foi banida permanentemente.${motivoTrecho} Esta acao e irreversivel e o saldo ficou congelado.`;
  }

  return "Sua conta esta bloqueada para movimentacoes no momento. Consulte o suporte para mais detalhes.";
}

function normalizeRecargaCelularOperadora(value) {
  const normalized = String(value || "").trim().toLowerCase();
  return RECARGA_CELULAR_OPERADORAS_IDS.has(normalized) ? normalized : "";
}

function getRecargaCelularOperadoraLabel(value) {
  const normalized = normalizeRecargaCelularOperadora(value);
  return RECARGA_CELULAR_OPERADORAS_LABELS[normalized] || String(value || "").trim();
}

function normalizeRecargaCelularStatus(value) {
  const normalized = String(value || "").trim().toLowerCase();
  if (["pendente", "aprovado", "recusado"].includes(normalized)) {
    return normalized;
  }
  return "pendente";
}

function normalizeRecargaCelularDdd(value) {
  return normalizeDigits(value, 2);
}

function normalizeRecargaCelularNumero(value) {
  return normalizeDigits(value, 9);
}

function isValidRecargaCelularTelefone(ddd, numero) {
  return /^\d{2}$/.test(ddd) && /^\d{8,9}$/.test(numero);
}

function buildRecargaCelularTelefone(ddd, numero) {
  const normalizedDdd = normalizeRecargaCelularDdd(ddd);
  const normalizedNumero = normalizeRecargaCelularNumero(numero);
  return `${normalizedDdd}${normalizedNumero}`;
}

function formatRecargaCelularTelefone(ddd, numero) {
  const normalizedDdd = normalizeRecargaCelularDdd(ddd);
  const normalizedNumero = normalizeRecargaCelularNumero(numero);

  if (!normalizedDdd && !normalizedNumero) {
    return "";
  }

  return `(${normalizedDdd}) ${normalizedNumero}`;
}

function normalizeRecargaCelularMotivoRecusa(value) {
  return String(value || "")
    .trim()
    .replace(/\s+/g, " ")
    .slice(0, 220);
}

function normalizeRecargaCelularClientRequestId(value) {
  return String(value || "")
    .trim()
    .replace(/[^A-Za-z0-9_-]/g, "")
    .slice(0, 72);
}

function getRecargaCelularValoresPermitidos(operadora) {
  const normalized = normalizeRecargaCelularOperadora(operadora);
  return Array.isArray(RECARGA_CELULAR_VALORES_POR_OPERADORA[normalized])
    ? RECARGA_CELULAR_VALORES_POR_OPERADORA[normalized]
    : [];
}

function isRecargaCelularValorPermitido(operadora, valorRecarga) {
  const valor = toMoney(valorRecarga);
  return getRecargaCelularValoresPermitidos(operadora).includes(valor);
}

function formatRecargaCelularValoresPermitidos(operadora) {
  return getRecargaCelularValoresPermitidos(operadora)
    .map((valor) => `R$${toMoney(valor).toFixed(2)}`)
    .join(", ");
}

function calcularDetalhesRecargaCelular(valorRecarga) {
  const valor = toMoney(valorRecarga);
  const taxa = toMoney(valor * TAXA_RECARGA_CELULAR_PERCENTUAL);
  const valorTotalDebitado = toMoney(valor + taxa);

  return {
    valorRecarga: valor,
    taxaValor: taxa,
    valorTotalDebitado
  };
}

function montarDescricaoRecargaCelular(recarga = {}) {
  const operadoraLabel = getRecargaCelularOperadoraLabel(recarga.operadora);
  const telefone = formatRecargaCelularTelefone(recarga.ddd, recarga.numero);

  return [
    `Recarga ${operadoraLabel}`.trim(),
    telefone ? `para ${telefone}` : "",
    `| Valor da recarga: R$${toMoney(recarga.valorRecarga).toFixed(2)}`,
    `| Taxa: R$${toMoney(recarga.taxaValor).toFixed(2)}`,
    `| Total debitado: R$${toMoney(recarga.valorTotalDebitado).toFixed(2)}`
  ]
    .filter(Boolean)
    .join(" ");
}

function buildContaBanidaPayload(user, code = "") {
  const statusConta = normalizeStatusConta(user?.statusConta);
  const contaBloqueada = isContaBloqueada(user);
  const contaBanidaPermanente = isContaPermanentementeBanida(user);

  return {
    error: getMensagemContaBanida(user),
    code: code || (contaBloqueada ? "ACCOUNT_BLOCKED" : "ACCOUNT_BANNED"),
    statusConta,
    contaBanida: true,
    contaBloqueada,
    contaBanidaPermanente,
    contaBanidaEm: user?.contaBanidaEm || null,
    motivoBanimento: contaBanidaPermanente
      ? user?.motivoBanimento || MOTIVO_BANIMENTO_FRAUDE_BONUS
      : "",
    contaBloqueadaEm: user?.contaBloqueadaEm || null,
    motivoBloqueio: user?.motivoBloqueio || "",
    bloqueioTemporario: Boolean(user?.bloqueioTemporario),
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
  await ensureColumn("usuarios", "conta_bloqueada_em", "TIMESTAMP");
  await ensureColumn("usuarios", "motivo_bloqueio", "TEXT DEFAULT ''");
  await ensureColumn("usuarios", "bloqueio_temporario", "BOOLEAN DEFAULT FALSE");
  await ensureColumn("usuarios", "bonus_boas_vindas", "NUMERIC DEFAULT 0");
  await ensureColumn("usuarios", "bonus_boas_vindas_concedido_em", "TIMESTAMP");
  await ensureColumn("usuarios", "indicado_por_user_id", "TEXT DEFAULT ''");
  await ensureColumn("usuarios", "indicado_por_email", "TEXT DEFAULT ''");
  await ensureColumn("usuarios", "indicacao_vinculada_em", "TIMESTAMP");
  await ensureColumn("usuarios", "indicacao_qualificada_em", "TIMESTAMP");
  await ensureColumn("usuarios", "indicacao_bonus_creditado_em", "TIMESTAMP");
  await ensureColumn("usuarios", "indicacao_bonus_creditado_valor", "NUMERIC DEFAULT 0");
  await ensureColumn("usuarios", "indicacao_bonus_transacao_id", "TEXT DEFAULT ''");
  await ensureColumn("usuarios", "referral_code", "TEXT DEFAULT ''");
  await ensureColumn("usuarios", "pin_transacao_hash", "TEXT DEFAULT ''");
  await ensureColumn("usuarios", "pin_transacao_atualizado_em", "TIMESTAMP");
  await ensureIndex(
    "idx_usuarios_indicado_por_user_id",
    "CREATE INDEX idx_usuarios_indicado_por_user_id ON usuarios(indicado_por_user_id)"
  );
  await ensureIndex(
    "idx_usuarios_referral_code_unique",
    "CREATE UNIQUE INDEX idx_usuarios_referral_code_unique ON usuarios(referral_code) WHERE referral_code <> ''"
  );

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
    CREATE TABLE IF NOT EXISTS banner_settings (
      id TEXT PRIMARY KEY,
      rotation_ms INTEGER NOT NULL DEFAULT ${BANNER_ROTATION_DEFAULT_MS},
      updated_at TIMESTAMP
    );
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS banner_assets (
      id TEXT PRIMARY KEY,
      mime_type TEXT NOT NULL,
      image_data BYTEA NOT NULL,
      alt_text TEXT DEFAULT '',
      click_url TEXT DEFAULT '',
      is_active BOOLEAN NOT NULL DEFAULT true,
      duration_ms INTEGER NOT NULL DEFAULT ${BANNER_DURATION_DEFAULT_MS},
      sort_order INTEGER NOT NULL DEFAULT 0,
      created_at TIMESTAMP,
      updated_at TIMESTAMP
    );
  `);

  await ensureColumn(
    "banner_assets",
    "duration_ms",
    `INTEGER NOT NULL DEFAULT ${BANNER_DURATION_DEFAULT_MS}`
  );

  await pool.query(
    `
    INSERT INTO banner_settings (id, rotation_ms, updated_at)
    VALUES ($1, $2, $3)
    ON CONFLICT (id) DO NOTHING
    `,
    [BANNER_SETTINGS_ID, BANNER_ROTATION_DEFAULT_MS, db()]
  );

  await pool.query(`
    CREATE TABLE IF NOT EXISTS app_runtime_settings (
      id TEXT PRIMARY KEY,
      maintenance_enabled BOOLEAN NOT NULL DEFAULT false,
      maintenance_message TEXT NOT NULL DEFAULT '${APP_MAINTENANCE_MESSAGE_DEFAULT}',
      maintenance_eta_minutes INTEGER NOT NULL DEFAULT ${APP_MAINTENANCE_ETA_DEFAULT_MINUTES},
      updated_at TIMESTAMP
    );
  `);

  await pool.query(
    `
    INSERT INTO app_runtime_settings (
      id,
      maintenance_enabled,
      maintenance_message,
      maintenance_eta_minutes,
      updated_at
    )
    VALUES ($1, $2, $3, $4, $5)
    ON CONFLICT (id) DO NOTHING
    `,
    [
      APP_RUNTIME_SETTINGS_ID,
      false,
      APP_MAINTENANCE_MESSAGE_DEFAULT,
      APP_MAINTENANCE_ETA_DEFAULT_MINUTES,
      db()
    ]
  );

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
    CREATE TABLE IF NOT EXISTS user_notifications (
      id TEXT PRIMARY KEY,
      user_id TEXT NOT NULL,
      type TEXT NOT NULL DEFAULT '',
      title TEXT NOT NULL DEFAULT '',
      body TEXT NOT NULL DEFAULT '',
      metadata JSONB DEFAULT '{}'::jsonb,
      read_at TIMESTAMP,
      created_at TIMESTAMP
    );
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS movement_limit_requests (
      id TEXT PRIMARY KEY,
      user_id TEXT NOT NULL,
      requested_amount NUMERIC NOT NULL DEFAULT 0,
      status TEXT NOT NULL DEFAULT '${MOVEMENT_LIMIT_REQUEST_STATUS_PENDING}',
      admin_message TEXT DEFAULT '',
      pix_key TEXT DEFAULT '',
      notification_id TEXT DEFAULT '',
      created_at TIMESTAMP,
      updated_at TIMESTAMP,
      responded_at TIMESTAMP,
      closed_at TIMESTAMP
    );
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS investment_reserves (
      id TEXT PRIMARY KEY,
      user_id TEXT NOT NULL,
      product_key TEXT NOT NULL,
      product_name TEXT NOT NULL DEFAULT '',
      product_headline TEXT NOT NULL DEFAULT '',
      cdi_multiplier NUMERIC NOT NULL DEFAULT 0,
      annual_rate NUMERIC NOT NULL DEFAULT 0,
      principal_invested_total NUMERIC NOT NULL DEFAULT 0,
      principal_remaining NUMERIC NOT NULL DEFAULT 0,
      profit_paid_total NUMERIC NOT NULL DEFAULT 0,
      capacity_limit NUMERIC NOT NULL DEFAULT 0,
      min_amount NUMERIC NOT NULL DEFAULT 0,
      release_at TIMESTAMP,
      profit_eligible_at TIMESTAMP,
      lock_months INTEGER NOT NULL DEFAULT 0,
      min_hold_days_for_profit INTEGER NOT NULL DEFAULT 0,
      movement_required_per_month NUMERIC NOT NULL DEFAULT 0,
      allow_partial_withdraw BOOLEAN NOT NULL DEFAULT false,
      status TEXT NOT NULL DEFAULT '${INVESTMENT_RESERVE_STATUS_ACTIVE}',
      created_at TIMESTAMP,
      updated_at TIMESTAMP,
      last_withdrawn_at TIMESTAMP,
      closed_at TIMESTAMP
    );
  `);

  await ensureIndex(
    "idx_user_notifications_user_id",
    "CREATE INDEX idx_user_notifications_user_id ON user_notifications(user_id)"
  );
  await ensureIndex(
    "idx_movement_limit_requests_user_id",
    "CREATE INDEX idx_movement_limit_requests_user_id ON movement_limit_requests(user_id)"
  );
  await ensureIndex(
    "idx_investment_reserves_user_id",
    "CREATE INDEX idx_investment_reserves_user_id ON investment_reserves(user_id)"
  );

  await pool.query(`
    CREATE TABLE IF NOT EXISTS shop_categories (
      id TEXT PRIMARY KEY,
      source_key TEXT UNIQUE NOT NULL,
      source TEXT NOT NULL DEFAULT '${SHOP_PRODUCT_SOURCE_DEFAULT}',
      slug TEXT UNIQUE NOT NULL,
      name TEXT NOT NULL DEFAULT '',
      description TEXT DEFAULT '',
      image_url TEXT DEFAULT '',
      is_active BOOLEAN NOT NULL DEFAULT true,
      sort_order INTEGER NOT NULL DEFAULT 0,
      created_at TIMESTAMP,
      updated_at TIMESTAMP
    );
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS shop_products (
      id TEXT PRIMARY KEY,
      category_id TEXT NOT NULL,
      source_key TEXT UNIQUE NOT NULL,
      source TEXT NOT NULL DEFAULT '${SHOP_PRODUCT_SOURCE_DEFAULT}',
      external_id TEXT DEFAULT '',
      external_url TEXT DEFAULT '',
      slug TEXT UNIQUE NOT NULL,
      name TEXT NOT NULL DEFAULT '',
      short_description TEXT DEFAULT '',
      description TEXT DEFAULT '',
      image_url TEXT DEFAULT '',
      supplier_price NUMERIC NOT NULL DEFAULT 0,
      markup_percent NUMERIC NOT NULL DEFAULT ${SHOP_DEFAULT_MARKUP_PERCENTUAL},
      price NUMERIC NOT NULL DEFAULT 0,
      currency TEXT NOT NULL DEFAULT 'BRL',
      is_active BOOLEAN NOT NULL DEFAULT true,
      raw_payload JSONB DEFAULT '{}'::jsonb,
      created_at TIMESTAMP,
      updated_at TIMESTAMP
    );
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS shop_orders (
      id TEXT PRIMARY KEY,
      user_id TEXT NOT NULL,
      status TEXT NOT NULL DEFAULT '${SHOP_ORDER_STATUS_PENDING}',
      subtotal_amount NUMERIC NOT NULL DEFAULT 0,
      total_amount NUMERIC NOT NULL DEFAULT 0,
      bonus_debitado NUMERIC NOT NULL DEFAULT 0,
      real_debitado NUMERIC NOT NULL DEFAULT 0,
      shipping_name TEXT DEFAULT '',
      shipping_phone TEXT DEFAULT '',
      shipping_zip TEXT DEFAULT '',
      shipping_street TEXT DEFAULT '',
      shipping_number TEXT DEFAULT '',
      shipping_complement TEXT DEFAULT '',
      shipping_neighborhood TEXT DEFAULT '',
      shipping_city TEXT DEFAULT '',
      shipping_state TEXT DEFAULT '',
      shipping_reference TEXT DEFAULT '',
      customer_note TEXT DEFAULT '',
      refusal_reason TEXT DEFAULT '',
      financial_transaction_id_debito TEXT DEFAULT '',
      financial_transaction_id_estorno TEXT DEFAULT '',
      admin_id TEXT DEFAULT '',
      created_at TIMESTAMP,
      updated_at TIMESTAMP,
      approved_at TIMESTAMP,
      refused_at TIMESTAMP,
      refunded_at TIMESTAMP
    );
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS shop_order_items (
      id TEXT PRIMARY KEY,
      order_id TEXT NOT NULL,
      product_id TEXT NOT NULL,
      category_id TEXT NOT NULL,
      source_key TEXT DEFAULT '',
      external_url TEXT DEFAULT '',
      product_name TEXT NOT NULL DEFAULT '',
      product_slug TEXT DEFAULT '',
      image_url TEXT DEFAULT '',
      supplier_price NUMERIC NOT NULL DEFAULT 0,
      unit_price NUMERIC NOT NULL DEFAULT 0,
      quantity INTEGER NOT NULL DEFAULT 1,
      total_price NUMERIC NOT NULL DEFAULT 0,
      metadata JSONB DEFAULT '{}'::jsonb,
      created_at TIMESTAMP,
      updated_at TIMESTAMP
    );
  `);

  await ensureIndex(
    "idx_shop_categories_sort_order",
    "CREATE INDEX idx_shop_categories_sort_order ON shop_categories(sort_order)"
  );
  await ensureIndex(
    "idx_shop_products_category_id",
    "CREATE INDEX idx_shop_products_category_id ON shop_products(category_id)"
  );
  await ensureIndex(
    "idx_shop_products_is_active",
    "CREATE INDEX idx_shop_products_is_active ON shop_products(is_active)"
  );
  await ensureIndex(
    "idx_shop_orders_user_id",
    "CREATE INDEX idx_shop_orders_user_id ON shop_orders(user_id)"
  );
  await ensureIndex(
    "idx_shop_orders_status",
    "CREATE INDEX idx_shop_orders_status ON shop_orders(status)"
  );
  await ensureIndex(
    "idx_shop_order_items_order_id",
    "CREATE INDEX idx_shop_order_items_order_id ON shop_order_items(order_id)"
  );

  await pool.query(`
    CREATE TABLE IF NOT EXISTS topup_orders (
      id TEXT PRIMARY KEY,
      user_id TEXT NOT NULL,
      operadora TEXT NOT NULL,
      ddd TEXT NOT NULL DEFAULT '',
      numero TEXT NOT NULL DEFAULT '',
      telefone TEXT NOT NULL DEFAULT '',
      valor_recarga NUMERIC NOT NULL DEFAULT 0,
      taxa_valor NUMERIC NOT NULL DEFAULT 0,
      valor_total_debitado NUMERIC NOT NULL DEFAULT 0,
      bonus_debitado NUMERIC NOT NULL DEFAULT 0,
      real_debitado NUMERIC NOT NULL DEFAULT 0,
      status TEXT NOT NULL DEFAULT 'pendente',
      motivo_recusa TEXT DEFAULT '',
      financial_transaction_id_debito TEXT DEFAULT '',
      financial_transaction_id_estorno TEXT DEFAULT '',
      admin_id TEXT DEFAULT '',
      criado_em TIMESTAMP,
      atualizado_em TIMESTAMP,
      aprovado_em TIMESTAMP,
      recusado_em TIMESTAMP,
      estornado_em TIMESTAMP
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
    "idx_topup_orders_user_id",
    "CREATE INDEX idx_topup_orders_user_id ON topup_orders (user_id)"
  );

  await ensureIndex(
    "idx_topup_orders_status",
    "CREATE INDEX idx_topup_orders_status ON topup_orders (status)"
  );

  await ensureIndex(
    "idx_banner_assets_sort_order",
    "CREATE INDEX idx_banner_assets_sort_order ON banner_assets (sort_order)"
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

app.use(
  helmet({
    crossOriginResourcePolicy: false
  })
);
app.use(express.json());

function shouldSkipGlobalRateLimit(req) {
  const path = String(req?.path || "").trim();
  return path === "/public/shop/catalog";
}

app.use(
  rateLimit({
    windowMs: GLOBAL_API_RATE_LIMIT_WINDOW_MS,
    max: GLOBAL_API_RATE_LIMIT_MAX,
    standardHeaders: true,
    legacyHeaders: false,
    skip: shouldSkipGlobalRateLimit
  })
);

const publicShopCatalogLimiter = rateLimit({
  windowMs: PUBLIC_SHOP_CATALOG_RATE_LIMIT_WINDOW_MS,
  max: PUBLIC_SHOP_CATALOG_RATE_LIMIT_MAX,
  standardHeaders: true,
  legacyHeaders: false,
  message: { error: "Muitas consultas na vitrine. Aguarde alguns segundos." }
});

const shopOrderLimiter = rateLimit({
  windowMs: SHOP_ORDER_RATE_LIMIT_WINDOW_MS,
  max: SHOP_ORDER_RATE_LIMIT_MAX,
  standardHeaders: true,
  legacyHeaders: false,
  keyGenerator: (req) => String(req?.userAuth?.sub || "shop-order"),
  message: { error: "Muitas tentativas de concluir compra. Aguarde 1 minuto." }
});

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

const bannerUpload = multer({
  storage: multer.memoryStorage(),
  limits: { fileSize: BANNER_IMAGE_MAX_BYTES, files: 12 },
  fileFilter: (_, file, cb) => {
    const allowed = ["image/png", "image/jpeg", "image/jpg", "image/webp"];

    if (!allowed.includes(String(file.mimetype || "").toLowerCase())) {
      return cb(new Error("Formato de banner invalido"));
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

function clampBannerRotationMs(value) {
  const parsed = Number(value);

  if (!Number.isFinite(parsed)) {
    return BANNER_ROTATION_DEFAULT_MS;
  }

  return Math.min(
    BANNER_ROTATION_MAX_MS,
    Math.max(BANNER_ROTATION_MIN_MS, Math.round(parsed))
  );
}

function clampBannerDurationMs(value) {
  return clampBannerRotationMs(value);
}

function clampMaintenanceEtaMinutes(value) {
  const parsed = Number(value);

  if (!Number.isFinite(parsed)) {
    return APP_MAINTENANCE_ETA_DEFAULT_MINUTES;
  }

  return Math.min(
    APP_MAINTENANCE_ETA_MAX_MINUTES,
    Math.max(APP_MAINTENANCE_ETA_MIN_MINUTES, Math.round(parsed))
  );
}

function normalizeMaintenanceMessage(value) {
  return (
    String(value || "").trim().slice(0, 220) || APP_MAINTENANCE_MESSAGE_DEFAULT
  );
}

function normalizeBannerAlt(value) {
  return String(value || "").trim().slice(0, 180) || "Banner Sigmo";
}

function normalizeBannerHref(value) {
  const href = String(value || "").trim();

  if (!href) return "";
  if (/^https?:\/\//i.test(href)) return href;
  if (href.startsWith("/")) return href;

  return "";
}

function buildBannerImageUrl(id) {
  return `/public/banner-images/${encodeURIComponent(String(id || "").trim())}`;
}

function mapBannerAsset(row) {
  if (!row) return null;

  return {
    id: row.id,
    imageUrl: buildBannerImageUrl(row.id),
    alt: normalizeBannerAlt(row.alt_text || ""),
    href: normalizeBannerHref(row.click_url || ""),
    active: row.is_active !== false,
    durationMs: clampBannerDurationMs(row.duration_ms),
    sortOrder: Number(row.sort_order || 0),
    createdAt: row.created_at || null,
    updatedAt: row.updated_at || null
  };
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
    statusConta: normalizeStatusConta(row.status_conta),
    contaBanidaEm: row.conta_banida_em || null,
    motivoBanimento: row.motivo_banimento || "",
    contaBloqueadaEm: row.conta_bloqueada_em || null,
    motivoBloqueio: row.motivo_bloqueio || "",
    bloqueioTemporario: Boolean(row.bloqueio_temporario),
    bonusBoasVindas: toMoney(row.bonus_boas_vindas),
    bonusBoasVindasConcedidoEm: row.bonus_boas_vindas_concedido_em || null,
    indicadoPorUserId: row.indicado_por_user_id || "",
    indicadoPorEmail: row.indicado_por_email || "",
    indicacaoVinculadaEm: row.indicacao_vinculada_em || null,
    indicacaoQualificadaEm: row.indicacao_qualificada_em || null,
    indicacaoBonusCreditadoEm: row.indicacao_bonus_creditado_em || null,
    indicacaoBonusCreditadoValor: toMoney(row.indicacao_bonus_creditado_valor),
    indicacaoBonusTransacaoId: row.indicacao_bonus_transacao_id || "",
    referralCode: normalizeReferralCode(row.referral_code || ""),
    pinTransacaoHash: row.pin_transacao_hash || "",
    pinTransacaoAtualizadoEm: row.pin_transacao_atualizado_em || null
  };
}

function getIndicacaoStatus(user, qualifyingPixTotal = 0) {
  if (!user?.indicadoPorUserId) return "sem_indicacao";
  if (
    user.indicacaoQualificadaEm ||
    user.indicacaoBonusCreditadoEm ||
    toMoney(qualifyingPixTotal) >= INDICACAO_PIX_QUALIFICACAO_MIN
  ) {
    return "concluido";
  }
  return "pendente";
}

function buildIndicacaoParticipacao(user, qualifyingPixTotal = 0, referrer = null) {
  if (!user?.indicadoPorUserId) {
    return {
      possuiIndicador: false,
      status: "sem_indicacao",
      qualifyingPixTotal: toMoney(qualifyingPixTotal),
      valorNecessario: INDICACAO_PIX_QUALIFICACAO_MIN,
      bonusValor: BONUS_INDICACAO_VALOR,
      indicadoPor: null
    };
  }

  return {
    possuiIndicador: true,
    status: getIndicacaoStatus(user, qualifyingPixTotal),
    qualifyingPixTotal: toMoney(qualifyingPixTotal),
    valorNecessario: INDICACAO_PIX_QUALIFICACAO_MIN,
    bonusValor: BONUS_INDICACAO_VALOR,
    vinculadaEm: user.indicacaoVinculadaEm || null,
    qualificadaEm: user.indicacaoQualificadaEm || null,
    bonusCreditadoEm: user.indicacaoBonusCreditadoEm || null,
    bonusCreditadoValor: toMoney(user.indicacaoBonusCreditadoValor),
    indicadoPor: {
      userId: user.indicadoPorUserId || "",
      email: user.indicadoPorEmail || "",
      nome: referrer?.nome || referrer?.email?.split("@")[0] || ""
    }
  };
}

function buildUserPublicResponse(user, extras = {}) {
  return {
    id: user.id,
    nome: user.nome,
    email: user.email,
    saldo: toMoney(user.saldo),
    criadoEm: user.criadoEm || null,
    statusConta: normalizeStatusConta(user.statusConta),
    contaBanida: isContaBanida(user),
    contaBloqueada: isContaBloqueada(user),
    contaBanidaPermanente: isContaPermanentementeBanida(user),
    contaBanidaEm: user.contaBanidaEm || null,
    motivoBanimento: user.motivoBanimento || "",
    contaBloqueadaEm: user.contaBloqueadaEm || null,
    motivoBloqueio: user.motivoBloqueio || "",
    bloqueioTemporario: Boolean(user.bloqueioTemporario),
    bonusBoasVindas: toMoney(user.bonusBoasVindas),
    bonusBoasVindasConcedidoEm: user.bonusBoasVindasConcedidoEm || null,
    indicadoPorUserId: user.indicadoPorUserId || "",
    indicadoPorEmail: user.indicadoPorEmail || "",
    indicacaoVinculadaEm: user.indicacaoVinculadaEm || null,
    indicacaoQualificadaEm: user.indicacaoQualificadaEm || null,
    indicacaoBonusCreditadoEm: user.indicacaoBonusCreditadoEm || null,
    indicacaoBonusCreditadoValor: toMoney(user.indicacaoBonusCreditadoValor),
    referralCode: normalizeReferralCode(user.referralCode),
    referralLink: buildReferralLinkFromCode(user.referralCode),
    ...extras
  };
}

async function buildUserPublicResponseWithPix(user, client = pool, extras = {}) {
  const ensuredUser = await ensureUserReferralCode(user, client);
  const valorRecebidoViaPix = await getValorRecebidoViaPix(user.id, client);
  const referrer = ensuredUser?.indicadoPorUserId
    ? await getUserById(ensuredUser.indicadoPorUserId, client)
    : null;
  const indicacao = buildIndicacaoParticipacao(
    ensuredUser,
    valorRecebidoViaPix,
    referrer
  );
  const deviceId = String(extras.deviceId || "").trim();
  const activeCard =
    Object.prototype.hasOwnProperty.call(extras, "activeCard")
      ? extras.activeCard
      : await buildUserActiveCardResponse(ensuredUser, deviceId, client);
  const extraPayload = { ...extras };
  delete extraPayload.deviceId;
  delete extraPayload.activeCard;

  return buildUserPublicResponse(ensuredUser, {
    pixDesbloqueado: valorRecebidoViaPix >= PIX_SAQUE_DESBLOQUEIO_MIN,
    valorRecebidoViaPix,
    valorMinimoDesbloqueioPix: PIX_SAQUE_DESBLOQUEIO_MIN,
    indicacao,
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

function attachUserAuthToPayload(payload, token) {
  return {
    ...payload,
    authToken: token,
    authTokenType: "Bearer",
    authTokenTtl: USER_MOBILE_TOKEN_TTL
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

function mapRecargaCelularPedido(row) {
  if (!row) return null;

  const operadora = normalizeRecargaCelularOperadora(row.operadora);
  const ddd = normalizeRecargaCelularDdd(row.ddd);
  const numero = normalizeRecargaCelularNumero(row.numero);

  return {
    id: row.id,
    userId: row.user_id,
    operadora,
    operadoraLabel: getRecargaCelularOperadoraLabel(operadora),
    ddd,
    numero,
    telefone: buildRecargaCelularTelefone(ddd, numero),
    telefoneFormatado: formatRecargaCelularTelefone(ddd, numero),
    valorRecarga: toMoney(row.valor_recarga),
    taxaValor: toMoney(row.taxa_valor),
    valorTotalDebitado: toMoney(row.valor_total_debitado),
    bonusDebitado: toMoney(row.bonus_debitado),
    realDebitado: toMoney(row.real_debitado),
    status: normalizeRecargaCelularStatus(row.status),
    motivoRecusa: row.motivo_recusa || "",
    financialTransactionIdDebito: row.financial_transaction_id_debito || "",
    financialTransactionIdEstorno: row.financial_transaction_id_estorno || "",
    adminId: row.admin_id || "",
    criadoEm: row.criado_em || null,
    atualizadoEm: row.atualizado_em || null,
    aprovadoEm: row.aprovado_em || null,
    recusadoEm: row.recusado_em || null,
    estornadoEm: row.estornado_em || null,
    descricao: montarDescricaoRecargaCelular({
      operadora,
      ddd,
      numero,
      valorRecarga: row.valor_recarga,
      taxaValor: row.taxa_valor,
      valorTotalDebitado: row.valor_total_debitado
    }),
    user:
      row.user_email || row.user_nome
        ? {
            id: row.user_id,
            nome: row.user_nome || row.user_email?.split("@")[0] || "",
            email: row.user_email || ""
          }
        : null
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

function mapUserNotification(row) {
  if (!row) return null;

  return {
    id: row.id,
    userId: row.user_id,
    type: row.type || "",
    title: row.title || "",
    body: row.body || "",
    metadata: row.metadata || {},
    readAt: row.read_at || null,
    createdAt: row.created_at || null
  };
}

function mapMovementLimitRequest(row) {
  if (!row) return null;

  return {
    id: row.id,
    userId: row.user_id,
    requestedAmount: toMoney(row.requested_amount),
    status: row.status || MOVEMENT_LIMIT_REQUEST_STATUS_PENDING,
    adminMessage: row.admin_message || "",
    pixKey: row.pix_key || "",
    notificationId: row.notification_id || "",
    createdAt: row.created_at || null,
    updatedAt: row.updated_at || null,
    respondedAt: row.responded_at || null,
    closedAt: row.closed_at || null
  };
}

function mapInvestmentReserve(row) {
  if (!row) return null;

  return {
    id: row.id,
    userId: row.user_id,
    productKey: String(row.product_key || "").trim().toLowerCase(),
    productName: row.product_name || "",
    productHeadline: row.product_headline || "",
    cdiMultiplier: Number(row.cdi_multiplier || 0),
    annualRate: Number(row.annual_rate || 0),
    principalInvestedTotal: toMoney(row.principal_invested_total),
    principalRemaining: toMoney(row.principal_remaining),
    profitPaidTotal: toMoney(row.profit_paid_total),
    capacityLimit: toMoney(row.capacity_limit),
    minAmount: toMoney(row.min_amount),
    releaseAt: row.release_at || null,
    profitEligibleAt: row.profit_eligible_at || null,
    lockMonths: Number(row.lock_months || 0),
    minHoldDaysForProfit: Number(row.min_hold_days_for_profit || 0),
    movementRequiredPerMonth: toMoney(row.movement_required_per_month),
    allowPartialWithdraw: Boolean(row.allow_partial_withdraw),
    status: row.status || INVESTMENT_RESERVE_STATUS_ACTIVE,
    createdAt: row.created_at || null,
    updatedAt: row.updated_at || null,
    lastWithdrawnAt: row.last_withdrawn_at || null,
    closedAt: row.closed_at || null
  };
}

function mapShopCategory(row) {
  if (!row) return null;

  return {
    id: row.id,
    sourceKey: row.source_key || "",
    source: row.source || SHOP_PRODUCT_SOURCE_DEFAULT,
    slug: row.slug || "",
    name: row.name || "",
    description: row.description || "",
    imageUrl: row.image_url || "",
    active: row.is_active !== false,
    sortOrder: Number(row.sort_order || 0),
    createdAt: row.created_at || null,
    updatedAt: row.updated_at || null
  };
}

function mapShopProduct(row) {
  if (!row) return null;

  const category =
    row.category_slug || row.category_name
      ? {
          id: row.category_id,
          sourceKey: row.category_source_key || "",
          source: row.category_source || SHOP_PRODUCT_SOURCE_DEFAULT,
          slug: row.category_slug || "",
          name: row.category_name || "",
          description: row.category_description || "",
          imageUrl: row.category_image_url || "",
          active: row.category_is_active !== false,
          sortOrder: Number(row.category_sort_order || 0)
        }
      : null;

  return {
    id: row.id,
    categoryId: row.category_id,
    sourceKey: row.source_key || "",
    source: row.source || SHOP_PRODUCT_SOURCE_DEFAULT,
    externalId: row.external_id || "",
    externalUrl: row.external_url || "",
    slug: row.slug || "",
    name: row.name || "",
    shortDescription: row.short_description || "",
    description: row.description || "",
    imageUrl: row.image_url || "",
    supplierPrice: toMoney(row.supplier_price),
    markupPercent: toMoney(row.markup_percent),
    price: toMoney(row.price),
    currency: row.currency || "BRL",
    active: row.is_active !== false,
    rawPayload: row.raw_payload || {},
    createdAt: row.created_at || null,
    updatedAt: row.updated_at || null,
    category
  };
}

function buildPublicShopCategoryResponse(category, extras = {}) {
  if (!category) return null;

  const name = normalizeShopCatalogCategoryName(category.name, 120);
  const slug = slugifyShopValue(name || category.slug, "categoria");

  return {
    id: category.id,
    slug,
    name,
    description: normalizeShopCatalogText(category.description, 1000),
    imageUrl: normalizeShopUrl(category.imageUrl),
    productCount: Math.max(0, Number(extras.productCount || 0))
  };
}

function buildPublicShopProductResponse(product) {
  if (!product) return null;

  const categoryName = product.category
    ? normalizeShopCatalogCategoryName(product.category.name, 120)
    : "";
  const categorySlug = categoryName
    ? slugifyShopValue(categoryName, "categoria")
    : "";

  return {
    id: product.id,
    categoryId: product.categoryId,
    slug: product.slug || "",
    name: normalizeShopCatalogText(product.name, 180),
    shortDescription: normalizeShopCatalogText(product.shortDescription, 320),
    description: normalizeShopCatalogText(product.description, 2400),
    imageUrl: normalizeShopUrl(product.imageUrl),
    price: toMoney(product.price),
    currency: product.currency || "BRL",
    category: product.category
      ? {
          id: product.category.id,
          slug: categorySlug,
          name: categoryName
        }
      : null
  };
}

async function loadShopPublicCatalogSnapshotFromDb(client = pool) {
  const [categories, products] = await Promise.all([
    listShopCategories({ activeOnly: true }, client),
    listShopProducts({ activeOnly: true }, client)
  ]);

  return {
    generatedAt: db(),
    categories,
    publicCategories: categories.map((category) => buildPublicShopCategoryResponse(category)),
    indexedProducts: products.map((product) => {
      const publicProduct = buildPublicShopProductResponse(product);

      return {
        categoryId: product.categoryId,
        categorySlug: String(publicProduct?.category?.slug || "").trim().toLowerCase(),
        searchText: [
          publicProduct?.name,
          publicProduct?.shortDescription,
          publicProduct?.description,
          publicProduct?.category?.name
        ]
          .join(" ")
          .toLowerCase(),
        publicProduct
      };
    })
  };
}

async function refreshShopPublicCatalogSnapshotCache(force = false) {
  const now = Date.now();

  if (!force && shopPublicCatalogCache.refreshPromise) {
    return shopPublicCatalogCache.refreshPromise;
  }

  if (!force && shopPublicCatalogCache.snapshot && shopPublicCatalogCache.expiresAtMs > now) {
    return shopPublicCatalogCache.snapshot;
  }

  shopPublicCatalogCache.refreshPromise = (async () => {
    const snapshot = await loadShopPublicCatalogSnapshotFromDb();
    const refreshedAtMs = Date.now();

    shopPublicCatalogCache.snapshot = snapshot;
    shopPublicCatalogCache.expiresAtMs = refreshedAtMs + SHOP_PUBLIC_CATALOG_CACHE_TTL_MS;
    shopPublicCatalogCache.staleUntilMs =
      refreshedAtMs + SHOP_PUBLIC_CATALOG_STALE_WHILE_REVALIDATE_MS;
    shopPublicCatalogCache.version += 1;

    return snapshot;
  })().finally(() => {
    shopPublicCatalogCache.refreshPromise = null;
  });

  return shopPublicCatalogCache.refreshPromise;
}

async function getShopPublicCatalogSnapshotCached() {
  const now = Date.now();

  if (!shopPublicCatalogCache.snapshot) {
    return {
      cacheStatus: "miss",
      snapshot: await refreshShopPublicCatalogSnapshotCache(true)
    };
  }

  if (shopPublicCatalogCache.expiresAtMs > now) {
    return {
      cacheStatus: "hit",
      snapshot: shopPublicCatalogCache.snapshot
    };
  }

  if (shopPublicCatalogCache.staleUntilMs > now) {
    refreshShopPublicCatalogSnapshotCache().catch((error) => {
      console.error("[shop-cache] erro ao atualizar snapshot publico", error);
    });

    return {
      cacheStatus: "stale",
      snapshot: shopPublicCatalogCache.snapshot
    };
  }

  return {
    cacheStatus: "miss",
    snapshot: await refreshShopPublicCatalogSnapshotCache(true)
  };
}

function invalidateShopPublicCatalogSnapshotCache(options = {}) {
  shopPublicCatalogCache.snapshot = null;
  shopPublicCatalogCache.expiresAtMs = 0;
  shopPublicCatalogCache.staleUntilMs = 0;
  shopPublicCatalogCache.version += 1;

  if (options.prewarm === false) {
    return;
  }

  setImmediate(() => {
    refreshShopPublicCatalogSnapshotCache(true).catch((error) => {
      console.error("[shop-cache] erro ao preaquecer snapshot publico", error);
    });
  });
}

function hasShopSnapshotProducts(snapshot) {
  return Array.isArray(snapshot?.indexedProducts) && snapshot.indexedProducts.length > 0;
}

function hasKairossSeedFile() {
  return fs.existsSync(SHOP_CATALOG_SEED_FILE);
}

function readKairossSeedFile() {
  if (!hasKairossSeedFile()) {
    return null;
  }

  try {
    const raw = fs.readFileSync(SHOP_CATALOG_SEED_FILE, "utf8").replace(/^\uFEFF/, "");
    const parsed = JSON.parse(raw);
    const products = Array.isArray(parsed)
      ? parsed
      : Array.isArray(parsed?.products)
        ? parsed.products
        : Array.isArray(parsed?.products?.value)
          ? parsed.products.value
          : Array.isArray(parsed?.value)
            ? parsed.value
        : [];

    if (!products.length) {
      return null;
    }

    return {
      exportedAt: parsed?.exportedAt || null,
      baseUrl: normalizeHttpBaseUrl(
        parsed?.baseUrl || KAIROSS_BASE_URL,
        "https://app.kaiross.com.br"
      ),
      products
    };
  } catch (error) {
    console.error("[shop-recovery] erro ao ler seed local da Kaiross", error);
    return null;
  }
}

async function importShopCatalogFromKairossProducts(products, options = {}) {
  const payload = buildKairossShopImportPayload(products, {
    source: SHOP_PRODUCT_SOURCE_DEFAULT,
    markupPercent:
      options.markupPercent === undefined
        ? SHOP_DEFAULT_MARKUP_PERCENTUAL
        : options.markupPercent,
    baseUrl: options.baseUrl || KAIROSS_BASE_URL
  });

  payload.deactivateMissing = options.deactivateMissing === true;
  return importShopCatalog(payload, options.client || pool);
}

async function recoverShopCatalogIfEmpty(reason = "unknown") {
  const now = Date.now();

  if (
    shopCatalogRecoveryState.lastAttemptMs > 0 &&
    now - shopCatalogRecoveryState.lastAttemptMs < SHOP_EMPTY_CATALOG_RECOVERY_MIN_INTERVAL_MS &&
    !shopCatalogRecoveryState.activePromise
  ) {
    return null;
  }

  if (shopCatalogRecoveryState.activePromise) {
    return shopCatalogRecoveryState.activePromise;
  }

  shopCatalogRecoveryState.lastAttemptMs = now;
  shopCatalogRecoveryState.activePromise = (async () => {
    const activeProducts = await listShopProducts({ activeOnly: true });

    if (activeProducts.length > 0) {
      return null;
    }

    let summary = null;
    let source = "";

    if (KAIROSS_EMAIL && KAIROSS_PASSWORD) {
      try {
        const provider = await fetchKairossProducts();
        summary = await runInTransaction(async (client) =>
          importShopCatalogFromKairossProducts(provider.products, {
            client,
            baseUrl: provider.baseUrl,
            deactivateMissing: true
          })
        );
        source = "kaiross";
      } catch (error) {
        console.error(`[shop-recovery] falha ao sincronizar Kaiross (${reason})`, error);
      }
    }

    if (!summary) {
      const seed = readKairossSeedFile();

      if (seed?.products?.length) {
        summary = await runInTransaction(async (client) =>
          importShopCatalogFromKairossProducts(seed.products, {
            client,
            baseUrl: seed.baseUrl,
            deactivateMissing: false
          })
        );
        source = "seed";
      }
    }

    if (!summary) {
      return null;
    }

    invalidateShopPublicCatalogSnapshotCache({ prewarm: false });
    const snapshot = await refreshShopPublicCatalogSnapshotCache(true);

    shopCatalogRecoveryState.lastSuccessAtMs = Date.now();
    shopCatalogRecoveryState.lastSuccessSource = source;
    shopCatalogRecoveryState.lastFailureAtMs = 0;
    shopCatalogRecoveryState.lastFailureMessage = "";

    console.log(
      `[shop-recovery] catalogo recuperado via ${source} (${reason}) com ${summary.productsImported} produtos`
    );

    return {
      source,
      summary,
      snapshot
    };
  })()
    .catch((error) => {
      shopCatalogRecoveryState.lastFailureAtMs = Date.now();
      shopCatalogRecoveryState.lastFailureMessage = String(error?.message || error || "");
      console.error(`[shop-recovery] erro ao recuperar catalogo (${reason})`, error);
      return null;
    })
    .finally(() => {
      shopCatalogRecoveryState.activePromise = null;
    });

  return shopCatalogRecoveryState.activePromise;
}

function buildPublicShopCatalogPayload(snapshot, options = {}) {
  const categorySlug = String(options.categorySlug || "").trim().toLowerCase();
  const search = normalizeShopText(options.search || "", 180).toLowerCase();
  const includeGrouped = options.includeGrouped === true;
  let filteredEntries = snapshot.indexedProducts;

  if (categorySlug) {
    filteredEntries = filteredEntries.filter((entry) => entry.categorySlug === categorySlug);
  }

  if (search) {
    filteredEntries = filteredEntries.filter((entry) => entry.searchText.includes(search));
  }

  const countsByCategorySlug = new Map();
  const publicProducts = [];
  const productsByCategorySlug = includeGrouped ? new Map() : null;
  const categoriesBySlug = new Map();
  let categoryOrder = 0;

  function registerCategory(category) {
    const normalized = buildPublicShopCategoryResponse(category);
    const slug = String(normalized?.slug || "").trim().toLowerCase();

    if (!normalized || !slug) {
      return;
    }

    const current = categoriesBySlug.get(slug);
    if (!current) {
      categoriesBySlug.set(slug, {
        ...normalized,
        _order: categoryOrder++
      });
      return;
    }

    if (!current.description && normalized.description) {
      current.description = normalized.description;
    }
    if (!current.imageUrl && normalized.imageUrl) {
      current.imageUrl = normalized.imageUrl;
    }
  }

  for (const category of Array.isArray(snapshot.publicCategories) ? snapshot.publicCategories : []) {
    registerCategory(category);
  }

  for (const entry of filteredEntries) {
    const publicProduct = entry.publicProduct;
    const normalizedCategory = publicProduct?.category
      ? buildPublicShopCategoryResponse(publicProduct.category)
      : null;
    const normalizedSlug = String(normalizedCategory?.slug || entry.categorySlug || "")
      .trim()
      .toLowerCase();

    if (normalizedCategory) {
      registerCategory(normalizedCategory);
    }

    if (normalizedSlug) {
      countsByCategorySlug.set(
        normalizedSlug,
        (countsByCategorySlug.get(normalizedSlug) || 0) + 1
      );
    }

    publicProducts.push(publicProduct);

    if (productsByCategorySlug && normalizedSlug) {
      const list = productsByCategorySlug.get(normalizedSlug) || [];
      list.push(publicProduct);
      productsByCategorySlug.set(normalizedSlug, list);
    }
  }

  const publicCategories = Array.from(categoriesBySlug.values())
    .map((category) => ({
      ...category,
      productCount: countsByCategorySlug.get(category.slug) || 0
    }))
    .filter((category) => category.productCount > 0)
    .sort((a, b) => a._order - b._order || a.name.localeCompare(b.name, "pt-BR"))
    .map(({ _order, ...category }) => category);

  const payload = {
    generatedAt: snapshot.generatedAt,
    markupPercentDefault: normalizeShopMarkupPercent(SHOP_DEFAULT_MARKUP_PERCENTUAL),
    categories: publicCategories,
    products: publicProducts
  };

  if (productsByCategorySlug) {
    payload.grouped = publicCategories.map((category) => ({
      ...category,
      products: productsByCategorySlug.get(category.slug) || []
    }));
  }

  return payload;
}

function mapShopOrderItem(row) {
  if (!row) return null;

  return {
    id: row.id,
    orderId: row.order_id,
    productId: row.product_id,
    categoryId: row.category_id,
    sourceKey: row.source_key || "",
    externalUrl: row.external_url || "",
    productName: row.product_name || "",
    productSlug: row.product_slug || "",
    imageUrl: row.image_url || "",
    supplierPrice: toMoney(row.supplier_price),
    unitPrice: toMoney(row.unit_price),
    quantity: Number(row.quantity || 0),
    totalPrice: toMoney(row.total_price),
    metadata: row.metadata || {},
    createdAt: row.created_at || null,
    updatedAt: row.updated_at || null
  };
}

function mapShopOrder(row) {
  if (!row) return null;

  const shipping = {
    name: row.shipping_name || "",
    phone: row.shipping_phone || "",
    zip: row.shipping_zip || "",
    street: row.shipping_street || "",
    number: row.shipping_number || "",
    complement: row.shipping_complement || "",
    neighborhood: row.shipping_neighborhood || "",
    city: row.shipping_city || "",
    state: row.shipping_state || "",
    reference: row.shipping_reference || ""
  };

  return {
    id: row.id,
    userId: row.user_id,
    status: normalizeShopOrderStatus(row.status),
    subtotalAmount: toMoney(row.subtotal_amount),
    totalAmount: toMoney(row.total_amount),
    bonusDebitado: toMoney(row.bonus_debitado),
    realDebitado: toMoney(row.real_debitado),
    customerNote: row.customer_note || "",
    refusalReason: row.refusal_reason || "",
    financialTransactionIdDebito: row.financial_transaction_id_debito || "",
    financialTransactionIdEstorno: row.financial_transaction_id_estorno || "",
    adminId: row.admin_id || "",
    shipping,
    shippingAddress: buildShopFormattedAddress({
      shippingStreet: shipping.street,
      shippingNumber: shipping.number,
      shippingComplement: shipping.complement,
      shippingNeighborhood: shipping.neighborhood,
      shippingCity: shipping.city,
      shippingState: shipping.state,
      shippingZip: shipping.zip
    }),
    createdAt: row.created_at || null,
    updatedAt: row.updated_at || null,
    approvedAt: row.approved_at || null,
    refusedAt: row.refused_at || null,
    refundedAt: row.refunded_at || null,
    user:
      row.user_email || row.user_nome
        ? {
            id: row.user_id,
            nome: row.user_nome || row.user_email?.split("@")[0] || "",
            email: row.user_email || ""
          }
        : null
  };
}

function normalizeInvestmentProductKey(value) {
  const key = String(value || "").trim().toLowerCase();
  return Object.prototype.hasOwnProperty.call(INVESTMENT_PRODUCT_DEFINITIONS, key)
    ? key
    : "";
}

function getInvestmentProductConfig(productKey) {
  const key = normalizeInvestmentProductKey(productKey);
  return key ? INVESTMENT_PRODUCT_DEFINITIONS[key] : null;
}

function getInvestmentReferenceAnnualRate(config) {
  return Number(
    (INVESTIMENTOS_CDI_ANUAL_REFERENCIA * Number(config?.cdiMultiplier || 0)).toFixed(6)
  );
}

function addDaysToDate(dateValue, days = 0) {
  const date = new Date(dateValue || Date.now());
  if (Number.isNaN(date.getTime())) return null;
  date.setUTCDate(date.getUTCDate() + Number(days || 0));
  return date;
}

function addMonthsToDate(dateValue, months = 0) {
  const date = new Date(dateValue || Date.now());
  if (Number.isNaN(date.getTime())) return null;
  const day = date.getUTCDate();
  date.setUTCDate(1);
  date.setUTCMonth(date.getUTCMonth() + Number(months || 0));
  const lastDay = new Date(Date.UTC(date.getUTCFullYear(), date.getUTCMonth() + 1, 0)).getUTCDate();
  date.setUTCDate(Math.min(day, lastDay));
  return date;
}

function toIsoOrNull(dateValue) {
  if (!dateValue) return null;
  const date = dateValue instanceof Date ? dateValue : new Date(dateValue);
  if (Number.isNaN(date.getTime())) return null;
  return date.toISOString();
}

function diffDaysFloor(startValue, endValue = new Date()) {
  const start = new Date(startValue || 0);
  const end = new Date(endValue || Date.now());
  if (Number.isNaN(start.getTime()) || Number.isNaN(end.getTime())) return 0;
  const diffMs = end.getTime() - start.getTime();
  if (diffMs <= 0) return 0;
  return Math.floor(diffMs / (24 * 60 * 60 * 1000));
}

function getMovementRequestStatusLabel(status) {
  const normalized = String(status || "").trim().toLowerCase();
  if (normalized === MOVEMENT_LIMIT_REQUEST_STATUS_RESPONDED) return "responded";
  if (normalized === MOVEMENT_LIMIT_REQUEST_STATUS_CLOSED) return "closed";
  return MOVEMENT_LIMIT_REQUEST_STATUS_PENDING;
}

function getInvestmentReserveStatus(value) {
  const normalized = String(value || "").trim().toLowerCase();
  if (normalized === INVESTMENT_RESERVE_STATUS_PARTIAL) return INVESTMENT_RESERVE_STATUS_PARTIAL;
  if (normalized === INVESTMENT_RESERVE_STATUS_CLOSED) return INVESTMENT_RESERVE_STATUS_CLOSED;
  return INVESTMENT_RESERVE_STATUS_ACTIVE;
}

function getStartOfMonthIso(referenceDate = new Date()) {
  const date = new Date(referenceDate || Date.now());
  return new Date(Date.UTC(date.getUTCFullYear(), date.getUTCMonth(), 1)).toISOString();
}

function getStartOfNextMonthIso(referenceDate = new Date()) {
  const date = new Date(referenceDate || Date.now());
  return new Date(Date.UTC(date.getUTCFullYear(), date.getUTCMonth() + 1, 1)).toISOString();
}

function calculateInvestmentProjectedProfit(principal, annualRate, holdingDays) {
  const amount = toMoney(principal);
  const rate = Number(annualRate || 0);
  const days = Math.max(0, Number(holdingDays || 0));

  if (!Number.isFinite(amount) || amount <= 0) return 0;
  if (!Number.isFinite(rate) || rate <= 0) return 0;
  if (!Number.isFinite(days) || days <= 0) return 0;

  return toMoney(amount * rate * (days / 365));
}

function buildInvestmentProductPublicConfig(config) {
  if (!config) return null;

  return {
    key: config.key,
    name: config.name,
    headline: config.headline,
    cdiMultiplier: Number(config.cdiMultiplier || 0),
    cdiReferenceAnnualPercent: Number(
      (INVESTIMENTOS_CDI_ANUAL_REFERENCIA * 100).toFixed(2)
    ),
    effectiveAnnualPercent: Number(
      (getInvestmentReferenceAnnualRate(config) * 100).toFixed(2)
    ),
    minAmount: toMoney(config.minAmount),
    maxAmount:
      config.maxAmount === null || config.maxAmount === undefined
        ? null
        : toMoney(config.maxAmount),
    displayCapacityBase: toMoney(config.minDisplayCapacity || config.maxAmount || 0),
    lockMonths: Number(config.lockMonths || 0),
    minHoldDaysForProfit: Number(config.minHoldDaysForProfit || 0),
    allowPartialWithdraw: Boolean(config.allowPartialWithdraw),
    withdrawLock: Boolean(config.withdrawLock),
    movementRequiredPerMonth: toMoney(config.movementRequiredPerMonth),
    description: config.description || ""
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

async function getUserByReferralCode(referralCode, client = pool) {
  const normalized = normalizeReferralCode(referralCode);
  if (!normalized) return null;

  const result = await client.query(
    "SELECT * FROM usuarios WHERE referral_code = $1 LIMIT 1",
    [normalized]
  );
  return mapUser(result.rows[0]);
}

async function ensureUserReferralCode(user, client = pool) {
  if (!user?.id) return user;

  const currentCode = normalizeReferralCode(user.referralCode);
  if (currentCode) {
    if (user.referralCode !== currentCode) {
      user.referralCode = currentCode;
      await saveUser(user, client);
    }
    return user;
  }

  for (let attempt = 0; attempt < 12; attempt += 1) {
    const candidate = buildReferralCodeCandidate();
    const existing = await client.query(
      "SELECT id FROM usuarios WHERE referral_code = $1 AND id <> $2 LIMIT 1",
      [candidate, user.id]
    );

    if (existing.rows.length > 0) {
      continue;
    }

    user.referralCode = candidate;
    await saveUser(user, client);
    return user;
  }

  throw new Error("Nao foi possivel gerar um codigo de indicacao unico");
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

async function getLatestAutoAssignableAdditionalSigmoCardByHolderForUpdate(
  holderUserId,
  client
) {
  const holderId = String(holderUserId || "").trim();

  if (!holderId) {
    return null;
  }

  const result = await client.query(
    `
    SELECT *
    FROM sigmo_cards
    WHERE holder_user_id = $1
      AND card_type = 'additional'
      AND status = 'active'
      AND COALESCE(device_id, '') = ''
    ORDER BY created_at DESC NULLS LAST, id DESC
    LIMIT 1
    FOR UPDATE
    `,
    [holderId]
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

async function deleteSigmoCardById(id, client = pool) {
  await client.query("DELETE FROM sigmo_cards WHERE id = $1", [String(id || "").trim()]);
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

async function autoBindAdditionalSigmoCardForDevice(holderUserId, deviceId, client = pool) {
  const holderId = String(holderUserId || "").trim();
  const normalizedDeviceId = String(deviceId || "").trim();

  if (!holderId || !normalizedDeviceId) {
    return null;
  }

  const execute = async (txClient) => {
    const alreadyBound = await getBoundSigmoCardByHolderAndDevice(
      holderId,
      normalizedDeviceId,
      txClient
    );

    if (alreadyBound) {
      return alreadyBound;
    }

    const card = await getLatestAutoAssignableAdditionalSigmoCardByHolderForUpdate(
      holderId,
      txClient
    );

    if (!card) {
      return null;
    }

    const nextCard = {
      ...card,
      deviceId: normalizedDeviceId,
      boundAt: card.boundAt || db(),
      updatedAt: db()
    };

    await saveSigmoCard(nextCard, txClient);
    return nextCard;
  };

  if (client === pool) {
    return runInTransaction((txClient) => execute(txClient));
  }

  return execute(client);
}

async function buildUserActiveCardResponse(user, deviceId, client = pool) {
  if (!user?.id || !String(deviceId || "").trim()) {
    return null;
  }

  let card = await getBoundSigmoCardByHolderAndDevice(user.id, deviceId, client);

  if (!card) {
    card = await autoBindAdditionalSigmoCardForDevice(user.id, deviceId, client);
  }

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
      conta_bloqueada_em, motivo_bloqueio, bloqueio_temporario,
      bonus_boas_vindas, bonus_boas_vindas_concedido_em,
      indicado_por_user_id, indicado_por_email, indicacao_vinculada_em,
      indicacao_qualificada_em, indicacao_bonus_creditado_em,
      indicacao_bonus_creditado_valor, indicacao_bonus_transacao_id,
      referral_code, pin_transacao_hash, pin_transacao_atualizado_em
    )
    VALUES (
      $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27
    )
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
      conta_bloqueada_em = EXCLUDED.conta_bloqueada_em,
      motivo_bloqueio = EXCLUDED.motivo_bloqueio,
      bloqueio_temporario = EXCLUDED.bloqueio_temporario,
      bonus_boas_vindas = EXCLUDED.bonus_boas_vindas,
      bonus_boas_vindas_concedido_em = EXCLUDED.bonus_boas_vindas_concedido_em,
      indicado_por_user_id = EXCLUDED.indicado_por_user_id,
      indicado_por_email = EXCLUDED.indicado_por_email,
      indicacao_vinculada_em = EXCLUDED.indicacao_vinculada_em,
      indicacao_qualificada_em = EXCLUDED.indicacao_qualificada_em,
      indicacao_bonus_creditado_em = EXCLUDED.indicacao_bonus_creditado_em,
      indicacao_bonus_creditado_valor = EXCLUDED.indicacao_bonus_creditado_valor,
      indicacao_bonus_transacao_id = EXCLUDED.indicacao_bonus_transacao_id,
      referral_code = EXCLUDED.referral_code,
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
      normalizeStatusConta(user.statusConta),
      user.contaBanidaEm || null,
      user.motivoBanimento || "",
      user.contaBloqueadaEm || null,
      normalizeAccountRestrictionReason(user.motivoBloqueio),
      Boolean(user.bloqueioTemporario),
      toMoney(user.bonusBoasVindas),
      user.bonusBoasVindasConcedidoEm || null,
      user.indicadoPorUserId || "",
      user.indicadoPorEmail || "",
      user.indicacaoVinculadaEm || null,
      user.indicacaoQualificadaEm || null,
      user.indicacaoBonusCreditadoEm || null,
      toMoney(user.indicacaoBonusCreditadoValor),
      user.indicacaoBonusTransacaoId || "",
      normalizeReferralCode(user.referralCode),
      user.pinTransacaoHash || "",
      user.pinTransacaoAtualizadoEm || null
    ]
  );
}

async function saveUserNotification(notification, client = pool) {
  await client.query(
    `
    INSERT INTO user_notifications (
      id, user_id, type, title, body, metadata, read_at, created_at
    )
    VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
    ON CONFLICT (id) DO UPDATE SET
      user_id = EXCLUDED.user_id,
      type = EXCLUDED.type,
      title = EXCLUDED.title,
      body = EXCLUDED.body,
      metadata = EXCLUDED.metadata,
      read_at = EXCLUDED.read_at,
      created_at = COALESCE(user_notifications.created_at, EXCLUDED.created_at)
    `,
    [
      notification.id,
      notification.userId,
      notification.type || "",
      notification.title || "",
      notification.body || "",
      JSON.stringify(notification.metadata || {}),
      notification.readAt || null,
      notification.createdAt || db()
    ]
  );
}

async function listUserNotificationsByUserId(userId, client = pool) {
  const result = await client.query(
    `
    SELECT *
    FROM user_notifications
    WHERE user_id = $1
    ORDER BY created_at DESC NULLS LAST, id DESC
    LIMIT 100
    `,
    [userId]
  );

  return result.rows.map(mapUserNotification);
}

async function markUserNotificationsAsRead(userId, client = pool) {
  const now = db();
  await client.query(
    `
    UPDATE user_notifications
    SET read_at = COALESCE(read_at, $2)
    WHERE user_id = $1
      AND read_at IS NULL
    `,
    [userId, now]
  );
}

async function saveMovementLimitRequest(request, client = pool) {
  await client.query(
    `
    INSERT INTO movement_limit_requests (
      id, user_id, requested_amount, status, admin_message, pix_key,
      notification_id, created_at, updated_at, responded_at, closed_at
    )
    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
    ON CONFLICT (id) DO UPDATE SET
      user_id = EXCLUDED.user_id,
      requested_amount = EXCLUDED.requested_amount,
      status = EXCLUDED.status,
      admin_message = EXCLUDED.admin_message,
      pix_key = EXCLUDED.pix_key,
      notification_id = EXCLUDED.notification_id,
      updated_at = EXCLUDED.updated_at,
      responded_at = EXCLUDED.responded_at,
      closed_at = EXCLUDED.closed_at
    `,
    [
      request.id,
      request.userId,
      toMoney(request.requestedAmount),
      getMovementRequestStatusLabel(request.status),
      request.adminMessage || "",
      request.pixKey || "",
      request.notificationId || "",
      request.createdAt || db(),
      request.updatedAt || db(),
      request.respondedAt || null,
      request.closedAt || null
    ]
  );
}

async function getMovementLimitRequestById(id, client = pool) {
  const result = await client.query(
    "SELECT * FROM movement_limit_requests WHERE id = $1 LIMIT 1",
    [id]
  );
  return mapMovementLimitRequest(result.rows[0]);
}

async function getMovementLimitRequestByIdForUpdate(id, client) {
  const result = await client.query(
    "SELECT * FROM movement_limit_requests WHERE id = $1 LIMIT 1 FOR UPDATE",
    [id]
  );
  return mapMovementLimitRequest(result.rows[0]);
}

async function getOpenMovementLimitRequestByUserId(userId, client = pool) {
  const result = await client.query(
    `
    SELECT *
    FROM movement_limit_requests
    WHERE user_id = $1
      AND status IN ($2, $3)
    ORDER BY created_at DESC NULLS LAST, id DESC
    LIMIT 1
    `,
    [
      userId,
      MOVEMENT_LIMIT_REQUEST_STATUS_PENDING,
      MOVEMENT_LIMIT_REQUEST_STATUS_RESPONDED
    ]
  );
  return mapMovementLimitRequest(result.rows[0]);
}

async function listMovementLimitRequests(client = pool) {
  const result = await client.query(
    `
    SELECT
      r.*,
      u.email AS user_email,
      u.nome AS user_nome
    FROM movement_limit_requests r
    LEFT JOIN usuarios u ON u.id = r.user_id
    ORDER BY r.created_at DESC NULLS LAST, r.id DESC
    `
  );

  return result.rows.map((row) => ({
    ...mapMovementLimitRequest(row),
    user: row.user_email || row.user_nome
      ? {
          id: row.user_id,
          nome: row.user_nome || row.user_email?.split("@")[0] || "",
          email: row.user_email || ""
        }
      : null
  }));
}

async function saveInvestmentReserve(reserve, client = pool) {
  await client.query(
    `
    INSERT INTO investment_reserves (
      id, user_id, product_key, product_name, product_headline, cdi_multiplier,
      annual_rate, principal_invested_total, principal_remaining, profit_paid_total,
      capacity_limit, min_amount, release_at, profit_eligible_at, lock_months,
      min_hold_days_for_profit, movement_required_per_month, allow_partial_withdraw,
      status, created_at, updated_at, last_withdrawn_at, closed_at
    )
    VALUES (
      $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23
    )
    ON CONFLICT (id) DO UPDATE SET
      user_id = EXCLUDED.user_id,
      product_key = EXCLUDED.product_key,
      product_name = EXCLUDED.product_name,
      product_headline = EXCLUDED.product_headline,
      cdi_multiplier = EXCLUDED.cdi_multiplier,
      annual_rate = EXCLUDED.annual_rate,
      principal_invested_total = EXCLUDED.principal_invested_total,
      principal_remaining = EXCLUDED.principal_remaining,
      profit_paid_total = EXCLUDED.profit_paid_total,
      capacity_limit = EXCLUDED.capacity_limit,
      min_amount = EXCLUDED.min_amount,
      release_at = EXCLUDED.release_at,
      profit_eligible_at = EXCLUDED.profit_eligible_at,
      lock_months = EXCLUDED.lock_months,
      min_hold_days_for_profit = EXCLUDED.min_hold_days_for_profit,
      movement_required_per_month = EXCLUDED.movement_required_per_month,
      allow_partial_withdraw = EXCLUDED.allow_partial_withdraw,
      status = EXCLUDED.status,
      updated_at = EXCLUDED.updated_at,
      last_withdrawn_at = EXCLUDED.last_withdrawn_at,
      closed_at = EXCLUDED.closed_at
    `,
    [
      reserve.id,
      reserve.userId,
      normalizeInvestmentProductKey(reserve.productKey),
      reserve.productName || "",
      reserve.productHeadline || "",
      Number(reserve.cdiMultiplier || 0),
      Number(reserve.annualRate || 0),
      toMoney(reserve.principalInvestedTotal),
      toMoney(reserve.principalRemaining),
      toMoney(reserve.profitPaidTotal),
      toMoney(reserve.capacityLimit),
      toMoney(reserve.minAmount),
      reserve.releaseAt || null,
      reserve.profitEligibleAt || null,
      Number(reserve.lockMonths || 0),
      Number(reserve.minHoldDaysForProfit || 0),
      toMoney(reserve.movementRequiredPerMonth),
      Boolean(reserve.allowPartialWithdraw),
      getInvestmentReserveStatus(reserve.status),
      reserve.createdAt || db(),
      reserve.updatedAt || db(),
      reserve.lastWithdrawnAt || null,
      reserve.closedAt || null
    ]
  );
}

async function getInvestmentReserveById(id, client = pool) {
  const result = await client.query(
    "SELECT * FROM investment_reserves WHERE id = $1 LIMIT 1",
    [id]
  );
  return mapInvestmentReserve(result.rows[0]);
}

async function getInvestmentReserveByIdForUpdate(id, client) {
  const result = await client.query(
    "SELECT * FROM investment_reserves WHERE id = $1 LIMIT 1 FOR UPDATE",
    [id]
  );
  return mapInvestmentReserve(result.rows[0]);
}

async function listInvestmentReservesByUserId(userId, client = pool) {
  const result = await client.query(
    `
    SELECT *
    FROM investment_reserves
    WHERE user_id = $1
    ORDER BY created_at DESC NULLS LAST, id DESC
    `,
    [userId]
  );

  return result.rows.map(mapInvestmentReserve);
}

async function listInvestmentReserves(client = pool) {
  const result = await client.query(
    `
    SELECT
      r.*,
      u.email AS user_email,
      u.nome AS user_nome
    FROM investment_reserves r
    LEFT JOIN usuarios u ON u.id = r.user_id
    ORDER BY r.created_at DESC NULLS LAST, r.id DESC
    `
  );

  return result.rows.map((row) => ({
    ...mapInvestmentReserve(row),
    user: row.user_email || row.user_nome
      ? {
          id: row.user_id,
          nome: row.user_nome || row.user_email?.split("@")[0] || "",
          email: row.user_email || ""
        }
      : null
  }));
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

async function getBannerSettings(client = pool) {
  const result = await client.query(
    `
    SELECT rotation_ms, updated_at
    FROM banner_settings
    WHERE id = $1
    LIMIT 1
    `,
    [BANNER_SETTINGS_ID]
  );

  if (!result.rows.length) {
    const fallback = {
      rotationMs: BANNER_ROTATION_DEFAULT_MS,
      updatedAt: db()
    };

    await client.query(
      `
      INSERT INTO banner_settings (id, rotation_ms, updated_at)
      VALUES ($1, $2, $3)
      ON CONFLICT (id) DO NOTHING
      `,
      [BANNER_SETTINGS_ID, fallback.rotationMs, fallback.updatedAt]
    );

    return fallback;
  }

  return {
    rotationMs: clampBannerRotationMs(result.rows[0].rotation_ms),
    updatedAt: result.rows[0].updated_at || null
  };
}

async function setBannerSettings(rotationMs, client = pool) {
  const nextRotationMs = clampBannerRotationMs(rotationMs);
  const updatedAt = db();

  await client.query(
    `
    INSERT INTO banner_settings (id, rotation_ms, updated_at)
    VALUES ($1, $2, $3)
    ON CONFLICT (id) DO UPDATE SET
      rotation_ms = EXCLUDED.rotation_ms,
      updated_at = EXCLUDED.updated_at
    `,
    [BANNER_SETTINGS_ID, nextRotationMs, updatedAt]
  );

  return {
    rotationMs: nextRotationMs,
    updatedAt
  };
}

async function getAppRuntimeSettings(client = pool) {
  const result = await client.query(
    `
    SELECT maintenance_enabled, maintenance_message, maintenance_eta_minutes, updated_at
    FROM app_runtime_settings
    WHERE id = $1
    LIMIT 1
    `,
    [APP_RUNTIME_SETTINGS_ID]
  );

  if (!result.rows.length) {
    const fallback = {
      maintenanceEnabled: false,
      maintenanceMessage: APP_MAINTENANCE_MESSAGE_DEFAULT,
      maintenanceEtaMinutes: APP_MAINTENANCE_ETA_DEFAULT_MINUTES,
      updatedAt: db()
    };

    await client.query(
      `
      INSERT INTO app_runtime_settings (
        id,
        maintenance_enabled,
        maintenance_message,
        maintenance_eta_minutes,
        updated_at
      )
      VALUES ($1, $2, $3, $4, $5)
      ON CONFLICT (id) DO NOTHING
      `,
      [
        APP_RUNTIME_SETTINGS_ID,
        fallback.maintenanceEnabled,
        fallback.maintenanceMessage,
        fallback.maintenanceEtaMinutes,
        fallback.updatedAt
      ]
    );

    return fallback;
  }

  return {
    maintenanceEnabled: result.rows[0].maintenance_enabled === true,
    maintenanceMessage: normalizeMaintenanceMessage(result.rows[0].maintenance_message),
    maintenanceEtaMinutes: clampMaintenanceEtaMinutes(
      result.rows[0].maintenance_eta_minutes
    ),
    updatedAt: result.rows[0].updated_at || null
  };
}

async function setAppRuntimeSettings(input = {}, client = pool) {
  const current = await getAppRuntimeSettings(client);
  const updatedAt = db();
  const nextSettings = {
    maintenanceEnabled:
      typeof input.maintenanceEnabled === "boolean"
        ? input.maintenanceEnabled
        : current.maintenanceEnabled,
    maintenanceMessage: Object.prototype.hasOwnProperty.call(
      input,
      "maintenanceMessage"
    )
      ? normalizeMaintenanceMessage(input.maintenanceMessage)
      : current.maintenanceMessage,
    maintenanceEtaMinutes: Object.prototype.hasOwnProperty.call(
      input,
      "maintenanceEtaMinutes"
    )
      ? clampMaintenanceEtaMinutes(input.maintenanceEtaMinutes)
      : current.maintenanceEtaMinutes,
    updatedAt
  };

  await client.query(
    `
    INSERT INTO app_runtime_settings (
      id,
      maintenance_enabled,
      maintenance_message,
      maintenance_eta_minutes,
      updated_at
    )
    VALUES ($1, $2, $3, $4, $5)
    ON CONFLICT (id) DO UPDATE SET
      maintenance_enabled = EXCLUDED.maintenance_enabled,
      maintenance_message = EXCLUDED.maintenance_message,
      maintenance_eta_minutes = EXCLUDED.maintenance_eta_minutes,
      updated_at = EXCLUDED.updated_at
    `,
    [
      APP_RUNTIME_SETTINGS_ID,
      nextSettings.maintenanceEnabled,
      nextSettings.maintenanceMessage,
      nextSettings.maintenanceEtaMinutes,
      nextSettings.updatedAt
    ]
  );

  return nextSettings;
}

async function listBannerAssets(client = pool, { activeOnly = false } = {}) {
  const clauses = [];
  const params = [];

  if (activeOnly) {
    clauses.push(`is_active = true`);
  }

  const whereSql = clauses.length ? `WHERE ${clauses.join(" AND ")}` : "";
  const result = await client.query(
    `
    SELECT
      id,
      mime_type,
      alt_text,
      click_url,
      is_active,
      duration_ms,
      sort_order,
      created_at,
      updated_at
    FROM banner_assets
    ${whereSql}
    ORDER BY sort_order ASC, created_at ASC, id ASC
    `,
    params
  );

  return result.rows.map(mapBannerAsset).filter(Boolean);
}

async function getBannerAssetBinary(id, client = pool) {
  const result = await client.query(
    `
    SELECT id, mime_type, image_data, updated_at
    FROM banner_assets
    WHERE id = $1
    LIMIT 1
    `,
    [String(id || "").trim()]
  );

  if (!result.rows.length) return null;

  return {
    id: result.rows[0].id,
    mimeType: result.rows[0].mime_type || "image/jpeg",
    imageData: result.rows[0].image_data,
    updatedAt: result.rows[0].updated_at || null
  };
}

async function createBannerAssets(files = [], client = pool) {
  if (!Array.isArray(files) || !files.length) {
    return [];
  }

  const existing = await listBannerAssets(client);
  let nextOrder = existing.reduce(
    (maxValue, item) => Math.max(maxValue, Number(item?.sortOrder || 0)),
    -1
  ) + 1;
  const createdAt = db();
  const created = [];

  for (const file of files) {
    if (!file?.buffer?.length) continue;

    const id = buildId("banner");
    const asset = {
      id,
      mimeType: String(file.mimetype || "image/jpeg").toLowerCase(),
      alt: normalizeBannerAlt(file.originalname || "Banner Sigmo"),
      href: "",
      active: true,
      durationMs: BANNER_DURATION_DEFAULT_MS,
      sortOrder: nextOrder++,
      createdAt
    };

    await client.query(
      `
      INSERT INTO banner_assets (
        id, mime_type, image_data, alt_text, click_url, is_active, duration_ms, sort_order, created_at, updated_at
      )
      VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
      `,
      [
        asset.id,
        asset.mimeType,
        file.buffer,
        asset.alt,
        asset.href,
        asset.active,
        asset.durationMs,
        asset.sortOrder,
        asset.createdAt,
        asset.createdAt
      ]
    );

    created.push({
      id: asset.id,
      imageUrl: buildBannerImageUrl(asset.id),
      alt: asset.alt,
      href: asset.href,
      active: asset.active,
      durationMs: asset.durationMs,
      sortOrder: asset.sortOrder,
      createdAt: asset.createdAt,
      updatedAt: asset.createdAt
    });
  }

  return created;
}

async function updateBannerAssetMetadata(items = [], client = pool) {
  const existing = await listBannerAssets(client);
  const existingById = new Map(existing.map((item) => [item.id, item]));
  const nextItems = [];

  for (let index = 0; index < items.length; index += 1) {
    const raw = items[index];
    const id = String(raw?.id || "").trim();
    const current = existingById.get(id);

    if (!current) {
      continue;
    }

    nextItems.push({
      id,
      alt: normalizeBannerAlt(raw?.alt || current.alt),
      href: normalizeBannerHref(raw?.href || current.href),
      active: raw?.active !== false,
      durationMs: clampBannerDurationMs(raw?.durationMs || current.durationMs),
      sortOrder: index
    });
  }

  for (const item of nextItems) {
    await client.query(
      `
      UPDATE banner_assets
      SET alt_text = $2,
          click_url = $3,
          is_active = $4,
          duration_ms = $5,
          sort_order = $6,
          updated_at = $7
      WHERE id = $1
      `,
      [item.id, item.alt, item.href, item.active, item.durationMs, item.sortOrder, db()]
    );
  }
}

async function deleteBannerAsset(id, client = pool) {
  const bannerId = String(id || "").trim();

  if (!bannerId) return false;

  const result = await client.query(
    "DELETE FROM banner_assets WHERE id = $1",
    [bannerId]
  );

  return result.rowCount > 0;
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

async function saveRecargaCelularPedido(pedido, client = pool) {
  const operadora = normalizeRecargaCelularOperadora(pedido.operadora);
  const ddd = normalizeRecargaCelularDdd(pedido.ddd);
  const numero = normalizeRecargaCelularNumero(pedido.numero);
  const status = normalizeRecargaCelularStatus(pedido.status);

  await client.query(
    `
    INSERT INTO topup_orders (
      id, user_id, operadora, ddd, numero, telefone, valor_recarga, taxa_valor,
      valor_total_debitado, bonus_debitado, real_debitado, status, motivo_recusa,
      financial_transaction_id_debito, financial_transaction_id_estorno, admin_id,
      criado_em, atualizado_em, aprovado_em, recusado_em, estornado_em
    )
    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21)
    ON CONFLICT (id) DO UPDATE SET
      user_id = EXCLUDED.user_id,
      operadora = EXCLUDED.operadora,
      ddd = EXCLUDED.ddd,
      numero = EXCLUDED.numero,
      telefone = EXCLUDED.telefone,
      valor_recarga = EXCLUDED.valor_recarga,
      taxa_valor = EXCLUDED.taxa_valor,
      valor_total_debitado = EXCLUDED.valor_total_debitado,
      bonus_debitado = EXCLUDED.bonus_debitado,
      real_debitado = EXCLUDED.real_debitado,
      status = EXCLUDED.status,
      motivo_recusa = EXCLUDED.motivo_recusa,
      financial_transaction_id_debito = EXCLUDED.financial_transaction_id_debito,
      financial_transaction_id_estorno = EXCLUDED.financial_transaction_id_estorno,
      admin_id = EXCLUDED.admin_id,
      criado_em = COALESCE(topup_orders.criado_em, EXCLUDED.criado_em),
      atualizado_em = EXCLUDED.atualizado_em,
      aprovado_em = EXCLUDED.aprovado_em,
      recusado_em = EXCLUDED.recusado_em,
      estornado_em = EXCLUDED.estornado_em
    `,
    [
      pedido.id,
      pedido.userId,
      operadora,
      ddd,
      numero,
      buildRecargaCelularTelefone(ddd, numero),
      toMoney(pedido.valorRecarga),
      toMoney(pedido.taxaValor),
      toMoney(pedido.valorTotalDebitado),
      toMoney(pedido.bonusDebitado),
      toMoney(pedido.realDebitado),
      status,
      normalizeRecargaCelularMotivoRecusa(pedido.motivoRecusa),
      String(pedido.financialTransactionIdDebito || "").trim(),
      String(pedido.financialTransactionIdEstorno || "").trim(),
      String(pedido.adminId || "").trim(),
      pedido.criadoEm || db(),
      pedido.atualizadoEm || db(),
      pedido.aprovadoEm || null,
      pedido.recusadoEm || null,
      pedido.estornadoEm || null
    ]
  );
}

async function getRecargaCelularPedidoById(id, client = pool) {
  const result = await client.query(
    "SELECT * FROM topup_orders WHERE id = $1 LIMIT 1",
    [String(id || "").trim()]
  );
  return mapRecargaCelularPedido(result.rows[0]);
}

async function getRecargaCelularPedidoByIdForUpdate(id, client) {
  const result = await client.query(
    "SELECT * FROM topup_orders WHERE id = $1 LIMIT 1 FOR UPDATE",
    [String(id || "").trim()]
  );
  return mapRecargaCelularPedido(result.rows[0]);
}

async function listRecargaCelularPedidosByUser(userId, client = pool) {
  const result = await client.query(
    `
    SELECT *
    FROM topup_orders
    WHERE user_id = $1
    ORDER BY criado_em DESC NULLS LAST, id DESC
    `,
    [String(userId || "").trim()]
  );

  return result.rows.map(mapRecargaCelularPedido);
}

async function listRecargaCelularPedidos(status = "", client = pool) {
  const rawStatus = String(status || "").trim().toLowerCase();
  const shouldFilter = ["pendente", "aprovado", "recusado"].includes(rawStatus);
  const whereSql = shouldFilter ? "WHERE t.status = $1" : "";
  const params = shouldFilter ? [rawStatus] : [];
  const result = await client.query(
    `
    SELECT
      t.*,
      u.nome AS user_nome,
      u.email AS user_email
    FROM topup_orders t
    LEFT JOIN usuarios u ON u.id = t.user_id
    ${whereSql}
    ORDER BY t.criado_em DESC NULLS LAST, t.id DESC
    `,
    params
  );

  return result.rows.map(mapRecargaCelularPedido);
}

async function saveShopCategory(category, client = pool) {
  const payload = {
    id: String(category.id || buildId("shopcat")).trim(),
    sourceKey: normalizeShopCategorySourceKey(category.sourceKey, category.source),
    source: normalizeShopSource(category.source),
    slug: slugifyShopValue(
      normalizeShopCatalogCategoryName(category.name, 120) || category.slug,
      "categoria"
    ),
    name: normalizeShopCatalogCategoryName(category.name, 120),
    description: normalizeShopCatalogText(category.description, 1000),
    imageUrl: normalizeShopUrl(category.imageUrl),
    active: category.active !== false,
    sortOrder: Number(category.sortOrder || 0),
    createdAt: category.createdAt || db(),
    updatedAt: category.updatedAt || db()
  };

  const result = await client.query(
    `
    INSERT INTO shop_categories (
      id, source_key, source, slug, name, description, image_url,
      is_active, sort_order, created_at, updated_at
    )
    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
    ON CONFLICT (source_key) DO UPDATE SET
      source = EXCLUDED.source,
      slug = EXCLUDED.slug,
      name = EXCLUDED.name,
      description = EXCLUDED.description,
      image_url = EXCLUDED.image_url,
      is_active = EXCLUDED.is_active,
      sort_order = EXCLUDED.sort_order,
      updated_at = EXCLUDED.updated_at
    RETURNING *
    `,
    [
      payload.id,
      payload.sourceKey,
      payload.source,
      payload.slug,
      payload.name,
      payload.description,
      payload.imageUrl,
      payload.active,
      payload.sortOrder,
      payload.createdAt,
      payload.updatedAt
    ]
  );

  return mapShopCategory(result.rows[0]);
}

async function saveShopProduct(product, client = pool) {
  const payload = {
    id: String(product.id || buildId("shopprd")).trim(),
    categoryId: String(product.categoryId || "").trim(),
    sourceKey: normalizeShopProductSourceKey(product.sourceKey, product.source),
    source: normalizeShopSource(product.source),
    externalId: normalizeShopCatalogText(product.externalId, 120),
    externalUrl: normalizeShopUrl(product.externalUrl),
    slug: slugifyShopValue(
      normalizeShopCatalogText(product.slug || product.name, 180),
      "produto"
    ),
    name: normalizeShopCatalogText(product.name, 180),
    shortDescription: normalizeShopCatalogText(product.shortDescription, 320),
    description: normalizeShopCatalogText(product.description, 2400),
    imageUrl: normalizeShopUrl(product.imageUrl),
    supplierPrice: Math.max(0, toMoney(product.supplierPrice)),
    markupPercent: normalizeShopMarkupPercent(product.markupPercent),
    price: Math.max(
      0,
      toMoney(
        product.price !== undefined && product.price !== null
          ? product.price
          : calculateShopSalePrice(product.supplierPrice, product.markupPercent)
      )
    ),
    currency: normalizeShopText(product.currency || "BRL", 12).toUpperCase() || "BRL",
    active: product.active !== false,
    rawPayload: product.rawPayload || {},
    createdAt: product.createdAt || db(),
    updatedAt: product.updatedAt || db()
  };

  const result = await client.query(
    `
    INSERT INTO shop_products (
      id, category_id, source_key, source, external_id, external_url, slug, name,
      short_description, description, image_url, supplier_price, markup_percent,
      price, currency, is_active, raw_payload, created_at, updated_at
    )
    VALUES (
      $1,$2,$3,$4,$5,$6,$7,$8,
      $9,$10,$11,$12,$13,
      $14,$15,$16,$17,$18,$19
    )
    ON CONFLICT (source_key) DO UPDATE SET
      category_id = EXCLUDED.category_id,
      source = EXCLUDED.source,
      external_id = EXCLUDED.external_id,
      external_url = EXCLUDED.external_url,
      slug = EXCLUDED.slug,
      name = EXCLUDED.name,
      short_description = EXCLUDED.short_description,
      description = EXCLUDED.description,
      image_url = EXCLUDED.image_url,
      supplier_price = EXCLUDED.supplier_price,
      markup_percent = EXCLUDED.markup_percent,
      price = EXCLUDED.price,
      currency = EXCLUDED.currency,
      is_active = EXCLUDED.is_active,
      raw_payload = EXCLUDED.raw_payload,
      updated_at = EXCLUDED.updated_at
    RETURNING *
    `,
    [
      payload.id,
      payload.categoryId,
      payload.sourceKey,
      payload.source,
      payload.externalId,
      payload.externalUrl,
      payload.slug,
      payload.name,
      payload.shortDescription,
      payload.description,
      payload.imageUrl,
      payload.supplierPrice,
      payload.markupPercent,
      payload.price,
      payload.currency,
      payload.active,
      JSON.stringify(payload.rawPayload || {}),
      payload.createdAt,
      payload.updatedAt
    ]
  );

  return mapShopProduct(result.rows[0]);
}

async function saveShopOrder(order, client = pool) {
  const shipping = order.shipping || {};
  const payload = {
    id: String(order.id || buildId("shopord")).trim(),
    userId: String(order.userId || "").trim(),
    status: normalizeShopOrderStatus(order.status),
    subtotalAmount: toMoney(order.subtotalAmount),
    totalAmount: toMoney(order.totalAmount),
    bonusDebitado: toMoney(order.bonusDebitado),
    realDebitado: toMoney(order.realDebitado),
    shippingName: normalizeShopText(shipping.name, 120),
    shippingPhone: normalizeShopPhone(shipping.phone),
    shippingZip: normalizeShopPostalCode(shipping.zip),
    shippingStreet: normalizeShopText(shipping.street, 120),
    shippingNumber: normalizeShopText(shipping.number, 20),
    shippingComplement: normalizeShopText(shipping.complement, 120),
    shippingNeighborhood: normalizeShopText(shipping.neighborhood, 120),
    shippingCity: normalizeShopText(shipping.city, 120),
    shippingState: normalizeShopState(shipping.state),
    shippingReference: normalizeShopText(shipping.reference, 220),
    customerNote: normalizeShopText(order.customerNote, 1000),
    refusalReason: normalizeShopText(order.refusalReason, 320),
    financialTransactionIdDebito: String(order.financialTransactionIdDebito || "").trim(),
    financialTransactionIdEstorno: String(order.financialTransactionIdEstorno || "").trim(),
    adminId: String(order.adminId || "").trim(),
    createdAt: order.createdAt || db(),
    updatedAt: order.updatedAt || db(),
    approvedAt: order.approvedAt || null,
    refusedAt: order.refusedAt || null,
    refundedAt: order.refundedAt || null
  };

  const result = await client.query(
    `
    INSERT INTO shop_orders (
      id, user_id, status, subtotal_amount, total_amount, bonus_debitado,
      real_debitado, shipping_name, shipping_phone, shipping_zip, shipping_street,
      shipping_number, shipping_complement, shipping_neighborhood, shipping_city,
      shipping_state, shipping_reference, customer_note, refusal_reason,
      financial_transaction_id_debito, financial_transaction_id_estorno, admin_id,
      created_at, updated_at, approved_at, refused_at, refunded_at
    )
    VALUES (
      $1,$2,$3,$4,$5,$6,
      $7,$8,$9,$10,$11,
      $12,$13,$14,$15,
      $16,$17,$18,$19,
      $20,$21,$22,
      $23,$24,$25,$26,$27
    )
    ON CONFLICT (id) DO UPDATE SET
      user_id = EXCLUDED.user_id,
      status = EXCLUDED.status,
      subtotal_amount = EXCLUDED.subtotal_amount,
      total_amount = EXCLUDED.total_amount,
      bonus_debitado = EXCLUDED.bonus_debitado,
      real_debitado = EXCLUDED.real_debitado,
      shipping_name = EXCLUDED.shipping_name,
      shipping_phone = EXCLUDED.shipping_phone,
      shipping_zip = EXCLUDED.shipping_zip,
      shipping_street = EXCLUDED.shipping_street,
      shipping_number = EXCLUDED.shipping_number,
      shipping_complement = EXCLUDED.shipping_complement,
      shipping_neighborhood = EXCLUDED.shipping_neighborhood,
      shipping_city = EXCLUDED.shipping_city,
      shipping_state = EXCLUDED.shipping_state,
      shipping_reference = EXCLUDED.shipping_reference,
      customer_note = EXCLUDED.customer_note,
      refusal_reason = EXCLUDED.refusal_reason,
      financial_transaction_id_debito = EXCLUDED.financial_transaction_id_debito,
      financial_transaction_id_estorno = EXCLUDED.financial_transaction_id_estorno,
      admin_id = EXCLUDED.admin_id,
      created_at = COALESCE(shop_orders.created_at, EXCLUDED.created_at),
      updated_at = EXCLUDED.updated_at,
      approved_at = EXCLUDED.approved_at,
      refused_at = EXCLUDED.refused_at,
      refunded_at = EXCLUDED.refunded_at
    `,
    [
      payload.id,
      payload.userId,
      payload.status,
      payload.subtotalAmount,
      payload.totalAmount,
      payload.bonusDebitado,
      payload.realDebitado,
      payload.shippingName,
      payload.shippingPhone,
      payload.shippingZip,
      payload.shippingStreet,
      payload.shippingNumber,
      payload.shippingComplement,
      payload.shippingNeighborhood,
      payload.shippingCity,
      payload.shippingState,
      payload.shippingReference,
      payload.customerNote,
      payload.refusalReason,
      payload.financialTransactionIdDebito,
      payload.financialTransactionIdEstorno,
      payload.adminId,
      payload.createdAt,
      payload.updatedAt,
      payload.approvedAt,
      payload.refusedAt,
      payload.refundedAt
    ]
  );

  return getShopOrderById(payload.id, client);
}

async function replaceShopOrderItems(orderId, items = [], client = pool) {
  const normalizedOrderId = String(orderId || "").trim();

  await client.query("DELETE FROM shop_order_items WHERE order_id = $1", [normalizedOrderId]);

  for (const rawItem of Array.isArray(items) ? items : []) {
    const item = {
      id: String(rawItem.id || buildId("shopitem")).trim(),
      orderId: normalizedOrderId,
      productId: String(rawItem.productId || "").trim(),
      categoryId: String(rawItem.categoryId || "").trim(),
      sourceKey: normalizeShopProductSourceKey(rawItem.sourceKey, rawItem.source),
      externalUrl: normalizeShopUrl(rawItem.externalUrl),
      productName: normalizeShopText(rawItem.productName, 180),
      productSlug: slugifyShopValue(rawItem.productSlug || rawItem.productName, "produto"),
      imageUrl: normalizeShopUrl(rawItem.imageUrl),
      supplierPrice: Math.max(0, toMoney(rawItem.supplierPrice)),
      unitPrice: Math.max(0, toMoney(rawItem.unitPrice)),
      quantity: Math.max(1, normalizeShopQuantity(rawItem.quantity)),
      totalPrice: Math.max(0, toMoney(rawItem.totalPrice)),
      metadata: rawItem.metadata || {},
      createdAt: rawItem.createdAt || db(),
      updatedAt: rawItem.updatedAt || db()
    };

    await client.query(
      `
      INSERT INTO shop_order_items (
        id, order_id, product_id, category_id, source_key, external_url, product_name,
        product_slug, image_url, supplier_price, unit_price, quantity, total_price,
        metadata, created_at, updated_at
      )
      VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16)
      `,
      [
        item.id,
        item.orderId,
        item.productId,
        item.categoryId,
        item.sourceKey,
        item.externalUrl,
        item.productName,
        item.productSlug,
        item.imageUrl,
        item.supplierPrice,
        item.unitPrice,
        item.quantity,
        item.totalPrice,
        JSON.stringify(item.metadata || {}),
        item.createdAt,
        item.updatedAt
      ]
    );
  }
}

async function listShopCategories(options = {}, client = pool) {
  const activeOnly = options?.activeOnly === true;
  const result = await client.query(
    `
    SELECT *
    FROM shop_categories
    ${activeOnly ? "WHERE is_active = true" : ""}
    ORDER BY sort_order ASC, name ASC, created_at ASC NULLS LAST, id ASC
    `
  );

  return result.rows.map(mapShopCategory);
}

async function listShopProducts(options = {}, client = pool) {
  const params = [];
  const clauses = [];

  if (options?.activeOnly) {
    clauses.push("p.is_active = true");
    clauses.push("c.is_active = true");
  }

  if (options?.categoryId) {
    params.push(String(options.categoryId || "").trim());
    clauses.push(`p.category_id = $${params.length}`);
  } else if (options?.categorySlug) {
    params.push(String(options.categorySlug || "").trim());
    clauses.push(`c.slug = $${params.length}`);
  }

  if (options?.search) {
    params.push(`%${String(options.search || "").trim().toLowerCase()}%`);
    clauses.push(`(
      LOWER(p.name) LIKE $${params.length}
      OR LOWER(p.short_description) LIKE $${params.length}
      OR LOWER(p.description) LIKE $${params.length}
      OR LOWER(c.name) LIKE $${params.length}
    )`);
  }

  const whereSql = clauses.length ? `WHERE ${clauses.join(" AND ")}` : "";
  const result = await client.query(
    `
    SELECT
      p.*,
      c.source_key AS category_source_key,
      c.source AS category_source,
      c.slug AS category_slug,
      c.name AS category_name,
      c.description AS category_description,
      c.image_url AS category_image_url,
      c.is_active AS category_is_active,
      c.sort_order AS category_sort_order
    FROM shop_products p
    INNER JOIN shop_categories c ON c.id = p.category_id
    ${whereSql}
    ORDER BY c.sort_order ASC, c.name ASC, p.name ASC, p.created_at ASC NULLS LAST, p.id ASC
    `,
    params
  );

  return result.rows.map(mapShopProduct);
}

async function getShopProductById(id, client = pool) {
  const result = await client.query(
    `
    SELECT
      p.*,
      c.source_key AS category_source_key,
      c.source AS category_source,
      c.slug AS category_slug,
      c.name AS category_name,
      c.description AS category_description,
      c.image_url AS category_image_url,
      c.is_active AS category_is_active,
      c.sort_order AS category_sort_order
    FROM shop_products p
    INNER JOIN shop_categories c ON c.id = p.category_id
    WHERE p.id = $1
    LIMIT 1
    `,
    [String(id || "").trim()]
  );

  return mapShopProduct(result.rows[0]);
}

async function listShopProductsByIds(ids = [], client = pool) {
  const normalizedIds = Array.from(
    new Set(
      (Array.isArray(ids) ? ids : [])
        .map((id) => String(id || "").trim())
        .filter(Boolean)
    )
  );

  if (!normalizedIds.length) {
    return [];
  }

  const result = await client.query(
    `
    SELECT
      p.*,
      c.source_key AS category_source_key,
      c.source AS category_source,
      c.slug AS category_slug,
      c.name AS category_name,
      c.description AS category_description,
      c.image_url AS category_image_url,
      c.is_active AS category_is_active,
      c.sort_order AS category_sort_order
    FROM shop_products p
    INNER JOIN shop_categories c ON c.id = p.category_id
    WHERE p.id = ANY($1::text[])
    `,
    [normalizedIds]
  );

  return result.rows.map(mapShopProduct);
}

async function getShopProductByIdForUpdate(id, client) {
  const result = await client.query(
    `
    SELECT *
    FROM shop_products
    WHERE id = $1
    LIMIT 1
    FOR UPDATE
    `,
    [String(id || "").trim()]
  );

  return mapShopProduct(result.rows[0]);
}

async function getShopOrderById(id, client = pool) {
  const result = await client.query(
    `
    SELECT
      o.*,
      u.nome AS user_nome,
      u.email AS user_email
    FROM shop_orders o
    LEFT JOIN usuarios u ON u.id = o.user_id
    WHERE o.id = $1
    LIMIT 1
    `,
    [String(id || "").trim()]
  );

  const order = mapShopOrder(result.rows[0]);
  if (!order) return null;
  const [withItems] = await attachShopItemsToOrders([order], client);
  return withItems || null;
}

async function getShopOrderByIdForUpdate(id, client) {
  const result = await client.query(
    `
    SELECT *
    FROM shop_orders
    WHERE id = $1
    LIMIT 1
    FOR UPDATE
    `,
    [String(id || "").trim()]
  );

  return mapShopOrder(result.rows[0]);
}

async function listShopOrdersByUser(userId, client = pool) {
  const result = await client.query(
    `
    SELECT
      o.*,
      u.nome AS user_nome,
      u.email AS user_email
    FROM shop_orders o
    LEFT JOIN usuarios u ON u.id = o.user_id
    WHERE o.user_id = $1
    ORDER BY o.created_at DESC NULLS LAST, o.id DESC
    `,
    [String(userId || "").trim()]
  );

  return attachShopItemsToOrders(result.rows.map(mapShopOrder), client);
}

async function listShopOrders(status = "", client = pool) {
  const requestedStatus = String(status || "").trim().toLowerCase();
  const shouldFilter = [
    SHOP_ORDER_STATUS_PENDING,
    SHOP_ORDER_STATUS_APPROVED,
    SHOP_ORDER_STATUS_REFUSED
  ].includes(requestedStatus);
  const rawStatus = shouldFilter
    ? requestedStatus
    : SHOP_ORDER_STATUS_PENDING;
  const params = shouldFilter ? [rawStatus] : [];
  const result = await client.query(
    `
    SELECT
      o.*,
      u.nome AS user_nome,
      u.email AS user_email
    FROM shop_orders o
    LEFT JOIN usuarios u ON u.id = o.user_id
    ${shouldFilter ? "WHERE o.status = $1" : ""}
    ORDER BY o.created_at DESC NULLS LAST, o.id DESC
    `,
    params
  );

  return attachShopItemsToOrders(result.rows.map(mapShopOrder), client);
}

async function attachShopItemsToOrders(orders = [], client = pool) {
  const orderList = Array.isArray(orders) ? orders.filter(Boolean) : [];
  const ids = orderList.map((order) => String(order.id || "").trim()).filter(Boolean);

  if (!ids.length) {
    return orderList.map((order) => ({ ...order, items: [] }));
  }

  const result = await client.query(
    `
    SELECT *
    FROM shop_order_items
    WHERE order_id = ANY($1::text[])
    ORDER BY created_at ASC NULLS LAST, id ASC
    `,
    [ids]
  );

  const itemsByOrderId = new Map();
  for (const row of result.rows) {
    const item = mapShopOrderItem(row);
    const list = itemsByOrderId.get(item.orderId) || [];
    list.push(item);
    itemsByOrderId.set(item.orderId, list);
  }

  return orderList.map((order) => ({
    ...order,
    items: itemsByOrderId.get(order.id) || []
  }));
}

async function importShopCatalog(input = {}, client = pool) {
  const source = normalizeShopSource(input.source || SHOP_PRODUCT_SOURCE_DEFAULT);
  const defaultMarkupPercent = normalizeShopMarkupPercent(
    input.markupPercent === undefined ? SHOP_DEFAULT_MARKUP_PERCENTUAL : input.markupPercent
  );
  const deactivateMissing = input.deactivateMissing === true;
  const groupedCategories = [];
  const importedCategoryIds = new Set();
  const importedProductKeys = new Set();
  let importedProducts = 0;
  let skippedProducts = 0;

  if (Array.isArray(input.categories)) {
    for (let index = 0; index < input.categories.length; index += 1) {
      const category = input.categories[index] || {};
      groupedCategories.push({
        ...category,
        sortOrder: Number(category.sortOrder ?? index),
        products: Array.isArray(category.products) ? category.products : []
      });
    }
  }

  if (Array.isArray(input.products) && input.products.length) {
    const productsByCategory = new Map();
    for (const product of input.products) {
      const categoryName =
        normalizeShopCatalogCategoryName(
          product?.categoryName || product?.category || "Sem categoria",
          120
        );
      if (!productsByCategory.has(categoryName)) {
        productsByCategory.set(categoryName, []);
      }
      productsByCategory.get(categoryName).push(product);
    }

    let sortOrderBase = groupedCategories.length;
    for (const [categoryName, products] of productsByCategory.entries()) {
      groupedCategories.push({
        name: categoryName,
        sortOrder: sortOrderBase++,
        products
      });
    }
  }

  for (const rawCategory of groupedCategories) {
    const categoryName = normalizeShopCatalogText(
      rawCategory.name || rawCategory.title || rawCategory.categoryName,
      120
    );

    if (!categoryName) {
      continue;
    }

    const categorySourceKey = normalizeShopCategorySourceKey(
      rawCategory.sourceKey || `${source}:category:${slugifyShopValue(categoryName, "categoria")}`,
      source
    );
    const categorySlug = slugifyShopValue(rawCategory.slug || categoryName, "categoria");
    const savedCategory = await saveShopCategory(
      {
        source,
        sourceKey: categorySourceKey,
        slug: categorySlug,
        name: categoryName,
        description: rawCategory.description || "",
        imageUrl: rawCategory.imageUrl || rawCategory.image || "",
        active: rawCategory.active !== false,
        sortOrder: rawCategory.sortOrder
      },
      client
    );

    importedCategoryIds.add(savedCategory.id);

    for (const rawProduct of Array.isArray(rawCategory.products) ? rawCategory.products : []) {
      const productName = normalizeShopCatalogText(
        rawProduct?.name || rawProduct?.title || rawProduct?.produto,
        180
      );
      const supplierPrice = Math.max(
        0,
        toMoney(
          rawProduct?.supplierPrice ??
            rawProduct?.cost ??
            rawProduct?.priceCost ??
            rawProduct?.valorCusto ??
            rawProduct?.precoCusto ??
            rawProduct?.price
        )
      );

      if (!productName || supplierPrice <= 0) {
        skippedProducts += 1;
        continue;
      }

      const externalId = normalizeShopCatalogText(
        rawProduct?.externalId || rawProduct?.id || rawProduct?.sku,
        120
      );
      const externalUrl = normalizeShopUrl(
        rawProduct?.externalUrl || rawProduct?.url || rawProduct?.href || rawProduct?.link
      );
      const sourceKey = normalizeShopProductSourceKey(
        rawProduct?.sourceKey ||
          `${source}:product:${externalId || slugifyShopValue(productName, "produto")}:${slugifyShopValue(categoryName, "categoria")}`,
        source
      );
      const productSlug = buildShopUniqueSlug(
        rawProduct?.slug || productName,
        sourceKey,
        "produto"
      );
      const markupPercent = normalizeShopMarkupPercent(
        rawProduct?.markupPercent === undefined ? defaultMarkupPercent : rawProduct?.markupPercent
      );
      const finalPrice =
        rawProduct?.price !== undefined && rawProduct?.price !== null
          ? Math.max(0, toMoney(rawProduct.price))
          : calculateShopSalePrice(supplierPrice, markupPercent);

      await saveShopProduct(
        {
          categoryId: savedCategory.id,
          source,
          sourceKey,
          externalId,
          externalUrl,
          slug: productSlug,
          name: productName,
          shortDescription:
            rawProduct?.shortDescription ||
            rawProduct?.headline ||
            rawProduct?.subtitle ||
            "",
          description: rawProduct?.description || rawProduct?.descricao || "",
          imageUrl: rawProduct?.imageUrl || rawProduct?.image || rawProduct?.thumbnail || "",
          supplierPrice,
          markupPercent,
          price: finalPrice,
          currency: rawProduct?.currency || "BRL",
          active: rawProduct?.active !== false,
          rawPayload: rawProduct
        },
        client
      );

      importedProductKeys.add(sourceKey);
      importedProducts += 1;
    }
  }

  let deactivatedProducts = 0;
  if (deactivateMissing && importedProductKeys.size > 0) {
    const result = await client.query(
      `
      UPDATE shop_products
      SET is_active = false,
          updated_at = $3
      WHERE source = $1
        AND NOT (source_key = ANY($2::text[]))
        AND is_active = true
      `,
      [source, Array.from(importedProductKeys), db()]
    );
    deactivatedProducts = Number(result.rowCount || 0);
  }

  return {
    source,
    markupPercent: defaultMarkupPercent,
    categoriesImported: importedCategoryIds.size,
    productsImported: importedProducts,
    productsSkipped: skippedProducts,
    deactivatedProducts
  };
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
        tx.operationType === "welcome_bonus" ||
        tx.sourceType === "referral_bonus" ||
        tx.operationType === "referral_bonus"
      ) {
        saldoBonusAtual = toMoney(saldoBonusAtual + amount);
        continue;
      }

      if (tx.sourceType === "topup_order" && tx.operationType === "topup_refund") {
        const split = getBalanceSplitFromMetadata(tx.metadata, amount);

        if (split) {
          saldoBonusAtual = toMoney(saldoBonusAtual + split.bonusAmount);
          saldoRealAtual = toMoney(saldoRealAtual + split.realAmount);
          continue;
        }
      }

      if (tx.sourceType === "shop_order" && tx.operationType === "shop_refund") {
        const split = getBalanceSplitFromMetadata(tx.metadata, amount);

        if (split) {
          saldoBonusAtual = toMoney(saldoBonusAtual + split.bonusAmount);
          saldoRealAtual = toMoney(saldoRealAtual + split.realAmount);
          continue;
        }
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
  const indicacao = buildIndicacaoParticipacao(
    user,
    financialContext.valorRecebidoViaPix
  );

  return {
    id: user.id,
    nome: user.nome,
    email: user.email,
    saldo: toMoney(user.saldo),
    criadoEm: user.criadoEm || null,
    statusConta: normalizeStatusConta(user.statusConta),
    contaBanida: isContaBanida(user),
    contaBloqueada: isContaBloqueada(user),
    contaBanidaPermanente: isContaPermanentementeBanida(user),
    contaBanidaEm: user.contaBanidaEm || null,
    motivoBanimento: user.motivoBanimento || "",
    contaBloqueadaEm: user.contaBloqueadaEm || null,
    motivoBloqueio: user.motivoBloqueio || "",
    bloqueioTemporario: Boolean(user.bloqueioTemporario),
    bonusBoasVindas: toMoney(user.bonusBoasVindas),
    bonusBoasVindasConcedidoEm: user.bonusBoasVindasConcedidoEm || null,
    indicadoPorUserId: user.indicadoPorUserId || "",
    indicadoPorEmail: user.indicadoPorEmail || "",
    indicacaoVinculadaEm: user.indicacaoVinculadaEm || null,
    indicacaoQualificadaEm: user.indicacaoQualificadaEm || null,
    indicacaoBonusCreditadoEm: user.indicacaoBonusCreditadoEm || null,
    indicacaoBonusCreditadoValor: toMoney(user.indicacaoBonusCreditadoValor),
    indicacaoStatus: indicacao.status,
    indicacaoValorNecessario: indicacao.valorNecessario,
    investimosLiberado:
      toMoney(financialContext.valorRecebidoViaPix) >= INVESTIMENTOS_PIX_DESBLOQUEIO_MIN,
    investimosValorNecessario: INVESTIMENTOS_PIX_DESBLOQUEIO_MIN,
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

async function getInvestimentosEligibilityContext(userId, client = pool) {
  const valorRecebidoViaPix = await getValorRecebidoViaPix(userId, client);
  const faltaParaDesbloqueio = Math.max(
    0,
    toMoney(INVESTIMENTOS_PIX_DESBLOQUEIO_MIN - valorRecebidoViaPix)
  );

  return {
    valorRecebidoViaPix,
    valorNecessarioDesbloqueio: INVESTIMENTOS_PIX_DESBLOQUEIO_MIN,
    faltaParaDesbloqueio,
    investimosLiberado: valorRecebidoViaPix >= INVESTIMENTOS_PIX_DESBLOQUEIO_MIN
  };
}

async function getUserMonthlyMovementTotal(userId, referenceDate = new Date(), client = pool) {
  const result = await client.query(
    `
    SELECT COALESCE(SUM(amount), 0) AS total
    FROM financial_transactions
    WHERE user_id = $1
      AND status = 'completed'
      AND created_at >= $2
      AND created_at < $3
      AND source_type NOT IN ('welcome_bonus', 'referral_bonus', 'admin_adjustment', 'investment_reserve')
      AND operation_type NOT IN ('manual_balance_adjustment', 'welcome_bonus', 'referral_bonus')
    `,
    [userId, getStartOfMonthIso(referenceDate), getStartOfNextMonthIso(referenceDate)]
  );

  return toMoney(result.rows[0]?.total);
}

function getInvestmentReserveCapacityBase(reserve, productConfig = null) {
  const config = productConfig || getInvestmentProductConfig(reserve?.productKey);
  if (Number.isFinite(Number(reserve?.capacityLimit)) && Number(reserve?.capacityLimit) > 0) {
    return toMoney(reserve.capacityLimit);
  }
  if (config?.maxAmount !== null && config?.maxAmount !== undefined) {
    return toMoney(config.maxAmount);
  }
  return toMoney(config?.minDisplayCapacity || config?.minAmount || reserve?.principalInvestedTotal || 0);
}

function buildInvestmentReserveResponse(
  reserve,
  {
    now = new Date(),
    currentMonthMovement = 0
  } = {}
) {
  const config = getInvestmentProductConfig(reserve?.productKey);
  if (!reserve || !config) return null;

  const nowDate = now instanceof Date ? now : new Date(now || Date.now());
  const principalRemaining = toMoney(reserve.principalRemaining);
  const principalInvestedTotal = toMoney(reserve.principalInvestedTotal);
  const createdAt = reserve.createdAt || null;
  const releaseAt = reserve.releaseAt || null;
  const profitEligibleAt = reserve.profitEligibleAt || null;
  const holdDays = diffDaysFloor(createdAt, nowDate);
  const isClosed = getInvestmentReserveStatus(reserve.status) === INVESTMENT_RESERVE_STATUS_CLOSED || principalRemaining <= 0;
  const releaseReached = !releaseAt || new Date(releaseAt).getTime() <= nowDate.getTime();
  const movementRequired = toMoney(reserve.movementRequiredPerMonth || config.movementRequiredPerMonth || 0);
  const movementCurrent = toMoney(currentMonthMovement);
  const missingMovement = Math.max(0, toMoney(movementRequired - movementCurrent));
  const juniorMovementOk = movementRequired <= 0 || movementCurrent >= movementRequired;
  const profitEligibleByTime =
    !profitEligibleAt || new Date(profitEligibleAt).getTime() <= nowDate.getTime();
  const canWithdrawPrincipal =
    !isClosed &&
    (config.withdrawLock ? releaseReached : principalRemaining > 0);
  const profitActiveNow =
    !isClosed &&
    principalRemaining > 0 &&
    profitEligibleByTime &&
    (!config.withdrawLock ? juniorMovementOk : releaseReached);
  const projectedDaysNow = config.withdrawLock
    ? diffDaysFloor(createdAt, releaseAt || nowDate)
    : holdDays;
  const projectedProfitNow = profitActiveNow
    ? calculateInvestmentProjectedProfit(principalRemaining, reserve.annualRate, projectedDaysNow)
    : 0;
  const projectedProfitOnRelease = calculateInvestmentProjectedProfit(
    principalRemaining,
    reserve.annualRate,
    config.withdrawLock
      ? diffDaysFloor(createdAt, releaseAt || nowDate)
      : Math.max(projectedDaysNow, diffDaysFloor(createdAt, profitEligibleAt || nowDate))
  );
  const capacityBase = getInvestmentReserveCapacityBase(reserve, config);
  const capacityPercent = capacityBase > 0
    ? Math.min(100, Number(((principalRemaining / capacityBase) * 100).toFixed(2)))
    : 0;
  const remainingCapacity = Math.max(0, toMoney(capacityBase - principalRemaining));
  const daysUntilRelease = releaseAt
    ? Math.max(0, diffDaysFloor(nowDate, releaseAt))
    : 0;
  const daysUntilProfitEligible = profitEligibleAt
    ? Math.max(0, diffDaysFloor(nowDate, profitEligibleAt))
    : 0;
  const productStatus = isClosed
    ? "encerrada"
    : config.withdrawLock
      ? releaseReached
        ? "liberada"
        : "travada"
      : profitActiveNow
        ? "rendendo"
        : "aguardando_meta";

  return {
    id: reserve.id,
    productKey: config.key,
    productName: reserve.productName || config.name,
    productHeadline: reserve.productHeadline || config.headline,
    status: productStatus,
    principalInvestedTotal,
    principalRemaining,
    principalWithdrawableNow: canWithdrawPrincipal ? principalRemaining : 0,
    profitPaidTotal: toMoney(reserve.profitPaidTotal),
    annualRatePercent: Number((Number(reserve.annualRate || 0) * 100).toFixed(2)),
    cdiMultiplier: Number(reserve.cdiMultiplier || config.cdiMultiplier || 0),
    capacityBase,
    remainingCapacity,
    capacityPercent,
    releaseAt,
    profitEligibleAt,
    createdAt,
    lastWithdrawnAt: reserve.lastWithdrawnAt || null,
    closedAt: reserve.closedAt || null,
    holdDays,
    daysUntilRelease,
    daysUntilProfitEligible,
    canWithdraw: Boolean(canWithdrawPrincipal),
    allowPartialWithdraw: Boolean(reserve.allowPartialWithdraw),
    withdrawLock: Boolean(config.withdrawLock),
    movementRequiredPerMonth: movementRequired,
    currentMonthMovement: movementCurrent,
    missingMovementThisMonth: missingMovement,
    juniorMovementOk,
    profitEligibleByTime,
    profitActiveNow,
    estimatedProfitNow: projectedProfitNow,
    estimatedProfitOnRelease: projectedProfitOnRelease
  };
}

function buildInvestmentSummary(reserves, eligibilityContext, currentMonthMovement) {
  const list = Array.isArray(reserves) ? reserves : [];
  const totalPrincipal = toMoney(
    list.reduce((sum, item) => sum + toMoney(item?.principalRemaining), 0)
  );
  const totalProfitPaid = toMoney(
    list.reduce((sum, item) => sum + toMoney(item?.profitPaidTotal), 0)
  );
  const withdrawableNow = toMoney(
    list.reduce((sum, item) => sum + toMoney(item?.canWithdraw ? item.principalWithdrawableNow : 0), 0)
  );
  const nextRelease = list
    .filter((item) => item?.releaseAt && item?.status !== "encerrada")
    .sort((a, b) => new Date(a.releaseAt).getTime() - new Date(b.releaseAt).getTime())[0];

  return {
    totalPrincipal,
    totalProfitPaid,
    activeReserveCount: list.filter((item) => item?.status !== "encerrada").length,
    withdrawableNow,
    nextReleaseAt: nextRelease?.releaseAt || null,
    currentMonthMovement: toMoney(currentMonthMovement),
    juniorMovementRequiredPerMonth: INVESTIMENTOS_JUNIOR_MOVIMENTACAO_MENSAL_MIN,
    juniorMovementMissing: Math.max(
      0,
      toMoney(INVESTIMENTOS_JUNIOR_MOVIMENTACAO_MENSAL_MIN - toMoney(currentMonthMovement))
    ),
    investimosLiberado: Boolean(eligibilityContext?.investimosLiberado),
    valorRecebidoViaPix: toMoney(eligibilityContext?.valorRecebidoViaPix),
    valorNecessarioDesbloqueio: toMoney(eligibilityContext?.valorNecessarioDesbloqueio),
    faltaParaDesbloqueio: toMoney(eligibilityContext?.faltaParaDesbloqueio)
  };
}

async function buildInvestimentosDashboardResponse(user, client = pool) {
  const eligibilityContext = await getInvestimentosEligibilityContext(user.id, client);
  const currentMonthMovement = await getUserMonthlyMovementTotal(user.id, new Date(), client);
  const [reserveRows, currentLimitRequest] = await Promise.all([
    listInvestmentReservesByUserId(user.id, client),
    getOpenMovementLimitRequestByUserId(user.id, client)
  ]);

  const reserves = reserveRows.map((reserve) =>
    buildInvestmentReserveResponse(reserve, {
      now: new Date(),
      currentMonthMovement
    })
  );

  return {
    eligibility: eligibilityContext,
    currentMonthMovement,
    products: Object.values(INVESTMENT_PRODUCT_DEFINITIONS).map(
      buildInvestmentProductPublicConfig
    ),
    summary: buildInvestmentSummary(reserves, eligibilityContext, currentMonthMovement),
    reserves,
    currentLimitRequest: currentLimitRequest
      ? {
          id: currentLimitRequest.id,
          requestedAmount: toMoney(currentLimitRequest.requestedAmount),
          status: currentLimitRequest.status,
          adminMessage: currentLimitRequest.adminMessage || "",
          pixKey: currentLimitRequest.pixKey || "",
          createdAt: currentLimitRequest.createdAt || null,
          respondedAt: currentLimitRequest.respondedAt || null
        }
      : null
  };
}

function buildMovementLimitPixKeyNotification({
  userId,
  requestId,
  requestedAmount,
  pixKey
}) {
  return {
    id: buildId("notif"),
    userId,
    type: USER_NOTIFICATION_TYPE_LIMIT_REQUEST_PIX_KEY,
    title: "Limite especial em análise",
    body: "Recebemos sua solicitação, envie essa chave pix para o pagador.",
    metadata: {
      requestId,
      requestedAmount: toMoney(requestedAmount),
      pixKey: String(pixKey || "").trim(),
      copyLabel: "Copiar chave Pix"
    },
    readAt: null,
    createdAt: db()
  };
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
          AND source_type NOT IN ('welcome_bonus', 'referral_bonus', 'transfer')
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
          AND source_type NOT IN ('welcome_bonus', 'referral_bonus', 'transfer')
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

function buildIndicacaoListItem(user, qualifyingPixTotal = 0) {
  return {
    userId: user.id,
    nome: user.nome || user.email?.split("@")[0] || "",
    email: user.email || "",
    criadoEm: user.criadoEm || null,
    status: getIndicacaoStatus(user, qualifyingPixTotal),
    qualifyingPixTotal: toMoney(qualifyingPixTotal),
    valorNecessario: INDICACAO_PIX_QUALIFICACAO_MIN,
    bonusValor: BONUS_INDICACAO_VALOR,
    vinculadaEm: user.indicacaoVinculadaEm || null,
    qualificadaEm: user.indicacaoQualificadaEm || null,
    bonusCreditadoEm: user.indicacaoBonusCreditadoEm || null,
    bonusCreditadoValor: toMoney(user.indicacaoBonusCreditadoValor)
  };
}

function buildIndicacoesResumo(items = []) {
  const total = Array.isArray(items) ? items.length : 0;
  const concluidos = (Array.isArray(items) ? items : []).filter(
    (item) => item?.status === "concluido"
  ).length;
  const pendentes = Math.max(0, total - concluidos);

  return {
    total,
    pendentes,
    concluidos,
    bonusTotalLiberado: toMoney(concluidos * BONUS_INDICACAO_VALOR)
  };
}

async function listIndicacoesPorIndicadorIds(indicadorIds, client = pool) {
  const ids = Array.from(
    new Set(
      (Array.isArray(indicadorIds) ? indicadorIds : [])
        .map((id) => String(id || "").trim())
        .filter(Boolean)
    )
  );

  const indicacoesMap = new Map(ids.map((id) => [id, []]));

  if (!ids.length) {
    return indicacoesMap;
  }

  const result = await client.query(
    `
    SELECT *
    FROM usuarios
    WHERE indicado_por_user_id = ANY($1::text[])
    ORDER BY criado_em DESC NULLS LAST, id ASC
    `,
    [ids]
  );

  const indicados = result.rows.map(mapUser);
  const resumoMap = await getResumoFinanceiroUsuarios(
    indicados.map((user) => user.id),
    client
  );

  for (const indicado of indicados) {
    const qualifyingPixTotal = toMoney(
      resumoMap.get(indicado.id)?.qualifyingPixTotal
    );
    if (!indicacoesMap.has(indicado.indicadoPorUserId)) {
      indicacoesMap.set(indicado.indicadoPorUserId, []);
    }
    indicacoesMap
      .get(indicado.indicadoPorUserId)
      .push(buildIndicacaoListItem(indicado, qualifyingPixTotal));
  }

  return indicacoesMap;
}

async function buildIndicacoesProgramaResposta(user, client = pool) {
  const ensuredUser = await ensureUserReferralCode(user, client);
  const [referrer, indicacoesMap, qualifyingPixTotal] = await Promise.all([
    ensuredUser?.indicadoPorUserId ? getUserById(ensuredUser.indicadoPorUserId, client) : null,
    listIndicacoesPorIndicadorIds([ensuredUser.id], client),
    getValorRecebidoViaPix(ensuredUser.id, client)
  ]);

  const meusIndicados = indicacoesMap.get(ensuredUser.id) || [];

  return {
    meuEmailIndicacao: ensuredUser.email,
    meuCodigoIndicacao: normalizeReferralCode(ensuredUser.referralCode),
    meuLinkIndicacao: buildReferralLinkFromCode(ensuredUser.referralCode),
    bonusValor: BONUS_INDICACAO_VALOR,
    valorNecessarioDepositos: INDICACAO_PIX_QUALIFICACAO_MIN,
    participacao: buildIndicacaoParticipacao(ensuredUser, qualifyingPixTotal, referrer),
    resumo: buildIndicacoesResumo(meusIndicados),
    meusIndicados
  };
}

async function aplicarBonusIndicacaoSeElegivel(userId, client = pool) {
  const indicado =
    client === pool
      ? await getUserById(userId, client)
      : await getUserByIdForUpdate(userId, client);

  if (!indicado?.id || !indicado.indicadoPorUserId) {
    return { creditado: false, motivo: "sem_indicacao" };
  }

  if (indicado.indicacaoBonusCreditadoEm) {
    return { creditado: false, motivo: "bonus_ja_creditado" };
  }

  const qualifyingPixTotal = await getValorRecebidoViaPix(indicado.id, client);

  if (qualifyingPixTotal < INDICACAO_PIX_QUALIFICACAO_MIN) {
    return {
      creditado: false,
      motivo: "meta_nao_atingida",
      qualifyingPixTotal
    };
  }

  if (!indicado.indicacaoQualificadaEm) {
    indicado.indicacaoQualificadaEm = db();
  }

  const indicador =
    client === pool
      ? await getUserById(indicado.indicadoPorUserId, client)
      : await getUserByIdForUpdate(indicado.indicadoPorUserId, client);

  if (!indicador?.id) {
    await saveUser(indicado, client);
    return {
      creditado: false,
      motivo: "indicador_nao_encontrado",
      qualifyingPixTotal
    };
  }

  const descricaoBonus = `Bonus por indicacao liberado por ${indicado.email}`;
  const bonusTx = await createFinancialTransaction(client, {
    userId: indicador.id,
    referenceKey: `indicacao:${indicado.id}:bonus`,
    sourceType: "referral_bonus",
    sourceId: indicado.id,
    operationType: "referral_bonus",
    direction: "credit",
    amount: BONUS_INDICACAO_VALOR,
    status: "completed",
    description: descricaoBonus,
    metadata: {
      tipoBonus: "indicacao",
      indicadoUserId: indicado.id,
      indicadoEmail: indicado.email,
      qualifyingPixTotal
    }
  });

  const indicadorAtualizado = await applyLedgerChange(client, {
    userId: indicador.id,
    financialTransactionId: bonusTx.id,
    entryType: "credit",
    amount: BONUS_INDICACAO_VALOR,
    description: descricaoBonus,
    metadata: {
      tipoBonus: "indicacao",
      indicadoUserId: indicado.id,
      indicadoEmail: indicado.email
    }
  });

  indicado.indicacaoBonusCreditadoEm = db();
  indicado.indicacaoBonusCreditadoValor = BONUS_INDICACAO_VALOR;
  indicado.indicacaoBonusTransacaoId = bonusTx.id;

  await saveUser(indicado, client);

  return {
    creditado: true,
    indicador: indicadorAtualizado,
    indicado,
    qualifyingPixTotal
  };
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
  user.contaBloqueadaEm = null;
  user.motivoBloqueio = "";
  user.bloqueioTemporario = false;

  await saveUser(user, client);

  return user;
}

async function atualizarBloqueioManualConta(
  userId,
  { bloqueada, motivoBloqueio = "", bloqueioTemporario = false } = {},
  client = pool
) {
  const user =
    client === pool ? await getUserById(userId, client) : await getUserByIdForUpdate(userId, client);

  if (!user) {
    throw new Error("Usuario nao encontrado");
  }

  if (!bloqueada) {
    if (isContaPermanentementeBanida(user)) {
      throw new Error("Conta banida permanentemente nao pode ser desbloqueada por este painel.");
    }

    user.statusConta = STATUS_CONTA_ATIVA;
    user.contaBloqueadaEm = null;
    user.motivoBloqueio = "";
    user.bloqueioTemporario = false;
    await saveUser(user, client);
    return user;
  }

  if (isContaPermanentementeBanida(user)) {
    throw new Error("Conta ja esta banida permanentemente.");
  }

  const motivoNormalizado = normalizeAccountRestrictionReason(motivoBloqueio);

  if (!motivoNormalizado) {
    throw new Error("Informe o motivo do bloqueio.");
  }

  user.statusConta = STATUS_CONTA_BLOQUEADA;
  user.contaBloqueadaEm = db();
  user.motivoBloqueio = motivoNormalizado;
  user.bloqueioTemporario = Boolean(bloqueioTemporario);
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
  const now = db();
  const result = await client.query(
    `
    INSERT INTO financial_transactions (
      id, user_id, reference_key, source_type, source_id,
      operation_type, direction, amount, status, description, metadata,
      created_at, updated_at
    )
    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)
    ON CONFLICT (reference_key) DO UPDATE SET
      reference_key = financial_transactions.reference_key
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

function getAuthenticatedUserId(req) {
  return String(req.userAuth?.sub || "").trim();
}

function isAuthenticatedUserMismatch(authenticatedUserId, candidateUserId) {
  const candidate = String(candidateUserId || "").trim();
  return Boolean(candidate) && candidate !== String(authenticatedUserId || "").trim();
}

app.get("/", (req, res) => {
  res.json({ ok: true });
});

app.get("/public/topup-config", async (req, res) => {
  res.json({
    operadoras: RECARGA_CELULAR_OPERADORAS,
    taxaPercentual: TAXA_RECARGA_CELULAR_PERCENTUAL,
    valorMinimo: LIMITE_RECARGA_CELULAR_MIN,
    valorMaximo: LIMITE_RECARGA_CELULAR_MAX
  });
});

app.get("/public/shop/catalog", publicShopCatalogLimiter, async (req, res) => {
  try {
    const categorySlug = normalizeShopText(req.query.category || "", 120).toLowerCase();
    const search = normalizeShopText(req.query.search || "", 180);
    const includeGrouped =
      String(req.query.grouped || "").trim() === "1" ||
      String(req.query.includeGrouped || "").trim() === "1";
    let { snapshot, cacheStatus } = await getShopPublicCatalogSnapshotCached();
    let recoverySource = "none";

    if (!hasShopSnapshotProducts(snapshot)) {
      const recovered = await recoverShopCatalogIfEmpty("public_catalog_request");

      if (recovered?.snapshot && hasShopSnapshotProducts(recovered.snapshot)) {
        snapshot = recovered.snapshot;
        cacheStatus = "recovered";
        recoverySource = recovered.source || "unknown";
      }
    }

    const payload = buildPublicShopCatalogPayload(snapshot, {
      categorySlug,
      search,
      includeGrouped
    });

    res.set(
      "Cache-Control",
      `public, max-age=${SHOP_PUBLIC_CATALOG_MAX_AGE_SECONDS}, stale-while-revalidate=${SHOP_PUBLIC_CATALOG_STALE_WHILE_REVALIDATE_SECONDS}`
    );
    res.set("X-Shop-Catalog-Cache", cacheStatus);
    res.set("X-Shop-Catalog-Recovery", recoverySource);
    res.json(payload);
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: "Erro ao carregar catalogo da shop" });
  }
});

app.get("/public/app-status", async (req, res) => {
  try {
    const settings = await getAppRuntimeSettings();
    res.set("Cache-Control", "no-store");
    res.json({
      maintenance: {
        enabled: settings.maintenanceEnabled,
        message: settings.maintenanceMessage,
        etaMinutes: settings.maintenanceEtaMinutes,
        updatedAt: settings.updatedAt
      }
    });
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: "Erro ao carregar status do app" });
  }
});

app.get("/public/banner-config", async (req, res) => {
  try {
    const [settings, banners] = await Promise.all([
      getBannerSettings(),
      listBannerAssets(pool, { activeOnly: true })
    ]);

    res.set("Cache-Control", "no-store");
    res.json({
      rotationMs: settings.rotationMs,
      banners
    });
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: "Erro ao carregar banners" });
  }
});

app.get("/public/banner-images/:id", async (req, res) => {
  try {
    const banner = await getBannerAssetBinary(req.params.id);

    if (!banner) {
      return res.status(404).send("Banner nao encontrado");
    }

    if (banner.updatedAt) {
      res.set("Last-Modified", new Date(banner.updatedAt).toUTCString());
    }

    res.set("Cache-Control", "public, max-age=300");
    res.set("Cross-Origin-Resource-Policy", "cross-origin");
    res.type(banner.mimeType);
    res.send(banner.imageData);
  } catch (error) {
    console.error(error);
    res.status(500).send("Erro ao carregar imagem");
  }
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

app.get("/admin/banner-config", authAdmin, async (req, res) => {
  try {
    const [settings, banners] = await Promise.all([
      getBannerSettings(),
      listBannerAssets()
    ]);

    res.set("Cache-Control", "no-store");
    res.json({
      rotationMs: settings.rotationMs,
      banners
    });
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: "Erro ao carregar configuracao de banners" });
  }
});

app.get("/admin/app-status", authAdmin, async (req, res) => {
  try {
    const settings = await getAppRuntimeSettings();
    res.set("Cache-Control", "no-store");
    res.json(settings);
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: "Erro ao carregar status do app" });
  }
});

app.post("/admin/app-status", authAdmin, async (req, res) => {
  try {
    const maintenanceEnabled = req.body?.maintenanceEnabled === true;
    const maintenanceEtaMinutes = req.body?.maintenanceEtaMinutes;

    const settings = await runInTransaction(async (client) => {
      const updated = await setAppRuntimeSettings(
        {
          maintenanceEnabled,
          maintenanceEtaMinutes,
          maintenanceMessage: APP_MAINTENANCE_MESSAGE_DEFAULT
        },
        client
      );

      await createAuditLog(client, {
        adminId: req.admin.sub,
        action: "update_app_runtime_status",
        targetType: "app_runtime_settings",
        targetId: APP_RUNTIME_SETTINGS_ID,
        details: {
          maintenanceEnabled: updated.maintenanceEnabled,
          maintenanceEtaMinutes: updated.maintenanceEtaMinutes
        },
        ipAddress: getRequestIp(req)
      });

      return updated;
    });

    res.json({
      message: settings.maintenanceEnabled
        ? "Aviso de atualizacao ativado"
        : "Aviso de atualizacao desativado",
      settings
    });
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: "Erro ao atualizar status do app" });
  }
});

app.get("/admin/shop/catalog", authAdmin, async (req, res) => {
  try {
    const [categories, products] = await Promise.all([
      listShopCategories(),
      listShopProducts()
    ]);

    res.set("Cache-Control", "no-store");
    res.json({
      stats: {
        categories: categories.length,
        activeCategories: categories.filter((item) => item.active).length,
        products: products.length,
        activeProducts: products.filter((item) => item.active).length
      },
      categories,
      products
    });
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: "Erro ao carregar catalogo da shop" });
  }
});

app.post("/admin/shop/catalog/import", authAdmin, async (req, res) => {
  try {
    const payload = req.body || {};
    const summary = await runInTransaction(async (client) => {
      const imported = await importShopCatalog(payload, client);

      await createAuditLog(client, {
        adminId: req.admin.sub,
        action: "import_shop_catalog",
        targetType: "shop_catalog",
        targetId: imported.source,
        details: imported,
        ipAddress: getRequestIp(req)
      });

      return imported;
    });
    invalidateShopPublicCatalogSnapshotCache();

    res.json({
      message: "Catalogo da shop importado com sucesso",
      summary
    });
  } catch (error) {
    console.error(error);
    res.status(400).json({ error: error.message || "Erro ao importar catalogo da shop" });
  }
});

app.post("/admin/shop/catalog/import-kaiross", authAdmin, async (req, res) => {
  try {
    const markupPercent =
      req.body?.markupPercent === undefined
        ? SHOP_DEFAULT_MARKUP_PERCENTUAL
        : req.body.markupPercent;
    const deactivateMissing = req.body?.deactivateMissing === true;
    const provider = await fetchKairossProducts({
      email: req.body?.email,
      senha: req.body?.senha,
      baseUrl: req.body?.baseUrl
    });
    const payload = buildKairossShopImportPayload(provider.products, {
      source: SHOP_PRODUCT_SOURCE_DEFAULT,
      markupPercent,
      baseUrl: provider.baseUrl
    });
    payload.deactivateMissing = deactivateMissing;
    const kairossCategoriesFetched = new Set(
      provider.products
        .map((item) => normalizeShopCatalogCategoryName(item?.categoria, 120))
        .filter(Boolean)
    ).size;

    const summary = await runInTransaction(async (client) => {
      const imported = await importShopCatalog(payload, client);
      const details = {
        ...imported,
        kairossProductsFetched: provider.products.length,
        kairossCategoriesFetched,
        kairossUserId: provider.user?.id || null,
        baseUrl: provider.baseUrl
      };

      await createAuditLog(client, {
        adminId: req.admin.sub,
        action: "import_shop_catalog_kaiross",
        targetType: "shop_catalog",
        targetId: imported.source,
        details,
        ipAddress: getRequestIp(req)
      });

      return details;
    });
    invalidateShopPublicCatalogSnapshotCache();

    res.json({
      message: "Catalogo da Kaiross importado com sucesso",
      summary
    });
  } catch (error) {
    console.error(error);
    res.status(400).json({ error: error.message || "Erro ao importar catalogo da Kaiross" });
  }
});

app.post("/admin/shop/products/:id", authAdmin, async (req, res) => {
  try {
    const productId = String(req.params.id || "").trim();

    if (!productId) {
      return res.status(400).json({ error: "Produto obrigatorio" });
    }

    const result = await runInTransaction(async (client) => {
      const current = await getShopProductById(productId, client);

      if (!current) {
        throw new Error("Produto da shop nao encontrado");
      }

      const nextCategoryId = String(req.body?.categoryId || current.categoryId || "").trim();

      if (!nextCategoryId) {
        throw new Error("Categoria do produto obrigatoria");
      }

      const categoryExists = await client.query(
        "SELECT id FROM shop_categories WHERE id = $1 LIMIT 1",
        [nextCategoryId]
      );

      if (!categoryExists.rows.length) {
        throw new Error("Categoria da shop nao encontrada");
      }

      const updated = await saveShopProduct(
        {
          id: current.id,
          categoryId: nextCategoryId,
          sourceKey: current.sourceKey,
          source: current.source,
          externalId:
            req.body?.externalId !== undefined ? req.body.externalId : current.externalId,
          externalUrl:
            req.body?.externalUrl !== undefined ? req.body.externalUrl : current.externalUrl,
          slug: req.body?.slug !== undefined ? req.body.slug : current.slug,
          name: req.body?.name !== undefined ? req.body.name : current.name,
          shortDescription:
            req.body?.shortDescription !== undefined
              ? req.body.shortDescription
              : current.shortDescription,
          description:
            req.body?.description !== undefined ? req.body.description : current.description,
          imageUrl: req.body?.imageUrl !== undefined ? req.body.imageUrl : current.imageUrl,
          supplierPrice:
            req.body?.supplierPrice !== undefined
              ? req.body.supplierPrice
              : current.supplierPrice,
          markupPercent:
            req.body?.markupPercent !== undefined
              ? req.body.markupPercent
              : current.markupPercent,
          price: req.body?.price !== undefined ? req.body.price : current.price,
          currency: req.body?.currency !== undefined ? req.body.currency : current.currency,
          active: req.body?.active !== undefined ? req.body.active === true : current.active,
          rawPayload: current.rawPayload || {}
        },
        client
      );

      await createAuditLog(client, {
        adminId: req.admin.sub,
        action: "update_shop_product",
        targetType: "shop_product",
        targetId: updated.id,
        details: {
          categoryId: updated.categoryId,
          supplierPrice: updated.supplierPrice,
          markupPercent: updated.markupPercent,
          price: updated.price,
          active: updated.active
        },
        ipAddress: getRequestIp(req)
      });

      return await getShopProductById(updated.id, client);
    });
    invalidateShopPublicCatalogSnapshotCache();

    res.json({
      message: "Produto da shop atualizado com sucesso",
      product: result
    });
  } catch (error) {
    console.error(error);
    res.status(400).json({ error: error.message || "Erro ao atualizar produto da shop" });
  }
});

app.post(
  "/admin/banner-images/upload",
  authAdmin,
  bannerUpload.array("banners", 12),
  async (req, res) => {
    try {
      const files = Array.isArray(req.files) ? req.files : [];

      if (!files.length) {
        return res.status(400).json({ error: "Nenhuma imagem enviada" });
      }

      const result = await runInTransaction(async (client) => {
        const created = await createBannerAssets(files, client);

        await createAuditLog(client, {
          adminId: req.admin.sub,
          action: "upload_banner_images",
          targetType: "banner",
          targetId: created.map((item) => item.id).join(","),
          details: {
            total: created.length
          },
          ipAddress: getRequestIp(req)
        });

        return created;
      });

      const [settings, banners] = await Promise.all([
        getBannerSettings(),
        listBannerAssets()
      ]);

      res.json({
        message: "Banners enviados com sucesso",
        uploaded: result,
        rotationMs: settings.rotationMs,
        banners
      });
    } catch (error) {
      console.error(error);
      res.status(500).json({ error: "Erro ao enviar banners" });
    }
  }
);

app.post("/admin/banner-images/delete", authAdmin, async (req, res) => {
  try {
    const bannerId = String(req.body?.id || "").trim();

    if (!bannerId) {
      return res.status(400).json({ error: "id obrigatorio" });
    }

    const deleted = await runInTransaction(async (client) => {
      const removed = await deleteBannerAsset(bannerId, client);

      if (!removed) {
        throw new Error("Banner nao encontrado");
      }

      await createAuditLog(client, {
        adminId: req.admin.sub,
        action: "delete_banner_image",
        targetType: "banner",
        targetId: bannerId,
        details: {},
        ipAddress: getRequestIp(req)
      });

      return removed;
    });

    if (!deleted) {
      return res.status(404).json({ error: "Banner nao encontrado" });
    }

    const [settings, banners] = await Promise.all([
      getBannerSettings(),
      listBannerAssets()
    ]);

    res.json({
      message: "Banner removido com sucesso",
      rotationMs: settings.rotationMs,
      banners
    });
  } catch (error) {
    console.error(error);
    res.status(400).json({
      error: error.message || "Erro ao remover banner"
    });
  }
});

app.post("/admin/banner-config", authAdmin, async (req, res) => {
  try {
    const banners = Array.isArray(req.body?.banners) ? req.body.banners : null;

    if (!banners) {
      return res.status(400).json({ error: "banners obrigatorio" });
    }

    const rotationMs = clampBannerRotationMs(req.body?.rotationMs);

    await runInTransaction(async (client) => {
      await updateBannerAssetMetadata(banners, client);
      await setBannerSettings(rotationMs, client);

      await createAuditLog(client, {
        adminId: req.admin.sub,
        action: "update_banner_config",
        targetType: "banner_config",
        targetId: BANNER_SETTINGS_ID,
        details: {
          total: banners.length,
          rotationMs
        },
        ipAddress: getRequestIp(req)
      });
    });

    const [settings, nextBanners] = await Promise.all([
      getBannerSettings(),
      listBannerAssets()
    ]);

    res.json({
      message: "Configuracao de banners atualizada",
      rotationMs: settings.rotationMs,
      banners: nextBanners
    });
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: "Erro ao salvar configuracao de banners" });
  }
});

app.post("/register", async (req, res) => {
  try {
    const {
      email,
      senha,
      veioPorIndicacao,
      emailIndicador,
      codigoIndicador
    } = req.body;

    if (!email || !senha) {
      return res.status(400).json({ error: "Email e senha sao obrigatorios" });
    }

    const emailNorm = normalizeEmail(email);
    const codigoIndicadorNorm = normalizeReferralCode(codigoIndicador);
    const usarIndicacao =
      veioPorIndicacao === true ||
      Boolean(String(emailIndicador || "").trim()) ||
      Boolean(codigoIndicadorNorm);
    const emailIndicadorNorm = usarIndicacao
      ? normalizeEmail(emailIndicador)
      : "";

    if (usarIndicacao && !codigoIndicadorNorm && !emailIndicadorNorm) {
      return res.status(400).json({
        error: "Informe o email do indicador ou abra a conta pelo link de indicacao"
      });
    }

    if (usarIndicacao && !codigoIndicadorNorm && emailIndicadorNorm === emailNorm) {
      return res.status(400).json({ error: "Voce nao pode indicar a propria conta" });
    }

    const exists = await pool.query(
      "SELECT id FROM usuarios WHERE email = $1 LIMIT 1",
      [emailNorm]
    );

    if (exists.rows.length > 0) {
      return res.status(400).json({ error: "Usuario ja existe" });
    }

    const indicador = codigoIndicadorNorm
      ? await getUserByReferralCode(codigoIndicadorNorm)
      : usarIndicacao
        ? await getUserByEmail(emailIndicadorNorm)
        : null;

    if (usarIndicacao && !indicador) {
      return res.status(400).json({
        error: codigoIndicadorNorm
          ? "Link de indicacao invalido"
          : "Email do indicador nao encontrado"
      });
    }

    if (indicador && normalizeEmail(indicador.email) === emailNorm) {
      return res.status(400).json({ error: "Voce nao pode indicar a propria conta" });
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
        contaBloqueadaEm: null,
        motivoBloqueio: "",
        bloqueioTemporario: false,
        bonusBoasVindas: 0,
        bonusBoasVindasConcedidoEm: null,
        indicadoPorUserId: indicador?.id || "",
        indicadoPorEmail: indicador?.email || "",
        indicacaoVinculadaEm: indicador ? db() : null,
        indicacaoQualificadaEm: null,
        indicacaoBonusCreditadoEm: null,
        indicacaoBonusCreditadoValor: 0,
        indicacaoBonusTransacaoId: "",
        referralCode: ""
      };

      await ensureUserReferralCode(user, client);
      return user;
    });

    const token = signUserToken(novoUsuario);
    const indicacao = buildIndicacaoParticipacao(novoUsuario, 0, indicador);

    res.status(201).json(
      attachUserAuthToPayload(
        buildUserPublicResponse(novoUsuario, {
          pixDesbloqueado: false,
          valorRecebidoViaPix: 0,
          valorMinimoDesbloqueioPix: PIX_SAQUE_DESBLOQUEIO_MIN,
          indicacao
        }),
        token
      )
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
      return res.status(400).json({ error: "Email e senha sao obrigatorios" });
    }

    const authUserRecord = await getUserByEmail(email);

    if (!authUserRecord) {
      return res.status(401).json({ error: "Login invalido" });
    }

    const senhaOk = await bcrypt.compare(String(senha), String(authUserRecord.senha));

    if (!senhaOk) {
      return res.status(401).json({ error: "Login invalido" });
    }

    const token = signUserToken(authUserRecord);
    const payload = await buildUserPublicResponseWithPix(authUserRecord, pool, {
      deviceId: getRequestDeviceId(req)
    });

    return res.json(attachUserAuthToPayload(payload, token));

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

app.get("/sigmo-cards", authUser, async (req, res) => {
  try {
    const ownerUserId = getAuthenticatedUserId(req);

    if (!ownerUserId) {
      return sendJsonError(res, 400, "CARD_OWNER_REQUIRED", "Usuario nao informado");
    }

    if (isAuthenticatedUserMismatch(ownerUserId, req.query?.userId)) {
      return sendJsonError(res, 403, "CARD_FORBIDDEN", "Acesso negado para estes cartoes");
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

app.post("/sigmo-cards/primary", authUser, async (req, res) => {
  try {
    const ownerUserId = getAuthenticatedUserId(req);
    const label = String(req.body?.label || "").trim();
    const spendingLimit = Math.max(0, toMoney(req.body?.spendingLimit));

    if (!ownerUserId) {
      return sendJsonError(res, 400, "CARD_OWNER_REQUIRED", "Usuario nao informado");
    }

    if (isAuthenticatedUserMismatch(ownerUserId, req.body?.userId)) {
      return sendJsonError(res, 403, "CARD_FORBIDDEN", "Acesso negado para este cartao");
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

app.post("/sigmo-cards/additional", authUser, async (req, res) => {
  try {
    const ownerUserId = getAuthenticatedUserId(req);
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

    if (isAuthenticatedUserMismatch(ownerUserId, req.body?.userId)) {
      return sendJsonError(res, 403, "CARD_FORBIDDEN", "Acesso negado para este cartao");
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

app.post("/sigmo-cards/:id", authUser, async (req, res) => {
  try {
    const ownerUserId = getAuthenticatedUserId(req);

    if (!ownerUserId) {
      return sendJsonError(res, 400, "CARD_OWNER_REQUIRED", "Usuario nao informado");
    }

    if (isAuthenticatedUserMismatch(ownerUserId, req.body?.userId)) {
      return sendJsonError(res, 403, "CARD_FORBIDDEN", "Acesso negado para este cartao");
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

app.post("/sigmo-cards/:id/reissue", authUser, async (req, res) => {
  try {
    const ownerUserId = getAuthenticatedUserId(req);

    if (!ownerUserId) {
      return sendJsonError(res, 400, "CARD_OWNER_REQUIRED", "Usuario nao informado");
    }

    if (isAuthenticatedUserMismatch(ownerUserId, req.body?.userId)) {
      return sendJsonError(res, 403, "CARD_FORBIDDEN", "Acesso negado para este cartao");
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

app.post("/sigmo-cards/:id/delete", authUser, async (req, res) => {
  try {
    const ownerUserId = getAuthenticatedUserId(req);

    if (!ownerUserId) {
      return sendJsonError(res, 400, "CARD_OWNER_REQUIRED", "Usuario nao informado");
    }

    if (isAuthenticatedUserMismatch(ownerUserId, req.body?.userId)) {
      return sendJsonError(res, 403, "CARD_FORBIDDEN", "Acesso negado para este cartao");
    }

    const result = await runInTransaction(async (client) => {
      const owner = await getUserByIdForUpdate(ownerUserId, client);
      const card = await getSigmoCardByIdForUpdate(req.params.id, client);

      if (!owner) {
        return { statusCode: 404, code: "USER_NOT_FOUND", error: "Usuario nao encontrado" };
      }

      if (isContaBanida(owner)) {
        return { statusCode: 403, payload: buildContaBanidaPayload(owner) };
      }

      if (!card || card.ownerUserId !== owner.id) {
        return { statusCode: 404, code: "CARD_NOT_FOUND", error: "Cartao nao encontrado" };
      }

      if (card.cardType === "primary") {
        return {
          statusCode: 400,
          code: "CARD_PRIMARY_DELETE_FORBIDDEN",
          error: "O cartao principal nao pode ser deletado"
        };
      }

      await deleteSigmoCardById(card.id, client);

      return {
        deletedCardId: card.id
      };
    });

    if (result?.statusCode) {
      if (result.payload) {
        return res.status(result.statusCode).json(result.payload);
      }
      return sendJsonError(res, result.statusCode, result.code, result.error);
    }

    res.json({
      ok: true,
      deletedCardId: result.deletedCardId
    });
  } catch (error) {
    console.error(error);
    sendJsonError(res, 500, "CARD_DELETE_ERROR", "Erro ao deletar cartao");
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

app.post("/mobile/cards/:id/delete", authUser, async (req, res) => {
  try {
    const deviceId = String(req.deviceId || "").trim();
    const cardId = String(req.params.id || "").trim();

    if (!cardId) {
      return sendJsonError(res, 400, "CARD_ID_REQUIRED", "Cartao nao informado");
    }

    const result = await runInTransaction(async (client) => {
      const holder = await getUserByIdForUpdate(req.userAuth.sub, client);
      const card = await getSigmoCardByIdForUpdate(cardId, client);

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
          error: "Este cartao nao pertence a esta conta"
        };
      }

      if (card.cardType === "primary") {
        return {
          statusCode: 400,
          code: "CARD_PRIMARY_DELETE_FORBIDDEN",
          error: "O cartao principal nao pode ser deletado neste app"
        };
      }

      if (deviceId && String(card.deviceId || "").trim() && String(card.deviceId || "").trim() !== deviceId) {
        return {
          statusCode: 409,
          code: "CARD_DEVICE_MISMATCH",
          error: "Este cartao esta liberado em outro aparelho"
        };
      }

      await deleteSigmoCardById(card.id, client);

      return {
        deletedCardId: card.id,
        user: await buildUserPublicResponseWithPix(holder, client, { deviceId })
      };
    });

    if (result?.statusCode) {
      if (result.payload) {
        return res.status(result.statusCode).json(result.payload);
      }
      return sendJsonError(res, result.statusCode, result.code, result.error);
    }

    res.json({
      ok: true,
      deletedCardId: result.deletedCardId,
      user: result.user
    });
  } catch (error) {
    console.error(error);
    sendJsonError(res, 500, "MOBILE_CARD_DELETE_ERROR", "Erro ao deletar cartao");
  }
});

app.post("/sigmo-tap-charges", authUser, async (req, res) => {
  try {
    const receiverUserId = getAuthenticatedUserId(req);
    const amount = toMoney(req.body?.amount);
    const description = String(req.body?.description || "").trim();

    if (!receiverUserId) {
      return sendJsonError(res, 400, "TAP_CHARGE_USER_REQUIRED", "Usuario nao informado");
    }

    if (
      isAuthenticatedUserMismatch(receiverUserId, req.body?.userId) ||
      isAuthenticatedUserMismatch(receiverUserId, req.body?.receiverUserId)
    ) {
      return sendJsonError(
        res,
        403,
        "TAP_CHARGE_FORBIDDEN",
        "Acesso negado para esta cobranca"
      );
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

app.get("/sigmo-tap-charges/:id", authUser, async (req, res) => {
  try {
    const userId = getAuthenticatedUserId(req);
    const charge = await getSigmoTapChargeById(req.params.id);

    if (!charge) {
      return sendJsonError(res, 404, "TAP_CHARGE_NOT_FOUND", "Cobranca nao encontrada");
    }

    if (isAuthenticatedUserMismatch(userId, req.query?.userId)) {
      return sendJsonError(res, 403, "TAP_CHARGE_FORBIDDEN", "Cobranca indisponivel");
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

app.post("/sigmo-tap-charges/:id/cancel", authUser, async (req, res) => {
  try {
    const userId = getAuthenticatedUserId(req);

    if (!userId) {
      return sendJsonError(res, 400, "TAP_CHARGE_USER_REQUIRED", "Usuario nao informado");
    }

    if (isAuthenticatedUserMismatch(userId, req.body?.userId)) {
      return sendJsonError(
        res,
        403,
        "TAP_CHARGE_FORBIDDEN",
        "Acesso negado para esta cobranca"
      );
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

app.get("/usuario/:id", authUser, async (req, res) => {
  try {
    const authenticatedUserId = getAuthenticatedUserId(req);
    const requestedUserId = String(req.params.id || "").trim();

    if (!authenticatedUserId || requestedUserId !== authenticatedUserId) {
      return sendJsonError(res, 403, "USER_FORBIDDEN", "Acesso negado para esta conta");
    }

    const user = await getUserById(authenticatedUserId);

    if (!user) {
      return res.status(404).json({ error: "Usuário não encontrado" });
    }

    res.json(await buildUserPublicResponseWithPix(user));
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: "Erro ao buscar usuário" });
  }
});

app.get("/usuario/:id/indicacoes", authUser, async (req, res) => {
  try {
    const authenticatedUserId = getAuthenticatedUserId(req);
    const requestedUserId = String(req.params.id || "").trim();

    if (!authenticatedUserId || requestedUserId !== authenticatedUserId) {
      return sendJsonError(
        res,
        403,
        "USER_FORBIDDEN",
        "Acesso negado para as indicacoes desta conta"
      );
    }

    const user = await getUserById(authenticatedUserId);

    if (!user) {
      return res.status(404).json({ error: "Usuário não encontrado" });
    }

    res.json(await buildIndicacoesProgramaResposta(user));
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: "Erro ao buscar indicacoes" });
  }
});

app.get("/usuario/:id/notificacoes", authUser, async (req, res) => {
  try {
    const authenticatedUserId = getAuthenticatedUserId(req);
    const requestedUserId = String(req.params.id || "").trim();

    if (!authenticatedUserId || requestedUserId !== authenticatedUserId) {
      return sendJsonError(
        res,
        403,
        "USER_FORBIDDEN",
        "Acesso negado para as notificacoes desta conta"
      );
    }

    const user = await getUserById(authenticatedUserId);

    if (!user) {
      return res.status(404).json({ error: "Usuário não encontrado" });
    }

    const notifications = await listUserNotificationsByUserId(user.id);
    res.json({
      notifications,
      unreadCount: notifications.filter((item) => !item.readAt).length
    });
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: "Erro ao buscar notificacoes" });
  }
});

app.post("/usuario/:id/notificacoes/marcar-lidas", authUser, async (req, res) => {
  try {
    const authenticatedUserId = getAuthenticatedUserId(req);
    const requestedUserId = String(req.params.id || "").trim();

    if (!authenticatedUserId || requestedUserId !== authenticatedUserId) {
      return sendJsonError(
        res,
        403,
        "USER_FORBIDDEN",
        "Acesso negado para esta conta"
      );
    }

    await runInTransaction(async (client) => {
      await markUserNotificationsAsRead(authenticatedUserId, client);
    });

    res.json({ message: "Notificacoes marcadas como lidas" });
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: "Erro ao atualizar notificacoes" });
  }
});

app.get("/usuario/:id/investimos", authUser, async (req, res) => {
  try {
    const authenticatedUserId = getAuthenticatedUserId(req);
    const requestedUserId = String(req.params.id || "").trim();

    if (!authenticatedUserId || requestedUserId !== authenticatedUserId) {
      return sendJsonError(
        res,
        403,
        "INVESTIMENTOS_FORBIDDEN",
        "Acesso negado para a area Investimos"
      );
    }

    const user = await getUserById(authenticatedUserId);

    if (!user) {
      return res.status(404).json({ error: "Usuário não encontrado" });
    }

    if (isContaBanida(user)) {
      return res.status(403).json(buildContaBanidaPayload(user));
    }

    res.json(await buildInvestimentosDashboardResponse(user));
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: "Erro ao carregar Investimos" });
  }
});

app.post("/usuario/:id/investimos/reservas", authUser, async (req, res) => {
  try {
    const authenticatedUserId = getAuthenticatedUserId(req);
    const requestedUserId = String(req.params.id || "").trim();
    const productKey = normalizeInvestmentProductKey(req.body?.productKey);
    const amount = toMoney(req.body?.amount);

    if (!authenticatedUserId || requestedUserId !== authenticatedUserId) {
      return sendJsonError(
        res,
        403,
        "INVESTIMENTOS_FORBIDDEN",
        "Acesso negado para esta conta"
      );
    }

    if (!productKey) {
      return res.status(400).json({ error: "Produto de investimento invalido" });
    }

    if (!Number.isFinite(amount) || amount <= 0) {
      return res.status(400).json({ error: "Informe um valor valido para investir" });
    }

    const result = await runInTransaction(async (client) => {
      const user = await getUserByIdForUpdate(authenticatedUserId, client);

      if (!user) {
        throw new Error("Usuário não encontrado");
      }

      if (isContaBanida(user)) {
        const error = new Error(getMensagemContaBanida());
        error.statusCode = 403;
        error.payload = buildContaBanidaPayload(user);
        throw error;
      }

      const eligibility = await getInvestimentosEligibilityContext(user.id, client);

      if (!eligibility.investimosLiberado) {
        const error = new Error(
          `A area Investimos sera liberada apos ${eligibility.valorNecessarioDesbloqueio.toFixed(2)} em depositos Pix aprovados`
        );
        error.statusCode = 403;
        error.payload = {
          error:
            `A area Investimos sera liberada apos R$${eligibility.valorNecessarioDesbloqueio.toFixed(2)} em depositos Pix aprovados`,
          eligibility
        };
        throw error;
      }

      const config = getInvestmentProductConfig(productKey);

      if (!config) {
        throw new Error("Produto de investimento invalido");
      }

      if (amount < toMoney(config.minAmount)) {
        throw new Error(
          `O aporte minimo para ${config.name} e de R$${toMoney(config.minAmount).toFixed(2)}`
        );
      }

      if (
        config.maxAmount !== null &&
        config.maxAmount !== undefined &&
        amount > toMoney(config.maxAmount)
      ) {
        throw new Error(
          `O valor maximo por estrutura ${config.name} e de R$${toMoney(config.maxAmount).toFixed(2)}`
        );
      }

      if (toMoney(user.saldo) < amount) {
        throw new Error("Saldo insuficiente");
      }

      const now = new Date();
      const releaseAt = config.lockMonths > 0 ? addMonthsToDate(now, config.lockMonths) : null;
      const profitEligibleAt =
        config.minHoldDaysForProfit > 0 ? addDaysToDate(now, config.minHoldDaysForProfit) : null;
      const reserve = {
        id: buildId("invest"),
        userId: user.id,
        productKey: config.key,
        productName: config.name,
        productHeadline: config.headline,
        cdiMultiplier: Number(config.cdiMultiplier || 0),
        annualRate: getInvestmentReferenceAnnualRate(config),
        principalInvestedTotal: amount,
        principalRemaining: amount,
        profitPaidTotal: 0,
        capacityLimit:
          config.maxAmount === null || config.maxAmount === undefined
            ? toMoney(config.minDisplayCapacity || config.minAmount || amount)
            : toMoney(config.maxAmount),
        minAmount: toMoney(config.minAmount),
        releaseAt: toIsoOrNull(releaseAt),
        profitEligibleAt: toIsoOrNull(profitEligibleAt),
        lockMonths: Number(config.lockMonths || 0),
        minHoldDaysForProfit: Number(config.minHoldDaysForProfit || 0),
        movementRequiredPerMonth: toMoney(config.movementRequiredPerMonth),
        allowPartialWithdraw: Boolean(config.allowPartialWithdraw),
        status: INVESTMENT_RESERVE_STATUS_ACTIVE,
        createdAt: db(now),
        updatedAt: db(now),
        lastWithdrawnAt: null,
        closedAt: null
      };

      const financialTx = await createFinancialTransaction(client, {
        userId: user.id,
        referenceKey: `investment:${reserve.id}:create`,
        sourceType: "investment_reserve",
        sourceId: reserve.id,
        operationType: "investment_reserve_create",
        direction: "debit",
        amount,
        status: "completed",
        description: `Aporte em ${config.name}`,
        metadata: {
          reserveId: reserve.id,
          productKey: config.key,
          productName: config.name
        }
      });

      const updatedUser = await applyLedgerChange(client, {
        userId: user.id,
        financialTransactionId: financialTx.id,
        entryType: "debit",
        amount,
        description: `Aporte em ${config.name}`,
        metadata: {
          reserveId: reserve.id,
          productKey: config.key,
          productName: config.name
        }
      });

      await saveInvestmentReserve(reserve, client);

      await createAuditLog(client, {
        adminId: "",
        action: "user_create_investment_reserve",
        targetType: "investment_reserve",
        targetId: reserve.id,
        details: {
          userId: user.id,
          productKey: config.key,
          amount
        },
        ipAddress: getRequestIp(req)
      });

      return {
        reserve,
        user: updatedUser
      };
    });

    const dashboard = await buildInvestimentosDashboardResponse(result.user);
    res.status(201).json({
      message: "Estrutura criada com sucesso",
      reserve: dashboard.reserves.find((item) => item.id === result.reserve.id) || null,
      saldoAtual: toMoney(result.user.saldo),
      dashboard
    });
  } catch (error) {
    console.error(error);
    if (error?.statusCode && error?.payload) {
      return res.status(error.statusCode).json(error.payload);
    }
    res.status(400).json({ error: error.message || "Erro ao criar estrutura" });
  }
});

app.post("/usuario/:id/investimos/reservas/:reserveId/resgatar", authUser, async (req, res) => {
  try {
    const authenticatedUserId = getAuthenticatedUserId(req);
    const requestedUserId = String(req.params.id || "").trim();
    const requestedAmount = req.body?.amount === undefined ? null : toMoney(req.body?.amount);

    if (!authenticatedUserId || requestedUserId !== authenticatedUserId) {
      return sendJsonError(
        res,
        403,
        "INVESTIMENTOS_FORBIDDEN",
        "Acesso negado para esta conta"
      );
    }

    const result = await runInTransaction(async (client) => {
      const user = await getUserByIdForUpdate(authenticatedUserId, client);
      const reserve = await getInvestmentReserveByIdForUpdate(req.params.reserveId, client);

      if (!user) {
        throw new Error("Usuário não encontrado");
      }

      if (isContaBanida(user)) {
        const error = new Error(getMensagemContaBanida());
        error.statusCode = 403;
        error.payload = buildContaBanidaPayload(user);
        throw error;
      }

      if (!reserve || reserve.userId !== user.id) {
        throw new Error("Estrutura de investimento nao encontrada");
      }

      if (getInvestmentReserveStatus(reserve.status) === INVESTMENT_RESERVE_STATUS_CLOSED) {
        throw new Error("Esta estrutura ja foi encerrada");
      }

      const config = getInvestmentProductConfig(reserve.productKey);
      const now = new Date();
      const releaseReached =
        !reserve.releaseAt || new Date(reserve.releaseAt).getTime() <= now.getTime();

      if (config?.withdrawLock && !releaseReached) {
        const error = new Error("Resgate indisponivel antes da data de liberacao");
        error.statusCode = 403;
        error.payload = {
          error: "Resgate indisponivel antes da data de liberacao",
          releaseAt: reserve.releaseAt
        };
        throw error;
      }

      let principalAmount = requestedAmount;
      if (!config?.allowPartialWithdraw) {
        principalAmount = toMoney(reserve.principalRemaining);
      }

      if (!Number.isFinite(principalAmount) || principalAmount <= 0) {
        throw new Error("Informe um valor valido para resgatar");
      }

      if (principalAmount > toMoney(reserve.principalRemaining)) {
        throw new Error("O valor informado excede o saldo guardado");
      }

      if (!config?.allowPartialWithdraw && principalAmount !== toMoney(reserve.principalRemaining)) {
        throw new Error("Esta estrutura permite apenas resgate total");
      }

      const currentMonthMovement = await getUserMonthlyMovementTotal(user.id, now, client);
      const reserveView = buildInvestmentReserveResponse(reserve, {
        now,
        currentMonthMovement
      });

      let profitAmount = 0;
      if (config?.withdrawLock) {
        if (reserveView.profitActiveNow) {
          profitAmount = calculateInvestmentProjectedProfit(
            principalAmount,
            reserve.annualRate,
            diffDaysFloor(reserve.createdAt, reserve.releaseAt || now)
          );
        }
      } else if (reserveView.profitActiveNow) {
        profitAmount = calculateInvestmentProjectedProfit(
          principalAmount,
          reserve.annualRate,
          diffDaysFloor(reserve.createdAt, now)
        );
      }

      const creditAmount = toMoney(principalAmount + profitAmount);
      const financialTx = await createFinancialTransaction(client, {
        userId: user.id,
        referenceKey: `investment:${reserve.id}:withdraw:${Date.now()}`,
        sourceType: "investment_reserve",
        sourceId: reserve.id,
        operationType: "investment_reserve_withdrawal",
        direction: "credit",
        amount: creditAmount,
        status: "completed",
        description: `Resgate de ${reserve.productName || config?.name || "investimento"}`,
        metadata: {
          reserveId: reserve.id,
          productKey: reserve.productKey,
          principalAmount,
          profitAmount
        }
      });

      const updatedUser = await applyLedgerChange(client, {
        userId: user.id,
        financialTransactionId: financialTx.id,
        entryType: "credit",
        amount: creditAmount,
        description: `Resgate de ${reserve.productName || config?.name || "investimento"}`,
        metadata: {
          reserveId: reserve.id,
          productKey: reserve.productKey,
          principalAmount,
          profitAmount
        }
      });

      reserve.principalRemaining = toMoney(reserve.principalRemaining - principalAmount);
      reserve.profitPaidTotal = toMoney(reserve.profitPaidTotal + profitAmount);
      reserve.lastWithdrawnAt = db(now);
      reserve.updatedAt = db(now);

      if (reserve.principalRemaining <= 0) {
        reserve.principalRemaining = 0;
        reserve.status = INVESTMENT_RESERVE_STATUS_CLOSED;
        reserve.closedAt = db(now);
      } else {
        reserve.status = config?.allowPartialWithdraw
          ? INVESTMENT_RESERVE_STATUS_PARTIAL
          : INVESTMENT_RESERVE_STATUS_ACTIVE;
      }

      await saveInvestmentReserve(reserve, client);

      await createAuditLog(client, {
        adminId: "",
        action: "user_withdraw_investment_reserve",
        targetType: "investment_reserve",
        targetId: reserve.id,
        details: {
          userId: user.id,
          productKey: reserve.productKey,
          principalAmount,
          profitAmount,
          creditAmount
        },
        ipAddress: getRequestIp(req)
      });

      return {
        reserve,
        user: updatedUser,
        principalAmount,
        profitAmount,
        creditAmount
      };
    });

    const dashboard = await buildInvestimentosDashboardResponse(result.user);
    res.json({
      message: "Resgate concluido com sucesso",
      principalAmount: toMoney(result.principalAmount),
      profitAmount: toMoney(result.profitAmount),
      creditAmount: toMoney(result.creditAmount),
      saldoAtual: toMoney(result.user.saldo),
      reserve: dashboard.reserves.find((item) => item.id === result.reserve.id) || null,
      dashboard
    });
  } catch (error) {
    console.error(error);
    if (error?.statusCode && error?.payload) {
      return res.status(error.statusCode).json(error.payload);
    }
    res.status(400).json({ error: error.message || "Erro ao resgatar estrutura" });
  }
});

app.post("/usuario/:id/limite-movimentacao", authUser, async (req, res) => {
  try {
    const authenticatedUserId = getAuthenticatedUserId(req);
    const requestedUserId = String(req.params.id || "").trim();
    const requestedAmount = toMoney(req.body?.requestedAmount);

    if (!authenticatedUserId || requestedUserId !== authenticatedUserId) {
      return sendJsonError(
        res,
        403,
        "LIMIT_REQUEST_FORBIDDEN",
        "Acesso negado para esta conta"
      );
    }

    if (!Number.isFinite(requestedAmount) || requestedAmount <= LIMITE_DEPOSITO_MAX) {
      return res.status(400).json({
        error: `Informe um valor acima de R$${LIMITE_DEPOSITO_MAX.toFixed(2)}`
      });
    }

    const request = await runInTransaction(async (client) => {
      const user = await getUserByIdForUpdate(authenticatedUserId, client);

      if (!user) {
        throw new Error("Usuário não encontrado");
      }

      if (isContaBanida(user)) {
        const error = new Error(getMensagemContaBanida());
        error.statusCode = 403;
        error.payload = buildContaBanidaPayload(user);
        throw error;
      }

      const existingRequest = await getOpenMovementLimitRequestByUserId(user.id, client);
      if (existingRequest && String(existingRequest.status || "").toLowerCase() === MOVEMENT_LIMIT_REQUEST_STATUS_PENDING) {
        throw new Error("Ja existe uma solicitacao aberta para esta conta");
      }

      const nextRequest = {
        id: buildId("limitreq"),
        userId: user.id,
        requestedAmount,
        status: MOVEMENT_LIMIT_REQUEST_STATUS_PENDING,
        adminMessage: "",
        pixKey: "",
        notificationId: "",
        createdAt: db(),
        updatedAt: db(),
        respondedAt: null,
        closedAt: null
      };

      await saveMovementLimitRequest(nextRequest, client);

      await createAuditLog(client, {
        adminId: "",
        action: "user_create_movement_limit_request",
        targetType: "movement_limit_request",
        targetId: nextRequest.id,
        details: {
          userId: user.id,
          requestedAmount
        },
        ipAddress: getRequestIp(req)
      });

      return nextRequest;
    });

    res.status(201).json({
      message:
        "Assim que liberarmos o limite de movimentação, vamos avisar pela aba de Notificações",
      request: {
        id: request.id,
        requestedAmount: toMoney(request.requestedAmount),
        status: request.status,
        createdAt: request.createdAt
      }
    });
  } catch (error) {
    console.error(error);
    if (error?.statusCode && error?.payload) {
      return res.status(error.statusCode).json(error.payload);
    }
    res.status(400).json({ error: error.message || "Erro ao enviar solicitacao" });
  }
});

app.post("/usuario/update-nome", authUser, async (req, res) => {
  try {
    const authenticatedUserId = getAuthenticatedUserId(req);
    const { userId, nome } = req.body;

    if (isAuthenticatedUserMismatch(authenticatedUserId, userId)) {
      return sendJsonError(res, 403, "USER_FORBIDDEN", "Acesso negado para esta conta");
    }

    if (!authenticatedUserId || !userId || !nome) {
      return res.status(400).json({ error: "Dados inválidos" });
    }

    const user = await getUserById(authenticatedUserId);

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

app.post("/usuario/delete", authUser, async (req, res) => {
  try {
    const authenticatedUserId = getAuthenticatedUserId(req);
    const { userId, email, senha } = req.body;

    if (isAuthenticatedUserMismatch(authenticatedUserId, userId)) {
      return sendJsonError(res, 403, "USER_FORBIDDEN", "Acesso negado para esta conta");
    }

    if (!authenticatedUserId || !userId || !email || !senha) {
      return res.status(400).json({ error: "Dados obrigatórios" });
    }

    const user = await getUserById(authenticatedUserId);

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
      await client.query("DELETE FROM depositos WHERE user_id = $1", [authenticatedUserId]);
      await client.query("DELETE FROM topup_orders WHERE user_id = $1", [authenticatedUserId]);
      await client.query("DELETE FROM shop_order_items WHERE order_id IN (SELECT id FROM shop_orders WHERE user_id = $1)", [authenticatedUserId]);
      await client.query("DELETE FROM shop_orders WHERE user_id = $1", [authenticatedUserId]);
      await client.query("DELETE FROM financial_transactions WHERE user_id = $1", [authenticatedUserId]);
      await client.query("DELETE FROM ledger_entries WHERE user_id = $1", [authenticatedUserId]);
      await client.query("DELETE FROM user_notifications WHERE user_id = $1", [authenticatedUserId]);
      await client.query("DELETE FROM movement_limit_requests WHERE user_id = $1", [authenticatedUserId]);
      await client.query("DELETE FROM investment_reserves WHERE user_id = $1", [authenticatedUserId]);
      await client.query("DELETE FROM audit_logs WHERE target_id = $1", [authenticatedUserId]);
      await client.query("DELETE FROM usuarios WHERE id = $1", [authenticatedUserId]);
    });

    res.json({ message: "Conta deletada com sucesso" });
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: "Erro ao deletar conta" });
  }
});

app.post("/deposito", authUser, async (req, res) => {
  try {
    const authenticatedUserId = getAuthenticatedUserId(req);
    const {
      userId,
      valor,
      chavePix,
      tipoChave,
      tipoTransacao,
      repassarTaxa
    } = req.body;

    if (isAuthenticatedUserMismatch(authenticatedUserId, userId)) {
      return sendJsonError(res, 403, "DEPOSITO_FORBIDDEN", "Acesso negado para esta conta");
    }

    if (!authenticatedUserId || !userId || valor === undefined || valor === null) {
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

    const user = await getUserById(authenticatedUserId);

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
      userId: authenticatedUserId,
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

app.post("/deposito/pix-code", authUser, async (req, res) => {
  try {
    const authenticatedUserId = getAuthenticatedUserId(req);
    const { userId, valor } = req.body;

    if (isAuthenticatedUserMismatch(authenticatedUserId, userId)) {
      return sendJsonError(res, 403, "DEPOSITO_FORBIDDEN", "Acesso negado para esta conta");
    }

    if (!authenticatedUserId || !userId || valor === undefined || valor === null) {
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

    const user = await getUserById(authenticatedUserId);

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

app.post("/deposito/:id/comprovante", authUser, upload.single("comprovante"), async (req, res) => {
  try {
    const authenticatedUserId = getAuthenticatedUserId(req);
    const pedido = await getDepositoById(req.params.id);

    if (!pedido) {
      return res.status(404).json({ error: "Pedido não encontrado" });
    }

    if (!authenticatedUserId || pedido.userId !== authenticatedUserId) {
      return sendJsonError(res, 403, "DEPOSITO_FORBIDDEN", "Acesso negado para este deposito");
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

app.get("/depositos/user/:id", authUser, async (req, res) => {
  try {
    const authenticatedUserId = getAuthenticatedUserId(req);
    const requestedUserId = String(req.params.id || "").trim();

    if (!authenticatedUserId || requestedUserId !== authenticatedUserId) {
      return sendJsonError(
        res,
        403,
        "DEPOSITO_FORBIDDEN",
        "Acesso negado para estes depositos"
      );
    }

    const lista = await listDepositosByUser(authenticatedUserId);
    res.json(lista);
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: "Erro ao buscar depósitos do usuário" });
  }
});

app.get("/topups/user/:id", authUser, async (req, res) => {
  try {
    const authenticatedUserId = String(req.userAuth?.sub || "").trim();
    const requestedUserId = String(req.params.id || "").trim();

    if (!authenticatedUserId || requestedUserId !== authenticatedUserId) {
      return sendJsonError(res, 403, "TOPUP_FORBIDDEN", "Acesso negado para estas recargas");
    }

    const lista = await listRecargaCelularPedidosByUser(authenticatedUserId);
    res.json(lista);
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: "Erro ao buscar recargas do usuario" });
  }
});

app.get("/shop/orders/user/:id", authUser, async (req, res) => {
  try {
    const authenticatedUserId = getAuthenticatedUserId(req);
    const requestedUserId = String(req.params.id || "").trim();

    if (!authenticatedUserId || requestedUserId !== authenticatedUserId) {
      return sendJsonError(res, 403, "SHOP_FORBIDDEN", "Acesso negado para estes pedidos");
    }

    const orders = await listShopOrdersByUser(authenticatedUserId);
    res.json(orders);
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: "Erro ao buscar pedidos da shop" });
  }
});

app.post("/shop/orders", authUser, shopOrderLimiter, async (req, res) => {
  try {
    const authenticatedUserId = getAuthenticatedUserId(req);
    const {
      userId,
      items,
      shipping,
      customerNote,
      clientRequestId
    } = req.body;

    if (isAuthenticatedUserMismatch(authenticatedUserId, userId)) {
      return sendJsonError(res, 403, "SHOP_FORBIDDEN", "Acesso negado para este pedido");
    }

    const normalizedItems = new Map();
    for (const rawItem of Array.isArray(items) ? items : []) {
      const productId = String(rawItem?.productId || rawItem?.id || "").trim();
      const quantity = normalizeShopQuantity(rawItem?.quantity);
      if (!productId || quantity <= 0) continue;
      normalizedItems.set(productId, (normalizedItems.get(productId) || 0) + quantity);
    }

    const normalizedShipping = {
      name: normalizeShopText(shipping?.name, 120),
      phone: normalizeShopPhone(shipping?.phone),
      zip: normalizeShopPostalCode(shipping?.zip),
      street: normalizeShopText(shipping?.street, 120),
      number: normalizeShopText(shipping?.number, 20),
      complement: normalizeShopText(shipping?.complement, 120),
      neighborhood: normalizeShopText(shipping?.neighborhood, 120),
      city: normalizeShopText(shipping?.city, 120),
      state: normalizeShopState(shipping?.state),
      reference: normalizeShopText(shipping?.reference, 220)
    };
    const normalizedClientRequestId =
      normalizeRecargaCelularClientRequestId(clientRequestId);

    if (!authenticatedUserId || !normalizedItems.size) {
      return res.status(400).json({ error: "Selecione pelo menos um produto" });
    }

    if (
      !normalizedShipping.name ||
      !normalizedShipping.phone ||
      !normalizedShipping.zip ||
      !normalizedShipping.street ||
      !normalizedShipping.number ||
      !normalizedShipping.neighborhood ||
      !normalizedShipping.city ||
      !normalizedShipping.state
    ) {
      return res.status(400).json({ error: "Preencha o endereco completo para entrega" });
    }

    const result = await runInTransaction(async (client) => {
      const user = await getUserByIdForUpdate(authenticatedUserId, client);

      if (!user) {
        throw new Error("Usuario nao encontrado");
      }

      if (isContaBanida(user)) {
        const error = new Error(getMensagemContaBanida());
        error.statusCode = 403;
        error.payload = buildContaBanidaPayload(user);
        throw error;
      }

      const products = await listShopProductsByIds(Array.from(normalizedItems.keys()), client);
      const productsById = new Map(products.map((product) => [product.id, product]));

      if (productsById.size !== normalizedItems.size) {
        throw new Error("Um ou mais produtos da shop nao foram encontrados");
      }

      const orderItems = [];
      let subtotalAmount = 0;

      for (const [productId, quantity] of normalizedItems.entries()) {
        const product = productsById.get(productId);

        if (!product || !product.active || product.category?.active === false) {
          throw new Error("Existe produto indisponivel no carrinho");
        }

        const unitPrice = toMoney(product.price);
        const totalPrice = toMoney(unitPrice * quantity);
        subtotalAmount = toMoney(subtotalAmount + totalPrice);

        orderItems.push({
          productId: product.id,
          categoryId: product.categoryId,
          source: product.source,
          sourceKey: product.sourceKey,
          externalUrl: product.externalUrl,
          productName: product.name,
          productSlug: product.slug,
          imageUrl: product.imageUrl,
          supplierPrice: product.supplierPrice,
          unitPrice,
          quantity,
          totalPrice,
          metadata: {
            categoryName: product.category?.name || "",
            categorySlug: product.category?.slug || ""
          }
        });
      }

      if (subtotalAmount <= 0) {
        throw new Error("Nao foi possivel calcular o valor do pedido");
      }

      if (toMoney(user.saldo) < subtotalAmount) {
        throw new Error("Saldo insuficiente");
      }

      const contexto = await getUserFinancialContext(user, client);
      const bonusDebitado = Math.min(
        toMoney(contexto.saldoBonusAtual),
        subtotalAmount
      );
      const realDebitado = toMoney(subtotalAmount - bonusDebitado);
      const orderId = buildId("shopord");
      const referenceKey = normalizedClientRequestId
        ? `shop:${authenticatedUserId}:${normalizedClientRequestId}:debit`
        : `shop:${orderId}:debit`;
      const now = db();
      const order = {
        id: orderId,
        userId: user.id,
        status: SHOP_ORDER_STATUS_PENDING,
        subtotalAmount,
        totalAmount: subtotalAmount,
        bonusDebitado,
        realDebitado,
        shipping: normalizedShipping,
        customerNote: customerNote || "",
        refusalReason: "",
        financialTransactionIdDebito: "",
        financialTransactionIdEstorno: "",
        adminId: "",
        createdAt: now,
        updatedAt: now,
        approvedAt: null,
        refusedAt: null,
        refundedAt: null
      };
      const itemCount = orderItems.reduce((sum, item) => sum + item.quantity, 0);
      const description = montarDescricaoShopPedido({
        totalAmount: order.totalAmount,
        items: orderItems
      });
      const metadataBase = {
        itemCount,
        shippingCity: normalizedShipping.city,
        shippingState: normalizedShipping.state,
        shippingZip: normalizedShipping.zip,
        customerNote: normalizeShopText(customerNote, 240),
        items: orderItems.map((item) => ({
          productId: item.productId,
          productName: item.productName,
          quantity: item.quantity,
          unitPrice: item.unitPrice,
          totalPrice: item.totalPrice
        })),
        bonusAmount: bonusDebitado,
        realAmount: realDebitado
      };

      const financialTx = await createFinancialTransaction(client, {
        userId: user.id,
        referenceKey,
        sourceType: "shop_order",
        sourceId: orderId,
        operationType: "shop_purchase",
        direction: "debit",
        amount: order.totalAmount,
        status: "completed",
        description,
        metadata: metadataBase
      });

      if (
        normalizedClientRequestId &&
        String(financialTx.sourceId || "").trim() !== orderId
      ) {
        const existingOrder = await getShopOrderById(financialTx.sourceId, client);

        if (existingOrder) {
          return {
            order: existingOrder,
            saldoAtual: toMoney(user.saldo),
            duplicate: true
          };
        }

        throw new Error("Pedido da shop duplicado sem registro associado");
      }

      const usuarioAtualizado = await applyLedgerChange(client, {
        userId: user.id,
        financialTransactionId: financialTx.id,
        entryType: "debit",
        amount: order.totalAmount,
        description,
        metadata: {
          orderId,
          ...metadataBase
        }
      });

      order.financialTransactionIdDebito = financialTx.id;
      await saveShopOrder(order, client);
      await replaceShopOrderItems(order.id, orderItems, client);
      await saveUserNotification(
        {
          id: buildId("notif"),
          userId: user.id,
          type: "shop_order",
          title: "Pedido recebido",
          body: "Seu pedido da Shop Sigmo foi recebido e esta aguardando execucao manual.",
          metadata: {
            orderId: order.id,
            totalAmount: order.totalAmount,
            itemCount
          },
          createdAt: now
        },
        client
      );

      return {
        order: await getShopOrderById(order.id, client),
        saldoAtual: toMoney(usuarioAtualizado.saldo)
      };
    });

    res.status(result.duplicate ? 200 : 201).json(result);
  } catch (error) {
    console.error(error);

    if (error?.statusCode && error?.payload) {
      return res.status(error.statusCode).json(error.payload);
    }

    res.status(400).json({ error: error.message || "Erro ao criar pedido da shop" });
  }
});

app.post("/topups", authUser, async (req, res) => {
  try {
    const { userId, operadora, ddd, numero, valorRecarga, clientRequestId } = req.body;
    const authenticatedUserId = String(req.userAuth?.sub || "").trim();
    const normalizedClientRequestId =
      normalizeRecargaCelularClientRequestId(clientRequestId);
    const operadoraNormalizada = normalizeRecargaCelularOperadora(operadora);
    const dddNormalizado = normalizeRecargaCelularDdd(ddd);
    const numeroNormalizado = normalizeRecargaCelularNumero(numero);
    const detalhes = calcularDetalhesRecargaCelular(valorRecarga);

    if (!authenticatedUserId || !operadoraNormalizada || !dddNormalizado || !numeroNormalizado) {
      return res.status(400).json({ error: "Dados obrigatorios para a recarga" });
    }

    if (!isValidRecargaCelularTelefone(dddNormalizado, numeroNormalizado)) {
      return res.status(400).json({ error: "DDD ou numero de celular invalido" });
    }

    if (
      !Number.isFinite(detalhes.valorRecarga) ||
      !isRecargaCelularValorPermitido(operadoraNormalizada, detalhes.valorRecarga)
    ) {
      const operadoraLabel = getRecargaCelularOperadoraLabel(operadoraNormalizada);
      const valoresPermitidos = formatRecargaCelularValoresPermitidos(operadoraNormalizada);
      return res.status(400).json({
        error: `Selecione um valor disponivel para ${operadoraLabel}: ${valoresPermitidos}`
      });
    }

    if (!authenticatedUserId) {
      return sendJsonError(res, 401, "TOPUP_UNAUTHORIZED", "Nao autorizado");
    }

    if (userId && String(userId).trim() !== authenticatedUserId) {
      return sendJsonError(res, 403, "TOPUP_FORBIDDEN", "Acesso negado para esta recarga");
    }


    const result = await runInTransaction(async (client) => {
      const user = await getUserByIdForUpdate(authenticatedUserId, client);

      if (!user) {
        throw new Error("Usuario nao encontrado");
      }


      if (isContaBanida(user)) {
        const error = new Error(getMensagemContaBanida());
        error.statusCode = 403;
        error.payload = buildContaBanidaPayload(user);
        throw error;
      }

      if (toMoney(user.saldo) < detalhes.valorTotalDebitado) {
        throw new Error("Saldo insuficiente");
      }

      const contexto = await getUserFinancialContext(user, client);
      const bonusDebitado = Math.min(
        toMoney(contexto.saldoBonusAtual),
        detalhes.valorTotalDebitado
      );
      const realDebitado = toMoney(detalhes.valorTotalDebitado - bonusDebitado);
      const pedidoId = buildId("topup");
      const referenceKey = normalizedClientRequestId
        ? `topup:${authenticatedUserId}:${normalizedClientRequestId}:debit`
        : `topup:${pedidoId}:debit`;
      const now = db();
      const pedido = {
        id: pedidoId,
        userId: user.id,
        operadora: operadoraNormalizada,
        ddd: dddNormalizado,
        numero: numeroNormalizado,
        valorRecarga: detalhes.valorRecarga,
        taxaValor: detalhes.taxaValor,
        valorTotalDebitado: detalhes.valorTotalDebitado,
        bonusDebitado,
        realDebitado,
        status: "pendente",
        motivoRecusa: "",
        financialTransactionIdDebito: "",
        financialTransactionIdEstorno: "",
        adminId: "",
        criadoEm: now,
        atualizadoEm: now,
        aprovadoEm: null,
        recusadoEm: null,
        estornadoEm: null
      };
      const descricao = montarDescricaoRecargaCelular(pedido);
      const metadataBase = {
        operadora: operadoraNormalizada,
        operadoraLabel: getRecargaCelularOperadoraLabel(operadoraNormalizada),
        ddd: dddNormalizado,
        numero: numeroNormalizado,
        telefone: buildRecargaCelularTelefone(dddNormalizado, numeroNormalizado),
        valorRecarga: detalhes.valorRecarga,
        taxaValor: detalhes.taxaValor,
        clientRequestId: normalizedClientRequestId,
        bonusAmount: bonusDebitado,
        realAmount: realDebitado
      };

      const financialTx = await createFinancialTransaction(client, {
        userId: user.id,
        referenceKey,
        sourceType: "topup_order",
        sourceId: pedidoId,
        operationType: "topup_purchase",
        direction: "debit",
        amount: detalhes.valorTotalDebitado,
        status: "completed",
        description: descricao,
        metadata: metadataBase
      });

      if (
        normalizedClientRequestId &&
        String(financialTx.sourceId || "").trim() !== pedidoId
      ) {
        const pedidoExistente = await getRecargaCelularPedidoById(
          financialTx.sourceId,
          client
        );

        if (pedidoExistente) {
          return {
            pedido: pedidoExistente,
            saldoAtual: toMoney(user.saldo),
            duplicate: true
          };
        }

        throw new Error("Pedido de recarga duplicado sem registro associado");
      }

      const usuarioAtualizado = await applyLedgerChange(client, {
        userId: user.id,
        financialTransactionId: financialTx.id,
        entryType: "debit",
        amount: detalhes.valorTotalDebitado,
        description: descricao,
        metadata: {
          pedidoId,
          ...metadataBase
        }
      });

      pedido.financialTransactionIdDebito = financialTx.id;
      await saveRecargaCelularPedido(pedido, client);

      return {
        pedido: await getRecargaCelularPedidoById(pedido.id, client),
        saldoAtual: toMoney(usuarioAtualizado.saldo)
      };
    });

    res.status(result.duplicate ? 200 : 201).json(result);
  } catch (error) {
    console.error(error);

    if (error?.statusCode && error?.payload) {
      return res.status(error.statusCode).json(error.payload);
    }

    res.status(400).json({ error: error.message || "Erro ao criar recarga" });
  }
});

app.post("/transferir-sigmo", authUser, async (req, res) => {
  try {
    const authenticatedUserId = getAuthenticatedUserId(req);
    const { fromUserId, emailDestino, valor } = req.body;

    if (isAuthenticatedUserMismatch(authenticatedUserId, fromUserId)) {
      return sendJsonError(
        res,
        403,
        "TRANSFER_FORBIDDEN",
        "Acesso negado para esta transferencia"
      );
    }

    if (!authenticatedUserId || !fromUserId || !emailDestino || valor === undefined || valor === null) {
      return res.status(400).json({ error: "Dados obrigatórios" });
    }

    const valorNum = toMoney(valor);

    if (!Number.isFinite(valorNum) || valorNum <= 0) {
      return res.status(400).json({ error: "Valor inválido" });
    }

    const result = await runInTransaction(async (client) => {
      const remetente = await getUserByIdForUpdate(authenticatedUserId, client);
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

app.get("/admin/shop/orders", authAdmin, async (req, res) => {
  try {
    const orders = await listShopOrders(String(req.query.status || ""));
    res.json(orders);
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: "Erro ao buscar pedidos da shop" });
  }
});

app.post("/admin/shop/orders/:id/approve", authAdmin, async (req, res) => {
  try {
    const orderId = String(req.params.id || "").trim();

    if (!orderId) {
      return res.status(400).json({ error: "Pedido da shop obrigatorio" });
    }

    const result = await runInTransaction(async (client) => {
      const current = await getShopOrderById(orderId, client);
      const order = await getShopOrderByIdForUpdate(orderId, client);

      if (!order || !current) {
        throw new Error("Pedido da shop nao encontrado");
      }

      if (order.status === SHOP_ORDER_STATUS_APPROVED) {
        throw new Error("Pedido da shop ja aprovado");
      }

      if (order.status === SHOP_ORDER_STATUS_REFUSED) {
        throw new Error("Pedido da shop ja recusado");
      }

      const now = db();
      order.status = SHOP_ORDER_STATUS_APPROVED;
      order.adminId = req.admin.sub;
      order.approvedAt = now;
      order.updatedAt = now;

      await saveShopOrder(order, client);
      await saveUserNotification(
        {
          id: buildId("notif"),
          userId: order.userId,
          type: "shop_order_approved",
          title: "Pedido aprovado",
          body: "Seu pedido da Shop Sigmo foi aprovado e segue para execucao manual.",
          metadata: {
            orderId: order.id,
            totalAmount: order.totalAmount
          },
          createdAt: now
        },
        client
      );

      await createAuditLog(client, {
        adminId: req.admin.sub,
        action: "approve_shop_order",
        targetType: "shop_order",
        targetId: order.id,
        details: {
          userId: order.userId,
          totalAmount: order.totalAmount,
          itemCount: Array.isArray(current.items) ? current.items.length : 0
        },
        ipAddress: getRequestIp(req)
      });

      return {
        order: await getShopOrderById(order.id, client)
      };
    });

    res.json({
      message: "Pedido da shop aprovado com sucesso",
      order: result.order
    });
  } catch (error) {
    console.error(error);
    res.status(400).json({ error: error.message || "Erro ao aprovar pedido da shop" });
  }
});

app.post("/admin/shop/orders/:id/refuse", authAdmin, async (req, res) => {
  try {
    const orderId = String(req.params.id || "").trim();
    const refusalReason = normalizeShopText(req.body?.refusalReason, 320);

    if (!orderId) {
      return res.status(400).json({ error: "Pedido da shop obrigatorio" });
    }

    if (!refusalReason) {
      return res.status(400).json({ error: "Motivo da recusa obrigatorio" });
    }

    const result = await runInTransaction(async (client) => {
      const current = await getShopOrderById(orderId, client);
      const order = await getShopOrderByIdForUpdate(orderId, client);

      if (!order || !current) {
        throw new Error("Pedido da shop nao encontrado");
      }

      if (order.status === SHOP_ORDER_STATUS_APPROVED) {
        throw new Error("Pedido da shop ja aprovado");
      }

      if (order.status === SHOP_ORDER_STATUS_REFUSED) {
        throw new Error("Pedido da shop ja recusado");
      }

      const now = db();
      const itemCount = Array.isArray(current.items)
        ? current.items.reduce((sum, item) => sum + Number(item.quantity || 0), 0)
        : 0;
      const metadataBase = {
        refusalReason,
        itemCount,
        items: Array.isArray(current.items)
          ? current.items.map((item) => ({
              productId: item.productId,
              productName: item.productName,
              quantity: item.quantity,
              unitPrice: item.unitPrice,
              totalPrice: item.totalPrice
            }))
          : [],
        bonusAmount: order.bonusDebitado,
        realAmount: order.realDebitado
      };
      const description = `Estorno ${montarDescricaoShopPedido({
        totalAmount: order.totalAmount,
        items: current.items
      })}`;

      const financialTx = await createFinancialTransaction(client, {
        userId: order.userId,
        referenceKey: `shop:${order.id}:refund`,
        sourceType: "shop_order",
        sourceId: order.id,
        operationType: "shop_refund",
        direction: "credit",
        amount: order.totalAmount,
        status: "completed",
        description,
        metadata: metadataBase
      });

      const usuarioAtualizado = await applyLedgerChange(client, {
        userId: order.userId,
        financialTransactionId: financialTx.id,
        entryType: "credit",
        amount: order.totalAmount,
        description,
        metadata: {
          orderId: order.id,
          ...metadataBase
        }
      });

      order.status = SHOP_ORDER_STATUS_REFUSED;
      order.refusalReason = refusalReason;
      order.adminId = req.admin.sub;
      order.financialTransactionIdEstorno = financialTx.id;
      order.refusedAt = now;
      order.refundedAt = now;
      order.updatedAt = now;

      await saveShopOrder(order, client);
      await saveUserNotification(
        {
          id: buildId("notif"),
          userId: order.userId,
          type: "shop_order_refused",
          title: "Pedido recusado",
          body: "Seu pedido da Shop Sigmo foi recusado e o valor voltou para sua carteira.",
          metadata: {
            orderId: order.id,
            totalAmount: order.totalAmount,
            refusalReason
          },
          createdAt: now
        },
        client
      );

      await createAuditLog(client, {
        adminId: req.admin.sub,
        action: "refuse_shop_order",
        targetType: "shop_order",
        targetId: order.id,
        details: {
          userId: order.userId,
          totalAmount: order.totalAmount,
          refusalReason
        },
        ipAddress: getRequestIp(req)
      });

      return {
        order: await getShopOrderById(order.id, client),
        saldoAtual: toMoney(usuarioAtualizado.saldo)
      };
    });

    res.json({
      message: "Pedido da shop recusado e estornado com sucesso",
      order: result.order,
      saldoAtual: result.saldoAtual
    });
  } catch (error) {
    console.error(error);
    res.status(400).json({ error: error.message || "Erro ao recusar pedido da shop" });
  }
});

app.get("/admin/topups", authAdmin, async (req, res) => {
  try {
    const lista = await listRecargaCelularPedidos(String(req.query.status || ""));
    res.json(lista);
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: "Erro ao buscar recargas" });
  }
});

app.post("/admin/topups/:id/approve", authAdmin, async (req, res) => {
  try {
    const topupId = String(req.params.id || "").trim();

    if (!topupId) {
      return res.status(400).json({ error: "Pedido de recarga obrigatorio" });
    }

    const result = await runInTransaction(async (client) => {
      const pedido = await getRecargaCelularPedidoByIdForUpdate(topupId, client);

      if (!pedido) {
        throw new Error("Pedido de recarga nao encontrado");
      }

      if (pedido.status === "aprovado") {
        throw new Error("Pedido de recarga ja aprovado");
      }

      if (pedido.status === "recusado") {
        throw new Error("Pedido de recarga ja recusado");
      }

      const now = db();
      pedido.status = "aprovado";
      pedido.adminId = req.admin.sub;
      pedido.aprovadoEm = now;
      pedido.atualizadoEm = now;

      await saveRecargaCelularPedido(pedido, client);

      await createAuditLog(client, {
        adminId: req.admin.sub,
        action: "approve_topup_order",
        targetType: "topup_order",
        targetId: pedido.id,
        details: {
          userId: pedido.userId,
          operadora: pedido.operadora,
          ddd: pedido.ddd,
          numero: pedido.numero,
          valorRecarga: pedido.valorRecarga,
          valorTotalDebitado: pedido.valorTotalDebitado
        },
        ipAddress: getRequestIp(req)
      });

      return {
        pedido: await getRecargaCelularPedidoById(pedido.id, client)
      };
    });

    res.json({
      message: "Recarga aprovada com sucesso",
      pedido: result.pedido
    });
  } catch (error) {
    console.error(error);
    res.status(400).json({ error: error.message || "Erro ao aprovar recarga" });
  }
});

app.post("/admin/topups/:id/refuse", authAdmin, async (req, res) => {
  try {
    const topupId = String(req.params.id || "").trim();
    const motivoRecusa = normalizeRecargaCelularMotivoRecusa(req.body?.motivoRecusa);

    if (!topupId) {
      return res.status(400).json({ error: "Pedido de recarga obrigatorio" });
    }

    if (!motivoRecusa) {
      return res.status(400).json({ error: "Motivo da recusa obrigatorio" });
    }

    const result = await runInTransaction(async (client) => {
      const pedido = await getRecargaCelularPedidoByIdForUpdate(topupId, client);

      if (!pedido) {
        throw new Error("Pedido de recarga nao encontrado");
      }

      if (pedido.status === "aprovado") {
        throw new Error("Pedido de recarga ja aprovado");
      }

      if (pedido.status === "recusado") {
        throw new Error("Pedido de recarga ja recusado");
      }

      const now = db();
      const descricaoEstorno = `Estorno ${montarDescricaoRecargaCelular(pedido)}`;
      const metadataBase = {
        operadora: pedido.operadora,
        operadoraLabel: getRecargaCelularOperadoraLabel(pedido.operadora),
        ddd: pedido.ddd,
        numero: pedido.numero,
        telefone: pedido.telefone,
        valorRecarga: pedido.valorRecarga,
        taxaValor: pedido.taxaValor,
        motivoRecusa,
        bonusAmount: pedido.bonusDebitado,
        realAmount: pedido.realDebitado
      };

      const financialTx = await createFinancialTransaction(client, {
        userId: pedido.userId,
        referenceKey: `topup:${pedido.id}:refund`,
        sourceType: "topup_order",
        sourceId: pedido.id,
        operationType: "topup_refund",
        direction: "credit",
        amount: pedido.valorTotalDebitado,
        status: "completed",
        description: descricaoEstorno,
        metadata: metadataBase
      });

      const usuarioAtualizado = await applyLedgerChange(client, {
        userId: pedido.userId,
        financialTransactionId: financialTx.id,
        entryType: "credit",
        amount: pedido.valorTotalDebitado,
        description: descricaoEstorno,
        metadata: {
          pedidoId: pedido.id,
          ...metadataBase
        }
      });

      pedido.status = "recusado";
      pedido.motivoRecusa = motivoRecusa;
      pedido.adminId = req.admin.sub;
      pedido.financialTransactionIdEstorno = financialTx.id;
      pedido.recusadoEm = now;
      pedido.estornadoEm = now;
      pedido.atualizadoEm = now;

      await saveRecargaCelularPedido(pedido, client);

      await createAuditLog(client, {
        adminId: req.admin.sub,
        action: "refuse_topup_order",
        targetType: "topup_order",
        targetId: pedido.id,
        details: {
          userId: pedido.userId,
          operadora: pedido.operadora,
          ddd: pedido.ddd,
          numero: pedido.numero,
          valorRecarga: pedido.valorRecarga,
          valorTotalDebitado: pedido.valorTotalDebitado,
          motivoRecusa
        },
        ipAddress: getRequestIp(req)
      });

      return {
        pedido: await getRecargaCelularPedidoById(pedido.id, client),
        saldoAtual: toMoney(usuarioAtualizado.saldo)
      };
    });

    res.json({
      message: "Recarga recusada e estornada com sucesso",
      pedido: result.pedido,
      saldoAtual: result.saldoAtual
    });
  } catch (error) {
    console.error(error);
    res.status(400).json({ error: error.message || "Erro ao recusar recarga" });
  }
});

app.get("/admin/investimos/reservas", authAdmin, async (req, res) => {
  try {
    const reserves = await listInvestmentReserves();
    const uniqueUserIds = Array.from(
      new Set(reserves.map((item) => String(item.userId || "").trim()).filter(Boolean))
    );
    const movementEntries = await Promise.all(
      uniqueUserIds.map(async (userId) => [userId, await getUserMonthlyMovementTotal(userId)])
    );
    const movementMap = new Map(movementEntries);

    res.json(
      reserves.map((reserve) => ({
        ...buildInvestmentReserveResponse(reserve, {
          now: new Date(),
          currentMonthMovement: movementMap.get(reserve.userId) || 0
        }),
        user: reserve.user || null
      }))
    );
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: "Erro ao buscar estruturas do Investimos" });
  }
});

app.get("/admin/limite-movimentacao", authAdmin, async (req, res) => {
  try {
    const requests = await listMovementLimitRequests();
    res.json(requests);
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: "Erro ao buscar solicitacoes de limite" });
  }
});

app.post("/admin/limite-movimentacao/:id/responder", authAdmin, async (req, res) => {
  try {
    const pixKey = String(req.body?.pixKey || "").trim();

    if (!pixKey) {
      return res.status(400).json({ error: "A chave Pix e obrigatoria" });
    }

    const result = await runInTransaction(async (client) => {
      const request = await getMovementLimitRequestByIdForUpdate(req.params.id, client);

      if (!request) {
        throw new Error("Solicitacao nao encontrada");
      }

      if (getMovementRequestStatusLabel(request.status) === MOVEMENT_LIMIT_REQUEST_STATUS_CLOSED) {
        throw new Error("Solicitacao encerrada");
      }

      const user = await getUserById(request.userId, client);

      if (!user) {
        throw new Error("Usuário não encontrado");
      }

      const notification = buildMovementLimitPixKeyNotification({
        userId: user.id,
        requestId: request.id,
        requestedAmount: request.requestedAmount,
        pixKey
      });

      request.status = MOVEMENT_LIMIT_REQUEST_STATUS_RESPONDED;
      request.adminMessage = notification.body;
      request.pixKey = pixKey;
      request.notificationId = notification.id;
      request.updatedAt = db();
      request.respondedAt = db();

      await saveUserNotification(notification, client);
      await saveMovementLimitRequest(request, client);

      await createAuditLog(client, {
        adminId: req.admin.sub,
        action: "respond_movement_limit_request",
        targetType: "movement_limit_request",
        targetId: request.id,
        details: {
          userId: request.userId,
          requestedAmount: request.requestedAmount,
          pixKey
        },
        ipAddress: getRequestIp(req)
      });

      return {
        request,
        notification
      };
    });

    res.json({
      message: "Resposta enviada para o usuario",
      request: result.request,
      notification: result.notification
    });
  } catch (error) {
    console.error(error);
    res.status(400).json({ error: error.message || "Erro ao responder solicitacao" });
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

      if (!isSaida) {
        await aplicarBonusIndicacaoSeElegivel(usuario.id, client);
      }

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

app.post("/admin/bloquear-usuario", authAdmin, async (req, res) => {
  try {
    const { userId, motivoBloqueio, bloqueioTemporario } = req.body;

    if (!userId) {
      return res.status(400).json({ error: "userId e obrigatorio" });
    }

    if (!normalizeAccountRestrictionReason(motivoBloqueio)) {
      return res.status(400).json({ error: "Motivo do bloqueio e obrigatorio" });
    }

    const result = await runInTransaction(async (client) => {
      const usuarioAntes = await getUserByIdForUpdate(userId, client);

      if (!usuarioAntes) {
        throw new Error("Usuario nao encontrado");
      }

      const usuario = await atualizarBloqueioManualConta(
        userId,
        {
          bloqueada: true,
          motivoBloqueio,
          bloqueioTemporario
        },
        client
      );

      await createAuditLog(client, {
        adminId: req.admin.sub,
        action: "manual_account_block",
        targetType: "usuario",
        targetId: usuario.id,
        details: {
          previousStatus: normalizeStatusConta(usuarioAntes.statusConta),
          motivoBloqueio: usuario.motivoBloqueio,
          bloqueioTemporario: Boolean(usuario.bloqueioTemporario)
        },
        ipAddress: getRequestIp(req)
      });

      const context = await getAdminUserContext(usuario, client);
      return buildAdminUserResponse(usuario, context);
    });

    res.json({
      message: "Conta bloqueada com sucesso",
      user: result
    });
  } catch (error) {
    console.error(error);
    res.status(400).json({ error: error.message || "Erro ao bloquear usuario" });
  }
});

app.post("/admin/desbloquear-usuario", authAdmin, async (req, res) => {
  try {
    const { userId } = req.body;

    if (!userId) {
      return res.status(400).json({ error: "userId e obrigatorio" });
    }

    const result = await runInTransaction(async (client) => {
      const usuarioAntes = await getUserByIdForUpdate(userId, client);

      if (!usuarioAntes) {
        throw new Error("Usuario nao encontrado");
      }

      const usuario = await atualizarBloqueioManualConta(
        userId,
        {
          bloqueada: false
        },
        client
      );

      await createAuditLog(client, {
        adminId: req.admin.sub,
        action: "manual_account_unblock",
        targetType: "usuario",
        targetId: usuario.id,
        details: {
          previousStatus: normalizeStatusConta(usuarioAntes.statusConta),
          motivoBloqueioAnterior: usuarioAntes.motivoBloqueio || "",
          bloqueioTemporarioAnterior: Boolean(usuarioAntes.bloqueioTemporario)
        },
        ipAddress: getRequestIp(req)
      });

      const context = await getAdminUserContext(usuario, client);
      return buildAdminUserResponse(usuario, context);
    });

    res.json({
      message: "Conta desbloqueada com sucesso",
      user: result
    });
  } catch (error) {
    console.error(error);
    res.status(400).json({ error: error.message || "Erro ao desbloquear usuario" });
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

      await client.query("DELETE FROM shop_order_items WHERE order_id IN (SELECT id FROM shop_orders WHERE user_id = $1)", [userId]);
      await client.query("DELETE FROM shop_orders WHERE user_id = $1", [userId]);
      await client.query("DELETE FROM user_notifications WHERE user_id = $1", [userId]);
      await client.query("DELETE FROM movement_limit_requests WHERE user_id = $1", [userId]);
      await client.query("DELETE FROM investment_reserves WHERE user_id = $1", [userId]);
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
      await aplicarBonusIndicacaoSeElegivel(usuario.id, client);

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
      await aplicarBonusIndicacaoSeElegivel(usuario.id, client);

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
      await aplicarBonusIndicacaoSeElegivel(usuario.id, client);

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
    refreshShopPublicCatalogSnapshotCache(true)
      .then((snapshot) => {
        if (!hasShopSnapshotProducts(snapshot)) {
          recoverShopCatalogIfEmpty("startup").catch((error) => {
            console.error("[shop-recovery] erro no bootstrap do catalogo", error);
          });
        }
      })
      .catch((error) => {
        console.error("[shop-cache] erro ao aquecer snapshot publico", error);
      });
    app.listen(PORT, () => {
      console.log(`Servidor rodando na porta ${PORT}`);
    });
  })
  .catch((error) => {
    console.error("Erro ao iniciar banco:", error);
    process.exit(1);
  });

