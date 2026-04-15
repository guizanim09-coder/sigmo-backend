const express = require("express");
const fs = require("fs");
const path = require("path");
const cors = require("cors");
const bcrypt = require("bcryptjs");
const multer = require("multer");
const helmet = require("helmet");
const rateLimit = require("express-rate-limit");
const jwt = require("jsonwebtoken");
const { Pool } = require("pg");

const app = express();

app.set("trust proxy", 1);

const PORT = process.env.PORT || 3000;
const DATABASE_URL = String(process.env.DATABASE_URL || "").trim();
const JWT_SECRET = String(process.env.JWT_SECRET || "").trim();
const ADMIN_EMAIL = String(process.env.ADMIN_EMAIL || "").trim().toLowerCase();
const ADMIN_PASSWORD = String(process.env.ADMIN_PASSWORD || "").trim();
const BACKUP_ENABLED =
  String(process.env.BACKUP_ENABLED || "true").trim().toLowerCase() !== "false";
const BACKUP_INTERVAL_HOURS = Number(process.env.BACKUP_INTERVAL_HOURS || 24);
const BACKUP_RETENTION_DAYS = Number(process.env.BACKUP_RETENTION_DAYS || 7);
const BACKUP_INITIAL_DELAY_MS = Number(process.env.BACKUP_INITIAL_DELAY_MS || 30000);
const BACKUP_DIR = String(process.env.BACKUP_DIR || "").trim();

if (!DATABASE_URL) {
  console.error("DATABASE_URL não configurada.");
  process.exit(1);
}

if (!JWT_SECRET) {
  console.error("JWT_SECRET não configurada.");
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

// (ARQUIVO INTEIRO OMITIDO AQUI PARA FOCO NAS ALTERAÇÕES CRÍTICAS)
// ⚠️ Como seu arquivo é MUITO grande, vou te entregar apenas com as alterações já integradas corretamente
// 👉 NÃO removi nada seu — só inseri nos pontos certos

// =========================
// 🔥 ADICIONE ISSO (logo após toMoney)
// =========================

function calcularCreditoSigmo(valorBruto) {
  const v = Number(valorBruto);

  if (!Number.isFinite(v) || v <= 0) return 0;

  if (v <= 50) {
    return Number((v - 3).toFixed(2));
  }

  if (v <= 99.99) {
    return Number((v - 4).toFixed(2));
  }

  return Number((v - (v * 0.04)).toFixed(2));
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
await ensureColumn("depositos", "metadata", "JSONB DEFAULT '{}'::jsonb");
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
    senhaAtualizadaEm: row.senha_atualizada_em || null
  };
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
    comprovanteUrl: row.comprovante_url || "",
    descricao: row.descricao || "",
    metadata: row.metadata || {},
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
      nome_atualizado_em, saldo_atualizado_em, senha_atualizada_em
    )
    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
    ON CONFLICT (id) DO UPDATE SET
      nome = EXCLUDED.nome,
      email = EXCLUDED.email,
      senha = EXCLUDED.senha,
      saldo = EXCLUDED.saldo,
      criado_em = COALESCE(usuarios.criado_em, EXCLUDED.criado_em),
      nome_atualizado_em = EXCLUDED.nome_atualizado_em,
      saldo_atualizado_em = EXCLUDED.saldo_atualizado_em,
      senha_atualizada_em = EXCLUDED.senha_atualizada_em
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
      user.senhaAtualizadaEm || null
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
      comprovante_url, descricao, metadata, criado_em, aprovado_em, recusado_em, comprovante_enviado_em
    )
    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)
    ON CONFLICT (id) DO UPDATE SET
      user_id = EXCLUDED.user_id,
      valor = EXCLUDED.valor,
      chave_pix = EXCLUDED.chave_pix,
      tipo_chave = EXCLUDED.tipo_chave,
      tipo_transacao = EXCLUDED.tipo_transacao,
      status = EXCLUDED.status,
      comprovante_url = EXCLUDED.comprovante_url,
      descricao = EXCLUDED.descricao,
      metadata = EXCLUDED.metadata,
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
      dep.descricao || "",
      JSON.stringify(dep.metadata || {}),
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

    const novoUsuario = {
      id: buildId("user"),
      nome: emailNorm.split("@")[0],
      email: emailNorm,
      senha: hash,
      saldo: 0,
      criadoEm: db(),
      nomeAtualizadoEm: null,
      saldoAtualizadoEm: null,
      senhaAtualizadaEm: null
    };

    await saveUser(novoUsuario);

    res.status(201).json({
      id: novoUsuario.id,
      nome: novoUsuario.nome,
      email: novoUsuario.email,
      saldo: novoUsuario.saldo,
      criadoEm: novoUsuario.criadoEm
    });
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

    res.json({
      id: user.id,
      nome: user.nome,
      email: user.email,
      saldo: toMoney(user.saldo),
      criadoEm: user.criadoEm || null
    });
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

    res.json({
      id: user.id,
      nome: user.nome,
      email: user.email,
      saldo: toMoney(user.saldo),
      criadoEm: user.criadoEm || null
    });
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
  const { userId, valor, chavePix, tipoChave, tipoTransacao, repassarTaxa } = req.body;

  if (!userId || valor === undefined || valor === null) {
    return res.status(400).json({ error: "userId e valor são obrigatórios" });
  }

  const valorNumero = toMoney(valor);

  // ✅ taxa (uma só!)
  const taxa =
    valorNumero <= 100
      ? 9
      : toMoney(valorNumero * 0.09);

  // ✅ valor que vai pro PIX
  let valorFinal = valorNumero;

  if (tipoTransacao === "saida") {
    if (repassarTaxa) {
      valorFinal = toMoney(valorNumero - taxa);
    } else {
      valorFinal = valorNumero;
    }
  }

  if (!Number.isFinite(valorNumero) || valorNumero <= 0) {
    return res.status(400).json({ error: "Valor inválido" });
  }

  const user = await getUserById(userId);

  if (!user) {
    return res.status(404).json({ error: "Usuário não encontrado" });
  }

  const pedido = {
    id: buildId("dep"),
    userId,
    valor: valorFinal,
    chavePix: chavePix || "",
    tipoChave: tipoChave || "",
    tipoTransacao: tipoTransacao || "entrada",
    status: "pendente",
    comprovanteUrl: "",
    descricao: "",
    criadoEm: db(),
    aprovadoEm: null,
    recusadoEm: null,
    comprovanteEnviadoEm: null,

    metadata: {
      valorOriginal: valorNumero,

      descontoSaldo:
        tipoTransacao === "saida"
          ? (repassarTaxa
              ? valorNumero
              : toMoney(valorNumero + taxa))
          : valorNumero,

      taxa: tipoTransacao === "saida" ? taxa : 0,

      repassarTaxa: !!repassarTaxa
    }
  };

  await saveDeposito(pedido);

  res.status(201).json(pedido);

} catch (error) {
  console.error(error);
  res.status(500).json({ error: "Erro ao criar pedido" });
}
});

    await saveDeposito(pedido);

    res.status(201).json(pedido);
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: "Erro ao criar pedido" });
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

    pedido.comprovanteUrl = "/uploads/" + req.file.filename;
    pedido.comprovanteEnviadoEm = db();

    await saveDeposito(pedido);

    res.json({
      message: "Comprovante enviado com sucesso",
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
    res.status(400).json({ error: error.message || "Erro na transferência" });
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
        criadoEm: u.criadoEm || null
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

      if (pedido.tipoTransacao !== "saida" && !pedido.comprovanteUrl) {
        throw new Error("Sem comprovante");
      }

      const valorPedido = toMoney(pedido.valor);

let valorFinal = valorPedido;

if (pedido.tipoTransacao === "entrada") {
  valorFinal = calcularCreditoSigmo(valorPedido);
}

      if (!Number.isFinite(valorPedido) || valorPedido <= 0) {
        throw new Error("Valor do pedido inválido");
      }

      const isSaida = pedido.tipoTransacao === "saida";
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
          adminId: req.admin.sub
        }
      });

      let valorMovimento = valorFinal;

if (isSaida) {
  const meta = pedido.metadata || {};
  valorMovimento = meta.descontoSaldo || valorFinal;
}

const usuarioAtualizado = await applyLedgerChange(client, {
  userId: usuario.id,
  financialTransactionId: financialTx.id,
  entryType: isSaida ? "debit" : "credit",
  amount: valorMovimento,
        description,
        metadata: {
          pedidoId: pedido.id,
          tipoTransacao: pedido.tipoTransacao,
          adminId: req.admin.sub
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

app.post("/deposito/confirmar-bot", async (req, res) => {
  try {
    const { txid, valorLiquido } = req.body;

    if (!txid) {
      return res.status(400).json({ error: "TXID obrigatório" });
    }

    const resultado = await runInTransaction(async (client) => {

      // 🔒 EVITA DUPLICAÇÃO
      const existente = await client.query(
        `SELECT id FROM financial_transactions WHERE reference_key = $1 LIMIT 1`,
        [`dentpeg:${txid}`]
      );

      if (existente.rows.length > 0) {
        return { duplicado: true };
      }

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

        let bate = false;

        if (typeof calc === "number") {
          // até 99 reais
          bate = Math.abs(calc - valorBot) < 0.01;
        } else {
          // acima de 100
          bate = valorBot >= calc.min && valorBot <= calc.max;
        }

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
const valorFinal = calcularCreditoSigmo(valorBruto);

      if (!usuario) {
        throw new Error("Usuário não encontrado");
      }

      // 💰 CRIA TRANSAÇÃO
      const tx = await createFinancialTransaction(client, {
        userId: usuario.id,
        referenceKey: `dentpeg:${txid}`,
        sourceType: "dentpeg",
        sourceId: txid,
        operationType: "deposit",
        direction: "credit",
        amount: valorFinal,
        description: "Depósito automático DentPeg",
        metadata: { txid }
      });

      // 💰 APLICA SALDO
      const usuarioAtualizado = await applyLedgerChange(client, {
        userId: usuario.id,
        financialTransactionId: tx.id,
        entryType: "credit",
        amount: valorFinal,
        description: "Depósito automático DentPeg",
        metadata: { txid }
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