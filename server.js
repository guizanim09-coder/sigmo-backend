const express = require("express");
const fs = require("fs");
const path = require("path");
const cors = require("cors");
const bcrypt = require("bcryptjs");
const multer = require("multer");
const helmet = require("helmet");
const rateLimit = require("express-rate-limit");
const { Pool } = require("pg");

const app = express();

const PORT = process.env.PORT || 3000;
const DB_FILE = path.join(__dirname, "database.json");
const ADMIN_KEY = String(process.env.ADMIN_KEY || "").trim();
const DATABASE_URL = process.env.DATABASE_URL || "";

// =========================
// POSTGRES
// =========================
const pool = new Pool({
  connectionString: DATABASE_URL || undefined,
  ssl: DATABASE_URL
    ? {
        rejectUnauthorized: false
      }
    : false
});

function hasPostgres() {
  return Boolean(DATABASE_URL);
}

async function initDB() {
  if (!hasPostgres()) {
    console.log("Postgres não configurado. Rodando com fallback em JSON.");
    return;
  }

  await pool.query(`
    CREATE TABLE IF NOT EXISTS usuarios (
      id TEXT PRIMARY KEY,
      nome TEXT,
      email TEXT UNIQUE,
      senha TEXT,
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
      user_id TEXT,
      valor NUMERIC,
      chave_pix TEXT,
      tipo_chave TEXT,
      tipo_transacao TEXT,
      status TEXT,
      comprovante_url TEXT,
      descricao TEXT,
      criado_em TIMESTAMP,
      aprovado_em TIMESTAMP,
      recusado_em TIMESTAMP,
      comprovante_enviado_em TIMESTAMP
    );
  `);

  console.log("Banco Postgres pronto");
}

// =========================
// SEGURANÇA
// =========================
app.use(helmet());

const globalLimiter = rateLimit({
  windowMs: 60 * 1000,
  max: 120,
  standardHeaders: true,
  legacyHeaders: false,
  message: { error: "Muitas requisições, tente novamente." }
});

const loginLimiter = rateLimit({
  windowMs: 60 * 1000,
  max: 5,
  standardHeaders: true,
  legacyHeaders: false,
  message: { error: "Muitas tentativas de login. Aguarde 1 minuto." }
});

app.use(globalLimiter);

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
      return callback(new Error("Origem não permitida"));
    },
    methods: ["GET", "POST", "OPTIONS"],
    allowedHeaders: ["Content-Type", "x-admin-key"]
  })
);

app.use(express.json());

// =========================
// UPLOAD
// =========================
const UPLOADS_DIR = path.join(__dirname, "uploads");

if (!fs.existsSync(UPLOADS_DIR)) {
  fs.mkdirSync(UPLOADS_DIR, { recursive: true });
}

app.use("/uploads", express.static(UPLOADS_DIR));

const storage = multer.diskStorage({
  destination: (_, __, cb) => cb(null, UPLOADS_DIR),
  filename: (_, file, cb) => {
    const ext = path.extname(file.originalname || "");
    cb(null, "comp_" + Date.now() + ext);
  }
});

const upload = multer({
  storage,
  limits: { fileSize: 5 * 1024 * 1024 },
  fileFilter: (_, file, cb) => {
    const allowed = ["image/png", "image/jpeg", "image/jpg", "application/pdf"];
    if (!allowed.includes(file.mimetype)) {
      return cb(new Error("Formato inválido"));
    }
    cb(null, true);
  }
});

// =========================
// JSON FALLBACK
// =========================
function ensureDB() {
  if (!fs.existsSync(DB_FILE)) {
    fs.writeFileSync(
      DB_FILE,
      JSON.stringify(
        {
          usuarios: [],
          depositos: []
        },
        null,
        2
      )
    );
  }
}

function readDB() {
  ensureDB();
  return JSON.parse(fs.readFileSync(DB_FILE, "utf8"));
}

function writeDB(data) {
  fs.writeFileSync(DB_FILE, JSON.stringify(data, null, 2));
}

// =========================
// HELPERS
// =========================
function normalizeEmail(email) {
  return String(email || "").trim().toLowerCase();
}

function mapPgUser(row) {
  if (!row) return null;
  return {
    id: row.id,
    nome: row.nome || row.email?.split("@")[0] || "",
    email: row.email,
    senha: row.senha,
    saldo: Number(row.saldo || 0),
    criadoEm: row.criado_em || null,
    nomeAtualizadoEm: row.nome_atualizado_em || null,
    saldoAtualizadoEm: row.saldo_atualizado_em || null,
    senhaAtualizadaEm: row.senha_atualizada_em || null
  };
}

function mapPgDeposit(row) {
  if (!row) return null;
  return {
    id: row.id,
    userId: row.user_id,
    valor: Number(row.valor || 0),
    chavePix: row.chave_pix || "",
    tipoChave: row.tipo_chave || "",
    tipoTransacao: row.tipo_transacao || "entrada",
    status: row.status || "pendente",
    comprovanteUrl: row.comprovante_url || "",
    descricao: row.descricao || "",
    criadoEm: row.criado_em || null,
    aprovadoEm: row.aprovado_em || null,
    recusadoEm: row.recusado_em || null,
    comprovanteEnviadoEm: row.comprovante_enviado_em || null
  };
}

async function pgGetUserByEmail(email) {
  if (!hasPostgres()) return null;
  const result = await pool.query("SELECT * FROM usuarios WHERE email = $1 LIMIT 1", [
    normalizeEmail(email)
  ]);
  return mapPgUser(result.rows[0]);
}

async function pgGetUserById(id) {
  if (!hasPostgres()) return null;
  const result = await pool.query("SELECT * FROM usuarios WHERE id = $1 LIMIT 1", [id]);
  return mapPgUser(result.rows[0]);
}

async function pgListUsers() {
  if (!hasPostgres()) return [];
  const result = await pool.query("SELECT * FROM usuarios ORDER BY criado_em DESC NULLS LAST");
  return result.rows.map(mapPgUser);
}

async function pgUpsertUser(user) {
  if (!hasPostgres()) return;
  await pool.query(
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
      user.senha || "",
      Number(user.saldo || 0),
      user.criadoEm || new Date().toISOString(),
      user.nomeAtualizadoEm || null,
      user.saldoAtualizadoEm || null,
      user.senhaAtualizadaEm || null
    ]
  );
}

async function pgGetDepositById(id) {
  if (!hasPostgres()) return null;
  const result = await pool.query("SELECT * FROM depositos WHERE id = $1 LIMIT 1", [id]);
  return mapPgDeposit(result.rows[0]);
}

async function pgListDeposits() {
  if (!hasPostgres()) return [];
  const result = await pool.query("SELECT * FROM depositos ORDER BY criado_em DESC NULLS LAST");
  return result.rows.map(mapPgDeposit);
}

async function pgListDepositsByUser(userId) {
  if (!hasPostgres()) return [];
  const result = await pool.query(
    "SELECT * FROM depositos WHERE user_id = $1 ORDER BY criado_em DESC NULLS LAST",
    [userId]
  );
  return result.rows.map(mapPgDeposit);
}

async function pgUpsertDeposit(dep) {
  if (!hasPostgres()) return;
  await pool.query(
    `
    INSERT INTO depositos (
      id, user_id, valor, chave_pix, tipo_chave, tipo_transacao, status,
      comprovante_url, descricao, criado_em, aprovado_em, recusado_em, comprovante_enviado_em
    )
    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)
    ON CONFLICT (id) DO UPDATE SET
      user_id = EXCLUDED.user_id,
      valor = EXCLUDED.valor,
      chave_pix = EXCLUDED.chave_pix,
      tipo_chave = EXCLUDED.tipo_chave,
      tipo_transacao = EXCLUDED.tipo_transacao,
      status = EXCLUDED.status,
      comprovante_url = EXCLUDED.comprovante_url,
      descricao = EXCLUDED.descricao,
      criado_em = COALESCE(depositos.criado_em, EXCLUDED.criado_em),
      aprovado_em = EXCLUDED.aprovado_em,
      recusado_em = EXCLUDED.recusado_em,
      comprovante_enviado_em = EXCLUDED.comprovante_enviado_em
  `,
    [
      dep.id,
      dep.userId,
      Number(dep.valor || 0),
      dep.chavePix || "",
      dep.tipoChave || "",
      dep.tipoTransacao || "entrada",
      dep.status || "pendente",
      dep.comprovanteUrl || "",
      dep.descricao || "",
      dep.criadoEm || new Date().toISOString(),
      dep.aprovadoEm || null,
      dep.recusadoEm || null,
      dep.comprovanteEnviadoEm || null
    ]
  );
}

function jsonGetUserByEmail(email) {
  const db = readDB();
  return (
    db.usuarios.find((u) => normalizeEmail(u.email) === normalizeEmail(email)) || null
  );
}

function jsonGetUserById(id) {
  const db = readDB();
  return db.usuarios.find((u) => u.id === id) || null;
}

function jsonListUsers() {
  const db = readDB();
  return db.usuarios || [];
}

function jsonSaveUser(user) {
  const db = readDB();
  const idx = db.usuarios.findIndex((u) => u.id === user.id);

  if (idx >= 0) {
    db.usuarios[idx] = { ...db.usuarios[idx], ...user };
  } else {
    db.usuarios.push(user);
  }

  writeDB(db);
}

function jsonGetDepositById(id) {
  const db = readDB();
  return db.depositos.find((d) => d.id === id) || null;
}

function jsonListDeposits() {
  const db = readDB();
  return db.depositos || [];
}

function jsonListDepositsByUser(userId) {
  const db = readDB();
  return db.depositos.filter((d) => d.userId === userId);
}

function jsonSaveDeposit(dep) {
  const db = readDB();
  const idx = db.depositos.findIndex((d) => d.id === dep.id);

  if (idx >= 0) {
    db.depositos[idx] = { ...db.depositos[idx], ...dep };
  } else {
    db.depositos.push(dep);
  }

  writeDB(db);
}

async function getUserByEmail(email) {
  const fromPg = await pgGetUserByEmail(email);
  if (fromPg) return fromPg;

  const fromJson = jsonGetUserByEmail(email);
  if (fromJson) {
    await pgUpsertUser(fromJson);
    return {
      ...fromJson,
      saldo: Number(fromJson.saldo || 0)
    };
  }

  return null;
}

async function getUserById(id) {
  const fromPg = await pgGetUserById(id);
  if (fromPg) return fromPg;

  const fromJson = jsonGetUserById(id);
  if (fromJson) {
    await pgUpsertUser(fromJson);
    return {
      ...fromJson,
      saldo: Number(fromJson.saldo || 0)
    };
  }

  return null;
}

async function saveUserEverywhere(user) {
  const safeUser = {
    ...user,
    email: normalizeEmail(user.email),
    saldo: Number(user.saldo || 0)
  };

  await pgUpsertUser(safeUser);
  jsonSaveUser(safeUser);
}

async function getDepositById(id) {
  const fromPg = await pgGetDepositById(id);
  if (fromPg) return fromPg;

  const fromJson = jsonGetDepositById(id);
  if (fromJson) {
    await pgUpsertDeposit(fromJson);
    return {
      ...fromJson,
      valor: Number(fromJson.valor || 0)
    };
  }

  return null;
}

async function saveDepositEverywhere(dep) {
  const safeDep = {
    ...dep,
    valor: Number(dep.valor || 0)
  };

  await pgUpsertDeposit(safeDep);
  jsonSaveDeposit(safeDep);
}

async function listUsersMerged() {
  const pgUsers = await pgListUsers();
  const jsonUsers = jsonListUsers();

  const map = new Map();

  for (const u of pgUsers) {
    map.set(u.id, { ...u, saldo: Number(u.saldo || 0) });
  }

  for (const u of jsonUsers) {
    if (!map.has(u.id)) {
      map.set(u.id, { ...u, saldo: Number(u.saldo || 0) });
    }
  }

  return Array.from(map.values()).sort(
    (a, b) => new Date(b.criadoEm || 0) - new Date(a.criadoEm || 0)
  );
}

async function listDepositsMerged() {
  const pgDeps = await pgListDeposits();
  const jsonDeps = jsonListDeposits();

  const map = new Map();

  for (const d of pgDeps) {
    map.set(d.id, { ...d, valor: Number(d.valor || 0) });
  }

  for (const d of jsonDeps) {
    if (!map.has(d.id)) {
      map.set(d.id, { ...d, valor: Number(d.valor || 0) });
    }
  }

  return Array.from(map.values()).sort(
    (a, b) => new Date(b.criadoEm || 0) - new Date(a.criadoEm || 0)
  );
}

async function listDepositsByUserMerged(userId) {
  const pgDeps = await pgListDepositsByUser(userId);
  const jsonDeps = jsonListDepositsByUser(userId);

  const map = new Map();

  for (const d of pgDeps) {
    map.set(d.id, { ...d, valor: Number(d.valor || 0) });
  }

  for (const d of jsonDeps) {
    if (!map.has(d.id)) {
      map.set(d.id, { ...d, valor: Number(d.valor || 0) });
    }
  }

  return Array.from(map.values()).sort(
    (a, b) => new Date(b.criadoEm || 0) - new Date(a.criadoEm || 0)
  );
}

// =========================
// ADMIN
// =========================
function requireAdmin(req, res, next) {
  const adminKey = String(req.headers["x-admin-key"] || "").trim();

  if (!ADMIN_KEY || adminKey !== ADMIN_KEY) {
    return res.status(401).json({ error: "Acesso não autorizado" });
  }

  next();
}

// =========================
// HEALTH
// =========================
app.get("/", (req, res) => {
  res.json({
    ok: true,
    message: "Sigmo backend online"
  });
});

// =========================
// REGISTER
// =========================
app.post("/register", async (req, res) => {
  try {
    const { email, senha } = req.body;

    if (!email || !senha) {
      return res.status(400).json({ error: "Email e senha são obrigatórios" });
    }

    const emailNormalizado = normalizeEmail(email);
    const existe = await getUserByEmail(emailNormalizado);

    if (existe) {
      return res.status(400).json({ error: "Usuário já existe" });
    }

    const senhaHash = await bcrypt.hash(String(senha), 10);

    const novoUsuario = {
      id: "user_" + Date.now(),
      nome: emailNormalizado.split("@")[0],
      email: emailNormalizado,
      senha: senhaHash,
      saldo: 0,
      criadoEm: new Date().toISOString()
    };

    await saveUserEverywhere(novoUsuario);

    return res.status(201).json({
      id: novoUsuario.id,
      nome: novoUsuario.nome,
      email: novoUsuario.email,
      saldo: novoUsuario.saldo,
      criadoEm: novoUsuario.criadoEm
    });
  } catch (error) {
    console.error(error);
    return res.status(500).json({ error: "Erro interno no registro" });
  }
});

// =========================
// LOGIN
// =========================
app.post("/login", loginLimiter, async (req, res) => {
  try {
    const { email, senha } = req.body;

    if (!email || !senha) {
      return res.status(400).json({ error: "Email e senha são obrigatórios" });
    }

    const emailNormalizado = normalizeEmail(email);
    const user = await getUserByEmail(emailNormalizado);

    if (!user) {
      return res.status(401).json({ error: "Login inválido" });
    }

    const senhaOk = await bcrypt.compare(String(senha), String(user.senha));

    if (!senhaOk) {
      return res.status(401).json({ error: "Login inválido" });
    }

    return res.json({
      id: user.id,
      nome: user.nome || user.email?.split("@")[0] || "",
      email: user.email,
      saldo: Number(user.saldo || 0),
      criadoEm: user.criadoEm || null
    });
  } catch (error) {
    console.error(error);
    return res.status(500).json({ error: "Erro no login" });
  }
});

// =========================
// USUÁRIO LOGADO
// =========================
app.get("/usuario/:id", async (req, res) => {
  try {
    const { id } = req.params;
    const user = await getUserById(id);

    if (!user) {
      return res.status(404).json({ error: "Usuário não encontrado" });
    }

    return res.json({
      id: user.id,
      nome: user.nome || user.email?.split("@")[0] || "",
      email: user.email,
      saldo: Number(user.saldo || 0),
      criadoEm: user.criadoEm || null
    });
  } catch (error) {
    console.error(error);
    return res.status(500).json({ error: "Erro ao buscar usuário" });
  }
});

// =========================
// ATUALIZAR NOME
// =========================
app.post("/usuario/update-nome", async (req, res) => {
  try {
    const { userId, nome } = req.body;

    if (!userId || !nome) {
      return res.status(400).json({ error: "Dados inválidos" });
    }

    const nomeLimpo = String(nome).trim();

    if (nomeLimpo.length < 2) {
      return res.status(400).json({ error: "Nome muito curto" });
    }

    const user = await getUserById(userId);

    if (!user) {
      return res.status(404).json({ error: "Usuário não encontrado" });
    }

    user.nome = nomeLimpo;
    user.nomeAtualizadoEm = new Date().toISOString();

    await saveUserEverywhere(user);

    return res.json({ message: "Nome atualizado com sucesso" });
  } catch (error) {
    console.error(error);
    return res.status(500).json({ error: "Erro ao atualizar nome" });
  }
});

// =========================
// CRIAR PEDIDO
// =========================
app.post("/deposito", async (req, res) => {
  try {
    const { userId, valor, chavePix, tipoChave, tipoTransacao } = req.body;

    if (!userId || valor === undefined || valor === null) {
      return res.status(400).json({ error: "userId e valor são obrigatórios" });
    }

    const valorNumero = Number(valor);

    if (!Number.isFinite(valorNumero) || valorNumero <= 0) {
      return res.status(400).json({ error: "Valor inválido" });
    }

    const usuario = await getUserById(userId);

    if (!usuario) {
      return res.status(404).json({ error: "Usuário não encontrado" });
    }

    const pedido = {
      id: "dep_" + Date.now(),
      userId,
      valor: valorNumero,
      chavePix: chavePix || "",
      tipoChave: tipoChave || "",
      tipoTransacao: tipoTransacao || "entrada",
      status: "pendente",
      comprovanteUrl: "",
      descricao: "",
      criadoEm: new Date().toISOString(),
      aprovadoEm: null,
      recusadoEm: null,
      comprovanteEnviadoEm: null
    };

    await saveDepositEverywhere(pedido);

    return res.status(201).json(pedido);
  } catch (error) {
    console.error(error);
    return res.status(500).json({ error: "Erro ao criar pedido" });
  }
});

// =========================
// ANEXAR COMPROVANTE
// =========================
app.post("/deposito/:id/comprovante", upload.single("comprovante"), async (req, res) => {
  try {
    const pedido = await getDepositById(req.params.id);

    if (!pedido) {
      return res.status(404).json({ error: "Pedido não encontrado" });
    }

    if (!req.file) {
      return res.status(400).json({ error: "Arquivo obrigatório" });
    }

    pedido.comprovanteUrl = "/uploads/" + req.file.filename;
    pedido.comprovanteEnviadoEm = new Date().toISOString();

    await saveDepositEverywhere(pedido);

    return res.json({
      message: "Comprovante enviado com sucesso",
      pedido
    });
  } catch (error) {
    console.error(error);
    return res.status(500).json({ error: "Erro upload" });
  }
});

// =========================
// TRANSFERÊNCIA INTERNA SIGMO
// =========================
app.post("/transferir-sigmo", async (req, res) => {
  try {
    const { fromUserId, emailDestino, valor } = req.body;

    if (!fromUserId || !emailDestino || valor === undefined || valor === null) {
      return res.status(400).json({ error: "Dados obrigatórios" });
    }

    const valorNum = Number(valor);

    if (!Number.isFinite(valorNum) || valorNum <= 0) {
      return res.status(400).json({ error: "Valor inválido" });
    }

    const remetente = await getUserById(fromUserId);
    if (!remetente) {
      return res.status(404).json({ error: "Remetente não encontrado" });
    }

    const destino = await getUserByEmail(emailDestino);
    if (!destino) {
      return res.status(404).json({ error: "Usuário destino não encontrado" });
    }

    if (remetente.id === destino.id) {
      return res.status(400).json({ error: "Não pode transferir para si mesmo" });
    }

    if (Number(remetente.saldo || 0) < valorNum) {
      return res.status(400).json({ error: "Saldo insuficiente" });
    }

    remetente.saldo = Number(remetente.saldo || 0) - valorNum;
    destino.saldo = Number(destino.saldo || 0) + valorNum;
    remetente.saldoAtualizadoEm = new Date().toISOString();
    destino.saldoAtualizadoEm = new Date().toISOString();

    await saveUserEverywhere(remetente);
    await saveUserEverywhere(destino);

    const agora = new Date().toISOString();

    const saida = {
      id: "dep_" + Date.now(),
      userId: remetente.id,
      valor: valorNum,
      chavePix: "",
      tipoChave: "",
      tipoTransacao: "saida",
      status: "aprovado",
      comprovanteUrl: "",
      descricao: `Transferência enviada para ${destino.email}`,
      criadoEm: agora,
      aprovadoEm: agora,
      recusadoEm: null,
      comprovanteEnviadoEm: null
    };

    const entrada = {
      id: "dep_" + (Date.now() + 1),
      userId: destino.id,
      valor: valorNum,
      chavePix: "",
      tipoChave: "",
      tipoTransacao: "entrada",
      status: "aprovado",
      comprovanteUrl: "",
      descricao: `Transferência recebida de ${remetente.email}`,
      criadoEm: agora,
      aprovadoEm: agora,
      recusadoEm: null,
      comprovanteEnviadoEm: null
    };

    await saveDepositEverywhere(saida);
    await saveDepositEverywhere(entrada);

    return res.json({
      message: "Transferência realizada com sucesso",
      saldoAtual: remetente.saldo
    });
  } catch (error) {
    console.error(error);
    return res.status(500).json({ error: "Erro na transferência" });
  }
});

// =========================
// PEDIDOS DO USUÁRIO
// =========================
app.get("/depositos/user/:id", async (req, res) => {
  try {
    const lista = await listDepositsByUserMerged(req.params.id);
    return res.json(lista);
  } catch (error) {
    console.error(error);
    return res.status(500).json({ error: "Erro ao buscar depósitos do usuário" });
  }
});

// =========================
// ADMIN - LISTAR USUÁRIOS
// =========================
app.get("/usuarios", requireAdmin, async (req, res) => {
  try {
    const usuarios = await listUsersMerged();

    const usuariosSeguros = usuarios.map((u) => ({
      id: u.id,
      nome: u.nome || u.email?.split("@")[0] || "",
      email: u.email,
      saldo: Number(u.saldo || 0),
      criadoEm: u.criadoEm || null
    }));

    return res.json(usuariosSeguros);
  } catch (error) {
    console.error(error);
    return res.status(500).json({ error: "Erro ao listar usuários" });
  }
});

// =========================
// ADMIN - LISTAR PEDIDOS
// =========================
app.get("/depositos", requireAdmin, async (req, res) => {
  try {
    const pedidos = await listDepositsMerged();
    return res.json(pedidos);
  } catch (error) {
    console.error(error);
    return res.status(500).json({ error: "Erro ao listar pedidos" });
  }
});

// =========================
// ADMIN - APROVAR PEDIDO
// =========================
app.post("/aprovar", requireAdmin, async (req, res) => {
  try {
    const { depositoId } = req.body;

    if (!depositoId) {
      return res.status(400).json({ error: "depositoId é obrigatório" });
    }

    const pedido = await getDepositById(depositoId);
    if (!pedido) {
      return res.status(404).json({ error: "Pedido não encontrado" });
    }

    if (pedido.status === "aprovado") {
      return res.status(400).json({ error: "Pedido já aprovado" });
    }

    if (pedido.status === "recusado") {
      return res.status(400).json({ error: "Pedido já recusado" });
    }

    const usuario = await getUserById(pedido.userId);
    if (!usuario) {
      return res.status(404).json({ error: "Usuário não encontrado" });
    }

    const saldoAtualLocal = Number(usuario.saldo || 0);
    const valorPedido = Number(pedido.valor || 0);

    if (pedido.tipoTransacao !== "saida" && !pedido.comprovanteUrl) {
      return res.status(400).json({ error: "Sem comprovante" });
    }

    if (pedido.tipoTransacao === "saida") {
      if (saldoAtualLocal < valorPedido) {
        return res.status(400).json({ error: "Saldo insuficiente para aprovar saída" });
      }

      usuario.saldo = saldoAtualLocal - valorPedido;
    } else {
      usuario.saldo = saldoAtualLocal + valorPedido;
    }

    usuario.saldoAtualizadoEm = new Date().toISOString();
    pedido.status = "aprovado";
    pedido.aprovadoEm = new Date().toISOString();

    await saveUserEverywhere(usuario);
    await saveDepositEverywhere(pedido);

    return res.json({
      message: "Pedido aprovado com sucesso",
      pedido,
      saldoAtual: usuario.saldo
    });
  } catch (error) {
    console.error(error);
    return res.status(500).json({ error: "Erro ao aprovar pedido" });
  }
});

// =========================
// ADMIN - RECUSAR PEDIDO
// =========================
app.post("/recusar", requireAdmin, async (req, res) => {
  try {
    const { depositoId } = req.body;

    if (!depositoId) {
      return res.status(400).json({ error: "depositoId é obrigatório" });
    }

    const pedido = await getDepositById(depositoId);

    if (!pedido) {
      return res.status(404).json({ error: "Pedido não encontrado" });
    }

    if (pedido.status === "aprovado") {
      return res.status(400).json({ error: "Pedido já aprovado, não pode recusar" });
    }

    if (pedido.status === "recusado") {
      return res.status(400).json({ error: "Pedido já recusado" });
    }

    pedido.status = "recusado";
    pedido.recusadoEm = new Date().toISOString();

    await saveDepositEverywhere(pedido);

    return res.json({
      message: "Pedido recusado com sucesso",
      pedido
    });
  } catch (error) {
    console.error(error);
    return res.status(500).json({ error: "Erro ao recusar pedido" });
  }
});

// =========================
// ADMIN - AJUSTAR SALDO
// =========================
app.post("/admin/update-balance", requireAdmin, async (req, res) => {
  try {
    const { userId, saldo } = req.body;

    if (!userId || saldo === undefined || saldo === null) {
      return res.status(400).json({ error: "userId e saldo são obrigatórios" });
    }

    const saldoNumero = Number(saldo);

    if (!Number.isFinite(saldoNumero) || saldoNumero < 0) {
      return res.status(400).json({ error: "Saldo inválido" });
    }

    const usuario = await getUserById(userId);

    if (!usuario) {
      return res.status(404).json({ error: "Usuário não encontrado" });
    }

    usuario.saldo = saldoNumero;
    usuario.saldoAtualizadoEm = new Date().toISOString();

    await saveUserEverywhere(usuario);

    return res.json({
      message: "Saldo atualizado com sucesso",
      user: {
        id: usuario.id,
        email: usuario.email,
        saldo: usuario.saldo
      }
    });
  } catch (error) {
    console.error(error);
    return res.status(500).json({ error: "Erro ao atualizar saldo" });
  }
});

// =========================
// ADMIN - RESETAR SENHA
// =========================
app.post("/admin/reset-password", requireAdmin, async (req, res) => {
  try {
    const { userId, novaSenha } = req.body;

    if (!userId || !novaSenha) {
      return res.status(400).json({ error: "userId e novaSenha são obrigatórios" });
    }

    const usuario = await getUserById(userId);

    if (!usuario) {
      return res.status(404).json({ error: "Usuário não encontrado" });
    }

    const senhaHash = await bcrypt.hash(String(novaSenha), 10);
    usuario.senha = senhaHash;
    usuario.senhaAtualizadaEm = new Date().toISOString();

    await saveUserEverywhere(usuario);

    return res.json({
      message: "Senha redefinida com sucesso"
    });
  } catch (error) {
    console.error(error);
    return res.status(500).json({ error: "Erro ao redefinir senha" });
  }
});

initDB()
  .then(() => {
    app.listen(PORT, () => {
      console.log(`Servidor rodando na porta ${PORT}`);
    });
  })
  .catch((error) => {
    console.error("Erro ao iniciar banco:", error);
    process.exit(1);
  });