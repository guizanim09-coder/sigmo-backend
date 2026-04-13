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

const PORT = process.env.PORT || 3000;
const DATABASE_URL = String(process.env.DATABASE_URL || "").trim();
const JWT_SECRET = String(process.env.JWT_SECRET || "").trim();
const ADMIN_EMAIL = String(process.env.ADMIN_EMAIL || "").trim().toLowerCase();
const ADMIN_PASSWORD = String(process.env.ADMIN_PASSWORD || "").trim();

if (!DATABASE_URL) {
  console.error("DATABASE_URL não configurada.");
  process.exit(1);
}

if (!JWT_SECRET) {
  console.error("JWT_SECRET não configurada.");
  process.exit(1);
}

// =========================
// POSTGRES
// =========================
const pool = new Pool({
  connectionString: DATABASE_URL,
  ssl: { rejectUnauthorized: false }
});

// =========================
// INIT DB
// =========================
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

  await ensureAdmin();
  console.log("Banco pronto");
}

// =========================
// ADMIN AUTO
// =========================
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
        "admin_" + Date.now(),
        ADMIN_EMAIL,
        hash,
        "Administrador",
        "admin",
        true,
        new Date().toISOString(),
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

// =========================
// SEGURANÇA
// =========================
app.use(helmet());
app.use(express.json());

app.use(rateLimit({
  windowMs: 60 * 1000,
  max: 120,
  standardHeaders: true,
  legacyHeaders: false
}));

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

// =========================
// CORS
// =========================
const allowedOrigins = [
  /^https:\/\/.*\.netlify\.app$/,
  "http://localhost:3000",
  "http://127.0.0.1:3000",
  "http://localhost:5173",
  "http://127.0.0.1:5173"
];

app.use(cors({
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
}));

app.options("*", cors());

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
  limits: { fileSize: 8 * 1024 * 1024 },
  fileFilter: (_, file, cb) => {
    const allowed = ["image/png", "image/jpeg", "image/jpg", "application/pdf"];
    if (!allowed.includes(file.mimetype)) {
      return cb(new Error("Formato inválido"));
    }
    cb(null, true);
  }
});

// =========================
// HELPERS
// =========================
function normalizeEmail(email) {
  return String(email || "").trim().toLowerCase();
}

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
    saldo: Number(row.saldo || 0),
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

async function getUserById(id) {
  const result = await pool.query(
    "SELECT * FROM usuarios WHERE id = $1 LIMIT 1",
    [id]
  );
  return mapUser(result.rows[0]);
}

async function getUserByEmail(email) {
  const result = await pool.query(
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

async function saveUser(user) {
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
      user.senha,
      Number(user.saldo || 0),
      user.criadoEm || new Date().toISOString(),
      user.nomeAtualizadoEm || null,
      user.saldoAtualizadoEm || null,
      user.senhaAtualizadaEm || null
    ]
  );
}

async function getDepositoById(id) {
  const result = await pool.query(
    "SELECT * FROM depositos WHERE id = $1 LIMIT 1",
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

async function saveDeposito(dep) {
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

// =========================
// AUTH ADMIN
// =========================
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

// =========================
// HEALTH
// =========================
app.get("/", (req, res) => {
  res.json({ ok: true });
});

// =========================
// LOGIN ADMIN
// =========================
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
      [new Date().toISOString(), admin.id]
    );

    const token = signToken(admin);
    res.json({ token });
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: "Erro no login admin" });
  }
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
      id: "user_" + Date.now(),
      nome: emailNorm.split("@")[0],
      email: emailNorm,
      senha: hash,
      saldo: 0,
      criadoEm: new Date().toISOString(),
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

// =========================
// LOGIN USUÁRIO
// =========================
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
      saldo: Number(user.saldo || 0),
      criadoEm: user.criadoEm || null
    });
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: "Erro no login" });
  }
});

// =========================
// USUÁRIO
// =========================
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
      saldo: Number(user.saldo || 0),
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
    user.nomeAtualizadoEm = new Date().toISOString();

    await saveUser(user);

    res.json({ message: "Nome atualizado com sucesso" });
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: "Erro ao atualizar nome" });
  }
});

// =========================
// DEPÓSITOS
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

    const user = await getUserById(userId);

    if (!user) {
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
    pedido.comprovanteEnviadoEm = new Date().toISOString();

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

// =========================
// TRANSFERÊNCIA INTERNA
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
    const destino = await getUserByEmail(emailDestino);

    if (!remetente) {
      return res.status(404).json({ error: "Remetente não encontrado" });
    }

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

    await saveUser(remetente);
    await saveUser(destino);

    const agora = new Date().toISOString();

    await saveDeposito({
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
    });

    await saveDeposito({
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
    });

    res.json({
      message: "Transferência realizada com sucesso",
      saldoAtual: remetente.saldo
    });
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: "Erro na transferência" });
  }
});

// =========================
// ADMIN
// =========================
app.get("/usuarios", authAdmin, async (req, res) => {
  try {
    const result = await listUsers();
    res.json(result.map((u) => ({
      id: u.id,
      nome: u.nome,
      email: u.email,
      saldo: Number(u.saldo || 0),
      criadoEm: u.criadoEm || null
    })));
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

    const pedido = await getDepositoById(depositoId);
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

    if (pedido.tipoTransacao !== "saida" && !pedido.comprovanteUrl) {
      return res.status(400).json({ error: "Sem comprovante" });
    }

    const valorPedido = Number(pedido.valor || 0);

    if (pedido.tipoTransacao === "saida") {
      if (Number(usuario.saldo || 0) < valorPedido) {
        return res.status(400).json({ error: "Saldo insuficiente para aprovar saída" });
      }
      usuario.saldo = Number(usuario.saldo || 0) - valorPedido;
    } else {
      usuario.saldo = Number(usuario.saldo || 0) + valorPedido;
    }

    usuario.saldoAtualizadoEm = new Date().toISOString();
    pedido.status = "aprovado";
    pedido.aprovadoEm = new Date().toISOString();

    await saveUser(usuario);
    await saveDeposito(pedido);

    res.json({
      message: "Pedido aprovado com sucesso",
      pedido,
      saldoAtual: usuario.saldo
    });
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: "Erro ao aprovar pedido" });
  }
});

app.post("/recusar", authAdmin, async (req, res) => {
  try {
    const { depositoId } = req.body;

    const pedido = await getDepositoById(depositoId);
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

    await saveDeposito(pedido);

    res.json({
      message: "Pedido recusado com sucesso",
      pedido
    });
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: "Erro ao recusar pedido" });
  }
});

app.post("/admin/update-balance", authAdmin, async (req, res) => {
  try {
    const { userId, saldo } = req.body;

    if (!userId || saldo === undefined || saldo === null) {
      return res.status(400).json({ error: "userId e saldo são obrigatórios" });
    }

    const usuario = await getUserById(userId);

    if (!usuario) {
      return res.status(404).json({ error: "Usuário não encontrado" });
    }

    const saldoNumero = Number(saldo);

    if (!Number.isFinite(saldoNumero) || saldoNumero < 0) {
      return res.status(400).json({ error: "Saldo inválido" });
    }

    usuario.saldo = saldoNumero;
    usuario.saldoAtualizadoEm = new Date().toISOString();

    await saveUser(usuario);

    res.json({
      message: "Saldo atualizado com sucesso",
      user: {
        id: usuario.id,
        email: usuario.email,
        saldo: usuario.saldo
      }
    });
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: "Erro ao atualizar saldo" });
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
    usuario.senhaAtualizadaEm = new Date().toISOString();

    await saveUser(usuario);

    res.json({ message: "Senha redefinida com sucesso" });
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: "Erro ao redefinir senha" });
  }
});

// =========================
// START
// =========================
initDB()
  .then(() => {
    app.listen(PORT, () => {
      console.log("Servidor rodando");
    });
  })
  .catch((error) => {
    console.error("Erro ao iniciar banco:", error);
    process.exit(1);
  });