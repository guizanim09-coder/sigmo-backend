const express = require("express");
const path = require("path");
const cors = require("cors");
const bcrypt = require("bcryptjs");
const multer = require("multer");
const helmet = require("helmet");
const rateLimit = require("express-rate-limit");
const { Pool } = require("pg");

const app = express();

const PORT = process.env.PORT || 3000;
const ADMIN_KEY = String(process.env.ADMIN_KEY || "").trim();
const DATABASE_URL = process.env.DATABASE_URL;

// =========================
// POSTGRES
// =========================
const pool = new Pool({
  connectionString: DATABASE_URL,
  ssl: { rejectUnauthorized: false }
});

async function initDB() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS usuarios (
      id TEXT PRIMARY KEY,
      nome TEXT,
      email TEXT UNIQUE,
      senha TEXT,
      saldo NUMERIC DEFAULT 0,
      criado_em TIMESTAMP
    );
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS depositos (
      id TEXT PRIMARY KEY,
      user_id TEXT,
      valor NUMERIC,
      tipo_transacao TEXT,
      status TEXT,
      comprovante_url TEXT,
      descricao TEXT,
      criado_em TIMESTAMP
    );
  `);

  console.log("Banco pronto");
}

// =========================
// SEGURANÇA
// =========================
app.use(helmet());

app.use(rateLimit({
  windowMs: 60 * 1000,
  max: 120
}));

const loginLimiter = rateLimit({
  windowMs: 60 * 1000,
  max: 5
});

app.use(cors({
  origin: true
}));

app.use(express.json());

// =========================
// UPLOAD
// =========================
const UPLOADS_DIR = path.join(__dirname, "uploads");

app.use("/uploads", express.static(UPLOADS_DIR));

const upload = multer({
  dest: UPLOADS_DIR
});

// =========================
// ADMIN
// =========================
function requireAdmin(req, res, next) {
  const key = req.headers["x-admin-key"];
  if (!ADMIN_KEY || key !== ADMIN_KEY) {
    return res.status(401).json({ error: "Não autorizado" });
  }
  next();
}

// =========================
// REGISTER
// =========================
app.post("/register", async (req, res) => {
  try {
    const { email, senha } = req.body;

    const emailNorm = email.toLowerCase();

    const existe = await pool.query(
      "SELECT id FROM usuarios WHERE email=$1",
      [emailNorm]
    );

    if (existe.rows.length) {
      return res.status(400).json({ error: "Usuário já existe" });
    }

    const hash = await bcrypt.hash(senha, 10);

    const user = {
      id: "user_" + Date.now(),
      nome: emailNorm.split("@")[0],
      email: emailNorm,
      senha: hash,
      saldo: 0,
      criado_em: new Date()
    };

    await pool.query(
      `INSERT INTO usuarios VALUES ($1,$2,$3,$4,$5,$6)`,
      [user.id, user.nome, user.email, user.senha, user.saldo, user.criado_em]
    );

    res.json(user);

  } catch {
    res.status(500).json({ error: "Erro registro" });
  }
});

// =========================
// LOGIN
// =========================
app.post("/login", loginLimiter, async (req, res) => {
  try {
    const { email, senha } = req.body;

    const result = await pool.query(
      "SELECT * FROM usuarios WHERE email=$1",
      [email.toLowerCase()]
    );

    const user = result.rows[0];

    if (!user) return res.status(401).json({ error: "Login inválido" });

    const ok = await bcrypt.compare(senha, user.senha);

    if (!ok) return res.status(401).json({ error: "Login inválido" });

    res.json({
      id: user.id,
      nome: user.nome,
      email: user.email,
      saldo: Number(user.saldo)
    });

  } catch {
    res.status(500).json({ error: "Erro login" });
  }
});

// =========================
// USUARIO
// =========================
app.get("/usuario/:id", async (req, res) => {
  const result = await pool.query(
    "SELECT * FROM usuarios WHERE id=$1",
    [req.params.id]
  );

  const user = result.rows[0];

  if (!user) return res.status(404).json({ error: "Não encontrado" });

  res.json({
    ...user,
    saldo: Number(user.saldo)
  });
});

// =========================
// CRIAR PEDIDO
// =========================
app.post("/deposito", async (req, res) => {
  const { userId, valor } = req.body;

  const dep = {
    id: "dep_" + Date.now(),
    user_id: userId,
    valor: Number(valor),
    tipo_transacao: "entrada",
    status: "pendente",
    criado_em: new Date()
  };

  await pool.query(
    `INSERT INTO depositos VALUES ($1,$2,$3,$4,$5,NULL,NULL,$6)`,
    [
      dep.id,
      dep.user_id,
      dep.valor,
      dep.tipo_transacao,
      dep.status,
      dep.criado_em
    ]
  );

  res.json(dep);
});

// =========================
// UPLOAD COMPROVANTE
// =========================
app.post("/deposito/:id/comprovante", upload.single("comprovante"), async (req, res) => {
  const url = "/uploads/" + req.file.filename;

  await pool.query(
    "UPDATE depositos SET comprovante_url=$1 WHERE id=$2",
    [url, req.params.id]
  );

  res.json({ message: "Enviado" });
});

// =========================
// LISTAR USER
// =========================
app.get("/depositos/user/:id", async (req, res) => {
  const result = await pool.query(
    "SELECT * FROM depositos WHERE user_id=$1 ORDER BY criado_em DESC",
    [req.params.id]
  );

  res.json(result.rows);
});

// =========================
// ADMIN
// =========================
app.get("/usuarios", requireAdmin, async (req, res) => {
  const result = await pool.query("SELECT * FROM usuarios");
  res.json(result.rows);
});

app.get("/depositos", requireAdmin, async (req, res) => {
  const result = await pool.query("SELECT * FROM depositos");
  res.json(result.rows);
});

// =========================
// APROVAR
// =========================
app.post("/aprovar", requireAdmin, async (req, res) => {
  const { depositoId } = req.body;

  const dep = await pool.query(
    "SELECT * FROM depositos WHERE id=$1",
    [depositoId]
  );

  const pedido = dep.rows[0];

  await pool.query(
    "UPDATE usuarios SET saldo = saldo + $1 WHERE id=$2",
    [pedido.valor, pedido.user_id]
  );

  await pool.query(
    "UPDATE depositos SET status='aprovado' WHERE id=$1",
    [depositoId]
  );

  res.json({ message: "Aprovado" });
});

// =========================
// START
// =========================
initDB().then(() => {
  app.listen(PORT, () => {
    console.log("Servidor rodando");
  });
});