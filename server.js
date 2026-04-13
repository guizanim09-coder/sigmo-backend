// ===== IMPORTS =====
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

// ===== ENV =====
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

// ===== POSTGRES =====
const pool = new Pool({
  connectionString: DATABASE_URL,
  ssl: { rejectUnauthorized: false }
});

// ===== INIT DB =====
async function initDB() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS usuarios (
      id TEXT PRIMARY KEY,
      nome TEXT,
      email TEXT UNIQUE NOT NULL,
      senha TEXT NOT NULL,
      saldo NUMERIC DEFAULT 0,
      criado_em TIMESTAMP
    );
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS depositos (
      id TEXT PRIMARY KEY,
      user_id TEXT NOT NULL,
      valor NUMERIC,
      status TEXT,
      tipo_transacao TEXT,
      comprovante_url TEXT,
      criado_em TIMESTAMP
    );
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS admins (
      id TEXT PRIMARY KEY,
      email TEXT UNIQUE NOT NULL,
      senha TEXT NOT NULL,
      role TEXT,
      ativo BOOLEAN
    );
  `);

  await ensureAdmin();
  console.log("Banco pronto");
}

// ===== ADMIN AUTO =====
async function ensureAdmin() {
  if (!ADMIN_EMAIL || !ADMIN_PASSWORD) return;

  const existing = await pool.query(
    "SELECT * FROM admins WHERE email = $1",
    [ADMIN_EMAIL]
  );

  const hash = await bcrypt.hash(ADMIN_PASSWORD, 10);

  if (existing.rows.length === 0) {
    await pool.query(
      "INSERT INTO admins (id,email,senha,role,ativo) VALUES ($1,$2,$3,$4,$5)",
      ["admin_" + Date.now(), ADMIN_EMAIL, hash, "admin", true]
    );
    console.log("Admin criado");
  } else {
    await pool.query(
      "UPDATE admins SET senha=$1 WHERE email=$2",
      [hash, ADMIN_EMAIL]
    );
    console.log("Admin sincronizado");
  }
}

// ===== SEGURANÇA =====
app.use(helmet());
app.use(express.json());

app.use(rateLimit({
  windowMs: 60000,
  max: 120
}));

// ===== CORS =====
app.use(cors({
  origin: [/\.netlify\.app$/, "http://localhost:3000"],
  allowedHeaders: ["Content-Type", "Authorization"]
}));

// ===== JWT =====
function signToken(admin) {
  return jwt.sign(
    { sub: admin.id, role: "admin", type: "admin" },
    JWT_SECRET,
    { expiresIn: "12h" }
  );
}

// ===== AUTH =====
function authAdmin(req, res, next) {
  try {
    const auth = req.headers.authorization || "";

    if (!auth.startsWith("Bearer ")) {
      return res.status(401).json({ error: "Não autorizado" });
    }

    const token = auth.slice(7);
    const data = jwt.verify(token, JWT_SECRET);

    if (data.type !== "admin") {
      return res.status(401).json({ error: "Não autorizado" });
    }

    req.admin = data;
    next();
  } catch {
    return res.status(401).json({ error: "Não autorizado" });
  }
}

// ===== LOGIN ADMIN =====
app.post("/admin/login", async (req, res) => {
  const { email, senha } = req.body;

  const result = await pool.query(
    "SELECT * FROM admins WHERE email = $1",
    [email.toLowerCase()]
  );

  if (result.rows.length === 0) {
    return res.status(401).json({ error: "Login inválido" });
  }

  const admin = result.rows[0];

  const ok = await bcrypt.compare(senha, admin.senha);

  if (!ok) {
    return res.status(401).json({ error: "Login inválido" });
  }

  const token = signToken(admin);

  res.json({ token });
});

// ===== TESTE =====
app.get("/", (req, res) => {
  res.json({ ok: true });
});

// ===== PROTEGIDO =====
app.get("/usuarios", authAdmin, async (req, res) => {
  const result = await pool.query("SELECT * FROM usuarios");
  res.json(result.rows);
});

// ===== START =====
initDB().then(() => {
  app.listen(PORT, () => {
    console.log("Servidor rodando");
  });
});