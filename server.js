const express = require("express");
const fs = require("fs");
const path = require("path");
const cors = require("cors");
const bcrypt = require("bcryptjs");
const multer = require("multer");

const app = express();

app.use(express.json());
app.use(cors());

const PORT = process.env.PORT || 3000;
const DB_FILE = path.join(__dirname, "database.json");
const ADMIN_KEY = String(process.env.ADMIN_KEY || "").trim();

// =========================
// UPLOAD CONFIG
// =========================
const UPLOADS_DIR = path.join(__dirname, "uploads");

if (!fs.existsSync(UPLOADS_DIR)) {
  fs.mkdirSync(UPLOADS_DIR, { recursive: true });
}

app.use("/uploads", express.static(UPLOADS_DIR));

const storage = multer.diskStorage({
  destination: function (req, file, cb) {
    cb(null, UPLOADS_DIR);
  },
  filename: function (req, file, cb) {
    const ext = path.extname(file.originalname || "");
    cb(null, "comp_" + Date.now() + ext);
  }
});

const upload = multer({
  storage,
  limits: { fileSize: 8 * 1024 * 1024 }
});

// =========================
// BANCO
// =========================
function ensureDB() {
  if (!fs.existsSync(DB_FILE)) {
    const initialData = {
      usuarios: [],
      depositos: []
    };
    fs.writeFileSync(DB_FILE, JSON.stringify(initialData, null, 2));
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
// SEGURANÇA ADMIN
// =========================
function requireAdmin(req, res, next) {
  const adminKey = String(req.headers["x-admin-key"] || "").trim();

  if (!ADMIN_KEY || !adminKey || adminKey !== ADMIN_KEY) {
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
// REGISTRO
// =========================
app.post("/register", async (req, res) => {
  try {
    const { email, senha } = req.body;

    if (!email || !senha) {
      return res.status(400).json({ error: "Email e senha são obrigatórios" });
    }

    const emailNormalizado = String(email).trim().toLowerCase();
    const db = readDB();

    const existe = db.usuarios.find(
      (u) => String(u.email).toLowerCase() === emailNormalizado
    );

    if (existe) {
      return res.status(400).json({ error: "Usuário já existe" });
    }

    const senhaHash = await bcrypt.hash(String(senha), 10);

    const novoUsuario = {
      id: "user_" + Date.now(),
      email: emailNormalizado,
      senha: senhaHash,
      saldo: 0,
      criadoEm: new Date().toISOString()
    };

    db.usuarios.push(novoUsuario);
    writeDB(db);

    return res.status(201).json({
      id: novoUsuario.id,
      email: novoUsuario.email,
      saldo: novoUsuario.saldo
    });
  } catch {
    return res.status(500).json({ error: "Erro interno no registro" });
  }
});

// =========================
// LOGIN
// =========================
app.post("/login", async (req, res) => {
  try {
    const { email, senha } = req.body;

    const db = readDB();
    const user = db.usuarios.find(
      (u) => u.email === String(email).trim().toLowerCase()
    );

    if (!user) return res.status(401).json({ error: "Login inválido" });

    const ok = await bcrypt.compare(senha, user.senha);
    if (!ok) return res.status(401).json({ error: "Login inválido" });

    return res.json({
      id: user.id,
      email: user.email,
      saldo: Number(user.saldo || 0)
    });
  } catch {
    return res.status(500).json({ error: "Erro no login" });
  }
});

// =========================
// CRIAR PEDIDO
// =========================
app.post("/deposito", (req, res) => {
  try {
    const { userId, valor, tipoTransacao } = req.body;

    const db = readDB();

    const pedido = {
      id: "dep_" + Date.now(),
      userId,
      valor: Number(valor),
      tipoTransacao: tipoTransacao || "entrada",
      status: "pendente",
      comprovanteUrl: "",
      criadoEm: new Date().toISOString()
    };

    db.depositos.push(pedido);
    writeDB(db);

    res.json(pedido);
  } catch {
    res.status(500).json({ error: "Erro ao criar pedido" });
  }
});

// =========================
// ANEXAR COMPROVANTE
// =========================
app.post("/deposito/:id/comprovante", upload.single("comprovante"), (req, res) => {
  try {
    const db = readDB();
    const pedido = db.depositos.find(d => d.id === req.params.id);

    if (!pedido) {
      return res.status(404).json({ error: "Pedido não encontrado" });
    }

    if (!req.file) {
      return res.status(400).json({ error: "Arquivo obrigatório" });
    }

    pedido.comprovanteUrl = "/uploads/" + req.file.filename;

    writeDB(db);

    res.json({ ok: true });
  } catch {
    res.status(500).json({ error: "Erro upload" });
  }
});

// =========================
// LISTAR PEDIDOS
// =========================
app.get("/depositos", requireAdmin, (req, res) => {
  const db = readDB();
  res.json(db.depositos);
});

// =========================
// APROVAR
// =========================
app.post("/aprovar", requireAdmin, (req, res) => {
  const db = readDB();
  const pedido = db.depositos.find(d => d.id === req.body.depositoId);

  if (!pedido) return res.status(404).json({ error: "Não encontrado" });

  // BLOQUEIO IMPORTANTE
  if (pedido.tipoTransacao !== "saida" && !pedido.comprovanteUrl) {
    return res.status(400).json({ error: "Sem comprovante" });
  }

  const user = db.usuarios.find(u => u.id === pedido.userId);

  if (pedido.tipoTransacao === "saida") {
    user.saldo -= pedido.valor;
  } else {
    user.saldo += pedido.valor;
  }

  pedido.status = "aprovado";

  writeDB(db);

  res.json({ ok: true });
});

// =========================
// RECUSAR
// =========================
app.post("/recusar", requireAdmin, (req, res) => {
  const db = readDB();
  const pedido = db.depositos.find(d => d.id === req.body.depositoId);

  if (!pedido) return res.status(404).json({ error: "Não encontrado" });

  pedido.status = "recusado";
  writeDB(db);

  res.json({ ok: true });
});

app.listen(PORT, () => {
  console.log("Servidor rodando");
});