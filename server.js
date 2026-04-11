const express = require("express");
const fs = require("fs");
const cors = require("cors");
const path = require("path");
const bcrypt = require("bcryptjs");

const app = express();

app.use(express.json());
app.use(cors());

const PORT = process.env.PORT || 3000;
const DB_FILE = path.join(__dirname, "database.json");

// Defina essa chave no Railway
// Exemplo: ADMIN_KEY = minha-chave-super-segura-123
const ADMIN_KEY = process.env.ADMIN_KEY || "troque-essa-chave-agora";

// ===== BANCO =====
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

// ===== MIDDLEWARE ADMIN =====
function requireAdmin(req, res, next) {
  const adminKey = req.headers["x-admin-key"];

  if (!adminKey || adminKey !== ADMIN_KEY) {
    return res.status(401).json({ error: "Acesso não autorizado" });
  }

  next();
}

// ===== HEALTH CHECK =====
app.get("/", (req, res) => {
  res.json({ ok: true, message: "Sigmo backend online" });
});

// ===== REGISTRO =====
app.post("/register", async (req, res) => {
  try {
    const { email, senha } = req.body;

    if (!email || !senha) {
      return res.status(400).json({ error: "Email e senha são obrigatórios" });
    }

    const db = readDB();

    const existe = db.usuarios.find(
      (u) => u.email.toLowerCase() === String(email).toLowerCase()
    );

    if (existe) {
      return res.status(400).json({ error: "Usuário já existe" });
    }

    const senhaHash = await bcrypt.hash(senha, 10);

    const novoUsuario = {
      id: "user_" + Date.now(),
      email: String(email).trim().toLowerCase(),
      senha: senhaHash,
      saldo: 0,
      criadoEm: new Date().toISOString()
    };

    db.usuarios.push(novoUsuario);
    writeDB(db);

    res.status(201).json({
      id: novoUsuario.id,
      email: novoUsuario.email,
      saldo: novoUsuario.saldo
    });
  } catch (error) {
    res.status(500).json({ error: "Erro interno no registro" });
  }
});

// ===== LOGIN =====
app.post("/login", async (req, res) => {
  try {
    const { email, senha } = req.body;

    if (!email || !senha) {
      return res.status(400).json({ error: "Email e senha são obrigatórios" });
    }

    const db = readDB();

    const user = db.usuarios.find(
      (u) => u.email.toLowerCase() === String(email).toLowerCase()
    );

    if (!user) {
      return res.status(401).json({ error: "Login inválido" });
    }

    const senhaOk = await bcrypt.compare(senha, user.senha);

    if (!senhaOk) {
      return res.status(401).json({ error: "Login inválido" });
    }

    res.json({
      id: user.id,
      email: user.email,
      saldo: Number(user.saldo || 0)
    });
  } catch (error) {
    res.status(500).json({ error: "Erro interno no login" });
  }
});

// ===== CRIAR PEDIDO =====
// Mantém tudo junto: entrada, saída, QR, chave digitada
app.post("/deposito", (req, res) => {
  try {
    const {
      userId,
      valor,
      chavePix,
      tipoChave,
      tipoTransacao
    } = req.body;

    if (!userId || !valor) {
      return res.status(400).json({ error: "userId e valor são obrigatórios" });
    }

    const valorNumero = Number(valor);

    if (!Number.isFinite(valorNumero) || valorNumero <= 0) {
      return res.status(400).json({ error: "Valor inválido" });
    }

    const db = readDB();

    const usuarioExiste = db.usuarios.find((u) => u.id === userId);
    if (!usuarioExiste) {
      return res.status(404).json({ error: "Usuário não encontrado" });
    }

    const pedido = {
      id: "dep_" + Date.now(),
      userId,
      valor: valorNumero,
      chavePix: chavePix || "",
      tipoChave: tipoChave || "",
      tipoTransacao: tipoTransacao || "entrada", // entrada | saida
      status: "pendente",
      criadoEm: new Date().toISOString()
    };

    db.depositos.push(pedido);
    writeDB(db);

    res.status(201).json(pedido);
  } catch (error) {
    res.status(500).json({ error: "Erro interno ao criar pedido" });
  }
});

// ===== USUÁRIOS (ADMIN) =====
app.get("/usuarios", requireAdmin, (req, res) => {
  try {
    const db = readDB();

    const usuariosSeguros = db.usuarios.map((u) => ({
      id: u.id,
      email: u.email,
      saldo: Number(u.saldo || 0),
      criadoEm: u.criadoEm || null
    }));

    res.json(usuariosSeguros);
  } catch (error) {
    res.status(500).json({ error: "Erro ao listar usuários" });
  }
});

// ===== PEDIDOS (ADMIN) =====
app.get("/depositos", requireAdmin, (req, res) => {
  try {
    const db = readDB();
    res.json(db.depositos);
  } catch (error) {
    res.status(500).json({ error: "Erro ao listar pedidos" });
  }
});

// ===== APROVAR PEDIDO (ADMIN) =====
app.post("/aprovar", requireAdmin, (req, res) => {
  try {
    const { depositoId } = req.body;

    if (!depositoId) {
      return res.status(400).json({ error: "depositoId é obrigatório" });
    }

    const db = readDB();

    const pedido = db.depositos.find((d) => d.id === depositoId);
    if (!pedido) {
      return res.status(404).json({ error: "Pedido não encontrado" });
    }

    if (pedido.status === "aprovado") {
      return res.status(400).json({ error: "Pedido já aprovado" });
    }

    const usuario = db.usuarios.find((u) => u.id === pedido.userId);
    if (!usuario) {
      return res.status(404).json({ error: "Usuário do pedido não encontrado" });
    }

    pedido.status = "aprovado";
    pedido.aprovadoEm = new Date().toISOString();

    // Regra:
    // entrada = soma saldo
    // saida = subtrai saldo
    if (pedido.tipoTransacao === "saida") {
      const saldoAtual = Number(usuario.saldo || 0);

      if (saldoAtual < pedido.valor) {
        pedido.status = "recusado";
        pedido.recusadoEm = new Date().toISOString();
        writeDB(db);

        return res.status(400).json({ error: "Saldo insuficiente para aprovar saída" });
      }

      usuario.saldo = saldoAtual - pedido.valor;
    } else {
      usuario.saldo = Number(usuario.saldo || 0) + pedido.valor;
    }

    writeDB(db);

    res.json({
      message: "Pedido aprovado com sucesso",
      pedido,
      saldoAtual: usuario.saldo
    });
  } catch (error) {
    res.status(500).json({ error: "Erro ao aprovar pedido" });
  }
});

// ===== RECUSAR PEDIDO (ADMIN) =====
app.post("/recusar", requireAdmin, (req, res) => {
  try {
    const { depositoId } = req.body;

    if (!depositoId) {
      return res.status(400).json({ error: "depositoId é obrigatório" });
    }

    const db = readDB();

    const pedido = db.depositos.find((d) => d.id === depositoId);
    if (!pedido) {
      return res.status(404).json({ error: "Pedido não encontrado" });
    }

    if (pedido.status === "aprovado") {
      return res.status(400).json({ error: "Pedido já aprovado, não pode recusar" });
    }

    pedido.status = "recusado";
    pedido.recusadoEm = new Date().toISOString();

    writeDB(db);

    res.json({ message: "Pedido recusado com sucesso", pedido });
  } catch (error) {
    res.status(500).json({ error: "Erro ao recusar pedido" });
  }
});

// ===== BUSCAR DADOS DO USUÁRIO LOGADO =====
app.get("/usuario/:id", (req, res) => {
  try {
    const { id } = req.params;
    const db = readDB();

    const user = db.usuarios.find((u) => u.id === id);

    if (!user) {
      return res.status(404).json({ error: "Usuário não encontrado" });
    }

    res.json({
      id: user.id,
      email: user.email,
      saldo: Number(user.saldo || 0),
      criadoEm: user.criadoEm || null
    });
  } catch (error) {
    res.status(500).json({ error: "Erro ao buscar usuário" });
  }
});

// ===== PEDIDOS DO USUÁRIO =====
app.get("/depositos/user/:userId", (req, res) => {
  try {
    const { userId } = req.params;
    const db = readDB();

    const pedidos = db.depositos.filter((d) => d.userId === userId);

    res.json(pedidos);
  } catch (error) {
    res.status(500).json({ error: "Erro ao buscar pedidos do usuário" });
  }
});

app.listen(PORT, () => {
  console.log(`Servidor rodando na porta ${PORT}`);
});