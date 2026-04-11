const express = require("express");
const fs = require("fs");
const path = require("path");
const cors = require("cors");
const bcrypt = require("bcryptjs");

const app = express();

app.use(express.json());
app.use(cors());

const PORT = process.env.PORT || 3000;
const DB_FILE = path.join(__dirname, "database.json");
const ADMIN_KEY = String(process.env.ADMIN_KEY || "").trim();

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
      saldo: novoUsuario.saldo,
      criadoEm: novoUsuario.criadoEm
    });
  } catch (error) {
    return res.status(500).json({ error: "Erro interno no registro" });
  }
});

// =========================
// LOGIN
// =========================
app.post("/login", async (req, res) => {
  try {
    const { email, senha } = req.body;

    if (!email || !senha) {
      return res.status(400).json({ error: "Email e senha são obrigatórios" });
    }

    const emailNormalizado = String(email).trim().toLowerCase();
    const db = readDB();

    const user = db.usuarios.find(
      (u) => String(u.email).toLowerCase() === emailNormalizado
    );

    if (!user) {
      return res.status(401).json({ error: "Login inválido" });
    }

    const senhaOk = await bcrypt.compare(String(senha), String(user.senha));

    if (!senhaOk) {
      return res.status(401).json({ error: "Login inválido" });
    }

    return res.json({
      id: user.id,
      email: user.email,
      saldo: Number(user.saldo || 0),
      criadoEm: user.criadoEm || null
    });
  } catch (error) {
    return res.status(500).json({ error: "Erro interno no login" });
  }
});

// =========================
// USUÁRIO LOGADO
// =========================
app.get("/usuario/:id", (req, res) => {
  try {
    const { id } = req.params;
    const db = readDB();

    const user = db.usuarios.find((u) => u.id === id);

    if (!user) {
      return res.status(404).json({ error: "Usuário não encontrado" });
    }

    return res.json({
      id: user.id,
      email: user.email,
      saldo: Number(user.saldo || 0),
      criadoEm: user.criadoEm || null
    });
  } catch (error) {
    return res.status(500).json({ error: "Erro ao buscar usuário" });
  }
});

// =========================
// CRIAR PEDIDO
// entrada | saida
// =========================
app.post("/deposito", (req, res) => {
  try {
    const {
      userId,
      valor,
      chavePix,
      tipoChave,
      tipoTransacao
    } = req.body;

    if (!userId || valor === undefined || valor === null) {
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
      tipoTransacao: tipoTransacao || "entrada",
      status: "pendente",
      criadoEm: new Date().toISOString()
    };

    db.depositos.push(pedido);
    writeDB(db);

    return res.status(201).json(pedido);
  } catch (error) {
    return res.status(500).json({ error: "Erro interno ao criar pedido" });
  }
});

// =========================
// PEDIDOS DO USUÁRIO
// =========================
app.get("/depositos/user/:userId", (req, res) => {
  try {
    const { userId } = req.params;
    const db = readDB();

    const pedidos = db.depositos
      .filter((d) => d.userId === userId)
      .sort((a, b) => new Date(b.criadoEm || 0) - new Date(a.criadoEm || 0));

    return res.json(pedidos);
  } catch (error) {
    return res.status(500).json({ error: "Erro ao buscar pedidos do usuário" });
  }
});

// =========================
// ADMIN - LISTAR USUÁRIOS
// =========================
app.get("/usuarios", requireAdmin, (req, res) => {
  try {
    const db = readDB();

    const usuariosSeguros = db.usuarios.map((u) => ({
      id: u.id,
      email: u.email,
      saldo: Number(u.saldo || 0),
      criadoEm: u.criadoEm || null
    }));

    return res.json(usuariosSeguros);
  } catch (error) {
    return res.status(500).json({ error: "Erro ao listar usuários" });
  }
});

// =========================
// ADMIN - LISTAR PEDIDOS
// =========================
app.get("/depositos", requireAdmin, (req, res) => {
  try {
    const db = readDB();

    const pedidos = [...db.depositos].sort(
      (a, b) => new Date(b.criadoEm || 0) - new Date(a.criadoEm || 0)
    );

    return res.json(pedidos);
  } catch (error) {
    return res.status(500).json({ error: "Erro ao listar pedidos" });
  }
});

// =========================
// ADMIN - APROVAR PEDIDO
// entrada soma
// saida subtrai
// =========================
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

    if (pedido.status === "recusado") {
      return res.status(400).json({ error: "Pedido já recusado" });
    }

    const usuario = db.usuarios.find((u) => u.id === pedido.userId);
    if (!usuario) {
      return res.status(404).json({ error: "Usuário do pedido não encontrado" });
    }

    const saldoAtual = Number(usuario.saldo || 0);

    if (pedido.tipoTransacao === "saida") {
      if (saldoAtual < Number(pedido.valor)) {
        return res.status(400).json({ error: "Saldo insuficiente para aprovar saída" });
      }

      usuario.saldo = saldoAtual - Number(pedido.valor);
    } else {
      usuario.saldo = saldoAtual + Number(pedido.valor);
    }

    pedido.status = "aprovado";
    pedido.aprovadoEm = new Date().toISOString();

    writeDB(db);

    return res.json({
      message: "Pedido aprovado com sucesso",
      pedido,
      saldoAtual: usuario.saldo
    });
  } catch (error) {
    return res.status(500).json({ error: "Erro ao aprovar pedido" });
  }
});

// =========================
// ADMIN - RECUSAR PEDIDO
// =========================
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

    return res.json({
      message: "Pedido recusado com sucesso",
      pedido
    });
  } catch (error) {
    return res.status(500).json({ error: "Erro ao recusar pedido" });
  }
});

// =========================
// ADMIN - AJUSTAR SALDO
// =========================
app.post("/admin/update-balance", requireAdmin, (req, res) => {
  try {
    const { userId, saldo } = req.body;

    if (!userId || saldo === undefined || saldo === null) {
      return res.status(400).json({ error: "userId e saldo são obrigatórios" });
    }

    const saldoNumero = Number(saldo);

    if (!Number.isFinite(saldoNumero) || saldoNumero < 0) {
      return res.status(400).json({ error: "Saldo inválido" });
    }

    const db = readDB();
    const usuario = db.usuarios.find((u) => u.id === userId);

    if (!usuario) {
      return res.status(404).json({ error: "Usuário não encontrado" });
    }

    usuario.saldo = saldoNumero;
    usuario.saldoAtualizadoEm = new Date().toISOString();

    writeDB(db);

    return res.json({
      message: "Saldo atualizado com sucesso",
      user: {
        id: usuario.id,
        email: usuario.email,
        saldo: usuario.saldo
      }
    });
  } catch (error) {
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

    const db = readDB();
    const usuario = db.usuarios.find((u) => u.id === userId);

    if (!usuario) {
      return res.status(404).json({ error: "Usuário não encontrado" });
    }

    const senhaHash = await bcrypt.hash(String(novaSenha), 10);
    usuario.senha = senhaHash;
    usuario.senhaAtualizadaEm = new Date().toISOString();

    writeDB(db);

    return res.json({
      message: "Senha redefinida com sucesso"
    });
  } catch (error) {
    return res.status(500).json({ error: "Erro ao redefinir senha" });
  }
});

app.listen(PORT, () => {
  console.log(`Servidor rodando na porta ${PORT}`);
});