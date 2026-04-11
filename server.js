const express = require("express");
const fs = require("fs");
const cors = require("cors");
const path = require("path");

const app = express();
app.use(express.json());
app.use(cors());

const DB_FILE = path.join(__dirname, "database.json");

// ===== DB =====
function readDB() {
  if (!fs.existsSync(DB_FILE)) {
    fs.writeFileSync(DB_FILE, JSON.stringify({ usuarios: [], depositos: [] }, null, 2));
  }
  return JSON.parse(fs.readFileSync(DB_FILE));
}

function writeDB(data) {
  fs.writeFileSync(DB_FILE, JSON.stringify(data, null, 2));
}

// ===== REGISTRO =====
app.post("/register", (req, res) => {
  const { email, senha } = req.body;
  let db = readDB();

  if (db.usuarios.find(u => u.email === email)) {
    return res.status(400).send("Usuário já existe");
  }

  const novo = {
    id: "user_" + Date.now(),
    email,
    senha,
    saldo: 0
  };

  db.usuarios.push(novo);
  writeDB(db);

  res.send(novo);
});

// ===== LOGIN =====
app.post("/login", (req, res) => {
  const { email, senha } = req.body;
  let db = readDB();

  const user = db.usuarios.find(
    u => u.email === email && u.senha === senha
  );

  if (!user) return res.status(401).send("Login inválido");

  res.send(user);
});

// ===== CRIAR DEPÓSITO =====
app.post("/deposito", (req, res) => {
  const { userId, valor } = req.body;
  let db = readDB();

  const deposito = {
    id: "dep_" + Date.now(),
    userId,
    valor,
    status: "pendente"
  };

  db.depositos.push(deposito);
  writeDB(db);

  res.send(deposito);
});

// ===== LISTAR DEPÓSITOS =====
app.get("/depositos", (req, res) => {
  const db = readDB();
  res.send(db.depositos);
});

// ===== APROVAR DEPÓSITO =====
app.post("/aprovar", (req, res) => {
  const { depositoId } = req.body;
  let db = readDB();

  const deposito = db.depositos.find(d => d.id === depositoId);
  if (!deposito) return res.status(404).send("Depósito não encontrado");

  if (deposito.status === "aprovado") {
    return res.send("Já aprovado");
  }

  deposito.status = "aprovado";

  const user = db.usuarios.find(u => u.id === deposito.userId);
  if (user) {
    user.saldo += Number(deposito.valor);
  }

  writeDB(db);
  res.send("Depósito aprovado");
});

// ===== LISTAR USUÁRIOS =====
app.get("/usuarios", (req, res) => {
  const db = readDB();
  res.send(db.usuarios);
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log("rodando"));