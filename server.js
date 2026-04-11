const express = require("express");
const fs = require("fs");
const cors = require("cors");

const app = express();
app.use(express.json());
app.use(cors());

const DB_FILE = "./database.json";

// ===== FUNÇÕES =====
function readDB() {
  if (!fs.existsSync(DB_FILE)) {
    fs.writeFileSync(DB_FILE, JSON.stringify({ usuarios: [] }, null, 2));
  }
  return JSON.parse(fs.readFileSync(DB_FILE));
}

function writeDB(data) {
  fs.writeFileSync(DB_FILE, JSON.stringify(data, null, 2));
}

// ===== REGISTRO =====
app.post("/register", (req, res) => {
  const { email, senha } = req.body;

  if (!email || !senha) {
    return res.status(400).send("Dados inválidos");
  }

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

  if (!user) {
    return res.status(401).send("Login inválido");
  }

  res.send(user);
});

// ===== LISTAR USUÁRIOS (pra você ver no navegador) =====
app.get("/usuarios", (req, res) => {
  const db = readDB();
  res.send(db.usuarios);
});

const PORT = process.env.PORT || 3000;

app.listen(PORT, () => {
  console.log("Servidor rodando");
});