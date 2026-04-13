// (mantive todo seu código original + melhorias)

// =========================
// ALTERAÇÃO PRINCIPAL:
// =========================

// 👇 ADICIONE NO REGISTER:
nome: emailNormalizado.split("@")[0],

// =========================
// NOVA ROTA: ATUALIZAR PERFIL
// =========================
app.post("/usuario/update-nome", (req, res) => {
  try {
    const { userId, nome } = req.body;

    if (!userId || !nome) {
      return res.status(400).json({ error: "Dados inválidos" });
    }

    const db = readDB();
    const user = db.usuarios.find(u => u.id === userId);

    if (!user) {
      return res.status(404).json({ error: "Usuário não encontrado" });
    }

    user.nome = String(nome).trim();

    writeDB(db);

    res.json({ message: "Nome atualizado com sucesso" });

  } catch {
    res.status(500).json({ error: "Erro ao atualizar nome" });
  }
});

// =========================
// NOVA ROTA: TRANSFERÊNCIA SIGMO
// =========================
app.post("/transferir-sigmo", (req, res) => {
  try {
    const { fromUserId, emailDestino, valor } = req.body;

    if (!fromUserId || !emailDestino || !valor) {
      return res.status(400).json({ error: "Dados obrigatórios" });
    }

    const valorNum = Number(valor);

    if (!Number.isFinite(valorNum) || valorNum <= 0) {
      return res.status(400).json({ error: "Valor inválido" });
    }

    const db = readDB();

    const remetente = db.usuarios.find(u => u.id === fromUserId);
    if (!remetente) {
      return res.status(404).json({ error: "Remetente não encontrado" });
    }

    const destino = db.usuarios.find(
      u => u.email.toLowerCase() === emailDestino.toLowerCase()
    );

    if (!destino) {
      return res.status(404).json({ error: "Usuário destino não encontrado" });
    }

    if (remetente.id === destino.id) {
      return res.status(400).json({ error: "Não pode transferir para si mesmo" });
    }

    if (Number(remetente.saldo) < valorNum) {
      return res.status(400).json({ error: "Saldo insuficiente" });
    }

    // 💸 ATUALIZA SALDO
    remetente.saldo -= valorNum;
    destino.saldo += valorNum;

    // 📄 REGISTRA SAÍDA
    db.depositos.push({
      id: "dep_" + Date.now(),
      userId: remetente.id,
      valor: valorNum,
      tipoTransacao: "saida",
      status: "aprovado",
      descricao: "Transferência enviada",
      criadoEm: new Date().toISOString()
    });

    // 📄 REGISTRA ENTRADA
    db.depositos.push({
      id: "dep_" + (Date.now() + 1),
      userId: destino.id,
      valor: valorNum,
      tipoTransacao: "entrada",
      status: "aprovado",
      descricao: "Transferência recebida",
      criadoEm: new Date().toISOString()
    });

    writeDB(db);

    res.json({
      message: "Transferência realizada com sucesso",
      saldoAtual: remetente.saldo
    });

  } catch {
    res.status(500).json({ error: "Erro na transferência" });
  }
});