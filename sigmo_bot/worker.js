require("dotenv").config();
const fetch = require("node-fetch");
const fs = require("fs");

const {
  setupLogin,
  capturarTransacoes,
  resetBrowser
} = require("./dentpeg-bot");

// 🔒 CONFIG PRODUÇÃO
const LOOP_INTERVAL = 4000; // mais rápido
const MAX_CONCURRENCY = 3; // paralelismo controlado
const RETRY_LIMIT = 3;

// 🔒 cache persistente
const CACHE_FILE = "./txids.json";

let txidsProcessados = new Set();
let fila = [];
let executando = false;
let ultimoLoop = Date.now();

// 🔄 carregar cache
if (fs.existsSync(CACHE_FILE)) {
  try {
    const data = JSON.parse(fs.readFileSync(CACHE_FILE));
    txidsProcessados = new Set(data);
  } catch {
    txidsProcessados = new Set();
  }
}

// 🔥 LOGIN
if (process.argv.includes("setup")) {
  setupLogin();
  return;
}

// 🔥 valida env
if (!process.env.BACKEND_URL) {
  console.log("❌ BACKEND_URL não definida");
  process.exit(1);
}

// 💾 salvar cache
function salvarCache() {
  try {
    fs.writeFileSync(CACHE_FILE, JSON.stringify([...txidsProcessados]));
  } catch (e) {
    console.log("⚠️ Erro cache:", e.message);
  }
}

// 🔥 ENVIO COM RETRY
async function enviarParaBackend(tx, tentativa = 1) {
  try {
    if (!tx.txid || tx.txid.length < 10) return false;
    if (!tx.valorLiquido || tx.valorLiquido <= 0) return false;

    const payload = {
      txid: tx.txid,
      valorLiquido: tx.valorLiquido,
      nomePagador: tx.nomePagador || null,
      dataHora: tx.dataHora || null,
      idTransacao: tx.idTransacao || null,
      raw: tx.raw || null
    };

    const res = await fetch(process.env.BACKEND_URL + "/deposito/confirmar-bot", {
      method: "POST",
      headers: {
        "Content-Type": "application/json"
      },
      body: JSON.stringify(payload),
      timeout: 10000
    });

    if (!res.ok) {
      throw new Error(await res.text());
    }

    console.log("✅ Enviado:", tx.txid);
    return true;

  } catch (e) {
    if (tentativa < RETRY_LIMIT) {
      console.log("🔁 Retry:", tx.txid, "| tentativa", tentativa);
      await new Promise(r => setTimeout(r, 1000));
      return enviarParaBackend(tx, tentativa + 1);
    }

    console.log("❌ Falha final:", tx.txid, e.message);
    return false;
  }
}

// 🔥 PROCESSAMENTO PARALELO
async function processarFila() {
  const chunk = fila.splice(0, MAX_CONCURRENCY);

  await Promise.all(chunk.map(async (tx) => {
    const sucesso = await enviarParaBackend(tx);

    if (sucesso) {
  txidsProcessados.add(tx.txid);
  salvarCache();
} else {
  console.log("❌ Ignorado após falha:", tx.txid);

  // 🔥 marca como processado para não travar fila
  txidsProcessados.add(tx.txid);
  salvarCache();
}
  }));
}

// 🔥 LOOP PRINCIPAL
ultimoLoop = Date.now();
async function loop() {
  ultimoLoop = Date.now(); // 🔥 ESSENCIAL

  if (executando) return;
  executando = true;

  try {
    const transacoes = await capturarTransacoes();

    if (!transacoes || transacoes.length === 0) {
      console.log("🔍 Nenhuma transação...");
      return;
    }

    console.log("📊 Capturadas:", transacoes.length);

    for (const tx of transacoes) {
      if (!tx.txid || tx.txid.length < 10) continue;
      if (txidsProcessados.has(tx.txid)) continue;

      // evita duplicar na fila
      if (!fila.find(t => t.txid === tx.txid)) {
        fila.push(tx);
      }
    }

    console.log("📦 Fila:", fila.length);

    await processarFila();

  } catch (e) {
    console.log("❌ Loop erro:", e.message);

    // 🔥 RECUPERAÇÃO AUTOMÁTICA
    console.log("🔄 Reiniciando processo por erro...");
process.exit(1);
  } finally {
    executando = false;
  }
}

// 🔥 WATCHDOG (ANTI-TRAVA)
setInterval(() => {
  const tempoParado = Date.now() - ultimoLoop;

  if (tempoParado > 60000) { // 🔥 60 segundos (IDEAL)
    console.log("💥 Bot travou de verdade, reiniciando processo...");
    process.exit(1);
  }
}, 30000);

// 🚀 START
async function start() {
  console.log("🚀 BOT PRODUÇÃO INICIADO");
  console.log("🔗 Backend:", process.env.BACKEND_URL);

  await loop();
  setInterval(loop, LOOP_INTERVAL);
}

start();