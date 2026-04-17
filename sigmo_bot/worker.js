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
let ultimaAtividade = Date.now();
let resetEmAndamento = false;

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
    if (!tx.valorLiquido || tx.valorLiquido <= 0) return false;
    if (!tx.valorLiquido || tx.valorLiquido <= 0) return false;

    const payload = {
  txid: tx.txid || null,
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
  const chave = tx.txid || `${tx.valorLiquido}-${tx.dataHora}-${tx.nomePagador}`;
  txidsProcessados.add(chave);
  salvarCache();
} else {
  if (fila.length < 500) {
    fila.push(tx);
  }
}
}
  }));
}

// 🔥 LOOP PRINCIPAL
async function loop() {
 if (executando) return;
executando = true;
ultimaAtividade = Date.now();

  try {
    const transacoes = await capturarTransacoes();
ultimaAtividade = Date.now();

    if (!transacoes || transacoes.length === 0) {
      console.log("🔍 Nenhuma transação...");
ultimaAtividade = Date.now();
return;
    }

    console.log("📊 Capturadas:", transacoes.length);

let jaProcessadosSeguidos = 0;

for (const tx of transacoes) {
  const chave = tx.txid || `${tx.valorLiquido}-${tx.dataHora}-${tx.nomePagador}`;

  if (txidsProcessados.has(chave)) {
    jaProcessadosSeguidos++;

    if (jaProcessadosSeguidos >= 5) {
      console.log("⛔ 5 já processados seguidos, parando varredura");
      break;
    }

    continue;
  }

  jaProcessadosSeguidos = 0;

  if (!tx.valorLiquido || tx.valorLiquido <= 0) continue;

  if (!fila.find(t => {
    const chaveFila = t.txid || `${t.valorLiquido}-${t.dataHora}-${t.nomePagador}`;
    return chaveFila === chave;
  })) {
    fila.push(tx);
  }
}

    console.log("📦 Fila:", fila.length);

    await processarFila();
ultimaAtividade = Date.now();

  } catch (e) {
    console.log("❌ Loop erro:", e.message);

    // 🔥 RECUPERAÇÃO AUTOMÁTICA
    await resetBrowser();
  } finally {
    executando = false;
  }
}

// 🔥 WATCHDOG (ANTI-TRAVA)
setInterval(async () => {
  const agora = Date.now();
  const travado = executando && (agora - ultimaAtividade > 60000);

  if (!travado || resetEmAndamento) return;

  resetEmAndamento = true;
  console.log("⚠️ Travou de verdade, resetando browser...");

  try {
    await resetBrowser();
  } catch (e) {
    console.log("Erro reset:", e.message);
  } finally {
    executando = false;
    ultimaAtividade = Date.now();
    resetEmAndamento = false;
  }
}, 15000);

// 🚀 START
async function start() {
  console.log("🚀 BOT PRODUÇÃO INICIADO");
  console.log("🔗 Backend:", process.env.BACKEND_URL);

  await loop();
  setInterval(loop, LOOP_INTERVAL);
}

start();