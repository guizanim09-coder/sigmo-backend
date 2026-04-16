require("dotenv").config();
const fetch = require("node-fetch");
const fs = require("fs");
const Tesseract = require("tesseract.js");

const {
  setupLogin,
  capturarTransacoes,
  resetBrowser
} = require("./dentpeg-bot");

// 🔒 CONFIG PRODUÇÃO
const LOOP_INTERVAL = 4000;
const MAX_CONCURRENCY = 3;
const RETRY_LIMIT = 2;

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

// 🔥 OCR PARA IMAGEM
async function lerTextoImagem(caminho) {
  try {
    const { data: { text } } = await Tesseract.recognize(
      caminho,
      "por"
    );

    console.log("🧠 OCR EXTRAÍDO:", text.slice(0, 200));
    return text;
  } catch (e) {
    console.log("❌ Erro OCR:", e.message);
    return "";
  }
}

// 🔥 NORMALIZAÇÃO (IMPORTANTE PRA MATCH)
function normalizarTexto(txt) {
  return (txt || "")
    .toUpperCase()
    .replace(/[^A-Z0-9 ]/g, "")
    .trim();
}

// 🔥 ENVIO
async function enviarParaBackend(tx, tentativa = 1) {
  try {
    if (!tx.txid || tx.txid.length < 5) return false;

    let nomePagador = tx.nomePagador || null;

    // 🔥 SE NÃO TEM NOME → TENTA OCR
    if (!nomePagador && tx.imagemComprovante) {
      const textoOCR = await lerTextoImagem(tx.imagemComprovante);

      // tentativa simples de pegar nome
      nomePagador = textoOCR.split("\n")[0] || null;
    }

    const payload = {
      txid: tx.txid,
      valorLiquido: tx.valorLiquido,
      nomePagador: normalizarTexto(nomePagador),
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

    console.log("✅ Aprovado:", tx.txid);
    return true;

  } catch (e) {
    if (tentativa < RETRY_LIMIT) {
      console.log("🔁 Retry:", tx.txid);
      await new Promise(r => setTimeout(r, 1000));
      return enviarParaBackend(tx, tentativa + 1);
    }

    console.log("❌ Falha final:", tx.txid, e.message);
    return false;
  }
}

// 🔥 PROCESSAMENTO
async function processarFila() {
  const chunk = fila.splice(0, MAX_CONCURRENCY);

  await Promise.all(chunk.map(async (tx) => {
    const sucesso = await enviarParaBackend(tx);

    // 🔥 SEM LOOP INFINITO
    txidsProcessados.add(tx.txid);
    salvarCache();
  }));
}

// 🔥 LOOP
async function loop() {
  ultimoLoop = Date.now();

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
      if (!tx.txid) continue;
      if (txidsProcessados.has(tx.txid)) continue;

      if (!fila.find(t => t.txid === tx.txid)) {
        fila.push(tx);
      }
    }

    console.log("📦 Fila:", fila.length);

    await processarFila();

  } catch (e) {
    console.log("❌ Loop erro:", e.message);
    console.log("🔄 Reiniciando...");
    process.exit(1);
  } finally {
    executando = false;
  }
}

// 🔥 WATCHDOG
setInterval(() => {
  const tempoParado = Date.now() - ultimoLoop;

  if (tempoParado > 60000) {
    console.log("💥 Travou, reiniciando...");
    process.exit(1);
  }
}, 30000);

// 🚀 START
async function start() {
  console.log("🚀 BOT INICIADO");
  console.log("🔗 Backend:", process.env.BACKEND_URL);

  await loop();
  setInterval(loop, LOOP_INTERVAL);
}

start();