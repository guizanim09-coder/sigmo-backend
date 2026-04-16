require("dotenv").config();
const fetch = require("node-fetch");
const fs = require("fs");
const Tesseract = require("tesseract.js");

const {
  setupLogin,
  capturarTransacoes
} = require("./dentpeg-bot");

const LOOP_INTERVAL = 4000;
const CACHE_FILE = "./txids.json";

let txidsProcessados = new Set();
let ultimoLoop = Date.now();

// 🔄 carregar cache
if (fs.existsSync(CACHE_FILE)) {
  try {
    txidsProcessados = new Set(JSON.parse(fs.readFileSync(CACHE_FILE)));
  } catch {
    txidsProcessados = new Set();
  }
}

function salvarCache() {
  fs.writeFileSync(CACHE_FILE, JSON.stringify([...txidsProcessados]));
}

// 🔥 OCR
async function lerTextoImagem(url) {
  try {
    const response = await fetch(url);
    const buffer = await response.buffer();

    const path = "./tmp.png";
    fs.writeFileSync(path, buffer);

    const { data: { text } } = await Tesseract.recognize(path, "por");

    return text.toUpperCase();
  } catch (e) {
    console.log("❌ OCR erro:", e.message);
    return "";
  }
}

// 🔥 NORMALIZA TEXTO
function normalizar(txt) {
  return (txt || "")
    .toUpperCase()
    .replace(/[^A-Z0-9 ]/g, "")
    .trim();
}

// 🔥 COMPARAÇÃO COM TAXA REAL
function bateValorComTaxa(valorExtrato, valorPedido) {
  if (!valorExtrato || !valorPedido) return false;

  const taxaMin = (valorPedido * 0.0079) + 0.99;
  const taxaMax = (valorPedido * 0.019) + 0.99;

  const valorMin = valorPedido - taxaMax;
  const valorMax = valorPedido - taxaMin;

  return valorExtrato >= valorMin && valorExtrato <= valorMax;
}

// 🔥 BUSCAR PEDIDOS
async function buscarPendentes() {
  const res = await fetch(process.env.BACKEND_URL + "/deposito/pendentes");
  return res.json();
}

// 🔥 APROVAR
async function aprovar(pedidoId, tx) {
  await fetch(process.env.BACKEND_URL + "/deposito/confirmar-bot", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      pedidoId,
      txid: tx.txid,
      valor: tx.valorLiquido
    })
  });

  console.log("✅ APROVADO:", pedidoId);
}

// 🔥 LOOP PRINCIPAL
async function loop() {
  ultimoLoop = Date.now();

  try {
    const transacoes = await capturarTransacoes();
    const pedidos = await buscarPendentes();

    if (!transacoes.length || !pedidos.length) {
      console.log("🔍 Nada para processar");
      return;
    }

    console.log("📊 Transações:", transacoes.length);
    console.log("📦 Pendentes:", pedidos.length);

    for (const tx of transacoes) {
      if (!tx.txid || txidsProcessados.has(tx.txid)) continue;

      for (const pedido of pedidos) {
        try {
          const url = process.env.BACKEND_URL + "/" + pedido.comprovante;

          const textoOCR = await lerTextoImagem(url);
          const nomeOCR = textoOCR.split("\n")[0];

          const nomeExtrato = tx.nomePagador;

          if (
            bateValorComTaxa(tx.valorLiquido, pedido.valor) &&
            normalizar(nomeOCR).includes(normalizar(nomeExtrato))
          ) {
            await aprovar(pedido.id, tx);

            txidsProcessados.add(tx.txid);
            salvarCache();
            break;
          }

        } catch (e) {
          console.log("Erro match:", e.message);
        }
      }
    }

  } catch (e) {
    console.log("❌ Erro loop:", e.message);
  }
}

// 🔁 WATCHDOG
setInterval(() => {
  if (Date.now() - ultimoLoop > 60000) {
    console.log("💥 Travou, reiniciando...");
    process.exit(1);
  }
}, 30000);

// 🚀 START
(async () => {
  console.log("🚀 BOT FINAL INICIADO");
  console.log("🔗 Backend:", process.env.BACKEND_URL);

  await loop();
  setInterval(loop, LOOP_INTERVAL);
})();