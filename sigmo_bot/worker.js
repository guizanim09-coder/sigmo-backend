// 🔥 SETUP LOGIN
if (process.argv.includes("setup")) {
  const { setupLogin } = require("./dentpeg-bot");
  setupLogin();
  return;
}

require("dotenv").config();
const fetch = require("node-fetch");
const fs = require("fs");
const Tesseract = require("tesseract.js");

const { capturarTransacoes } = require("./dentpeg-bot");

const LOOP_INTERVAL = 4000;
const CACHE_FILE = "./txids.json";

let txidsProcessados = new Set();
let ultimoLoop = Date.now();

// 🔄 carregar cache
if (fs.existsSync(CACHE_FILE)) {
  try {
    txidsProcessados = new Set(JSON.parse(fs.readFileSync(CACHE_FILE, "utf8")));
  } catch {
    txidsProcessados = new Set();
  }
}

function salvarCache() {
  try {
    fs.writeFileSync(CACHE_FILE, JSON.stringify([...txidsProcessados], null, 2), "utf8");
  } catch (e) {
    console.log("❌ Erro cache:", e.message);
  }
}

// 🔥 OCR
async function lerTextoImagem(url) {
  try {
    const response = await fetch(url);

    if (!response.ok) {
      throw new Error(`Falha ao baixar comprovante: ${response.status}`);
    }

    const buffer = await response.buffer();
    const path = `./tmp_${Date.now()}.png`;

    fs.writeFileSync(path, buffer);

    const { data: { text } } = await Tesseract.recognize(path, "por");

    fs.unlink(path, () => {});

    return String(text || "").toUpperCase();
  } catch (e) {
    console.log("❌ OCR erro:", e.message);
    return "";
  }
}

// 🔥 NORMALIZA
function normalizar(txt) {
  return String(txt || "")
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .toUpperCase()
    .replace(/[^A-Z0-9 ]/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

// 🔥 COMPARAÇÃO DE VALOR (DENTPEG)
function bateValorComTaxa(valorExtrato, valorPedido) {
  const extrato = Number(valorExtrato);
  const pedido = Number(valorPedido);

  if (!Number.isFinite(extrato) || extrato <= 0) return false;
  if (!Number.isFinite(pedido) || pedido <= 0) return false;

  const taxaMin = (pedido * 0.0079) + 0.99;
  const taxaMax = (pedido * 0.019) + 0.99;

  const valorMin = Number((pedido - taxaMax).toFixed(2));
  const valorMax = Number((pedido - taxaMin).toFixed(2));

  return extrato >= valorMin && extrato <= valorMax;
}

// 🔥 COMPARAÇÃO DE NOME
function bateNome(nomeOCR, nomeExtrato) {
  const ocr = normalizar(nomeOCR);
  const extrato = normalizar(nomeExtrato);

  if (!ocr || !extrato) return false;

  return ocr.includes(extrato) || extrato.includes(ocr);
}

// 🔥 BUSCAR PENDENTES
async function buscarPendentes() {
  try {
    const res = await fetch(process.env.BACKEND_URL + "/deposito/pendentes");

    if (!res.ok) {
      throw new Error(`Backend respondeu ${res.status}`);
    }

    const data = await res.json();

    if (!Array.isArray(data)) {
      throw new Error("Resposta inválida de /deposito/pendentes");
    }

    return data;
  } catch (e) {
    console.log("❌ Erro pendentes:", e.message);
    return [];
  }
}

// 🔥 APROVAR
async function aprovar(pedidoId, tx) {
  try {
    const res = await fetch(process.env.BACKEND_URL + "/deposito/confirmar-bot", {
      method: "POST",
      headers: {
        "Content-Type": "application/json"
      },
      body: JSON.stringify({
        txid: tx.txid,
        valorLiquido: tx.valorLiquido
      })
    });

    const json = await res.json().catch(() => null);

    if (!res.ok) {
      throw new Error(json?.error || `Backend respondeu ${res.status}`);
    }

    console.log("✅ APROVADO:", pedidoId, "| TXID:", tx.txid);
    return true;
  } catch (e) {
    console.log("❌ Erro aprovar:", e.message);
    return false;
  }
}

// 🔥 LOOP
async function loop() {
  ultimoLoop = Date.now();

  try {
    const transacoes = await capturarTransacoes();
    const pedidos = await buscarPendentes();

    console.log("📊 Transações:", transacoes.length);
    console.log("📦 Pendentes:", pedidos.length);

    if (!transacoes.length || !pedidos.length) {
      console.log("🔍 Nada para processar");
      return;
    }

    for (const tx of transacoes) {
      console.log("TX:", tx);

      if (!tx?.txid) continue;
      if (txidsProcessados.has(tx.txid)) continue;

      let aprovado = false;

      for (const pedido of pedidos) {
        try {
          if (!pedido?.comprovante) continue;

          const comprovante = String(pedido.comprovante).replace(/^\/+/, "");
          const url = process.env.BACKEND_URL.replace(/\/+$/, "") + "/" + comprovante;

          const textoOCR = await lerTextoImagem(url);
          const nomeOCR = String(textoOCR || "").split("\n")[0] || "";
          const nomeExtrato = tx.nomePagador || "";

          const matchValor = bateValorComTaxa(tx.valorLiquido, pedido.valor);
          const matchNome = bateNome(nomeOCR, nomeExtrato);

          console.log("🔎 MATCH:", {
            pedidoId: pedido.id,
            valorPedido: pedido.valor,
            valorExtrato: tx.valorLiquido,
            nomeOCR,
            nomeExtrato,
            matchValor,
            matchNome
          });

          if (matchValor && matchNome) {
            const ok = await aprovar(pedido.id, tx);

            if (ok) {
              txidsProcessados.add(tx.txid);
              salvarCache();
              aprovado = true;
              break;
            }
          }
        } catch (e) {
          console.log("❌ Erro match:", e.message);
        }
      }

      if (!aprovado) {
        console.log("⏭️ Nenhum pedido compatível para TXID:", tx.txid);
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