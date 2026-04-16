const { chromium } = require("playwright");
const fs = require("fs");

const STORAGE_PATH = "./storage.json";
const APP_URL = "https://app.dentpeg.com/";

let browserRef = null;
let contextRef = null;
let pageRef = null;
let booting = null;

// 🔥 INIT
async function iniciarBrowser() {
  if (browserRef && contextRef && pageRef) {
    return { browser: browserRef, context: contextRef, page: pageRef };
  }

  if (booting) return booting;

  booting = (async () => {
    const browser = await chromium.launch({
      headless: true,
      args: ["--no-sandbox"]
    });

    const contextOptions = {
      viewport: { width: 1366, height: 900 }
    };

    if (fs.existsSync(STORAGE_PATH)) {
      contextOptions.storageState = STORAGE_PATH;
    }

    const context = await browser.newContext(contextOptions);
    const page = await context.newPage();

    browserRef = browser;
    contextRef = context;
    pageRef = page;

    return { browser, context, page };
  })();

  try {
    return await booting;
  } finally {
    booting = null;
  }
}

// 🔥 RESET
async function resetBrowser() {
  try {
    if (pageRef) await pageRef.close().catch(() => {});
    if (contextRef) await contextRef.close().catch(() => {});
    if (browserRef) await browserRef.close().catch(() => {});
  } catch {}

  browserRef = null;
  contextRef = null;
  pageRef = null;
}

// 🔥 VALOR
function normalizarValorBR(valorTexto) {
  if (!valorTexto) return null;

  const limpo = String(valorTexto)
    .replace(/[^\d,]/g, "")
    .replace(",", ".");

  const numero = Number(limpo);
  return Number.isFinite(numero) ? numero : null;
}

// 🔥 NOME
function extrairNome(texto) {
  const linhas = texto.split("\n").map(l => l.trim()).filter(Boolean);

  for (const linha of linhas) {
    const upper = linha.toUpperCase();

    if (
      linha.length > 5 &&
      !upper.includes("DEPIX") &&
      !upper.includes("ENTRADA") &&
      !upper.includes("EXPIRADO") &&
      !upper.includes("QR") &&
      !upper.includes("DINAMICO") &&
      !linha.match(/\d{2}\/\d{2}\/\d{4}/)
    ) {
      return linha;
    }
  }

  return null;
}

// 🔥 TXID
async function capturarTxid(page, card, texto) {
  try {
    const match = texto.match(/#\s*([a-f0-9]+)/i);
    if (match) return match[1];
  } catch {}

  return null;
}

// 🔥 ABRIR EXTRATO
async function abrirExtrato(page) {
  await page.goto(APP_URL, {
    waitUntil: "domcontentloaded"
  });

  await page.waitForTimeout(3000);

  // 🔥 menu superior correto
  const menu = page.locator("a:has-text('Extrato')");

  if (await menu.count()) {
    await menu.first().click();
    await page.waitForTimeout(4000);
  } else {
    throw new Error("Menu extrato não encontrado");
  }

  const body = await page.locator("body").innerText().catch(() => "");

  if (!body.includes("DePix")) {
    throw new Error("Extrato não carregou");
  }
}

// 🔥 CAPTURA REAL
async function capturarTransacoes() {
  try {
    const { page } = await iniciarBrowser();

    await abrirExtrato(page);

    // 🔥 pega cards reais
    const cards = page.locator("div").filter({
      hasText: "DePix"
    });

    const total = Math.min(await cards.count(), 20);
    const transacoes = [];

    for (let i = 0; i < total; i++) {
      try {
        const card = cards.nth(i);
        const texto = await card.innerText();

        if (!texto) continue;

        // 🔥 só entradas
        if (!texto.includes("Entrada")) continue;
if (texto.includes("EXPIRADO")) continue;

        const valorTexto = texto.match(/DePix\s?([\d.,]+)/)?.[1];
        const valor = normalizarValorBR(valorTexto);

        if (!valor || valor < 5) continue;

        const nomePagador = extrairNome(texto);
        const txid = await capturarTxid(page, card, texto);

        const path = null;

        transacoes.push({
  valorLiquido: valor,
  nomePagador,
  txid,
  imagemComprovante: null,
  raw: texto
});

      } catch (e) {
        console.log("Erro card:", e.message);
      }
    }

    return transacoes;

} catch (e) {
  console.log("❌ Erro captura:", e.message);
  console.log("⚠️ Falha leve, tentando novamente no próximo loop...");
  return [];
}
}

// 🔥 LOGIN
async function setupLogin() {
  await resetBrowser();

  const browser = await chromium.launch({
    headless: false,
    args: ["--no-sandbox"]
  });

  const context = await browser.newContext();
  const page = await context.newPage();

  await page.goto(APP_URL);

  console.log("👉 Faça login manualmente");
  console.log("👉 Vá até o EXTRATO");
  console.log("👉 Pressione ENTER");

  await new Promise(resolve => {
    process.stdin.once("data", resolve);
  });

  await context.storageState({ path: STORAGE_PATH });
  await browser.close();

  console.log("✅ Sessão salva");
}

module.exports = {
  setupLogin,
  capturarTransacoes,
  resetBrowser
};