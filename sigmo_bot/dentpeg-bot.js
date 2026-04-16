const { chromium } = require("playwright");
const fs = require("fs");

const STORAGE_PATH = "./storage.json";
const APP_URL = "https://app.dentpeg.com/";
const STATEMENT_URL = "https://app.dentpeg.com/app/statement";

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

// 🔥 NORMALIZAR VALOR
function normalizarValorBR(valorTexto) {
  if (!valorTexto) return null;

  const limpo = String(valorTexto)
    .replace(/[^\d,]/g, "")
    .replace(",", ".");

  const numero = Number(limpo);
  return Number.isFinite(numero) ? numero : null;
}

// 🔥 EXTRAIR TEXTO
function extrairTexto(texto, regex) {
  const match = String(texto || "").match(regex);
  return match ? match[1].trim() : null;
}

// 🔥 NOME MAIS INTELIGENTE
function extrairNome(texto) {
  const linhas = texto.split("\n").map(l => l.trim()).filter(Boolean);

  for (const linha of linhas) {
    const upper = linha.toUpperCase();

    if (
      linha.length > 5 &&
      !upper.includes("PIX") &&
      !upper.includes("CONFIRMADO") &&
      !upper.includes("TXID") &&
      !upper.includes("ENTRADA") &&
      !upper.includes("SAÍDA") &&
      !linha.match(/\d{2}\/\d{2}\/\d{4}/) &&
      !linha.match(/^[\d\s.,\-R$#]+$/)
    ) {
      return linha;
    }
  }

  return null;
}

// 🔥 TXID
async function capturarTxid(page, card, texto) {
  try {
    const botao = card.locator('text=TXID').first();

    if (await botao.count()) {
      await botao.click().catch(() => {});
      await page.waitForTimeout(200);

      const txid = await page.evaluate(async () => {
        try {
          return await navigator.clipboard.readText();
        } catch {
          return "";
        }
      });

      if (txid && txid.length > 10) return txid.trim();
    }
  } catch {}

  const match = texto.match(/([a-f0-9]{20,})/i);
  return match ? match[1] : null;
}

// 🔥 ABRIR EXTRATO
async function abrirExtrato(page) {
  await page.goto(STATEMENT_URL, { waitUntil: "domcontentloaded" });
  await page.waitForTimeout(800);

  const body = await page.locator("body").innerText().catch(() => "");

  if (!body.includes("Entrada")) {
    throw new Error("Extrato não carregou");
  }
}

// 🔥 CAPTURA PRINCIPAL
async function capturarTransacoes() {
  try {
    const { page } = await iniciarBrowser();

    await abrirExtrato(page);

    const cards = page.locator("div").filter({
      has: page.getByText("Entrada")
    });

    const total = Math.min(await cards.count(), 15);
    const transacoes = [];

    for (let i = 0; i < total; i++) {
      try {
        const card = cards.nth(i);
        const texto = await card.innerText();

        if (!texto || !texto.includes("CONFIRMADO")) continue;

        const valorTexto = extrairTexto(texto, /([\d.,]+)/);
        const valor = normalizarValorBR(valorTexto);

        if (!valor) continue;

        const nomePagador = extrairNome(texto);
        const txid = await capturarTxid(page, card, texto);

        // 🔥 screenshot para OCR
        const path = `./tmp_${Date.now()}_${i}.png`;
        await card.screenshot({ path }).catch(() => {});

        transacoes.push({
          valorLiquido: valor,
          nomePagador,
          txid,
          imagemComprovante: path,
          raw: texto
        });

      } catch (e) {
        console.log("Erro card:", e.message);
      }
    }

    return transacoes;

  } catch (e) {
    console.log("Erro captura:", e.message);
    await resetBrowser();
    return [];
  }
}

// 🔥 LOGIN
async function setupLogin() {
  await resetBrowser();
  const { browser, context, page } = await iniciarBrowser();

  await page.goto(APP_URL);

  console.log("👉 Faça login manualmente");
  console.log("👉 Vá até o extrato");
  console.log("👉 Pressione ENTER");

  await new Promise(r => {
    process.stdin.resume();
    process.stdin.once("data", r);
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