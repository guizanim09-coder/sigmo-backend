const { chromium } = require("playwright");
const fs = require("fs");

const STORAGE_PATH = "./storage.json";
const APP_URL = "https://app.dentpeg.com/";
const STATEMENT_URL = "https://app.dentpeg.com/app/statement";

let browserRef = null;
let contextRef = null;
let pageRef = null;
let booting = null;

// 🔥 INICIAR BROWSER
async function iniciarBrowser() {
  if (browserRef && contextRef && pageRef) {
    return { browser: browserRef, context: contextRef, page: pageRef };
  }

  if (booting) return booting;

  booting = (async () => {
    const browser = await chromium.launch({
      headless: true,
      args: [
        "--disable-dev-shm-usage",
        "--no-sandbox"
      ]
    });

    const contextOptions = {
      permissions: ["clipboard-read", "clipboard-write"],
      viewport: { width: 1366, height: 900 }
    };

    if (fs.existsSync(STORAGE_PATH)) {
      contextOptions.storageState = STORAGE_PATH;
    }

    const context = await browser.newContext(contextOptions);
    const page = await context.newPage();

    page.setDefaultTimeout(5000);

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

// 🔥 SALVAR SESSÃO
async function salvarSessao(context) {
  await context.storageState({ path: STORAGE_PATH });
}

// 🔥 NORMALIZAR VALOR
function normalizarValorBR(valorTexto) {
  if (!valorTexto) return null;

  const limpo = String(valorTexto)
    .replace(/[^\d,.-]/g, "")
    .replace(/\./g, "")
    .replace(",", ".");

  const numero = Number(limpo);
  return Number.isFinite(numero) ? numero : null;
}

// 🔥 EXTRAIR TEXTO
function extrairTextoSeguro(texto, regex) {
  const match = String(texto || "").match(regex);
  return match ? match[1].trim() : null;
}

// 🔥 NOME MAIS INTELIGENTE
function extrairNomePagador(texto) {
  const linhas = String(texto || "")
    .split("\n")
    .map(l => l.trim())
    .filter(Boolean);

  for (const linha of linhas) {
    const upper = linha.toUpperCase();

    if (
      linha.length > 5 &&
      !upper.includes("PIX") &&
      !upper.includes("CONFIRMADO") &&
      !upper.includes("BRUTO") &&
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

// 🔥 LOGIN CHECK
async function estaNaAreaLogada(page) {
  const texto = await page.locator("body").innerText().catch(() => "");
  const lower = String(texto || "").toLowerCase();

  return !(lower.includes("sign in") || lower.includes("get started"));
}

// 🔥 FECHAR POPUPS
async function fecharPopups(page) {
  try {
    const botoes = [
      'button:has-text("Depois")',
      'button:has-text("Later")',
      'button:has-text("Close")',
      'button[aria-label="Close"]'
    ];

    for (const sel of botoes) {
      const el = page.locator(sel).first();
      if (await el.count()) {
        await el.click({ force: true }).catch(() => {});
      }
    }
  } catch {}
}

// 🔥 ABRIR EXTRATO
async function abrirExtratoRapido(page) {
  await page.goto(STATEMENT_URL, { waitUntil: "domcontentloaded" }).catch(() => {});
  await page.waitForTimeout(500);

  const logado = await estaNaAreaLogada(page);
  if (!logado) {
    throw new Error("Sessão inválida");
  }
}

// 🔥 LOGIN MANUAL
async function setupLogin() {
  await resetBrowser();
  const { browser, context, page } = await iniciarBrowser();

  await page.goto(APP_URL);

  console.log("👉 Faça login e vá até o extrato");
  console.log("👉 Pressione ENTER aqui");

  await new Promise(resolve => {
    process.stdin.resume();
    process.stdin.once("data", resolve);
  });

  await salvarSessao(context);
  await browser.close();

  console.log("✅ Sessão salva");
}

// 🔥 CAPTURA TXID
async function capturarTxidDoCard(page, card, textoCard) {
  let txid = null;

  try {
    const botao = card.locator('text=TXID').first();

    if (await botao.count()) {
      await botao.click({ force: true }).catch(() => {});
      await page.waitForTimeout(200);

      txid = await page.evaluate(async () => {
        try {
          return await navigator.clipboard.readText();
        } catch {
          return "";
        }
      });
    }
  } catch {}

  if (!txid) {
    const match = String(textoCard).match(/([a-f0-9]{20,})/i);
    txid = match ? match[1] : null;
  }

  return txid;
}

// 🔥 CAPTURA PRINCIPAL (AGORA PREPARADA PRA OCR)
async function capturarTransacoes() {
  try {
    const { page } = await iniciarBrowser();

    await abrirExtratoRapido(page);

    const cards = page.locator("div").filter({
      has: page.getByText("Entrada")
    });

    const total = Math.min(await cards.count(), 20);
    const transacoes = [];

    for (let i = 0; i < total; i++) {
      try {
        const card = cards.nth(i);
        const texto = await card.innerText();

        if (!texto.includes("CONFIRMADO")) continue;

        const valorTexto = extrairTextoSeguro(texto, /([\d.,]+)/);
        const valor = normalizarValorBR(valorTexto);

        const nomePagador = extrairNomePagador(texto);
        const txid = await capturarTxidDoCard(page, card, texto);

        // 🔥 PRINT DO CARD (PARA OCR FUTURO)
        const imagemPath = `./tmp_${Date.now()}_${i}.png`;
        await card.screenshot({ path: imagemPath }).catch(() => {});

        transacoes.push({
          valorLiquido: valor,
          nomePagador,
          txid,
          imagemComprovante: imagemPath,
          raw: texto
        });

      } catch (e) {
        console.log("Erro card:", e.message);
      }
    }

    return transacoes;

  } catch (e) {
    await resetBrowser();
    return [];
  }
}

module.exports = {
  setupLogin,
  capturarTransacoes,
  resetBrowser
};