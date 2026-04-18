const { chromium } = require("playwright");
const fs = require("fs");

const STORAGE_PATH = "./storage.json";
const APP_URL = "https://app.dentpeg.com/";
const STATEMENT_URL = "https://app.dentpeg.com/app/statement";

let browserRef = null;
let contextRef = null;
let pageRef = null;
let booting = null;

async function iniciarBrowser() {
  if (browserRef && contextRef && pageRef) {
    return { browser: browserRef, context: contextRef, page: pageRef };
  }

  if (booting) {
    return booting;
  }

  booting = (async () => {
    const browser = await chromium.launch({
      headless: true,
      args: [
        "--disable-dev-shm-usage",
        "--no-sandbox",
        "--disable-blink-features=AutomationControlled"
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
    page.setDefaultNavigationTimeout(10000);

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

async function resetBrowser() {
  try {
    if (pageRef) await pageRef.close().catch(() => {});
    if (contextRef) await contextRef.close().catch(() => {});
    if (browserRef) await browserRef.close().catch(() => {});
  } catch (_) {}

  browserRef = null;
  contextRef = null;
  pageRef = null;
  booting = null;
}

async function salvarSessao(context) {
  await context.storageState({ path: STORAGE_PATH });
}

function normalizarValorBR(valorTexto) {
  if (!valorTexto) return null;

  const limpo = String(valorTexto)
    .replace(/[^\d,.-]/g, "")
    .replace(/\./g, "")
    .replace(",", ".");

  const numero = Number(limpo);
  return Number.isFinite(numero) ? numero : null;
}

function extrairTextoSeguro(texto, regex) {
  const match = String(texto || "").match(regex);
  return match ? match[1].trim() : null;
}

async function estaNaAreaLogada(page) {
  const texto = await page.locator("body").innerText().catch(() => "");
  const lower = String(texto || "").toLowerCase();

  if (lower.includes("sign in") || lower.includes("get started")) {
    return false;
  }

  return true;
}

async function fecharPopups(page) {
  try {
    for (let i = 0; i < 4; i += 1) {
      const checkbox = page.locator('input[type="checkbox"]').first();
      if (await checkbox.count()) {
        await checkbox.check().catch(() => {});
      }

      const botoesDepois = [
        page.locator('button:has-text("Depois")').first(),
        page.locator('button:has-text("Later")').first(),
        page.locator('button:has-text("Close")').first()
      ];

      let clicou = false;

      for (const botao of botoesDepois) {
        if (await botao.count()) {
          await botao.click({ force: true }).catch(() => {});
          clicou = true;
          break;
        }
      }

      const fechar = page.locator('button[aria-label="Close"], button:has-text("×")').first();
      if (!clicou && await fechar.count()) {
        await fechar.click({ force: true }).catch(() => {});
        clicou = true;
      }

      if (!clicou) {
        break;
      }

      await page.waitForTimeout(200);
    }
  } catch (_) {}
}

async function abrirExtratoRapido(page) {
  await page.goto(STATEMENT_URL, { waitUntil: "domcontentloaded" }).catch(() => {});
  await page.waitForTimeout(500);
  await fecharPopups(page);

  const logado = await estaNaAreaLogada(page);
  if (!logado) {
    throw new Error("Sessão inválida. Rode novamente: node worker.js setup");
  }

  const bodyText = await page.locator("body").innerText().catch(() => "");
  const urlAtual = page.url();

  if (
    !urlAtual.includes("/statement") &&
    !bodyText.includes("Entrada") &&
    !bodyText.includes("Saída")
  ) {
    await page.goto(APP_URL, { waitUntil: "domcontentloaded" });
    await page.waitForTimeout(500);
    await fecharPopups(page);

    const tentativas = [
      async () => page.getByText("Extrato", { exact: true }).click(),
      async () => page.locator('a:has-text("Extrato")').first().click(),
      async () => page.locator('button:has-text("Extrato")').first().click(),
      async () => page.goto(STATEMENT_URL, { waitUntil: "domcontentloaded" })
    ];

    let abriu = false;

    for (const tentativa of tentativas) {
      try {
        await tentativa();
        await page.waitForTimeout(500);
        await fecharPopups(page);

        const body = await page.locator("body").innerText().catch(() => "");
        const url = page.url();

        if (
          url.includes("/statement") ||
          body.includes("Entrada") ||
          body.includes("Saída")
        ) {
          abriu = true;
          break;
        }
      } catch (_) {}
    }

    if (!abriu) {
      throw new Error("Não foi possível abrir o extrato");
    }
  }
}

// LOGIN
async function fazerLogin(page) {
  await page.goto("https://app.dentpeg.com/", { waitUntil: "domcontentloaded" });

  await page.waitForTimeout(2000);

  await page.fill('input[type="email"]', process.env.DENTPEG_EMAIL);
  await page.fill('input[type="password"]', process.env.DENTPEG_SENHA);

  await page.click('button[type="submit"]');

  await page.waitForTimeout(5000);

  console.log("✅ Login automático feito");
}

function extrairNomePagador(texto) {
  const linhas = String(texto || "")
    .split("\n")
    .map(l => l.trim())
    .filter(Boolean);

  const ignorar = [
    "depix",
    "confirmado",
    "bruto",
    "txid",
    "entrada",
    "saida",
    "pix",
    "r$",
    "banco",
    "ltda",
    "eireli",
    "solucoes",
    "tecnologia",
    "pagamento"
  ];

  for (const linha of linhas) {
    const lower = linha.toLowerCase();

    // ignora lixo
    if (ignorar.some(p => lower.includes(p))) continue;
    if (lower.match(/\d{2}\/\d{2}\/\d{4}/)) continue;
    if (lower.match(/^[\d\s.,\-#]+$/)) continue;

    // 🔒 precisa ter pelo menos 2 palavras (nome + sobrenome)
    const partes = linha.split(" ").filter(p => p.length > 2);

    if (partes.length >= 2 && linha.length > 6 && linha.length < 80) {
      return linha;
    }
  }

  return null;
}

async function capturarTxidDoCard(page, card, textoCard) {
  let txid = null;

  try {
    const botao = card.locator('text=TXID').first();

    if (await botao.count()) {
      await botao.click({ force: true }).catch(() => {});
      await page.waitForTimeout(250);

      txid = await page.evaluate(async () => {
        try {
          return await navigator.clipboard.readText();
        } catch {
          return "";
        }
      });

      txid = String(txid || "").trim().replace(/\s/g, "");
    }
  } catch (_) {}

  if (!txid) {
    const match = String(textoCard || "").match(/TXID.*?([a-f0-9]{20,})/i);
    txid = match ? match[1] : null;
  }

  return txid || null;
}

// CAPTURA OTIMIZADA
async function capturarTransacoes() {
  let tentativa = 0;

  while (tentativa < 2) {
    tentativa += 1;

    try {
      const { page } = await iniciarBrowser();

// 🔥 garante login antes de tudo
const logado = await estaNaAreaLogada(page);

if (!logado) {
  console.log("🔐 Fazendo login automático...");
  await fazerLogin(page);
}

// agora sim abre extrato
await abrirExtratoRapido(page);

      // recarrega leve para pegar últimos itens sem reconstruir tudo
      if (tentativa === 1) {
        await page.reload({ waitUntil: "domcontentloaded" }).catch(() => {});
        await page.waitForTimeout(400);
        await fecharPopups(page);
      }

      const cards = page.locator("div").filter({
        has: page.getByText("Entrada", { exact: true })
      });

      const total = Math.min(await cards.count(), 100);
      const transacoes = [];

      for (let i = 0; i < total; i += 1) {
        try {
          const card = cards.nth(i);
          const texto = await card.innerText().catch(() => "");

          if (!texto) continue;
          if (!texto.includes("CONFIRMADO")) continue;
          if (!texto.includes("DePix")) continue;

          const valorLiquidoTexto = extrairTextoSeguro(texto, /DePix\s*([\d.,]+)/i);
          const valorBrutoTexto = extrairTextoSeguro(texto, /BRUTO\s*R\$\s*([\d.,]+)/i);

          const dataHora = extrairTextoSeguro(
            texto,
            /(\d{2}\/\d{2}\/\d{4}.*?\d{2}:\d{2}:\d{2})/i
          );

          const idTransacao = extrairTextoSeguro(
            texto,
            /#\s*([a-f0-9-]{6,})/i
          );

          const nomePagadorRaw = extrairNomePagador(texto);

let nomePagador = null;

if (nomePagadorRaw) {
  nomePagador = nomePagadorRaw
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[^a-z\s]/g, "")
    .replace(/\s+/g, " ")
    .trim();
}

if (!nomePagador) continue;

const txid = await capturarTxidDoCard(page, card, texto);

transacoes.push({
  valorLiquido: normalizarValorBR(valorLiquidoTexto),
  valorBruto: normalizarValorBR(valorBrutoTexto),
  nomePagador,
  txid,
  idTransacao,
  dataHora,
  raw: texto
});
        } catch (e) {
          console.log("Erro card:", e.message);
        }
      }

      return transacoes;
    } catch (e) {
      if (tentativa >= 2) {
        throw e;
      }

      await resetBrowser();
      await new Promise((resolve) => setTimeout(resolve, 500));
    }
  }

  return [];
}

module.exports = {
  capturarTransacoes,
  resetBrowser
};