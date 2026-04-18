require("dotenv").config();

const { chromium } = require("playwright");
const fs = require("fs");
const path = require("path");

const STORAGE_PATH = path.join(__dirname, "storage.json");
const APP_URL = "https://app.dentpeg.com/";
const STATEMENT_URL = "https://app.dentpeg.com/app/statement";

const DENTPEG_EMAIL = String(process.env.DENTPEG_EMAIL || "").trim();
const DENTPEG_PASSWORD = String(
  process.env.DENTPEG_SENHA || process.env.DENTPEG_PASSWORD || ""
).trim();

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
      args: ["--no-sandbox", "--disable-setuid-sandbox"]
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

    page.setDefaultTimeout(10000);
    page.setDefaultNavigationTimeout(20000);

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

function normalizarEspacos(valor) {
  return String(valor || "")
    .replace(/\u00a0/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function normalizarValorBR(valorTexto) {
  if (!valorTexto) return null;

  const limpo = String(valorTexto)
    .replace(/[^\d,.-]/g, "")
    .replace(/\./g, "")
    .replace(",", ".");

  const numero = Number(limpo);
  return Number.isFinite(numero) ? Number(numero.toFixed(2)) : null;
}

function extrairTextoSeguro(texto, regex) {
  const textoNormalizado = normalizarEspacos(texto);
  const match = textoNormalizado.match(regex);
  return match ? normalizarEspacos(match[1]) : null;
}

function normalizarDataHoraBR(texto) {
  const match = normalizarEspacos(texto).match(
    /(\d{2}\/\d{2}\/\d{4})[^\d]*(\d{2}:\d{2})(?::(\d{2}))?/i
  );

  if (!match) return null;

  const segundos = match[3] || "00";
  return `${match[1]} ${match[2]}:${segundos}`;
}

function normalizarNomePagador(nome) {
  return normalizarEspacos(nome)
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[^a-z\s]/g, "")
    .replace(/\s+/g, " ")
    .trim();
}

async function obterSnapshotPagina(page) {
  const bodyText = await page.locator("body").innerText().catch(() => "");
  return {
    url: String(page.url() || ""),
    bodyText,
    bodyLower: String(bodyText || "").toLowerCase()
  };
}

async function estaNaAreaLogada(page) {
  const snapshot = await obterSnapshotPagina(page);

  if (snapshot.url.includes("/app/")) {
    return true;
  }

  const marcadoresLogado = [
    "extrato",
    "saldo",
    "dashboard",
    "transferir",
    "carteira"
  ];

  if (marcadoresLogado.some((item) => snapshot.bodyLower.includes(item))) {
    return true;
  }

  const marcadoresDeslogado = [
    "sign in",
    "login",
    "entrar",
    "get started",
    "password",
    "senha"
  ];

  if (marcadoresDeslogado.some((item) => snapshot.bodyLower.includes(item))) {
    return false;
  }

  return false;
}

async function estaNoExtrato(page) {
  const snapshot = await obterSnapshotPagina(page);

  return (
    snapshot.url.includes("/statement") ||
    snapshot.bodyLower.includes("entrada") ||
    snapshot.bodyLower.includes("saida") ||
    snapshot.bodyLower.includes("extrato")
  );
}

async function fecharPopups(page) {
  try {
    for (let i = 0; i < 5; i += 1) {
      const checkbox = page.locator('input[type="checkbox"]').first();
      if (await checkbox.count()) {
        await checkbox.check().catch(() => {});
      }

      const botoes = [
        page.locator('button:has-text("Depois")').first(),
        page.locator('button:has-text("Later")').first(),
        page.locator('button:has-text("Close")').first(),
        page.locator('button[aria-label="Close"]').first()
      ];

      let clicou = false;

      for (const botao of botoes) {
        const existe = await botao.count().catch(() => 0);
        if (!existe) continue;

        const visivel = await botao.isVisible().catch(() => false);
        if (!visivel) continue;

        await botao.click({ force: true }).catch(() => {});
        clicou = true;
        break;
      }

      if (!clicou) {
        break;
      }

      await page.waitForTimeout(250);
    }
  } catch (_) {}
}

async function preencherPrimeiroDisponivel(page, seletores, valor) {
  for (const seletor of seletores) {
    const campo = page.locator(seletor).first();
    const existe = await campo.count().catch(() => 0);

    if (!existe) continue;

    const visivel = await campo.isVisible().catch(() => false);
    if (!visivel) continue;

    await campo.fill(valor);
    return true;
  }

  return false;
}

async function clicarPrimeiroDisponivel(page, seletores) {
  for (const seletor of seletores) {
    const elemento = page.locator(seletor).first();
    const existe = await elemento.count().catch(() => 0);

    if (!existe) continue;

    const visivel = await elemento.isVisible().catch(() => false);
    if (!visivel) continue;

    await elemento.click({ force: true });
    return true;
  }

  return false;
}

async function navegarParaExtratoPeloMenu(page) {
  const tentativas = [
    async () =>
      clicarPrimeiroDisponivel(page, [
        'a[href*="/statement"]',
        'button:has-text("Extrato")',
        'a:has-text("Extrato")',
        'button:has-text("Statement")',
        'a:has-text("Statement")'
      ]),
    async () => page.goto(STATEMENT_URL, { waitUntil: "domcontentloaded" })
  ];

  for (const tentativa of tentativas) {
    try {
      await tentativa();
      await page.waitForTimeout(1000);
      await fecharPopups(page);

      if (await estaNoExtrato(page)) {
        return true;
      }
    } catch (_) {}
  }

  return false;
}

async function fazerLogin(page) {
  if (!DENTPEG_EMAIL || !DENTPEG_PASSWORD) {
    throw new Error("Credenciais DentPeg ausentes (DENTPEG_EMAIL/DENTPEG_SENHA)");
  }

  console.log("[dentpeg] iniciando login automatico...");
  console.log("[dentpeg] email:", DENTPEG_EMAIL);

  await page.goto(APP_URL, { waitUntil: "domcontentloaded" });
  await page.waitForTimeout(2000);
  await fecharPopups(page);

  if (await estaNaAreaLogada(page)) {
    await salvarSessao(page.context()).catch(() => {});
    return;
  }

  await clicarPrimeiroDisponivel(page, [
    'button:has-text("Entrar")',
    'a:has-text("Entrar")',
    'button:has-text("Login")',
    'a:has-text("Login")',
    'button:has-text("Sign in")',
    'a:has-text("Sign in")'
  ]).catch(() => {});

  await page.waitForTimeout(1000);

  const emailOk = await preencherPrimeiroDisponivel(
    page,
    [
      'input[type="email"]',
      'input[name="email"]',
      'input[autocomplete="username"]'
    ],
    DENTPEG_EMAIL
  );

  const senhaOk = await preencherPrimeiroDisponivel(
    page,
    [
      'input[type="password"]',
      'input[name="password"]',
      'input[autocomplete="current-password"]'
    ],
    DENTPEG_PASSWORD
  );

  if (!emailOk || !senhaOk) {
    throw new Error("Nao foi possivel localizar os campos de login da DentPeg");
  }

  const clicouSubmit = await clicarPrimeiroDisponivel(page, [
    'button[type="submit"]',
    'button:has-text("Entrar")',
    'button:has-text("Login")',
    'button:has-text("Sign in")',
    'input[type="submit"]'
  ]);

  if (!clicouSubmit) {
    throw new Error("Nao foi possivel localizar o botao de login da DentPeg");
  }

  await page.waitForTimeout(5000);
  await fecharPopups(page);

  if (!(await estaNaAreaLogada(page))) {
    throw new Error("Falha no login DentPeg");
  }

  await salvarSessao(page.context()).catch(() => {});
  console.log("[dentpeg] login realizado com sucesso");
}

async function abrirExtratoRapido(page) {
  for (let tentativa = 1; tentativa <= 2; tentativa += 1) {
    await page.goto(STATEMENT_URL, { waitUntil: "domcontentloaded" }).catch(() => {});
    await page.waitForTimeout(1000);
    await fecharPopups(page);

    if (await estaNoExtrato(page)) {
      return;
    }

    if (!(await estaNaAreaLogada(page))) {
      await fazerLogin(page);
      continue;
    }

    const abriu = await navegarParaExtratoPeloMenu(page);
    if (abriu) {
      return;
    }
  }

  throw new Error("Nao foi possivel abrir o extrato da DentPeg");
}

function extrairNomePagador(texto) {
  const linhas = String(texto || "")
    .split("\n")
    .map((linha) => linha.trim())
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

    if (ignorar.some((item) => lower.includes(item))) continue;
    if (/\d{2}\/\d{2}\/\d{4}/.test(lower)) continue;
    if (/^[\d\s.,\-#]+$/.test(lower)) continue;

    const partes = linha.split(" ").filter((parte) => parte.length > 2);

    if (partes.length >= 2 && linha.length > 6 && linha.length < 100) {
      return linha;
    }
  }

  return null;
}

async function capturarTxidDoCard(page, card, textoCard) {
  let txid = null;

  try {
    const botoesTxid = [
      card.locator('button:has-text("TXID")').first(),
      card.locator('text=TXID').first()
    ];

    for (const botao of botoesTxid) {
      const existe = await botao.count().catch(() => 0);
      if (!existe) continue;

      await botao.click({ force: true }).catch(() => {});
      await page.waitForTimeout(250);

      txid = await page.evaluate(async () => {
        try {
          return await navigator.clipboard.readText();
        } catch {
          return "";
        }
      });

      txid = normalizarEspacos(txid).replace(/\s/g, "");
      if (txid) break;
    }
  } catch (_) {}

  if (!txid) {
    const match = String(textoCard || "").match(/TXID[^A-Za-z0-9]*([A-Za-z0-9._:-]{10,})/i);
    txid = match ? match[1] : null;
  }

  return txid || null;
}

function extrairIdTransacao(texto) {
  const textoNormalizado = normalizarEspacos(texto);
  const match =
    textoNormalizado.match(/#\s*([A-Za-z0-9-]{6,})/i) ||
    textoNormalizado.match(/id(?:\s+da\s+transacao)?\s*:?\s*([A-Za-z0-9-]{6,})/i);

  return match ? match[1] : null;
}

async function capturarTransacoes() {
  let tentativa = 0;

  while (tentativa < 2) {
    tentativa += 1;

    try {
      const { page } = await iniciarBrowser();

      await abrirExtratoRapido(page);

      if (tentativa === 1) {
        await page.reload({ waitUntil: "domcontentloaded" }).catch(() => {});
        await page.waitForTimeout(750);
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
          const textoNormalizado = normalizarEspacos(texto);

          if (!textoNormalizado) continue;
          if (!/confirmado/i.test(textoNormalizado)) continue;
          if (!/depix/i.test(textoNormalizado)) continue;

          const valorLiquidoTexto = extrairTextoSeguro(textoNormalizado, /DePix\s*R?\$?\s*([\d.,]+)/i);
          const valorBrutoTexto = extrairTextoSeguro(textoNormalizado, /BRUTO\s*R?\$?\s*([\d.,]+)/i);
          const dataHora = normalizarDataHoraBR(textoNormalizado);
          const idTransacao = extrairIdTransacao(textoNormalizado);
          const nomePagadorRaw = extrairNomePagador(texto);
          const nomePagador = nomePagadorRaw ? normalizarNomePagador(nomePagadorRaw) : null;

          if (!nomePagador) continue;

          const txid = await capturarTxidDoCard(page, card, textoNormalizado);

          transacoes.push({
            valorLiquido: normalizarValorBR(valorLiquidoTexto),
            valorBruto: normalizarValorBR(valorBrutoTexto),
            nomePagador,
            txid,
            idTransacao,
            dataHora,
            raw: texto
          });
        } catch (error) {
          console.log("[dentpeg] erro ao ler card:", error.message);
        }
      }

      return transacoes;
    } catch (error) {
      if (tentativa >= 2) {
        throw error;
      }

      console.log("[dentpeg] falha ao capturar, reiniciando browser:", error.message);
      await resetBrowser();
      await new Promise((resolve) => setTimeout(resolve, 750));
    }
  }

  return [];
}

module.exports = {
  capturarTransacoes,
  resetBrowser
};
