require("dotenv").config();

const { chromium } = require("playwright");
const crypto = require("crypto");
const fs = require("fs");
const path = require("path");

const STORAGE_PATH = path.join(__dirname, "storage.json");
const APP_URL = "https://app.dentpeg.com/";
const STATEMENT_URL = "https://app.dentpeg.com/app/statement";

const DENTPEG_EMAIL = String(process.env.DENTPEG_EMAIL || "").trim();
const DENTPEG_PASSWORD = String(
  process.env.DENTPEG_SENHA || process.env.DENTPEG_PASSWORD || ""
).trim();
const DENTPEG_DEBUG =
  String(process.env.DENTPEG_DEBUG || "false").trim().toLowerCase() === "true";
const DENTPEG_DEBUG_CARD_LIMIT = Math.max(
  1,
  Number(process.env.DENTPEG_DEBUG_CARD_LIMIT || 5)
);
const DENTPEG_MAX_CARDS_POR_VARREDURA = 100;
const DENTPEG_MAX_CLIQUES_VER_MAIS = 10;

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

function resumirTextoParaLog(valor, maxLen = 500) {
  const texto = String(valor || "")
    .replace(/\r/g, "")
    .replace(/\n+/g, " | ")
    .replace(/\s+/g, " ")
    .trim();

  if (texto.length <= maxLen) {
    return texto;
  }

  return `${texto.slice(0, maxLen)}...`;
}

function extrairDatasHorasVisiveis(texto) {
  const resultados = [];
  const regexes = [
    /(\d{2}\/\d{2}\/\d{4})[^\d]{0,20}(\d{2}:\d{2}(?::\d{2})?)/gi,
    /(\d{4}-\d{2}-\d{2})[^\d]{0,20}(\d{2}:\d{2}(?::\d{2})?)/gi
  ];

  for (const regex of regexes) {
    for (const match of String(texto || "").matchAll(regex)) {
      resultados.push(`${match[1]} ${match[2]}`);
    }
  }

  return [...new Set(resultados)];
}

function contarOcorrencias(texto, regex) {
  return (String(texto || "").match(regex) || []).length;
}

function debugDentpeg(evento, payload) {
  if (!DENTPEG_DEBUG) return;

  try {
    console.log(`[dentpeg][debug] ${evento}: ${JSON.stringify(payload)}`);
  } catch {
    console.log(`[dentpeg][debug] ${evento}:`, payload);
  }
}

function contarDetalhesNoTexto(texto) {
  return contarOcorrencias(texto, /\bdetalhes\b/gi);
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

function gerarHashCurto(valor) {
  return crypto.createHash("sha1").update(String(valor || "")).digest("hex");
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

async function logDiagnosticoExtrato(page, cards) {
  if (!DENTPEG_DEBUG) return;

  const snapshot = await obterSnapshotPagina(page);
  const titulo = await page.title().catch(() => "");
  const h1 = await page.locator("h1").first().innerText().catch(() => "");
  const quantCards = await cards.count().catch(() => 0);

  debugDentpeg("pagina_extrato", {
    url: snapshot.url,
    titulo: normalizarEspacos(titulo),
    h1: normalizarEspacos(h1),
    cardCount: quantCards,
    hasExtrato: snapshot.bodyLower.includes("extrato"),
    hasEntrada: snapshot.bodyLower.includes("entrada"),
    hasSaida: snapshot.bodyLower.includes("saida"),
    bodyPreview: resumirTextoParaLog(snapshot.bodyText, 350)
  });
}

async function marcarCardsExtrato(page) {
  return page.evaluate(() => {
    const normalizar = (valor) =>
      String(valor || "")
        .replace(/\u00a0/g, " ")
        .replace(/\s+/g, " ")
        .trim();

    const contar = (texto, regex) => (String(texto || "").match(regex) || []).length;
    const detalhesButtons = Array.from(document.querySelectorAll("button")).filter((button) =>
      /detalhes/i.test(normalizar(button.innerText))
    );

    document
      .querySelectorAll("[data-codex-card-root]")
      .forEach((el) => el.removeAttribute("data-codex-card-root"));

    const roots = [];
    const seen = new Set();

    for (const button of detalhesButtons) {
      let atual = button;
      let root = null;

      while (atual && atual !== document.body) {
        const texto = normalizar(atual.innerText);

        if (texto) {
          const detalhesCount = contar(texto, /\bdetalhes\b/gi);
          const entradaCount = contar(texto, /\bentrada\b/gi);
          const possuiMarcadores =
            /entrada/i.test(texto) &&
            /confirmado/i.test(texto) &&
            /depix/i.test(texto) &&
            /detalhes/i.test(texto) &&
            /txid/i.test(texto);

          if (possuiMarcadores && detalhesCount === 1 && entradaCount === 1) {
            root = atual;
            break;
          }
        }

        atual = atual.parentElement;
      }

      if (!root || seen.has(root)) continue;

      seen.add(root);
      roots.push(root);
    }

    roots.forEach((root, index) => {
      root.setAttribute("data-codex-card-root", String(index));
    });

    return roots.map((root, index) => {
      const texto = normalizar(root.innerText);
      return {
        index,
        entradaCount: contar(texto, /\bentrada\b/gi),
        detalhesCount: contar(texto, /\bdetalhes\b/gi),
        txidCount: contar(texto, /\btxid\b/gi),
        preview: texto.slice(0, 500)
      };
    });
  });
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

async function localizarPrimeiroDisponivel(page, seletores) {
  for (const seletor of seletores) {
    const elemento = page.locator(seletor).first();
    const existe = await elemento.count().catch(() => 0);

    if (!existe) continue;

    return elemento;
  }

  return null;
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

async function expandirExtratoAteLimite(page) {
  let cardsMarcados = await marcarCardsExtrato(page);
  let totalAtual = cardsMarcados.length;
  let cliquesEfetivos = 0;

  for (
    let tentativa = 0;
    tentativa < DENTPEG_MAX_CLIQUES_VER_MAIS && totalAtual < DENTPEG_MAX_CARDS_POR_VARREDURA;
    tentativa += 1
  ) {
    const botaoVerMais = await localizarPrimeiroDisponivel(page, [
      'button:has-text("Ver mais")',
      'a:has-text("Ver mais")',
      'button:has-text("Mostrar mais")',
      'a:has-text("Mostrar mais")',
      'button:has-text("Load more")',
      'a:has-text("Load more")'
    ]);

    if (!botaoVerMais) {
      break;
    }

    const visivel = await botaoVerMais.isVisible().catch(() => false);
    if (!visivel) {
      break;
    }

    await botaoVerMais.scrollIntoViewIfNeeded().catch(() => {});
    await botaoVerMais.click({ force: true }).catch(() => {});
    await page.waitForTimeout(1200);
    await fecharPopups(page);

    const cardsDepoisClique = await marcarCardsExtrato(page);
    const totalDepoisClique = cardsDepoisClique.length;

    if (totalDepoisClique <= totalAtual) {
      break;
    }

    cardsMarcados = cardsDepoisClique;
    totalAtual = totalDepoisClique;
    cliquesEfetivos += 1;
  }

  if (DENTPEG_DEBUG) {
    debugDentpeg("extrato_expandido", {
      totalCards: totalAtual,
      cliquesEfetivos,
      limiteCards: DENTPEG_MAX_CARDS_POR_VARREDURA,
      limiteCliques: DENTPEG_MAX_CLIQUES_VER_MAIS
    });
  }

  return {
    cardsMarcados,
    totalAtual,
    cliquesEfetivos
  };
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
  const candidatos = [
    textoNormalizado.match(/#\s*([A-Za-z0-9-]{6,})\b/i),
    textoNormalizado.match(
      /\bid(?:\s+da\s+transac(?:a|ã)o)?\b\s*:?\s*([A-Za-z0-9-]{6,})\b/i
    )
  ];
  const bloqueados = new Set([
    "detalhes",
    "entrada",
    "saida",
    "confirmado",
    "txid",
    "pix",
    "qr",
    "dinamico"
  ]);

  for (const match of candidatos) {
    if (!match || !match[1]) continue;

    const valor = normalizarEspacos(match[1]).replace(/^#+/, "").trim();
    if (!valor) continue;
    if (bloqueados.has(valor.toLowerCase())) continue;

    return valor;
  }

  return null;
}

function buildCardUniqueKey({
  txid,
  idTransacao,
  dataHora,
  valorLiquido,
  valorBruto,
  nomePagador,
  raw
}) {
  if (txid) {
    return `txid:${String(txid).trim()}`;
  }

  if (idTransacao) {
    return `id:${String(idTransacao).trim()}`;
  }

  const base = [
    normalizarDataHoraBR(dataHora || "") || "",
    Number(valorLiquido || 0).toFixed(2),
    Number(valorBruto || 0).toFixed(2),
    normalizarNomePagador(nomePagador || "") || "",
    normalizarEspacos(raw || "")
  ].join("|");

  return `fallback:${gerarHashCurto(base)}`;
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

      const { cardsMarcados } = await expandirExtratoAteLimite(page);
      const cards = page.locator("[data-codex-card-root]");

      await logDiagnosticoExtrato(page, cards);
      debugDentpeg("cards_marcados", cardsMarcados.slice(0, DENTPEG_DEBUG_CARD_LIMIT));

      const total = Math.min(await cards.count(), DENTPEG_MAX_CARDS_POR_VARREDURA);
      const transacoes = [];
      const cardsUnicos = new Set();

      for (let i = 0; i < total; i += 1) {
        try {
          const card = cards.nth(i);
          const texto = await card.innerText().catch(() => "");
          const textoNormalizado = normalizarEspacos(texto);
          const datasEncontradas = extrairDatasHorasVisiveis(texto);
          const entradasNoBloco = contarOcorrencias(texto, /\bentrada\b/gi);
          const detalhesNoBloco = contarDetalhesNoTexto(texto);

          if (!textoNormalizado) {
            if (DENTPEG_DEBUG && i < DENTPEG_DEBUG_CARD_LIMIT) {
              debugDentpeg("card_vazio", { index: i });
            }
            continue;
          }

          if (!/confirmado/i.test(textoNormalizado)) {
            if (DENTPEG_DEBUG && i < DENTPEG_DEBUG_CARD_LIMIT) {
              debugDentpeg("card_ignorado", {
                index: i,
                motivo: "sem_confirmado",
                entradasNoBloco,
                detalhesNoBloco,
                datasEncontradas,
                rawPreview: resumirTextoParaLog(texto)
              });
            }
            continue;
          }

          if (!/depix/i.test(textoNormalizado)) {
            if (DENTPEG_DEBUG && i < DENTPEG_DEBUG_CARD_LIMIT) {
              debugDentpeg("card_ignorado", {
                index: i,
                motivo: "sem_depix",
                entradasNoBloco,
                detalhesNoBloco,
                datasEncontradas,
                rawPreview: resumirTextoParaLog(texto)
              });
            }
            continue;
          }

          if (entradasNoBloco !== 1 || detalhesNoBloco !== 1) {
            if (DENTPEG_DEBUG && i < DENTPEG_DEBUG_CARD_LIMIT) {
              debugDentpeg("card_ignorado", {
                index: i,
                motivo: "bloco_nao_unico",
                entradasNoBloco,
                detalhesNoBloco,
                datasEncontradas,
                rawPreview: resumirTextoParaLog(texto, 700)
              });
            }
            continue;
          }

          const valorLiquidoTexto = extrairTextoSeguro(textoNormalizado, /DePix\s*R?\$?\s*([\d.,]+)/i);
          const valorBrutoTexto = extrairTextoSeguro(textoNormalizado, /BRUTO\s*R?\$?\s*([\d.,]+)/i);
          const dataHora = normalizarDataHoraBR(textoNormalizado);
          const idTransacao = extrairIdTransacao(textoNormalizado);
          const nomePagadorRaw = extrairNomePagador(texto);
          const nomePagador = nomePagadorRaw ? normalizarNomePagador(nomePagadorRaw) : null;

          if (!nomePagador) continue;

          const txid = await capturarTxidDoCard(page, card, textoNormalizado);

          if (DENTPEG_DEBUG && i < DENTPEG_DEBUG_CARD_LIMIT) {
            debugDentpeg("card_extraido", {
              index: i,
              entradasNoBloco,
              detalhesNoBloco,
              datasEncontradas,
              dataHoraExtraida: dataHora,
              valorLiquidoTexto,
              valorBrutoTexto,
              nomePagadorRaw: nomePagadorRaw,
              nomePagador,
              idTransacao,
              txid,
              rawPreview: resumirTextoParaLog(texto, 700)
            });
          }

          if (
            DENTPEG_DEBUG &&
            (entradasNoBloco > 1 || detalhesNoBloco > 1) &&
            i < DENTPEG_DEBUG_CARD_LIMIT
          ) {
            debugDentpeg("alerta_bloco_amplo", {
              index: i,
              entradasNoBloco,
              detalhesNoBloco,
              datasEncontradas,
              observacao:
                "O bloco capturado contem mais de uma ocorrencia de 'Entrada' ou 'Detalhes' e pode estar juntando mais de um card."
            });
          }

          const valorLiquido = normalizarValorBR(valorLiquidoTexto);
          const valorBruto = normalizarValorBR(valorBrutoTexto);
          const cardKey = buildCardUniqueKey({
            txid,
            idTransacao,
            dataHora,
            valorLiquido,
            valorBruto,
            nomePagador,
            raw: texto
          });

          if (cardsUnicos.has(cardKey)) {
            if (DENTPEG_DEBUG && i < DENTPEG_DEBUG_CARD_LIMIT) {
              debugDentpeg("card_ignorado", {
                index: i,
                motivo: "card_duplicado",
                cardKey,
                dataHoraExtraida: dataHora,
                nomePagador,
                txid,
                idTransacao,
                rawPreview: resumirTextoParaLog(texto, 700)
              });
            }
            continue;
          }

          cardsUnicos.add(cardKey);

          transacoes.push({
            valorLiquido,
            valorBruto,
            nomePagador,
            txid,
            idTransacao,
            dataHora,
            cardKey,
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
