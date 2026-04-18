require("dotenv").config();

const crypto = require("crypto");
const fetch = require("node-fetch");
const fs = require("fs");
const path = require("path");

const { capturarTransacoes, resetBrowser } = require("./dentpeg-bot");

const LOOP_INTERVAL = 4000;
const MAX_CONCURRENCY = 3;
const HTTP_RETRY_LIMIT = 3;
const QUEUE_RETRY_LIMIT = 10;
const MAX_QUEUE_SIZE = 500;
const WATCHDOG_TIMEOUT_MS = 90000;
const WATCHDOG_INTERVAL_MS = 15000;
const NO_MATCH_RETRY_DELAY_MS = 15000;
const ERROR_RETRY_DELAY_MS = 5000;
const BOT_TIMEZONE_OFFSET = String(process.env.BOT_TIMEZONE_OFFSET || "-03:00").trim();
const CACHE_FILE = path.join(__dirname, "txids.json");

let txidsProcessados = new Set();
let fila = [];
let executando = false;
let ultimaAtividade = Date.now();
let resetEmAndamento = false;

if (fs.existsSync(CACHE_FILE)) {
  try {
    const data = JSON.parse(fs.readFileSync(CACHE_FILE, "utf8"));
    if (Array.isArray(data)) {
      txidsProcessados = new Set(data.filter(Boolean));
    }
  } catch {
    txidsProcessados = new Set();
  }
}

function validarEnvObrigatorias() {
  const faltando = [];

  if (!process.env.BACKEND_URL) faltando.push("BACKEND_URL");
  if (!process.env.BOT_SECRET) faltando.push("BOT_SECRET");

  if (faltando.length > 0) {
    console.log("[worker] env obrigatoria ausente:", faltando.join(", "));
    process.exit(1);
  }
}

function salvarCache() {
  try {
    fs.writeFileSync(CACHE_FILE, JSON.stringify([...txidsProcessados], null, 2));
  } catch (error) {
    console.log("[worker] erro ao salvar cache:", error.message);
  }
}

function normalizarDataHoraParaIso(data) {
  if (!data) return null;

  const valor = String(data).trim().replace(/\s+/g, " ");

  if (!valor) return null;

  if (/^\d{4}-\d{2}-\d{2}T/.test(valor)) {
    if (/[zZ]|[+\-]\d{2}:\d{2}$/.test(valor)) {
      return valor;
    }

    return `${valor}${BOT_TIMEZONE_OFFSET}`;
  }

  const match = valor.match(/(\d{2})\/(\d{2})\/(\d{4})\s+(\d{2}):(\d{2})(?::(\d{2}))?/);
  if (!match) return null;

  const [, dia, mes, ano, hora, minuto, segundoBruto] = match;
  const segundo = segundoBruto || "00";

  return `${ano}-${mes}-${dia}T${hora}:${minuto}:${segundo}${BOT_TIMEZONE_OFFSET}`;
}

function toEpoch(data) {
  const iso = normalizarDataHoraParaIso(data);
  if (iso) {
    const parsed = Date.parse(iso);
    if (!Number.isNaN(parsed)) return parsed;
  }

  const parsed = Date.parse(String(data || ""));
  return Number.isNaN(parsed) ? NaN : parsed;
}

function buildFallbackKey(tx) {
  const base = [
    Number(tx.valorLiquido || 0).toFixed(2),
    normalizarDataHoraParaIso(tx.dataHora) || "",
    String(tx.nomePagador || "").trim().toLowerCase(),
    String(tx.raw || "").trim()
  ].join("|");

  return crypto.createHash("sha1").update(base).digest("hex");
}

function getTransactionKey(tx) {
  if (tx.txid) {
    return `txid:${String(tx.txid).trim()}`;
  }

  if (tx.idTransacao) {
    return `id:${String(tx.idTransacao).trim()}`;
  }

  const fallbackKey = tx.fallbackKey || buildFallbackKey(tx);
  return `fallback:${fallbackKey}`;
}

function getDelayForResult(resultado) {
  if (resultado.reason === "no-match") {
    return NO_MATCH_RETRY_DELAY_MS;
  }

  return ERROR_RETRY_DELAY_MS;
}

function isRetryableStatus(statusCode) {
  return statusCode >= 500 || statusCode === 429;
}

async function enviarParaBackend(tx, tentativa = 1) {
  if (!tx.valorLiquido || tx.valorLiquido <= 0) {
    return { ok: false, requeue: false, reason: "invalid-value" };
  }

  const dataHoraIso = tx.dataHoraIso || normalizarDataHoraParaIso(tx.dataHora);

  if (!dataHoraIso) {
    return { ok: false, requeue: false, reason: "invalid-date" };
  }

  const payload = {
    txid: tx.txid || null,
    valorLiquido: tx.valorLiquido,
    valorBruto: tx.valorBruto || null,
    nomePagador: tx.nomePagador,
    dataHora: dataHoraIso,
    idTransacao: tx.idTransacao || null,
    fallbackKey: tx.fallbackKey || buildFallbackKey(tx),
    raw: tx.raw || null
  };

  try {
    const res = await fetch(`${process.env.BACKEND_URL}/deposito/confirmar-bot`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "x-bot-token": process.env.BOT_SECRET
      },
      body: JSON.stringify(payload),
      timeout: 15000
    });

    const bodyText = await res.text();

    if (!res.ok) {
      if (bodyText.includes("Nenhum depósito compatível encontrado")) {
        return { ok: false, requeue: true, reason: "no-match", error: bodyText };
      }

      if (isRetryableStatus(res.status) && tentativa < HTTP_RETRY_LIMIT) {
        console.log("[worker] retry http:", getTransactionKey(tx), "| tentativa", tentativa);
        await new Promise((resolve) => setTimeout(resolve, 1000));
        return enviarParaBackend(tx, tentativa + 1);
      }

      return {
        ok: false,
        requeue: isRetryableStatus(res.status),
        reason: `http-${res.status}`,
        error: bodyText || `HTTP ${res.status}`
      };
    }

    console.log("[worker] enviado:", getTransactionKey(tx));
    return { ok: true };
  } catch (error) {
    if (tentativa < HTTP_RETRY_LIMIT) {
      console.log("[worker] retry rede:", getTransactionKey(tx), "| tentativa", tentativa);
      await new Promise((resolve) => setTimeout(resolve, 1000));
      return enviarParaBackend(tx, tentativa + 1);
    }

    return {
      ok: false,
      requeue: true,
      reason: "network",
      error: error.message || "network error"
    };
  }
}

function puxarChunkProcessavel() {
  const agora = Date.now();
  const prontas = [];
  const pendentes = [];

  for (const item of fila) {
    const liberada = !item.proximaTentativaEm || item.proximaTentativaEm <= agora;

    if (liberada && prontas.length < MAX_CONCURRENCY) {
      prontas.push(item);
      continue;
    }

    pendentes.push(item);
  }

  fila = pendentes;
  return prontas;
}

async function processarFila() {
  const chunk = puxarChunkProcessavel();
  if (chunk.length === 0) return;

  await Promise.all(
    chunk.map(async (tx) => {
      const resultado = await enviarParaBackend(tx);

      if (resultado.ok) {
        txidsProcessados.add(tx.chave);
        salvarCache();
        return;
      }

      const tentativas = (tx.tentativas || 0) + 1;
      tx.tentativas = tentativas;

      if (!resultado.requeue) {
        console.log(
          "[worker] falha definitiva:",
          tx.chave,
          resultado.reason,
          resultado.error || ""
        );
        return;
      }

      if (tentativas >= QUEUE_RETRY_LIMIT) {
        console.log("[worker] limite de retries atingido:", tx.chave, resultado.reason);
        return;
      }

      if (fila.length >= MAX_QUEUE_SIZE) {
        console.log("[worker] fila cheia, descartando retry:", tx.chave);
        return;
      }

      tx.proximaTentativaEm = Date.now() + getDelayForResult(resultado);
      fila.push(tx);

      if (resultado.reason === "no-match") {
        console.log("[worker] sem match ainda, reprogramado:", tx.chave);
      } else {
        console.log("[worker] reprogramado para retry:", tx.chave, resultado.reason);
      }
    })
  );
}

async function loop() {
  if (executando) return;

  executando = true;
  ultimaAtividade = Date.now();

  try {
    const transacoes = (await capturarTransacoes()) || [];
    ultimaAtividade = Date.now();

    if (transacoes.length === 0) {
      console.log("[worker] nenhuma transacao nova capturada");
    } else {
      console.log("[worker] capturadas:", transacoes.length);
    }

    let jaProcessadosSeguidos = 0;

    for (const tx of transacoes) {
      const dataHoraIso = normalizarDataHoraParaIso(tx.dataHora);
      const dataTxEpoch = toEpoch(dataHoraIso || tx.dataHora);

      if (!dataHoraIso || Number.isNaN(dataTxEpoch)) {
        console.log("[worker] transacao ignorada por data invalida:", tx.dataHora || "(vazia)");
        continue;
      }

      const diffHoras = Math.abs(Date.now() - dataTxEpoch) / 3600000;
      if (diffHoras > 2) {
        console.log("[worker] ignorada por ser antiga:", dataHoraIso);
        continue;
      }

      if (!tx.valorLiquido || tx.valorLiquido <= 0) {
        console.log("[worker] ignorada por valor invalido");
        continue;
      }

      const fallbackKey = buildFallbackKey({
        ...tx,
        dataHora: dataHoraIso
      });

      const itemFila = {
        ...tx,
        dataHoraIso,
        fallbackKey
      };

      itemFila.chave = getTransactionKey(itemFila);

      if (txidsProcessados.has(itemFila.chave)) {
        jaProcessadosSeguidos += 1;

        if (jaProcessadosSeguidos >= 5) {
          console.log("[worker] 5 processadas seguidas, encerrando a varredura");
          break;
        }

        continue;
      }

      jaProcessadosSeguidos = 0;

      if (!itemFila.txid && !itemFila.idTransacao) {
        console.log("[worker] sem txid/id, usando fallback deterministico:", itemFila.fallbackKey);
      }

      const jaNaFila = fila.some((existente) => existente.chave === itemFila.chave);
      if (!jaNaFila) {
        fila.push(itemFila);
      }
    }

    console.log("[worker] fila:", fila.length);
    await processarFila();
    ultimaAtividade = Date.now();
  } catch (error) {
    console.log("[worker] erro no loop:", error.message);
    await resetBrowser();
  } finally {
    executando = false;
  }
}

setInterval(async () => {
  const travado = executando && Date.now() - ultimaAtividade > WATCHDOG_TIMEOUT_MS;

  if (!travado || resetEmAndamento) return;

  resetEmAndamento = true;
  console.log("[worker] watchdog acionado, resetando browser");

  try {
    await resetBrowser();
  } catch (error) {
    console.log("[worker] erro no reset do watchdog:", error.message);
  } finally {
    executando = false;
    ultimaAtividade = Date.now();
    resetEmAndamento = false;
  }
}, WATCHDOG_INTERVAL_MS);

async function start() {
  validarEnvObrigatorias();
  console.log("[worker] bot iniciado");
  console.log("[worker] backend:", process.env.BACKEND_URL);

  await loop();
  setInterval(loop, LOOP_INTERVAL);
}

start().catch((error) => {
  console.error("[worker] falha fatal ao iniciar:", error);
  process.exit(1);
});
