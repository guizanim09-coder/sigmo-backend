require("dotenv").config();

const crypto = require("crypto");
const fetch = require("node-fetch");
const fs = require("fs");
const path = require("path");

const { capturarTransacoes, resetBrowser } = require("./dentpeg-bot");

function getBoundedInt(value, fallback, { min = 1, max = Number.MAX_SAFE_INTEGER } = {}) {
  const numero = Number(value);

  if (!Number.isFinite(numero)) return fallback;

  const inteiro = Math.trunc(numero);
  if (inteiro < min) return min;
  if (inteiro > max) return max;
  return inteiro;
}

const LOOP_INTERVAL = 4000;
const MAX_CONCURRENCY = getBoundedInt(process.env.WORKER_MAX_CONCURRENCY, 6, {
  min: 1,
  max: 8
});
const MAX_BATCHES_PER_LOOP = getBoundedInt(process.env.WORKER_MAX_BATCHES_PER_LOOP, 4, {
  min: 1,
  max: 10
});
const HTTP_RETRY_LIMIT = 3;
const QUEUE_RETRY_LIMIT = 10;
const MAX_QUEUE_SIZE = 500;
const WATCHDOG_TIMEOUT_MS = 90000;
const WATCHDOG_INTERVAL_MS = 15000;
const NO_MATCH_RETRY_DELAY_MS = 15000;
const ERROR_RETRY_DELAY_MS = 5000;
const APP_LOCAL_TIMEZONE = String(
  process.env.APP_LOCAL_TIMEZONE || "America/Sao_Paulo"
).trim();
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

function parseDataHoraLocal(data) {
  if (!data) return null;

  const valor = String(data).trim().replace(/\s+/g, " ");

  if (!valor) return null;

  let match = valor.match(
    /(\d{2})\/(\d{2})\/(\d{4})\s+(\d{2}):(\d{2})(?::(\d{2}))?/
  );

  if (match) {
    const [, dia, mes, ano, hora, minuto, segundoBruto] = match;
    return {
      ano: Number(ano),
      mes: Number(mes),
      dia: Number(dia),
      hora: Number(hora),
      minuto: Number(minuto),
      segundo: Number(segundoBruto || "0")
    };
  }

  match = valor.match(
    /(\d{4})-(\d{2})-(\d{2})[T\s](\d{2}):(\d{2})(?::(\d{2}))?/
  );
  if (!match) return null;

  const [, ano, mes, dia, hora, minuto, segundoBruto] = match;
  return {
    ano: Number(ano),
    mes: Number(mes),
    dia: Number(dia),
    hora: Number(hora),
    minuto: Number(minuto),
    segundo: Number(segundoBruto || "0")
  };
}

function normalizarDataHoraLocal(data) {
  const partes = parseDataHoraLocal(data);
  if (!partes) return null;

  const pad = (numero) => String(numero).padStart(2, "0");

  return `${partes.ano}-${pad(partes.mes)}-${pad(partes.dia)} ${pad(
    partes.hora
  )}:${pad(partes.minuto)}:${pad(partes.segundo)}`;
}

function toEpochLocal(data) {
  const partes = parseDataHoraLocal(data);
  if (!partes) return NaN;

  return Date.UTC(
    partes.ano,
    partes.mes - 1,
    partes.dia,
    partes.hora,
    partes.minuto,
    partes.segundo || 0
  );
}

function formatDateKey(partes) {
  const pad = (numero) => String(numero).padStart(2, "0");
  return `${partes.ano}-${pad(partes.mes)}-${pad(partes.dia)}`;
}

function getDateKeyFromLocalDateTime(data) {
  const partes = parseDataHoraLocal(data);
  if (!partes) return null;
  return formatDateKey(partes);
}

function getAgoraLocalParts() {
  const formatter = new Intl.DateTimeFormat("en-CA", {
    timeZone: APP_LOCAL_TIMEZONE,
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: false
  });

  const parts = Object.fromEntries(
    formatter
      .formatToParts(new Date())
      .filter((item) => item.type !== "literal")
      .map((item) => [item.type, item.value])
  );

  return {
    ano: Number(parts.year),
    mes: Number(parts.month),
    dia: Number(parts.day),
    hora: Number(parts.hour),
    minuto: Number(parts.minute),
    segundo: Number(parts.second)
  };
}

function getAgoraLocalDateKey() {
  try {
    return formatDateKey(getAgoraLocalParts());
  } catch {
    return new Date().toISOString().slice(0, 10);
  }
}

function getAgoraLocalEpoch() {
  try {
    const parts = getAgoraLocalParts();

    return Date.UTC(
      Number(parts.ano),
      Number(parts.mes) - 1,
      Number(parts.dia),
      Number(parts.hora),
      Number(parts.minuto),
      Number(parts.segundo)
    );
  } catch {
    return Date.now();
  }
}

function buildFallbackKey(tx) {
  if (tx.cardKey) {
    return crypto
      .createHash("sha1")
      .update(String(tx.cardKey).trim())
      .digest("hex");
  }

  const base = [
    Number(tx.valorLiquido || 0).toFixed(2),
    normalizarDataHoraLocal(tx.dataHora) || "",
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

  if (tx.cardKey) {
    return `card:${String(tx.cardKey).trim()}`;
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

  const dataHoraLocal = tx.dataHoraLocal || normalizarDataHoraLocal(tx.dataHora);

  if (!dataHoraLocal) {
    return { ok: false, requeue: false, reason: "invalid-date" };
  }

  const payload = {
    txid: tx.txid || null,
    valorLiquido: tx.valorLiquido,
    valorBruto: tx.valorBruto || null,
    nomePagador: tx.nomePagador,
    dataHora: dataHoraLocal,
    idTransacao: tx.idTransacao || null,
    cardKey: tx.cardKey || null,
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
  let lotesProcessados = 0;
  let itensProcessados = 0;

  while (lotesProcessados < MAX_BATCHES_PER_LOOP) {
    const chunk = puxarChunkProcessavel();
    if (chunk.length === 0) break;

    lotesProcessados += 1;
    itensProcessados += chunk.length;

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

    ultimaAtividade = Date.now();
  }

  return {
    lotesProcessados,
    itensProcessados
  };
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
      const dataHoraLocal = normalizarDataHoraLocal(tx.dataHora);
      const dataTxDia = getDateKeyFromLocalDateTime(dataHoraLocal || tx.dataHora);

      if (!dataHoraLocal || !dataTxDia) {
        console.log("[worker] transacao ignorada por data invalida:", tx.dataHora || "(vazia)");
        continue;
      }

      const hojeLocal = getAgoraLocalDateKey();
      if (dataTxDia !== hojeLocal) {
        console.log("[worker] ignorada por ser de outra data:", dataHoraLocal);
        continue;
      }

      if (!tx.valorLiquido || tx.valorLiquido <= 0) {
        console.log("[worker] ignorada por valor invalido");
        continue;
      }

      const fallbackKey = buildFallbackKey({
        ...tx,
        dataHora: dataHoraLocal
      });

      const itemFila = {
        ...tx,
        dataHoraLocal,
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
    const resumoFila = await processarFila();
    if (resumoFila.itensProcessados > 0) {
      console.log(
        "[worker] lotes enviados no ciclo:",
        `${resumoFila.lotesProcessados} lotes / ${resumoFila.itensProcessados} itens`
      );
    }
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
  console.log(
    "[worker] throughput:",
    `concorrencia=${MAX_CONCURRENCY} lotesPorLoop=${MAX_BATCHES_PER_LOOP}`
  );

  await loop();
  setInterval(loop, LOOP_INTERVAL);
}

start().catch((error) => {
  console.error("[worker] falha fatal ao iniciar:", error);
  process.exit(1);
});
