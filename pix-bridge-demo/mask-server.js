const express = require("express");
const path = require("path");
const { createPixViaDentpegCheckout, DENTPEG_CHECKOUT_URL } = require("./dentpeg-checkout");

const app = express();
const PORT = Number(process.env.PORT || process.env.MASK_PORT || 3100);
const DENTPEG_API_BASE = String(
  process.env.DENTPEG_API_BASE || "https://api.dentpeg.com/api/v1"
).trim();
const DENTPEG_API_KEY = String(process.env.DENTPEG_API_KEY || "").trim();
const UPSTREAM_API_BASE = String(process.env.UPSTREAM_API_BASE || "http://localhost:4100").trim();
const PUBLIC_DIR = path.join(__dirname, "public");
const FRONTEND_ORIGIN = String(process.env.FRONTEND_ORIGIN || "").trim();

app.use(express.json());
app.use(express.urlencoded({ extended: true }));
app.use(express.static(PUBLIC_DIR));

function isAllowedOrigin(origin) {
  if (!origin) return true;

  const allowList = new Set(
    [
      FRONTEND_ORIGIN,
      "http://localhost:3100",
      "http://localhost:5173",
      "http://localhost:5500",
      "http://127.0.0.1:5500"
    ].filter(Boolean)
  );

  return allowList.has(origin);
}

app.use((req, res, next) => {
  const origin = String(req.headers.origin || "");

  if (isAllowedOrigin(origin)) {
    res.setHeader("Access-Control-Allow-Origin", origin || "*");
    res.setHeader("Vary", "Origin");
  }

  res.setHeader("Access-Control-Allow-Headers", "Content-Type");
  res.setHeader("Access-Control-Allow-Methods", "GET,POST,OPTIONS");

  if (req.method === "OPTIONS") {
    return res.sendStatus(204);
  }

  next();
});

app.get("/health", (_req, res) => {
  res.json({
    ok: true,
    service: "mask-backend",
    dentpegApiBase: DENTPEG_API_BASE,
    hasDentpegApiKey: Boolean(DENTPEG_API_KEY),
    upstreamApiBase: UPSTREAM_API_BASE || null
  });
});

// A UI fica completamente em arquivos estaticos dentro de public/.
app.get(["/", "/sigmo"], (_req, res) => {
  res.sendFile(path.join(PUBLIC_DIR, "index.html"));
});

// O frontend do exemplomascara.com conversa apenas com este endpoint.
// Quando DENTPEG_API_KEY existir, a cobranca e criada pela API oficial da DentPeg.
// Sem a chave, o backend usa automacao no checkout proprio da DentPeg.
// O simulador local ainda pode ser usado via UPSTREAM_API_BASE, se necessario.
app.post("/api/pix", async (req, res) => {
  const amount = Number(req.body.amount || 0);

  if (!amount || amount <= 0) {
    return res.status(400).json({
      ok: false,
      error: "amount must be greater than zero"
    });
  }

  try {
    if (DENTPEG_API_KEY) {
      const dentpegResponse = await fetch(`${DENTPEG_API_BASE}/deposits`, {
        method: "POST",
        headers: {
          "content-type": "application/json",
          "x-api-key": DENTPEG_API_KEY
        },
        body: JSON.stringify({
          amountInCents: Math.round(amount * 100)
        })
      });

      const dentpegData = await dentpegResponse.json();

      if (!dentpegResponse.ok) {
        return res.status(dentpegResponse.status).json({
          ok: false,
          error: dentpegData.error || dentpegData.message || "dentpeg request failed"
        });
      }

      return res.json({
        ok: true,
        processor: "dentpeg.com/checkout/sigmo",
        source: "sigmo-mask",
        orderId: dentpegData.deposit?.id || `dep_${Date.now()}`,
        amount,
        pixKey: dentpegData.deposit?.qrCode || "",
        qrCodeBase64: dentpegData.deposit?.qrImageUrl || "",
        expiresInSeconds: dentpegData.deposit?.expiration
          ? Math.max(
              0,
              Math.floor((new Date(dentpegData.deposit.expiration).getTime() - Date.now()) / 1000)
            )
          : 0,
        rawDeposit: dentpegData.deposit || null
      });
    }

    if (!DENTPEG_API_KEY && DENTPEG_CHECKOUT_URL) {
      const checkoutData = await createPixViaDentpegCheckout(amount);
      return res.json(checkoutData);
    }

    const upstreamResponse = await fetch(`${UPSTREAM_API_BASE}/api/pix`, {
      method: "POST",
      headers: {
        "content-type": "application/json"
      },
      body: JSON.stringify({
        amount,
        source: "sigmo-mask"
      })
    });

    const upstreamData = await upstreamResponse.json();

    if (!upstreamResponse.ok) {
      return res.status(upstreamResponse.status).json({
        ok: false,
        error: upstreamData.error || "upstream request failed"
      });
    }

    return res.json(upstreamData);
  } catch (error) {
    return res.status(502).json({
      ok: false,
      error: "Nao foi possivel contactar o servico configurado",
      details: error.message
    });
  }
});

app.listen(PORT, () => {
  console.log(`exemplomascara.com demo running at http://localhost:${PORT}`);
  console.log(`Using DENTPEG_API_BASE=${DENTPEG_API_BASE}`);
  console.log(`Using DENTPEG_CHECKOUT_URL=${DENTPEG_CHECKOUT_URL}`);
  console.log(`Using UPSTREAM_API_BASE=${UPSTREAM_API_BASE}`);
});
