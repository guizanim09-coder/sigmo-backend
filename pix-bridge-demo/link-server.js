const express = require("express");

const app = express();
const PORT = Number(process.env.PORT || process.env.LINK_PORT || 4100);

app.use(express.json());
app.use(express.urlencoded({ extended: true }));

// Este servidor simula o que seria o backend de dentpeg.com/checkout/sigmo.
// Ele nao precisa servir a interface da mascara: sua funcao e apenas
// receber o valor, criar o pedido e devolver a chave PIX.
app.get("/", (_req, res) => {
  res.send(`
    <!doctype html>
    <html lang="pt-BR">
      <head>
        <meta charset="utf-8" />
        <meta name="viewport" content="width=device-width, initial-scale=1" />
        <title>dentpeg.com/checkout/sigmo</title>
        <style>
          body { font-family: Arial, sans-serif; padding: 32px; background: #f6f8fb; }
          .card { max-width: 640px; margin: 0 auto; background: #fff; padding: 24px; border-radius: 18px; box-shadow: 0 12px 40px rgba(0,0,0,.08); }
          input, button { width: 100%; padding: 14px; margin-top: 12px; border-radius: 12px; border: 1px solid #cfd8e3; }
          button { cursor: pointer; background: #0b7d4e; color: #fff; border: 0; font-weight: 700; }
          pre { white-space: pre-wrap; word-break: break-word; background: #f2f7f4; padding: 16px; border-radius: 12px; }
        </style>
      </head>
      <body>
        <div class="card">
          <h1>dentpeg.com/checkout/sigmo</h1>
          <p>Este e o servico que realmente cria a cobranca PIX.</p>
          <input id="amount" type="number" min="1" placeholder="Digite o valor" />
          <button onclick="generatePix()">Gerar chave PIX no dentpeg.com/checkout/sigmo</button>
          <pre id="result">Aguardando geracao...</pre>
        </div>

        <script>
          async function generatePix() {
            const amount = Number(document.getElementById("amount").value || 0);
            const result = document.getElementById("result");

            if (!amount || amount <= 0) {
              result.textContent = "Digite um valor valido.";
              return;
            }

            const response = await fetch("/api/pix", {
              method: "POST",
              headers: { "content-type": "application/json" },
              body: JSON.stringify({ amount, source: "dentpeg-ui" })
            });

            const data = await response.json();
            result.textContent = JSON.stringify(data, null, 2);
          }
        </script>
      </body>
    </html>
  `);
});

// Endpoint que representa a API oficial de geracao de cobranca.
app.post("/api/pix", (req, res) => {
  const amount = Number(req.body.amount || 0);
  const source = req.body.source || "unknown";

  if (!amount || amount <= 0) {
    return res.status(400).json({
      ok: false,
      error: "amount must be greater than zero"
    });
  }

  const orderId = `LINK-${Date.now()}`;
  const pixKey = `pix|order=${orderId}|amount=${amount.toFixed(2)}|issued-by=dentpeg.com/checkout/sigmo`;

  // Em um sistema real, aqui ficaria a criacao da cobranca no PSP/gateway.
  return res.json({
    ok: true,
    processor: "dentpeg.com/checkout/sigmo",
    source,
    orderId,
    amount,
    pixKey,
    qrCodeBase64: "DEMO_QR_CODE_BASE64",
    expiresInSeconds: 900
  });
});

app.listen(PORT, () => {
  console.log(`dentpeg upstream demo running at http://localhost:${PORT}`);
});
