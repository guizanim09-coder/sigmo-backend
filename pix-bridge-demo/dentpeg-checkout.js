const { chromium } = require("playwright");

const DENTPEG_CHECKOUT_URL = String(
  process.env.DENTPEG_CHECKOUT_URL || "https://dentpeg.com/checkout/sigmo"
).trim();

const DENTPEG_HEADLESS = String(process.env.DENTPEG_HEADLESS || "true").trim() !== "false";
const DENTPEG_TIMEOUT_MS = Number(process.env.DENTPEG_TIMEOUT_MS || 45000);

function formatAmountForInput(amount) {
  return Number(amount).toFixed(2).replace(".", ",");
}

async function firstVisibleLocator(page, selectors) {
  for (const selector of selectors) {
    const locator = page.locator(selector).first();
    if (await locator.count()) {
      try {
        if (await locator.isVisible({ timeout: 1000 })) {
          return locator;
        }
      } catch (_error) {
        // Tenta o proximo seletor.
      }
    }
  }

  return null;
}

async function waitForVisibleLocator(page, selectors, timeoutMs) {
  const deadline = Date.now() + timeoutMs;

  while (Date.now() < deadline) {
    const locator = await firstVisibleLocator(page, selectors);
    if (locator) {
      return locator;
    }

    await page.waitForTimeout(300);
  }

  return null;
}

async function extractVisiblePixCode(page) {
  const candidates = [
    "textarea",
    "input[readonly]",
    "[data-testid*='pix']",
    "[class*='pix']",
    "code",
    "pre",
    "p",
    "div",
    "span"
  ];

  const values = [];

  for (const selector of candidates) {
    const locators = page.locator(selector);
    const count = Math.min(await locators.count(), 20);

    for (let index = 0; index < count; index += 1) {
      const locator = locators.nth(index);

      try {
        if (!(await locator.isVisible({ timeout: 250 }))) {
          continue;
        }

        const value =
          (await locator.inputValue().catch(() => "")) ||
          (await locator.textContent().catch(() => "")) ||
          "";

        const text = String(value).trim();
        if (!text || text.length < 20) {
          continue;
        }

        values.push(text);
      } catch (_error) {
        // Ignora e segue.
      }
    }
  }

  const pixLike = values.find((text) => /^000201/.test(text.replace(/\s+/g, "")));
  if (pixLike) {
    return pixLike.replace(/\s+/g, "");
  }

  return values[0] || "";
}

async function createPixViaDentpegCheckout(amount) {
  const browser = await chromium.launch({
    headless: DENTPEG_HEADLESS
  });

  const context = await browser.newContext({
    permissions: ["clipboard-read", "clipboard-write"],
    viewport: { width: 1365, height: 900 }
  });

  const page = await context.newPage();
  page.setDefaultTimeout(DENTPEG_TIMEOUT_MS);

  try {
    await page.goto(DENTPEG_CHECKOUT_URL, {
      waitUntil: "domcontentloaded",
      timeout: DENTPEG_TIMEOUT_MS
    });

    const amountInput = await firstVisibleLocator(page, [
      "input[type='number']",
      "input[inputmode='decimal']",
      "input[inputmode='numeric']",
      "input[placeholder*='valor' i]",
      "input"
    ]);

    if (!amountInput) {
      throw new Error("Nao encontrei o campo de valor no checkout");
    }

    await amountInput.click();
    await amountInput.fill("");
    await amountInput.type(formatAmountForInput(amount), { delay: 35 });

    const generateButton = await firstVisibleLocator(page, [
      "button:has-text('Gerar PIX')",
      "button:has-text('Gerar Pix')",
      "button:has-text('Gerar')"
    ]);

    if (!generateButton) {
      throw new Error("Nao encontrei o botao de gerar PIX");
    }

    await generateButton.click();

    const copyButton = await waitForVisibleLocator(page, [
      "button:has-text('Copiar codigo PIX')",
      "button:has-text('Copiar código PIX')",
      "button:has-text('Copiar codigo Pix')",
      "button:has-text('Copiar')"
    ], 15000);

    if (!copyButton) {
      throw new Error("Nao encontrei o botao de copiar o codigo PIX");
    }

    await copyButton.click();
    await page.waitForTimeout(500);

    let pixKey = "";
    try {
      pixKey = await page.evaluate(async () => navigator.clipboard.readText());
    } catch (_error) {
      pixKey = "";
    }

    if (!pixKey) {
      pixKey = await extractVisiblePixCode(page);
    }

    if (!pixKey) {
      throw new Error("Nao foi possivel extrair o codigo PIX da pagina");
    }

    const qrImage = await firstVisibleLocator(page, [
      "img[src^='data:image']",
      "img[src*='qr']",
      "canvas"
    ]);

    let qrCodeBase64 = "";
    if (qrImage) {
      qrCodeBase64 =
        (await qrImage.getAttribute("src").catch(() => "")) ||
        (await qrImage.evaluate((node) => {
          if (node instanceof HTMLCanvasElement) {
            return node.toDataURL("image/png");
          }

          return "";
        }).catch(() => ""));
    }

    const timerBadge = await firstVisibleLocator(page, [
      "text=/\\d{1,2}:\\d{2}/",
      "[class*='timer']",
      "[class*='countdown']"
    ]);

    const timerText = timerBadge ? String(await timerBadge.textContent().catch(() => "")).trim() : "";

    return {
      ok: true,
      processor: "dentpeg.com/checkout/sigmo",
      source: "dentpeg-checkout-browser",
      orderId: `checkout_${Date.now()}`,
      amount,
      pixKey,
      qrCodeBase64,
      expiresInSeconds: 0,
      timerText
    };
  } finally {
    await context.close().catch(() => {});
    await browser.close().catch(() => {});
  }
}

module.exports = {
  createPixViaDentpegCheckout,
  DENTPEG_CHECKOUT_URL
};
